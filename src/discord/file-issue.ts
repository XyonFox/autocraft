// Filing a drafted issue from Discord under the person's own GitHub account: the reporter sign-in, the junk check and
// the limits. The Discord flow (src/discord/issue-new.ts) calls fileIssue() when the person presses File it.
// The reporter's token set is kept by src/discord/reporter.ts; the sign-in routes are src/dash/server/reporter-signin.ts.
import { jevAvailable } from "../agents/jev.ts";
import { runModelStep, type ClaudeStep, type JevStep } from "../agents/step.ts";
import { envVar } from "../env.ts";
import { TokenBox } from "../dash/server/crypto.ts";
import { readSecrets } from "../dash/server/config.ts";
import { GitHubProvider, type Provider } from "../dash/server/provider.ts";
import { exec as defaultExec, type Exec } from "../fixup/git.ts";
import { createReporterIssue, IssueError, labelReporterIssue } from "../github/write.ts";
import { appChildEnv } from "../github/install-token.ts";
import { openAppKeys } from "../github/app-keys.ts";
import { JEV_PROMPTS } from "../prompts/index.ts";
import { isPublicRepo } from "../public/status.ts";
import { jevJunk, junkBand, junkState, parseJunk, type JunkAsk } from "./junk-ask.ts";
import { failReporter, forgetReporterRow, newReporterState, readReporter, reporterToken } from "./reporter.ts";
import type { Db } from "../db/index.ts";
import * as kx from "../db/kx.ts";
import type { SelectQueryBuilder } from "kysely";

/** Filing from Discord under the person's account is built but off: the sign-in needs a Discord identity step first, so a forwarded sign-in link cannot link someone else's GitHub account. While off, the draft offers only the prefilled GitHub link and the sign-in routes answer 404. */
export const FILING_IN_DISCORD = false;

export type IssueKind = "bug" | "feature";

export type FileRequest = {
  /** Keyed hash of the Discord user (`keys.hash("author", id)`). */
  reporter: string;
  repo: string;
  title: string;
  body: string;
  kind: IssueKind;
};

export type FileResult =
  /** The person has not signed in with GitHub (or the sign-in expired): show a link button to `url`. */
  | { status: "sign-in"; url: string }
  | { status: "filed"; number: number; url: string; login: string }
  /** Not filed: a limit, the junk check or a GitHub error. `reason` is shown to the person; the flow offers the GitHub link instead. */
  | { status: "refused"; reason: string };

export type FileDeps = { db: Db; log: (line: string) => void };

/** Limits over 24 hours. `DAILY_WRITES` is the GitHub write limit of the audit action `discord-issue-open`. */
export const PER_LOGIN = 3;
export const PER_REPO = 30;
export const DAILY_WRITES = 50;
export const FOOTER = "Filed from Discord with AutoCraft.";
export const AUDIT_ACTION = "discord-issue-open";
const DAY_MS = 86_400_000;
const TITLE_MAX = 256;
const LABEL: Record<IssueKind, string> = { bug: "bug", feature: "enhancement" };

/** What a test replaces: the token box and provider (else the secrets file's), `gh`, the junk judges, the clock and the public base URL. */
export type FileHooks = {
  box?: TokenBox; provider?: Provider; exec?: Exec; baseUrl?: string | null; now?: () => number;
  judge?: { jev: (db: Db, a: JunkAsk) => Promise<number | null>; haiku: (db: Db, a: JunkAsk) => Promise<boolean | null> };
};

const JEV_STEP: JevStep<JunkAsk, number> = {
  provider: "jev", id: "discord.junk.jev",
  request: (a) => ({ item: { ref: "discord" }, body: JSON.stringify({ ...JEV_PROMPTS["discord.junk.jev"].request, state: junkState(a) }) }),
  parse: (answers, _a, ctx) => {
    const p = ctx.code === 0 ? jevJunk(answers) : null;
    return p === null ? null : { value: p };
  },
};
const HAIKU_STEP: ClaudeStep<JunkAsk, boolean> = {
  provider: "claude", id: "discord.junk.haiku",
  toPrompt: (a) => [a],
  parse: async (text) => {
    const v = parseJunk(text);
    return v === null ? null : { value: v };
  },
};
const MODEL_WAIT_MS = 20_000;
const modelJudge: NonNullable<FileHooks["judge"]> = {
  jev: async (db, a) => { const r = await runModelStep(db, JEV_STEP, a); return r.ok ? r.value : null; },
  haiku: async (db, a) => { const r = await runModelStep(db, HAIKU_STEP, a, { maxWaitMs: MODEL_WAIT_MS }); return r.ok ? r.value : null; },
};

/** Jev first, Haiku where Jev is unsure or unavailable. With no model answer the draft is not filed ("unknown"): the person still has the GitHub link. The texts of the calls are deleted when they end. */
async function junkCheck(db: Db, a: JunkAsk, judge: NonNullable<FileHooks["judge"]>, log: (l: string) => void): Promise<"junk" | "ok" | "unknown"> {
  try {
    if (await jevAvailable(db)) {
      const p = await judge.jev(db, a);
      const band = p === null ? "unsure" : junkBand(p);
      if (band === "yes") return "junk";
      if (band === "no") return "ok";
    }
    const v = await judge.haiku(db, a);
    return v === null ? "unknown" : v ? "junk" : "ok";
  } catch (e) {
    log(`discord: junk check failed: ${(e as Error).message.slice(0, 200)}`);
    return "unknown";
  } finally {
    try {
      await db.run(db.k.deleteFrom("agent_run_texts").where("run_id", "in", (eb) => eb.selectFrom("agent_runs").select("id").where("step", "=", "discord-issue").where("state", "<>", "running")));
    } catch (e) { log(`discord: removing junk texts failed: ${(e as Error).message}`); }
  }
}

async function secretsOf(db: Db, env = process.env): Promise<{ box: TokenBox; provider: Provider } | null> {
  try {
    const s = readSecrets(envVar("SECRETS_FILE", env));
    const app = (await openAppKeys(db, env))?.current() ?? null;
    return app ? { box: new TokenBox(s.sessionKey), provider: new GitHubProvider(app.clientId, app.clientSecret) } : null;
  } catch {
    return null;
  }
}

const count = async (db: Db, q: SelectQueryBuilder<any, any, { c: number }>): Promise<number> => Number((await db.get(q))!.c);

export async function fileIssue(deps: FileDeps, req: FileRequest, hooks: FileHooks = {}): Promise<FileResult> {
  const { db, log } = deps;
  const now = (hooks.now ?? Date.now)();
  const title = req.title.replace(/\s+/g, " ").trim();
  const body = req.body.trim();
  if (!await isPublicRepo(db, req.repo)) return { status: "refused", reason: "That repo is not open for filing from Discord." };
  if (!title || !body) return { status: "refused", reason: "The title and the description must not be empty." };
  if (title.length > TITLE_MAX) return { status: "refused", reason: "The title is too long." };

  const keys = hooks.box && hooks.provider ? { box: hooks.box, provider: hooks.provider } : await secretsOf(db);
  const baseUrl = (hooks.baseUrl === undefined ? envVar("BASE_URL") ?? null : hooks.baseUrl)?.replace(/\/+$/, "") ?? null;
  if (!keys || !baseUrl) return { status: "refused", reason: "Filing from Discord is not set up." };

  const tok = await reporterToken(db, keys.box, keys.provider, req.reporter, now);
  if (!tok.ok && tok.signIn) return { status: "sign-in", url: `${baseUrl}/discord/github/start?state=${encodeURIComponent(await newReporterState(db, req.reporter, now))}` };
  if (!tok.ok) return { status: "refused", reason: "GitHub did not answer. Try again in a minute." };

  const since = new Date(now - DAY_MS).toISOString();
  await db.run(db.k.deleteFrom("discord_filed").where("at", "<", since));
  if (await count(db, db.k.selectFrom("discord_filed").select((eb) => eb.fn.countAll<number>().as("c")).where("login", "=", tok.login).where("at", ">=", since)) >= PER_LOGIN) return { status: "refused", reason: `You have filed ${PER_LOGIN} issues from Discord in the last 24 hours.` };
  if (await count(db, db.k.selectFrom("discord_filed").select((eb) => eb.fn.countAll<number>().as("c")).where("repo", "=", req.repo).where("at", ">=", since)) >= PER_REPO) return { status: "refused", reason: "This repo has reached its limit of issues filed from Discord today." };
  if (await count(db, db.k.selectFrom("audit").select((eb) => eb.fn.countAll<number>().as("c")).where("action", "=", AUDIT_ACTION).where("at", ">=", since).where(kx.jsonText(db, "details", "$.state"), "<>", "failed")) >= DAILY_WRITES) return { status: "refused", reason: "Filing from Discord has reached its daily limit." };
  // The slot is reserved before any await, so parallel sessions of one person cannot pass the limit together; every failure below frees it.
  const slot = Number((await db.get(db.k.insertInto("discord_filed").values({ repo: req.repo, number: 0, login: tok.login, reporter: req.reporter, at: new Date(now).toISOString() }).returning(kx.rowid("discord_filed").as("rowid"))))!.rowid);
  let filed = false;
  try {
    const junk = await junkCheck(db, { title, body }, hooks.judge ?? modelJudge, log);
    if (junk === "junk") return { status: "refused", reason: "That does not look like a bug report or a feature request." };
    if (junk === "unknown") return { status: "refused", reason: "The report could not be checked just now. Try again in a few minutes." };

    const run = hooks.exec ?? defaultExec;
    const env = appChildEnv(tok.access);
    const asUser: Exec = (cmd, args, opts) => run(cmd, args, { ...opts, env, replaceEnv: true });
    const w = { db, login: tok.login, now: () => now };
    let made: { number: number; url: string };
    try {
      made = await createReporterIssue(w, asUser, { repo: req.repo, title, body: `${body}\n\n${FOOTER}` });
    } catch (e) {
      const err = String((e as Error).message ?? e);
      if (/HTTP 401|bad credentials/i.test(err)) {
        await failReporter(db, req.reporter, tok.version, now);
        return { status: "sign-in", url: `${baseUrl}/discord/github/start?state=${encodeURIComponent(await newReporterState(db, req.reporter, now))}` };
      }
      if (e instanceof IssueError && !e.clear) { filed = true; /* the issue may exist: the slot stays used */ return { status: "refused", reason: "GitHub could not be reached. Try again in a minute." }; }
      return { status: "refused", reason: "GitHub did not accept the issue." };
    }
    filed = true;
    await db.run(db.k.updateTable("discord_filed").set({ number: made.number }).where(kx.rowid("discord_filed"), "=", slot));
    // A label is a courtesy: GitHub refuses it when the repo has no such label or the account may not set labels.
    await labelReporterIssue(w, asUser, { repo: req.repo, number: made.number, label: LABEL[req.kind] });
    return { status: "filed", number: made.number, url: made.url, login: tok.login };
  } finally {
    if (!filed) await db.run(db.k.deleteFrom("discord_filed").where(kx.rowid("discord_filed"), "=", slot));
  }
}

/** Removes the person's GitHub sign-in (on /autocraft unlink and `discord forget --user`) and queues its token's revoke. Returns whether one existed. */
export async function forgetReporter(db: Db, reporter: string): Promise<boolean> {
  return await forgetReporterRow(db, reporter);
}
