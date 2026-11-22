// Decisions for people. The pipeline asks them with the CLI; only a signed-in
// person on the dashboard answers them. The one command that sets answers is
// the operator's bulk Backlog (`decisions backlog`), which only holds items.
import { settledBy, settledByExpr } from "./settlers.ts";
import { lockLiveExpr } from "./answers/hold.ts";
import { readFileSync } from "node:fs";
import { userInfo } from "node:os";
import { audit } from "./dash/audit.ts";
import { inPilot } from "./pilot.ts";
import { filterSeq, someSeq } from "./async-seq.ts";
import type { Db } from "./db/index.ts";
import * as kx from "./db/kx.ts";
import type { AliasableExpression, Expression, Insertable, SqlBool } from "kysely";
import type { DB } from "./db/types.ts";
import { openDb } from "./db/open.ts";

export const KINDS = ["scope", "duplicate", "close", "merge", "hold", "split", "other", "backlog"] as const;
export type Kind = (typeof KINDS)[number];

/** The kind of the decision a person's Backlog press makes for an item with no open question: answered Backlog when written, never asked. */
export const BACKLOG_KIND = "backlog";
export const BACKLOG_QUESTION = "Work on this item?";
export const backlogDecisionId = (repo: string, number: number): string => `${repo}#${number}:backlog`;

export type Option = { key: string; label: string; hint?: string };
export type Ref = { repo: string; number: number };
export type Exception = Ref & { answer: string };

export type DecisionRow = {
  id: string; kind: Kind; repo: string | null; number: number | null; items: string | null;
  question: string; context: string | null; facts: string | null; options: string;
  recommended: string | null; recommended_reason: string | null; recommended_by: string | null; public_text: string | null; asked_at: string; asked_by: string | null;
  docket: string | null; answer: string | null; answer_note: string | null; exceptions: string | null;
  answered_by: string | null; answered_at: string | null; applied_at: string | null;
  prepared_at?: string | null; prepared_head?: string | null; prepared_discussion?: string | null; prepare_requested_at?: string | null; prepare_failed_head?: string | null; recommend_failures?: number | null; recommend_failed_at?: string | null;
  summary: string | null; summary_by: string | null; summary_at: string | null; adopted_opinion: string | null; snoozed_by: string | null; snoozed_at: string | null;
  exec_state?: string | null; exec_reason?: string | null; dispute_by?: string | null; dispute_reason?: string | null; dispute_at?: string | null;
};

const j = <T>(s: string | null, d: T): T => {
  if (!s) return d;
  try { return JSON.parse(s) as T; } catch { return d; }
};
/** The answer every decision offers last: nothing is written to GitHub and the items stay held (see src/dupes/held.ts). */
export const BACKLOG: Option = { key: "backlog", label: "Backlog", hint: "Hold this undecided: nothing is written and no later step takes the item up." };
/** The answer of a decision the conversation settled without choosing an option: no step acts on it and nothing is written to GitHub. The note holds Opus's one-line summary. */
export const HANDLED: Option = { key: "handled", label: "Handled in the conversation" };
/** The answer the pipeline gives a question that can no longer be answered (see src/merge/withdraw.ts). */
export const WITHDRAWN_OPTION: Option = { key: "withdrawn", label: "Withdrawn" };
/**
 * A decision is snoozed while `snoozed_at` is set and the item has had no activity since: a GitHub comment or review, a pushed commit or force-push,
 * a decision comment or opinion, an operator's or maintainer's conversation message. Evaluated at read time, so no writer has to clear it and a missed
 * clear is impossible. `a` names the decisions table in the caller's query.
 */
export function snoozedExpr(eb: kx.AnyEb, a = "decisions"): AliasableExpression<SqlBool> {
  const t = kx.schemaEb(eb), c = (col: string) => eb.ref(`${a}.${col}`), at = c("snoozed_at");
  // The item itself, or for a duplicate-group decision any of its member items.
  const onItem = (x: string) => eb.or([eb.and([eb(eb.ref(`${x}.repo`), "=", c("repo")), eb(eb.ref(`${x}.number`), "=", c("number"))]),
    eb.exists(t.selectFrom("dupe_group_items as gi").select(eb.lit(1).as("one")).where("gi.decision_id", "=", c("id"))
      .where("gi.repo", "=", eb.ref(`${x}.repo`)).where("gi.number", "=", eb.ref(`${x}.number`)))]);
  const one = eb.lit(1).as("one");
  return eb.and([eb(c("answer"), "is", null), eb(at, "is not", null), eb.not(eb.or([
    eb.exists(t.selectFrom("comments as sc").select(one).where(onItem("sc")).where("sc.created_at", ">", at)),
    eb.exists(t.selectFrom("pr_commits as sp").select(one).where(onItem("sp")).where((w) => w(w.fn.coalesce("sp.first_seen_at", "sp.committed_at"), ">", at))),
    eb.exists(t.selectFrom("item_activity as sf").select(one).where(onItem("sf")).where("sf.kind", "=", "force-push").where("sf.at", ">", at)),
    eb.exists(t.selectFrom("decision_comments as sd").select(one).where("sd.decision_id", "=", c("id")).where("sd.at", ">", at)),
    eb.exists(t.selectFrom("decision_opinions as so").select(one).where("so.decision_id", "=", c("id")).where("so.at", ">", at)),
    eb.exists(t.selectFrom("conversation_turns as st").select(one).where(onItem("st")).where("st.kind", "in", ["operator", "maintainer"]).where("st.at", ">", at)),
  ]))]);
}

/** A settled decision makes the drafts still proposed on its item obsolete (the ask card, the other options' drafts) and replaces its own choices. */
export async function replaceProposedCards(db: Db, repo: string | null, number: number | null, decisionId: string): Promise<void> {
  if (repo && number !== null) {
    await db.run(db.k.updateTable("main.conversation_cards").set({ state: "replaced" }).where("repo", "=", repo).where("number", "=", number).where("state", "=", "proposed"));
    // Only the choices of the decision that settled: another open decision of the item keeps its own.
    await db.run(db.k.updateTable("main.decision_choices").set({ state: "replaced" }).where("decision_id", "=", decisionId).where("state", "=", "current"));
  }
}

export const isBacklog = (answer: string | null): boolean => answer === BACKLOG.key;

/** The options the pipeline asked, then Backlog. */
export const optionsOf = (d: DecisionRow): Option[] => {
  const asked = j<Option[]>(d.options, []);
  return asked.some((o) => o.key === BACKLOG.key) ? asked : [...asked, BACKLOG];
};
export const factsOf = (d: DecisionRow): string[] => j<string[]>(d.facts, []);
export const itemsOf = (d: DecisionRow): Ref[] => (d.number === null ? j<Ref[]>(d.items, []) : [{ repo: d.repo!, number: d.number }]);
export const exceptionsOf = (d: DecisionRow): Exception[] => j<Exception[]>(d.exceptions, []);
export const isBatch = (d: DecisionRow): boolean => d.number === null;
export const reposOf = (d: DecisionRow): string[] => [...new Set(itemsOf(d).map((r) => r.repo).concat(d.repo ? [d.repo] : []))].sort();

const decisionRows = (db: Db) => db.k.selectFrom("decisions").select(kx.cols("decisions")).$narrowType<{ kind: Kind }>();

export async function getDecision(db: Db, id: string): Promise<DecisionRow | undefined> {
  return await db.get(decisionRows(db).where("id", "=", id));
}

const ASK_FIELDS = ["kind", "repo", "number", "items", "question", "context", "facts", "options", "recommended", "recommended_reason", "public_text", "asked_at", "asked_by", "docket", "summary", "summary_by", "summary_at"] as const;

// A re-ask without a summary keeps the one the decision has.
const SUMMARY_FIELDS: readonly string[] = ["summary", "summary_by", "summary_at"];

const str = (v: unknown): string | null => (typeof v === "string" && v ? v : null);

// Validates one question from the pipeline and returns the column values.
export function validateAsk(q: Record<string, unknown>): Record<string, string | number | null> & Insertable<DB["main.decisions"]> {
  const err = (m: string) => new Error(`decision ${String(q.id ?? "?")}: ${m}`);
  if (typeof q.id !== "string" || !q.id) throw err("id is required");
  if (!KINDS.includes(q.kind as Kind) || q.kind === BACKLOG_KIND) throw err(`kind must be one of ${KINDS.filter((k) => k !== BACKLOG_KIND).join(", ")}`);
  if (typeof q.question !== "string" || !q.question.trim()) throw err("question is required");
  const options = q.options as Option[];
  if (!Array.isArray(options) || options.length < 2 || options.length > 5 ||
    !options.every((o) => typeof o?.key === "string" && typeof o?.label === "string" && (o.hint === undefined || typeof o.hint === "string")))
    throw err("options must be 2–5 {key, label, hint?}");
  if (new Set(options.map((o) => o.key)).size !== options.length) throw err("option keys must be unique");
  if (q.recommended != null && !options.some((o) => o.key === q.recommended)) throw err("recommended must be an option key");
  const facts = (q.facts ?? []) as unknown[];
  if (!Array.isArray(facts) || facts.length > 5 || !facts.every((f) => typeof f === "string")) throw err("facts must be up to five strings");
  const batch = q.number == null;
  let items: Ref[] | null = null;
  if (batch) {
    items = q.items as Ref[];
    if (!Array.isArray(items) || !items.length || !items.every((r) => typeof r?.repo === "string" && Number.isInteger(r?.number)))
      throw err("a batch needs items: [{repo, number}]");
  } else {
    if (typeof q.repo !== "string" || !Number.isInteger(q.number)) throw err("repo and number are required for one item");
  }
  const repo = typeof q.repo === "string" ? q.repo : batch && new Set(items!.map((r) => r.repo)).size === 1 ? items![0].repo : null;
  return {
    id: q.id, kind: q.kind as string, repo, number: batch ? null : (q.number as number),
    items: items ? JSON.stringify(items.map((r) => ({ repo: r.repo, number: r.number }))) : null,
    question: q.question.trim(), context: typeof q.context === "string" ? q.context : null,
    facts: JSON.stringify(facts), options: JSON.stringify(options.map((o) => ({ key: o.key, label: o.label, ...(o.hint ? { hint: o.hint } : {}) }))),
    recommended: (q.recommended as string) ?? null,
    // A reason without a recommendation would be shown with nothing to explain.
    recommended_reason: q.recommended ? str(q.recommended_reason) : null,
    public_text: typeof q.public_text === "string" ? q.public_text : null,
    asked_at: typeof q.asked_at === "string" ? q.asked_at : new Date().toISOString(),
    asked_by: typeof q.asked_by === "string" ? q.asked_by : null, docket: typeof q.docket === "string" ? q.docket : null,
    summary: str(q.summary), summary_by: str(q.summary_by), summary_at: str(q.summary_at),
  };
}

// Inserts or updates the question by id; answer columns are never touched. A re-ask replaces the recommendation along with its reason and clears its author, except that a question and options the prepare step already covered keep its pick. A changed question or options clear the snooze and the prepare. A summary the new question lacks is kept.
export async function ask(db: Db, questions: Record<string, unknown>[], inTransaction = false): Promise<{ inserted: number; updated: number }> {
  const rows = questions.map(validateAsk);
  let inserted = 0, updated = 0;
  const write = async () => {
    for (const r of rows) {
      if (await db.get(db.k.selectFrom("decisions").select("id").where("id", "=", r.id))) updated++; else inserted++;
      await db.run(db.k.insertInto("main.decisions").values(r).onConflict((oc) => oc.column("id").doUpdateSet((eb) => {
        const was = (c: string) => kx.exprOf(eb, `main.decisions.${c}`);
        const changed = eb.or([eb("excluded.question", "is distinct from", was("question")), eb("excluded.options", "is distinct from", was("options"))]);
        const keepsPick = eb.and([eb(was("prepared_at"), "is not", null), eb.not(changed)]);
        const set: Record<string, Expression<unknown>> = {};
        for (const c of ASK_FIELDS) {
          set[c] = SUMMARY_FIELDS.includes(c) ? eb.fn.coalesce(`excluded.${c}`, `main.decisions.${c}`)
            : c === "recommended" || c === "recommended_reason" ? eb.case().when(keepsPick).then(was(c)).else(eb.ref(`excluded.${c}`)).end()
            : eb.ref(`excluded.${c}`);
        }
        const clearOnChange = (c: "prepared_at" | "recommend_failures" | "snoozed_by" | "snoozed_at") => eb.case().when(changed).then(eb.lit(null)).else(was(c)).end();
        return {
          ...set,
          recommended_by: eb.case().when(keepsPick).then(was("recommended_by")).else(eb.lit(null)).end(),
          prepared_at: clearOnChange("prepared_at"), recommend_failures: clearOnChange("recommend_failures"),
          snoozed_by: clearOnChange("snoozed_by"), snoozed_at: clearOnChange("snoozed_at"),
        };
      })));
    }
  };
  if (inTransaction) await write(); else await db.write(write);
  return { inserted, updated };
}

// Answers the pipeline may act on: answered by someone who settles now (an operator, or a maintainer while
// settings.maintainer_writes is on, see src/settlers.ts), not yet applied, and not Backlog (which only holds). Option keys only, never free text.
// Opinions and comments are display-only and never read here.
export type Actionable = { id: string; kind: Kind; repo: string | null; number: number | null; items: Ref[]; answer: string; exceptions: Exception[] };

export async function operators(db: Db): Promise<Set<string>> {
  return new Set((await db.all(db.k.selectFrom("access").select("login").where("role", "=", "operator"))).map((r) => r.login));
}

export async function actionable(db: Db): Promise<Actionable[]> {
  const rows = await db.all(decisionRows(db).where("answer", "is not", null).where("answer", "<>", BACKLOG.key).where("applied_at", "is", null)
    .where((eb) => settledByExpr(db, eb, "decisions")).orderBy("answered_at", (o) => o.asc().nullsFirst()));
  return rows.map((d) => ({
    id: d.id, kind: d.kind, repo: d.repo, number: d.number, items: itemsOf(d), answer: d.answer!,
    exceptions: exceptionsOf(d).map((e) => ({ repo: e.repo, number: e.number, answer: e.answer })),
  }));
}

/** `only`: a decision is in scope only when this accepts every repo it covers (its repos and its duplicate-group items'). */
export type BacklogScope = { outsidePilot?: boolean; repo?: string; only?: (repos: string[]) => Promise<boolean> };
export type BacklogResult = { applied: boolean; decisions: { id: string; kind: Kind; repo: string | null; number: number | null }[] };

/** Open decisions in scope: of the repo when given, and, with `outsidePilot`, none of whose items is in the pilot (with no pilot set, none is outside). */
export async function backlogCandidates(db: Db, scope: BacklogScope): Promise<DecisionRow[]> {
  if (!scope.outsidePilot && !scope.repo) throw new Error("name a scope: outsidePilot or a repo");
  const rows = await db.all(decisionRows(db).where("answer", "is", null).orderBy("asked_at").orderBy("id"));
  return await filterSeq(rows, async (d) => {
    const refs = [...itemsOf(d), ...(await db.all(db.k.selectFrom("dupe_group_items").select(["repo", "number"]).where("decision_id", "=", d.id)))];
    if (scope.repo && !reposOf(d).includes(scope.repo) && !refs.some((x) => x.repo === scope.repo)) return false;
    if (scope.only && !await scope.only([...new Set([...reposOf(d), ...refs.map((x) => x.repo)])])) return false;
    // Outside the scope: no item in a repo whose pipeline is on and, with an item pilot, in that pilot.
    // An item held for its outside author (src/outside.ts) is in scope: the hold is temporary.
    return !scope.outsidePilot || !await someSeq(refs, async (x) => await inPilot(db, x.repo, x.number) || !!await db.get(db.k.selectFrom("main.outside_holds").select("repo").where("repo", "=", x.repo).where("number", "=", x.number)));
  });
}

/** Answers the open decisions in scope with Backlog and audits each; a dry run only lists them. The answer is written for `by`. */
export async function backlogOpen(db: Db, scope: BacklogScope, by: string, apply: boolean, at: string = new Date().toISOString()): Promise<BacklogResult> {
  const rows = await backlogCandidates(db, scope);
  const decisions = rows.map((d) => ({ id: d.id, kind: d.kind, repo: d.repo, number: d.number }));
  if (!apply) return { applied: false, decisions };
  await db.write(async () => {
    for (const d of rows) {
      const set = db.k.updateTable("main.decisions")
        .set({ answer: BACKLOG.key, answer_note: null, exceptions: isBatch(d) ? "[]" : null, answered_by: by, answered_at: at, adopted_opinion: null, snoozed_by: null, snoozed_at: null })
        .where("id", "=", d.id).where("answer", "is", null).where((eb) => eb.not(lockLiveExpr(db, eb, "decisions")));
      if ((await db.run(set)).changes === 1) await audit(db, by, "answer", d.id, { answer: BACKLOG.key, bulk: true });
    }
    await audit(db, by, "backlog-bulk", null, { outsidePilot: scope.outsidePilot, repo: scope.repo, count: rows.length });
  });
  return { applied: true, decisions };
}

/**
 * Returns a function that removes the stages row Decide wrote when it asked a scope decision, so the item counts as never
 * asked (a scope question is asked only for an item whose stage is not past classified). item_stage is kept by an insert
 * trigger only, so it is rebuilt from the remaining stages rows.
 */
export function clearAskedStage(db: Db): (repo: string, number: number, decisionId: string) => Promise<void> {
  return async (repo, number, decisionId) => {
    await db.run(db.k.deleteFrom("main.stages").where("repo", "=", repo).where("number", "=", number).where("stage", "=", "needs-decision").where("note", "=", `scope decision ${decisionId}`));
    const l = await db.get(db.k.selectFrom("stages").select(["stage", "at"]).where("repo", "=", repo).where("number", "=", number).orderBy("at", "desc").orderBy(kx.rowid("stages"), "desc").limit(1));
    if (l) {
      const reviewed = await db.get(db.k.selectFrom("stages").select("stage").where("repo", "=", repo).where("number", "=", number).where("stage", "=", "reviewed").limit(1));
      await db.run(db.k.updateTable("item_stage").set({ stage: l.stage, at: l.at, reviewed: reviewed ? 1 : 0 }).where("repo", "=", repo).where("number", "=", number));
    } else await db.run(db.k.deleteFrom("item_stage").where("repo", "=", repo).where("number", "=", number));
  };
}

/**
 * Withdraws open scope decisions in scope: deletes each with its opinions, comments, claims and the stages row Decide
 * wrote when it asked, so the item counts as never asked and Decide asks it again later. Other kinds are left alone
 * (their answers drive side tables). A dry run only lists them. Writes nothing to GitHub.
 */
export async function withdrawOpen(db: Db, scope: BacklogScope, by: string, apply: boolean): Promise<BacklogResult> {
  const rows = (await backlogCandidates(db, scope)).filter((d) => d.kind === "scope" && !isBatch(d) && d.repo !== null && d.number !== null);
  const decisions = rows.map((d) => ({ id: d.id, kind: d.kind, repo: d.repo, number: d.number }));
  if (!apply) return { applied: false, decisions };
  await db.write(async () => {
    const clear = clearAskedStage(db);
    for (const d of rows) {
      if ((await db.run(db.k.deleteFrom("main.decisions").where("id", "=", d.id).where("answer", "is", null))).changes !== 1) continue;
      await db.run(db.k.deleteFrom("decision_opinions").where("decision_id", "=", d.id));
      await db.run(db.k.deleteFrom("decision_comments").where("decision_id", "=", d.id));
      await db.run(db.k.deleteFrom("decision_claims").where("decision_id", "=", d.id));
      await clear(d.repo!, d.number!, d.id);
      await audit(db, by, "withdraw", d.id, { repo: d.repo, number: d.number, question: d.question });
    }
  });
  return { applied: true, decisions };
}

const testKey = (repo: string): string => `test_repo:${repo}`;

/** Test repos accept answers from the CLI (`decisions answer`), so a session can run scenarios on them without a person pressing buttons. */
export async function isTestRepo(db: Db, repo: string): Promise<boolean> {
  return await db.get(db.k.selectFrom("main.settings").select("key").where("key", "=", testKey(repo))) !== undefined;
}

export async function testRepos(db: Db): Promise<string[]> {
  return (await db.all(db.k.selectFrom("main.settings").select("key").where((eb) => kx.like(db, "key", eb.val("test_repo:%"))).orderBy("key"))).map((r) => r.key.slice("test_repo:".length));
}

/** Returns false when nothing changed. */
export async function setTestRepo(db: Db, by: string, repo: string, on: boolean): Promise<boolean> {
  if (!await db.get(db.k.selectFrom("main.tracked_repos").select("name").where("name", "=", repo))) throw new Error(`${repo} is not tracked`);
  if (await isTestRepo(db, repo) === on) return false;
  if (on) await db.run(db.k.insertInto("main.settings").values({ key: testKey(repo), value: "1" }));
  else await db.run(db.k.deleteFrom("main.settings").where("key", "=", testKey(repo)));
  await audit(db, by, "test-repo", repo, { on });
  return true;
}

/**
 * Answers an open decision as `login` the way the dashboard's Answer button does. Refused unless every repo the decision
 * concerns is a test repo and `login` is an operator. The audit row says the answer came from a session.
 */
export async function answerAsSession(db: Db, id: string, option: string, login: string, note?: string): Promise<DecisionRow> {
  const d = await sessionDecision(db, id, login);
  if (d.answer) throw new Error(`decision ${id} is already answered by ${d.answered_by}`);
  const { applyAnswer } = await import("./dash/api/decisions.ts");
  // A group answer approves the comments stored for its option; the session approves the ones stored now (answerCmd prints them).
  const { textsFor, textsHash } = await import("./dupes/texts.ts");
  const texts = d.id.includes(":dupegroup:") ? await textsFor(db, id, option) : [];
  await applyAnswer(db, d, login, { answer: option, note, ...(texts.length && { textsHash: textsHash(texts) }) }, "cli", "session");
  await db.run(db.k.deleteFrom("decision_claims").where("decision_id", "=", id));
  return (await getDecision(db, id))!;
}

/** A decision a session may act on as `login`: every repo it concerns is a test repo and `login` is an operator. */
async function sessionDecision(db: Db, id: string, login: string): Promise<DecisionRow> {
  const d = await getDecision(db, id);
  if (!d) throw new Error(`no decision ${id}`);
  const repos = [...new Set([...reposOf(d), ...itemsOf(d).map((r) => r.repo)])];
  const outside = await filterSeq(repos, async (r) => !await isTestRepo(db, r));
  if (!repos.length || outside.length) throw new Error(`refused: ${outside.join(", ") || "no repo"} is not marked as a test repo (decisions test-repo REPO on)`);
  if (!(await operators(db)).has(login)) throw new Error(`refused: ${login} is not an operator`);
  return d;
}

/** Tries the GitHub write of a waiting or failed answer again, like the dashboard's Retry; test repos only. */
export async function retryAsSession(db: Db, id: string, login: string): Promise<void> {
  const d = await sessionDecision(db, id, login);
  if (!d.answer || d.applied_at) throw new Error(`decision ${id} has nothing waiting to run`);
  const { retryDecision } = await import("./github/user-lane.ts");
  if (!await retryDecision(db, id)) throw new Error(`decision ${id} is not waiting or failed`);
  await audit(db, login, "retry", id, { answer: d.answer, source: "cli", via: "session" });
}

/** The repos a card writes to: its item's, and a resolution's transfer and new-issue targets. */
function cardRepos(c: { repo: string; params: string }): string[] {
  const p = JSON.parse(c.params) as { kind: string; steps?: { kind: string; to?: string; repo?: string }[] };
  return [c.repo, ...(p.kind === "resolution" ? (p.steps ?? []).flatMap((x) => (x.kind === "transfer" && x.to ? [x.to] : x.kind === "open_issue" && x.repo ? [x.repo] : [])) : [])];
}

/** Runs a proposed card as `login` the way the dashboard's button does; test repos only, operator logins only. */
export async function cardAsSession(db: Db, id: number, login: string, opts: { note?: string; skip?: number[] } = {}): Promise<{ state: string; result: string | null; error: string | null }> {
  const c = await db.get(db.k.selectFrom("main.conversation_cards").select(["id", "repo", "number", "params", "state"]).where("id", "=", id));
  if (!c) throw new Error(`no card ${id}`);
  const outside = await filterSeq([...new Set(cardRepos(c))], async (r) => !await isTestRepo(db, r));
  if (outside.length) throw new Error(`refused: ${outside.join(", ")} is not marked as a test repo (decisions test-repo REPO on)`);
  if (!(await operators(db)).has(login)) throw new Error(`refused: ${login} is not an operator`);
  const { doCard } = await import("./conversations/act.ts");
  const { card, ran } = await doCard(db, id, login, opts);
  if (!ran) throw new Error(`card ${id} is ${card.state}, not proposed`);
  await audit(db, login, "card", `${c.repo}#${c.number}`, { card: id, source: "cli", via: "session" });
  const after = await db.get(db.k.selectFrom("main.conversation_cards").select(["state", "result", "error"]).where("id", "=", id));
  return after!;
}

async function cardsCmd(args: string[]): Promise<void> {
  const m = /^(.+)#(\d+)$/.exec(args[0] ?? "");
  if (!m) throw new Error("usage: decisions cards REPO#N");
  const db = await openDb();
  const rows = await db.all(db.k.selectFrom("main.conversation_cards").select(["id", "kind", "state", "description"]).where("repo", "=", m[1]!).where("number", "=", Number(m[2])).orderBy("id"));
  for (const r of rows) console.log(`${r.id}\t${r.kind}\t${r.state}\t${r.description ?? ""}`);
}

async function cardCmd(args: string[]): Promise<void> {
  const id = Number(args[0]);
  const ai = args.indexOf("--as");
  const ni = args.indexOf("--note");
  const si = args.indexOf("--skip");
  const login = ai >= 0 ? args[ai + 1] : undefined;
  if (!Number.isInteger(id) || !login || (ni >= 0 && args[ni + 1] === undefined) || (si >= 0 && !/^\d+(,\d+)*$/.test(args[si + 1] ?? "")))
    throw new Error("usage: decisions card ID --as LOGIN [--skip N,N] [--note TEXT]");
  const r = await cardAsSession(await openDb(), id, login, { note: ni >= 0 ? args[ni + 1] : undefined, skip: si >= 0 ? args[si + 1]!.split(",").map(Number) : undefined });
  console.log(`card ${id}\t${r.state} as ${login} (source cli, via session)${r.result ? `: ${r.result}` : ""}${r.error ? `: ${r.error}` : ""}`);
}

async function retryCmd(args: string[]): Promise<void> {
  const [id] = args;
  const ai = args.indexOf("--as");
  const login = ai >= 0 ? args[ai + 1] : undefined;
  if (!id || id.startsWith("--") || !login) throw new Error("usage: decisions retry ID --as LOGIN");
  await retryAsSession(await openDb(), id, login);
  console.log(`${id}\tqueued again as ${login} (source cli, via session)`);
}

async function answerCmd(args: string[]): Promise<void> {
  const [id, option] = args;
  const ai = args.indexOf("--as");
  const ni = args.indexOf("--note");
  const login = ai >= 0 ? args[ai + 1] : undefined;
  if (!id || !option || id.startsWith("--") || option.startsWith("--") || !login || (ni >= 0 && args[ni + 1] === undefined))
    throw new Error("usage: decisions answer ID OPTION --as LOGIN [--note TEXT]");
  const d = await answerAsSession(await openDb(), id, option, login, ni >= 0 ? args[ni + 1] : undefined);
  console.log(`${d.id}\tanswered ${d.answer} as ${d.answered_by} (source cli, via session)`);
  if (d.id.includes(":dupegroup:")) {
    const { textsFor } = await import("./dupes/texts.ts");
    for (const t of await textsFor(await openDb(), d.id, d.answer!)) console.log(`approved ${t.step} on #${t.number}: ${t.text}`);
  }
  console.log(`state: ${d.applied_at ? "applied" : "waiting for the pipeline to act"}`);
}

async function testRepoCmd(args: string[]): Promise<void> {
  const db = await openDb();
  const [repo, mode] = args;
  if (!repo) {
    for (const r of await testRepos(db)) console.log(r);
    return;
  }
  if (mode !== "on" && mode !== "off") throw new Error("usage: decisions test-repo [REPO on|off]");
  console.log(await setTestRepo(db, "cli", repo, mode === "on") ? `${repo} test repo ${mode}` : `${repo} test repo already ${mode}`);
}

async function withdrawCmd(args: string[]): Promise<void> {
  const ri = args.indexOf("--repo");
  const scope: BacklogScope = { outsidePilot: args.includes("--outside-pilot"), repo: ri >= 0 ? args[ri + 1] : undefined };
  if (ri >= 0 && !scope.repo) throw new Error("--repo needs a repo name");
  if (!scope.outsidePilot && !scope.repo) throw new Error("usage: decisions withdraw (--outside-pilot | --repo R) [--apply]");
  const apply = args.includes("--apply");
  const res = await withdrawOpen(await openDb(), scope, userInfo().username, apply);
  for (const d of res.decisions) console.log(`${d.id}\t${d.repo}#${d.number}`);
  console.log(`${res.decisions.length} open scope decision${res.decisions.length === 1 ? "" : "s"} ${apply ? "withdrawn" : "would be withdrawn (dry run; --apply writes)"}`);
}

async function backlogCmd(args: string[]): Promise<void> {
  const ri = args.indexOf("--repo");
  const scope: BacklogScope = { outsidePilot: args.includes("--outside-pilot"), repo: ri >= 0 ? args[ri + 1] : undefined };
  if (ri >= 0 && !scope.repo) throw new Error("--repo needs a repo name");
  if (!scope.outsidePilot && !scope.repo) throw new Error("usage: decisions backlog (--outside-pilot | --repo R) [--apply]");
  const apply = args.includes("--apply");
  const res = await backlogOpen(await openDb(), scope, userInfo().username, apply);
  for (const d of res.decisions) console.log(`${d.id}\t${d.kind}\t${d.repo ?? ""}${d.number === null ? "" : `#${d.number}`}`);
  console.log(`${res.decisions.length} open decision${res.decisions.length === 1 ? "" : "s"} ${apply ? "answered Backlog" : "would be answered Backlog (dry run; --apply writes)"}`);
}

async function list(args: string[]): Promise<void> {
  if (args.includes("--actionable")) {
    for (const a of await actionable(await openDb())) console.log(JSON.stringify(a));
    return;
  }
  const which = args.includes("--all") ? "all" : args.includes("--answered") ? "answered" : "open";
  const ri = args.indexOf("--repo");
  const repo = ri >= 0 ? args[ri + 1] : undefined;
  const db = await openDb();
  const rows = await db.all(decisionRows(db)
    .$if(which === "open", (q) => q.where("answer", "is", null)).$if(which === "answered", (q) => q.where("answer", "is not", null))
    .$if(!!repo, (q) => q.where((eb) => eb.or([eb("repo", "=", repo!), kx.like(db, "items", eb.val(`%"repo":"${repo}"%`))])))
    .orderBy("asked_at"));
  for (const d of rows) {
    const target = isBatch(d) ? `batch of ${itemsOf(d).length}` : `${d.repo}#${d.number}`;
    const by = isBacklog(d.answer) || await settledBy(db, d) ? "" : " (not actionable: not answered by someone who settles)";
    const state = d.answer ? `answered ${d.answer}${d.applied_at ? ", applied" : ""}${by}` : "open";
    const keys = optionsOf(d).map((o) => o.key).join(",");
    console.log(`${d.id}\t${d.kind}\t${target}\t${state}\t${d.question}\toptions: ${keys}`);
  }
}

async function show(args: string[]): Promise<void> {
  const id = args[0];
  if (!id) throw new Error("usage: decisions show <id>");
  const db = await openDb();
  const d = await getDecision(db, id);
  if (!d) throw new Error(`no decision ${id}`);
  // Agents run this command: no free text from people (answer_note), and
  // adopted_opinion points at an opinion, which agents never see.
  const { adopted_opinion: _adopted, answer_note: _note, snoozed_by: _sb, snoozed_at: _sa, ...rest } = d;
  const actionable = d.answer !== null && !isBacklog(d.answer) && await settledBy(db, d);
  const out = { ...rest, actionable, items: j(d.items, null), facts: factsOf(d), options: optionsOf(d), exceptions: j(d.exceptions, null) };
  console.log(JSON.stringify(out, null, 2));
}

async function askCmd(args: string[]): Promise<void> {
  const file = args[0];
  if (!file) throw new Error("usage: decisions ask <file.json>  (one object or an array)");
  const data = JSON.parse(readFileSync(file, "utf8")) as unknown;
  const qs = (Array.isArray(data) ? data : [data]) as Record<string, unknown>[];
  for (const q of qs) {
    if (q && typeof q === "object" && ["answer", "answer_note", "answered_by", "answered_at", "exceptions", "applied_at", "adopted_opinion"].some((k) => k in q))
      throw new Error(`decision ${String(q.id)}: the CLI cannot set answers`);
  }
  const r = await ask(await openDb(), qs);
  console.log(`${r.inserted} asked, ${r.updated} updated`);
}

async function convertChoicesCmd(args: string[]): Promise<void> {
  const { convertOpenDecisions } = await import("./decide/choices-convert.ts");
  const dryRun = args.includes("--dry-run");
  const r = await convertOpenDecisions(await openDb(), { dryRun });
  console.log(`${dryRun ? "would convert" : "converted"} ${r.converted} open decisions to choices; skipped ${r.skipped}; ${r.stale} stale cards, ${r.unconverted} cards matching no choice`);
  for (const note of r.notes) console.log(note);
}

const SUB: Record<string, (args: string[]) => Promise<void | Promise<void>>> = { list, show, ask: askCmd, backlog: backlogCmd, withdraw: withdrawCmd, answer: answerCmd, retry: retryCmd, cards: cardsCmd, card: cardCmd, "test-repo": testRepoCmd, "convert-choices": convertChoicesCmd };

export const commands = {
  decisions: {
    summary: "decisions for people: list [--open|--answered|--all|--actionable] [--repo R] | show ID | ask FILE.json | backlog (--outside-pilot | --repo R) [--apply] | withdraw (--outside-pilot | --repo R) [--apply] | answer ID OPTION --as LOGIN [--note TEXT] | retry ID --as LOGIN | cards REPO#N | card ID --as LOGIN [--skip N,N] [--note TEXT] | test-repo [REPO on|off] | convert-choices [--dry-run] (answers come from the dashboard, or `answer`/`card` on test repos as an operator login; backlog is the operator bulk hold; withdraw deletes open scope decisions so Decide asks them again later)",
    async run(args: string[]): Promise<void | Promise<void>> {
      const [sub, ...rest] = args;
      const fn = sub ? SUB[sub] : undefined;
      if (!fn) {
        console.log("usage: autocraft-server decisions list|show|ask|backlog|withdraw|answer|retry|test-repo ...");
        process.exit(sub ? 1 : 0);
      }
      return await fn(rest);
    },
  },
};
