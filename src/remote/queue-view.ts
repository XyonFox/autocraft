// The model call queue as the dashboard shows it (GET /api/model-queue, src/dash/api/model-queue.ts): every queued call with
// a live caller in claim order, why it waits, and the ready Task Runners. Reads only; picks are in picks.ts.
import type { ItemRef, ModelQueueWhyCode } from "../../web/src/api/types.ts";
import { WRITE_PERMISSIONS } from "../dash/server/provider.ts";
import { ownLaneLeasesOfToken, leasedBy, pickerLogins, pickCounts, queueSnapshot, runnerTakes } from "./calls.ts";
import { gating } from "./claim-class.ts";
import { cooloffOf } from "./cooloff.ts";
import { STATUS_FRESH_MS } from "./helpers.ts";
import { manualExpr } from "./manual-only.ts";
import { userMaxCalls } from "./max-calls.ts";
import { acceptedModels, familyOf, isTier } from "./mode.ts";
import { claimRepos } from "./my-repos.ts";
import { taskOfStep, TASK_CHOICES, userSkips } from "./my-tasks.ts";
import { itemFilter, itemsFit, type ItemFilter } from "./task-items.ts";
import type { Tier } from "./protocol.ts";
import { CALLER_STALE_MS } from "./room.ts";
import type { Db } from "../db/index.ts";
import { inList } from "../db/kx.ts";

/** A fresh, ready Task Runner of a user who is not cooled off. `free`: slots without the fast lane its token does not use, within the user's Max calls at once. */
export type ReadyRunner = { login: string; role: string; manual: boolean; tiers: ReadonlySet<Tier>; free: number; repos: ReadonlySet<string> | null; skip: ReadonlySet<string>; items?: ItemFilter };

export async function readyRunners(db: Db, nowMs: number): Promise<ReadyRunner[]> {
  const rows = await db.all(db.k.selectFrom("main.mcp_tokens as t").innerJoin("main.remote_helpers as h", "h.token_id", "t.id").innerJoin("main.access as a", "a.login", "t.login")
    .select((eb) => ["t.id", "t.login", "a.role", "h.models", eb.fn.coalesce("h.slots", eb.lit(1)).as("slots"), manualExpr(db, eb, "t.login").as("manual"),
      eb.selectFrom("main.remote_calls as c").select((e) => e(e.fn.countAll<number>(), "-", ownLaneLeasesOfToken(db, "t.id")).as("n")).where("c.state", "=", "leased").whereRef("c.token_id", "=", "t.id").as("leased")])
    .where("t.kind", "=", "runner").where("t.expires_at", ">", new Date(nowMs).toISOString()).where("h.readiness", "=", "ready").where("h.reported_at", ">=", nowMs - STATUS_FRESH_MS)
    .orderBy("t.id"));
  const accepted = await acceptedModels(db);
  const userLeft = new Map<string, number>();
  const left = async (login: string): Promise<number> => {
    const key = login.toLowerCase();
    let n = userLeft.get(key);
    if (n === undefined) {
      const max = await userMaxCalls(db, login);
      n = max === null ? Infinity : Math.max(0, max - await leasedBy(db, login));
      userLeft.set(key, n);
    }
    return n;
  };
  const out: ReadyRunner[] = [];
  for (const r of rows) {
    if (await cooloffOf(db, r.login, nowMs)) continue;
    let models: Record<string, unknown> = {};
    try { models = JSON.parse(r.models) as Record<string, unknown>; } catch { /* no models */ }
    const tiers = new Set<Tier>();
    for (const [t, m] of Object.entries(models)) if (isTier(t) && typeof m === "string" && familyOf(m, accepted[t]) !== null) tiers.add(t);
    const slots = Number(r.slots) - (Number(r.slots) >= 2 ? 1 : 0);
    out.push({ login: r.login, role: r.role, manual: Number(r.manual) === 1, tiers, free: Math.min(await left(r.login), Math.max(0, slots - Number(r.leased))), repos: await claimRepos(db, r.login, undefined), skip: await userSkips(db, r.login), items: await itemFilter(db, r.login) });
  }
  return out;
}

/** What whyWaiting reads of a queued call. `picked`: its live pick (login null when the viewer may not see it). */
export type WhyCall = { tier: Tier; promptId: string; step: string; repos: readonly string[]; refs?: readonly ItemRef[]; followUp: boolean; picked: { login: string | null } | null;
  /** The user whose own runners alone take it; `shown`: the viewer may see the login. */
  bound?: { login: string; shown: boolean } | null };

/**
 * Why a queued call waits, most specific first: its pick; the runner that holds its session; the user it is bound to (their
 * runners not ready, busy or next); no ready runner for its tier;
 * every such runner Manual only; no such runner's user may take it (role rules, write permission as last confirmed, My
 * repos); none of them takes its task (my-tasks.ts); none of those takes its items (task-items.ts); every runner that may take
 * it busy; else the calls ahead of it.
 */
export function whyWaiting(c: WhyCall, runners: readonly ReadyRunner[], ahead: number, canWrite: (login: string, repo: string) => boolean): { code: ModelQueueWhyCode; text: string } {
  if (c.picked) return { code: "picked", text: c.picked.login ? `Picked by ${c.picked.login}` : "Picked by a user" };
  if (c.followUp) return { code: "session", text: "Waiting for the Task Runner that holds its session" };
  if (c.bound) {
    const whose = c.bound.shown ? `${c.bound.login}'s Task Runner` : "the asker's own Task Runner";
    const own = runners.filter((r) => r.login.toLowerCase() === c.bound!.login.toLowerCase() && r.tiers.has(c.tier));
    if (!own.length) return { code: "bound-off", text: `Runs only on ${whose}, which is not ready` };
    return { code: "bound", text: own.some((r) => r.free > 0) ? `Next on ${whose}` : `Waiting for ${whose}: it is busy` };
  }
  const forTier = runners.filter((r) => r.tiers.has(c.tier));
  if (!forTier.length) return { code: "no-runner", text: `No Task Runner is ready for the ${c.tier} tier` };
  const auto = forTier.filter((r) => !r.manual);
  if (!auto.length) return { code: "manual-only", text: "Every ready Task Runner is Manual only" };
  // The items are the last question: here only the role, the repos and write permission.
  const may = auto.filter((r) => runnerTakes({ ...r, items: null }, { prompt_id: c.promptId, repos: c.repos }, canWrite));
  if (!may.length) return { code: "no-repo-access", text: c.repos.length ? "No ready Task Runner's user takes calls on its repo" : "No ready Task Runner's user may take it" };
  const task = taskOfStep(c.step);
  const takes = may.filter((r) => !r.skip.has(task));
  if (!takes.length) return { code: "task-off", text: `No ready Task Runner's user takes ${TASK_CHOICES.find((t) => t.id === task)?.title ?? task} tasks` };
  const forItems = takes.filter((r) => itemsFit(r.items ?? null, { repos: c.repos, refs: c.refs }));
  if (!forItems.length) return { code: "items-off", text: "No ready Task Runner's user takes its items: they take fast-tracked items only" };
  if (!forItems.some((r) => r.free > 0)) return { code: "busy", text: "Every Task Runner that may take it is busy" };
  return { code: "behind", text: ahead === 0 ? "Next in line" : `${ahead} call${ahead === 1 ? "" : "s"} ahead` };
}

/** A queued call of the view, before the role's redaction. `pickedBy`: the login of its live pick. */
export type QueuedCall = {
  id: number; step: string; promptId: string; tier: Tier; model: string; refs: ItemRef[]; repos: string[]; authors: ReadonlySet<string>; forLogin: string | null;
  waitedMs: number; followUp: boolean; gating: boolean; pickedBy: string | null; pickUntil: number | null; seenAt: number; lastError: string | null;
  why: { code: ModelQueueWhyCode; text: string };
};

/**
 * The queued calls with a live caller in claim order, why each waits (`showPicker`: name the picker), and the ready runners.
 * Write permission is the one last confirmed, unless `canWriteOf` gives the rule for the runners' logins.
 */
export async function queueView(db: Db, nowMs: number, showPicker: boolean, canWriteOf?: (logins: string[]) => (login: string, repo: string) => boolean): Promise<{ calls: QueuedCall[]; runners: ReadyRunner[] }> {
  const snap = (await queueSnapshot(db, nowMs)).filter((r) => r.seen_at >= nowMs - CALLER_STALE_MS);
  const runners = await readyRunners(db, nowMs);
  if (!snap.length) return { calls: [], runners };
  const extra = new Map((await db.all(db.k.selectFrom("main.remote_calls").select(["id", "asked_model", "queued_ms", "queued_since", "error"]).where(inList(db, "id", snap.map((r) => r.id), "bigint")))).map((r) => [Number(r.id), r]));
  const writable = new Set<string>();
  if (!canWriteOf) for (const p of await db.all(db.k.selectFrom("main.repo_permissions").select(["login", "repo", "permission"]).where(inList(db, "login", runners.map((r) => r.login), "citext"))))
    if (WRITE_PERMISSIONS.has(p.permission)) writable.add(`${p.login.toLowerCase()}\n${p.repo}`);
  const canWrite = canWriteOf?.(runners.map((r) => r.login)) ?? ((l: string, repo: string) => writable.has(`${l.toLowerCase()}\n${repo}`));
  const fresh = await pickerLogins(db, nowMs);
  const ahead = new Map<Tier, number>();
  const calls: QueuedCall[] = [];
  for (const r of snap) {
    const x = extra.get(r.id);
    if (!x) continue;
    const picked = await pickCounts(r, nowMs, async (l) => fresh.has(l)) ? r.pickedBy : null;
    const followUp = r.parent_id !== null;
    const n = ahead.get(r.tier) ?? 0;
    const why = whyWaiting({ tier: r.tier, promptId: r.prompt_id, step: r.step, repos: r.repos, refs: r.refs, followUp, picked: picked === null ? null : { login: showPicker ? picked : null }, bound: r.forLogin === null ? null : { login: r.forLogin, shown: showPicker } }, runners, n, canWrite);
    // A call of repos or a task no ready runner takes is passed over, so it is no call ahead of the next; nor is one bound to a user.
    if (!followUp && picked === null && r.forLogin === null && why.code !== "task-off" && why.code !== "items-off" && why.code !== "no-repo-access") ahead.set(r.tier, n + 1);
    calls.push({
      id: r.id, step: r.step, promptId: r.prompt_id, tier: r.tier, model: x.asked_model, refs: r.refs, repos: r.repos, authors: r.authors, forLogin: r.forLogin,
      waitedMs: Number(x.queued_ms) + (x.queued_since === null ? 0 : Math.max(0, nowMs - Number(x.queued_since))), followUp, gating: gating(r),
      pickedBy: picked, pickUntil: picked === null ? null : r.pickUntil, seenAt: r.seen_at, lastError: x.error, why,
    });
  }
  return { calls, runners };
}
