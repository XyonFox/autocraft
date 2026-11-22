// What our pushes did to the required checks of their PRs, and the circuit breaker built on it (pure rules: core/push-outcome.ts).
// Recording happens where the push is audited (github/write.ts); the outcome is computed where the checks are stored (review/checks.ts).
import { audit } from "./dash/audit.ts";
import { optionalChecks } from "./optional-checks.ts";
import {
  BREAKER_WINDOW, breakerTripped, classifyPush, parseTripKey, PUSH_STAGE, requiredResult, STEP_STAGE, TRIP_STAGES, tripKey, tripOf,
  type Outcome, type Required, type StageTripRow, type TripStage,
} from "./core/push-outcome.ts";
import { filterSeq } from "./async-seq.ts";
import type { Db } from "./db/index.ts";
import * as kx from "./db/kx.ts";
import type { NotNull } from "kysely";

/** Unresolved pushes older than this get the outcome `unknown` (the head moved on, or the checks never ran). */
const EXPIRE_MS = 3 * 24 * 3_600_000;

/** Pushes of a repo whose checks are not final yet: at most this many before the pushing steps wait for their results. */
export const UNSETTLED_PUSHES_MAX = 3;

/** Only pushes of the last 2 hours count: older ones are settled as unknown or will never settle. */
const UNSETTLED_WINDOW_MS = 2 * 3_600_000;

/**
 * True when the repo has UNSETTLED_PUSHES_MAX pushes that still wait for their required checks: the PR is open, its head is
 * still the pushed commit, and the repo has required checks (the commit's checks are pending, or none yet in a repo whose pushes
 * have settled before). Items that find it true are not worked and nothing is counted, so the step ends idle.
 */
export async function pushesUnsettled(db: Db, repo: string, log: (line: string) => void, now = new Date()): Promise<boolean> {
  const rows = await db.all(db.k.selectFrom("push_outcomes as p")
    .innerJoin("items as i", (j) => j.onRef("i.repo", "=", "p.repo").onRef("i.number", "=", "p.number").on("i.state", "=", "OPEN").onRef("i.head_oid", "=", "p.to_oid"))
    .leftJoin("pr_checks as c", (j) => j.onRef("c.repo", "=", "p.repo").onRef("c.number", "=", "p.number").onRef("c.head_oid", "=", "p.to_oid"))
    .select(["p.to_oid as oid", "c.checks as checks"])
    .where("p.repo", "=", repo).where("p.outcome", "is", null).where("p.number", "is not", null)
    .where("p.at", ">=", new Date(now.getTime() - UNSETTLED_WINDOW_MS).toISOString()));
  if (rows.length < UNSETTLED_PUSHES_MAX) return false;
  const optional = await optionalChecks(db, repo);
  const known = async () => !!await db.get(db.k.selectFrom("push_outcomes").select((eb) => eb.lit(1).as("one")).where("repo", "=", repo).where("after_res", "in", ["green", "red"]).limit(1));
  const waiting = (await filterSeq(rows, async (r) => { const res = requiredResult(parse(r.checks), optional); return res === "pending" || (res === "none" && await known()); })).length;
  if (waiting < UNSETTLED_PUSHES_MAX) return false;
  log(`  ${repo}: waiting: ${waiting} earlier pushes have no CI result yet`);
  return true;
}

type CheckRow = { name: string; state: string }[];
const parse = (s: string | null | undefined): CheckRow => { try { const v = JSON.parse(s ?? "[]") as unknown; return Array.isArray(v) ? (v as CheckRow) : []; } catch { return []; } };

/**
 * Records a push that succeeded: the required-check result at the head before it (from the stored checks, when they are for that head).
 * A branch the push created has no earlier head: it takes the CI result of the commit it was built on (`base`), else of the default branch's latest CI
 * (a red or unknown baseline is no baseline for "broke"). Pushes of other kinds (batches, main fixes) are not recorded. A push
 * is recorded once: a resumed job (src/runner/resuming.ts) records the push its earlier run made, unless that run did.
 */
export async function recordPush(db: Db, action: string, o: { repo: string; number: number | null; from: string | null; to: string; base?: string | null }, at = new Date().toISOString()): Promise<void> {
  const stage = PUSH_STAGE[action];
  if (!stage) return;
  let before: Required | "unknown" = "unknown";
  if (!o.from) {
    const m = (o.base ? await db.get(db.k.selectFrom("main_ci").select("state").where("repo", "=", o.repo).where("head_sha", "=", o.base)) : undefined)
      ?? await db.get(db.k.selectFrom("main_ci").select("state").where("repo", "=", o.repo)
        .orderBy("committed_at", (ob) => ob.desc().nullsLast()).orderBy("first_seen_at", "desc").limit(1));
    before = m?.state === "green" || m?.state === "optional" ? "green" : m?.state === "red" ? "red" : "unknown";
  }
  else if (o.number !== null) {
    const row = await db.get(db.k.selectFrom("pr_checks").select(["head_oid", "checks"]).where("repo", "=", o.repo).where("number", "=", o.number));
    if (row && row.head_oid === o.from) before = requiredResult(parse(row.checks), await optionalChecks(db, o.repo));
  }
  await db.run(db.k.insertInto("push_outcomes").columns(["at", "stage", "action", "repo", "number", "from_oid", "to_oid", "before_res"])
    .expression(db.k.selectNoFrom((eb) => [eb.val(at).as("at"), eb.val(stage).as("stage"), eb.val(action).as("action"), eb.val(o.repo).as("repo"), eb.val(o.number).as("number"),
      eb.val(o.from).as("from_oid"), eb.val(o.to).as("to_oid"), eb.val(before).as("before_res")])
      .where((eb) => eb.not(eb.exists(eb.selectFrom("push_outcomes").select(eb.lit(1).as("one")).where("repo", "=", o.repo)
        .where((ie) => o.number === null ? ie("number", "is", null) : ie("number", "=", o.number)).where("to_oid", "=", o.to).where("action", "=", action))))));
}

const isTripStage = (s: string): s is TripStage => (TRIP_STAGES as readonly string[]).includes(s);

/**
 * Called with freshly stored checks: settles the pushes whose resulting commit is the PR's head and whose checks are final,
 * expires old unsettled ones, and lets the breaker look at every stage and repo that got a `broke`.
 */
export async function resolvePushes(db: Db, rows: { repo: string; number: number; head_oid: string; checks: CheckRow }[], at = new Date().toISOString()): Promise<void> {
  // The unsettled pushes at the rows' heads in one read per batch, so a checks run of a large repo makes no read per PR.
  const pending: { id: number; stage: string; before_res: string | null; repo: string; number: number | null; to_oid: string }[] = [];
  for (const heads of kx.batches([...new Map(rows.map((r) => [`${r.repo}\n${r.head_oid}`, [r.repo, r.head_oid] as const])).values()]))
    pending.push(...await db.all(db.k.selectFrom("push_outcomes").select(["id", "stage", "before_res", "repo", "number", "to_oid"])
      .where("outcome", "is", null).where((eb) => kx.tupleIn(db, [eb.ref("repo"), eb.ref("to_oid")], heads, ["text", "text"])).orderBy("id")));
  const optional = new Map<string, string[]>();
  const optionalOf = async (repo: string): Promise<string[]> => {
    if (!optional.has(repo)) optional.set(repo, await optionalChecks(db, repo));
    return optional.get(repo)!;
  };
  const settled: { id: number; number: number; after_res: string; outcome: Outcome }[] = [];
  const done = new Set<number>();
  const broke = new Map<string, { stage: TripStage; repo: string }>();
  for (const r of rows) {
    // A push recorded without a number settles with the first PR at its head, as it then carries that PR's number.
    const open = pending.filter((p) => !done.has(p.id) && p.repo === r.repo && p.to_oid === r.head_oid && (p.number === r.number || p.number === null));
    if (!open.length) continue;
    const after = requiredResult(r.checks, await optionalOf(r.repo));
    for (const p of open) {
      const outcome = classifyPush(p.before_res as Required | "unknown", after);
      if (outcome === null) continue;
      done.add(p.id);
      settled.push({ id: p.id, number: r.number, after_res: after, outcome });
      // A push of a retired stage (fixissue) settles but trips nothing.
      if (outcome === "broke" && isTripStage(p.stage)) broke.set(`${p.stage}\n${p.repo}`, { stage: p.stage, repo: p.repo });
    }
  }
  const old = new Date(Date.parse(at) - EXPIRE_MS).toISOString();
  await db.write(async () => {
    for (const p of settled) await db.run(db.k.updateTable("push_outcomes").set({ number: p.number, after_res: p.after_res, outcome: p.outcome, resolved_at: at }).where("id", "=", p.id));
    // A push whose PR is closed or merged, or whose head moved past the pushed commit (the mirror may still show the head before the push), will not get a result of its own.
    await db.run(db.k.updateTable("push_outcomes").set({ outcome: "unknown", resolved_at: at }).where("outcome", "is", null).where("number", "is not", null)
      .where((eb) => eb.exists(eb.selectFrom("items as i").select(eb.lit(1).as("one"))
        .whereRef("i.repo", "=", "push_outcomes.repo").whereRef("i.number", "=", "push_outcomes.number")
        .where((ie) => ie.or([ie("i.state", "<>", "OPEN"), ie.and([
          ie("i.head_oid", "is not", null),
          ie("i.head_oid", "<>", ie.ref("push_outcomes.to_oid")),
          ie("i.head_oid", "is distinct from", ie.ref("push_outcomes.from_oid")),
        ])])))));
    await db.run(db.k.updateTable("push_outcomes").set({ outcome: "unknown", resolved_at: at }).where("outcome", "is", null).where("at", "<", old));
  });
  for (const b of broke.values()) await evaluatePushBreaker(db, b.stage, b.repo, at);
}

/** The head the PR is at when our push to it broke a required check and nothing was pushed since, or null. */
export async function brokeHeads(db: Db, repo?: string): Promise<Map<string, string>> {
  const rows = await db.all(db.k.selectFrom("push_outcomes").select(["repo", "number", "to_oid"]).$narrowType<{ number: NotNull }>()
    .where("outcome", "=", "broke").where("number", "is not", null)
    .$if(!!repo, (q) => q.where("repo", "=", repo!)).orderBy("id"));
  return new Map(rows.map((r) => [`${r.repo}#${r.number}`, r.to_oid]));
}

// ---- circuit breaker --------------------------------------------------------------------------------------
// Two tiers per push stage (core/push-outcome.ts tripKey): `<stage>:<repo>`, which a repo's own broken pushes or merges trip and
// which stops the stage on that repo, and the bare `<stage>`, which broken pushes on 2 or more repos trip and which stops it on
// every repo until an operator resumes it. `outside-surge` (src/outside.ts) is the outside-author protections' breaker.

export type TripRow = StageTripRow;
/** The key of the outside-author surge breaker (OUTSIDE_SURGE in src/outside.ts, which imports this module). */
const OUTSIDE_SURGE_KEY = "outside-surge";
export const tripRow = async (db: Db, key: string): Promise<TripRow | undefined> => await db.get(db.k.selectFrom("stage_trips").select(kx.cols("stage_trips")).where("stage", "=", key));

/** Push-breaker trips active now, by key (`implement:deckcraft`, or `implement` for the instance-wide tier), with the reason. */
export async function activeTrips(db: Db): Promise<Record<string, string>> {
  const out: Record<string, string> = {};
  for (const r of await db.all(db.k.selectFrom("stage_trips").select(["stage", "reason"]).where("active", "=", 1))) if (tripOf(r.stage)) out[r.stage] = r.reason;
  return out;
}

/** Push-breaker trips active now, with their stage and repo (null: instance-wide), oldest first. */
export async function activeTripRows(db: Db): Promise<(TripRow & { tripStage: TripStage; repo: string | null })[]> {
  const rows = await db.all(db.k.selectFrom("stage_trips").select(kx.cols("stage_trips")).where("active", "=", 1).orderBy("tripped_at").orderBy("stage"));
  return rows.flatMap((r) => { const k = tripOf(r.stage); return k ? [{ ...r, tripStage: k.stage, repo: k.repo }] : []; });
}

const tripReason = (reason: string, wide: boolean): string =>
  `tripped: ${reason}; ${wide ? "an operator resumes it" : "an operator, or a maintainer with write access to the repo, resumes it"}`;

/** Why the runner step cannot start because its stage tripped on every repo, or (with `repo`) on that repo; null when it did not. */
export async function tripReasonOfStep(db: Db, step: string, repo?: string | null): Promise<string | null> {
  const stage = STEP_STAGE[step];
  if (!stage) return null;
  const wide = await tripRow(db, stage);
  if (wide?.active) return tripReason(wide.reason, true);
  const r = repo ? await tripRow(db, tripKey(stage, repo)) : undefined;
  return r?.active ? tripReason(r.reason, false) : null;
}

/** tripReasonOfStep from the trips active now (activeTrips). */
export function tripReasonIn(trips: Readonly<Record<string, string>>, step: string, repo?: string | null): string | null {
  const stage = STEP_STAGE[step];
  if (!stage) return null;
  if (trips[stage] !== undefined) return tripReason(trips[stage], true);
  const reason = repo ? trips[tripKey(stage, repo)] : undefined;
  return reason === undefined ? null : tripReason(reason, false);
}

/** Evidence of a trip, for the Pipeline page and the audit log. */
export async function tripEvidence(db: Db, key: string): Promise<string[]> {
  const r = await tripRow(db, key);
  try { return r ? (JSON.parse(r.evidence) as string[]) : []; } catch { return []; }
}

export async function trip(db: Db, key: string, reason: string, evidence: string[], at: string): Promise<void> {
  const r = await tripRow(db, key);
  if (r?.active) return;
  await db.run(db.k.insertInto("stage_trips").values({ stage: key, active: 1, reason, evidence: JSON.stringify(evidence), tripped_at: at })
    .onConflict((oc) => oc.column("stage").doUpdateSet((eb) => ({ active: 1, reason: eb.ref("excluded.reason"), evidence: eb.ref("excluded.evidence"), tripped_at: eb.ref("excluded.tripped_at") }))));
  await audit(db, "pipeline", "stage-trip", key, { reason, evidence });
}

export const sinceOf = async (db: Db, key: string): Promise<string> => (await tripRow(db, key))?.cleared_at ?? "";

const evidenceOfPushes = (rows: { repo: string; number: number | null; to_oid: string }[]): string[] => rows.map((r) => `${r.repo}#${r.number}@${r.to_oid.slice(0, 8)}`);

/**
 * After a push of `stage` on `repo` broke a required check: trips the stage on `repo` when BREAKER_BROKE of that repo's last
 * BREAKER_WINDOW pushes of the stage since its last resume broke one, and on every repo when the broken ones among the stage's
 * last BREAKER_WINDOW pushes across repos since the instance-wide row's last resume come from 2 or more repos (a fault in the step
 * itself, caught before it breaks 2 heads in every repo). A repo's own breaker has dealt with its broken pushes up to its last resume,
 * and with all of them while it is tripped (the repo is stopped already): those do not count towards the instance-wide one.
 */
export async function evaluatePushBreaker(db: Db, stage: TripStage, repo: string, at = new Date().toISOString()): Promise<void> {
  if (!isTripStage(stage)) return;
  const last = async (key: string, oneRepo: boolean) => await db.all(db.k.selectFrom("push_outcomes").select(["outcome", "repo", "number", "to_oid", "at"]).$narrowType<{ outcome: Outcome | null }>()
    .where("stage", "=", stage).$if(oneRepo, (q) => q.where("repo", "=", repo)).where("at", ">", await sinceOf(db, key)).orderBy("id", "desc").limit(BREAKER_WINDOW));
  const key = tripKey(stage, repo);
  if (!(await tripRow(db, key))?.active) {
    const rows = await last(key, true);
    if (breakerTripped(rows.map((r) => r.outcome))) {
      const bad = rows.filter((r) => r.outcome === "broke");
      await trip(db, key, `${bad.length} of the last ${rows.length} ${stage} pushes in ${repo} broke a required check`, evidenceOfPushes(bad), at);
    }
  }
  if ((await tripRow(db, stage))?.active) return;
  const rows = await last(stage, false);
  const own = new Map<string, TripRow | undefined>();
  for (const r of rows) if (r.outcome === "broke" && !own.has(r.repo)) own.set(r.repo, await tripRow(db, tripKey(stage, r.repo)));
  const bad = rows.filter((r) => { const o = own.get(r.repo); return r.outcome === "broke" && !o?.active && !(o?.cleared_at && r.at <= o.cleared_at); });
  const repos = [...new Set(bad.map((r) => r.repo))].sort();
  if (repos.length < 2) return;
  await trip(db, stage, `${bad.length} of the last ${rows.length} ${stage} pushes broke a required check, in ${repos.length} repos (${repos.join(", ")})`, evidenceOfPushes(bad), at);
}

const weMerged = async (db: Db, repo: string, number: number): Promise<{ at: string } | undefined> =>
  await db.get(db.k.selectFrom("merges").select("at").where("repo", "=", repo).where("number", "=", number).where("outcome", "=", "merged").orderBy("at", "desc").limit(1));

/** The default branch of `repo` is red and its culprit is PR `number` (main CI): trips merges on that repo when we merged that PR after their last resume. */
export async function mainRedAfterMerge(db: Db, repo: string, number: number, sha: string, at = new Date().toISOString()): Promise<void> {
  const m = await weMerged(db, repo, number), key = tripKey("merge", repo);
  if (!m || m.at <= await sinceOf(db, key)) return;
  await trip(db, key, `main CI of ${repo} is red after our merge of #${number}`, [`${repo}#${number}`, `${repo}@${sha.slice(0, 8)}`], at);
}

/** A post-merge review of a PR we merged found defects: trips merges on its repo. */
export async function mergeDefect(db: Db, repo: string, number: number, defects: number, at = new Date().toISOString()): Promise<void> {
  if (defects < 1) return;
  const m = await weMerged(db, repo, number), key = tripKey("merge", repo);
  if (!m || m.at <= await sinceOf(db, key)) return;
  await trip(db, key, `the post-merge review of ${repo}#${number} found ${defects} defect${defects === 1 ? "" : "s"}`, [`${repo}#${number}`], at);
}

/**
 * Who may resume a trip: an operator any trip; a maintainer a per-repo push-breaker trip (`<stage>:<repo>`) when GitHub gives them
 * write permission on its repo. An instance-wide push-stage trip (`<stage>`) and the outside-author surge breaker are operator-only.
 */
export async function mayResumeTrip(user: { role: string; canWrite(repo: string): Promise<boolean> }, key: string): Promise<boolean> {
  if (user.role === "operator") return true;
  const k = parseTripKey(key);
  if (user.role !== "maintainer" || !k) return false;
  return await user.canWrite(k.repo).catch(() => false);
}

export class TripError extends Error {
  status = 400;
}

/** An operator or a maintainer (mayResumeTrip) clears a trip. Pushes and merges before this moment no longer count for that breaker. */
export async function resumeStage(db: Db, key: string, by: string): Promise<void> {
  if (key !== OUTSIDE_SURGE_KEY && !tripOf(key)) throw new TripError(`no such breaker: ${key}; breakers are <stage>:<repo> or <stage> with stage one of ${TRIP_STAGES.join(", ")}, or ${OUTSIDE_SURGE_KEY}`);
  const r = await tripRow(db, key);
  if (!r?.active) throw new TripError(`${key} is not paused by the circuit breaker`);
  const at = new Date().toISOString();
  await db.run(db.k.updateTable("stage_trips").set({ active: 0, cleared_at: at, cleared_by: by }).where("stage", "=", key));
  await audit(db, by, "stage-resume", key, { reason: r.reason });
}
