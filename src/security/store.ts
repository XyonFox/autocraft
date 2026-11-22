// Rows of the security deep scan and their views for the API (web/src/api/types.ts).
import { dropSessions } from "../prepush/resume.ts";
import { slugOf } from "../owners.ts";
import { securityRepo } from "./rules.ts";
import type { FindingCall, FindingRef, FindingSeverity, FindingState, FindingSummary, FindingVerification, FindingView, ScanArea, ScanCall, ScanDetail, ScanEntryPoint, ScanState, ScanTestState, FindingScanTest, ScanView, SecurityOverview } from "../../web/src/api/types.ts";
import { mapSeq } from "../async-seq.ts";
import type { Db } from "../db/index.ts";
import * as kx from "../db/kx.ts";
import type { ExpressionBuilder } from "kysely";
import type { DB } from "../db/types.ts";

export const ACTIVE_STATES: ScanState[] = ["modeling", "scanning", "verifying"];
/** Findings in these states are the scan's own bookkeeping, never listed. */
export const INTERNAL_FINDING_STATES = ["unverified", "disproved", "duplicate"] as const;
const now = (): string => new Date().toISOString();

type ScanRow = {
  id: number; repo: string; commit_sha: string | null; state: ScanState; cap_usd: number | null; cost_usd: number; estimate_usd: number | null;
  areas_total: number; areas_done: number; started_by: string; started_at: string; ended_at: string | null; error: string | null; job_id: number | null; threat_model: string | null;
};

const scans = (db: Db) => db.k.selectFrom("security_scans").select(kx.cols("security_scans")).$narrowType<{ state: ScanState }>();
const findings = (db: Db) => db.k.selectFrom("security_findings").select(kx.cols("security_findings")).$narrowType<{ severity: FindingSeverity; state: FindingState }>();

export const scanRow = async (db: Db, id: number): Promise<ScanRow | undefined> => await db.get(scans(db).where("id", "=", id));

export async function toScan(db: Db, r: ScanRow): Promise<ScanView> {
  const n = (await db.get(db.k.selectFrom("security_findings").select((eb) => eb.fn.countAll<number>().as("n")).where("scan_id", "=", r.id).where("state", "not in", INTERNAL_FINDING_STATES)))!.n;
  return {
    id: r.id, repo: r.repo, commit: r.commit_sha, state: r.state, capUsd: r.cap_usd, costUsd: r.cost_usd, estimateUsd: r.estimate_usd,
    areasTotal: r.areas_total, areasDone: r.areas_done, findingCount: n, startedBy: r.started_by, startedAt: r.started_at, endedAt: r.ended_at, error: r.error, canAct: false, canContinue: false,
  };
}

/** The active scan of a repo, if any. Scans of one repo never overlap. */
export async function activeScan(db: Db, repo: string): Promise<ScanRow | undefined> {
  return await db.get(scans(db).where("repo", "=", repo).where("state", "in", ACTIVE_STATES).orderBy("id", "desc").limit(1));
}

/** An active scan whose job has ended without the scan finishing (a killed runner, a cancelled job) is closed: stopped when the job was cancelled, else failed. */
export async function reconcileScans(db: Db, repo?: string): Promise<void> {
  const rows = await db.all(db.k.selectFrom("security_scans as s").innerJoin("jobs as j", "j.id", "s.job_id").select(["s.id", "j.state as job_state"])
    .where("s.state", "in", ACTIVE_STATES).where("j.state", "in", ["done", "failed", "cancelled"]).$if(!!repo, (q) => q.where("s.repo", "=", repo!)));
  // A scan whose job was never recorded (the request died between the insert and the enqueue) would block its repo for good.
  const orphans = await db.all(db.k.selectFrom("security_scans").select("id").where("job_id", "is", null).where("started_at", "<", new Date(Date.now() - 60_000).toISOString())
    .where("state", "in", ACTIVE_STATES).$if(!!repo, (q) => q.where("repo", "=", repo!)));
  for (const o of orphans) await finishScan(db, o.id, "failed", "The scan was never queued.");
  for (const r of rows) {
    const cancelled = r.job_state === "cancelled";
    await finishScan(db, r.id, cancelled ? "stopped" : "failed", cancelled ? null : "The scan's job ended before the scan finished.");
  }
}

export const UNCHECKED_RESULT = "Not verified: the scan ended before this finding was checked.";

/** Findings a capped scan left unchecked, and that nobody has acted on since, go back to the scan's own bookkeeping so a continued scan verifies them. Returns how many. */
export async function reopenUnchecked(db: Db, scanId: number): Promise<number> {
  return (await db.run(db.k.updateTable("security_findings").set({ state: "unverified", verification: null, updated_at: now() }).where("scan_id", "=", scanId).where("state", "=", "new")
    .where((eb) => eb(kx.jsonText(db, "verification", "$.result"), "=", UNCHECKED_RESULT)))).changes;
}

/** Ends an active scan; a scan that already ended keeps its state. Leftover unverified findings become listed findings that say so. */
export async function finishScan(db: Db, id: number, state: ScanState, error: string | null = null): Promise<void> {
  await db.run(db.k.updateTable("security_scans").set({ state, ended_at: now(), error }).where("id", "=", id).where("state", "in", ACTIVE_STATES));
  const unchecked = JSON.stringify({ kind: "reasoning", result: UNCHECKED_RESULT, notes: "", score: null, proposedTest: null } satisfies FindingVerification);
  await db.run(db.k.updateTable("security_findings").set({ state: "new", verification: unchecked, updated_at: now() }).where("scan_id", "=", id).where("state", "=", "unverified"));
}

const parse = <T>(s: string | null, fallback: T): T => {
  try { return s ? JSON.parse(s) as T : fallback; } catch { return fallback; }
};

export type FindingRow = {
  repo: string; number: number; scan_id: number; area: string; title: string; severity: FindingSeverity; urgent: number; state: FindingState; refs: string;
  reach: string; impact: string; verification: string | null; fix_commit: string | null; fix_pr: number | null; ghsa_id: string | null; fork: string | null; fix_patch: string | null; fix_title: string | null; fix_summary: string | null; fix_branch: string | null; fix_files: string | null; fix_ref: string | null; note: string | null; env_wait_at: string | null; reason: string | null; decided_by: string | null; created_at: string; updated_at: string; fix_attempts: number; last_failure: string | null;
};

export const findingKey = (repo: string, n: number): string => `${repo}:finding:${n}`;

const summary = (r: FindingRow): FindingSummary => ({
  repo: r.repo, number: r.number, key: findingKey(r.repo, r.number), scanId: r.scan_id, title: r.title, severity: r.severity, urgent: r.urgent === 1, area: r.area,
  state: r.state, createdAt: r.created_at, updatedAt: r.updated_at,
});

/** The finding's scan test, or null when it has none. */
export async function scanTestOf(db: Db, repo: string, n: number): Promise<FindingScanTest | null> {
  const r = await db.get(db.k.selectFrom("security_scan_tests").select(["state", "detail", "test_path", "test_source"]).$narrowType<{ state: ScanTestState }>().where("repo", "=", repo).where("number", "=", n));
  return r ? { state: r.state, detail: r.detail, path: r.test_path, source: r.test_source } : null;
}

/** Adds each finding's scan-test state; findings without a scan test (every finding while the switch is off) keep no key. */
async function withScanTests(db: Db, rows: FindingSummary[]): Promise<FindingSummary[]> {
  if (!rows.length) return rows;
  const states = new Map((await db.all(db.k.selectFrom("security_scan_tests").select(["repo", "number", "state"]).$narrowType<{ state: ScanTestState }>())).map((t) => [`${t.repo}#${t.number}`, t.state]));
  return states.size ? rows.map((r) => (states.has(`${r.repo}#${r.number}`) ? { ...r, scanTest: states.get(`${r.repo}#${r.number}`)! } : r)) : rows;
}

const severityOrder = (eb: ExpressionBuilder<DB, "security_findings">) => eb.case("severity").when("critical").then(0).when("high").then(1).when("medium").then(2).else(3).end();

/** Forgets the rounds of a finding's fix (it ended, ready or not) and removes the sessions they still keep. The rounds stay in security_fix_log, without patches. */
export async function clearRounds(db: Db, repo: string, n: number): Promise<void> {
  const f = await db.get(db.k.selectFrom("security_findings").select(["fix_attempts", "state"]).where("repo", "=", repo).where("number", "=", n));
  const attempt = f?.fix_attempts ?? 0;
  const ended = new Date().toISOString();
  for (const r of await db.all(db.k.selectFrom("security_fix_rounds").select(kx.cols("security_fix_rounds")).where("repo", "=", repo).where("number", "=", n).orderBy("round"))) {
    dropSessions(r.session);
    await db.run(db.k.insertInto("security_fix_log")
      .values({ repo, number: n, attempt, round: r.round, state: r.state === "building" && f?.state === "ready" ? "passed" : r.state, failure: r.failure, title: r.title, created_at: r.created_at, ended_at: ended })
      .onConflict((oc) => oc.columns(["repo", "number", "attempt", "round"]).doUpdateSet(kx.excluded(["state", "failure", "title", "created_at", "ended_at"]))));
  }
  await db.run(db.k.deleteFrom("security_fix_rounds").where("repo", "=", repo).where("number", "=", n));
}

/**
 * A finding that is `fixing`, or `landing` without a pull request, while no step job of it is queued or running (the job was
 * killed or cancelled) goes back to where an operator can act again. A fixing finding with a round in the build, or one
 * waiting for a confidential build environment, has no job on purpose: advanceSecurityFixes queues one when the round's
 * tasks end or the environment is ready.
 */
export async function reconcileFindings(db: Db, repo: string): Promise<void> {
  const grace = new Date(Date.now() - 60_000).toISOString();
  const rows = await db.all(db.k.selectFrom("security_findings").select(["number", "state"]).$narrowType<{ state: FindingState }>().where("repo", "=", repo).where("updated_at", "<", grace)
    .where((eb) => eb.or([eb("state", "=", "fixing"), eb.and([eb("state", "=", "landing"), eb("fix_pr", "is", null)])]))
    .where((eb) => eb.not(eb.exists(eb.selectFrom("jobs as j").select(eb.lit(1).as("one")).where("j.step", "in", ["security-fix", "security-land", "security-advisory"]).where("j.state", "in", ["queued", "running"])
      .where((e) => e.and([e(kx.jsonText(db, "j.params", "$.repo"), "=", e.ref("security_findings.repo")), e(kx.jsonNum(db, "j.params", "$.number"), "=", e.ref("security_findings.number"))])))))
    .where((eb) => eb.not(eb.exists(eb.selectFrom("security_fix_rounds as r").select(eb.lit(1).as("one")).whereRef("r.repo", "=", "security_findings.repo").whereRef("r.number", "=", "security_findings.number").where("r.state", "=", "building"))))
    .where((eb) => eb.not(eb.and([eb("state", "=", "fixing"), eb("env_wait_at", "is not", null)]))));
  for (const r of rows) {
    const note = "The job ended before it finished. Try again.";
    // The rounds a dead fix job left are logged under the attempt that made them, before the next Fix counts a new one.
    if (r.state === "fixing") await clearRounds(db, repo, r.number);
    await advance(db, repo, r.number, [r.state], r.state === "fixing" ? "new" : "ready", r.state === "fixing" ? { note, last_failure: note } : { note });
  }
}

export async function overview(db: Db, repo: string): Promise<SecurityOverview> {
  await reconcileScans(db, repo);
  await reconcileFindings(db, repo);
  const scanList = await mapSeq(await db.all(scans(db).where("repo", "=", repo).orderBy("id", "desc").limit(20)), async (r) => await toScan(db, r));
  const listed = (await db.all(findings(db).where("repo", "=", repo).where("state", "not in", INTERNAL_FINDING_STATES).orderBy(severityOrder).orderBy("number").limit(500))).map(summary);
  return { canStart: false, scans: scanList, findings: await withScanTests(db, listed) };
}

export const findingRow = async (db: Db, repo: string, n: number): Promise<FindingRow | undefined> =>
  await db.get(findings(db).where("repo", "=", repo).where("number", "=", n).where("state", "not in", INTERNAL_FINDING_STATES));

export async function toFinding(db: Db, r: FindingRow): Promise<FindingView> {
  const area = await db.get(db.k.selectFrom("security_areas").select("idx").where("scan_id", "=", r.scan_id).where("name", "=", r.area));
  const calls = await db.all(db.k.selectFrom("security_calls").select(["kind", "started_at", "ended_at", "ok", "cost_usd"]).$narrowType<{ kind: FindingCall["kind"] }>().where("scan_id", "=", r.scan_id)
    .where((eb) => eb.or([eb("kind", "=", "model"), eb.and([eb("kind", "=", "area"), eb("area_idx", "=", area?.idx ?? -1)]), eb.and([eb("kind", "in", ["verify", "test", "fix", "review"]), eb("finding_number", "=", r.number)])]))
    .orderBy("id"));
  const repeats = await db.all(db.k.selectFrom("security_findings").select("created_at").where("repo", "=", r.repo).where("duplicate_of", "=", r.number).orderBy("created_at"));
  const timeline: FindingCall[] = [
    ...calls.map((c) => ({ kind: c.kind, at: c.started_at, endedAt: c.ended_at, ok: c.ok === 1, costUsd: c.cost_usd })),
    ...repeats.map((d): FindingCall => ({ kind: "duplicate", at: d.created_at, endedAt: null, ok: true, costUsd: null })),
  ].sort((a, b) => a.at.localeCompare(b.at));
  return {
    ...summary(r), scanTestResult: await scanTestOf(db, r.repo, r.number), refs: parse<FindingRef[]>(r.refs, []), reach: r.reach, impact: r.impact, verification: parse<FindingVerification | null>(r.verification, null),
    fixDiff: r.fix_patch, fixPr: r.fix_pr, fixPrUrl: r.fix_pr === null ? null : `https://github.com/${r.fork ?? slugOf(r.repo)}/pull/${r.fix_pr}`,
    ghsaId: r.ghsa_id, advisoryUrl: r.ghsa_id ? `https://github.com/${slugOf(r.repo)}/security/advisories/${r.ghsa_id}` : null, note: r.note,
    reason: r.reason, decidedBy: r.decided_by,
    timeline,
  };
}

/** The states a finding moves to by each operator action, and from which states. */
export const TRANSITIONS: Record<"dismiss" | "backlog" | "reopen", { from: FindingState[]; to: FindingState }> = {
  dismiss: { from: ["new", "backlog", "ready", "awaiting-admin"], to: "dismissed" },
  backlog: { from: ["new", "dismissed"], to: "backlog" },
  reopen: { from: ["dismissed", "backlog"], to: "new" },
};

export async function moveFinding(db: Db, repo: string, n: number, action: keyof typeof TRANSITIONS, by: string, reason: string | null): Promise<FindingState | null> {
  const t = TRANSITIONS[action];
  const r = await db.run(db.k.updateTable("security_findings").set({ state: t.to, reason: action === "dismiss" ? reason : null, decided_by: by, updated_at: now() })
    .where("repo", "=", repo).where("number", "=", n).where("state", "in", t.from));
  return r.changes ? t.to : null;
}

/**
 * Moves a finding from one of the states `from` to `to` and sets more columns in the same statement. Returns false when the
 * finding is in none of them, so two requests cannot both start the same step.
 */
export async function advance(db: Db, repo: string, n: number, from: FindingState[], to: FindingState, set: Record<string, string | number | null> = {}, by?: string): Promise<boolean> {
  const r = await db.run(db.k.updateTable("security_findings").set({ state: to, updated_at: now(), ...(by ? { decided_by: by } : {}), ...set })
    .where("repo", "=", repo).where("number", "=", n).where(kx.inList(db, "state", from)));
  return r.changes > 0;
}

/** The finding's row whatever its state, for the steps that work on it. */
export const rawFinding = async (db: Db, repo: string, n: number): Promise<FindingRow | undefined> =>
  await db.get(findings(db).where("repo", "=", repo).where("number", "=", n));

/** Records one model call of a finding's fix in its timeline and returns nothing; the cost may be unknown. */
export async function recordFixCall(db: Db, scanId: number, n: number, kind: "fix" | "review", startedAt: string, ok: boolean, costUsd: number | null, resumed = false): Promise<void> {
  await db.run(db.k.insertInto("security_calls").values({ scan_id: scanId, kind, finding_number: n, started_at: startedAt, ended_at: now(), ok: ok ? 1 : 0, cost_usd: costUsd, resumed: resumed ? 1 : 0 }));
}

/** Scans of one repo or, with none given, of the repos in `repos`, newest first. */
export async function listScans(db: Db, repos: string[], repo?: string): Promise<ScanView[]> {
  const want = repo ? repos.filter((r) => r === repo) : repos;
  if (!want.length) return [];
  await reconcileScans(db, repo);
  const rows = await db.all(scans(db).where("repo", "in", want).orderBy("id", "desc").limit(200));
  return await mapSeq(rows, async (r) => await toScan(db, r));
}

/**
 * The scan with its threat model, calls and findings. A call keeps the agent run it recorded; a call without one (rows from before
 * the link, and a call still running) takes the first run of the scan that started within its time window.
 */
export async function scanDetail(db: Db, r: ScanRow): Promise<ScanDetail> {
  await reconcileScans(db, r.repo);
  r = (await scanRow(db, r.id))!;
  const model = parse<{ summary?: string; entryPoints?: ScanEntryPoint[] }>(r.threat_model, {});
  const callRows = await db.all(db.k.selectFrom("security_calls").select(kx.cols("security_calls")).$narrowType<{ kind: FindingCall["kind"] }>().where("scan_id", "=", r.id).orderBy("id"));
  const runFor = async (pseudo: string, scanId: number, startedAt: string, endedAt: string | null) =>
    await db.get(db.k.selectFrom("agent_runs as ar").innerJoin("agent_run_items as i", "i.run_id", "ar.id").select(["ar.id", "ar.state", "ar.progress"])
      .where("i.repo", "=", pseudo).where("i.number", "=", scanId).where("ar.started_at", ">=", startedAt).$if(endedAt !== null, (q) => q.where("ar.started_at", "<=", endedAt!)).orderBy("ar.id").limit(1));
  const runById = async (id: number) => await db.get(db.k.selectFrom("agent_runs").select(["id", "state", "progress"]).where("id", "=", id));
  const pseudo = securityRepo(r.repo);
  const areaName = async (idx: number | null): Promise<string | null> => idx === null ? null : (await db.get(db.k.selectFrom("security_areas").select("name").where("scan_id", "=", r.id).where("idx", "=", idx)))?.name ?? null;
  const live = ACTIVE_STATES.includes(r.state);
  const calls: ScanCall[] = await mapSeq(callRows, async (c) => {
    const run = c.agent_run_id !== null ? await runById(c.agent_run_id) : await runFor(pseudo, r.id, c.started_at, c.ended_at);
    const running = c.ended_at === null && live;
    return {
      id: c.id, kind: c.kind, areaName: await areaName(c.area_idx), findingNumber: c.finding_number,
      state: running ? "running" : c.ok === 1 ? "done" : "failed", startedAt: c.started_at, endedAt: c.ended_at, costUsd: c.cost_usd,
      runId: run?.id ?? null, progress: running ? run?.progress ?? null : null,
    };
  });
  const runningAreas = new Set(callRows.filter((c) => c.kind === "area" && c.ended_at === null && live).map((c) => c.area_idx));
  const areas = (await db.all(db.k.selectFrom("security_areas").select(["idx", "name", "paths", "risk", "why", "state", "cost_usd"]).where("scan_id", "=", r.id).orderBy("idx")))
    .map((a): ScanArea => ({ idx: a.idx, name: a.name, risk: a.risk, why: a.why, paths: parse<string[]>(a.paths, []), state: a.state === "done" || a.state === "failed" ? a.state : runningAreas.has(a.idx) ? "running" : "pending", costUsd: a.cost_usd }));
  const listed = await withScanTests(db, (await db.all(findings(db).where("scan_id", "=", r.id).where("state", "not in", INTERNAL_FINDING_STATES).orderBy(severityOrder).orderBy("number"))).map(summary));
  return { scan: await toScan(db, r), summary: model.summary ?? null, entryPoints: model.entryPoints ?? [], areas, calls, findings: listed };
}
