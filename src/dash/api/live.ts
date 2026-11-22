// The Live board on Home: every running and queued unit of the pipeline's work (jobs, model calls and build tasks of their own, open
// integration batches) on one time axis, with the model calls and build steps inside it and how long the same work took before. Jobs that
// ended in the last minutes stay on it, faded. Repos the reader cannot see are left out as on the Now page (src/dash/api/now.ts); account
// labels are for operators only. Staff also get the worker view: one row per Task Runner (a person's own tokens; private ones share one
// neutral row), box account (operators only) and Jev, with the model calls each ran in the last hour.
import type { ItemRef, ItemShown, LiveBoard, LiveEstimate, LiveLane, LiveOp, LiveOpKind, LiveWorker } from "../../../web/src/api/types.ts";
import { accountListOrNull } from "../../agents/account-actions.ts";
import { STATUS_FRESH_MS } from "../../remote/helpers.ts";
import { privateLogins } from "../../remote/runner-private.ts";
import { privateStepExpr } from "../../build/tasks.ts";
import { touchesHiddenExpr } from "../../agents/runs.ts";
import { filtered, hiddenTestExpr, type HiddenRepos } from "../../hidden-sql.ts";
import type { Req } from "../types.ts";
import { QUIET_STEPS } from "./now.ts";
import { shownLookup, shownMap } from "./shown.ts";
import { canSeeInternals } from "./staff.ts";
import { stepTitle } from "./trace.ts";
import { blankIfHidden, hiddenRepos } from "./visible.ts";
import { flatMapSeq, mapSeq } from "../../async-seq.ts";
import type { Db } from "../../db/index.ts";
import * as kx from "../../db/kx.ts";
import type { Eb } from "../../db/kx.ts";

/** Ended jobs stay on the board this long. */
const RECENT_MS = 20 * 60_000;
const RECENT_MAX = 8;
const QUEUED_MAX = 6;
/** Earlier runs an estimate is read from. */
const SAMPLES = 60;
/** Fewer earlier runs than this give no estimate. */
const MIN_SAMPLES = 3;

/** `col` is not in a hidden repo (or is null, with `nullOk`). */
const notHidden = (db: Db, eb: Eb, col: string, hidden: HiddenRepos, nullOk = false) => {
  const not = eb.not(hiddenTestExpr(db, col, hidden));
  return nullOk ? eb.or([eb(col, "is", null), not]) : not;
};

/** Median and 90th percentile of durations in ms; null with too few. */
function estimateOf(ms: number[]): LiveEstimate | null {
  const xs = ms.filter((x) => Number.isFinite(x) && x > 0).sort((a, b) => a - b);
  if (xs.length < MIN_SAMPLES) return null;
  const at = (q: number) => xs[Math.min(xs.length - 1, Math.floor(q * xs.length))]!;
  return { median: at(0.5), p90: at(0.9), samples: xs.length };
}

/** A build step's kind from its name, as the timeline colours it. */
export function buildKind(name: string): LiveOpKind {
  const n = name.toLowerCase();
  if (/test|spec|jest|vitest|pytest/.test(n)) return "tests";
  if (/lint|clippy|eslint|fmt|format|check|typecheck|tsc/.test(n)) return "lint";
  if (/build|compile|bundle|package|make/.test(n)) return "build";
  if (/install|setup|fetch|checkout|clone|deps|restore|cache|prepare|\bci\b/.test(n)) return "setup";
  return "other";
}

const iso = (ms: number) => new Date(ms).toISOString();
/** The milliseconds from `started_at` to `ended_at`. */
const durationMs = (eb: Eb) => eb(eb.parens(eb.fn<number>("julianday", ["ended_at"]), "-", eb.fn<number>("julianday", ["started_at"])), "*", eb.lit(86400000));

export async function live(db: Db, req: Req): Promise<LiveBoard> {
  const hidden = await hiddenRepos(db, req);
  const staff = canSeeInternals(req);
  const nowMs = Date.now();
  const shown = shownLookup(db);
  const item = async (repo: string, number: number): Promise<ItemRef & ItemShown> => ({ repo, number, ...await shown(repo, number) });

  // Estimates, read once per key.
  const memo = new Map<string, LiveEstimate | null>();
  const memoized = async (key: string, read: () => Promise<number[]>) => (memo.has(key) ? memo.get(key)! : memo.set(key, estimateOf(await read())).get(key)!);
  const jobEstimate = async (step: string) => await memoized(`job:${step}`, async () =>
    (await db.all(db.k.selectFrom("jobs").select((eb) => durationMs(eb).as("ms")).where("step", "=", step).where("state", "=", "done").where("started_at", "is not", null).where("ended_at", "is not", null)
      .orderBy("id", "desc").limit(SAMPLES))).map((r) => r.ms));
  const callEstimate = async (step: string, model: string) => await memoized(`call:${step}:${model}`, async () =>
    (await db.all(db.k.selectFrom("agent_runs").select((eb) => durationMs(eb).as("ms")).where("step", "=", step).where("model", "=", model).where("state", "=", "done").where("ended_at", "is not", null)
      .orderBy("id", "desc").limit(SAMPLES))).map((r) => r.ms));
  const taskEstimate = async (repo: string, step: string) => await memoized(`task:${repo}:${step}`, async () =>
    (await db.all(db.k.selectFrom("build_tasks").select((eb) => durationMs(eb).as("ms")).where("repo", "=", repo).where("step", "=", step).where("state", "=", "done").where("started_at", "is not", null).where("ended_at", "is not", null)
      .orderBy("id", "desc").limit(SAMPLES))).map((r) => r.ms));

  type Call = { id: number; job_id: number | null; step: string; model: string; provider: string; label: string | null; started_at: string; ended_at: string | null; state: string; progress: string | null };
  const callOp = async (c: Call): Promise<LiveOp> => ({
    kind: "model", label: c.provider === "jev" ? "Jev" : c.model, detail: c.provider !== "jev" && req.user.role === "operator" ? `${stepTitle(c.step)} on ${c.label ?? "an unknown account"}` : stepTitle(c.step),
    start: c.started_at, end: c.ended_at, estimate: c.state === "running" ? await callEstimate(c.step, c.model) : null, ...(c.state === "failed" ? { failed: true } : {}),
  });
  type Task = { id: number; job_id: number | null; step: string; repo: string; number: number | null; state: string; queued_at: string; started_at: string | null; ended_at: string | null; results: string; summary: string | null; current: string | null };
  /** A finished task as one op per step (from its recorded durations); a running one as one op named by the step its log is on. */
  const taskOps = async (t: Task): Promise<LiveOp[]> => {
    if (!t.started_at) return [];
    const start = Date.parse(t.started_at);
    if (t.ended_at) {
      let results: { step: string; durationMs: number; exitCode: number | null }[] = [];
      try { results = JSON.parse(t.results) as typeof results; } catch { /* none */ }
      if (!results.length) return [{ kind: "build", label: "Build task", detail: stepTitle(t.step), start: t.started_at, end: t.ended_at, estimate: null, ...(t.state !== "done" ? { failed: true } : {}) }];
      let at = start;
      return results.map((r) => {
        const op: LiveOp = { kind: buildKind(r.step), label: r.step, detail: stepTitle(t.step), start: iso(at), end: iso(at + r.durationMs), estimate: null, ...(r.exitCode !== 0 ? { failed: true } : {}) };
        at += r.durationMs;
        return op;
      });
    }
    return [{ kind: t.current ? buildKind(t.current) : "build", label: t.current ?? "Build task", detail: stepTitle(t.step), start: t.started_at, end: null, estimate: await taskEstimate(t.repo, t.step) }];
  };
  const taskQuery = () => db.k.selectFrom("build_tasks as t")
    .select(["t.id", "t.job_id", "t.step", "t.repo", "t.number", "t.state", "t.queued_at", "t.started_at", "t.ended_at", "t.results", "t.summary",
      (eb) => eb.selectFrom("build_task_logs as l").select("l.step").whereRef("l.task_id", "=", "t.id").orderBy("l.seq", "desc").limit(1).as("current")])
    .$if(!staff, (q) => q.where((eb) => eb.not(privateStepExpr(eb, "t.step"))));
  const callQuery = () => db.k.selectFrom("agent_runs as r").leftJoin("claude_accounts as a", "a.id", "r.account_id")
    .select(["r.id", "r.job_id", "r.step", "r.model", "r.provider", (eb) => eb.fn.coalesce("a.label", "r.account_id").as("label"), "r.started_at", "r.ended_at", "r.state", "r.progress"])
    .$if(filtered(hidden), (q) => q.where((eb) => touchesHiddenExpr(db, eb, hidden)));

  // Jobs: running, then queued (the next few), then the ones that ended lately.
  type Job = { id: number; step: string; state: string; requested_at: string; started_at: string | null; ended_at: string | null; progress: string | null; summary: string | null; error: string | null; repo: string | null };
  const jobQuery = () => db.k.selectFrom("jobs").select(["id", "step", "state", "requested_at", "started_at", "ended_at", "progress", "summary", "error", "repo"])
    .where((eb) => eb.or([eb("state", "=", "failed"), eb("step", "not in", quiet)]));
  const since = iso(nowMs - RECENT_MS);
  // Housekeeping steps show only when they fail; left out before any LIMIT, so they never take a slot.
  const quiet = [...QUIET_STEPS];
  const jobs = [
    ...await db.all(jobQuery().where("state", "=", "running").$if(filtered(hidden), (q) => q.where((eb) => notHidden(db, eb, "repo", hidden, true))).orderBy("id")),
    ...await db.all(jobQuery().where("state", "=", "queued").$if(filtered(hidden), (q) => q.where((eb) => notHidden(db, eb, "repo", hidden, true))).orderBy("id").limit(QUEUED_MAX)),
    ...await db.all(jobQuery().where("state", "in", ["done", "failed"]).where("ended_at", ">", since).where("started_at", "is not", null).$if(filtered(hidden), (q) => q.where((eb) => notHidden(db, eb, "repo", hidden, true)))
      .orderBy("ended_at", "desc").limit(RECENT_MAX * 3)),
  ];

  const lanes: LiveLane[] = [];
  let recent = 0;
  for (const j of jobs) {
    const ended = j.state === "done" || j.state === "failed";
    const its = await db.all(db.k.selectFrom("job_items").select(["repo", "number"]).where("job_id", "=", j.id).$if(filtered(hidden), (q) => q.where((eb) => notHidden(db, eb, "repo", hidden))).orderBy("id"));
    if (ended && its.length === 0 && j.state !== "failed") continue;
    if (ended && ++recent > RECENT_MAX) continue;
    const calls = await db.all(callQuery().where("r.job_id", "=", j.id).orderBy("r.id"));
    const tasks = await db.all(taskQuery().where("t.job_id", "=", j.id).$if(filtered(hidden), (q) => q.where((eb) => notHidden(db, eb, "t.repo", hidden))).orderBy("t.id"));
    const line = j.state === "running" ? blankIfHidden(j.progress, hidden) : j.state === "failed" ? (staff && j.error ? blankIfHidden(j.error, hidden) : "Failed") : j.state === "done" ? blankIfHidden(j.summary, hidden) : null;
    lanes.push({
      key: `job:${j.id}`, title: stepTitle(j.step), state: j.state === "running" ? "running" : j.state === "queued" ? "queued" : j.state === "failed" ? "failed" : "done",
      start: j.started_at ?? j.requested_at, end: j.ended_at, line: line || null,
      item: its[0] ? await item(its[0].repo, its[0].number) : null, itemCount: its.length, repo: j.repo, detail: `/pipeline/${j.id}`,
      estimate: j.state === "running" ? await jobEstimate(j.step) : null,
      ops: [...await mapSeq(calls, callOp), ...await flatMapSeq(tasks, taskOps)].sort((a, b) => a.start.localeCompare(b.start)),
    });
  }

  // Calls and build tasks that run without a job.
  for (const c of await db.all(callQuery().where("r.state", "=", "running").where("r.job_id", "is", null).orderBy("r.id"))) {
    const it = (await db.all(db.k.selectFrom("agent_run_items").select(["repo", "number"]).where("run_id", "=", c.id).orderBy("repo").orderBy("number").limit(1)))[0];
    const op = await callOp(c);
    lanes.push({ key: `run:${c.id}`, title: stepTitle(c.step), state: "running", start: c.started_at, end: null, line: blankIfHidden(c.progress, hidden) || null, item: it ? await item(it.repo, it.number) : null, itemCount: it ? 1 : 0, repo: it?.repo ?? null, detail: `/agents/${c.id}`, estimate: op.estimate, ops: [op] });
  }
  // Open integration batches, with the build task of their combined tree; that task gets no lane of its own.
  type Batch = { id: number; repo: string; state: string; created_at: string; summary: string | null; task_id: number | null; pr_number: number | null };
  const batches = await db.all(db.k.selectFrom("merge_batches").select(["id", "repo", "state", "created_at", "summary", "task_id", "pr_number"]).where("state", "in", ["checking", "open"])
    .$if(filtered(hidden), (q) => q.where((eb) => notHidden(db, eb, "repo", hidden))).orderBy("id"));
  const batchTasks = new Set(batches.map((b) => b.task_id).filter((id): id is number => id !== null));
  for (const b of batches) {
    const prs = await db.all(db.k.selectFrom("merge_batch_prs").select("number").where("batch_id", "=", b.id).orderBy("position"));
    const tasks = b.task_id === null ? [] : await db.all(taskQuery().where("t.id", "=", b.task_id));
    const ops = await flatMapSeq(tasks, taskOps);
    lanes.push({
      key: `batch:${b.id}`, title: "Integration batch", state: "running", start: b.created_at, end: null,
      line: blankIfHidden(b.summary, hidden) || (b.state === "checking" ? "Checking the combined tree" : b.pr_number !== null ? `Batch pull request #${b.pr_number} is open` : null),
      item: prs[0] ? await item(b.repo, prs[0].number) : null, itemCount: prs.length, repo: b.repo, detail: `/batches/${b.id}`, estimate: null, ops,
    });
  }
  for (const t of await db.all(taskQuery().where("t.state", "in", ["queued", "claimed", "running"]).where("t.job_id", "is", null).$if(filtered(hidden), (q) => q.where((eb) => notHidden(db, eb, "t.repo", hidden))).orderBy("t.id"))) {
    if (batchTasks.has(t.id)) continue;
    const ops = await taskOps(t);
    lanes.push({
      key: `task:${t.id}`, title: `Build task, ${stepTitle(t.step)}`, state: t.state === "queued" ? "queued" : "running", start: t.started_at ?? t.queued_at, end: null, line: t.summary,
      item: t.number === null ? null : await item(t.repo, t.number), itemCount: t.number === null ? 0 : 1, repo: t.repo, detail: `/envs/${t.id}`, estimate: ops[0]?.estimate ?? null, ops,
    });
  }
  if (!staff) return { lanes };
  return { lanes, workers: await workersOf(db, req, hidden, nowMs, callEstimate) };
}

/** The worker view shows the calls that ran in this last stretch. */
export const WORKER_WINDOW_MS = 60 * 60_000;
/** At most this many calls go on the worker view, the newest. */
const WORKER_CALLS_MAX = 400;
/** The row that holds the calls of every runner its owner keeps private. */
export const PRIVATE_RUNNER = "Private Task Runner";

const n0 = (v: number | bigint | string | null | undefined): number => (v === null || v === undefined ? 0 : Number(v));

/** A runner's label as the board may show it: never the machine's hostname, which a label defaults to (device-login.ts). */
function shownLabel(label: string | null, machine: string | null): string | null {
  if (!label) return null;
  let host = "";
  try { host = machine ? String((JSON.parse(machine) as { hostname?: unknown }).hostname ?? "") : ""; } catch { /* no machine */ }
  const l = label.trim().toLowerCase();
  const h = host.trim().toLowerCase();
  return h && (l === h.slice(0, 60) || l === h.split(".")[0]) ? null : label;
}

/** The distinct models a runner's status offers, from its `models` column (tier to model). */
function modelsOf(models: string | null): string[] {
  try { return [...new Set(Object.values(models ? JSON.parse(models) as Record<string, unknown> : {}).filter((m): m is string => typeof m === "string" && m !== ""))]; } catch { return []; }
}

/**
 * One row per worker: each shared Task Runner with a fresh status or a call in the window (its owner's login, label, slots and models),
 * one "Private Task Runner" row for the calls of runners whose owners keep them private (runner-private.ts; the reader's own runners
 * are always theirs), each enabled box account (operators only; a disabled one only with a call in the window) and Jev when it ran.
 * Rows keep a fixed order (runners by owner and id, the private row, accounts, then Jev), so a row does not move when its work starts
 * or ends. Calls on repos hidden from the reader are left out. No row carries a machine's hostname.
 */
async function workersOf(
  db: Db, req: Req, hidden: HiddenRepos, nowMs: number,
  callEstimate: (step: string, model: string) => Promise<LiveEstimate | null>,
): Promise<LiveWorker[]> {
  const operator = req.user.role === "operator";
  const me = req.user.login.toLowerCase();
  const since = iso(nowMs - WORKER_WINDOW_MS);
  const runs = (await db.all(db.k.selectFrom("agent_runs as r")
    .select(["r.id", "r.step", "r.model", "r.provider", "r.account_id", "r.user_login", "r.started_at", "r.ended_at", "r.state",
      "r.input_tokens", "r.output_tokens", "r.cache_read_tokens", "r.cache_write_tokens",
      (eb) => eb.selectFrom("agent_run_items as i").select((e) => e.fn.countAll<number>().as("n")).whereRef("i.run_id", "=", "r.id").as("n"),
      (eb) => eb.selectFrom("agent_run_items as i").select("i.repo").whereRef("i.run_id", "=", "r.id").orderBy("i.repo").orderBy("i.number").limit(1).as("repo"),
      (eb) => eb.selectFrom("agent_run_items as i").select("i.number").whereRef("i.run_id", "=", "r.id").orderBy("i.repo").orderBy("i.number").limit(1).as("number")])
    .where((eb) => eb.or([eb("r.state", "=", "running"), eb("r.ended_at", ">", since)]))
    .$if(filtered(hidden), (q) => q.where((eb) => touchesHiddenExpr(db, eb, hidden)))
    .orderBy("r.id", "desc").limit(WORKER_CALLS_MAX))).sort((a, b) => a.started_at.localeCompare(b.started_at));
  // The runner credential that leased each remote call (only the lease's run is linked; an older lease falls back to its owner).
  const leases = new Map((await db.all(db.k.selectFrom("remote_calls").select(["run_id", "token_id"]).where("run_id", "is not", null).where("token_id", "is not", null)
    .where((eb) => eb.or([eb("state", "=", "leased"), eb.and([eb("state", "in", ["done", "failed"]), eb("ended_at", ">", nowMs - WORKER_WINDOW_MS)])]))))
    .map((l) => [Number(l.run_id), Number(l.token_id)]));

  // Task Runners with a fresh status, and every one that ran a call in the window.
  const tokenIds = [...new Set(leases.values())];
  const helpers = (await db.all(db.k.selectFrom("mcp_tokens as t").leftJoin("remote_helpers as h", "h.token_id", "t.id")
    .select(["t.id", "t.login", "t.label", "t.machine", "h.readiness", "h.reason", "h.harness", "h.reported_at", "h.slots", "h.models"])
    .where("t.kind", "=", "runner")
    .where((eb) => eb.or([eb("h.reported_at", ">", nowMs - STATUS_FRESH_MS), kx.inList(db, "t.id", tokenIds, "bigint")]))))
    .sort((a, b) => a.login.localeCompare(b.login) || Number(a.id) - Number(b.id));
  const kept = await privateLogins(db, [...helpers.map((h) => h.login), ...runs.flatMap((r) => (r.user_login ? [r.user_login] : []))]);
  const isPrivate = (login: string) => kept.has(login.toLowerCase()) && login.toLowerCase() !== me;
  const ownerOf = new Map(helpers.map((h) => [Number(h.id), h.login]));

  type Run = (typeof runs)[number];
  const workerOfRun = (r: Run): string | null => {
    if (r.provider === "jev") return "jev";
    if (r.user_login) {
      const token = leases.get(Number(r.id));
      return isPrivate(token !== undefined ? ownerOf.get(token) ?? r.user_login : r.user_login) ? "private"
        : token !== undefined ? `runner:${token}` : `login:${r.user_login.toLowerCase()}`;
    }
    return r.account_id && operator ? `account:${r.account_id}` : null;
  };
  const keyOf = new Map(runs.map((r) => [Number(r.id), workerOfRun(r)]));

  const rows = new Map<string, LiveWorker>();
  for (const h of helpers) {
    if (isPrivate(h.login)) continue;
    const fresh = h.reported_at !== null && nowMs - Number(h.reported_at) <= STATUS_FRESH_MS;
    rows.set(`runner:${h.id}`, {
      key: `runner:${h.id}`, kind: "runner", name: shownLabel(h.label, h.machine) ?? "Task Runner", login: h.login, harness: h.harness,
      status: !fresh ? "offline" : h.readiness === "ready" ? "online" : "held", note: fresh && h.readiness !== "ready" ? blankIfHidden(h.reason, hidden) || h.readiness : null,
      busy: 0, slots: fresh && h.slots !== null ? Number(h.slots) : null, models: fresh ? modelsOf(h.models) : [], calls: [],
    });
  }
  for (const r of runs) {
    const key = keyOf.get(Number(r.id));
    if (!r.user_login || !key?.startsWith("login:") || rows.has(key)) continue;
    rows.set(key, { key, kind: "runner", name: "Task Runner", login: r.user_login, harness: null, status: "offline", note: null, busy: 0, slots: null, models: [], calls: [] });
  }
  if ([...keyOf.values()].includes("private")) {
    rows.set("private", { key: "private", kind: "private", name: PRIVATE_RUNNER, login: null, harness: null, status: "offline", note: null, busy: 0, slots: null, models: [], calls: [] });
  }
  if (operator) {
    const used = new Set(runs.map((r) => r.account_id));
    for (const a of await accountListOrNull(db, nowMs, false) ?? []) {
      if (!a.enabled && !used.has(a.id)) continue;
      rows.set(`account:${a.id}`, {
        key: `account:${a.id}`, kind: "account", name: a.label, login: null, harness: "claude",
        status: !a.enabled ? "offline" : a.pausedReason ? "held" : "online", note: a.enabled ? blankIfHidden(a.pausedReason, hidden) || null : "Disabled",
        busy: 0, slots: a.enabled ? a.maxConcurrent : null, models: [], calls: [],
      });
    }
  }
  if (runs.some((r) => r.provider === "jev")) rows.set("jev", { key: "jev", kind: "jev", name: "Jev", login: null, harness: null, status: "online", note: null, busy: 0, slots: null, models: [], calls: [] });

  const shown = await shownMap(db, runs.flatMap((r) => (r.repo !== null && r.number !== null ? [{ repo: r.repo, number: Number(r.number) }] : [])));
  for (const r of runs) {
    const key = keyOf.get(Number(r.id));
    const row = key ? rows.get(key) : undefined;
    if (!row) continue;
    const running = r.state === "running";
    if (running) row.busy++;
    const recorded = r.input_tokens !== null || r.output_tokens !== null;
    const ref = r.repo !== null && r.number !== null ? { repo: r.repo, number: Number(r.number) } : null;
    row.calls.push({
      id: Number(r.id), step: r.step, title: stepTitle(r.step), model: r.provider === "jev" ? "Jev" : r.model,
      item: ref ? { ...ref, ...shown.get(`${ref.repo}#${ref.number}`) ?? { kind: null, state: null, title: null } } : null, itemCount: n0(r.n),
      start: r.started_at, end: r.ended_at,
      tokens: recorded ? n0(r.input_tokens) + n0(r.output_tokens) + n0(r.cache_read_tokens) + n0(r.cache_write_tokens) : null,
      estimate: running ? await callEstimate(r.step, r.model) : null, ...(r.state === "failed" ? { failed: true } : {}),
    });
  }
  const priv = rows.get("private");
  if (priv) priv.status = priv.busy > 0 ? "online" : "offline";
  return [...rows.values()];
}
