// Approves the Actions runs that GitHub holds for approval (`action_required`) on pull requests from outside
// contributors, once the current head has a final take or take-fix review: the review read that code, and the
// runs use GitHub's runners. The checks sync records the waiting runs per PR head from the check suites in its
// query (recordRuns); a merge refused for `action_required` marks the head for a read of its runs (markReread).
// The ci-approve step approves them (approveRuns) for repos an operator approved. Set only from the dashboard
// (src/dash/api/admin.ts).
import { jobItem } from "../runner/job-items.ts";
import { MAX_FAILURES } from "../core/rules.ts";
import { audit } from "../dash/audit.ts";
import { effectiveCi } from "../optional-checks.ts";
import { planned } from "../core/select.ts";
import type { PlanSet } from "../core/plan-set.ts";
import type { Exec } from "../fixup/git.ts";
import { guard } from "../core/guard.ts";
import { CLI_BY, type By } from "../core/gates.ts";
import { ghApproveRun, type Writer } from "../github/write.ts";
import { mergeHeld } from "./holds.ts";
import { trackedNames } from "../repos.ts";
import { ghExec, type Gh } from "./safe-merge.ts";
import { slugOf } from "../owners.ts";
import * as kx from "../db/kx.ts";
import { setting } from "../settings-read.ts";
import type { Db } from "../db/index.ts";
import { openDb } from "../db/open.ts";

export type CiApproval = { by: string; at: string };
const key = (repo: string) => `ci_approved:${repo}`;

export async function ciApprovalOf(db: Db, repo: string): Promise<CiApproval | null> {
  const value = await setting(db, key(repo));
  if (value === undefined) return null;
  try {
    return JSON.parse(value) as CiApproval;
  } catch {
    return null;
  }
}

// Returns false when nothing changed.
export async function setCiApproval(db: Db, by: string, repo: string, approved: boolean): Promise<boolean> {
  if (!await db.get(db.k.selectFrom("main.tracked_repos").select((eb) => eb.lit(1).as("one")).where("name", "=", repo))) throw new Error(`${repo} is not tracked`);
  if ((await ciApprovalOf(db, repo) !== null) === approved) return false;
  if (approved) await db.run(db.k.insertInto("main.settings").values({ key: key(repo), value: JSON.stringify({ by, at: new Date().toISOString() }) }));
  else await db.run(db.k.deleteFrom("main.settings").where("key", "=", key(repo)));
  await audit(db, by, approved ? "ci-approve" : "ci-revoke", repo);
  return true;
}

/** The stage's audit login. */
export const ACTOR = "ci-approve";
/** Approval attempts per run before the PR is left to a person. */
export { MAX_FAILURES };
/** Refusals by GitHub that repeat when asked again; other errors (network, 5xx) are retried without counting. */
const REPEATABLE = /HTTP (403|422)/;
/** How long approved runs may stay unfinished before the PR is trusted to CI's own state again. */
const APPROVED_TTL_MS = 2 * 3_600_000;

export type WaitingRun = { id: number; name: string; event: string };
/** A workflow run's check suite as the checks query returns it. */
export type Suite = { id: number; name: string; event: string; status: string; conclusion: string | null };
type Row = { head_oid: string; cross_repo: number; runs: string; approved: string; reread: number; failed: string; approved_at: string | null };

type RunsInput = { repo: string; number: number; head_oid: string; state: string; cross_repo?: boolean; suites?: Suite[] };

/**
 * Records the workflow runs waiting for approval at each open PR's head, from the check suites the sync read. Called with
 * every PR the checks job stores: the recorded rows are read in one query and written in multi-row statements.
 */
export async function recordRuns(db: Db, rows: readonly RunsInput[], at = new Date().toISOString()): Promise<void> {
  const open = rows.filter((c) => c.state === "OPEN");
  if (!open.length) return;
  // The previous rows are read and rewritten in one write transaction, so a concurrent writer cannot slip between them.
  await db.write(async () => {
    const readPrev = (keys: (readonly [string, number])[]) => db.all(db.k.selectFrom("ci_run_approvals").select(kx.cols("ci_run_approvals")).where(kx.refIn(db, "ci_run_approvals", keys)));
    const prevs = new Map<string, Awaited<ReturnType<typeof readPrev>>[number]>();
    for (const keys of kx.batches(open.map((c) => [c.repo, c.number] as const)))
      for (const p of await readPrev(keys)) prevs.set(`${p.repo}#${p.number}`, p);
    const approvals = new Map<string, boolean>();
    const approvalOn = async (repo: string): Promise<boolean> => {
      if (!approvals.has(repo)) approvals.set(repo, await ciApprovalOf(db, repo) !== null);
      return approvals.get(repo)!;
    };
    const puts: { repo: string; number: number; head_oid: string; cross_repo: number; runs: string; approved: string; reread: number; failed: string; checked_at: string; approved_at: string | null }[] = [];
    for (const c of open) {
      const prev = prevs.get(`${c.repo}#${c.number}`);
      const suites = c.suites ?? [];
      const same = prev?.head_oid === c.head_oid ? prev : undefined;
      const approved = (JSON.parse(same?.approved ?? "[]") as number[]);
      const byId = new Map(suites.map((x) => [x.id, x]));
      const fresh = !!same?.approved_at && Date.now() - Date.parse(same.approved_at) < APPROVED_TTL_MS;
      // An approved run is settled once its suite completed with something other than the old action_required.
      const left = approved.filter((id) => fresh && !(byId.get(id)?.status === "COMPLETED" && byId.get(id)?.conclusion !== "ACTION_REQUIRED"));
      const waiting: WaitingRun[] = suites.filter((x) => x.conclusion === "ACTION_REQUIRED" && !approved.includes(x.id)).map((x) => ({ id: x.id, name: x.name, event: x.event }));
      if (!prev && !waiting.length) continue;
      // With the approval off nothing clears a mark the suites do not confirm, so the sync does.
      const reread = same?.reread && (suites.some((x) => x.conclusion === "ACTION_REQUIRED") || await approvalOn(c.repo)) ? 1 : 0;
      puts.push({ repo: c.repo, number: c.number, head_oid: c.head_oid, cross_repo: c.cross_repo ? 1 : 0, runs: JSON.stringify(waiting), approved: JSON.stringify(left), reread, failed: same?.failed ?? "{}", checked_at: at, approved_at: same?.approved_at ?? null });
    }
    for (const values of kx.batches(puts))
      await db.run(db.k.insertInto("ci_run_approvals").values(values)
        .onConflict((oc) => oc.columns(["repo", "number"]).doUpdateSet(kx.excluded(["head_oid", "cross_repo", "runs", "approved", "reread", "failed", "checked_at", "approved_at"]))));
  });
}

/** A merge was refused because of `action_required` runs the sync does not list: the step reads that head's runs. */
export async function markReread(db: Db, repo: string, number: number, head: string, at = new Date().toISOString()): Promise<void> {
  await db.run(db.k.deleteFrom("ci_run_approvals").where("repo", "=", repo).where("number", "=", number).where("head_oid", "<>", head));
  await db.run(db.k.insertInto("ci_run_approvals").values({ repo, number, head_oid: head, cross_repo: 1, runs: "[]", approved: "[]", reread: 1, failed: "{}", checked_at: at })
    .onConflict((oc) => oc.columns(["repo", "number"]).doUpdateSet({ reread: 1, ...kx.excluded(["checked_at"]) })));
}

/** `overall` as CI reads for the PR's head, with failures of the repo's optional checks left out (effectiveOverall): "pending" while runs wait for approval or the ones we approved have not finished, since the check rollup does not list them. */
export async function overallAt(db: Db, repo: string, number: number, head: string, overall: string): Promise<string> {
  const eff = overall === "failure" ? await effectiveCi(db, repo, overall, (await db.get(db.k.selectFrom("pr_checks").select("checks").where("repo", "=", repo).where("number", "=", number).where("head_oid", "=", head)))?.checks) : overall;
  const r = await db.get(db.k.selectFrom("ci_run_approvals").select(["runs", "approved", "reread"]).where("repo", "=", repo).where("number", "=", number).where("head_oid", "=", head));
  if (!r) return eff;
  return r.reread || JSON.parse(r.runs).length || JSON.parse(r.approved).length ? "pending" : eff;
}

export type Candidate = { number: number; head: string; runs: WaitingRun[]; reread: boolean; failed: Record<string, number>; crossRepo: boolean };

/**
 * The candidates the step has work on (approvable runs of a fork PR, or a head to read): the PRs whose Plan holds an `approve-ci` action: reviewed PRs of the repo with runs waiting at the reviewed head, and open integration
 * batch PRs with runs waiting at the batch tip (their code is the members' code, and every member is reviewed at the head it was batched at).
 * The runs are read from the recorded row at the action's head.
 */
export async function needsWork(db: Db, repo: string, run?: string | null, plans?: PlanSet): Promise<Candidate[]> {
  const out: Candidate[] = [];
  for (const { facts, action } of await planned(db, "approve-ci", { repo, ...(run !== undefined ? { run } : {}), ...(plans ? { plans } : {}) })) {
    const a = await db.get(db.k.selectFrom("ci_run_approvals").select(["runs", "reread", "failed", "cross_repo"]).where("repo", "=", repo).where("number", "=", facts.number).where("head_oid", "=", action.head!));
    if (!a) continue;
    out.push({ number: facts.number, head: action.head!, runs: JSON.parse(a.runs) as WaitingRun[], reread: !!a.reread, failed: JSON.parse(a.failed) as Record<string, number>, crossRepo: !!a.cross_repo });
  }
  return out.sort((a, b) => a.number - b.number);
}

/** Runs of a candidate the step may still try: pull_request runs with fewer than MAX_FAILURES failed attempts. */
export const approvable = (c: Candidate): WaitingRun[] => c.runs.filter((r) => r.event === "pull_request" && (c.failed[r.id] ?? 0) < MAX_FAILURES);

type RunsReply = { workflow_runs?: { id: number; name?: string | null; event?: string; head_sha?: string; head_repository?: { full_name?: string } | null; repository?: { full_name?: string } | null }[] };

/** Reads the runs waiting for approval at a head (one request); only runs of a fork's PR count. */
async function readRuns(gh: Gh, repo: string, head: string): Promise<WaitingRun[]> {
  const r = JSON.parse(await gh(["api", `repos/${slugOf(repo)}/actions/runs?head_sha=${head}&status=action_required&per_page=100`])) as RunsReply;
  return (r.workflow_runs ?? []).filter((w) => w.head_sha === head && w.head_repository?.full_name !== w.repository?.full_name)
    .map((w) => ({ id: w.id, name: w.name ?? "", event: w.event ?? "" }));
}

export type ApproveDeps = {
  /** Reads from GitHub (`gh api` arguments). */
  gh: Gh;
  /** Runs the approving write. */
  exec: Exec;
  /** Most PRs to approve runs for. */
  limit: number;
  log?: (line: string) => void;
  /** Who asked; a command-line run is an operator's. */
  by?: By;
};
export type ApproveResult = { approved: string[]; skipped: string[]; failed: string[] };

/** Approves the waiting runs of every candidate whose PR is still open, not a draft, a fork's, and at the reviewed head. */
export async function approveRuns(db: Db, repo: string, d: ApproveDeps): Promise<ApproveResult> {
  const log = d.log ?? (() => {});
  const w: Writer = { db, login: ACTOR };
  const out: ApproveResult = { approved: [], skipped: [], failed: [] };
  const save = async (c: Candidate, runs: WaitingRun[], approved: number[], failed: Record<string, number>): Promise<void> => {
    const prev = await db.get(db.k.selectFrom("ci_run_approvals").select("approved").where("repo", "=", repo).where("number", "=", c.number).where("head_oid", "=", c.head));
    const all = [...new Set([...(JSON.parse(prev?.approved ?? "[]") as number[]), ...approved])];
    await db.run(db.k.updateTable("ci_run_approvals")
      .set({ runs: JSON.stringify(runs), approved: JSON.stringify(all), reread: 0, failed: JSON.stringify(failed), ...(approved.length ? { approved_at: new Date().toISOString() } : {}) })
      .where("repo", "=", repo).where("number", "=", c.number).where("head_oid", "=", c.head));
  };
  for (const c of await needsWork(db, repo)) {
    if (out.approved.length + out.failed.length >= d.limit) break;
    if (!await ciApprovalOf(db, repo)) throw new Error(`approving CI runs in ${repo} is not approved`);
    if (await mergeHeld(db, repo, c.number)) continue;
    const ref = `${repo}#${c.number}`;
    let runs = c.runs;
    let pr: { state?: string; draft?: boolean; head?: { sha?: string; repo?: { full_name?: string } | null }; base?: { repo?: { full_name?: string } | null } };
    try {
      if (c.reread) runs = await readRuns(d.gh, repo, c.head);
      pr = JSON.parse(await d.gh(["api", `repos/${slugOf(repo)}/pulls/${c.number}`]));
    } catch (e) {
      await jobItem(db, repo, c.number, `CI runs not read: ${(e as Error).message}`, log);
      out.failed.push(ref);
      continue;
    }
    const sameRepo = !pr.head?.repo || pr.head.repo.full_name === pr.base?.repo?.full_name;
    if (pr.state !== "open" || pr.draft !== false || pr.head?.sha !== c.head || sameRepo) {
      await jobItem(db, repo, c.number, "CI runs skipped: closed, a draft, at another head or not from a fork", log);
      if (c.reread) await save(c, c.runs, [], c.failed);
      out.skipped.push(ref);
      continue;
    }
    const todo = runs.filter((r) => r.event === "pull_request" && (c.failed[r.id] ?? 0) < MAX_FAILURES);
    const failed = { ...c.failed };
    const done: number[] = [];
    let stopped: string | null = null;
    for (const run of todo) {
      if (!await ciApprovalOf(db, repo)) throw new Error(`approving CI runs in ${repo} is not approved`);
      const g = await guard(db, { repo, number: c.number }, { kind: "approve-ci", head: c.head }, d.by ?? CLI_BY);
      if ("refused" in g) { stopped = g.refused; break; }
      const r = await ghApproveRun(w, d.exec, g, run.id, { head: c.head, workflow: run.name });
      if (r.code === 0) done.push(run.id);
      else if (REPEATABLE.test(`${r.stderr} ${r.stdout}`)) failed[run.id] = (failed[run.id] ?? 0) + 1;
      else log(`${ref}: run ${run.id}: ${(r.stderr || r.stdout).trim().slice(0, 120)}; will retry`);
    }
    await save(c, runs.filter((r) => !done.includes(r.id)), done, failed);
    if (stopped !== null) { await jobItem(db, repo, c.number, `CI runs not approved: ${stopped}`, log); out.skipped.push(ref); continue; }
    const bad = todo.length - done.length;
    (bad ? out.failed : out.approved).push(ref);
    await jobItem(db, repo, c.number, bad ? `${bad} of ${todo.length} CI runs not approved` : `approved ${todo.length} CI run${todo.length === 1 ? "" : "s"}`, log);
  }
  return out;
}

export const commands = {
  "ci-approval": {
    summary: "ci-approval [--repo R] [on|off]: whether the pipeline may approve waiting CI runs of reviewed outside PRs, per tracked repo; on or off with --repo sets it",
    async run(args: string[]): Promise<void> {
      const db = await openDb();
      const i = args.indexOf("--repo");
      const set = args.find((a) => a === "on" || a === "off");
      if (set) {
        const repo = i >= 0 ? args[i + 1] ?? "" : "";
        if (!repo) throw new Error("ci-approval --repo R on|off");
        console.log(await setCiApproval(db, "cli", repo, set === "on") ? `${repo}: CI run approval ${set}` : `${repo}: CI run approval was already ${set}`);
        return;
      }
      for (const repo of i >= 0 ? [args[i + 1] ?? ""] : await trackedNames(db)) {
        const a = await ciApprovalOf(db, repo);
        console.log(`${repo}\t${a ? `approved by ${a.by} at ${a.at}` : "not approved"}`);
      }
    },
  },
};
