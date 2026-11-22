// The implementations and implementation_rounds tables: one row per attempt to implement a queued issue, and what each
// step of the attempt did. Every write of the tables goes through here.
import type { Db } from "../db/index.ts";
import * as kx from "../db/kx.ts";
import {
  ENDED_STATES, LIVE_STATES,
  type Implementation, type ImplementationPatch, type ImplementationRound, type ImplementationState, type PlanReply, type QuestionAudience, type QuestionRound,
  type RoundKind, type SessionReport,
} from "./types.ts";

type Row = {
  id: number; repo: string; number: number; queued_by: string; state: string; attempt: number; text_hash: string; base_sha: string | null;
  workspace_id: number | null; plan_json: string | null; plan_md: string | null; plan_call_id: number | null; session_call_id: number | null;
  report_json: string | null; diff_hash: string | null; diff_stat: string | null; branch: string | null; pr_number: number | null; pr_url: string | null;
  decision_id: string | null; reason: string | null; created_at: string; updated_at: string;
};

const parse = <T>(s: string | null): T | null => {
  if (s === null) return null;
  try { return JSON.parse(s) as T; } catch { return null; }
};

const ofRow = (r: Row): Implementation => ({
  id: Number(r.id), repo: r.repo, number: Number(r.number), queuedBy: r.queued_by, state: r.state as ImplementationState, attempt: Number(r.attempt),
  textHash: r.text_hash, baseSha: r.base_sha, workspaceId: r.workspace_id === null ? null : Number(r.workspace_id),
  plan: parse<PlanReply>(r.plan_json), planMd: r.plan_md,
  planCallId: r.plan_call_id === null ? null : Number(r.plan_call_id), sessionCallId: r.session_call_id === null ? null : Number(r.session_call_id),
  report: parse<SessionReport>(r.report_json), diffHash: r.diff_hash, diffStat: r.diff_stat, branch: r.branch,
  prNumber: r.pr_number === null ? null : Number(r.pr_number), prUrl: r.pr_url, decisionId: r.decision_id, reason: r.reason,
  createdAt: r.created_at, updatedAt: r.updated_at,
});

/** The columns of a patch. A key left out is not written; null clears the column. */
function columnsOf(p: ImplementationPatch): Partial<Row> {
  const out: Partial<Row> = {};
  if (p.textHash !== undefined) out.text_hash = p.textHash;
  if (p.baseSha !== undefined) out.base_sha = p.baseSha;
  if (p.workspaceId !== undefined) out.workspace_id = p.workspaceId;
  if (p.plan !== undefined) out.plan_json = p.plan === null ? null : JSON.stringify(p.plan);
  if (p.planMd !== undefined) out.plan_md = p.planMd;
  if (p.planCallId !== undefined) out.plan_call_id = p.planCallId;
  if (p.sessionCallId !== undefined) out.session_call_id = p.sessionCallId;
  if (p.report !== undefined) out.report_json = p.report === null ? null : JSON.stringify(p.report);
  if (p.diffHash !== undefined) out.diff_hash = p.diffHash;
  if (p.diffStat !== undefined) out.diff_stat = p.diffStat;
  if (p.branch !== undefined) out.branch = p.branch;
  if (p.prNumber !== undefined) out.pr_number = p.prNumber;
  if (p.prUrl !== undefined) out.pr_url = p.prUrl;
  if (p.decisionId !== undefined) out.decision_id = p.decisionId;
  if (p.reason !== undefined) out.reason = p.reason;
  return out;
}

const ENDED = [...ENDED_STATES];
const select = (db: Db) => db.k.selectFrom("main.implementations").select(kx.cols("implementations"));

export class ImplementationError extends Error {}

export async function implementationById(db: Db, id: number): Promise<Implementation | null> {
  const r = await db.get(select(db).where("id", "=", id));
  return r ? ofRow(r as Row) : null;
}

/** The issue's current attempt: the one row not ended, or null. */
export async function currentImplementation(db: Db, repo: string, number: number): Promise<Implementation | null> {
  const r = await db.get(select(db).where("repo", "=", repo).where("number", "=", number).where("state", "not in", ENDED));
  return r ? ofRow(r as Row) : null;
}

/** Every attempt of the issue, newest first. */
export async function implementationsOf(db: Db, repo: string, number: number): Promise<Implementation[]> {
  return (await db.all(select(db).where("repo", "=", repo).where("number", "=", number).orderBy("attempt", "desc").orderBy("id", "desc"))).map((r) => ofRow(r as Row));
}

/** Starts a new attempt in state `planning`; refuses while the issue has a current one. */
export async function startImplementation(db: Db, o: { repo: string; number: number; queuedBy: string; textHash: string; baseSha?: string | null }, at = new Date().toISOString()): Promise<Implementation> {
  return await db.write(async () => {
    const cur = await currentImplementation(db, o.repo, o.number);
    if (cur) throw new ImplementationError(`${o.repo}#${o.number} has implementation ${cur.id} in state ${cur.state}`);
    const last = await db.get(db.k.selectFrom("main.implementations").select((eb) => eb.fn.max("attempt").as("n")).where("repo", "=", o.repo).where("number", "=", o.number));
    const id = Number((await db.get(db.k.insertInto("main.implementations").values({
      repo: o.repo, number: o.number, queued_by: o.queuedBy, state: "planning", attempt: Number(last?.n ?? 0) + 1, text_hash: o.textHash,
      base_sha: o.baseSha ?? null, created_at: at, updated_at: at,
    }).returning("id")))!.id);
    return (await implementationById(db, id))!;
  });
}

/** Moves the row to `state` and writes the patch's columns. */
export async function setImplementationState(db: Db, id: number, state: ImplementationState, patch: ImplementationPatch = {}, at = new Date().toISOString()): Promise<void> {
  await db.run(db.k.updateTable("main.implementations").set({ ...columnsOf(patch), state, updated_at: at }).where("id", "=", id));
}

/** Writes the patch's columns and leaves the state as it is. */
export async function updateImplementation(db: Db, id: number, patch: ImplementationPatch, at = new Date().toISOString()): Promise<void> {
  await db.run(db.k.updateTable("main.implementations").set({ ...columnsOf(patch), updated_at: at }).where("id", "=", id));
}

export async function addRound(db: Db, implementationId: number, kind: RoundKind, o: { callId?: number | null; result?: unknown } = {}, at = new Date().toISOString()): Promise<number> {
  return Number((await db.get(db.k.insertInto("main.implementation_rounds").values({
    implementation_id: implementationId, kind, call_id: o.callId ?? null, result: o.result === undefined ? null : JSON.stringify(o.result), at,
  }).returning("id")))!.id);
}

/** The attempt's rounds, oldest first. */
export async function roundsOf(db: Db, implementationId: number): Promise<ImplementationRound[]> {
  return (await db.all(db.k.selectFrom("main.implementation_rounds").select(kx.cols("implementation_rounds")).where("implementation_id", "=", implementationId).orderBy("id")))
    .map((r) => ({ id: Number(r.id), implementationId: Number(r.implementation_id), kind: r.kind as RoundKind, callId: r.call_id === null ? null : Number(r.call_id), result: parse<unknown>(r.result), at: r.at }));
}

/** Live implementations (LIVE_STATES) of open issues per repo, as "repo#number" refs. */
export async function liveByRepo(db: Db): Promise<Map<string, Set<string>>> {
  const out = new Map<string, Set<string>>();
  for (const r of await db.all(db.k.selectFrom("main.implementations as m")
    .innerJoin("main.items as i", (j) => j.onRef("i.repo", "=", "m.repo").onRef("i.number", "=", "m.number").on("i.state", "=", "OPEN"))
    .select(["m.repo", "m.number"]).where("m.state", "in", [...LIVE_STATES]))) {
    (out.get(r.repo) ?? out.set(r.repo, new Set()).get(r.repo)!).add(`${r.repo}#${r.number}`);
  }
  return out;
}

/** An attempt as the planner reads it: with its latest question round (when it is `asked`), or null. */
export type LatestAttempt = Implementation & { question: { at: string; audience: QuestionAudience | null } | null };

/** Per issue ("repo#number"): its current attempt, else its newest ended one. */
export async function latestAttempts(db: Db, repo?: string): Promise<Map<string, LatestAttempt>> {
  const out = new Map<string, LatestAttempt>();
  const rows = (await db.all(select(db).$if(!!repo, (q) => q.where("repo", "=", repo!)).orderBy("attempt").orderBy("id"))).map((r) => ofRow(r as Row));
  for (const r of rows) {
    const k = `${r.repo}#${r.number}`, had = out.get(k);
    if (!had || ENDED_STATES.has(had.state)) out.set(k, { ...r, question: null });
  }
  const asked = new Map([...out.values()].filter((r) => r.state === "asked").map((r) => [r.id, r]));
  if (asked.size) {
    for (const q of await db.all(db.k.selectFrom("main.implementation_rounds").select(["implementation_id", "at", "result"])
      .where("kind", "=", "question").where("implementation_id", "in", [...asked.keys()]).orderBy("id"))) {
      const audience = parse<Partial<QuestionRound>>(q.result)?.audience;
      asked.get(Number(q.implementation_id))!.question = { at: q.at, audience: audience === "author" || audience === "maintainer" ? audience : null };
    }
  }
  return out;
}
