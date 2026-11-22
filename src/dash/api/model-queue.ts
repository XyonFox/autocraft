// The model call queue of the user-local inference mode (GET /api/model-queue) and picks ("Run on my Task Runner": POST
// /api/model-queue/<id>/pick and /unpick). Viewers see the calls of repos they can see, with no picker logins, errors or
// manual logins, and never repo-less calls; maintainers and operators see those; operators also end any user's pick.
import type { ModelQueue, ModelQueueCall } from "../../../web/src/api/types.ts";
import { pickerLogins } from "../../remote/calls.ts";
import { manualOnly } from "../../remote/manual-only.ts";
import { pickCall, pickerState, pickRefusal, unpickCall } from "../../remote/picks.ts";
import { queueView } from "../../remote/queue-view.ts";
import { HttpError } from "../server/util.ts";
import type { Req } from "../types.ts";
import { withShown } from "./shown.ts";
import { canSeeInternals } from "./staff.ts";
import { stepTitle } from "./trace.ts";
import { hiddenRepos } from "./visible.ts";
import type { Db } from "../../db/index.ts";

const ESTIMATE_RUNS = 60;
const ESTIMATE_TTL_MS = 5 * 60_000;
const estimates = new WeakMap<Db, Map<string, { at: number; usd: number | null }>>();

/** Median cost of the prompt's last ESTIMATE_RUNS finished runs, cached ESTIMATE_TTL_MS; null without history. */
async function estimate(db: Db, promptId: string, nowMs: number): Promise<number | null> {
  let m = estimates.get(db);
  if (!m) estimates.set(db, (m = new Map()));
  const hit = m.get(promptId);
  if (hit && nowMs - hit.at < ESTIMATE_TTL_MS) return hit.usd;
  const xs = (await db.all(db.k.selectFrom("main.agent_runs").select("cost_usd").where("prompt_id", "=", promptId).where("state", "=", "done").where("cost_usd", "is not", null)
    .$narrowType<{ cost_usd: number }>().orderBy("id", "desc").limit(ESTIMATE_RUNS)))
    .map((r) => Number(r.cost_usd)).sort((a, b) => a - b);
  const usd = xs.length ? xs[Math.floor(xs.length / 2)]! : null;
  m.set(promptId, { at: nowMs, usd });
  return usd;
}

export async function get(db: Db, req: Req): Promise<ModelQueue> {
  const nowMs = Date.now();
  const staff = canSeeInternals(req);
  const hidden = await hiddenRepos(db, req);
  // Operators see every repo; others only repos on their allow-list.
  const visible = req.user.role === "operator" ? null : new Set(hidden.visible ?? []);
  const { calls, runners } = await queueView(db, nowMs, staff);
  const state = await pickerState(db, req.user, nowMs);
  const fresh = await pickerLogins(db, nowMs);
  const memo = new Map<string, Promise<boolean>>();
  const user = { ...req.user, canWrite: (repo: string) => { let p = memo.get(repo); if (!p) memo.set(repo, (p = req.user.canWrite(repo).catch(() => false))); return p; } };
  const out: ModelQueueCall[] = [];
  for (const c of calls) {
    if (visible && c.repos.some((r) => !visible.has(r))) continue;
    if (!staff && !c.repos.length) continue;
    const pickable = state.canPick && (await pickRefusal(state, user, { id: c.id, tier: c.tier, promptId: c.promptId, parentId: c.followUp ? c.id : null, repos: c.repos, authors: c.authors, seenAt: c.seenAt, pickedBy: c.pickedBy, pickUntil: c.pickUntil, forLogin: c.forLogin }, nowMs, async (l) => fresh.has(l))) === null;
    out.push({
      id: c.id, items: await withShown(db, c.refs), kind: stepTitle(c.step), tier: c.tier, model: c.model, estCostUsd: await estimate(db, c.promptId, nowMs), waitedMs: c.waitedMs, why: c.why,
      picked: c.pickedBy === null ? null : staff ? { login: c.pickedBy } : {}, pickable, gating: c.gating, followUp: c.followUp,
      ...(staff && c.lastError && { lastError: c.lastError }),
    });
  }
  const manual = runners.filter((r) => r.manual);
  return {
    calls: out,
    runners: { autopilot: runners.length - manual.length, manual: manual.length, ...(staff && { manualLogins: [...new Set(manual.map((r) => r.login))].sort() }) },
    me: { canPick: state.canPick, manualOnly: await manualOnly(db, req.user.login), picks: state.picks, pickMax: state.pickMax },
  };
}

function callId(req: Req): number {
  const id = Number(req.params.id);
  if (!Number.isInteger(id) || id <= 0) throw new HttpError(404, "no such model call");
  return id;
}

/** `via` names the MCP server in the audit row when an agent picked. */
export async function pick(db: Db, req: Req, via?: "mcp"): Promise<ModelQueue> {
  await pickCall(db, req.user, callId(req), Date.now(), via);
  return get(db, req);
}

export async function unpick(db: Db, req: Req, via?: "mcp"): Promise<ModelQueue> {
  await unpickCall(db, req.user, callId(req), via);
  return get(db, req);
}
