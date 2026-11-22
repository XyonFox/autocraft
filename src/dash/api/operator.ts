// Releases (operator, or maintainer with write) on the Item page: review again, verify again, try the fix again.
// They change local tables only; the pipeline picks the item up on its next pass.
import { ActionError, batchAgain, fixAgain, reviewAgain, stoppedFix, verifyAgain } from "../../operator-actions.ts";
import { ask, getDecision } from "../../decisions.ts";
import { SCOPE_OPTIONS } from "../../decide/decision.ts";
import { applyAnswer } from "./decisions.ts";
import { cancelPreapproval, retryPreapproval } from "../../preapproval/index.ts";
import { HttpError } from "../server/util.ts";
import type { Req } from "../types.ts";
import { needVisible } from "./visible.ts";
import type { Db } from "../../db/index.ts";
import { mayRunRepo } from "../repo-access.ts";
import { requireSettle } from "../../settlers.ts";

async function run(db: Db, req: Req, f: (db: Db, by: string, repo: string, n: number) => Promise<void>): Promise<{ ok: true }> {
  const repo = req.params.repo!;
  if (!(await mayRunRepo(req.user, repo))) throw new HttpError(403, "no write access to this repo");
  const n = Number(req.params.n);
  if (!Number.isInteger(n)) throw new HttpError(400, "not an item number");
  await needVisible(db, req, repo);
  try {
    await f(db, req.user.login, repo, n);
  } catch (e) {
    if (e instanceof ActionError) throw new HttpError(e.status, e.message);
    throw e;
  }
  return { ok: true };
}

/** Operator: cancels the PR's pre-approved merge while it is before the merge queue's merge. */
export async function cancelItemPreapproval(db: Db, req: Req, via?: string) {
  const repo = req.params.repo!;
  if (!(await mayRunRepo(req.user, repo))) throw new HttpError(403, "no write access to this repo");
  const n = Number(req.params.n);
  if (!Number.isInteger(n)) throw new HttpError(400, "not an item number");
  await needVisible(db, req, repo);
  try {
    return await cancelPreapproval(db, req.user.login, repo, n, undefined, via);
  } catch (e) {
    throw new HttpError(409, (e as Error).message);
  }
}

/** Operator: queues the failed pre-approved merge again with the same instructions. */
export async function retryItemPreapproval(db: Db, req: Req) {
  const repo = req.params.repo!;
  if (!(await mayRunRepo(req.user, repo))) throw new HttpError(403, "no write access to this repo");
  const n = Number(req.params.n);
  if (!Number.isInteger(n)) throw new HttpError(400, "not an item number");
  await needVisible(db, req, repo);
  try {
    return await retryPreapproval(db, req.user.login, repo, n);
  } catch (e) {
    throw new HttpError(409, (e as Error).message);
  }
}

/**
 * Operator: puts an issue with a stopped fix in the Backlog, or closes it. Both go through a scope decision the operator answers
 * at once, so the Backlog hold and the close (comment, act limit, approvals) follow the same paths as any decision.
 */
export async function stopFixItem(db: Db, req: Req): Promise<{ ok: true }> {
  const repo = req.params.repo!;
  await requireSettle(db, req.user, [repo], "decides an issue");
  const n = Number(req.params.n);
  if (!Number.isInteger(n)) throw new HttpError(400, "not an item number");
  await needVisible(db, req, repo);
  const answer = (req.json as { answer?: unknown } | undefined)?.answer;
  if (answer !== "close" && answer !== "backlog") throw new HttpError(400, "answer must be close or backlog");
  const it = await db.get(db.k.selectFrom("items").select(["kind", "state", "title"]).where("repo", "=", repo).where("number", "=", n));
  if (!it || it.kind !== "issue" || it.state !== "OPEN") throw new HttpError(409, `${repo}#${n} is not an open issue`);
  const a = await stoppedFix(db, repo, n);
  if (!a) throw new HttpError(409, `${repo}#${n} has no skipped or failed fix attempt`);
  const id = `${repo}#${n}:scope:fix-stopped`;
  if ((await getDecision(db, id))?.answer) throw new HttpError(409, `${repo}#${n} was already decided this way`);
  await ask(db, [{
    id, kind: "scope", repo, number: n, question: "What happens to this issue?", context: `The fix attempt ${a.state === "failed" ? "failed" : "was skipped"}${a.reason ?? a.summary ? `: ${a.reason ?? a.summary}` : ""}`, facts: [],
    options: [{ ...SCOPE_OPTIONS.close }, { ...SCOPE_OPTIONS.take }], recommended: null, asked_by: req.user.login, asked_at: new Date().toISOString(),
  }]);
  await applyAnswer(db, (await getDecision(db, id))!, req.user.login, { answer }, "dashboard");
  return { ok: true };
}

export const reviewItemAgain = async (db: Db, req: Req) => run(db, req, reviewAgain);
export const verifyItemAgain = async (db: Db, req: Req) => run(db, req, verifyAgain);
export const fixItemAgain = async (db: Db, req: Req) => run(db, req, fixAgain);
export const batchItemAgain = async (db: Db, req: Req) => run(db, req, batchAgain);
