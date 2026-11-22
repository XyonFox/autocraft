// What an implement job needs before it starts, checked again at the run: the repo's issue implementation approval, its
// Implement switch, and a fast_track row on the issue by someone who settles on the repo (src/settlers.ts).
import { fixIssueApprovalOf } from "../fixissue/approval.ts";
import { featureOn } from "../build/features.ts";
import { settlesNow } from "../settlers.ts";
import type { Db } from "../db/index.ts";

/** The login that queued the issue (its fast_track row), or null. */
export async function queuerOf(db: Db, repo: string, number: number): Promise<string | null> {
  return (await db.get(db.k.selectFrom("main.fast_track").select("added_by").where("repo", "=", repo).where("number", "=", number)))?.added_by ?? null;
}

/** Why an implement job of the issue may not run, or null. */
export async function implementRefusal(db: Db, repo: string, number: number): Promise<string | null> {
  if (!await fixIssueApprovalOf(db, repo)) return `issue implementations in ${repo} are not approved`;
  if (!await featureOn(db, "implement", repo)) return `Implement is off for ${repo}`;
  const by = await queuerOf(db, repo, number);
  if (!by) return `${repo}#${number} is not queued`;
  if (!await settlesNow(db, by, [repo])) return `${by}, who queued ${repo}#${number}, does not settle on ${repo}`;
  return null;
}
