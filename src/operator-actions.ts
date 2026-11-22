// Operator releases for items that stop waiting for a person: clear a review hold at the current head,
// plan Verify again after it failed or errored, try a skipped or failed issue fix again, try the integration batch
// again after a single-PR batch failed. Each changes
// only local tables (the pipeline then picks the item up as usual) and is audited; none writes to GitHub.
import { FIX_RETIRED_REASON } from "./db/fix-retire.ts";
import { audit } from "./dash/audit.ts";
import { batchStates, updateBatch } from "./merge/batch-store.ts";
import { headOf } from "./review/candidates.ts";
import type { Db } from "./db/index.ts";
import { openDb } from "./db/open.ts";

export class ActionError extends Error {
  status: number;
  constructor(status: number, message: string) {
    super(message);
    this.status = status;
  }
}

export type ItemActions = { reviewAgain: boolean; verifyAgain: boolean; fixAgain: boolean; batchAgain: boolean };

type ItemRow = { kind: string; state: string; head_oid: string | null; updated_at: string | null; c_head: string | null; c_at: string | null };

async function openItem(db: Db, repo: string, n: number): Promise<ItemRow & { head: string | null }> {
  const r = await db.get(db.k.selectFrom("items as i")
    .leftJoin("pr_checks as c", (j) => j.onRef("c.repo", "=", "i.repo").onRef("c.number", "=", "i.number"))
    .select(["i.kind", "i.state", "i.head_oid", "i.updated_at", "c.head_oid as c_head", "c.fetched_at as c_at"])
    .where("i.repo", "=", repo).where("i.number", "=", n));
  if (!r) throw new ActionError(404, `no item ${repo}#${n} in the mirror`);
  if (r.state !== "OPEN") throw new ActionError(409, `${repo}#${n} is not open`);
  return { ...r, head: headOf(r) };
}

const heldReview = async (db: Db, repo: string, n: number, head: string | null) =>
  head ? await db.get(db.k.selectFrom("final_reviews").select("reason").where("repo", "=", repo).where("number", "=", n).where("head_oid", "=", head).where("verdict", "=", "hold")) : undefined;
const failedVerify = async (db: Db, repo: string, n: number, head: string | null) =>
  head ? await db.get(db.k.selectFrom("verifications").select(["state", "summary"]).where("repo", "=", repo).where("number", "=", n).where("head_sha", "=", head).where("state", "in", ["failed", "error"])) : undefined;
export const stoppedFix = async (db: Db, repo: string, n: number) =>
  await db.get(db.k.selectFrom("fix_attempts").select(["state", "round", "retries", "push_tries", "reason", "summary"])
    .where("repo", "=", repo).where("number", "=", n).where("state", "in", ["skipped", "failed"]).where("pr_url", "is", null)
    // An attempt the Implement migration ended is not the fix step's to retry.
    .where((eb) => eb.or([eb("reason", "is", null), eb("reason", "<>", FIX_RETIRED_REASON)])));

const failedBatch = async (db: Db, repo: string, n: number, head: string | null) =>
  head && (await batchStates(db, repo)).get(`${repo}#${n}@${head}`) === "batch-failed"
    ? await db.get(db.k.selectFrom("main.merge_batches as b").innerJoin("main.merge_batch_prs as p", "p.batch_id", "b.id")
      .select(["b.id", "b.summary"]).where("b.repo", "=", repo).where("b.state", "=", "failed").where("p.number", "=", n).where("p.head_oid", "=", head)
      .orderBy("b.id", "desc").limit(1))
    : undefined;

/** Which releases apply to the item now; the Item page shows only these. */
export async function actionsOf(db: Db, repo: string, n: number): Promise<ItemActions> {
  const r = await db.get(db.k.selectFrom("items as i")
    .leftJoin("pr_checks as c", (j) => j.onRef("c.repo", "=", "i.repo").onRef("c.number", "=", "i.number"))
    .select(["i.kind", "i.state", "i.head_oid", "i.updated_at", "c.head_oid as c_head", "c.fetched_at as c_at"])
    .where("i.repo", "=", repo).where("i.number", "=", n));
  if (!r || r.state !== "OPEN") return { reviewAgain: false, verifyAgain: false, fixAgain: false, batchAgain: false };
  const head = headOf(r);
  return r.kind === "pr"
    ? { reviewAgain: !!await heldReview(db, repo, n, head), verifyAgain: !!await failedVerify(db, repo, n, head), fixAgain: false, batchAgain: !!await failedBatch(db, repo, n, head) }
    : { reviewAgain: false, verifyAgain: false, fixAgain: !!await stoppedFix(db, repo, n), batchAgain: false };
}

/** Removes the hold review at the PR's current head so the Review stage reviews that head again. */
export async function reviewAgain(db: Db, by: string, repo: string, n: number): Promise<void> {
  const it = await openItem(db, repo, n);
  if (it.kind !== "pr") throw new ActionError(409, `${repo}#${n} is not a pull request`);
  const held = await heldReview(db, repo, n, it.head);
  if (!held) throw new ActionError(409, `${repo}#${n} has no review hold at its current head`);
  await db.run(db.k.deleteFrom("opus_reviews").where("repo", "=", repo).where("number", "=", n).where("head_oid", "=", it.head!));
  await audit(db, by, "review-again", `${repo}#${n}`, { head: it.head, reason: held.reason });
}

/** Marks the failed or errored verification of the current head stale so Verify plans it again. */
export async function verifyAgain(db: Db, by: string, repo: string, n: number): Promise<void> {
  const it = await openItem(db, repo, n);
  if (it.kind !== "pr") throw new ActionError(409, `${repo}#${n} is not a pull request`);
  const v = await failedVerify(db, repo, n, it.head);
  if (!v) throw new ActionError(409, `${repo}#${n} has no failed or errored verification at its current head`);
  await db.run(db.k.updateTable("verifications").set({ state: "stale" }).where("repo", "=", repo).where("number", "=", n).where("head_sha", "=", it.head!));
  await audit(db, by, "verify-again", `${repo}#${n}`, { head: it.head, was: v.state, summary: v.summary });
}

/** Forgets a skipped or failed fix attempt (and so its round and retry counts) so the issue is eligible for Fix once more. */
export async function fixAgain(db: Db, by: string, repo: string, n: number): Promise<void> {
  const it = await openItem(db, repo, n);
  if (it.kind !== "issue") throw new ActionError(409, `${repo}#${n} is not an issue`);
  const a = await stoppedFix(db, repo, n);
  if (!a) throw new ActionError(409, `${repo}#${n} has no skipped or failed fix attempt`);
  await db.run(db.k.deleteFrom("fix_attempts").where("repo", "=", repo).where("number", "=", n));
  await audit(db, by, "fix-again", `${repo}#${n}`, { was: a.state, round: a.round, reason: a.reason ?? a.summary });
}

/** Marks the failed batch that holds the PR's current head superseded, keeping its summary, so the merge queue considers the PR again. */
export async function batchAgain(db: Db, by: string, repo: string, n: number): Promise<void> {
  const it = await openItem(db, repo, n);
  if (it.kind !== "pr") throw new ActionError(409, `${repo}#${n} is not a pull request`);
  const b = await failedBatch(db, repo, n, it.head);
  if (!b) throw new ActionError(409, `${repo}#${n} is not held by a failed batch at its current head`);
  await updateBatch(db, b.id, new Date().toISOString(), { state: "superseded" });
  await audit(db, by, "batch-again", `${repo}#${n}`, { head: it.head, batch: b.id, summary: b.summary });
}

export const commands = {
  item: {
    summary: "item batch-again REPO NUMBER --by LOGIN: release a PR held by a failed integration batch | item backlog REPO NUMBER --by LOGIN: put an open item on the Backlog as LOGIN, who must settle on the repo",
    async run(args: string[]): Promise<void> {
      const [sub, repo, num] = args;
      const by = args[args.indexOf("--by") + 1];
      if ((sub !== "batch-again" && sub !== "backlog") || !repo || !/^\d+$/.test(num ?? "") || !args.includes("--by") || !by) {
        console.error("usage: autocraft-server item batch-again|backlog <repo> <number> --by <login>");
        process.exit(2);
      }
      const db = await openDb();
      try {
        if (sub === "backlog") {
          const [{ settlesNow }, { backlogItem }] = await Promise.all([import("./settlers.ts"), import("./dash/api/decisions.ts")]);
          if (!await settlesNow(db, by, [repo])) throw new ActionError(403, `${by} does not settle decisions on ${repo}`);
          const d = await backlogItem(db, repo, Number(num), by, "cli");
          console.log(`${repo}#${num}: on the Backlog (decision ${d.id})`);
          return;
        }
        await batchAgain(db, by, repo, Number(num));
        console.log(`${repo}#${num}: released from its failed batch`);
      } catch (e) {
        if (!(e instanceof ActionError) && typeof (e as { status?: unknown }).status !== "number") throw e;
        console.error((e as Error).message);
        process.exit(1);
      }
    },
  },
};
