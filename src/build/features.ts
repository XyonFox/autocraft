// Operator switches for the build features. Stored in settings as `build_feature:<f>` and, for repo-scoped
// features, `build_feature:<f>:<repo>`; a missing row means off.
import { audit } from "../dash/audit.ts";
import { TERMINAL, cancelTask } from "./tasks.ts";
import { setting } from "../settings-read.ts";
import { mapSeq } from "../async-seq.ts";
import type { Db } from "../db/index.ts";
import { excluded, like, likeEscape } from "../db/kx.ts";

export const BUILD_FEATURES = ["screenshots", "push-tests", "batch-build", "scan-tests", "nightly", "pr-builds", "implement"] as const;
export type BuildFeature = (typeof BUILD_FEATURES)[number];

/** Repo-scoped features need the global switch and the repo's own switch. */
export const REPO_SCOPED: Record<BuildFeature, boolean> = {
  screenshots: true, "push-tests": true, "batch-build": true, "scan-tests": false, nightly: true, "pr-builds": true, implement: true,
};

export const isFeature = (s: unknown): s is BuildFeature => typeof s === "string" && (BUILD_FEATURES as readonly string[]).includes(s);

const keyOf = (f: BuildFeature, repo?: string): string => (repo ? `build_feature:${f}:${repo}` : `build_feature:${f}`);

const read = async (db: Db, key: string): Promise<boolean> =>
  await setting(db, key) === "on";

/** Without `repo` a repo-scoped feature reports its global switch; callers that loop repos must pass the repo. */
export async function featureOn(db: Db, f: BuildFeature, repo?: string): Promise<boolean> {
  if (!await read(db, keyOf(f))) return false;
  if (!REPO_SCOPED[f] || repo === undefined) return true;
  return await read(db, keyOf(f, repo));
}

export async function setFeature(db: Db, f: BuildFeature, on: boolean, by: string, repo?: string): Promise<void> {
  const scoped = REPO_SCOPED[f] ? repo : undefined;
  await db.run(db.k.insertInto("settings").values({ key: keyOf(f, scoped), value: on ? "on" : "off" }).onConflict((oc) => oc.column("key").doUpdateSet(excluded(["value"]))));
  await audit(db, by, "build-feature", f, { feature: f, on, repo: scoped ?? null });
  if (!on) await stopQueuedTasks(db, f, by, scoped);
}

/** Build tasks of the features whose untrusted work is queued by the pipeline; a switch turned off stops them. */
const FEATURE_STEP: Partial<Record<BuildFeature, string>> = { "pr-builds": "pr-build", nightly: "nightly", screenshots: "screenshot" };

/** Cancels the queued and running tasks of a feature that was switched off (for one repo, or every repo), and marks the PR builds stale. */
async function stopQueuedTasks(db: Db, f: BuildFeature, by: string, repo?: string): Promise<void> {
  const step = FEATURE_STEP[f];
  if (!step) return;
  const rows = await db.all(db.k.selectFrom("build_tasks").select("id").where("step", "=", step).where("state", "not in", TERMINAL)
    .$if(!!repo, (q) => q.where("repo", "=", repo!)));
  for (const r of rows) await cancelTask(db, r.id, by);
  const now = new Date().toISOString();
  if (f === "screenshots") {
    await db.run(db.k.updateTable("main.screenshot_runs").set({ state: "skipped", reason: "screenshots switched off", finished_at: now })
      .where("state", "in", ["planned", "running"]).$if(!!repo, (q) => q.where("repo", "=", repo!)));
  }
  if (f === "pr-builds") {
    await db.run(db.k.updateTable("main.pr_builds").set({ state: "stale", finished_at: now }).where("state", "=", "queued")
      .$if(!!repo, (q) => q.where("repo", "=", repo!)));
  }
}

export async function featureStates(db: Db): Promise<{ feature: BuildFeature; on: boolean; repos: string[] }[]> {
  return await mapSeq(BUILD_FEATURES, async (feature) => {
    const repos = REPO_SCOPED[feature]
      ? (await db.all(db.k.selectFrom("settings").select("key")
        .where((eb) => like(db, "key", eb.val(`${likeEscape(`build_feature:${feature}:`)}%`), "\\")).where("value", "=", "on")))
        .map((r) => r.key.slice(`build_feature:${feature}:`.length)).sort()
      : [];
    return { feature, on: await read(db, keyOf(feature)), repos };
  });
}
