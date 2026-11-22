// The workspace half of the pod launcher: one pass every PASS_MS, per kubernetes environment, after the build-task pass.
//
//   requested  → starting  volume and pod created when a workspace slot is free
//   starting   → seeding   the pod runs and its container is ready; the node is recorded
//   seeding    → ready     `git archive <base_sha>` from AutoCraft's clone streamed into `ws untar`, which commits it
//   ready      → parked    no call for IDLE_PARK_MS, no bound remote call queued or leased and no background command
//                          running: checkpoint, pod deleted, volume kept (PARKED_VOLUME_MS, then deleted)
//   parked     → requested its bound remote call is queued or leased, or a session asks for it (resumeWorkspace). The new
//                          pod uses the kept volume, which seeding only checks (never seeds again); without one, seeding
//                          fills a new volume from the base and applies the checkpoint. A resume onto a volume whose node
//                          is gone drops the volume and rebuilds, unless the last checkpoint failed.
//   resuming   → parked    a resume that cannot start or seed goes back to parked with its kept volume and checkpoint, up to
//                          RESUME_TRIES times; then it stays parked with the reason in its error.
// A parked volume whose last checkpoint failed holds work no checkpoint has: it is kept past PARKED_VOLUME_MS.
//   any live   → failed    the pod could not start or seed, or passed its time limit; pod and volume deleted
//   ended                  set by whoever is done with it; the pass deletes pod and volume
// While ready, a checkpoint (`git diff --binary` against the base commit) is stored every CHECKPOINT_EVERY_MS. Seeding,
// checkpoints and parking run in the background, at most one per workspace, so a slow exec never holds up the pass.
// Stateless between passes apart from what is in flight: after a restart a `seeding` row seeds again (from an empty tree).
// A pod of a resumed workspace has a new name (workspaceName with an incarnation), and so has a volume it rebuilds on.
import { spawn } from "node:child_process";
import type { Db } from "../db/index.ts";
import * as kx from "../db/kx.ts";
import { exec, fixupDir, GIT_ENV, gitDirEnv, Git, HARDENING } from "../fixup/git.ts";
import { ownerOf } from "../owners.ts";
import { KubeError, type KPod, type KubeApi } from "./kube-api.ts";
import type { LauncherDeps } from "./launcher.ts";
import { CREATE_TRIES, LIST_GRACE_MS, PENDING_LIMIT_MS, PULL_LIMIT_MS, PULL_REASONS, settingsOf, STUCK_MS, type KubeSettings } from "./pods.ts";
import { workspaceEnvSelector, workspaceName, workspacePodSpec, workspacePvcSpec, workspaceSelector, WS_CONTAINER } from "./workspace-pods.ts";
import { describeHeader, execInWorkspace, execWs, getCheckpoint, getWorkspace, limitsOf, LIVE_STATES, moveWorkspace, POD_STATES, RESUME_TRIES, resumeWorkspace, WORKSPACE_COLUMNS, type WorkspaceRow, type WorkspaceStateName, type WsCallResult } from "./workspaces.ts";

export const SEED_LIMIT_MS = 10 * 60_000;
export const IDLE_PARK_MS = 30 * 60_000;
export const CHECKPOINT_EVERY_MS = 10 * 60_000;
/** A parked workspace's volume is deleted after this long; the checkpoint stays, and a resume rebuilds from base plus checkpoint. */
export const PARKED_VOLUME_MS = 60 * 60_000;
/** A parked workspace whose volume is gone ends this long after it parked; the checkpoint stays on the row. */
export const PARKED_END_MS = 24 * 3600_000;
/** A resume onto a kept volume whose pod is still unscheduled after this long asks whether the volume's node is gone. */
export const RESUME_PENDING_MS = 3 * 60_000;
/** A node not ready for this long counts as gone, with the volumes it holds. */
export const NODE_LOST_MS = 5 * 60_000;
/** How often seeding checks a kept volume again after an unclear answer. */
const KEPT_RETRY_MS = 10_000;
/** The longest a workspace takes from requested (or parked) to ready before the launcher fails it; a minute more covers its pass interval. */
export const READY_LIMIT_MS = PENDING_LIMIT_MS + SEED_LIMIT_MS + 60_000;
/** While the bound remote call is leased, the idle time is counted from at most this long ago, so it parks only after the lease ends. */
const LEASE_TOUCH_MS = 60_000;
/** A bound call back in the queue (its Task Runner was lost) gets a fresh checkpoint for the next session's prompt, tried this often. */
const REQUEUE_CHECKPOINT_MS = 60_000;
export const MAX_SOURCE_BYTES = 512 * 1024 ** 2;
export const MAX_CHECKPOINT_BYTES = 5 * 1024 ** 2;
/** The toolchain image workspace pods run. */
export const WORKSPACE_TOOLCHAIN = "rust-stable";
const CREATE_BACKOFF_MS = 60_000;
const CHECKPOINT_FILE = "/work/.ws/checkpoint.diff";
const RESUME_FILE = "/work/.ws/resume.diff";
/** Grace past a background command's own limit before the launcher stops counting it as running (the helper's kill -k 5). */
const BACKGROUND_GRACE_MS = 30_000;
// The diff is cut one byte past the cap, so an oversized one shows as such; head stopping early makes git's status 141.
// It works on a copy of the index, so the session's index and its lock are never touched.
const CHECKPOINT_SH = `set -e
GIT_INDEX_FILE=/work/.ws/ckpt.index
export GIT_INDEX_FILE
rm -f "$GIT_INDEX_FILE" "$GIT_INDEX_FILE.lock"
cp .git/index "$GIT_INDEX_FILE" 2>/dev/null || :
git add -A -N
{ git diff --binary --no-ext-diff --no-textconv "$1"; echo $? >/work/.ws/checkpoint.rc; } | head -c ${MAX_CHECKPOINT_BYTES + 1} >/work/.ws/checkpoint.tmp
rc=$(cat /work/.ws/checkpoint.rc)
[ "$rc" = 0 ] || [ "$(wc -c </work/.ws/checkpoint.tmp)" -gt ${MAX_CHECKPOINT_BYTES} ] || exit "$rc"
mv /work/.ws/checkpoint.tmp ${CHECKPOINT_FILE}`;

/** What is in flight between passes. */
export class WorkspaceState {
  busy = new Map<number, Promise<void>>();
  unknownSince = new Map<number, number>();
  /** When each workspace's last checkpoint was tried, so a failing one is retried on the same schedule, not every pass. */
  checkpointTried = new Map<number, number>();
  backoff = new Map<string, number>();
  counts = new Map<string, string>();
  /** Resolves when nothing runs in the background (tests). */
  async idle(): Promise<void> {
    while (this.busy.size) await Promise.allSettled([...this.busy.values()]);
  }
}

/**
 * Fails the calls still marked running past their deadline (their time limit plus CALL_GRACE_MS): the process that ran them
 * died (a runner or dashboard pod replaced mid-call), so nothing will settle them. Rows without a deadline are the
 * session relay's own, taken over when its job resumes. Returns the rows failed.
 */
export async function sweepStuckCalls(db: Db, now: number): Promise<number> {
  const at = new Date(now).toISOString();
  const rows = await db.all(db.k.updateTable("workspace_calls")
    .set({ state: "failed", result: JSON.stringify({ text: "failed: the process running this call ended before it finished, so its outcome is unknown; check the workspace and call it again", isError: true }), finished_at: at })
    .where("state", "=", "running").where("deadline_at", "is not", null).where("deadline_at", "<", at).returning("id"));
  return rows.length;
}

type EnvRow = { id: string; enabled: number; settings: string | null };
type Ctx = { d: LauncherDeps; state: WorkspaceState; api: KubeApi; env: EnvRow; s: KubeSettings; now: () => number };

const clean = (s: string | undefined): string => (s ?? "").replace(/[\u0000-\u001f\u007f]+/g, " ").trim().slice(0, 300);
const errText = (e: unknown): string => clean(e instanceof Error ? e.message : String(e));

export async function workspacePass(d: LauncherDeps, state: WorkspaceState): Promise<void> {
  try {
    // Loaded here: the session module imports this one.
    const { sweepSessions } = await import("./workspace-session.ts");
    await sweepSessions(d.db, d.log);
  } catch (e) {
    d.log(`workspace launcher: session sweep: ${(e as Error).stack ?? e}`);
  }
  try {
    await sweepStuckCalls(d.db, (d.now ?? Date.now)());
  } catch (e) {
    d.log(`workspace launcher: call sweep: ${(e as Error).stack ?? e}`);
  }
  const api = d.api;
  if (!api) return;
  const envs = await d.db.all(d.db.k.selectFrom("build_envs").select(["id", "enabled", "settings"]).where("kind", "=", "kubernetes").orderBy("id"));
  for (const env of envs) {
    try {
      await envPass({ d, state, api, env, s: settingsOf(env.settings), now: d.now ?? Date.now });
    } catch (e) {
      d.log(`workspace launcher: ${env.id}: ${(e as Error).stack ?? e}`);
    }
  }
}

const liveRows = async (c: Ctx) =>
  await c.d.db.all(c.d.db.k.selectFrom("workspaces").select(WORKSPACE_COLUMNS).where("env_id", "=", c.env.id).where("state", "in", LIVE_STATES).orderBy("id"));

async function envPass(c: Ctx): Promise<void> {
  const { api, s } = c;
  const ns = s.namespace;
  const [pods, pvcs] = await Promise.all([api.listPods(ns, workspaceEnvSelector(c.env.id)), api.listPvcs(ns, workspaceEnvSelector(c.env.id))]);
  for (const w of await liveRows(c)) if (!c.state.busy.has(w.id)) await advance(c, w, pods);

  const back = c.state.backoff.get(c.env.id);
  if (c.env.enabled && !(back && back > c.now())) {
    const rows = await liveRows(c);
    let active = rows.filter((w) => POD_STATES.includes(w.state as WorkspaceStateName)).length;
    for (const w of rows) {
      if (w.state !== "requested") continue;
      if (active >= s.workspaceSlots) break;
      if (!await start(c, w)) break;
      active++;
    }
  }

  // Pods and volumes no live row owns: ended and failed workspaces, a parked one's old pod, a create that failed late.
  const owned = await liveRows(c);
  const podNames = new Set(owned.filter((w) => POD_STATES.includes(w.state as WorkspaceStateName)).map((w) => w.pod));
  const pvcNames = new Set(owned.map((w) => w.pvc));
  for (const p of pods) if (!podNames.has(p.metadata.name) && !p.metadata.deletionTimestamp) await api.deletePod(ns, p.metadata.name).catch((e: Error) => c.d.log(`workspace launcher: delete pod ${p.metadata.name}: ${e.message}`));
  for (const v of pvcs) if (!pvcNames.has(v.metadata.name)) await api.deletePvc(ns, v.metadata.name).catch((e: Error) => c.d.log(`workspace launcher: delete volume ${v.metadata.name}: ${e.message}`));
  for (const m of [c.state.unknownSince, c.state.checkpointTried]) for (const id of m.keys()) if (!owned.some((w) => w.id === id)) m.delete(id);

  const n = (st: string) => owned.filter((w) => w.state === st).length;
  const line = `${n("starting") + n("seeding") + n("ready")}/${s.workspaceSlots} running (${n("ready")} ready), ${n("parked")} parked, ${n("requested")} waiting`;
  if (c.state.counts.get(c.env.id) !== line) {
    c.state.counts.set(c.env.id, line);
    c.d.log(`workspace launcher: ${c.env.id}: workspaces ${line}`);
  }
}

/** Runs `f` in the background as the workspace's one step in flight. */
function background(c: Ctx, id: number, f: () => Promise<void>): void {
  const p = (async () => {
    try {
      await f();
    } catch (e) {
      c.d.log(`workspace launcher: workspace ${id}: ${(e as Error).stack ?? e}`);
    } finally {
      c.state.busy.delete(id);
    }
  })();
  c.state.busy.set(id, p);
}

async function fail(c: Ctx, w: WorkspaceRow, reason: string): Promise<void> {
  if (!await moveWorkspace(c.d.db, w.id, [w.state as WorkspaceStateName], "failed", { error: reason.slice(0, 300) }, c.now())) return;
  c.d.log(`workspace launcher: workspace ${w.id} failed: ${reason}`);
  if (w.pod) await c.api.deletePod(c.s.namespace, w.pod).catch(() => {});
  if (w.pvc) await c.api.deletePvc(c.s.namespace, w.pvc).catch(() => {});
}

/**
 * A workspace that could not start or seed. One that is resuming (it was ready before) goes back to parked with its kept
 * volume and its checkpoint, counting the try: the launcher resumes it again while its call is live, up to RESUME_TRIES.
 * Any other fails. A volume made for this start holds nothing the checkpoint lacks and is deleted.
 */
async function giveUp(c: Ctx, w: WorkspaceRow, reason: string): Promise<void> {
  if (!w.base_commit) return fail(c, w, reason);
  const tries = w.resume_tries + 1;
  const keep = w.pvc !== null && w.volume_fresh === 0;
  const what = `${keep ? "its volume" : ""}${keep && w.checkpoint_at ? " and " : ""}${w.checkpoint_at ? "its checkpoint" : ""}` || "nothing to rebuild from";
  const error = `${reason}${tries >= RESUME_TRIES ? `; resuming failed ${tries} times, so it stays parked with ${what}` : ` (resume try ${tries} of ${RESUME_TRIES})`}`;
  if (!await moveWorkspace(c.d.db, w.id, [w.state as WorkspaceStateName], "parked", { pod: null, pvc: keep ? w.pvc : null, node: keep ? w.node : null, volume_fresh: 0, resume_tries: tries, error: error.slice(0, 300) }, c.now())) return;
  c.d.log(`workspace launcher: workspace ${w.id} back to parked: ${error}`);
  if (w.pod) await c.api.deletePod(c.s.namespace, w.pod).catch(() => {});
  if (!keep && w.pvc) await c.api.deletePvc(c.s.namespace, w.pvc).catch(() => {});
}

type Images = { appImage: string; toolchainImage: string };

/** The images a workspace pod needs, or why the environment cannot start one. */
function imagesOf(c: Ctx): Images | string {
  const appImage = c.s.image ?? c.d.defaultImage;
  const toolchainImage = c.s.toolchainImages[WORKSPACE_TOOLCHAIN];
  if (!appImage) return "No AutoCraft image is known for workspace pods: set AUTOCRAFT_IMAGE on the runner or the environment's image";
  if (!toolchainImage) return `The environment has no ${WORKSPACE_TOOLCHAIN} toolchain image for workspace pods`;
  return { appImage, toolchainImage };
}

/** Starts a requested workspace. False when the environment should wait before the next create. */
async function start(c: Ctx, w: WorkspaceRow): Promise<boolean> {
  const images = imagesOf(c);
  if (typeof images === "string") {
    await giveUp(c, w, images);
    return true;
  }
  // A workspace that was ready before (it has a base commit) is resuming: its pod gets a new name, and so does a volume it rebuilds on.
  const name = workspaceName(w.id, w.base_commit ? c.now().toString(36) : undefined);
  const pvc = w.pvc ?? name;
  const fresh = w.pvc ? 0 : 1;
  if (!await moveWorkspace(c.d.db, w.id, ["requested"], "starting", { pod: name, pvc, volume_fresh: fresh, create_tries: 0, error: null }, c.now())) return true;
  return await create(c, { ...w, state: "starting", pod: name, pvc, volume_fresh: fresh, create_tries: 0 }, images);
}

/** A pod of the same name is still being deleted: the create is tried again once it is gone. */
class PodDeleting extends Error {}

/**
 * Creates the volume and pod of a starting workspace, counting the try. The row keeps its pod and volume names whatever
 * happens, so the sweep leaves objects a failed create may still have made. False after a failure.
 */
async function create(c: Ctx, row: WorkspaceRow, images: Images): Promise<boolean> {
  const { db } = c.d;
  let w = row;
  const ns = c.s.namespace;
  const tries = w.create_tries + 1;
  const at = new Date(c.now()).toISOString();
  await db.run(db.k.updateTable("workspaces").set({ create_tries: tries, pod_created_at: at, updated_at: at }).where("id", "=", w.id).where("state", "=", "starting"));
  // A create that answered with an error may still have made the object; the name is the workspace's own.
  const conflict = (e: unknown) => e instanceof KubeError && e.status === 409;
  try {
    if (w.volume_fresh === 0 && w.pvc) {
      // The kept volume must still be there: a create would otherwise make an empty one of its name, which seeding would take for the kept tree.
      const v = (await c.api.listPvcs(ns, workspaceSelector(w.id))).find((x) => x.metadata.name === w.pvc);
      if (!v || v.metadata.deletionTimestamp) w = await freshVolume(c, w, v ? "its kept volume is being deleted" : "its kept volume is gone");
    }
    await c.api.createPvc(ns, workspacePvcSpec(c.s, c.env.id, w.id, w.pvc ?? undefined)).catch((e: unknown) => {
      if (!conflict(e)) throw e;
    });
    await c.api.createPod(ns, workspacePodSpec({ s: c.s, envId: c.env.id, id: w.id, ...images, limits: limitsOf(w), pod: w.pod ?? undefined, pvc: w.pvc ?? undefined })).catch(async (e: unknown) => {
      if (!conflict(e)) throw e;
      const old = (await c.api.listPods(ns, workspaceSelector(w.id))).find((p) => p.metadata.name === w.pod);
      if (old?.metadata.deletionTimestamp) throw new PodDeleting();
    });
    c.d.log(`workspace launcher: workspace ${w.id} (${w.repo}@${w.base_sha.slice(0, 8)}) starting on ${c.env.id}${tries > 1 ? ` (try ${tries})` : ""}`);
    return true;
  } catch (e) {
    if (e instanceof PodDeleting) {
      c.d.log(`workspace launcher: workspace ${w.id}: an earlier pod of the same name is still being deleted; creating again later`);
      return true;
    }
    // Quota, rate limits or an API error: tried again on a later pass, up to CREATE_TRIES.
    const why = `The workspace pod could not be created: ${errText(e)}`;
    c.state.backoff.set(c.env.id, c.now() + CREATE_BACKOFF_MS);
    c.d.log(`workspace launcher: workspace ${w.id}: ${why} (try ${tries} of ${CREATE_TRIES})`);
    if (tries >= CREATE_TRIES) await giveUp(c, w, why);
    else await db.run(db.k.updateTable("workspaces").set({ error: why.slice(0, 300) }).where("id", "=", w.id).where("state", "=", "starting"));
    return false;
  }
}

function podFailure(pod: KPod): string {
  const st = pod.status;
  return `The workspace pod stopped${st?.reason ? ` (${st.reason}${st.message ? `: ${st.message}` : ""})` : ""}`.slice(0, 300);
}

async function advance(c: Ctx, w: WorkspaceRow, pods: KPod[]): Promise<void> {
  const now = c.now();
  const age = now - Date.parse(w.state_at);
  if (w.state === "parked") {
    const bound = w.resume_tries < RESUME_TRIES ? await boundCall(c, w) : null;
    if (bound) {
      if (await resumeWorkspace(c.d.db, w.id, now)) c.d.log(`workspace launcher: workspace ${w.id} resuming for call ${w.remote_call_id} (${bound})${w.pvc ? " on its volume" : "; rebuilding from base and checkpoint"}`);
      return;
    }
    // TODO(AUTO-43): also delete parked volumes oldest first when the node's disk passes 70%, once there is a disk metric.
    // A volume whose last checkpoint failed holds the only copy of the latest work, so it stays (the row's error says why).
    if (w.pvc && age > PARKED_VOLUME_MS && w.checkpoint_error === null) {
      // The row lets go of the volume first, so a resume meanwhile starts on a new one instead of taking this one.
      const r = await c.d.db.run(c.d.db.k.updateTable("workspaces").set({ pvc: null, node: null, updated_at: new Date(now).toISOString() })
        .where("id", "=", w.id).where("state", "=", "parked").where("pvc", "=", w.pvc).where("checkpoint_error", "is", null));
      if (!r.changes) return;
      await c.api.deletePvc(c.s.namespace, w.pvc);
      c.d.log(`workspace launcher: workspace ${w.id}: parked volume deleted after ${PARKED_VOLUME_MS / 60_000} min; the checkpoint stays`);
    } else if (!w.pvc && age > PARKED_END_MS) {
      // TODO(W7): end a parked workspace when its item closes, and keep it while the item is open.
      if (await moveWorkspace(c.d.db, w.id, ["parked"], "ended", {}, now)) c.d.log(`workspace launcher: workspace ${w.id} ended after ${PARKED_END_MS / 3600_000} h parked; the checkpoint stays`);
    }
    return;
  }
  if (!POD_STATES.includes(w.state as WorkspaceStateName)) return;
  const pod = pods.find((p) => p.metadata.name === w.pod);
  if (!pod) {
    if (w.state !== "starting") return lost(c, w, "The workspace pod disappeared (deleted, or its node went away)");
    // Not made, or gone before it started (a failed create, an old pod of the same name deleted meanwhile): create again.
    if (now - Date.parse(w.pod_created_at ?? w.state_at) < LIST_GRACE_MS) return;
    if (w.create_tries >= CREATE_TRIES) return giveUp(c, w, `The workspace pod could not be created in ${CREATE_TRIES} tries${w.error ? `: ${w.error}` : ""}`);
    const images = imagesOf(c);
    if (typeof images === "string") return giveUp(c, w, images);
    await create(c, w, images);
    return;
  }
  const deleting = pod.metadata.deletionTimestamp;
  if (deleting) {
    if (now - Date.parse(deleting) > STUCK_MS) return lost(c, w, "The workspace pod has been stuck in deletion (its node may be gone)");
    return;
  }
  const phase = pod.status?.phase;
  if (phase === "Unknown") {
    const since = c.state.unknownSince.get(w.id) ?? now;
    c.state.unknownSince.set(w.id, since);
    if (now - since > STUCK_MS) return lost(c, w, "The workspace pod's node stopped reporting");
    return;
  }
  c.state.unknownSince.delete(w.id);
  if (phase === "Succeeded" || phase === "Failed") {
    if (pod.status?.reason === "DeadlineExceeded") return (w.state === "ready" ? fail : giveUp)(c, w, `The workspace passed its time limit of ${Math.round(limitsOf(w).deadlineSec / 60)} min`);
    return lost(c, w, podFailure(pod));
  }

  if (w.state === "starting") {
    const statuses = [...(pod.status?.initContainerStatuses ?? []), ...(pod.status?.containerStatuses ?? [])];
    const pull = statuses.find((x) => PULL_REASONS.has(x.state?.waiting?.reason ?? ""));
    if (pull && age > PULL_LIMIT_MS) return giveUp(c, w, `The workspace pod's container ${pull.name} could not start: ${pull.state!.waiting!.reason}${pull.state!.waiting!.message ? ` (${clean(pull.state!.waiting!.message)})` : ""}`);
    const main = pod.status?.containerStatuses?.find((x) => x.name === WS_CONTAINER);
    if (phase === "Running" && main?.ready && main.state?.running) {
      if (await moveWorkspace(c.d.db, w.id, ["starting"], "seeding", { node: pod.spec?.nodeName ?? null }, now)) {
        background(c, w.id, () => seed(c, w.id));
      }
      return;
    }
    const sched = pod.status?.conditions?.find((x) => x.type === "PodScheduled");
    // A resume onto a kept volume, which local-path pins to its node (`node`): a pod that cannot go there may mean the node is gone.
    if (w.volume_fresh === 0 && w.node !== null && sched?.status === "False" && now - Date.parse(w.pod_created_at ?? w.state_at) > RESUME_PENDING_MS) {
      const gone = await volumeNodeGone(c, w.node, sched.message ?? "", now);
      if (gone && w.checkpoint_error !== null) return giveUp(c, w, `${gone}, and the last checkpoint failed (${w.checkpoint_error}): the latest work is only on its volume`);
      if (gone) return dropVolume(c, w, gone);
    }
    if (age > PENDING_LIMIT_MS) {
      const why = sched?.status === "False" ? clean(sched.message ?? sched.reason ?? "not scheduled") : pull ? pull.state!.waiting!.reason! : "its container did not become ready";
      return giveUp(c, w, `The workspace pod did not start within ${PENDING_LIMIT_MS / 60_000} min: ${why}`);
    }
    return;
  }
  if (w.state === "seeding") {
    // Nothing in flight for a seeding row: the runner restarted while it seeded.
    if (age > SEED_LIMIT_MS) return giveUp(c, w, `Seeding took longer than ${SEED_LIMIT_MS / 60_000} min`);
    background(c, w.id, () => seed(c, w.id));
    return;
  }
  // ready
  const lastCall = Math.max(Date.parse(w.state_at), w.last_call_at ? Date.parse(w.last_call_at) : 0);
  const bound = await boundCall(c, w);
  if (bound === "leased" && now - lastCall > LEASE_TOUCH_MS) {
    const at = new Date(now).toISOString();
    await c.d.db.run(c.d.db.k.updateTable("workspaces").set({ last_call_at: at, updated_at: at }).where("id", "=", w.id).where("state", "=", "ready"));
  } else if (!bound && now - lastCall > IDLE_PARK_MS && !await callRunning(c, w.id, now) && !await backgroundRunning(c, w, now)) return background(c, w.id, () => park(c, w.id));
  const tried = c.state.checkpointTried.get(w.id) ?? 0;
  const lastCheckpoint = Math.max(Date.parse(w.state_at), w.checkpoint_at ? Date.parse(w.checkpoint_at) : 0, tried);
  // A call back in the queue after its session did work: the next session's prompt states the checkpoint's changes.
  const requeued = bound === "queued" && w.last_call_at !== null && (w.checkpoint_at === null || w.last_call_at > w.checkpoint_at) && now - tried > REQUEUE_CHECKPOINT_MS;
  if (now - lastCheckpoint > CHECKPOINT_EVERY_MS || requeued) {
    c.state.checkpointTried.set(w.id, now);
    background(c, w.id, async () => void await checkpoint(c, w.id));
  }
}

/** Whether a call is running in the workspace within its time limit (plus CALL_GRACE_MS); a row past it no longer holds the pod. */
async function callRunning(c: Ctx, id: number, now: number): Promise<boolean> {
  const { db } = c.d;
  return !!await db.get(db.k.selectFrom("workspace_calls").select(["id"]).where("workspace_id", "=", id).where("state", "=", "running")
    .where("deadline_at", ">", new Date(now).toISOString()).limit(1));
}

/** The state of the remote call bound to the workspace while it is queued or leased (its session may still make tool calls), else null. */
async function boundCall(c: Ctx, w: WorkspaceRow): Promise<"queued" | "leased" | null> {
  if (w.remote_call_id === null) return null;
  const { db } = c.d;
  const r = await db.get(db.k.selectFrom("remote_calls").select("state").where("id", "=", w.remote_call_id).where("state", "in", ["queued", "leased"]));
  return r ? (r.state as "queued" | "leased") : null;
}

/**
 * Whether a background shell command the session started in the current pod may still run, by the session's bookkeeping
 * on its rows (WorkspaceSession): started, not seen ending by a shell_status or shell_kill, and within its own limit.
 */
async function backgroundRunning(c: Ctx, w: WorkspaceRow, now: number): Promise<boolean> {
  const { db } = c.d;
  const rows = await db.all(db.k.selectFrom("workspace_calls")
    .select((eb) => {
      const res = eb.case().when(kx.jsonValid(db, "result")).then(eb.ref("result")).end();
      return ["id", "tool", "started_at", kx.jsonNum(db, res, "$.acct.bg").as("bg"), kx.jsonNum(db, res, "$.acct.limit").as("lim"),
        kx.jsonNum(db, res, "$.acct.ref").as("ref"), kx.jsonNum(db, res, "$.acct.ended").as("ended")] as const;
    })
    .where("workspace_id", "=", w.id).where("tool", "in", ["shell", "shell_status", "shell_kill"]).where("state", "=", "done")
    .where("started_at", ">=", w.pod_created_at ?? w.state_at));
  const ended = new Set(rows.filter((r) => r.tool !== "shell" && Number(r.ended) === 1).map((r) => Number(r.ref)));
  return rows.some((r) => r.tool === "shell" && Number(r.bg) === 1 && !ended.has(Number(r.id)) && r.started_at !== null
    && Date.parse(r.started_at) + Number(r.lim ?? 0) * 1000 + BACKGROUND_GRACE_MS > now);
}

/**
 * Why the node holding a kept volume is gone, or null when it may not be. Gone: the node does not exist, or has not been
 * ready for NODE_LOST_MS, or (when nodes cannot be read) the scheduler refused the pod on every node for the volume's
 * node affinity. Never for a node that is only full or tainted: the pod waits for it.
 */
async function volumeNodeGone(c: Ctx, node: string, message: string, now: number): Promise<string | null> {
  try {
    const n = await c.api.getNode(node);
    if (!n) return `its volume's node ${node} is gone`;
    const ready = n.status?.conditions?.find((x) => x.type === "Ready");
    const since = ready?.lastTransitionTime ? Date.parse(ready.lastTransitionTime) : NaN;
    if (ready && ready.status !== "True" && now - since > NODE_LOST_MS) return `its volume's node ${node} has not been ready since ${new Date(since).toISOString().slice(11, 16)}`;
    return null;
  } catch {
    // Nodes cannot be read: the scheduler's answer decides.
  }
  const m = /^0\/(\d+) nodes are available: ([^]*?)(?:preemption:|$)/.exec(message);
  if (!m) return null;
  let conflicts = 0;
  for (const x of m[2]!.matchAll(/(\d+) node\(s\) (?:had volume node affinity conflict|didn't match PersistentVolume's node affinity)/g)) conflicts += Number(x[1]);
  return Number(m[1]) > 0 && conflicts === Number(m[1]) ? "no node of the cluster can hold its volume (its node is gone)" : null;
}

/** Moves a starting workspace onto a new, empty volume, which seeding fills from the base and the checkpoint; returns the updated row. */
async function freshVolume(c: Ctx, w: WorkspaceRow, why: string): Promise<WorkspaceRow> {
  const name = workspaceName(w.id, c.now().toString(36));
  const at = new Date(c.now()).toISOString();
  const r = await c.d.db.run(c.d.db.k.updateTable("workspaces").set({ pvc: name, node: null, volume_fresh: 1, updated_at: at })
    .where("id", "=", w.id).where("state", "=", "starting").where("pvc", "=", w.pvc));
  if (!r.changes) throw new Error("the workspace changed while its volume was replaced");
  c.d.log(`workspace launcher: workspace ${w.id}: ${why}; starting on a new volume rebuilt from base and ${w.checkpoint_at ? `the checkpoint of ${w.checkpoint_at}` : "no checkpoint"}${w.checkpoint_error ? ` (the last checkpoint failed: ${w.checkpoint_error})` : ""}`);
  return { ...w, pvc: name, node: null, volume_fresh: 1 };
}

/** The kept volume of a resuming workspace is lost with its node: the pod and volume are deleted and the workspace starts on a new volume. */
async function dropVolume(c: Ctx, w: WorkspaceRow, why: string): Promise<void> {
  const name = workspaceName(w.id, c.now().toString(36));
  const at = new Date(c.now()).toISOString();
  const r = await c.d.db.run(c.d.db.k.updateTable("workspaces").set({ pod: name, pvc: name, node: null, volume_fresh: 1, create_tries: 0, pod_created_at: null, updated_at: at })
    .where("id", "=", w.id).where("state", "=", "starting").where("pod", "=", w.pod).where("checkpoint_error", "is", null));
  if (!r.changes) return;
  c.d.log(`workspace launcher: workspace ${w.id}: ${why}; rebuilding on a new volume from base and ${w.checkpoint_at ? `the checkpoint of ${w.checkpoint_at}` : "no checkpoint"}`);
  if (w.pod) await c.api.deletePod(c.s.namespace, w.pod).catch(() => {});
  if (w.pvc) await c.api.deletePvc(c.s.namespace, w.pvc).catch(() => {});
}

/** The pod went away. A ready workspace keeps its volume and checkpoint (parked, for a resume); an earlier one gives up. */
async function lost(c: Ctx, w: WorkspaceRow, reason: string): Promise<void> {
  if (w.state !== "ready") return giveUp(c, w, reason);
  if (!await moveWorkspace(c.d.db, w.id, ["ready"], "parked", { pod: null, error: reason.slice(0, 300) }, c.now())) return;
  c.d.log(`workspace launcher: workspace ${w.id} parked: ${reason}`);
  if (w.pod) await c.api.deletePod(c.s.namespace, w.pod).catch(() => {});
}

// ---- seeding ---------------------------------------------------------------------

/** The tar of `sha` from AutoCraft's clone of `repo`, fetching the commit first when the clone lacks it. Refuses sources over MAX_SOURCE_BYTES. */
export async function sourceArchive(repo: string, sha: string): Promise<{ bytes: number; open: () => AsyncIterable<Buffer> }> {
  const g = new Git(exec, fixupDir(), repo);
  await g.init();
  if ((await g.git(["cat-file", "-e", `${sha}^{commit}`])).code !== 0) {
    await g.must(["fetch", "--quiet", "--no-tags", "--no-recurse-submodules", "--no-write-fetch-head", `https://github.com/${ownerOf(repo)}/${repo}.git`, `+${sha}:refs/workspace/base`]);
  }
  // The tar's size from the tree: a 512-byte header per entry and each blob padded to 512 bytes.
  let bytes = 1024;
  for (const e of (await g.must(["ls-tree", "-r", "-t", "-l", "-z", sha])).split("\0")) {
    const m = /^\d+ \w+ [0-9a-f]+ +(\d+|-)\t/.exec(e);
    if (m) bytes += 512 + (m[1] === "-" ? 0 : Math.ceil(Number(m[1]) / 512) * 512);
  }
  if (bytes > MAX_SOURCE_BYTES) throw new Error(`the source of ${repo}@${sha.slice(0, 8)} is ${Math.round(bytes / 1024 ** 2)} MiB, over the ${MAX_SOURCE_BYTES / 1024 ** 2} MiB limit`);
  const env = { ...process.env, ...GIT_ENV, ...gitDirEnv(g.dir) };
  return {
    bytes,
    open: async function* () {
      const p = spawn("git", [...HARDENING, "archive", "--format=tar", sha], { env, stdio: ["ignore", "pipe", "ignore"], windowsHide: true });
      const closed = new Promise<number | null>((ok) => {
        p.on("close", (code) => ok(code));
        p.on("error", () => ok(null));
      });
      try {
        for await (const chunk of p.stdout) yield chunk as Buffer;
        if (await closed !== 0) throw new Error("git archive failed");
      } finally {
        if (p.exitCode === null) p.kill();
      }
    },
  };
}

/** The base commit `ws untar` made, or null when it failed. */
const untarCommit = (r: WsCallResult): string | null => (r.header?.kind === "ok" && /^[0-9a-f]{40}$/.test(r.header.fields[0] ?? "") ? r.header.fields[0]! : null);

async function seed(c: Ctx, id: number): Promise<void> {
  const { db } = c.d;
  const w = await getWorkspace(db, id);
  if (!w || w.state !== "seeding") return;
  const deadline = Date.parse(w.state_at) + SEED_LIMIT_MS;
  if (w.base_commit && w.volume_fresh === 0) return await seedKept(c, w, w.base_commit, deadline);
  try {
    // A volume made for this start: an earlier seed cut off by a restart may be wiped.
    const src = await sourceArchive(w.repo, w.base_sha);
    const untar = async () => await execWs(db, c.api, id, ["untar"], { tool: "seed", args: { repo: w.repo, sha: w.base_sha, bytes: src.bytes }, stdin: src.open(), timeoutMs: Math.max(10_000, deadline - c.now()) }, c.now);
    let r = await untar();
    if (r.header?.kind === "error" && r.header.code === "conflict") {
      // An earlier seed was cut off (a restart): start again from an empty tree.
      const wipe = await execWs(db, c.api, id, ["run", "300", "--", "find", "/work/src", "-mindepth", "1", "-delete"], { tool: "seed", args: { wipe: true }, timeoutMs: 310_000 }, c.now);
      if (wipe.header?.kind !== "status" || wipe.header.exit !== 0) throw new Error(`could not empty /work/src: ${describeHeader(wipe.header)}`);
      r = await untar();
    }
    const commit = untarCommit(r);
    if (!commit) throw new Error(`ws untar: ${r.error ?? describeHeader(r.header)}${r.stderr ? `: ${r.stderr.trim().split("\n").pop()}` : ""}`);
    const applied = w.base_commit ? await applyCheckpoint(c, id) : false;
    const restored = !w.base_commit ? null : applied && w.checkpoint_at ? `checkpoint ${w.checkpoint_at}` : "empty";
    const at = new Date(c.now()).toISOString();
    if (await moveWorkspace(db, id, ["seeding"], "ready", { base_commit: commit, volume_fresh: 0, resume_tries: 0, ...(restored && { restored, restored_at: at }) }, c.now())) {
      c.d.log(`workspace launcher: workspace ${id} ${w.base_commit ? `rebuilt from base and ${applied ? `the checkpoint of ${w.checkpoint_at}` : "no checkpoint"}` : "ready"} (base ${commit.slice(0, 8)})`);
    }
  } catch (e) {
    await giveUp(c, w, `Seeding failed: ${errText(e)}`);
  }
}

/**
 * Seeding a resume onto a kept volume: the volume holds the tree and every change since the last checkpoint, so it is only
 * checked for the base commit, never seeded or wiped. An unclear answer is asked again until SEED_LIMIT_MS; a volume
 * without the base commit, or no answer in time, sends the workspace back to parked with the volume as it is.
 */
async function seedKept(c: Ctx, w: WorkspaceRow, base: string, deadline: number): Promise<void> {
  const { db } = c.d;
  let why = "no answer";
  for (let i = 0; i < SEED_LIMIT_MS / KEPT_RETRY_MS; i++) {
    try {
      const r = await execWs(db, c.api, w.id, ["run", "60", "--", "git", "cat-file", "-e", `${base}^{commit}`], { tool: "seed", args: { resume: "check" }, timeoutMs: 70_000 }, c.now);
      if (r.header?.kind === "status" && r.header.state === "exited") {
        if (r.header.exit !== 0) return await giveUp(c, w, `The kept volume does not hold the base commit ${base.slice(0, 8)} (git exit ${r.header.exit}); the volume is left as it is`);
        const at = new Date(c.now()).toISOString();
        if (await moveWorkspace(db, w.id, ["seeding"], "ready", { resume_tries: 0, restored: "kept", restored_at: at }, c.now())) c.d.log(`workspace launcher: workspace ${w.id} resumed on its volume`);
        return;
      }
      why = r.error ?? describeHeader(r.header);
    } catch (e) {
      why = errText(e);
    }
    if (c.now() + KEPT_RETRY_MS >= deadline || (await getWorkspace(db, w.id))?.state !== "seeding") break;
    await new Promise((r) => setTimeout(r, KEPT_RETRY_MS));
  }
  const now = await getWorkspace(db, w.id);
  if (now?.state === "seeding") await giveUp(c, now, `The kept volume could not be checked: ${why}`);
}

/** Applies the stored checkpoint to a rebuilt tree; false when there is none. Throws when it does not apply. */
async function applyCheckpoint(c: Ctx, id: number): Promise<boolean> {
  const { db } = c.d;
  const diff = await getCheckpoint(db, id);
  if (!diff?.length) return false;
  const put = await execWs(db, c.api, id, ["put", RESUME_FILE], { tool: "seed", args: { resume: "checkpoint", bytes: diff.length }, stdin: Buffer.from(diff), timeoutMs: 120_000 }, c.now);
  if (put.header?.kind !== "ok") throw new Error(`the checkpoint could not be copied in: ${put.error ?? describeHeader(put.header)}`);
  const r = await execWs(db, c.api, id, ["run", "300", "--", "git", "apply", "--binary", "--whitespace=nowarn", RESUME_FILE], { tool: "seed", args: { resume: "apply" }, timeoutMs: 310_000 }, c.now);
  if (r.header?.kind !== "status" || r.header.state !== "exited" || r.header.exit !== 0) {
    throw new Error(`the checkpoint does not apply (${r.error ?? describeHeader(r.header)})${r.output.trim() ? `: ${clean(r.output.trim().split("\n").pop())}` : ""}`);
  }
  return true;
}

// ---- checkpoints and parking -------------------------------------------------------

/** Stores the workspace's diff against its base commit. Problems are recorded on the row and never fail the workspace; false then. */
async function checkpoint(c: Ctx, id: number): Promise<boolean> {
  const { db } = c.d;
  const w = await db.get(db.k.selectFrom("workspaces").select(["state", "base_commit"]).where("id", "=", id));
  if (w?.state !== "ready" || !w.base_commit) return false;
  // While checkpoint_error is set the volume holds work no checkpoint has, so it is not deleted with time or dropped with a lost node.
  const problem = async (why: string) => {
    await db.run(db.k.updateTable("workspaces").set({ error: `checkpoint: ${why}`.slice(0, 300), checkpoint_error: why.slice(0, 300), updated_at: new Date(c.now()).toISOString() }).where("id", "=", id));
    c.d.log(`workspace launcher: workspace ${id}: checkpoint: ${why}`);
    return false;
  };
  try {
    const r = await execWs(db, c.api, id, ["run", "300", "--", "sh", "-c", CHECKPOINT_SH, "checkpoint", w.base_commit], { tool: "checkpoint", timeoutMs: 310_000 }, c.now);
    if (r.header?.kind !== "status" || r.header.state !== "exited" || r.header.exit !== 0) {
      return await problem(`the diff failed (${r.error ?? describeHeader(r.header)})${r.output.trim() ? `: ${clean(r.output.trim().split("\n").pop())}` : ""}`);
    }
    const raw = await execInWorkspace(db, c.api, id, ["head", "-c", String(MAX_CHECKPOINT_BYTES + 1), CHECKPOINT_FILE], { maxOut: MAX_CHECKPOINT_BYTES + 1, timeoutMs: 120_000 });
    if (raw.exitCode !== 0) return await problem(`the diff could not be read (${raw.error ?? `exit ${raw.exitCode}`})`);
    if (raw.stdout.length > MAX_CHECKPOINT_BYTES) return await problem(`the diff is over ${MAX_CHECKPOINT_BYTES / 1024 ** 2} MiB; the previous checkpoint is kept`);
    const at = new Date(c.now()).toISOString();
    await db.run(db.k.updateTable("workspaces").set({ checkpoint: raw.stdout, checkpoint_at: at, checkpoint_error: null, error: null, updated_at: at }).where("id", "=", id).where("state", "=", "ready"));
    return true;
  } catch (e) {
    return await problem(errText(e));
  }
}

async function park(c: Ctx, id: number): Promise<void> {
  if (!await checkpoint(c, id)) c.d.log(`workspace launcher: workspace ${id}: parking without a new checkpoint; its volume is kept until one succeeds`);
  const w = await c.d.db.get(c.d.db.k.selectFrom("workspaces").select(["pod"]).where("id", "=", id).where("state", "=", "ready"));
  if (!w) return;
  if (!await moveWorkspace(c.d.db, id, ["ready"], "parked", { pod: null }, c.now())) return;
  if (w.pod) await c.api.deletePod(c.s.namespace, w.pod).catch((e: Error) => c.d.log(`workspace launcher: delete pod ${w.pod}: ${e.message}`));
  c.d.log(`workspace launcher: workspace ${id} parked after ${IDLE_PARK_MS / 60_000} min without a call`);
}
