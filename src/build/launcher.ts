// The pod launcher: runs in the runner and starts, follows and removes the build pods of kubernetes environments.
// Each pass, per environment: keep its pod runs (build_pod_runs) alive, list its pods and volumes, move every run on by
// what its pod's status says, claim queued tasks for free slots one at a time, delete pods and volumes no run owns, and
// record what the environment offers and its health. Stateless between passes apart from caches, so a restarted runner
// picks up where the last one stopped: a run whose pod is gone is lost (requeued once, then failed, as for a worker that
// stops answering), and pods and volumes of tasks that are no longer this environment's are deleted. The launcher owns
// the tasks it runs: reapLost leaves them alone.
import { randomBytes } from "node:crypto";
import { setTimeout as sleep } from "node:timers/promises";
import type { Requires, SandboxEnv, TaskResult, TaskStep } from "../../web/src/api/types.ts";
import { envVar } from "../env.ts";
import { hashKey, kubeReport, newPodKey, type EnvHealth } from "./envs.ts";
import { guardedApi, inClusterApi, type ContainerStatus, type KPod, type KubeApi } from "./kube-api.ts";
import {
  DISK_CONTAINER, envSelector, FIRST_GROUP, groupsOf, CREATE_TRIES, kubeCapabilities, LIST_GRACE_MS, PACKAGES_CONTAINER, parsePrepared, PENDING_LIMIT_MS, PENDING_WARN_MS, PULL_LIMIT_MS,
  PULL_REASONS, pvcSpec, settingsOf, sourcePod, STAGE_SOURCE, STAGE_TASK, stepCap, stepContainer, stepPod, STUCK_MS, taskPod, toolchainImage, uploadPod, uploadStage,
  type KubeSettings, type PodCtx, type Prepared,
} from "./pods.ts";
import { workspacePass, WorkspaceState } from "./workspace-launch.ts";
import { appendLog, claim, finishTask, loseTask, MAX_CHUNK_BYTES, MAX_LOG_BYTES, releaseClaim, startTask, touchTask } from "./tasks.ts";
import type { Db } from "../db/index.ts";
import { cols } from "../db/kx.ts";

export const PASS_MS = 5000;
/** Beyond a deadline Kubernetes should have enforced, the runner gives up on the pod itself. */
export const DEADLINE_MARGIN_MS = 5 * 60_000;
/** Failed creates of a stage's pod are retried with backoff (15 s doubling, at most 5 min) up to CREATE_TRIES times or this long. */
export const CREATE_GIVE_UP_MS = 15 * 60_000;
const ACCESS_EVERY_MS = 60_000;
const CREATE_BACKOFF_MS = 60_000;
/** Longest the task, source and upload pods may run. */
const APP_POD_SEC = 30 * 60;

type Run = {
  task_id: number; env_id: string; key_hash: string; pvc: string; stage: number; pod: string | null; pod_created_at: string | null;
  prepared: string | null; results: string; failed: string | null; ran: number; logged: string; create_tries: number; failing_since: string | null;
  retry_at: string | null; started_at: string;
};
type TaskRow = { id: number; state: string; env_id: string | null; repo: string; plan: string; requires: string; log_bytes: number };
type EnvRow = { id: string; enabled: number; slots: number; settings: string | null };
type Kept = Prepared & { image: string };

export type LauncherDeps = {
  db: Db;
  /** null outside a cluster: the environments then report that they cannot start pods. */
  api: KubeApi | null;
  /** The runner's own image (AUTOCRAFT_IMAGE), for environments that name none. */
  defaultImage: string | null;
  log: (line: string) => void;
  now?: () => number;
};

/** Caches between passes: the access review, a pause after a task could not be started, and when a pod's node stopped reporting. */
export class LauncherState {
  access = new Map<string, { at: number; missing: string[] }>();
  backoff = new Map<string, { until: number; why: string }>();
  unknownSince = new Map<string, number>();
}

const json = <T>(s: string | null, fallback: T): T => {
  try {
    return s ? (JSON.parse(s) as T) : fallback;
  } catch {
    return fallback;
  }
};
const minutes = (ms: number) => `${Math.max(1, Math.round(ms / 60_000))} min`;
const statusOf = (pod: KPod, name: string): ContainerStatus | undefined =>
  [...(pod.status?.initContainerStatuses ?? []), ...(pod.status?.containerStatuses ?? [])].find((c) => c.name === name);
const clean = (s: string | undefined): string => (s ?? "").replace(/[\u0000-\u001f\u007f]+/g, " ").trim().slice(0, 300);

export async function launcherPass(d: LauncherDeps, state: LauncherState): Promise<void> {
  const envs = await d.db.all(d.db.k.selectFrom("build_envs").select(["id", "enabled", "slots", "settings"]).where("kind", "=", "kubernetes").orderBy("id"));
  for (const env of envs) {
    try {
      await envPass(d, state, env);
    } catch (e) {
      d.log(`pod launcher: ${env.id}: ${(e as Error).stack ?? e}`);
    }
  }
}

type EnvCtx = { d: LauncherDeps; state: LauncherState; api: KubeApi | null; env: EnvRow; s: KubeSettings; image: string | null; pods: KPod[]; pending: string[]; now: () => number };

async function envPass(d: LauncherDeps, state: LauncherState, env: EnvRow): Promise<void> {
  const { db } = d;
  const now = d.now ?? Date.now;
  const s = settingsOf(env.settings);
  const caps = kubeCapabilities(s);
  const report = async (health: Omit<EnvHealth, "since"> | null, reachable: boolean) => await kubeReport(db, env.id, caps, env.slots, health, reachable);
  const image = s.image ?? d.defaultImage;
  const ctx: EnvCtx = { d, state, api: d.api, env, s, image, pods: [], pending: [], now };
  const runs = async () => await db.all(db.k.selectFrom("build_pod_runs").select(cols("build_pod_runs")).where("env_id", "=", env.id).orderBy("task_id"));
  for (const run of await runs()) await touchTask(db, env.id, run.task_id);

  const api = d.api;
  // Without the API, runs past their deadline are still given up, so their slots come back.
  const offline = async (text: string) => {
    for (const run of await runs()) if (overdue(ctx, run)) await lose(ctx, run, overdueText(ctx));
    await report({ state: "fail", text: text.slice(0, 300) }, false);
  };
  if (!api) return offline("The runner is not running in a Kubernetes cluster, so it cannot start build pods");
  const ns = s.namespace;
  let pvcs: { metadata: { name: string } }[];
  try {
    [ctx.pods, pvcs] = await Promise.all([api.listPods(ns, envSelector(env.id)), api.listPvcs(ns, envSelector(env.id))]);
  } catch (e) {
    return offline(`The Kubernetes API did not answer: ${(e as Error).message}`);
  }
  // Claims need the capabilities, before any health is known.
  await kubeReport(db, env.id, caps, env.slots, undefined, true);

  let access = state.access.get(env.id);
  if (!access || now() - access.at > ACCESS_EVERY_MS) {
    const wanted: [string, string][] = [["create", "pods"], ["delete", "pods"], ["get", "pods"], ["create", "persistentvolumeclaims"], ["delete", "persistentvolumeclaims"]];
    const allowed = await Promise.all(wanted.map(([v, r]) => api.canI(ns, v, r).catch(() => false)));
    access = { at: now(), missing: wanted.filter((_, i) => !allowed[i]).map(([v, r]) => `${v} ${r}`) };
    state.access.set(env.id, access);
  }

  for (const run of await runs()) await advance(ctx, run);

  const back = state.backoff.get(env.id);
  if (image && !access.missing.length && !(back && back.until > now())) {
    // One at a time: a claim that cannot start goes back to the queue before the next is taken.
    for (let active = (await runs()).length; active < env.slots; active++) {
      const t = (await claim(db, env.id, 1, now())).tasks[0];
      if (!t) break;
      const err = await start(ctx, t.id);
      if (err) {
        state.backoff.set(env.id, { until: now() + CREATE_BACKOFF_MS, why: err });
        break;
      }
      state.backoff.delete(env.id);
    }
  }

  // Pods and volumes no run owns: a finished stage whose delete failed, a create that failed late, or a task that is no longer this environment's.
  const owned = await runs();
  const podNames = new Set(owned.map((r) => r.pod)), pvcNames = new Set(owned.map((r) => r.pvc));
  for (const p of ctx.pods) if (!podNames.has(p.metadata.name)) await api.deletePod(ns, p.metadata.name).catch((e: Error) => d.log(`pod launcher: delete pod ${p.metadata.name}: ${e.message}`));
  for (const v of pvcs) if (!pvcNames.has(v.metadata.name)) await api.deletePvc(ns, v.metadata.name).catch((e: Error) => d.log(`pod launcher: delete volume ${v.metadata.name}: ${e.message}`));
  for (const name of state.unknownSince.keys()) if (!podNames.has(name)) state.unknownSince.delete(name);

  const backNow = state.backoff.get(env.id);
  const health: Omit<EnvHealth, "since"> | null =
    access.missing.length ? { state: "fail", text: `The runner may not ${access.missing.join(", ")} in namespace ${ns}` }
    : !image ? { state: "fail", text: "No AutoCraft image is known for build pods: set AUTOCRAFT_IMAGE on the runner or the environment's image" }
    : backNow && backNow.until > now() ? { state: "fail", text: `A build pod could not be created: ${backNow.why}`.slice(0, 300) }
    : ctx.pending.length ? { state: "warn", text: ctx.pending[0]! }
    : !Object.keys(s.toolchainImages).length ? { state: "warn", text: "No toolchain image is set, so tasks that name a toolchain (every pipeline build) are not taken" }
    : null;
  await report(health, true);
}

const taskOf = async (db: Db, id: number) => await db.get(db.k.selectFrom("build_tasks").select(["id", "state", "env_id", "repo", "plan", "requires", "log_bytes"]).where("id", "=", id));
const overdue = (c: EnvCtx, run: Run): boolean => c.now() > Date.parse(run.started_at) + c.s.deadlineSec * 1000 + DEADLINE_MARGIN_MS;
const overdueText = (c: EnvCtx) => `The task passed its deadline of ${c.s.deadlineSec} s and its pod did not stop`;

/** Starts a claimed task: its run, volume and task pod. Anything that fails undoes the claim; returns the error then. */
async function start(c: EnvCtx, taskId: number): Promise<string | null> {
  const { db } = c.d;
  const key = newPodKey(taskId);
  const pvc = `acb-${taskId}-${randomBytes(3).toString("hex")}`;
  try {
    await db.run(db.k.insertInto("build_pod_runs")
      .values({ task_id: taskId, env_id: c.env.id, key_hash: hashKey(key), pvc, stage: STAGE_TASK, started_at: new Date(c.now()).toISOString() }));
    await startTask(db, c.env.id, taskId);
    await c.api!.createPvc(c.s.namespace, pvcSpec(c.s, c.env.id, taskId, pvc));
    const run = (await db.get(db.k.selectFrom("build_pod_runs").select(cols("build_pod_runs")).where("task_id", "=", taskId)))!;
    await createPod(c, run, STAGE_TASK, (ctx) => taskPod(ctx, c.image!, key));
    c.d.log(`pod launcher: task ${taskId} started on ${c.env.id}`);
    return null;
  } catch (e) {
    const msg = (e as Error).message;
    c.d.log(`pod launcher: task ${taskId}: could not start: ${msg}`);
    await c.api!.deletePvc(c.s.namespace, pvc).catch(() => {});
    await db.run(db.k.deleteFrom("build_pod_runs").where("task_id", "=", taskId));
    await releaseClaim(db, c.env.id, taskId);
    return msg;
  }
}

/** Records the stage's pod on the run, then creates it; throws when the create fails. */
async function createPod(c: EnvCtx, run: Run, stage: number, spec: (ctx: PodCtx) => object, deadlineSec = APP_POD_SEC): Promise<void> {
  const now = c.now();
  const left = c.s.deadlineSec - (now - Date.parse(run.started_at)) / 1000;
  const name = `acb-${run.task_id}-${stage}-${randomBytes(3).toString("hex")}`;
  const ctx: PodCtx = { s: c.s, envId: c.env.id, taskId: run.task_id, stage, name, pvc: run.pvc, deadlineSec: Math.min(deadlineSec, left) };
  const at = new Date(now).toISOString();
  await c.d.db.run(c.d.db.k.updateTable("build_pod_runs").set({ stage, pod: name, pod_created_at: at }).where("task_id", "=", run.task_id));
  Object.assign(run, { stage, pod: name, pod_created_at: at });
  await c.api!.createPod(c.s.namespace, spec(ctx));
}

async function removeRun(c: EnvCtx, run: Run): Promise<void> {
  if (c.api) {
    if (run.pod) await c.api.deletePod(c.s.namespace, run.pod).catch(() => {});
    await c.api.deletePvc(c.s.namespace, run.pvc).catch(() => {});
  }
  await c.d.db.run(c.d.db.k.deleteFrom("build_pod_runs").where("task_id", "=", run.task_id));
}

async function lose(c: EnvCtx, run: Run, reason: string): Promise<void> {
  c.d.log(`pod launcher: task ${run.task_id}: ${reason}`);
  await removeRun(c, run);
  await loseTask(c.d.db, run.task_id, reason, "Cancelled; its build pod stopped.");
}

/** Appends a container's output to the task log under `step` once, within the task's log cap. */
async function podLog(c: EnvCtx, run: Run, container: string, step: string): Promise<void> {
  const t = await taskOf(c.d.db, run.task_id);
  const id = `${run.pod}/${container}`;
  const logged = json<string[]>(run.logged, []);
  if (!t || !run.pod || logged.includes(id)) return;
  let text: string;
  try {
    text = await c.api!.podLog(c.s.namespace, run.pod, container, Math.max(1024, MAX_LOG_BYTES - t.log_bytes));
  } catch (e) {
    text = `(the output of ${container} could not be read: ${(e as Error).message})\n`;
  }
  logged.push(id);
  run.logged = JSON.stringify(logged);
  await c.d.db.run(c.d.db.k.updateTable("build_pod_runs").set({ logged: run.logged }).where("task_id", "=", run.task_id));
  // A quarter of the chunk cap in characters stays under it in bytes.
  for (let i = 0; i < text.length; i += MAX_CHUNK_BYTES / 4) {
    if ((await appendLog(c.d.db, c.env.id, run.task_id, step, text.slice(i, i + MAX_CHUNK_BYTES / 4))).truncated) break;
  }
}

/** Why a pod ended without its containers saying so. */
function podFailure(pod: KPod): string {
  const st = pod.status;
  if (st?.reason === "DeadlineExceeded") return "The build pod passed its deadline";
  return `The build pod failed${st?.reason ? ` (${st.reason}${st.message ? `: ${st.message}` : ""})` : ""}`.slice(0, 300);
}

async function advance(c: EnvCtx, run: Run): Promise<void> {
  const { db } = c.d;
  const now = c.now();
  const t = await taskOf(db, run.task_id);
  if (!t || t.env_id !== c.env.id || (t.state !== "claimed" && t.state !== "running")) return removeRun(c, run);
  if (overdue(c, run)) return lose(c, run, overdueText(c));
  if (!run.pod) {
    if (run.retry_at && now < Date.parse(run.retry_at)) return;
    return createStage(c, run, t, run.stage);
  }
  const pod = c.pods.find((p) => p.metadata.name === run.pod);
  const age = now - Date.parse(run.pod_created_at ?? run.started_at);
  if (!pod) {
    if (age < LIST_GRACE_MS) return;
    return lose(c, run, "The build pod disappeared before it finished (deleted, or its node went away).");
  }
  const phase = pod.status?.phase;
  const deleting = pod.metadata.deletionTimestamp;
  if (deleting && now - Date.parse(deleting) > STUCK_MS) return lose(c, run, "The build pod has been stuck in deletion (its node may be gone).");
  if (phase === "Unknown") {
    const since = c.state.unknownSince.get(pod.metadata.name) ?? now;
    c.state.unknownSince.set(pod.metadata.name, since);
    if (now - since > STUCK_MS) return lose(c, run, "The build pod's node stopped reporting.");
    return;
  }
  c.state.unknownSince.delete(pod.metadata.name);
  const limit = pod.spec?.activeDeadlineSeconds;
  if (limit !== undefined && age > limit * 1000 + PENDING_LIMIT_MS + DEADLINE_MARGIN_MS) return lose(c, run, "The build pod ran past its deadline without stopping.");
  if (phase !== "Succeeded" && phase !== "Failed") {
    const statuses = [...(pod.status?.initContainerStatuses ?? []), ...(pod.status?.containerStatuses ?? [])];
    const pull = statuses.find((s) => PULL_REASONS.has(s.state?.waiting?.reason ?? ""));
    if (pull && age > PULL_LIMIT_MS) return lose(c, run, `The build pod's container ${pull.name} could not start: ${pull.state!.waiting!.reason}${pull.state!.waiting!.message ? ` (${pull.state!.waiting!.message})` : ""}`.slice(0, 300));
    if (phase === "Pending" && !statuses.some((s) => s.state?.running || s.state?.terminated)) {
      const sched = pod.status?.conditions?.find((x) => x.type === "PodScheduled");
      const why = sched?.status === "False" ? sched.message ?? sched.reason ?? "not scheduled" : pull ? `${pull.state!.waiting!.reason}` : "starting";
      if (sched?.status === "False" && age > PENDING_LIMIT_MS) return lose(c, run, `No node took the build pod within ${minutes(PENDING_LIMIT_MS)}: ${why}`.slice(0, 300));
      // The text stays the same while the pod waits, so the health check's `since` holds.
      if (age > PENDING_WARN_MS) c.pending.push(`The build pod of task ${t.id} is waiting to start: ${why}`.slice(0, 300));
    }
    return;
  }
  return endStage(c, run, t, pod);
}

async function endStage(c: EnvCtx, run: Run, t: TaskRow, pod: KPod): Promise<void> {
  const { db } = c.d;
  const plan = json<TaskStep[]>(t.plan, []);
  const groups = groupsOf(plan);
  const up = uploadStage(plan);

  if (run.stage === STAGE_TASK) {
    await podLog(c, run, "task", "source");
    if (statusOf(pod, "task")?.state?.terminated?.exitCode !== 0) return lose(c, run, `The task pod could not read its task: ${podFailure(pod)}`);
    // No key is valid while repository code runs; the upload pod gets a new one.
    await db.run(db.k.updateTable("build_pod_runs").set({ key_hash: hashKey(randomBytes(32).toString("hex")) }).where("task_id", "=", run.task_id));
    return next(c, run, t, STAGE_SOURCE);
  }
  if (run.stage === STAGE_SOURCE) {
    await podLog(c, run, "source", "source");
    const term = statusOf(pod, "source")?.state?.terminated;
    // Evicted or killed before it could report: the pod's fault, not the task's.
    if (!term || (term.exitCode !== 0 && !term.message?.trim())) return lose(c, run, `The source pod stopped without a report (${term ? term.reason ?? `exit ${term.exitCode}` : podFailure(pod)})`);
    const r = parsePrepared(term.message);
    if (term.exitCode !== 0 || !r.ok) return finish(c, run, r.ok ? `source failed (exit ${term.exitCode})` : r.summary);
    const image = toolchainImage(c.s, json<Requires>(t.requires, {}).toolchains);
    if (!image) return finish(c, run, `no toolchain image for ${json<Requires>(t.requires, {}).toolchains?.join(", ") || "this task"}`);
    const kept: Kept = { ...r.prepared, image };
    run.prepared = JSON.stringify(kept);
    await db.run(db.k.updateTable("build_pod_runs").set({ prepared: run.prepared }).where("task_id", "=", run.task_id));
    return next(c, run, t, FIRST_GROUP);
  }
  if (run.stage >= FIRST_GROUP && run.stage < up) {
    const g = groups[run.stage - FIRST_GROUP]!;
    const results = json<TaskResult[]>(run.results, []);
    let failed: string | null = null;
    let ran = run.ran;
    for (const [name, fallback] of [[DISK_CONTAINER, "the task's volume is over its size"], [PACKAGES_CONTAINER, "the toolchain image lacks packages the CI gate installs"]] as const) {
      const term = statusOf(pod, name)?.state?.terminated;
      if (!term || failed) continue;
      if (term.exitCode !== 0) {
        await podLog(c, run, name, "environment");
        failed = clean(term.message) || fallback;
      }
    }
    for (const i of failed ? [] : g.steps) {
      const step = plan[i]!;
      const st = statusOf(pod, stepContainer(i))?.state;
      const term = st?.terminated;
      const startedAt = term?.startedAt ?? st?.running?.startedAt;
      if (!term && !startedAt) break;
      await podLog(c, run, stepContainer(i), step.name);
      const end = term?.finishedAt ? Date.parse(term.finishedAt) : c.now();
      const durationMs = Math.max(0, end - (startedAt ? Date.parse(startedAt) : end)) || 0;
      // `timeout` exits 124, or 137 after its grace; a step's own 124 before its limit is an ordinary failure. A step still
      // running when the pod ended was stopped by the pod's deadline.
      const timedOut = !term || ((term.exitCode === 124 || term.exitCode === 137) && durationMs >= stepCap(c.s, step) * 1000 - 2000);
      const oomKilled = term?.reason === "OOMKilled";
      results.push({ step: step.name, exitCode: timedOut ? null : term!.exitCode, durationMs, timedOut, ...(oomKilled ? { oomKilled } : {}) });
      ran = Math.max(ran, i + 1);
      if (timedOut) failed = `${step.name} timed out`;
      else if (term!.exitCode !== 0) failed = pod.status?.reason === "Evicted" ? `${step.name} failed: ${pod.status.message ?? "evicted"}`.slice(0, 300) : `${step.name} failed (exit ${term!.exitCode})`;
      if (failed) break;
    }
    if (!failed && pod.status?.phase === "Failed") return lose(c, run, podFailure(pod));
    await db.run(db.k.updateTable("build_pod_runs").set({ results: JSON.stringify(results), ran, failed }).where("task_id", "=", run.task_id));
    Object.assign(run, { results: JSON.stringify(results), ran, failed });
    const nextGroup = run.stage - FIRST_GROUP + 1;
    if (!failed && nextGroup < groups.length) return next(c, run, t, FIRST_GROUP + nextGroup);
    if (plan.slice(0, ran).some((s) => s.artifacts?.length)) return next(c, run, t, up);
    return finish(c, run, failed);
  }
  if (run.stage === up) {
    await podLog(c, run, "upload", "artifacts");
    if (statusOf(pod, "upload")?.state?.terminated?.exitCode !== 0) await appendLog(db, c.env.id, run.task_id, "artifacts", `artifact upload failed: ${podFailure(pod)}\n`);
    return finish(c, run, run.failed);
  }
  return lose(c, run, `The build pod run is at an unknown stage ${run.stage}`);
}

/** Deletes the stage's pod and starts `stage`. */
async function next(c: EnvCtx, run: Run, t: TaskRow, stage: number): Promise<void> {
  if (run.pod) await c.api!.deletePod(c.s.namespace, run.pod).catch(() => {});
  await c.d.db.run(c.d.db.k.updateTable("build_pod_runs").set({ stage, pod: null, create_tries: 0, failing_since: null, retry_at: null }).where("task_id", "=", run.task_id));
  Object.assign(run, { stage, pod: null, create_tries: 0, failing_since: null, retry_at: null });
  return createStage(c, run, t, stage);
}

/**
 * Creates the pod of the run's stage. Past the task's deadline the task ends instead. A failed create (rate limit, quota,
 * an API error) leaves the run at the stage without a pod and is retried with backoff; after CREATE_TRIES or
 * CREATE_GIVE_UP_MS the task is lost.
 */
async function createStage(c: EnvCtx, run: Run, t: TaskRow, stage: number): Promise<void> {
  const { db } = c.d;
  const plan = json<TaskStep[]>(t.plan, []);
  const groups = groupsOf(plan);
  const left = c.s.deadlineSec - (c.now() - Date.parse(run.started_at)) / 1000;
  if (left < 60) return finish(c, run, run.failed ?? `the task passed its deadline of ${c.s.deadlineSec} s`);
  let spec: (ctx: PodCtx) => object;
  let deadline: number | undefined;
  if (stage === STAGE_SOURCE) spec = (ctx) => sourcePod(ctx, c.image!);
  else if (stage >= FIRST_GROUP && stage < FIRST_GROUP + groups.length) {
    const index = stage - FIRST_GROUP;
    const g = groups[index]!;
    const kept = json<Kept | null>(run.prepared, null);
    if (!kept) return lose(c, run, "The build pod run lost its source report");
    // The steps' limits, a minute each to copy the source, and two minutes to start the pod and measure its volume.
    deadline = g.steps.reduce((n, i) => n + stepCap(c.s, plan[i]!) + 60, 120);
    spec = (ctx) => stepPod(ctx, kept.image, c.image!, plan, g, kept, index === 0, t.repo);
  } else if (stage === uploadStage(plan)) {
    const key = newPodKey(run.task_id);
    await db.run(db.k.updateTable("build_pod_runs").set({ key_hash: hashKey(key) }).where("task_id", "=", run.task_id));
    spec = (ctx) => uploadPod(ctx, c.image!, key, run.ran);
  } else return lose(c, run, `The build pod run is at an unknown stage ${stage}`);
  try {
    await createPod(c, run, stage, spec, deadline);
    await db.run(db.k.updateTable("build_pod_runs").set({ create_tries: 0, failing_since: null, retry_at: null }).where("task_id", "=", run.task_id));
  } catch (e) {
    const now = c.now();
    const tries = run.create_tries + 1;
    const since = run.failing_since ?? new Date(now).toISOString();
    const why = `The next build pod could not be created: ${(e as Error).message}`.slice(0, 300);
    // A create that answered with an error may still have made the pod; with no run naming it, the orphan sweep deletes it.
    Object.assign(run, { pod: null, create_tries: tries, failing_since: since });
    if (tries >= CREATE_TRIES || now - Date.parse(since) > CREATE_GIVE_UP_MS) return lose(c, run, why);
    const retry = new Date(now + Math.min(15_000 * 2 ** (tries - 1), 5 * 60_000)).toISOString();
    await db.run(db.k.updateTable("build_pod_runs").set({ pod: null, create_tries: tries, failing_since: since, retry_at: retry }).where("task_id", "=", run.task_id));
    run.retry_at = retry;
    c.d.log(`pod launcher: task ${run.task_id}: ${why} (try ${tries}, again at ${retry})`);
  }
}

async function finish(c: EnvCtx, run: Run, failed: string | null): Promise<void> {
  const results = json<TaskResult[]>(run.results, []);
  const kept = json<Kept | null>(run.prepared, null);
  const environment: SandboxEnv | undefined = kept
    ? { image: kept.image, baseImage: kept.image, packages: kept.packages, craftFontsRef: kept.craftFontsRef, craftI18nRev: kept.craftI18nRev }
    : undefined;
  const summary = failed ?? (results.length ? `${results.length} step${results.length === 1 ? "" : "s"} passed` : "no step ran");
  try {
    await finishTask(c.d.db, c.env.id, run.task_id, { results, summary, ...(environment ? { environment } : {}) });
  } catch (e) {
    c.d.log(`pod launcher: task ${run.task_id}: finish: ${(e as Error).message}`);
  }
  await removeRun(c, run);
}

/** Runs build-task and workspace passes until `signal` aborts. Started by the runner; a pass with no kubernetes environment reads one table. */
/** `guard` runs before every Kubernetes call that changes something, and stops it by throwing (src/build/kube-api.ts guardedApi). */
export function startPodLauncher(db: Db, signal: AbortSignal, log: (line: string) => void, guard?: () => Promise<void>): void {
  const api = inClusterApi();
  const deps: LauncherDeps = { db, api: api && guard ? guardedApi(api, guard) : api, defaultImage: envVar("IMAGE") ?? null, log };
  const state = new LauncherState();
  const workspaces = new WorkspaceState();
  void (async () => {
    while (!signal.aborted) {
      try {
        await launcherPass(deps, state);
      } catch (e) {
        log(`pod launcher: ${(e as Error).stack ?? e}`);
      }
      try {
        await workspacePass(deps, workspaces);
      } catch (e) {
        log(`workspace launcher: ${(e as Error).stack ?? e}`);
      }
      await sleep(PASS_MS, undefined, { signal }).catch(() => {});
    }
  })();
}
