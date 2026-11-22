// Workspace rows and the one way to run something in a workspace pod. Every exec checks that the pod the row names is
// the pod labelled with the row's id, runs the helper `ws` with fixed argv positions for anything that came from a
// session, and records the call in workspace_calls. Output from the pod is untrusted: it is decoded as UTF-8 with
// replacement and stripped of control characters before it is stored or printed.
import type { Db } from "../db/index.ts";
import { BuildError } from "./envs.ts";
import { KubeError, type ExecOptions, type ExecResult, type KubeApi } from "./kube-api.ts";
import { settingsOf } from "./pods.ts";
import { WS_POD_PATH } from "./workspace.ts";
import { DEFAULT_WORKSPACE_DEADLINE_SEC, WS_CONTAINER, workspaceSelector, type WorkspaceLimits } from "./workspace-pods.ts";

export const WORKSPACE_STATES = ["requested", "starting", "seeding", "ready", "parked", "ended", "failed"] as const;
export type WorkspaceStateName = (typeof WORKSPACE_STATES)[number];
/** A live row owns its volume; one in POD_STATES also owns its pod. */
export const LIVE_STATES: WorkspaceStateName[] = ["requested", "starting", "seeding", "ready", "parked"];
export const POD_STATES: WorkspaceStateName[] = ["starting", "seeding", "ready"];
/** Output kept per call, after the helper's own cap. */
export const MAX_CALL_OUTPUT = 64 * 1024;

const SHA = /^[0-9a-f]{40}$/;
const REPO = /^[A-Za-z0-9._-]{1,100}$/;

/** Every column but the checkpoint, which only a resume and `workspace show` read. */
export const WORKSPACE_COLUMNS = [
  "id", "repo", "base_sha", "base_commit", "app", "env_id", "state", "state_at", "pod", "pod_created_at", "create_tries", "pvc", "node", "limits",
  "checkpoint_at", "last_call_at", "next_call", "error", "owner", "created_at", "updated_at", "ended_at", "remote_call_id",
  "volume_fresh", "resume_tries", "checkpoint_error", "restored", "restored_at",
] as const;
export const getWorkspace = async (db: Db, id: number) => await db.get(db.k.selectFrom("workspaces").select(WORKSPACE_COLUMNS).where("id", "=", id));
export type WorkspaceRow = NonNullable<Awaited<ReturnType<typeof getWorkspace>>>;
export const getCheckpoint = async (db: Db, id: number): Promise<Uint8Array | null> =>
  (await db.get(db.k.selectFrom("workspaces").select(["checkpoint"]).where("id", "=", id)))?.checkpoint ?? null;

/** A running call holds the pod until this long past its own time limit. */
export const CALL_GRACE_MS = 30_000;

export const limitsOf = (w: { limits: string }): WorkspaceLimits => {
  try {
    const o = JSON.parse(w.limits) as Partial<WorkspaceLimits>;
    return { deadlineSec: typeof o.deadlineSec === "number" && o.deadlineSec > 0 ? o.deadlineSec : DEFAULT_WORKSPACE_DEADLINE_SEC };
  } catch {
    return { deadlineSec: DEFAULT_WORKSPACE_DEADLINE_SEC };
  }
};

export type WorkspaceRequest = { repo: string; baseSha: string; envId: string; app?: string | null; limits?: Partial<WorkspaceLimits>; owner: string | null };

/** Records a `requested` workspace; the launcher creates its volume and pod when the environment has a free slot. */
export async function createWorkspace(db: Db, r: WorkspaceRequest, now = Date.now()): Promise<number> {
  if (!REPO.test(r.repo)) throw new BuildError(400, "repo must be a repository name");
  if (!SHA.test(r.baseSha)) throw new BuildError(400, "the base must be a full 40-character commit sha");
  const env = await db.get(db.k.selectFrom("build_envs").select(["kind"]).where("id", "=", r.envId));
  if (env?.kind !== "kubernetes") throw new BuildError(400, `${r.envId} is not a kubernetes environment`);
  const deadlineSec = r.limits?.deadlineSec ?? DEFAULT_WORKSPACE_DEADLINE_SEC;
  if (!Number.isInteger(deadlineSec) || deadlineSec < 300 || deadlineSec > 24 * 3600) throw new BuildError(400, "deadlineSec must be 300 to 86400");
  const at = new Date(now).toISOString();
  const row = await db.get(db.k.insertInto("workspaces").values({
    repo: r.repo, base_sha: r.baseSha, app: r.app ?? null, env_id: r.envId, state: "requested", state_at: at, limits: JSON.stringify({ deadlineSec }),
    owner: r.owner, created_at: at, updated_at: at,
  }).returning("id"));
  return Number(row!.id);
}

/**
 * Moves a row from one of `from` to `to` with `set`; false when it was no longer in `from` (another pass, the CLI or an
 * end got there first), so a step that ran in the background never revives a row that ended meanwhile.
 */
export async function moveWorkspace(db: Db, id: number, from: WorkspaceStateName[], to: WorkspaceStateName, set: Partial<Omit<WorkspaceRow, "id" | "state">> = {}, now = Date.now()): Promise<boolean> {
  const at = new Date(now).toISOString();
  const r = await db.run(db.k.updateTable("workspaces")
    .set({ ...set, state: to, state_at: at, updated_at: at, ...(to === "ended" || to === "failed" ? { ended_at: at } : {}) })
    .where("id", "=", id).where("state", "in", from));
  return r.changes > 0;
}

/** Failed resumes after which a parked workspace stays parked, with its volume and checkpoint, until someone looks at it. */
export const RESUME_TRIES = 3;

/**
 * Brings a parked workspace back: the launcher starts a new pod when a slot is free, on the kept volume when there is one,
 * else on a new volume rebuilt from the base and the checkpoint. True when the row is coming back or up; false when it is
 * gone, ended or failed, or its resumes failed RESUME_TRIES times (its error says why).
 */
export async function resumeWorkspace(db: Db, id: number, now = Date.now()): Promise<boolean> {
  const w = await getWorkspace(db, id);
  if (!w) return false;
  if (w.state !== "parked") return POD_STATES.includes(w.state as WorkspaceStateName) || w.state === "requested";
  if (w.resume_tries >= RESUME_TRIES) return false;
  if (await moveWorkspace(db, id, ["parked"], "requested", { create_tries: 0, ...(w.pvc ? {} : { node: null }) }, now)) return true;
  return await resumeWorkspace(db, id, now);
}

/** Ends a live workspace; the launcher's next pass deletes its pod and volume. */
export async function endWorkspace(db: Db, id: number, now = Date.now()): Promise<boolean> {
  return await moveWorkspace(db, id, LIVE_STATES, "ended", {}, now);
}

// ---- the helper's output -------------------------------------------------------

export type WsHeader =
  | { kind: "status"; state: "exited" | "killed" | "timed-out" | "running"; exit: number | null; ms: number; bytes: number }
  | { kind: "started"; n: number }
  | { kind: "ok"; fields: string[] }
  | { kind: "error"; code: string; message: string };

/** The first line `ws` prints; null when it is not one of its headers. */
export function parseHeader(line: string): WsHeader | null {
  const f = line.trim().split(" ");
  if (f[0] !== "WS") return null;
  if (f[1] === "ok") return { kind: "ok", fields: f.slice(2) };
  if (f[1] === "error" && f[2]) return { kind: "error", code: f[2], message: f.slice(3).join(" ") };
  if (f[1] === "started" && /^\d+$/.test(f[2] ?? "")) return { kind: "started", n: Number(f[2]) };
  const st = f[1];
  if ((st === "exited" || st === "killed" || st === "timed-out" || st === "running") && f.length === 5 && /^(-?\d+|-)$/.test(f[2]!) && /^\d+$/.test(f[3]!) && /^\d+$/.test(f[4]!)) {
    return { kind: "status", state: st, exit: f[2] === "-" ? null : Number(f[2]), ms: Number(f[3]), bytes: Number(f[4]) };
  }
  return null;
}

/** Bytes from the pod as text that is safe to store, log and print: UTF-8 with replacement, no control characters but \n and \t. */
export function cleanOutput(b: Uint8Array): string {
  return new TextDecoder("utf-8", { fatal: false }).decode(b).replace(/[\u0000-\u0008\u000b-\u001f\u007f-\u009f]/g, "");
}

export const describeHeader = (h: WsHeader | null): string =>
  !h ? "no helper header" : h.kind === "error" ? `${h.code}: ${h.message}` : h.kind === "status" ? `${h.state}${h.exit !== null ? ` ${h.exit}` : ""} after ${h.ms} ms` : h.kind === "started" ? `started ${h.n}` : `ok ${h.fields.join(" ")}`;

/** A copy of `v` small enough to store: strings, arrays and nesting cut, so its JSON is always whole. */
export function capJson(v: unknown, depth = 0): unknown {
  if (typeof v === "string") return v.length > 2000 ? `${v.slice(0, 2000)}…` : v;
  if (v === null || typeof v === "number" || typeof v === "boolean") return v;
  if (depth >= 4 || typeof v !== "object") return null;
  if (Array.isArray(v)) return v.slice(0, 40).map((x) => capJson(x, depth + 1));
  return Object.fromEntries(Object.entries(v).slice(0, 40).map(([k, x]) => [k.slice(0, 100), capJson(x, depth + 1)]));
}

// ---- exec ----------------------------------------------------------------------

/** The namespace and pod of a workspace whose pod is up, after checking that the pod labelled with its id is the one the row names. */
export async function workspacePod(db: Db, api: KubeApi, id: number): Promise<{ ns: string; pod: string; row: WorkspaceRow }> {
  const row = await getWorkspace(db, id);
  if (!row) throw new BuildError(404, `no workspace ${id}`);
  if ((row.state !== "seeding" && row.state !== "ready") || !row.pod) throw new BuildError(409, `workspace ${id} is ${row.state}, with no pod to run in`);
  const env = await db.get(db.k.selectFrom("build_envs").select(["settings"]).where("id", "=", row.env_id));
  const ns = settingsOf(env?.settings ?? null).namespace;
  // An earlier pod of the workspace may still be shutting down (a resume while its node is gone); it never runs anything.
  const pods = (await api.listPods(ns, workspaceSelector(id))).filter((p) => !p.metadata.deletionTimestamp);
  const pod = pods.find((p) => p.metadata.name === row.pod);
  if (pods.length !== 1 || !pod || pod.metadata.labels?.["craft.build/workspace"] !== String(id)) {
    throw new BuildError(409, `workspace ${id}: the pod labelled with its id is not ${row.pod}`);
  }
  if (pod.status?.phase !== "Running") throw new BuildError(409, `workspace ${id}: its pod is not running`);
  return { ns, pod: row.pod, row };
}

/** Runs fixed argv in a workspace's pod, after the pod check. For AutoCraft's own commands; session input goes through execWs. */
export async function execInWorkspace(db: Db, api: KubeApi, id: number, argv: string[], opts: ExecOptions): Promise<ExecResult> {
  const { ns, pod } = await workspacePod(db, api, id);
  return await api.exec(ns, pod, WS_CONTAINER, argv, opts);
}

/** One helper call's raw outcome: the parsed first line and the bytes after it, uncleaned (file content, images). */
export type WsRaw = { header: WsHeader | null; body: Buffer; stderr: string; truncated: boolean; error?: string };

/** Splits the helper's stdout into its header line and the raw bytes after it. */
export function splitHeader(stdout: Buffer): { header: WsHeader | null; body: Buffer } {
  const nl = stdout.indexOf(0x0a);
  const header = parseHeader(cleanOutput(nl < 0 ? stdout : stdout.subarray(0, nl)));
  return { header, body: header ? (nl < 0 ? Buffer.alloc(0) : stdout.subarray(nl + 1)) : stdout };
}

/**
 * Runs `ws <argv>` in the workspace's pod after the pod check (or in `at`, a pod the caller just checked) and returns
 * the raw outcome, with no call row: the caller records it. Throws when the pod check fails or the exec never started.
 */
export async function execHelper(db: Db, api: KubeApi, id: number, argv: string[], opts: ExecOptions, at?: { ns: string; pod: string }): Promise<WsRaw> {
  const { ns, pod } = at ?? await workspacePod(db, api, id);
  const r = await api.exec(ns, pod, WS_CONTAINER, [WS_POD_PATH, ...argv], opts);
  return { ...splitHeader(r.stdout), stderr: cleanOutput(r.stderr).slice(0, 8192), truncated: r.truncated, ...(r.error !== undefined ? { error: r.error } : {}) };
}

/** Takes the workspace's next `ws run` call number. */
export async function takeCallNumber(db: Db, id: number): Promise<number> {
  const r = await db.get(db.k.updateTable("workspaces").set((eb) => ({ next_call: eb("next_call", "+", 1) })).where("id", "=", id).returning("next_call"));
  if (!r) throw new BuildError(404, `no workspace ${id}`);
  return Number(r.next_call) - 1;
}

export type WsCallOptions = {
  /** What the call is for: a session tool name, or the launcher's `seed` and `checkpoint`. */
  tool: string;
  args?: unknown;
  stdin?: ExecOptions["stdin"];
  timeoutMs: number;
  maxOut?: number;
  /** A queued workspace_calls row to run; otherwise one is added. */
  row?: number;
  /** Counts as activity: the workspace is not parked for 30 min after it. The launcher's own calls do not count. */
  touch?: boolean;
};
export type WsCallResult = { callId: number; n: number | null; header: WsHeader | null; output: string; stderr: string; error?: string; truncated: boolean };

/**
 * Runs `ws <argv>` in the workspace's pod and records it. For `run`, the call number is taken from the row's counter and
 * goes in after `run`, so callers pass `["run", <timeout>, ..., "--", ...cmd]`. Throws when the pod check fails or the exec
 * never started; the call row is failed then.
 */
export async function execWs(db: Db, api: KubeApi, id: number, argv: string[], o: WsCallOptions, now: () => number = Date.now): Promise<WsCallResult> {
  const iso = () => new Date(now()).toISOString();
  const at = await workspacePod(db, api, id);
  const n = argv[0] === "run" ? await takeCallNumber(db, id) : null;
  const full = n === null ? argv : ["run", String(n), ...argv.slice(1)];
  const args = JSON.stringify(capJson(o.args ?? { argv }));
  const deadline = new Date(now() + o.timeoutMs + CALL_GRACE_MS).toISOString();
  let callId = o.row;
  if (callId === undefined) {
    const r = await db.get(db.k.insertInto("workspace_calls").values({ workspace_id: id, n, tool: o.tool, args, state: "running", created_at: iso(), started_at: iso(), deadline_at: deadline }).returning("id"));
    callId = Number(r!.id);
  } else {
    await db.run(db.k.updateTable("workspace_calls").set({ n, state: "running", started_at: iso(), deadline_at: deadline }).where("id", "=", callId).where("workspace_id", "=", id));
  }
  const touch = async () => {
    if (o.touch) await db.run(db.k.updateTable("workspaces").set({ last_call_at: iso(), updated_at: iso() }).where("id", "=", id));
  };
  await touch();
  const finish = async (state: "done" | "failed", result: object) => {
    await db.run(db.k.updateTable("workspace_calls").set({ state, result: JSON.stringify(result), finished_at: iso() }).where("id", "=", callId!));
    await touch();
  };
  let r: WsRaw;
  try {
    r = await execHelper(db, api, id, full, { stdin: o.stdin, maxOut: o.maxOut ?? MAX_CALL_OUTPUT, timeoutMs: o.timeoutMs }, at);
  } catch (e) {
    await finish("failed", { error: e instanceof KubeError ? e.message : String((e as Error).message) });
    throw e;
  }
  const header = r.header;
  const out: WsCallResult = {
    callId, n, header, output: cleanOutput(r.body).slice(0, MAX_CALL_OUTPUT), stderr: r.stderr,
    truncated: r.truncated, ...(r.error !== undefined ? { error: r.error } : {}),
  };
  await finish(header && header.kind !== "error" && r.error === undefined ? "done" : "failed",
    { header, output: out.output, ...(out.stderr ? { stderr: out.stderr } : {}), ...(r.error ? { error: r.error } : {}), ...(r.truncated ? { truncated: true } : {}) });
  return out;
}

// ---- diff stat ------------------------------------------------------------------

/**
 * A `git diff --stat`-like summary of a `git diff --binary` (a checkpoint), read from its headers and hunks: one line per
 * file (at most `maxFiles`) and a total. The diff is the session's own data; names are cleaned of control characters.
 */
export function diffStat(diff: Uint8Array, maxFiles = 40): string {
  const files: { name: string; add: number; del: number; binary: boolean }[] = [];
  let cur: (typeof files)[number] | null = null;
  let inHunk = false;
  for (const line of cleanOutput(diff).split("\n")) {
    const head = /^diff --git a\/(.*) b\/(.*)$/.exec(line);
    if (head) {
      cur = { name: head[2]!, add: 0, del: 0, binary: false };
      files.push(cur);
      inHunk = false;
    } else if (!cur) continue;
    else if (line.startsWith("@@")) inHunk = true;
    else if (!inHunk && (line.startsWith("GIT binary patch") || line.startsWith("Binary files "))) cur.binary = true;
    else if (inHunk && line.startsWith("+")) cur.add++;
    else if (inHunk && line.startsWith("-")) cur.del++;
  }
  if (!files.length) return "no changes";
  const shown = files.slice(0, maxFiles).map((f) => ` ${f.name} | ${f.binary ? "binary" : `+${f.add} -${f.del}`}`);
  const add = files.reduce((a, f) => a + f.add, 0), del = files.reduce((a, f) => a + f.del, 0);
  return [...shown, ...(files.length > maxFiles ? [` (${files.length - maxFiles} more files)`] : []),
    ` ${files.length} ${files.length === 1 ? "file" : "files"} changed, ${add} insertions(+), ${del} deletions(-)`].join("\n");
}
