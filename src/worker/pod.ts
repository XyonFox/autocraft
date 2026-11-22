// `autocraft-server worker pod task|source|upload`: the AutoCraft-image phases of a build task that runs in Kubernetes
// pods (src/build/pods.ts). They run no repository code. The plan's steps run in their own pods, in the toolchain image,
// and the runner reads their results from pod status; nothing here reports a result.
//   task    (network `api`)   fetches the task with the pod key and writes it to the task's volume
//   source  (network `fetch`) source tarball, patch and CI-gate inputs into the volume; its termination message tells the runner what it found
//   upload  (network `api`)   uploads the artifacts of the steps that ran
//   disk    (first in every step pod) fails the pod when the task's volume holds more than the environment's size,
//           which a local-path volume does not enforce itself
//   warm-restore  fills empty target / CARGO_HOME dirs from the warm store's newest signed version; a store or content
//                 failure is a cold start (logged, dirs emptied again, bad blob in `badBlobs`, exit 0)
//   warm-publish  publishes the target and CARGO_HOME dirs as a new version of the key
//   warm-gc       deletes old manifests and unreferenced blobs from the warm store
// Warm phases read AUTOCRAFT_WARM_* (warmOptions below). The store layout is in src/warm/store-client.ts.
import type { KeyObject } from "node:crypto";
import { existsSync, lstatSync, mkdirSync, readdirSync, readFileSync, realpathSync, writeFileSync } from "node:fs";
import { isAbsolute, join, relative } from "node:path";
import type { PodTask } from "../build/tasks.ts";
import { BadBlobError, clearDir, DEFAULT_EXCLUDES, loadEd25519, type Roots } from "../warm/manifest.ts";
import { WarmStore } from "../warm/store-client.ts";
import { checkServiceUrl } from "./config.ts";
import { WorkerApi } from "./client.ts";
import { gateInputs } from "./environment.ts";
import { collectArtifacts, contentTypeOf } from "./executor.ts";
import { resolveGate } from "./gate.ts";
import { applyPatch } from "./patch.ts";
import { downloadAndExtract } from "./tarball.ts";

const MAX_SOURCE_BYTES = 300 * 1024 * 1024;
/** Kubernetes keeps at most 4096 bytes of a termination message. */
const MESSAGE_MAX = 4096;

type PodEnv = { work: string; message: string; key?: string; url?: string; ran: number; fetchImpl?: typeof fetch; log: (s: string) => void };

function podEnv(env: NodeJS.ProcessEnv = process.env): PodEnv {
  return {
    work: env.AUTOCRAFT_POD_WORK || "/work", message: env.AUTOCRAFT_POD_MESSAGE || "/dev/termination-log",
    key: env.AUTOCRAFT_POD_KEY, url: env.AUTOCRAFT_WORKER_URL, ran: Number(env.AUTOCRAFT_POD_RAN ?? 0),
    log: (s) => process.stdout.write(s.endsWith("\n") ? s : `${s}\n`),
  };
}

function api(e: PodEnv): WorkerApi {
  if (!e.key || !e.url) throw new Error("AUTOCRAFT_POD_KEY and AUTOCRAFT_WORKER_URL are required");
  return new WorkerApi(e.url, e.key, e.fetchImpl);
}

async function taskPhase(e: PodEnv): Promise<number> {
  const t = await api(e).podTask();
  writeFileSync(join(e.work, "task.json"), JSON.stringify(t), { mode: 0o600 });
  e.log(`task ${t.id}: ${t.repo}@${t.headSha.slice(0, 12)}, ${t.steps.length} step${t.steps.length === 1 ? "" : "s"}${t.patch ? ", with a patch" : ""}`);
  return 0;
}

/** Writes the report the runner reads from pod status. */
function report(e: PodEnv, o: object): void {
  let text = JSON.stringify(o);
  if (Buffer.byteLength(text) > MESSAGE_MAX) text = JSON.stringify({ ok: false, summary: "source: the CI gate's env is too large for a build pod's report" });
  writeFileSync(e.message, text);
}

async function sourcePhase(e: PodEnv): Promise<number> {
  const fail = (summary: string) => {
    e.log(summary);
    report(e, { ok: false, summary: summary.slice(0, 300) });
    return 1;
  };
  let t: PodTask;
  try {
    t = JSON.parse(readFileSync(join(e.work, "task.json"), "utf8")) as PodTask;
  } catch (err) {
    return fail(`source: the task file is missing (${(err as Error).message})`);
  }
  const src = join(e.work, "src");
  for (const d of [src, join(e.work, "target"), join(e.work, "cache", "cargo"), join(e.work, "ext"), join(e.work, "scratch"), ...t.steps.map((_, i) => join(e.work, "out", String(i)))]) {
    mkdirSync(d, { recursive: true });
  }
  e.log(`fetching source for ${t.repo}@${t.headSha.slice(0, 12)}`);
  try {
    e.log(`unpacked ${await downloadAndExtract(t.sourceUrl, src, MAX_SOURCE_BYTES, undefined, e.fetchImpl)} files`);
  } catch (err) {
    return fail(`source failed: ${(err as Error).message}`);
  }
  if (t.patch !== undefined) {
    const err = await applyPatch(t.patch, src, join(e.work, "scratch"));
    e.log(err ?? "patch applied");
    if (err) return fail(err);
  }
  const out = { ok: true, env: {} as Record<string, string>, mounts: [] as { sub: string; target: string }[], packages: [] as string[], craftFontsRef: null as string | null, craftI18nRev: null as string | null, cacheVersion: null as string | null };
  try {
    const gate = t.baseSha ? await resolveGate(t.repo, t.baseSha, e.fetchImpl) : null;
    if (!t.baseSha || !gate) e.log(`${t.baseSha ? "no CI gate for this repo at the base commit" : "no base commit for this task"}; the steps run in the toolchain image as it is`);
    else {
      const g = await gateInputs(e.work, t.repo, t.baseSha, gate, { fetchImpl: e.fetchImpl, log: e.log });
      const root = realpathSync(e.work);
      Object.assign(out, { env: g.env, packages: gate.packages, craftFontsRef: gate.craftFontsRef, craftI18nRev: g.craftI18nRev, cacheVersion: gate.cacheVersion, mounts: g.mounts.map((m) => ({ sub: relative(root, realpathSync(m.source)), target: m.target })) });
      e.log(`environment: gate ${gate.gateRef} (gate.yml sha256 ${gate.gateSha256?.slice(0, 16) ?? "unknown"}), packages ${gate.packages.join(" ") || "none"}, craft-fonts ${gate.craftFontsRef?.slice(0, 12) ?? "none"}, craft-i18n ${g.craftI18nRev?.slice(0, 12) ?? "none"}, cache version ${gate.cacheVersion ?? "none"}, gate env ${Object.keys(gate.env).join(", ") || "none"}`);
    }
  } catch (err) {
    return fail(`environment failed: ${(err as Error).message}`);
  }
  report(e, out);
  return 0;
}

async function uploadPhase(e: PodEnv): Promise<number> {
  const client = api(e);
  const t = await client.podTask();
  const taken = new Set<string>();
  const budget = { total: 0, count: 0 };
  let n = 0;
  for (const [i, step] of t.steps.slice(0, e.ran).entries()) {
    if (!step.artifacts.length) continue;
    for (const f of collectArtifacts(join(e.work, "out", String(i)), step.artifacts, taken, budget, e.log)) {
      await client.upload(t.id, f.name, readFileSync(f.path), contentTypeOf(f.name));
      e.log(`uploaded ${f.name} (${f.bytes} bytes)`);
      n++;
    }
  }
  e.log(`${n} artifact${n === 1 ? "" : "s"} uploaded`);
  return 0;
}

/** Stops counting past this many entries: a tree that large fails the check as well. */
const DISK_MAX_ENTRIES = 2_000_000;

/** Space used under `root` (allocated blocks), without following links; null past DISK_MAX_ENTRIES entries. */
export function usedBytes(root: string): number | null {
  let total = 0, entries = 0;
  const stack = [root];
  while (stack.length) {
    const dir = stack.pop()!;
    let names: string[];
    try {
      names = readdirSync(dir);
    } catch {
      continue;
    }
    for (const n of names) {
      if (++entries > DISK_MAX_ENTRIES) return null;
      const p = join(dir, n);
      try {
        const st = lstatSync(p);
        total += st.blocks * 512;
        if (st.isDirectory()) stack.push(p);
      } catch { /* removed meanwhile */ }
    }
  }
  return total;
}

function diskPhase(e: PodEnv, limit: number): number {
  const gb = (b: number) => (b / 1024 ** 3).toFixed(1);
  const used = usedBytes(e.work);
  const over = used === null ? `the task's volume holds more than ${DISK_MAX_ENTRIES} files` : used > limit ? `the task's volume holds ${gb(used)} GB, over its ${gb(limit)} GB` : null;
  e.log(over ?? `the task's volume holds ${gb(used!)} GB of ${gb(limit)} GB`);
  if (!over) return 0;
  writeFileSync(e.message, over);
  return 4;
}

/**
 * AUTOCRAFT_WARM_* settings of the warm phases:
 *   STORE_URL        WebDAV root for reads (restore)          STORE_WRITE_URL  WebDAV root for writes (publish, gc)
 *   STORE_USER, STORE_PASSWORD  credentials for the write URL
 *   KEY              warm key                                  VERIFY_KEYS      comma-separated public key PEM files
 *   SIGNING_KEY      private key PEM file (publish)
 *   TARGET, CARGO_HOME  the dirs to fill or publish; restore skips one that is unset. Publish them at the paths the
 *                    build used: the manifest records them, and cargo's fingerprints only match at the same paths.
 *   CHECKOUT, IMAGE, ENV_HASH  recorded in the manifest header; RUSTC_FILE holds `rustc -vV` output
 *   REPO, COMMIT     default to the task file's repo and head
 *   EXCLUDE          comma-separated exclusion patterns (default DEFAULT_EXCLUDES)
 *   RECHECK          comma-separated sha256s a restore reported as badBlobs (publish repairs them)
 *   GC_KEEP (3), GC_MIN_AGE_HOURS (24, publish uses it too), GC_CAP_GB (150)
 * A configuration error fails every phase (exit 1); only store and content problems make warm-restore a cold start.
 */
function warmOptions(env: NodeJS.ProcessEnv, e: PodEnv) {
  const v = (n: string) => env[`AUTOCRAFT_WARM_${n}`] || undefined;
  const need = (n: string) => v(n) ?? fail(`AUTOCRAFT_WARM_${n} is required`);
  const fail = (msg: string): never => {
    throw new Error(msg);
  };
  const url = (n: string) => {
    const u = need(n);
    checkServiceUrl(u, `AUTOCRAFT_WARM_${n}`);
    return u;
  };
  const dir = (n: string) => {
    const d = v(n);
    if (d !== undefined && !isAbsolute(d)) fail(`AUTOCRAFT_WARM_${n} must be an absolute path`);
    return d;
  };
  const key = (n: string, kind: "public" | "private") => (f: string): KeyObject => {
    try {
      return loadEd25519(readFileSync(f.trim()), kind);
    } catch (err) {
      return fail(`AUTOCRAFT_WARM_${n}: cannot load ${f.trim()}: ${(err as Error).message}`);
    }
  };
  const count = (n: string, dflt: number) => {
    const x = Number(v(n) ?? dflt);
    return Number.isFinite(x) && x > 0 ? x : fail(`AUTOCRAFT_WARM_${n} must be a positive number`);
  };
  const store = (write: boolean) => new WarmStore({
    readUrl: write ? url("STORE_WRITE_URL") : url("STORE_URL"),
    writeUrl: write ? url("STORE_WRITE_URL") : undefined,
    auth: write ? { user: need("STORE_USER"), password: need("STORE_PASSWORD") } : undefined,
    fetchImpl: e.fetchImpl,
  });
  const roots = (): Partial<Roots> => ({ target: dir("TARGET"), cargo: dir("CARGO_HOME") });
  return {
    v, need, store, roots, count,
    key: () => need("KEY"),
    verifyKeys: () => need("VERIFY_KEYS").split(",").filter((f) => f.trim()).map(key("VERIFY_KEYS", "public")),
    signKey: () => key("SIGNING_KEY", "private")(need("SIGNING_KEY")),
    list: (n: string) => (v(n) ?? "").split(",").map((x) => x.trim()).filter(Boolean),
    exclude: () => (v("EXCLUDE") ? v("EXCLUDE")!.split(",").map((x) => x.trim()).filter(Boolean) : DEFAULT_EXCLUDES),
  };
}

async function warmRestorePhase(e: PodEnv, env: NodeJS.ProcessEnv): Promise<number> {
  const w = warmOptions(env, e);
  const key = w.key();
  const roots = w.roots();
  const verifyKeys = w.verifyKeys();
  const store = w.store(false);
  const dirs = Object.values(roots).filter((d): d is string => d !== undefined);
  const out = (o: object) => writeFileSync(e.message, JSON.stringify(o).slice(0, MESSAGE_MAX));
  const cold = (summary: string, badBlobs: string[] = []) => {
    e.log(`${summary}; cold start`);
    out({ ok: true, warm: null, badBlobs, summary: summary.slice(0, 300) });
    return 0;
  };
  if (!dirs.length) throw new Error("AUTOCRAFT_WARM_TARGET or AUTOCRAFT_WARM_CARGO_HOME is required");
  // Only a dir that was empty is emptied again after a failure.
  const busy = dirs.filter((d) => existsSync(d) && readdirSync(d).length);
  if (busy.length) throw new Error(`${busy.join(", ")} is not empty`);
  const t0 = Date.now();
  try {
    const r = await store.restore(key, roots, verifyKeys);
    if (!r) return cold(`warm restore: the store has no version of ${key}`);
    const s = r.stats;
    const summary = `warm restore: ${key}@${r.latest.commit.slice(0, 12)}, ${s.files} files (${(s.bytes / 1024 ** 3).toFixed(2)} GB), ${s.fetched} fetched, ${s.linked + s.copied} linked or copied, in ${((Date.now() - t0) / 1000).toFixed(1)} s`;
    e.log(summary);
    out({ ok: true, warm: { key, commit: r.latest.commit, stored: r.latest.stored, image: r.manifest.header.image, envHash: r.manifest.header.envHash, files: s.files, bytes: s.bytes, fetched: s.fetched, fetchedBytes: s.fetchedBytes }, summary });
    return 0;
  } catch (err) {
    try {
      for (const d of dirs) if (existsSync(d)) clearDir(d);
    } catch (cleanup) {
      // Never let a build run on a partial tree.
      const summary = `warm restore failed (${(err as Error).message}) and its partial tree could not be removed: ${(cleanup as Error).message}`;
      e.log(summary);
      out({ ok: false, summary: summary.slice(0, 300) });
      return 1;
    }
    return cold(`warm restore failed: ${(err as Error).message}`, err instanceof BadBlobError ? [err.sha256] : []);
  }
}

async function warmPublishPhase(e: PodEnv, env: NodeJS.ProcessEnv): Promise<number> {
  const w = warmOptions(env, e);
  const roots = w.roots();
  if (!roots.target || !roots.cargo) throw new Error("AUTOCRAFT_WARM_TARGET and AUTOCRAFT_WARM_CARGO_HOME are required");
  let task: PodTask | undefined;
  if (!w.v("REPO") || !w.v("COMMIT")) task = JSON.parse(readFileSync(join(e.work, "task.json"), "utf8")) as PodTask;
  const rustcFile = w.v("RUSTC_FILE");
  const rustc = rustcFile && existsSync(rustcFile) ? readFileSync(rustcFile, "utf8").trim().slice(0, 4096) : null;
  if (!rustc) e.log("warm publish: no rustc -vV output; the manifest records none");
  const s = await w.store(true).publish({
    key: w.key(), repo: w.v("REPO") ?? task!.repo, commit: w.v("COMMIT") ?? task!.headSha,
    image: w.v("IMAGE") ?? null, rustc, envHash: w.v("ENV_HASH") ?? null, checkout: w.v("CHECKOUT") ?? null,
    source: { target: roots.target, cargoHome: roots.cargo }, exclude: w.exclude(), signKey: w.signKey(), log: e.log,
    recheck: w.list("RECHECK").filter((x) => /^[0-9a-f]{64}$/.test(x)), minBlobAgeMs: w.count("GC_MIN_AGE_HOURS", 24) * 3600_000,
  });
  writeFileSync(e.message, JSON.stringify({ ok: true, commit: s.latest.commit, files: s.files, bytes: s.bytes, blobs: s.blobs, uploaded: s.uploaded, uploadedBytes: s.uploadedBytes, repaired: s.repaired }));
  return 0;
}

async function warmGcPhase(e: PodEnv, env: NodeJS.ProcessEnv): Promise<number> {
  const w = warmOptions(env, e);
  await w.store(true).gc({ verifyKeys: w.verifyKeys(), keep: Math.floor(w.count("GC_KEEP", 3)), minBlobAgeMs: w.count("GC_MIN_AGE_HOURS", 24) * 3600_000, capBytes: w.count("GC_CAP_GB", 150) * 1024 ** 3, log: e.log });
  return 0;
}

export async function podPhase(phase: string | undefined, env: NodeJS.ProcessEnv = process.env, fetchImpl?: typeof fetch): Promise<number> {
  const e = { ...podEnv(env), ...(fetchImpl ? { fetchImpl } : {}) };
  try {
    if (phase === "task") return await taskPhase(e);
    if (phase === "source") return await sourcePhase(e);
    if (phase === "upload") return await uploadPhase(e);
    if (phase === "disk") return diskPhase(e, Number(env.AUTOCRAFT_POD_DISK_LIMIT) || 0);
    if (phase === "warm-restore") return await warmRestorePhase(e, env);
    if (phase === "warm-publish") return await warmPublishPhase(e, env);
    if (phase === "warm-gc") return await warmGcPhase(e, env);
  } catch (err) {
    e.log(`${phase} failed: ${(err as Error).message}`);
    return 1;
  }
  e.log("usage: autocraft-server worker pod task|source|upload|disk|warm-restore|warm-publish|warm-gc");
  return 2;
}
