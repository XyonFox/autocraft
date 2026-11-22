// The worker API: a listener of its own in the runner process (AUTOCRAFT_WORKER_LISTEN,
// default 127.0.0.1:8981), separate from the dashboard. Workers authenticate with a bearer
// key; every call refreshes the environment's heartbeat. Bodies are size-capped and
// everything a worker sends is stored as data, never executed, rendered or put in a prompt.
// Under /remote/ it also serves what a remote call reads from the runner's disk (the item and
// reference tools, tarball mounts) to a dashboard that does not share that disk; those requests
// carry the call's lease secret, checked here as the dashboard checks it.
import { createWriteStream, unlinkSync } from "node:fs";
import { createServer, type IncomingMessage, type Server, type ServerResponse } from "node:http";
import type { AddressInfo } from "node:net";
import { parseListen } from "../dash/server/config.ts";
import { TokenBucket } from "../dash/server/limits.ts";
import { RateLimiter } from "../dash/server/util.ts";
import { ArtifactError, MAX_ARTIFACT_BYTES, NAME_RE, storeArtifact, tempPath } from "./artifacts.ts";
import { authenticate, BuildError, hello, touch } from "./envs.ts";
import { appendLog, claim, finishTask, heartbeat, MAX_CHUNK_BYTES, ownedTask, podTask, startTask } from "./tasks.ts";
import { dbReady, serveHealth } from "../health.ts";
import { envVar } from "../env.ts";
import type { Db } from "../db/index.ts";
import { archiveOf, callTaskTool, taskLease } from "../remote/tools.ts";
import { ITEM_SERVER } from "../itemtools/index.ts";
import { REFERENCE_SERVER } from "../remote/bundle.ts";
import { handleInternal } from "./internal-api.ts";

/** Largest tool-call body the runner takes for a dashboard (the dashboard's own limit for these tools). */
const REMOTE_TOOL_BODY_LIMIT = 2 * 1024 * 1024;

export const DEFAULT_WORKER_LISTEN = "127.0.0.1:8981";
/** Largest JSON body: a log chunk of 64 KB with its JSON escaping, or a result list. */
export const JSON_LIMIT = 4 * MAX_CHUNK_BYTES;

export type WorkerServerOptions = {
  db: Db;
  /** host:port; default AUTOCRAFT_WORKER_LISTEN or 127.0.0.1:8981 (port 0 picks a free one). */
  listen?: string;
  log?: (line: string) => void;
  /** Calls a key may make: a burst and a refill per second. */
  burst?: number;
  perSec?: number;
  /** The secrets file's session_key, which signs the dashboard's /internal/ requests (src/build/internal-api.ts); without it they are refused. */
  internalKey?: string | null;
};

class Refusal extends Error {
  status: number;
  headers: Record<string, string>;
  constructor(status: number, message: string, headers: Record<string, string> = {}) {
    super(message);
    this.status = status;
    this.headers = headers;
  }
}

function reply(res: ServerResponse, status: number, body: unknown, extra: Record<string, string> = {}): void {
  const data = JSON.stringify(body);
  res.writeHead(status, {
    "content-type": "application/json; charset=utf-8", "cache-control": "no-store", "x-content-type-options": "nosniff",
    "content-length": Buffer.byteLength(data), ...extra,
  });
  res.end(data);
}

async function readJson(req: IncomingMessage, limit: number): Promise<unknown> {
  const declared = Number(req.headers["content-length"] ?? 0);
  if (declared > limit) throw new Refusal(413, "request too large", { connection: "close" });
  const chunks: Buffer[] = [];
  let size = 0;
  for await (const c of req as AsyncIterable<Buffer>) {
    size += c.length;
    if (size > limit) throw new Refusal(413, "request too large", { connection: "close" });
    chunks.push(c);
  }
  if (size === 0) return {};
  if ((req.headers["content-type"] ?? "").split(";")[0]!.trim().toLowerCase() !== "application/json") throw new Refusal(415, "send the body as application/json");
  try {
    return JSON.parse(Buffer.concat(chunks).toString("utf8"));
  } catch {
    throw new Refusal(400, "the body is not valid JSON");
  }
}

/** Streams the body to a temp file, stopping at the cap. */
function receive(req: IncomingMessage, taskId: number, limit: number): Promise<string> {
  const declared = Number(req.headers["content-length"] ?? 0);
  if (declared > limit) return Promise.reject(new Refusal(413, "an artifact is at most 50 MB", { connection: "close" }));
  const path = tempPath(taskId);
  const out = createWriteStream(path, { mode: 0o600 });
  let size = 0;
  return new Promise((resolve, reject) => {
    const fail = (e: Error) => {
      out.destroy();
      try {
        unlinkSync(path);
      } catch { /* not created */ }
      reject(e);
    };
    req.on("data", (c: Buffer) => {
      size += c.length;
      if (size > limit) {
        req.removeAllListeners("data");
        fail(new Refusal(413, "an artifact is at most 50 MB", { connection: "close" }));
      } else out.write(c);
    });
    req.on("error", fail);
    req.on("aborted", () => fail(new Error("upload aborted")));
    req.on("end", () => out.end(() => resolve(path)));
  });
}

const taskId = (s: string | undefined): number => {
  if (!/^\d{1,15}$/.test(s ?? "")) throw new Refusal(400, "not a task id");
  return Number(s);
};

export function handler(o: WorkerServerOptions) {
  const { db } = o;
  const calls = new TokenBucket(o.burst ?? 120, o.perSec ?? 40);
  const badAuth = new RateLimiter(30, 60_000);
  // Per lease, as the dashboard limits them: tool calls and tarball fetches.
  const remoteTools = new RateLimiter(300, 60_000);
  const remoteArchives = new RateLimiter(20, 60_000);

  async function remoteRoute(req: IncomingMessage, res: ServerResponse, url: URL): Promise<void> {
    const secret = /^Bearer (\S{1,200})$/.exec(String(req.headers.authorization ?? ""))?.[1];
    const lease = secret ? await taskLease(db, secret) : null;
    if (!lease || !secret) {
      if (!badAuth.allow(req.socket.remoteAddress ?? "?")) throw new Refusal(429, "too many failed attempts", { "retry-after": "60" });
      throw new Refusal(401, "a live lease secret is required", { "www-authenticate": "Bearer" });
    }
    const method = req.method ?? "GET";
    if (url.pathname === "/remote/tarball") {
      if (method !== "GET") throw new Refusal(405, "use GET");
      if (!remoteArchives.allow(String(lease.id))) throw new Refusal(429, "too many requests", { "retry-after": "60" });
      const tar = await archiveOf(lease, url.searchParams.get("repo") ?? "", url.searchParams.get("sha") ?? "", { local: true });
      if (!tar) throw new Refusal(404, "the call has no such tarball");
      res.writeHead(200, { "content-type": "application/x-tar", "cache-control": "no-store", "x-content-type-options": "nosniff" });
      // A failed archive cuts the response, so the dashboard and the helper see an error rather than a short tree.
      tar.on("error", () => res.destroy());
      res.on("close", () => tar.destroy());
      return void tar.pipe(res);
    }
    const t = /^\/remote\/tools\/([a-z]+)\/([a-z][a-z0-9_]{0,60})$/.exec(url.pathname);
    if (!t) throw new Refusal(404, "no such endpoint");
    if (method !== "POST") throw new Refusal(405, "use POST");
    const server = t[1]!;
    if (server !== ITEM_SERVER && server !== REFERENCE_SERVER) throw new Refusal(404, "the runner serves only the item and reference tools");
    if (!remoteTools.allow(String(lease.id))) throw new Refusal(429, "too many requests", { "retry-after": "60" });
    const args = await readJson(req, REMOTE_TOOL_BODY_LIMIT);
    const r = await callTaskTool(db, lease, secret, server, t[2]!, args, { local: true });
    return reply(res, 200, { text: r.text, ...(r.isError && { isError: true }) });
  }

  async function route(req: IncomingMessage, res: ServerResponse): Promise<void> {
    const url = new URL(req.url ?? "/", "http://worker.invalid");
    if (url.pathname.startsWith("/remote/")) return await remoteRoute(req, res, url);
    if (url.pathname.startsWith("/internal/")) return await handleInternal(db, o.internalKey ?? null, req, res, url);
    const m = /^Bearer (\S{1,200})$/.exec(String(req.headers.authorization ?? ""));
    const addr = req.socket.remoteAddress ?? "?";
    const env = m ? await authenticate(db, m[1]!) : null;
    if (!env) {
      if (!badAuth.allow(addr)) throw new Refusal(429, "too many failed attempts", { "retry-after": "60" });
      throw new Refusal(401, "missing or wrong worker key", { "www-authenticate": "Bearer" });
    }
    const wait = calls.take(env.id);
    if (wait) throw new Refusal(429, "too many requests", { "retry-after": String(wait) });
    const path = url.pathname;
    const method = req.method ?? "GET";

    // A build pod's key reads its one task and uploads that task's artifacts; results come from pod status, read by the runner.
    if (env.taskId !== undefined) {
      if (method === "GET" && path === "/worker/pod/task") return reply(res, 200, await podTask(db, env.id, env.taskId));
      const a = /^\/worker\/tasks\/(\d{1,15})\/artifacts\/[^/]+$/.exec(path);
      if (!a || method !== "PUT" || Number(a[1]) !== env.taskId) throw new Refusal(403, "a build pod key may only read its task and upload its artifacts");
    } else if (path === "/worker/pod/task") throw new Refusal(403, "only a build pod key reads a pod task");

    if (method === "POST" && path === "/worker/hello") {
      return reply(res, 200, await hello(db, env.id, await readJson(req, 16 * 1024)));
    }
    if (method === "POST" && path === "/worker/claim") {
      const b = (await readJson(req, 1024)) as { free?: unknown; pausedReason?: unknown };
      if (typeof b.free !== "number" || !Number.isInteger(b.free) || b.free < 0 || b.free > 1024) throw new Refusal(400, "free must be a whole number");
      const paused = typeof b.pausedReason === "string" ? b.pausedReason.replace(/[\u0000-\u001f\u007f]+/g, " ").trim().slice(0, 200) || null : null;
      await touch(db, env.id, paused);
      return reply(res, 200, await claim(db, env.id, b.free));
    }
    const t = /^\/worker\/tasks\/([^/]+)\/(start|log|finish|heartbeat|artifacts\/([^/]+))$/.exec(path);
    if (t) {
      await touch(db, env.id);
      const id = taskId(t[1]);
      const action = t[2]!.startsWith("artifacts/") ? "artifacts" : t[2]!;
      if (action === "artifacts") {
        if (method !== "PUT") throw new Refusal(405, "use PUT");
        await ownedTask(db, env.id, id);
        let name: string;
        try {
          name = decodeURIComponent(t[3]!);
        } catch {
          name = "";
        }
        if (!NAME_RE.test(name)) throw new Refusal(400, "artifact names are 1 to 100 letters, digits, dots, underscores or hyphens");
        const tmp = await receive(req, id, MAX_ARTIFACT_BYTES);
        try {
          await ownedTask(db, env.id, id);
        } catch (e) {
          unlinkSync(tmp);
          throw e;
        }
        return reply(res, 200, await storeArtifact(db, id, name, tmp));
      }
      if (method !== "POST") throw new Refusal(405, "use POST");
      if (action === "start") return reply(res, 200, await startTask(db, env.id, id));
      if (action === "heartbeat") return reply(res, 200, await heartbeat(db, env.id, id));
      const body = (await readJson(req, action === "log" ? JSON_LIMIT : 256 * 1024)) as Record<string, unknown>;
      if (action === "log") return reply(res, 200, await appendLog(db, env.id, id, body.step, body.chunk));
      return reply(res, 200, await finishTask(db, env.id, id, body));
    }
    throw new Refusal(404, "no such endpoint");
  }

  return async (req: IncomingMessage, res: ServerResponse): Promise<void> => {
    if (await serveHealth(req, res, async () => await dbReady(db))) return;
    try {
      await route(req, res);
    } catch (e) {
      if (e instanceof Refusal) reply(res, e.status, { error: e.message }, e.headers);
      else if (e instanceof BuildError || e instanceof ArtifactError) reply(res, e.status, { error: e.message });
      else {
        o.log?.(`worker api: ${(e as Error).stack ?? e}`);
        if (!res.headersSent) reply(res, 500, { error: "server error" });
      }
    }
  };
}

export async function startWorkerServer(o: WorkerServerOptions): Promise<{ url: string; server: Server; close: () => Promise<void> }> {
  const { host, port } = parseListen(o.listen ?? envVar("WORKER_LISTEN") ?? DEFAULT_WORKER_LISTEN);
  const serve = handler(o);
  const server = createServer({ requestTimeout: 120_000, headersTimeout: 15_000, maxHeaderSize: 8 * 1024 }, (req, res) => void serve(req, res));
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(port, host, resolve);
  });
  const a = server.address() as AddressInfo;
  return {
    url: `http://${host.includes(":") ? `[${host}]` : host}:${a.port}`, server,
    close: () => new Promise<void>((r) => (server.closeAllConnections(), server.close(() => r()))),
  };
}
