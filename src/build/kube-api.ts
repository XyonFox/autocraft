// The few Kubernetes API calls the pod launcher makes, behind an interface so tests use a fake.
// The in-cluster client uses the runner container's service-account token and CA, read at each
// call because the projected token is rotated. Only `node:` modules: no client library.
import { readFileSync } from "node:fs";
import { request as httpRequest } from "node:http";
import { request as httpsRequest } from "node:https";
import { wsConnect, WsHandshakeError, type WsConn } from "./ws-client.ts";

export const SA_DIR = "/var/run/secrets/kubernetes.io/serviceaccount";

export type ContainerState = {
  waiting?: { reason?: string; message?: string };
  running?: { startedAt?: string };
  terminated?: { exitCode: number; reason?: string; message?: string; startedAt?: string; finishedAt?: string };
};
export type ContainerStatus = { name: string; ready?: boolean; state?: ContainerState };
export type KPod = {
  metadata: { name: string; labels?: Record<string, string>; creationTimestamp?: string; deletionTimestamp?: string };
  spec?: { activeDeadlineSeconds?: number; nodeName?: string };
  status?: {
    phase?: "Pending" | "Running" | "Succeeded" | "Failed" | "Unknown";
    reason?: string;
    message?: string;
    conditions?: { type: string; status: string; reason?: string; message?: string }[];
    initContainerStatuses?: ContainerStatus[];
    containerStatuses?: ContainerStatus[];
  };
};
export type KPvc = { metadata: { name: string; labels?: Record<string, string>; deletionTimestamp?: string } };
export type KNode = { metadata: { name: string }; status?: { conditions?: { type: string; status: string; lastTransitionTime?: string }[] } };

export interface KubeApi {
  listPods(ns: string, selector: string): Promise<KPod[]>;
  createPod(ns: string, pod: object): Promise<void>;
  /** A pod that is already gone counts as deleted. */
  deletePod(ns: string, name: string): Promise<void>;
  podLog(ns: string, name: string, container: string, limitBytes: number): Promise<string>;
  listPvcs(ns: string, selector: string): Promise<KPvc[]>;
  createPvc(ns: string, pvc: object): Promise<void>;
  deletePvc(ns: string, name: string): Promise<void>;
  /** The node, or null when there is none of that name. Throws (403) when the service account may not read nodes. */
  getNode(name: string): Promise<KNode | null>;
  /** A SelfSubjectAccessReview: whether this service account may `verb` the `resource` in `ns`. */
  canI(ns: string, verb: string, resource: string): Promise<boolean>;
  /**
   * Runs `argv` in a container and returns its output and exit code. Throws a KubeError when the
   * command never started (no connection, refused upgrade); once it may have run, every outcome is
   * an ExecResult.
   */
  exec(ns: string, pod: string, container: string, argv: string[], opts: ExecOptions): Promise<ExecResult>;
}

/** `api` with `guard` awaited before every call that changes the cluster or runs a command in it (a fenced leader's check). */
export function guardedApi(api: KubeApi, guard: () => Promise<void>): KubeApi {
  return {
    ...api,
    createPod: async (ns, pod) => (await guard(), await api.createPod(ns, pod)),
    deletePod: async (ns, name) => (await guard(), await api.deletePod(ns, name)),
    createPvc: async (ns, pvc) => (await guard(), await api.createPvc(ns, pvc)),
    deletePvc: async (ns, name) => (await guard(), await api.deletePvc(ns, name)),
    exec: async (ns, pod, container, argv, opts) => (await guard(), await api.exec(ns, pod, container, argv, opts)),
  };
}

export type ExecOptions = {
  /** A stream that fails ends the exec without closing stdin, so the command never takes a cut-off input as complete. */
  stdin?: Buffer | AsyncIterable<Buffer>;
  /** Bytes of stdout and stderr together; past it the stream is closed and `truncated` set. */
  maxOut: number;
  /** From the upgrade to the end of the stream. */
  timeoutMs: number;
  /** Default 10 s. */
  connectTimeoutMs?: number;
};
/** `exitCode` is null when the command's end was not seen (timeout, truncation, lost stream) or exec failed; `error` says why. */
export type ExecResult = { exitCode: number | null; stdout: Buffer; stderr: Buffer; truncated: boolean; error?: string };

/** Where an API server is and how to authenticate to it. `tls: false` is for tests only. */
export type KubeConn = { host: string; port: number; tls?: boolean; ca?: string | Buffer; token?: string; cert?: string | Buffer; key?: string | Buffer };

export class KubeError extends Error {
  status: number;
  constructor(status: number, message: string) {
    super(message);
    this.status = status;
  }
}

const enc = encodeURIComponent;

/** The client for the cluster the runner runs in, or null outside a cluster (no KUBERNETES_SERVICE_HOST). */
export function inClusterApi(env: NodeJS.ProcessEnv = process.env, dir = SA_DIR): KubeApi | null {
  const host = env.KUBERNETES_SERVICE_HOST;
  if (!host) return null;
  const port = Number(env.KUBERNETES_SERVICE_PORT || 443);
  return kubeApi(() => {
    try {
      return { host, port, token: readFileSync(`${dir}/token`, "utf8").trim(), ca: readFileSync(`${dir}/ca.crt`) };
    } catch (e) {
      throw new KubeError(0, `no service-account token in the runner container (${(e as NodeJS.ErrnoException).code ?? "error"})`);
    }
  });
}

const clean = (s: string): string => s.replace(/[\u0000-\u001f\u007f]+/g, " ").trim().slice(0, 300);

/** The API message of an error body: a Status object's `message`, or the body's start. */
function statusMessage(text: string): string {
  try {
    const m = (JSON.parse(text) as { message?: string }).message;
    if (m) return m;
  } catch { /* not JSON */ }
  return text.slice(0, 300);
}

/** The client for an explicit API server; `conn` is called at each request, so credentials may rotate. */
export function kubeApi(conn: KubeConn | (() => KubeConn)): KubeApi {
  const get = typeof conn === "function" ? conn : () => conn;

  function call(method: string, path: string, body?: object, accept = "application/json"): Promise<string> {
    let c: KubeConn;
    try {
      c = get();
    } catch (e) {
      return Promise.reject(e);
    }
    const data = body === undefined ? undefined : JSON.stringify(body);
    return new Promise((resolve, reject) => {
      const req = (c.tls === false ? httpRequest : httpsRequest)({
        host: c.host, port: c.port, method, path, timeout: 30_000,
        ...(c.tls === false ? {} : { ca: c.ca, cert: c.cert, key: c.key }),
        headers: {
          ...(c.token ? { authorization: `Bearer ${c.token}` } : {}),
          accept,
          ...(data !== undefined ? { "content-type": "application/json", "content-length": Buffer.byteLength(data) } : {}),
        },
      }, (res) => {
        const chunks: Buffer[] = [];
        res.on("data", (chunk: Buffer) => chunks.push(chunk));
        res.on("end", () => {
          const text = Buffer.concat(chunks).toString("utf8");
          const status = res.statusCode ?? 0;
          if (status >= 200 && status < 300) return resolve(text);
          reject(new KubeError(status, `${method} ${path.split("?")[0]}: HTTP ${status} ${statusMessage(text)}`));
        });
        res.on("error", reject);
      });
      req.on("timeout", () => req.destroy(new KubeError(0, `${method} ${path.split("?")[0]}: timed out`)));
      req.on("error", (e) => reject(e instanceof KubeError ? e : new KubeError(0, `${method} ${path.split("?")[0]}: ${e.message}`)));
      req.end(data);
    });
  }
  const gone = (e: unknown) => {
    if (!(e instanceof KubeError && e.status === 404)) throw e;
  };
  const pods = (ns: string) => `/api/v1/namespaces/${enc(ns)}/pods`;
  const pvcs = (ns: string) => `/api/v1/namespaces/${enc(ns)}/persistentvolumeclaims`;

  return {
    listPods: async (ns, selector) => (JSON.parse(await call("GET", `${pods(ns)}?labelSelector=${enc(selector)}`)) as { items: KPod[] }).items,
    createPod: async (ns, pod) => void (await call("POST", pods(ns), pod)),
    deletePod: (ns, name) => call("DELETE", `${pods(ns)}/${enc(name)}?gracePeriodSeconds=5`).then(() => {}, gone),
    podLog: (ns, name, container, limitBytes) => call("GET", `${pods(ns)}/${enc(name)}/log?container=${enc(container)}&limitBytes=${Math.max(1, limitBytes)}`, undefined, "*/*"),
    listPvcs: async (ns, selector) => (JSON.parse(await call("GET", `${pvcs(ns)}?labelSelector=${enc(selector)}`)) as { items: KPvc[] }).items,
    createPvc: async (ns, pvc) => void (await call("POST", pvcs(ns), pvc)),
    deletePvc: (ns, name) => call("DELETE", `${pvcs(ns)}/${enc(name)}`).then(() => {}, gone),
    getNode: (name) => call("GET", `/api/v1/nodes/${enc(name)}`).then((t) => JSON.parse(t) as KNode, (e: unknown) => {
      if (e instanceof KubeError && e.status === 404) return null;
      throw e;
    }),
    canI: async (ns, verb, resource) => {
      const r = JSON.parse(await call("POST", "/apis/authorization.k8s.io/v1/selfsubjectaccessreviews", {
        apiVersion: "authorization.k8s.io/v1", kind: "SelfSubjectAccessReview", spec: { resourceAttributes: { namespace: ns, verb, resource } },
      })) as { status?: { allowed?: boolean } };
      return r.status?.allowed === true;
    },
    exec: async (ns, pod, container, argv, opts) => {
      const c = get();
      const query = [`container=${enc(container)}`, "stdout=true", "stderr=true", "stdin=true", ...argv.map((a) => `command=${enc(a)}`)].join("&");
      let ws: WsConn;
      try {
        ws = await wsConnect({
          tls: c.tls !== false, host: c.host, port: c.port, path: `${pods(ns)}/${enc(pod)}/exec?${query}`,
          ca: c.ca, cert: c.cert, key: c.key,
          headers: c.token ? { authorization: `Bearer ${c.token}` } : {},
          protocol: EXEC_PROTOCOL,
          connectTimeoutMs: opts.connectTimeoutMs ?? 10_000,
        });
      } catch (e) {
        const status = e instanceof WsHandshakeError ? e.status : 0;
        const detail = e instanceof WsHandshakeError && e.body ? `${e.message} ${statusMessage(e.body)}` : (e as Error).message;
        throw new KubeError(status, `exec in ${pod}: ${clean(detail)}`);
      }
      return execStream(ws, opts);
    },
  };
}

/** v5 adds channel 255, which lets the client close the command's stdin. */
const EXEC_PROTOCOL = "v5.channel.k8s.io";
/** Payload of one stdin message, channel byte included. */
const STDIN_FRAME = 64 * 1024;

function execStream(ws: WsConn, o: ExecOptions): Promise<ExecResult> {
  return new Promise((resolve) => {
    const out: Buffer[] = [], err: Buffer[] = [];
    let used = 0, truncated = false, ended = false, sawStatus = false;
    let exitCode: number | null = null, error: string | undefined;
    const end = () => {
      if (ended) return;
      ended = true;
      clearTimeout(timer);
      ws.close();
      resolve({ exitCode, stdout: Buffer.concat(out), stderr: Buffer.concat(err), truncated, ...(error !== undefined ? { error } : {}) });
    };
    const timer = setTimeout(() => {
      exitCode = null;
      error = "timed out";
      end();
    }, o.timeoutMs);
    ws.start({
      onMessage: (m) => {
        if (ended || m.length === 0) return;
        const ch = m[0], data = m.subarray(1);
        if (ch === 1 || ch === 2) {
          const take = data.subarray(0, Math.max(0, o.maxOut - used));
          if (take.length) {
            (ch === 1 ? out : err).push(Buffer.from(take));
            used += take.length;
          }
          if (take.length < data.length) {
            truncated = true;
            end();
          }
        } else if (ch === 3) {
          sawStatus = true;
          ({ exitCode, error } = execStatus(data));
        }
      },
      onClose: (code, reason) => {
        if (!sawStatus) error = clean(`the stream closed without an exit status (${code}${reason ? ` ${reason}` : ""})`);
        end();
      },
    });
    // stdin is always requested and always closed, so a command without input reads EOF at once.
    void (async () => {
      const input = o.stdin ?? Buffer.alloc(0);
      try {
        for await (const chunk of Buffer.isBuffer(input) ? [input] : input) {
          for (let i = 0; i < chunk.length && !ended && !ws.closed; i += STDIN_FRAME - 1) {
            const part = chunk.subarray(i, i + STDIN_FRAME - 1);
            const f = Buffer.allocUnsafe(part.length + 1);
            f[0] = 0;
            part.copy(f, 1);
            if (!ws.send(f)) await ws.drained();
          }
          if (ended || ws.closed) return;
        }
      } catch (e) {
        exitCode = null;
        error = clean(`the input failed: ${(e as Error).message}`);
        return end();
      }
      if (!ended) ws.send(Buffer.from([255, 0]));
    })();
  });
}

/** The exit code from a channel-3 Status: 0 on Success, n on NonZeroExitCode, else null with the message. */
function execStatus(data: Buffer): { exitCode: number | null; error?: string } {
  let s: { status?: string; reason?: string; message?: string; details?: { causes?: { reason?: string; message?: string }[] } };
  try {
    s = JSON.parse(data.toString("utf8"));
  } catch {
    return { exitCode: null, error: "unreadable exec status" };
  }
  if (s.status === "Success") return { exitCode: 0 };
  if (s.reason === "NonZeroExitCode") {
    const n = Number(s.details?.causes?.find((c) => c.reason === "ExitCode")?.message);
    if (Number.isInteger(n)) return { exitCode: n };
  }
  return { exitCode: null, error: clean(s.message || s.reason || "exec failed") || "exec failed" };
}
