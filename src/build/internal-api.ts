// The runner's /internal/ API on the worker listener: what a dashboard without the runner's disk asks the runner for
// (src/remote/runner-client.ts). Every request carries a token the dashboard signed for this method, path and body
// with the key both derive from session_key; the dashboard has checked the user's access, and the runner acts as the
// login the token names. Without the key (no secrets file on the runner) every request is refused.
import { readFile } from "node:fs/promises";
import type { IncomingMessage, ServerResponse } from "node:http";
import { ACCOUNT_ACTIONS } from "../agents/account-actions.ts";
import { AccountError, listAccounts } from "../agents/accounts.ts";
import type { Db } from "../db/index.ts";
import { INTERNAL_HEADER, verifyInternal } from "../remote/runner-client.ts";
import { absolute } from "./artifacts.ts";

const BODY_LIMIT = 64 * 1024;

const send = (res: ServerResponse, status: number, body: unknown): void => {
  const data = JSON.stringify(body);
  res.writeHead(status, { "content-type": "application/json; charset=utf-8", "cache-control": "no-store", "x-content-type-options": "nosniff", "content-length": Buffer.byteLength(data) });
  res.end(data);
};

async function bodyOf(req: IncomingMessage): Promise<string | null> {
  const parts: Buffer[] = [];
  let size = 0;
  for await (const c of req as AsyncIterable<Buffer>) {
    size += c.length;
    if (size > BODY_LIMIT) return null;
    parts.push(c);
  }
  return Buffer.concat(parts).toString("utf8");
}

let refusalLogged = false;

/** Handles one /internal/ request; `key` is the runner's session_key, null when it has none. */
export async function handleInternal(db: Db, key: string | null, req: IncomingMessage, res: ServerResponse, url: URL): Promise<void> {
  if (!key) {
    if (!refusalLogged) { refusalLogged = true; console.error("runner: dashboard requests are refused: the runner has no session_key"); }
    return send(res, 503, { error: "the runner has no secrets file, so it takes no dashboard requests" });
  }
  const method = req.method ?? "GET";
  const body = await bodyOf(req);
  if (body === null) return send(res, 413, { error: "request too large" });
  const header = req.headers[INTERNAL_HEADER];
  const login = verifyInternal(key, typeof header === "string" ? header : undefined, method, url.pathname, body);
  if (!login) return send(res, 401, { error: "missing, expired or wrong dashboard token" });

  const art = /^\/internal\/artifacts\/(\d{1,15})\/([^/]{1,200})$/.exec(url.pathname);
  if (art && method === "GET") {
    let name: string;
    try { name = decodeURIComponent(art[2]!); } catch { return send(res, 404, { error: "no such artifact" }); }
    const row = await db.get(db.k.selectFrom("build_artifacts").select("file").where("task_id", "=", Number(art[1])).where("name", "=", name));
    const path = row && absolute(row.file);
    if (!path) return send(res, 404, { error: "no such artifact" });
    let data: Buffer;
    try { data = await readFile(path); } catch { return send(res, 404, { error: "the artifact file is gone" }); }
    res.writeHead(200, { "content-type": "application/octet-stream", "cache-control": "no-store", "content-length": data.length });
    return void res.end(data);
  }

  const acc = /^\/internal\/accounts\/([a-z-]{1,20})$/.exec(url.pathname);
  if (acc && method === "POST") {
    let args: { id?: unknown; input?: unknown; sync?: unknown };
    try { args = body ? (JSON.parse(body) as typeof args) : {}; } catch { return send(res, 400, { error: "the body is not valid JSON" }); }
    try {
      if (acc[1] === "list") return send(res, 200, await listAccounts(db, Date.now(), args.sync !== false));
      const action = Object.hasOwn(ACCOUNT_ACTIONS, acc[1]!) ? ACCOUNT_ACTIONS[acc[1]!] : undefined;
      if (!action) return send(res, 404, { error: "no such account action" });
      return send(res, 200, await action(db, args.id ?? undefined, args.input ?? undefined, login));
    } catch (e) {
      if (e instanceof AccountError) return send(res, e.status, { error: e.message, account: true });
      throw e;
    }
  }
  return send(res, 404, { error: "no such endpoint" });
}
