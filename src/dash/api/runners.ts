// The signed-in user's Task Runners (session routes): their list, approving or denying a runner's device login
// (src/remote/device-login.ts), setting a runner's scopes, and removing one. Maintainers and operators approve and deny;
// anyone lists, sets the scopes of and removes their own. Answering is granted on the rule of answering connector tokens
// (mcp.ts): only while answering is available, by an operator or by a maintainer where maintainers settle decisions.
// Removing deletes the credential: a lease it holds goes back to the queue at its next heartbeat (liveLease).
import { maintainerWritesOn } from "../../settlers.ts";
import { audit } from "../audit.ts";
import { answering } from "../mcp/index.ts";
import { revokeToken, setRunnerScopes, type Scopes } from "../mcp/tokens.ts";
import { HttpError, RateLimiter, share } from "../server/util.ts";
import type { Req } from "../types.ts";
import { approveLogin, denyLogin, normalizeCode, pendingLogin } from "../../remote/device-login.ts";
import { listHelpers } from "../../remote/helpers.ts";
import { inferenceMode } from "../../remote/mode.ts";
import type { Machine, PendingRunnerLogin, RunnersReply } from "../../remote/protocol.ts";
import type { Db } from "../../db/index.ts";

/** Lookups of a user code that found no waiting login, per login: guessing codes is slow. */
const failedLookups = new RateLimiter(share(10), 10 * 60_000);

const body = (req: Req): Record<string, unknown> => {
  const b = req.json;
  if (!b || typeof b !== "object" || Array.isArray(b)) throw new HttpError(400, "the body must be a JSON object");
  return b as Record<string, unknown>;
};

const staffOnly = (req: Req): void => {
  if (req.user.role === "viewer") throw new HttpError(403, "viewers cannot add Task Runners");
};

/** Runs `find` on a user code; a code that finds nothing counts against the login's lookups. */
async function byCode<T>(req: Req, raw: unknown, find: (code: string) => Promise<T | null>): Promise<T> {
  const key = req.user.login.toLowerCase();
  if (!failedLookups.allow(key)) throw new HttpError(429, "too many codes that matched no login; wait a few minutes");
  const code = normalizeCode(raw);
  let found: T | null;
  try {
    found = code ? await find(code) : null;
  } catch (e) {
    failedLookups.refund(key);
    throw e;
  }
  if (found === null) throw new HttpError(404, "no Task Runner login is waiting with that code; it may have expired");
  failedLookups.refund(key);
  return found;
}

/** Whether the signed-in user may let a runner answer. */
const mayGrantAnswer = async (db: Db, req: Req): Promise<boolean> =>
  answering.enabled && (req.user.role === "operator" || (req.user.role === "maintainer" && await maintainerWritesOn(db)));

/** The `agent` and `answer` ticks of a request body, checked; `hadAnswer`: the runner answers already, so keeping it grants nothing. */
async function scopesOf(db: Db, req: Req, b: Record<string, unknown>, hadAnswer = false): Promise<Scopes> {
  if (typeof b.agent !== "boolean" || typeof b.answer !== "boolean") throw new HttpError(400, "agent and answer must be true or false");
  if (b.agent && req.user.role === "viewer") throw new HttpError(403, "viewers cannot give a Task Runner agent access");
  if (b.answer && !hadAnswer && !await mayGrantAnswer(db, req)) {
    throw new HttpError(403, answering.enabled ? "only operators, and maintainers where they may settle decisions, let a Task Runner answer decisions" : "answering from an agent is not available yet");
  }
  return { agent: b.agent, answer: b.answer };
}

export async function list(db: Db, req: Req): Promise<RunnersReply> {
  const helpers = await listHelpers(db, req.user.login);
  const extra = new Map((await db.all(db.k.selectFrom("mcp_tokens as t").leftJoin("remote_helpers as h", "h.token_id", "t.id")
    .select(["t.id", "t.machine", "t.created_at", "t.expires_at", "t.can_agent", "t.can_answer", "h.helper_version"])
    .where("t.login", "=", req.user.login).where("t.kind", "=", "runner")))
    .map((r) => [Number(r.id), r]));
  return {
    runners: helpers.flatMap((h) => {
      const x = extra.get(h.tokenId);
      return x ? [{ ...h, machine: x.machine ? JSON.parse(x.machine) as Machine : null, helperVersion: x.helper_version, createdAt: x.created_at, expiresAt: x.expires_at, agent: x.can_agent === 1, answer: x.can_answer === 1 }] : [];
    }),
    answering: await mayGrantAnswer(db, req),
  };
}

export async function pending(db: Db, req: Req): Promise<PendingRunnerLogin> {
  staffOnly(req);
  return await byCode(req, req.query.get("code"), async (code) => await pendingLogin(db, code, req.ip ?? ""));
}

export async function approve(db: Db, req: Req): Promise<RunnersReply> {
  staffOnly(req);
  if (await inferenceMode(db) !== "user-local") throw new HttpError(409, "the instance runs model calls on its own accounts; Task Runners take none now");
  const b = body(req);
  if (b.otherMachine !== undefined && typeof b.otherMachine !== "boolean") throw new HttpError(400, "otherMachine must be true or false");
  const scopes = await scopesOf(db, req, b);
  const out = await byCode(req, b.code, async (code) => await approveLogin(db, req.user.login, code, b.label, scopes, req.ip ?? "", b.otherMachine === true));
  await audit(db, req.user.login, "runner-approve", req.user.login, { id: out.row.id, label: out.row.label ?? undefined, machine: out.machine ?? undefined, ip: out.ip ?? undefined, ...scopes });
  return await list(db, req);
}

export async function scopes(db: Db, req: Req): Promise<RunnersReply> {
  const b = body(req);
  if (!Number.isInteger(b.id)) throw new HttpError(400, "id must be a Task Runner id");
  const had = await db.get(db.k.selectFrom("mcp_tokens").select("can_answer").where("id", "=", b.id as number).where("login", "=", req.user.login).where("kind", "=", "runner"));
  if (!had) throw new HttpError(404, "you have no such Task Runner");
  const s = await scopesOf(db, req, b, had.can_answer === 1);
  if (!await setRunnerScopes(db, req.user.login, b.id as number, s)) throw new HttpError(404, "you have no such Task Runner");
  await audit(db, req.user.login, "runner-scopes", req.user.login, { id: b.id, ...s });
  return await list(db, req);
}

export async function deny(db: Db, req: Req): Promise<{ ok: true }> {
  staffOnly(req);
  const out = await byCode(req, body(req).code, async (code) => await denyLogin(db, req.user.login, code));
  await audit(db, req.user.login, "runner-deny", req.user.login, { label: out.label ?? undefined, machine: out.machine ?? undefined, ip: out.ip ?? undefined });
  return { ok: true };
}

export async function revoke(db: Db, req: Req): Promise<RunnersReply> {
  const id = body(req).id;
  if (!Number.isInteger(id)) throw new HttpError(400, "id must be a Task Runner id");
  if (!await revokeToken(db, req.user.login, id as number, "runner")) throw new HttpError(404, "you have no such Task Runner");
  await audit(db, req.user.login, "runner-revoke", req.user.login, { id });
  return await list(db, req);
}
