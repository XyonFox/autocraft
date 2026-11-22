// Claude accounts, agent runs and rates over JSON. Everyone signed in reads;
// only operators change an account (audited as `account-change`). The box
// config's directories never reach a response: accounts are returned by id and label.
import type { AgentRunFilter, AgentRunState, ClaudeAccount, JevStatus } from "../../../web/src/api/types.ts";
import { AccountError, setUsagePause, usagePause } from "../../agents/accounts.ts";
import { accountAction, accountList } from "../../agents/account-actions.ts";
import { jevEnabled, jevOutage, setJevEnabled } from "../../agents/jev.ts";
import { getRun, listRuns, rates as ratesOf, RunQueryError } from "../../agents/runs.ts";
import { HttpError } from "../server/util.ts";
import type { Req } from "../types.ts";
import { hiddenRepos } from "./visible.ts";
import { canSeeInternals } from "./staff.ts";
import { SECURITY_STEP } from "../../security/rules.ts";
import type { Db } from "../../db/index.ts";

const wrap = async <T>(f: () => Promise<T | Promise<T>>): Promise<T> => {
  try {
    return await f();
  } catch (e) {
    if (e instanceof AccountError || e instanceof RunQueryError) throw new HttpError(e.status, e.message);
    throw e;
  }
};

/** Operators see every account; others only the enabled ones. */
export const accounts = async (db: Db, req: Req): Promise<ClaudeAccount[]> =>
  (await accountList(db, Date.now(), true, req.user.login))
    .filter((a) => a.enabled || req.user.role === "operator")
    // The sign-in URL belongs to the operator who started it.
    .map((a) => (req.user.role === "operator" ? a : { ...a, login: null }));

export const putAccount = async (db: Db, req: Req) => wrap(async () => await accountAction(db, "update", req.params.id, req.json, req.user.login));

export const getPause = async (db: Db) => await usagePause(db);
export const putPause = async (db: Db, req: Req) => wrap(async () => await setUsagePause(db, req.json, req.user.login));

export const getJev = async (db: Db): Promise<JevStatus> => {
  const o = await jevOutage(db);
  return { enabled: await jevEnabled(db), unavailable: o ? { reason: o.reason, since: o.since } : null };
};
export const putJev = async (db: Db, req: Req) => wrap(async () => {
  const enabled = (req.json as { enabled?: unknown } | null)?.enabled;
  if (typeof enabled !== "boolean") throw new HttpError(400, "enabled must be true or false");
  await setJevEnabled(db, enabled, req.user.login);
  return await getJev(db);
});

export const postAccount = async (db: Db, req: Req) => wrap(async () => await accountAction(db, "add", undefined, req.json, req.user.login));
export const loginStart = async (db: Db, req: Req) => wrap(async () => await accountAction(db, "login-start", req.params.id, undefined, req.user.login));
export const loginCode = async (db: Db, req: Req) => wrap(async () => await accountAction(db, "login-code", req.params.id, req.json, req.user.login));
export const loginCancel = async (db: Db, req: Req) => wrap(async () => await accountAction(db, "login-cancel", req.params.id, undefined, req.user.login));
export const tokenStart = async (db: Db, req: Req) => wrap(async () => await accountAction(db, "token-start", req.params.id, undefined, req.user.login));
export const tokenCode = async (db: Db, req: Req) => wrap(async () => await accountAction(db, "token-code", req.params.id, req.json, req.user.login));
export const tokenCancel = async (db: Db, req: Req) => wrap(async () => await accountAction(db, "token-cancel", req.params.id, undefined, req.user.login));
export const tokenRemove = async (db: Db, req: Req) => wrap(async () => await accountAction(db, "token-remove", req.params.id, undefined, req.user.login));
export const accountSignOut = async (db: Db, req: Req) => wrap(async () => await accountAction(db, "sign-out", req.params.id, undefined, req.user.login));
export const accountRefresh = async (db: Db, req: Req) => wrap(async () => await accountAction(db, "refresh", req.params.id, undefined, req.user.login));

function intParam(req: Req, name: string): number | undefined {
  const v = req.query.get(name);
  if (v === null || v === "") return undefined;
  if (!/^\d{1,6}$/.test(v)) throw new HttpError(400, `${name} must be a whole number`);
  return Number(v);
}

export const list = async (db: Db, req: Req) => wrap(async () => {
  const q = (k: string) => req.query.get(k) || undefined;
  const f: AgentRunFilter = {
    state: q("state") as AgentRunState | "recent" | undefined, step: q("step"), account: q("account"), repo: q("repo"), cursor: q("cursor"), limit: intParam(req, "limit"),
  };
  return await listRuns(db, f, await hiddenRepos(db, req), canSeeInternals(req));
});

// The prompt and output texts hold item and comment text verbatim: operators and maintainers only.
export const detail = async (db: Db, req: Req) => wrap(async () => {
  if (!/^\d{1,15}$/.test(req.params.id ?? "")) throw new HttpError(400, "not a run id");
  return await getRun(db, Number(req.params.id), { hidden: await hiddenRepos(db, req), texts: canSeeInternals(req) });
});

export const rates = async (db: Db, req: Req) => wrap(async () => {
  const r = await ratesOf(db, req.query.get("window") ?? "24h", Date.now(), await hiddenRepos(db, req));
  return canSeeInternals(req) ? r : { ...r, stages: r.stages.filter((s) => s.step !== SECURITY_STEP) };
});
