// The Claude account operations the dashboard offers, run where the account directories are: in this process when they
// are on its disk, otherwise on the runner through its /internal/ API (src/remote/runner-client.ts), which runs the same
// functions as the signed-in operator. Replies are the ClaudeAccount views the UI already shows; no directory, token or
// credential comes back.
import type { ClaudeAccount } from "../../web/src/api/types.ts";
import type { Db } from "../db/index.ts";
import { callRunner, runnerClient, RunnerError } from "../remote/runner-client.ts";
import { AccountError, listAccounts, updateAccount } from "./accounts.ts";
import { addAccount, beginLogin, beginToken, refreshNow, removeToken, sendCode, sendTokenCode, signOut, stopLogin, stopToken } from "./manage.ts";

type Action = (db: Db, id: unknown, input: unknown, by: string) => Promise<ClaudeAccount>;

export const ACCOUNT_ACTIONS: Record<string, Action> = {
  add: (db, _id, input, by) => addAccount(db, input, by),
  update: (db, id, input, by) => updateAccount(db, id, input, by),
  "login-start": (db, id, _input, by) => beginLogin(db, id, by),
  "login-code": (db, id, input, by) => sendCode(db, id, input, by),
  "login-cancel": (db, id, _input, by) => stopLogin(db, id, by),
  "token-start": (db, id, _input, by) => beginToken(db, id, by),
  "token-code": (db, id, input, by) => sendTokenCode(db, id, input, by),
  "token-cancel": (db, id, _input, by) => stopToken(db, id, by),
  "token-remove": (db, id, _input, by) => removeToken(db, id, by),
  "sign-out": (db, id, _input, by) => signOut(db, id, by),
  refresh: (db, id, _input, by) => refreshNow(db, id, by),
};

/**
 * The runner's own AccountError answers read as the AccountError the local call would have thrown. Its other refusals
 * are failures between the two processes and answer 502, so the browser does not take a 401 for a lost sign-in.
 */
const asAccountError = (e: unknown): never => {
  if (!(e instanceof RunnerError)) throw e;
  if (e.answered) throw new AccountError(e.status, e.message);
  if (e.status === 401) throw new AccountError(502, "The dashboard and the runner do not share session_key (or their clocks differ)");
  if (e.status === 503) throw new AccountError(502, "The runner has no session_key");
  throw new AccountError(502, `The runner answered ${e.status}: ${e.message}`);
};

export async function accountAction(db: Db, action: keyof typeof ACCOUNT_ACTIONS, id: unknown, input: unknown, by: string): Promise<ClaudeAccount> {
  const c = runnerClient();
  if (!c) return await ACCOUNT_ACTIONS[action]!(db, id, input, by);
  try {
    return (await (await callRunner(c, "POST", `/internal/accounts/${action}`, by, { id: id ?? null, input: input ?? null })).json()) as ClaudeAccount;
  } catch (e) {
    return asAccountError(e);
  } finally {
    forgetAccountList();
  }
}

/** The runner's list, kept this long: the live and attention pages read it on every refresh. */
const LIST_TTL_MS = 2_000;
const LIST_TIMEOUT_MS = 5_000;
const listMemo = new Map<boolean, { at: number; list: Promise<ClaudeAccount[]> }>();
/** The last list the runner answered, served while it does not answer. */
const lastGood = new Map<boolean, ClaudeAccount[]>();

/** listAccounts, on the runner when the account directories are there. */
export async function accountList(db: Db, nowMs: number = Date.now(), sync = true, by = "dashboard"): Promise<ClaudeAccount[]> {
  const c = runnerClient();
  if (!c) return await listAccounts(db, nowMs, sync);
  const m = listMemo.get(sync);
  if (m && Date.now() - m.at < LIST_TTL_MS) return await m.list;
  const list = (async () => {
    try {
      const got = (await (await callRunner(c, "POST", "/internal/accounts/list", by, { sync }, LIST_TIMEOUT_MS)).json()) as ClaudeAccount[];
      lastGood.set(sync, got);
      return got;
    } catch (e) {
      const old = lastGood.get(sync);
      if (old) return old;
      throw e;
    }
  })();
  listMemo.set(sync, { at: Date.now(), list });
  void list.catch(() => listMemo.delete(sync));
  return await list;
}

/** accountList, or null when the runner does not answer and no earlier list is kept: callers then show no account data. */
export async function accountListOrNull(db: Db, nowMs: number = Date.now(), sync = true, by = "dashboard"): Promise<ClaudeAccount[] | null> {
  try { return await accountList(db, nowMs, sync, by); } catch { return null; }
}

/** After a change, the next list reads the runner again. */
export function forgetAccountList(): void {
  listMemo.clear();
}
