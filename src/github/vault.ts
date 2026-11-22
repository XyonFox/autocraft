// The user-token vault: a usable GitHub App access token for a login, for the dashboard and the runner process alike
// (both open the same database and derive the same TokenBox from the secrets file's session_key).
//
// Refresh protocol. GitHub refresh tokens are single use, and the token set lives in several copies: the login's
// user_tokens row and each session from the same sign-in. Any process may refresh:
//  1. read the row and its `version`; a fresh access token is returned as is;
//  2. otherwise call the refresh endpoint with the row's refresh token;
//  3. store the new pair only if `version` is unchanged (compare-and-swap), then copy it into every other stored
//     copy of the old refresh token (sessions keep running, see shareRefresh);
//  4. a refresh the endpoint rejects may mean another process used the token first: reread a few times, and only
//     when the row stays unchanged mark it `refresh-failed`, also by compare-and-swap.
// There is never a fallback to another identity: a final failure answers "needs sign-in".
import { envVar } from "../env.ts";
import { TokenBox } from "../dash/server/crypto.ts";
import { resealLegacyValues } from "../dash/server/reseal.ts";
import { readSecrets } from "../dash/server/config.ts";
import { openAppKeys } from "./app-keys.ts";
import { GitHubProvider, RevokeRefused, type Provider, type Tokens } from "../dash/server/provider.ts";
import { failUserTokens, readUserTokens, revocationAad, shareRefresh, swapUserTokens } from "../dash/server/user-tokens.ts";
import type { Db } from "../db/index.ts";

export type NeedsSignIn = { ok: false; needsSignIn: true; why: "no-token" | "refresh-failed" | "expired" };
/** A transient failure (GitHub unreachable): try again later, the token set is untouched. */
export type Unavailable = { ok: false; needsSignIn: false };
export type UserTokenResult = { ok: true; access: string } | NeedsSignIn | Unavailable;

const EXPIRY_MARGIN = 60_000;
const REREADS = 4;
const MAX_REVOKE_ATTEMPTS = 5;

export class Vault {
  #db: Db;
  #box: TokenBox;
  #providerOf: () => Provider;
  get #provider(): Provider { return this.#providerOf(); }
  #sweeping = false;
  #inflight = new Map<string, Promise<UserTokenResult>>();
  /** Pause between rereads after a rejected refresh; tests set it to 0. */
  waitMs = 300;

  /** `provider` may be a function, read at each use, so the GitHub App's registration can change while the vault lives. */
  constructor(db: Db, box: TokenBox, provider: Provider | (() => Provider)) {
    this.#db = db;
    this.#box = box;
    this.#providerOf = typeof provider === "function" ? provider : () => provider;
  }

  /** A usable access token for `login`, refreshed when it is within a minute of expiry or `rejected` (a token GitHub answered 401 to). */
  userToken(login: string, rejected?: string): Promise<UserTokenResult> {
    const key = login.toLowerCase();
    let p = this.#inflight.get(key);
    if (!p) {
      p = this.#obtain(login, rejected).finally(() => this.#inflight.delete(key));
      this.#inflight.set(key, p);
    }
    return p;
  }

  async #obtain(login: string, rejected: string | undefined): Promise<UserTokenResult> {
    for (let attempt = 0; attempt < REREADS; attempt++) {
      const row = await readUserTokens(this.#db, this.#box, login);
      if (!row?.tokens) return { ok: false, needsSignIn: true, why: "no-token" };
      if (row.status !== "ok") return { ok: false, needsSignIn: true, why: "refresh-failed" };
      const t = row.tokens;
      if (t.access !== rejected && (t.expiresAt === null || t.expiresAt - EXPIRY_MARGIN > Date.now())) return { ok: true, access: t.access };
      if (!t.refresh || (t.refreshExpiresAt !== null && t.refreshExpiresAt <= Date.now())) {
        await failUserTokens(this.#db, login, row.version);
        return { ok: false, needsSignIn: true, why: "expired" };
      }
      let next: Tokens | null;
      try {
        next = await this.#provider.refresh(t.refresh);
      } catch (e) {
        console.error(`user token refresh: ${(e as Error).message}`);
        return { ok: false, needsSignIn: false };
      }
      if (next) {
        if (await swapUserTokens(this.#db, this.#box, login, row.version, next)) {
          await shareRefresh(this.#db, this.#box, login, t.refresh, next);
          return { ok: true, access: next.access };
        }
        // Another writer stored a pair first; its pair is the one to use.
        continue;
      }
      // Rejected: either the refresh token is dead, or another process just used it and is about to store the new pair.
      if (this.waitMs) await new Promise((r) => setTimeout(r, this.waitMs));
      const again = await readUserTokens(this.#db, this.#box, login);
      if (again && again.version !== row.version) continue;
      if (attempt < REREADS - 1 && this.waitMs) continue;
      if (await failUserTokens(this.#db, login, row.version)) return { ok: false, needsSignIn: true, why: "refresh-failed" };
    }
    return { ok: false, needsSignIn: true, why: "refresh-failed" };
  }

  /**
   * Works through the revocation queue (a removed login's whole grant, or one revoked session's token). A row stays
   * until GitHub confirms the revoke. An expired stored access token, or a 404/422 answer, is retried once with a
   * freshly refreshed token. After MAX_REVOKE_ATTEMPTS failed sweeps the row is dropped. `fresh` handles only rows
   * not tried yet. Returns the number of rows handled.
   */
  async sweepRevocations(fresh = false): Promise<number> {
    if (this.#sweeping) return 0;
    this.#sweeping = true;
    try {
      const rows = await this.#db.all(this.#db.k.selectFrom("user_token_revocations")
        .select(["id", "login", "token_enc", "kind", "aad", "attempts"])
        .$if(fresh, (q) => q.where("attempts", "=", 0)).orderBy("id"));
      for (const r of rows) {
        // Claim the attempt first: a concurrent sweep (another process) that read the same row changes nothing here and skips it.
        if ((await this.#db.run(this.#db.k.updateTable("user_token_revocations").set((eb) => ({ attempts: eb("attempts", "+", 1) }))
          .where("id", "=", r.id).where("attempts", "=", r.attempts))).changes !== 1) continue;
        let done = false;
        try {
          done = await this.#revokeRow(r);
        } catch (e) {
          console.error(`revocation for ${r.login}: ${(e as Error).message}`);
        }
        if (done) {
          await this.#db.run(this.#db.k.deleteFrom("user_token_revocations").where("id", "=", r.id));
        } else if (r.attempts + 1 >= MAX_REVOKE_ATTEMPTS) {
          console.error(`revocation for ${r.login}: given up after ${r.attempts + 1} failed attempts`);
          await this.#db.run(this.#db.k.deleteFrom("user_token_revocations").where("id", "=", r.id));
        }
      }
      return rows.length;
    } finally {
      this.#sweeping = false;
    }
  }

  /** True once GitHub confirmed the revoke (or there is nothing left to revoke: an unreadable row). */
  async #revokeRow(r: { login: string; token_enc: string; kind: string; aad: string | null }): Promise<boolean> {
    const plain = this.#box.open(r.token_enc, r.aad ?? revocationAad(r.login));
    let t: Tokens | null = null;
    try {
      t = plain ? (JSON.parse(plain) as Tokens) : null;
    } catch { /* unreadable */ }
    if (!t) return true;
    const revoke = (access: string): Promise<void> => (r.kind === "token" ? this.#provider.revoke(access) : this.#provider.revokeGrant(access));
    // The login's user_tokens row written by the same sign-in holds this very token set and dies with it.
    const own = r.kind === "token" ? await readUserTokens(this.#db, this.#box, r.login) : null;
    const sameSignIn = !!own?.tokens && (own.tokens.access === t.access || (!!t.refresh && own.tokens.refresh === t.refresh));
    const refreshed = async (): Promise<string | null> => {
      if (!t.refresh || (t.refreshExpiresAt !== null && t.refreshExpiresAt <= Date.now())) return null;
      return (await this.#provider.refresh(t.refresh))?.access ?? null;
    };
    let ok = false;
    const expired = t.expiresAt !== null && t.expiresAt - EXPIRY_MARGIN <= Date.now();
    if (!expired) {
      try {
        await revoke(t.access);
        ok = true;
      } catch (e) {
        if (!(e instanceof RevokeRefused) || (e.status !== 404 && e.status !== 422)) throw e;
      }
    }
    if (!ok) {
      const access = await refreshed();
      if (!access) return false;
      await revoke(access);
    }
    if (sameSignIn) await this.#db.run(this.#db.k.deleteFrom("user_tokens").where("login", "=", r.login));
    return true;
  }
}

/** The runner's vault: the app's client credentials from the process's app-key store and the session key from the secrets file, as the dashboard reads them. Null without a registered app. */
export async function openVault(db: Db, env: NodeJS.ProcessEnv = process.env): Promise<Vault | null> {
  const secrets = readSecrets(envVar("SECRETS_FILE", env));
  const keys = await openAppKeys(db, env);
  const app = keys?.current() ?? null;
  if (!keys || !app) return null;
  const box = new TokenBox(secrets.sessionKey);
  // The runner may start before the dashboard has re-sealed values under the current key.
  await resealLegacyValues(db, box);
  return new Vault(db, box, () => {
    const now = keys.current() ?? app;
    return new GitHubProvider(now.clientId, now.clientSecret);
  });
}
