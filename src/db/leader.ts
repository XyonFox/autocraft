// Leader election for the roles of which one copy may work at a time: the runner and the periodic chores. On Postgres a
// copy leads while it holds the role's session-level advisory lock on a connection of its own; another copy waits as a
// standby, trying for the lock every few seconds, and takes over once the leader's session ends. On SQLite every process
// of an install runs on one host, where the lock files exclude a second copy, so a process always leads.
//
// Fencing. Winning moves the role's epoch (settings `leader:<role>:epoch`) in a write transaction, and the winner fences
// its writes to that epoch (src/db/fence.ts). Writes run one at a time under the writer lock, and each fenced write checks
// the epoch after taking it, so a write of the old leader either committed before the new epoch or fails.
//
// Losing. The leader runs `SELECT 1` on its lock connection every CHECK_MS. When the connection fails, the leader opens
// another, takes the lock again and reads the epoch under the writer lock: the same epoch means no other copy led
// meanwhile, and it carries on. A holder of the lock that is one of this copy's own discarded sessions (one the server
// has not dropped yet) is waited out. `lost` aborts when another session holds the lock or the epoch moved.
//
// Replicas (AUTOCRAFT_RUNNER_REPLICAS). With two or more, `lost` also aborts when no check has succeeded for LOSS_MS by
// the wall clock (an event-loop stall counts). LOSS_MS is below the time the server takes to drop a silent client
// (SERVER_KEEPALIVE in postgres.ts, about 11 seconds), so in a network split the old leader has stopped before another
// copy can win. With one, no standby waits to take over, so a slow or failed check never stops the leader: it keeps
// reconnecting and taking the lock again. A copy that does win meanwhile (the new pod of a deploy) moves the epoch, and
// the old leader stops at its next fenced write or when it takes the lock again.
import { hostname } from "node:os";
import type * as Pg from "pg";
import { envVar } from "../env.ts";
import type { Db } from "./index.ts";
import { dbTarget, isPgUrl, loadPg } from "./open.ts";
import { pgConnectionString, SERVER_KEEPALIVE, setSession, within } from "./postgres.ts";
import { epochKey, isFenced, type LeaderRole } from "./fence.ts";

/**
 * Exit code of a process that lost its leader lock: an ordinary failure (not a signal), so its supervisor restarts it
 * (Kubernetes, Compose, systemd Restart=on-failure) as a standby.
 */
export const LOST_LEADER_EXIT = 75;

/** Advisory lock keys, one per role ("aclr", "aclp"); below 2^31, so pg_locks shows them as classid 0, objid key. */
const LOCK_KEYS: Record<LeaderRole, number> = { runner: 0x61636c72, periodic: 0x61636c70 };

/** The granted holder of a role's lock ($1 the key). */
const HOLDER_SQL = "SELECT pid FROM pg_locks WHERE locktype = 'advisory' AND classid = 0 AND objid = $1 AND objsubid = 1 AND granted";

/** A standby tries for the lock this often. */
export const RETRY_MS = 2_000;
/** The leader checks its lock connection this often. */
export const CHECK_MS = 2_000;
/** Longest time without a successful check before the leader gives up: below the server's drop time of a silent client. */
export const LOSS_MS = 8_000;
/** Longest wait for a lock query of a standby or of a leader taking its lock again. */
const QUERY_MS = 3_000;
/** Leader and standbys write their status row this often (health checks read it). */
export const BEAT_MS = 10_000;
/** Standby rows not written for this long are removed by the leader. */
const STANDBY_KEEP_MS = 10 * 60_000;
const CONNECT_MS = 5_000;

/** Copies of the runner and periodic containers the install runs (AUTOCRAFT_RUNNER_REPLICAS, default 1). */
export function runnerReplicas(env: NodeJS.ProcessEnv = process.env): number {
  const n = Number(envVar("RUNNER_REPLICAS", env));
  return Number.isInteger(n) && n >= 1 ? n : 1;
}

/** Settings row the leader of `role` writes every BEAT_MS: LeaderStatus as JSON. */
export const leaderKey = (role: LeaderRole): string => `leader:${role}`;
/** Prefix of the rows standbys of `role` write, one per copy: StandbyStatus as JSON. */
export const standbyPrefix = (role: LeaderRole): string => `leader:${role}:standby:`;

/** What the server says of a lock connection: its backend, the keepalive settings in effect and the client address it sees. */
export type ConnInfo = { pid: number; keepalivesIdle: string | null; userTimeout: string | null; clientAddr: string | null };
/** `replicas`: the copy's AUTOCRAFT_RUNNER_REPLICAS (absent in rows of older versions). */
export type LeaderStatus = { holder: string; epoch: number; since: string; at: string; replicas?: number; conn: Omit<ConnInfo, "pid"> };
export type StandbyStatus = { holder: string; since: string; at: string; replicas?: number };

export type Lease = {
  role: LeaderRole;
  /** 0 on SQLite, where nothing is fenced. */
  epoch: number;
  holder: string;
  /** Aborts when the lock is lost; never on SQLite. */
  lost: AbortSignal;
  /** Gives the lock up (a clean stop); the next standby takes over within RETRY_MS. */
  release(): Promise<void>;
};

export type ElectOptions = {
  /** The database (a postgres:// URL; anything else leads at once). Default: the process's configured database. */
  url?: string;
  /** Stops waiting: elect resolves null. */
  signal?: AbortSignal;
  log?: (line: string) => void;
  /** Called once, when this copy starts to wait as a standby. */
  onStandby?: () => void;
  holder?: string;
  retryMs?: number;
  checkMs?: number;
  lossMs?: number;
  beatMs?: number;
  /** Copies of this role the install runs; with one, a failed or slow check alone never aborts `lost`. Default runnerReplicas(). */
  replicas?: number;
};

const iso = (ms = Date.now()): string => new Date(ms).toISOString();

const pause = (ms: number, signal?: AbortSignal): Promise<void> => new Promise((resolve) => {
  if (signal?.aborted) return resolve();
  const t = setTimeout(done, ms);
  function done(): void {
    clearTimeout(t);
    signal?.removeEventListener("abort", done);
    resolve();
  }
  signal?.addEventListener("abort", done, { once: true });
});

async function putSetting(db: Db, key: string, value: string): Promise<void> {
  await db.prepare("INSERT INTO settings (key, value) VALUES (?, ?) ON CONFLICT (key) DO UPDATE SET value = excluded.value").run(key, value);
}

/** A lock connection: errors and its end go to `onLost`, so a dropped connection never crashes the process. */
async function lockClient(pg: typeof Pg.default, url: string, role: LeaderRole, log: (l: string) => void, onLost: (c: Pg.Client, why: string) => void): Promise<{ client: Pg.Client; info: ConnInfo }> {
  const c = new pg.Client({
    connectionString: pgConnectionString(url), application_name: `autocraft-leader-${role}`, keepAlive: true, keepAliveInitialDelayMillis: 5_000,
    connectionTimeoutMillis: CONNECT_MS,
  });
  c.on("error", (e) => onLost(c, e.message));
  c.on("end", () => onLost(c, "the connection closed"));
  try {
    await c.connect();
    // A refused setting leaves the server's default; the status row shows what is in effect.
    await setSession((text) => c.query(text), SERVER_KEEPALIVE, `the ${role} lock connection`, log, QUERY_MS);
    const r = await within(c.query<{ pid: number; idle: string | null; ut: string | null; addr: string | null }>(
      "SELECT pg_backend_pid() AS pid, current_setting('tcp_keepalives_idle', true) AS idle, current_setting('tcp_user_timeout', true) AS ut, host(inet_client_addr()) AS addr"), QUERY_MS, "the connection check");
    const row = r.rows[0]!;
    return { client: c, info: { pid: Number(row.pid), keepalivesIdle: row.idle, userTimeout: row.ut, clientAddr: row.addr } };
  } catch (e) {
    await c.end().catch(() => undefined);
    throw e;
  }
}

/**
 * Waits until this copy leads `role`, then resolves its lease; null when `signal` aborted first. The caller fences its
 * process to `lease.epoch` (setFence) before it starts work and stops at once when `lease.lost` aborts.
 */
export async function elect(db: Db, role: LeaderRole, o: ElectOptions = {}): Promise<Lease | null> {
  const holder = o.holder ?? `${hostname()}:${process.pid}`;
  const log = o.log ?? ((l: string) => console.log(l));
  const url = o.url ?? dbTarget();
  if (db.dialect !== "postgres" || !isPgUrl(url)) {
    return o.signal?.aborted ? null : { role, epoch: 0, holder, lost: new AbortController().signal, release: async () => {} };
  }
  const pg = await loadPg();
  const key = LOCK_KEYS[role];
  const retryMs = o.retryMs ?? RETRY_MS, checkMs = o.checkMs ?? CHECK_MS, lossMs = o.lossMs ?? LOSS_MS, beatMs = o.beatMs ?? BEAT_MS;
  const replicas = o.replicas ?? runnerReplicas();
  const watchdog = replicas >= 2;
  const standbyRow = `${standbyPrefix(role)}${holder}`;
  const waitingSince = iso();
  let onLost: (c: Pg.Client, why: string) => void = () => {};
  const open = () => lockClient(pg, url, role, log, (c, why) => onLost(c, why));
  let conn: { client: Pg.Client; info: ConnInfo } | null = null;
  let standbyBeat = 0, announced = false, lastError = "";

  // Standby: try for the lock until it is ours.
  onLost = (c, why) => {
    if (conn?.client !== c) return;
    conn = null;
    log(`leader: the ${role} lock connection failed while waiting (${why}); opening another`);
    void c.end().catch(() => undefined);
  };
  for (;;) {
    if (o.signal?.aborted) {
      await conn?.client.end().catch(() => undefined);
      await db.prepare("DELETE FROM settings WHERE key = ?").run(standbyRow).catch(() => undefined);
      return null;
    }
    try {
      conn ??= await open();
      const r = await within(conn.client.query<{ ok: boolean }>("SELECT pg_try_advisory_lock($1) AS ok", [key]), QUERY_MS, "the lock query");
      if (r.rows[0]?.ok) break;
      if (!announced) {
        announced = true;
        log(`leader: another copy leads the ${role}; waiting as a standby`);
        // Outside a deploy's brief overlap, a standby means a second replica runs without the watchdog it needs.
        if (!watchdog) log(`leader: warning: AUTOCRAFT_RUNNER_REPLICAS is ${replicas}, but another copy of the ${role} runs; set it to the number of copies`);
        o.onStandby?.();
      }
      if (Date.now() - standbyBeat >= beatMs) {
        standbyBeat = Date.now();
        await putSetting(db, standbyRow, JSON.stringify({ holder, since: waitingSince, at: iso(), replicas } satisfies StandbyStatus));
      }
      lastError = "";
    } catch (e) {
      const msg = (e as Error).message;
      if (msg !== lastError) log(`leader: cannot try the ${role} lock: ${msg}`);
      lastError = msg;
      const c = conn?.client;
      conn = null;
      await c?.end().catch(() => undefined);
    }
    await pause(retryMs, o.signal);
  }

  let current: { client: Pg.Client; info: ConnInfo } = conn!;
  const ac = new AbortController();
  let released = false, recovering = false;
  /** Send time of the last check that succeeded. */
  let lastOk = Date.now();
  let check: NodeJS.Timeout | undefined, watch: NodeJS.Timeout | undefined, beat: NodeJS.Timeout | undefined;
  const stopTimers = () => { clearInterval(check); clearInterval(watch); clearInterval(beat); };
  const lose = (why: string): void => {
    if (released || ac.signal.aborted) return;
    stopTimers();
    log(`leader: lost the ${role} lock (${why})`);
    ac.abort(new Error(why));
    void current.client.end().catch(() => undefined);
  };
  const status = (at: string): string => {
    const { pid: _, ...c } = current.info;
    return JSON.stringify({ holder, epoch, since, at, replicas, conn: c } satisfies LeaderStatus);
  };

  /** Gives the lock back (if this session still holds it) and runs the election again. */
  const again = async (why: string): Promise<Lease | null> => {
    released = true;
    log(`leader: ${why}; trying again`);
    await current.client.query("SELECT pg_advisory_unlock($1)", [key]).catch(() => undefined);
    await current.client.end().catch(() => undefined);
    await pause(retryMs, o.signal);
    return await elect(db, role, { ...o, holder, onStandby: announced ? undefined : o.onStandby });
  };

  // The epoch moves while this copy holds the lock, so no two copies ever share one.
  let epoch: number;
  const since = iso();
  try {
    const row = await db.prepare("INSERT INTO settings (key, value) VALUES (?, '1') ON CONFLICT (key) DO UPDATE SET value = CAST(CAST(settings.value AS BIGINT) + 1 AS TEXT) RETURNING value")
      .get(epochKey(role)) as { value: string };
    epoch = Number(row.value);
  } catch (e) {
    // A write that cannot get the writer lock (a lock wait past its limit).
    return await again(`the ${role} lock was won but its epoch not moved: ${(e as Error).message}`);
  }
  // The lock connection may have dropped while the epoch write waited, and another copy won and moved the epoch first.
  const held = await within(current.client.query<{ held: boolean }>(`SELECT EXISTS (${HOLDER_SQL} AND pid = pg_backend_pid()) AS held`, [key]), QUERY_MS, "the lock confirmation")
    .then((r) => r.rows[0]?.held === true, () => false);
  if (!held) return await again(`the ${role} lock was lost while its epoch moved to ${epoch}`);
  try {
    await db.prepare("DELETE FROM settings WHERE key = ?").run(standbyRow);
    await putSetting(db, leaderKey(role), status(since));
  } catch (e) {
    return await again(`the ${role} status was not written: ${(e as Error).message}`);
  }
  log(`leader: this copy leads the ${role} (epoch ${epoch})`);

  /**
   * Backends of this copy's own lock sessions it has given up on: the first lock connection, and every later one that
   * failed or timed out (a lock query that timed out here may still have taken the lock on the server).
   */
  const ownPids = new Set<number>();
  const discard = (c: { client: Pg.Client; info: ConnInfo }): void => {
    ownPids.add(c.info.pid);
    void c.client.end().catch(() => undefined);
  };
  /** After the lock connection failed: a new connection, the lock again, and the epoch unchanged; otherwise `lost`. */
  const reacquire = async (why: string): Promise<void> => {
    if (recovering || released || ac.signal.aborted) return;
    recovering = true;
    discard(current);
    log(`leader: the ${role} lock connection failed (${why}); taking the lock again`);
    try {
      // With one replica no other copy waits to take over, so this keeps trying until the lock is back or lost.
      while (!released && !ac.signal.aborted && (!watchdog || Date.now() - lastOk <= lossMs)) {
        let next: { client: Pg.Client; info: ConnInfo } | null = null;
        try {
          next = await open();
          const got = (await within(next.client.query<{ ok: boolean }>("SELECT pg_try_advisory_lock($1) AS ok", [key]), QUERY_MS, "the lock query")).rows[0]?.ok;
          if (!got) {
            // Held by one of this copy's old sessions (the server has not dropped it yet) or by another copy.
            const h = await within(next.client.query<{ pid: number }>(HOLDER_SQL, [key]), QUERY_MS, "the lock holder query");
            const holderPid = h.rows[0]?.pid;
            discard(next);
            next = null;
            if (holderPid !== undefined && !ownPids.has(Number(holderPid))) return lose("another copy holds the lock");
            await pause(Math.min(retryMs, 500));
            continue;
          }
          // Under the writer lock: a new leader's epoch write has either committed or not begun.
          const row = await db.write(async () => await db.prepare("SELECT value FROM settings WHERE key = ?").get(epochKey(role)) as { value: string } | undefined);
          if (row?.value !== String(epoch)) {
            lose(`another copy led meanwhile (epoch ${row?.value ?? "none"})`);
            await next.client.end().catch(() => undefined);
            return;
          }
          if (released || ac.signal.aborted) {
            await next.client.end().catch(() => undefined);
            return;
          }
          current = next;
          lastOk = Date.now();
          // This session holds the lock now, so none of the old ones can.
          ownPids.clear();
          log(`leader: took the ${role} lock again (epoch ${epoch})`);
          return;
        } catch (e) {
          if (next) discard(next);
          if (isFenced(e)) return lose("another copy took over");
          await pause(Math.min(retryMs, 500));
        }
      }
    } finally {
      recovering = false;
    }
  };
  onLost = (c, why) => { if (c === current.client) void reacquire(why); };

  /** The client whose check has not answered yet: a hung check on a discarded client must not hold up checks on its successor. */
  let checkingOn: Pg.Client | null = null;
  check = setInterval(() => {
    if (checkingOn === current.client || recovering) return;
    const sent = Date.now(), c = current.client;
    checkingOn = c;
    void c.query("SELECT 1").then(
      () => { if (c === current.client) lastOk = Math.max(lastOk, sent); },
      (e: Error) => { if (c === current.client) void reacquire(e.message); },
    ).finally(() => { if (checkingOn === c) checkingOn = null; });
  }, checkMs);
  // Wall-clock time: after a stall of the event loop this fires late and sees the whole gap.
  watch = setInterval(() => {
    const gap = Date.now() - lastOk;
    if (gap <= lossMs) return;
    const why = `no successful check of the lock for ${Math.round(gap / 1000)} s`;
    // One replica: a check that hangs gets a new connection instead of a loss.
    if (watchdog) lose(why);
    else void reacquire(why);
  }, Math.min(250, checkMs));
  beat = setInterval(() => {
    // Fenced like every write of this process: a refusal means another copy has taken over.
    void (async () => {
      await putSetting(db, leaderKey(role), status(iso()));
      await pruneStandbys(db, role);
    })().catch((e: unknown) => {
      if (isFenced(e)) lose("another copy took over");
      else log(`leader: status of the ${role} not written: ${(e as Error).message}`);
    });
  }, beatMs);
  check.unref();
  watch.unref();
  beat.unref();

  return {
    role, epoch, holder, lost: ac.signal,
    release: async () => {
      if (released) return;
      released = true;
      stopTimers();
      if (!ac.signal.aborted) await current.client.query("SELECT pg_advisory_unlock($1)", [key]).catch(() => undefined);
      await current.client.end().catch(() => undefined);
      log(`leader: released the ${role} lock`);
    },
  };
}

/** Removes the rows of standbys that stopped writing them. */
async function pruneStandbys(db: Db, role: LeaderRole): Promise<void> {
  const rows = await db.prepare("SELECT key, value FROM settings WHERE key LIKE ?").all(`${standbyPrefix(role)}%`) as { key: string; value: string }[];
  for (const r of rows) {
    let at = NaN;
    try { at = Date.parse((JSON.parse(r.value) as StandbyStatus).at); } catch { /* removed below */ }
    if (Number.isNaN(at) || Date.now() - at > STANDBY_KEEP_MS) await db.prepare("DELETE FROM settings WHERE key = ? AND value = ?").run(r.key, r.value);
  }
}

/**
 * What the health checks show of a role: its leader's status, the standbys seen within `freshMs`, and `unwatched`, whether
 * a standby runs beside a copy set to one replica (which then runs without the watchdog two copies need); null when no
 * copy was ever elected (SQLite).
 */
export async function leaderStatus(db: Db, role: LeaderRole, freshMs: number, nowMs = Date.now()): Promise<{ leader: LeaderStatus | null; standbys: number; unwatched: boolean } | null> {
  const rows = await db.all(db.k.selectFrom("settings").select(["key", "value"]).where("key", "like", `${leaderKey(role)}%`));
  const lead = rows.find((r) => r.key === leaderKey(role));
  if (!lead) return null;
  const parse = <T>(v: string | null): T | null => { try { return v ? JSON.parse(v) as T : null; } catch { return null; } };
  const fresh = (s: { at?: unknown } | null): boolean => typeof s?.at === "string" && nowMs - Date.parse(s.at) <= freshMs;
  const live = rows.filter((r) => r.key.startsWith(standbyPrefix(role))).map((r) => parse<StandbyStatus>(r.value)).filter((s) => fresh(s));
  const leader = parse<LeaderStatus>(lead.value);
  const single = (s: { replicas?: unknown } | null): boolean => typeof s?.replicas === "number" && s.replicas < 2;
  return { leader, standbys: live.length, unwatched: live.length > 0 && (single(leader) || live.some(single)) };
}
