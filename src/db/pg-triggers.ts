// The live topics (src/live.ts) and the planner's change log (src/core/plan-changes.ts) on Postgres: one plpgsql function per
// table and operation (AFTER ROW) evaluates every topic and change-log trigger SQLite has for that table and operation.
//
// Counter rows move once per transaction, at commit. A row trigger only records what it touched in transaction-local settings
// (topics in `ac.topics`, full-pass causes in `ac.causes`) and, on the first touch, inserts one row into `live_flush`. That
// insert queues a deferred constraint trigger, which runs at commit: it locks the touched change_seq rows in sorted order, then
// plan_full, then the plan_full_causes rows in sorted order, and bumps each once. Every transaction locks counters in that one
// order, so two cannot deadlock on them, and the bump commits with the data (a sequence would show before the commit).
// A rolled-back savepoint undoes its settings, its live_flush row and the queued trigger together.
//
// item_changes rows are inserted by the row triggers themselves; the planner consumes them by key (src/core/plan-changes.ts).
// The bulk marker is the transaction-local setting `ac.bulk` (PG_BULK).
import { createHash } from "node:crypto";
import { BOOKKEEPING_COLUMNS, CHANGE_SEQ_TABLE, liveTriggers, TOPICS } from "../live.ts";
import { blockerFill, blockerSync, PG_BULK, PLAN_TABLES, planTriggers } from "../core/plan-changes.ts";
import { pgExpr } from "../core/pg-expr.ts";
import { applyPgSchema, pgSchemaStamp, type PgQueryable } from "./pg-migrate.ts";
import { translateSqlite } from "./pg-schema.ts";
import { pgScopeSql, SCOPED_SCHEMA } from "./scoped.ts";
import { splitStageTripsPg, TRIP_SPLIT_VERSION } from "./trip-split.ts";
import { retireFixAttemptsPg } from "./fix-retire.ts";

const OPS = ["INSERT", "UPDATE", "DELETE"] as const;
type Op = (typeof OPS)[number];

/** Separator of the causes list: source names are code strings, never multi-line. */
const NL = "E'\\n'";

const PROLOGUE = `-- SQLite's CAST(text AS INTEGER): the leading integer, else 0.
CREATE OR REPLACE FUNCTION ac_int(t text) RETURNS bigint LANGUAGE sql IMMUTABLE PARALLEL SAFE
  RETURN CASE WHEN t IS NOT NULL THEN COALESCE(substring(t from '^\\s*([+-]?[0-9]{1,18})')::bigint, 0) END;
-- One row per writing transaction that touched a counter, inserted and deleted inside it: it queues the commit-time flush.
CREATE UNLOGGED TABLE IF NOT EXISTS live_flush (xid xid8 PRIMARY KEY);
CREATE OR REPLACE FUNCTION ac_schedule() RETURNS void LANGUAGE plpgsql AS $$
BEGIN
  IF current_setting('ac.flush', true) IS DISTINCT FROM '1' THEN
    PERFORM set_config('ac.flush', '1', true);
    INSERT INTO live_flush (xid) VALUES (pg_current_xact_id());
  END IF;
END $$;
CREATE OR REPLACE FUNCTION ac_touch(topic text) RETURNS void LANGUAGE plpgsql AS $$
DECLARE cur text := COALESCE(current_setting('ac.topics', true), '');
BEGIN
  IF strpos(cur, ',' || topic || ',') = 0 THEN
    PERFORM set_config('ac.topics', CASE WHEN cur = '' THEN ',' ELSE cur END || topic || ',', true);
    PERFORM ac_schedule();
  END IF;
END $$;
CREATE OR REPLACE FUNCTION ac_mark_full(source text) RETURNS void LANGUAGE plpgsql AS $$
DECLARE cur text := COALESCE(current_setting('ac.causes', true), '');
BEGIN
  IF strpos(cur, ${NL} || source || ${NL}) = 0 THEN
    PERFORM set_config('ac.causes', CASE WHEN cur = '' THEN ${NL} ELSE cur END || source || ${NL}, true);
    PERFORM ac_schedule();
  END IF;
END $$;
CREATE OR REPLACE FUNCTION ac_flush() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  topics text[] := ARRAY(SELECT DISTINCT t FROM unnest(string_to_array(COALESCE(current_setting('ac.topics', true), ''), ',')) t WHERE t <> '' ORDER BY t);
  causes text[] := ARRAY(SELECT DISTINCT c FROM unnest(string_to_array(COALESCE(current_setting('ac.causes', true), ''), ${NL})) c WHERE c <> '' ORDER BY c);
  gen bigint;
BEGIN
  IF cardinality(topics) > 0 THEN
    PERFORM 1 FROM change_seq WHERE topic = ANY (topics) ORDER BY topic FOR UPDATE;
    UPDATE change_seq SET seq = seq + 1 WHERE topic = ANY (topics);
  END IF;
  IF cardinality(causes) > 0 THEN
    INSERT INTO plan_full (id, seq) VALUES (1, 0) ON CONFLICT (id) DO NOTHING;
    UPDATE plan_full SET seq = seq + 1 WHERE id = 1 RETURNING seq INTO gen;
    INSERT INTO plan_full_causes (source, n, last_seq) SELECT c, 1, gen FROM unnest(causes) c ORDER BY c
      ON CONFLICT (source) DO UPDATE SET n = plan_full_causes.n + 1, last_seq = excluded.last_seq;
  END IF;
  DELETE FROM live_flush WHERE xid = NEW.xid;
  PERFORM set_config('ac.topics', '', true), set_config('ac.causes', '', true), set_config('ac.flush', '', true);
  RETURN NULL;
END $$;`;

/** Drops every trigger this module made, so a trigger whose registry entry is gone does not stay behind. */
const DROP_OLD = `DO $$
DECLARE t record;
BEGIN
  FOR t IN SELECT tg.tgname, c.relname FROM pg_trigger tg JOIN pg_class c ON c.oid = tg.tgrelid JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = current_schema() AND NOT tg.tgisinternal AND tg.tgname LIKE 'ac\\_%'
  LOOP
    EXECUTE format('DROP TRIGGER %I ON %I', t.tgname, t.relname);
  END LOOP;
END $$;`;

const ifThen = (cond: string, body: string, indent: string): string =>
  cond ? `${indent}IF ${cond} THEN\n${indent}  ${body}\n${indent}END IF;` : `${indent}${body}`;

/**
 * The tables, functions, triggers and rows of the live topics and the change log, for a database that has the schema of
 * src/schema.pg.sql. Idempotent. With `columns` (pgColumns), an UPDATE counts only when a watched column changed.
 */
export function pgTriggerSql(columns?: Map<string, string[]>): string {
  // Indexes on schema tables are plain SQL both dialects take; the translator only knows the tables it is given.
  const ddl = [CHANGE_SEQ_TABLE, ...PLAN_TABLES].map((s) => s.replaceAll("main.", ""));
  const own = (s: string) => !/^CREATE INDEX/.test(s) || /\bON review_blockers\(/.test(s);
  const tables = [translateSqlite(ddl.filter(own).join("\n")), ...ddl.filter((s) => !own(s))].join("\n");
  const groups = new Map<string, { table: string; op: Op; topics: Map<string, string[]>; plan: string[] }>();
  const group = (table: string, op: Op) => {
    const key = `${table}|${op}`;
    let g = groups.get(key);
    if (!g) groups.set(key, (g = { table, op, topics: new Map(), plan: [] }));
    return g;
  };
  for (const t of liveTriggers(columns)) {
    const g = group(t.table, t.op);
    g.topics.set(t.topic, [...(g.topics.get(t.topic) ?? []), t.cond ? pgExpr(t.cond) : ""]);
  }
  for (const t of planTriggers("postgres", columns)) {
    const body = "full" in t ? `PERFORM ac_mark_full('${t.full}');`
      : `INSERT INTO item_changes (repo, number) SELECT s.repo, s.number FROM (${t.refs}) AS s WHERE s.repo IS NOT NULL AND s.number IS NOT NULL;`;
    group(t.table, t.op).plan.push(ifThen(t.cond, body, "    "));
  }
  const sync = blockerSync("postgres");
  for (const op of OPS) group("opus_reviews", op);
  const out = [PROLOGUE, tables, DROP_OLD];
  for (const { table, op, topics, plan } of [...groups.values()].sort((a, b) => (`${a.table}|${a.op}` < `${b.table}|${b.op}` ? -1 : 1))) {
    const o = op[0]!.toLowerCase();
    const lines: string[] = [];
    if (table === "opus_reviews") lines.push(ifThen(sync[op].cond, sync[op].body, "  "));
    for (const [topic, conds] of topics) lines.push(ifThen(conds.includes("") ? "" : conds.map((c) => `(${c})`).join(" OR "), `PERFORM ac_touch('${topic}');`, "  "));
    if (plan.length) lines.push(`  IF current_setting('${PG_BULK}', true) IS DISTINCT FROM '1' THEN\n${plan.join("\n")}\n  END IF;`);
    out.push(`CREATE OR REPLACE FUNCTION ac_trg_${table}_${o}() RETURNS trigger LANGUAGE plpgsql AS $f$
BEGIN
${lines.join("\n")}
  RETURN NULL;
END $f$;
CREATE TRIGGER ac_${table}_${o} AFTER ${op} ON ${table} FOR EACH ROW EXECUTE FUNCTION ac_trg_${table}_${o}();`);
  }
  out.push("CREATE CONSTRAINT TRIGGER ac_flush AFTER INSERT ON live_flush DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION ac_flush();");
  out.push(`INSERT INTO change_seq (topic, seq, at) VALUES ${TOPICS.map((t) => `('${t}', 0, NULL)`).join(", ")} ON CONFLICT (topic) DO NOTHING;`);
  out.push("INSERT INTO plan_full (id, seq) VALUES (1, 0) ON CONFLICT (id) DO NOTHING;");
  out.push("DELETE FROM review_blockers;", blockerFill("postgres"));
  return out.join("\n");
}

/** Column names of every table, as the SQLite schema has them: no generated search columns, no added rowid. */
export const PG_COLUMNS_SQL = `SELECT table_name AS t, column_name AS c FROM information_schema.columns
  WHERE table_schema = current_schema() AND is_generated = 'NEVER' AND column_name <> 'rowid' ORDER BY table_name, ordinal_position`;

export function pgColumns(rows: { t: string; c: string }[]): Map<string, string[]> {
  const out = new Map<string, string[]>();
  for (const { t, c } of rows) out.set(t, [...(out.get(t) ?? []), c]);
  return out;
}

/**
 * Pins the search path of every plpgsql function in the app schema (trigger bodies and their helpers) to that schema, so a
 * trigger fired from a scoped handle (src/db/scoped.ts) reads the tables, as SQLite resolves a trigger's names in its
 * own schema. SQL-language functions are left alone: a SET clause would stop them being inlined.
 */
const PIN_SEARCH_PATH = `DO $$
DECLARE f record;
BEGIN
  FOR f IN SELECT p.oid::regprocedure AS sig FROM pg_proc p JOIN pg_language l ON l.oid = p.prolang
    WHERE p.pronamespace = current_schema()::regnamespace AND l.lanname = 'plpgsql'
  LOOP
    EXECUTE format('ALTER FUNCTION %s SET search_path = %I', f.sig, current_schema());
  END LOOP;
END $$;`;

/** The schema stamp a migrated Postgres database stores in settings: the schema file's stamp and the trigger and view generators. */
export function pgStamp(): string {
  return createHash("sha256").update(pgSchemaStamp()).update(pgTriggerSql()).update(pgScopeSql("app")).update(PIN_SEARCH_PATH)
    .update(JSON.stringify(BOOKKEEPING_COLUMNS)).update(`trip-split ${TRIP_SPLIT_VERSION}`).digest("hex").slice(0, 16);
}

/** A connection that returns rows: a pg Client. */
export interface PgClient extends PgQueryable {
  query(text: string, values?: unknown[]): Promise<{ rows: Record<string, unknown>[] }>;
}

const STAMP_SQL = "SELECT value FROM settings WHERE key = 'schema_stamp'";

/** The stamp the database stores, or null before its first migration. `rows` runs one statement. */
export async function storedPgStamp(rows: (sql: string) => Promise<Record<string, unknown>[]>): Promise<string | null> {
  // A missing table would abort the migration's transaction, so its existence is asked first.
  if (!(await rows("SELECT to_regclass('settings') IS NOT NULL AS ok"))[0]?.ok) return null;
  return ((await rows(STAMP_SQL))[0]?.value as string | undefined) ?? null;
}

/**
 * Brings the database at `client` to the current schema and triggers in one transaction under the migration lock, unless it
 * already stores the current stamp (checked again under the lock, so concurrent openers migrate once).
 */
export async function migratePg(client: PgClient): Promise<void> {
  const stamp = pgStamp();
  const rows = async (sql: string) => (await client.query(sql)).rows;
  await applyPgSchema(client, async () => {
    await client.query(pgTriggerSql(pgColumns(await rows(PG_COLUMNS_SQL) as { t: string; c: string }[])));
    await client.query(pgScopeSql((await rows("SELECT current_schema() AS s"))[0]!.s as string));
    await client.query(PIN_SEARCH_PATH);
    await splitStageTripsPg(client);
    await retireFixAttemptsPg(client);
    await client.query("INSERT INTO settings (key, value) VALUES ('schema_stamp', $1) ON CONFLICT (key) DO UPDATE SET value = excluded.value", [stamp]);
  }, {
    unless: async () => (await storedPgStamp(rows)) === stamp,
    // The views name the tables' columns: dropped first, so the schema may change any column, and recreated after it.
    before: async () => void (await client.query(`DROP SCHEMA IF EXISTS ${SCOPED_SCHEMA} CASCADE`)),
  });
}
