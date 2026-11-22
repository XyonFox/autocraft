// Once: the breaker rows of a whole push stage (`fixup`, `cifix`, `fixissue`, `merge` in stage_trips, from before every push stage
// tripped per repo) are split into per-repo rows (`<stage>:<repo>`, splitStageTrips in core/push-outcome.ts). The stage rows stay
// as the instance-wide tier's rows, inactive and resumed at the split, so pushes before it never trip that tier. Runs on SQLite with
// the value migrations of connect() (src/db.ts) and on Postgres inside migratePg's transaction (src/db/pg-triggers.ts).
import type { DatabaseSync } from "node:sqlite";
import { SPLIT_STAGES, splitStageTrips, type StageTripRow } from "../core/push-outcome.ts";
import type { PgClient } from "./pg-triggers.ts";

/** Part of the Postgres stamp, so a database migrated before this code opens migratePg and reaches the split; the TRIP_SPLIT_DONE marker, not this number, decides whether the split runs. */
export const TRIP_SPLIT_VERSION = 2;

/** Set once the split ran: from then on a stage row is the instance-wide breaker and is never split again. */
export const TRIP_SPLIT_DONE = "stage_trips_split";

const STAGE_ROWS = `stage IN (${SPLIT_STAGES.map((s) => `'${s}'`).join(", ")})`;
const COLS = "stage, active, reason, evidence, tripped_at, cleared_at, cleared_by";
const ROWS_SQL = (main: string) => `SELECT ${COLS} FROM ${main}stage_trips WHERE ${STAGE_ROWS}`;
// Repos whose earlier pushes or merges count for a stage's breaker: those the last resume of a stage row carries to.
const REPOS_SQL = (main: string) => `SELECT DISTINCT stage, repo FROM ${main}push_outcomes UNION SELECT DISTINCT 'merge', repo FROM ${main}merges WHERE outcome = 'merged'`;
// A per-repo row that exists is kept, except that an active split row overrides an inactive one (keeping that row's own last resume).
const INSERT_SQL = (main: string, p: (n: number) => string) => `INSERT INTO ${main}stage_trips (${COLS}) VALUES (${[1, 2, 3, 4, 5, 6, 7].map(p).join(", ")})
  ON CONFLICT (stage) DO UPDATE SET active = excluded.active, reason = excluded.reason, evidence = excluded.evidence, tripped_at = excluded.tripped_at
  WHERE stage_trips.active = 0 AND excluded.active = 1`;
const RESUME_SQL = (main: string, p: (n: number) => string) => `UPDATE ${main}stage_trips SET active = 0, cleared_at = ${p(1)}, cleared_by = 'migration' WHERE ${STAGE_ROWS}`;
const DONE_SQL = (main: string, p: (n: number) => string) => `INSERT INTO ${main}settings (key, value) VALUES ('${TRIP_SPLIT_DONE}', ${p(1)}) ON CONFLICT (key) DO NOTHING`;
const DONE_READ = (main: string) => `SELECT 1 FROM ${main}settings WHERE key = '${TRIP_SPLIT_DONE}'`;

const reposBy = (rows: { stage: string; repo: string }[]) => (stage: string): string[] => [...new Set(rows.filter((r) => r.stage === stage).map((r) => r.repo))].sort();
const values = (r: StageTripRow) => [r.stage, r.active, r.reason, r.evidence, r.tripped_at, r.cleared_at, r.cleared_by];

export function splitStageTripsSqlite(db: DatabaseSync, at = new Date().toISOString()): void {
  if (db.prepare(DONE_READ("main.")).get()) return;
  db.exec("BEGIN IMMEDIATE");
  try {
    if (!db.prepare(DONE_READ("main.")).get()) {
      const rows = db.prepare(ROWS_SQL("main.")).all() as StageTripRow[];
      const insert = db.prepare(INSERT_SQL("main.", () => "?"));
      for (const r of splitStageTrips(rows, reposBy(db.prepare(REPOS_SQL("main.")).all() as { stage: string; repo: string }[]))) insert.run(...values(r));
      db.prepare(RESUME_SQL("main.", () => "?")).run(at);
      db.prepare(DONE_SQL("main.", () => "?")).run(at);
    }
    db.exec("COMMIT");
  } catch (e) {
    db.exec("ROLLBACK");
    throw e;
  }
}

/** The Postgres form, on the migration connection inside its transaction. */
export async function splitStageTripsPg(client: PgClient, at = new Date().toISOString()): Promise<void> {
  if ((await client.query(DONE_READ(""))).rows.length) return;
  // bigint columns come back as text from pg.
  const rows = ((await client.query(ROWS_SQL(""))).rows as StageTripRow[]).map((r) => ({ ...r, active: Number(r.active) }));
  const repos = (await client.query(REPOS_SQL(""))).rows as { stage: string; repo: string }[];
  for (const r of splitStageTrips(rows, reposBy(repos))) await client.query(INSERT_SQL("", (n) => `$${n}`), values(r));
  await client.query(RESUME_SQL("", (n) => `$${n}`), [at]);
  await client.query(DONE_SQL("", (n) => `$${n}`), [at]);
}
