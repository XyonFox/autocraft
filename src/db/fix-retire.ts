// What the retired Fix stage (text-only fix attempts, replaced by Implement) leaves behind ends: attempts still moving (retry,
// testing or passed) are failed with their rows kept, its breaker rows (`fixissue`, `fixissue:<repo>`) are resumed, and its
// unsettled pushes settle as unknown, so nothing pauses or waits on a stage that no longer runs. Runs with every schema
// migration, on SQLite in backfill() (src/db.ts) and on Postgres inside migratePg's transaction (src/db/pg-triggers.ts); no step
// writes those rows any more, so a second run changes nothing.
import type { DatabaseSync } from "node:sqlite";
import type { PgClient } from "./pg-triggers.ts";

export const FIX_RETIRED_REASON = "retired: replaced by Implement";

/** Each statement with its parameters, `p(n)` naming the n-th. */
const STATEMENTS = (main: string, p: (n: number) => string, at: string): [string, string[]][] => [
  [`UPDATE ${main}fix_attempts SET state = 'failed', reason = ${p(1)}, updated_at = ${p(2)} WHERE state IN ('retry', 'testing', 'passed')`, [FIX_RETIRED_REASON, at]],
  [`UPDATE ${main}stage_trips SET active = 0, cleared_at = ${p(1)}, cleared_by = 'retired' WHERE (stage = 'fixissue' OR stage LIKE 'fixissue:%') AND active = 1`, [at]],
  [`UPDATE ${main}push_outcomes SET outcome = 'unknown', resolved_at = ${p(1)} WHERE stage = 'fixissue' AND outcome IS NULL`, [at]],
];

export function retireFixAttemptsSqlite(db: DatabaseSync, at = new Date().toISOString()): void {
  for (const [q, args] of STATEMENTS("main.", () => "?", at)) db.prepare(q).run(...args);
}

export async function retireFixAttemptsPg(client: PgClient, at = new Date().toISOString()): Promise<void> {
  for (const [q, args] of STATEMENTS("", (n) => `$${n}`, at)) await client.query(q, args);
}
