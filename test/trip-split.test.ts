// The once-only split of stage-wide breaker rows into per-repo rows, on rows shaped like production's.
import { test } from "node:test";
import assert from "node:assert/strict";
import { connect } from "../src/db.ts";
import { splitStageTripsSqlite, TRIP_SPLIT_DONE } from "../src/db/trip-split.ts";

test("stage-wide breaker rows split into per-repo rows once, keeping what was paused and when each was last resumed", () => {
  const db = connect(":memory:");
  const T = "2026-11-20T10:00:00.000Z", SPLIT = "2026-11-22T12:00:00.000Z";
  const row = db.prepare("INSERT INTO main.stage_trips (stage, active, reason, evidence, tripped_at, cleared_at, cleared_by) VALUES (?, ?, ?, ?, ?, ?, ?)");
  row.run("fixissue", 1, "2 of the last 10 fixissue pushes broke a required check", JSON.stringify(["deckcraft#2031@6df9331b", "deckcraft#2011@2dfbd40d"]), T, null, null);
  row.run("fixup", 1, "2 of the last 10 fixup pushes broke a required check", JSON.stringify(["designcraft#2875@0b7f99b1", "designcraft#2434@4d408d54"]), T, null, null);
  row.run("cifix", 0, "2 of the last 10 cifix pushes broke a required check", JSON.stringify(["photocraft#1@aaaaaaaa", "wordcraft#2@bbbbbbbb"]), T, "2026-11-15T00:00:00.000Z", "op");
  row.run("merge:sandbox", 0, "the post-merge review of sandbox#1 found 1 defect", "[]", T, "2026-11-01T00:00:00.000Z", "op");
  // designcraft's fix-up breaker was resumed on its own before: the active split row takes over and keeps that resume.
  row.run("fixup:designcraft", 0, "old", "[]", "2026-11-02T00:00:00.000Z", "2026-11-03T00:00:00.000Z", "m1");
  const push = db.prepare("INSERT INTO main.push_outcomes (at, stage, action, repo, number, to_oid, before_res) VALUES ('2026-11-01T00:00:00Z', ?, 'x', ?, 1, 'abc', 'green')");
  for (const [stage, repo] of [["fixissue", "deckcraft"], ["fixissue", "photocraft"], ["cifix", "photocraft"], ["cifix", "gridcraft"]]) push.run(stage, repo);
  const rows = () => db.prepare("SELECT stage, active, reason, evidence, cleared_at, cleared_by FROM main.stage_trips ORDER BY stage").all().map((r) => ({ ...r }));

  db.prepare("DELETE FROM main.settings WHERE key = ?").run(TRIP_SPLIT_DONE); // opening the database ran it on the empty table
  splitStageTripsSqlite(db, SPLIT);
  const after = rows();
  assert.deepEqual(after.filter((r) => r.active === 1).map((r) => [r.stage, r.reason, JSON.parse(r.evidence as string)]), [
    ["fixissue:deckcraft", "2 of the last 10 fixissue pushes broke a required check in deckcraft", ["deckcraft#2031@6df9331b", "deckcraft#2011@2dfbd40d"]],
    ["fixup:designcraft", "2 of the last 10 fixup pushes broke a required check in designcraft", ["designcraft#2875@0b7f99b1", "designcraft#2434@4d408d54"]],
  ]);
  const of = (stage: string) => after.find((r) => r.stage === stage)!;
  assert.equal(of("fixup:designcraft").cleared_at, "2026-11-03T00:00:00.000Z", "the repo's own last resume stays");
  // The inactive cifix row's last resume carries to the repos with cifix pushes; repos without pushes get no row.
  assert.deepEqual(["cifix:gridcraft", "cifix:photocraft"].map((k) => [of(k).active, of(k).cleared_at]), [[0, "2026-11-15T00:00:00.000Z"], [0, "2026-11-15T00:00:00.000Z"]]);
  assert.equal(after.find((r) => r.stage === "cifix:wordcraft"), undefined);
  assert.equal(after.find((r) => r.stage === "fixissue:photocraft"), undefined, "a repo the active row does not name is not paused");
  assert.deepEqual([of("merge:sandbox").cleared_at, of("merge:sandbox").reason], ["2026-11-01T00:00:00.000Z", "the post-merge review of sandbox#1 found 1 defect"]);
  // The stage rows stay as the breakers on every repo, inactive from the split on.
  assert.deepEqual(["cifix", "fixissue", "fixup"].map((k) => [of(k).active, of(k).cleared_at, of(k).cleared_by]), Array(3).fill([0, SPLIT, "migration"]));

  // Once only: a later stage row is a trip on every repo and stays as it is.
  db.prepare("UPDATE main.stage_trips SET active = 1 WHERE stage = 'fixissue'").run();
  splitStageTripsSqlite(db, "2026-11-23T00:00:00.000Z");
  assert.deepEqual(rows(), after.map((r) => (r.stage === "fixissue" ? { ...r, active: 1 } : r)));
});
