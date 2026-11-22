import assert from "node:assert/strict";
import { test } from "node:test";
import { needs } from "../src/dash/api/needs.ts";
import type { Req } from "../src/dash/types.ts";
import type { NeedsPage } from "../web/src/api/types.ts";
import { connectDb } from "../src/db/open.ts";
import * as kx from "../src/db/kx.ts";

const as = (role: "viewer" | "operator", query: Record<string, string> = {}): Req => ({
  method: "GET", path: "", params: {}, query: new URLSearchParams(query), form: new URLSearchParams(),
  user: { login: "u", role, canWrite: async () => true, canRead: async (r: string) => r !== "secret" }, csrf: "", nonce: "",
});

test("needs lists feedback newest first with handled state, shows a tripped breaker per repo, and hides a private repo from a viewer", async () => {
  const db = await connectDb(":memory:");
  const now = Date.now();
  const at = (h: number) => new Date(now - h * 3600_000).toISOString();
  for (const [name, vis] of [["photocraft", "public"], ["secret", "private"]]) {
    await db.prepare("INSERT OR IGNORE INTO tracked_repos (name) VALUES (?)").run(name);
    await db.prepare("INSERT INTO repos (name, visibility) VALUES (?, ?) ON CONFLICT (name) DO UPDATE SET visibility = excluded.visibility").run(name, vis);
  }
  await db.run(db.k.insertInto("settings").values({ key: "pipeline_login", value: "bot" }));
  for (const [repo, n] of [["photocraft", 1], ["photocraft", 2], ["photocraft", 4], ["secret", 3]] as const)
    await db.run(db.k.insertInto("items").values({ repo, number: n, kind: "pr", state: "OPEN", title: `Title ${n}` }));
  // Item 1: a comment that mentions AutoCraft, answered by AutoCraft later. Item 2: a reply to an AutoCraft comment, unanswered.
  const c = (repo: string, number: number, id: string, author: string, created_at: string, body: string) => db.run(db.k.insertInto("comments").values({ repo, number, id, kind: "comment", author, created_at, body }));
  await c("photocraft", 1, "c1", "dev", at(10), "@bot please recheck");
  await c("photocraft", 1, "c2", "bot", at(9), "Rechecked");
  await c("photocraft", 2, "c3", "bot", at(8), "Review");
  await c("photocraft", 2, "c4", "dev", at(2), "I disagree");
  await c("photocraft", 2, "c5", "other", at(1), "unrelated chatter after a person");
  // The comment text was recorded with CRLF line endings; a review we posted is recorded by its node id only.
  await c("photocraft", 4, "R_node1", "bot", at(8), "A review");
  await c("photocraft", 4, "c7", "dev", at(3), "I disagree with the review");
  const au = (at: string, action: string, target: string, details: string) => db.run(db.k.insertInto("audit").values({ at, login: "bot", action, target, details }));
  await au(at(8), "act-comment", "photocraft#2", JSON.stringify({ url: "https://example.test/c3", text: "Review\r\n" }));
  await au(at(8), "review-post", "photocraft#4", JSON.stringify({ review: "R_node1" }));
  await c("secret", 3, "c6", "dev", at(3), "@bot hidden one");
  await db.run(db.k.updateTable("comments").set({ mentions_pipeline: 1 }).where((eb) => kx.like(db, "body", eb.val("@bot%")))); // what the mirror sets when it writes the comment
  await db.run(db.k.insertInto("decisions").values({ id: "d1", kind: "scope", repo: "photocraft", number: 2, question: "Q?", options: "[]", asked_at: at(20), answer: "yes", answered_at: at(5) }));
  await db.run(db.k.insertInto("decision_comments").values({ id: "dc1", decision_id: "d1", login: "maint", at: at(6), body: "before the answer" }));
  await db.run(db.k.insertInto("stage_trips").values({ stage: "merge:photocraft", active: 1, reason: "two merges broke main", evidence: "[]", tripped_at: at(1) }));
  await db.run(db.k.insertInto("stage_trips").values({ stage: "fixup:secret", active: 1, reason: "2 of the last 10 fixup pushes in secret broke a required check", evidence: "[]", tripped_at: at(2) }));

  const op = (await needs(db, as("operator", { feedback: "1" }))) as NeedsPage;
  assert.deepEqual(op.feedback!.rows.map((r) => [r.id, r.handled]), [["comment:c4", false], ["comment:c7", false], ["comment:c6", false], ["decision-comment:dc1", true], ["comment:c1", true]]);
  assert.equal(op.feedback!.rows[0]!.href, "/items/photocraft/2#event=comment%3Ac4");
  assert.equal(op.counts.feedback, 3);
  assert.equal(op.feedback!.rows.find((r) => r.id === "decision-comment:dc1")!.href, "/needs/decisions/d1", "the decision is opened by its id");
  assert.equal(op.counts.decisions, 0);
  assert.deepEqual(op.stopped.map((s) => [s.id, s.repo, s.resume]), [
    ["breaker:fixup:secret", "secret", { type: "trip", stage: "fixup:secret", canResume: true }],
    ["breaker:merge:photocraft", "photocraft", { type: "trip", stage: "merge:photocraft", canResume: true }],
  ]);

  const paged = (await needs(db, as("operator", { feedback: "1", limit: "3" }))) as NeedsPage;
  const next = (await needs(db, as("operator", { feedback: "1", limit: "3", cursor: paged.feedback!.nextCursor! }))) as NeedsPage;
  assert.deepEqual([...paged.feedback!.rows, ...next.feedback!.rows].map((r) => r.id), op.feedback!.rows.map((r) => r.id));

  const viewer = (await needs(db, as("viewer", { feedback: "1" }))) as NeedsPage;
  assert.ok(!viewer.feedback!.rows.some((r) => r.item?.repo === "secret"));
  assert.deepEqual(viewer.stopped.map((s) => s.id), ["breaker:merge:photocraft"], "a trip on a hidden repo is left out");
  assert.equal(viewer.counts.feedback, 2);
});
