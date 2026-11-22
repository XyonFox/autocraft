// Per-repo and per-item actions are for whoever runs the repo (operator, or maintainer with GitHub write); instance-wide ones stay with operators.
import { test } from "node:test";
import assert from "node:assert/strict";
import { connectDb } from "../src/db/open.ts";
import { ask } from "../src/decisions.ts";
import { SCOPE_OPTIONS } from "../src/decide/decision.ts";
import { snooze, backlogBulk } from "../src/dash/api/decisions.ts";
import { reviewItemAgain } from "../src/dash/api/operator.ts";
import { fastTrackAdd, mergeHoldPr } from "../src/dash/api/queue-page.ts";
import { release, releaseAll } from "../src/dash/api/outside.ts";
import { edit } from "../src/dash/api/areas.ts";

const t = "2026-11-01T00:00:00.000Z";
const as = (role: string, writable: string[], params: Record<string, string> = {}, json?: unknown) => ({
  method: "POST", path: "", params, query: new URLSearchParams(), form: new URLSearchParams(), csrf: "", nonce: "", json,
  user: { login: "u1", role, canWrite: async (r: string) => writable.includes(r), canRead: async () => true },
}) as never;

async function world() {
  const db = await connectDb(":memory:");
  await db.run(db.k.insertInto("repos").values({ name: "app", visibility: "public" }));
  await db.run(db.k.insertInto("tracked_repos").values({ name: "app", added_by: "op", added_at: t, pipeline: 1 }));
  await db.run(db.k.insertInto("items").values({ repo: "app", number: 1, kind: "pr", state: "OPEN", title: "PR", head_oid: "h1", updated_at: t }));
  await db.run(db.k.insertInto("opus_reviews").values({ repo: "app", number: 1, head_oid: "h1", at: t, mode: "full", verdict: "hold", reason: "needs work" }));
  await ask(db, [{
    id: "app#1:scope:x", kind: "scope", repo: "app", number: 1, question: "q", context: "c", facts: [],
    options: [{ ...SCOPE_OPTIONS.close }, { ...SCOPE_OPTIONS.take }], recommended: null, asked_by: "pipeline", asked_at: t,
  }]);
  return db;
}

test("a maintainer with write runs one action per area; without write, or as a viewer, every one is refused", async () => {
  const db = await world();
  const item = { repo: "app", n: "1" };
  const actions: [string, (req: never) => Promise<unknown>, Record<string, string>, unknown?][] = [
    ["snooze", (r) => snooze(db, r), { id: "app#1:scope:x" }, { on: true }],
    ["review again", (r) => reviewItemAgain(db, r), item],
    ["fast track", (r) => fastTrackAdd(db, r), {}, { item: "app#1" }],
    ["hold", (r) => mergeHoldPr(db, r, true), {}, { item: "app#1" }],
    ["release outside", (r) => release(db, r), item],
    ["area edit", (r) => edit(db, r), { repo: "app" }, { op: "add", name: "Canvas", description: "drawing" }],
  ];
  for (const [what, run, params, json] of actions) {
    await assert.rejects(run(as("maintainer", [], params, json)), { status: 403 }, `${what}: maintainer without write`);
    await assert.rejects(run(as("viewer", ["app"], params, json)), { status: 403 }, `${what}: viewer`);
  }
  for (const [what, run, params, json] of actions) {
    await assert.doesNotReject(run(as("maintainer", ["app"], params, json)).catch((e: { status?: number }) => { if (e.status === 403) throw e; }), `${what}: maintainer with write`);
  }
  assert.equal((await db.all(db.k.selectFrom("opus_reviews").select("head_oid"))).length, 0);
  assert.ok(await db.get(db.k.selectFrom("main.merge_holds").select("repo").where("repo", "=", "app")));
  assert.ok(await db.get(db.k.selectFrom("main.fast_track").select("repo").where("repo", "=", "app")));
});

test("bulk Backlog without a repo and release-all of outside authors stay with operators", async () => {
  const db = await world();
  await assert.rejects(backlogBulk(db, as("maintainer", ["app"], {}, { outsidePilot: true })), { status: 403 });
  await assert.rejects(releaseAll(db, as("maintainer", ["app"], {}, { author: "x" })), { status: 403 });
  await assert.rejects(backlogBulk(db, as("maintainer", [], {}, { repo: "app" })), { status: 403 });
});

test("scoped bulk Backlog leaves a decision alone when it covers a repo the maintainer cannot write, and does not list it", async () => {
  const db = await world();
  await db.run(db.k.insertInto("repos").values({ name: "other", visibility: "public" }));
  await db.run(db.k.insertInto("tracked_repos").values({ name: "other", added_by: "op", added_at: t, pipeline: 1 }));
  await db.run(db.k.insertInto("access").values({ login: "u1", role: "maintainer" }));
  await db.run(db.k.insertInto("settings").values([{ key: "maintainer_writes", value: "1" }, { key: "github_identity", value: "app" }]));
  await ask(db, [{
    id: "batch:x", kind: "scope", repo: "app", number: null, question: "batch", context: "c", facts: [], items: [{ repo: "app", number: 1 }, { repo: "other", number: 5 }],
    options: [{ ...SCOPE_OPTIONS.close }, { ...SCOPE_OPTIONS.take }], recommended: null, asked_by: "pipeline", asked_at: t,
  }]);
  const res = await backlogBulk(db, as("maintainer", ["app"], {}, { repo: "app" }));
  assert.deepEqual(res.decisions.map((d) => d.id), ["app#1:scope:x"]);
});
