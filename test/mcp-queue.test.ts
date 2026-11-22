// The Inbox MCP server's merge queue tools: an operator's agent queues, weights and withdraws PRs with the Queue page's
// own handlers (pre-approval, fast track); a maintainer who answers decisions queues items too; other roles only read the queue.
import assert from "node:assert/strict";
import { afterEach, before, describe, test } from "node:test";
import { answering, handleMcp, type McpEnv } from "../src/dash/mcp/index.ts";
import { RateLimiter } from "../src/dash/server/util.ts";
import type { Role } from "../src/dash/types.ts";
import { refreshPlanCache } from "../src/core/plan-cache.ts";
import { addFastTrack, fastTrackList } from "../src/fasttrack.ts";
import { insertBatch, updateBatch } from "../src/merge/batch-store.ts";
import { preapprovalOf } from "../src/preapproval/index.ts";
import { OPERATOR, world, type World } from "./world-db.ts";
import { excluded } from "../src/db/kx.ts";

let current: World | null = null;
// The pre-approval tools need an answering token, which exists only while answering is available.
before(() => { answering.enabled = true; });
const fresh = async (): Promise<World> => (current = await (await world()).repo());
afterEach(() => current?.close());

/** `who.role` is read on every call, as the server reads the access list: a test changes it to demote the login under a live session. */
const envOf = (w: World, role: Role, login = role === "operator" ? OPERATOR : "maint", who = { role }, canAnswer = true): McpEnv => ({
  db: w.db, baseUrl: null, limiter: new RateLimiter(60, 3600_000), canAnswer, expensive: () => 0,
  user: async () => ({ login, role: who.role, canWrite: async () => true, canRead: async () => true }),
});

let id = 0;
async function rpc(env: McpEnv, method: string, params?: Record<string, unknown>): Promise<any> {
  return (await handleMcp(env, { jsonrpc: "2.0", id: ++id, method, ...(params ? { params } : {}) }))!.result;
}
async function call(env: McpEnv, name: string, args: Record<string, unknown> = {}): Promise<{ text: string; isError: boolean }> {
  const r = await rpc(env, "tools/call", { name, arguments: args });
  return { text: r.content[0].text as string, isError: r.isError === true };
}
const names = async (env: McpEnv): Promise<string[]> => (await rpc(env, "tools/list")).tools.map((t: { name: string }) => t.name);
const weights = async (w: World) => (await fastTrackList(w.db)).map((f) => [f.number, f.weight]);
const data = (text: string): any => JSON.parse(text.slice(text.indexOf(">>>\n") + 4, text.lastIndexOf("\n<<<end untrusted>>>")));

describe("merge queue tools", () => {
  test("an operator is offered the write tools; a maintainer's token also steers the queue where they write, without weights on queue_pr; everyone reads the queue", async () => {
    const w = await fresh();
    const op = envOf(w, "operator"), maint = envOf(w, "maintainer"), viewer = envOf(w, "viewer");
    const maintNoAnswer = envOf(w, "maintainer", "maint", { role: "maintainer" }, false);
    const writes = (list: string[]) => list.filter((n) => ["queue_pr", "set_weight", "cancel_queued"].includes(n));
    for (const n of ["queue_pr", "set_weight", "cancel_queued", "list_merge_queue"]) assert.ok((await names(op)).includes(n), n);
    for (const env of [maint, maintNoAnswer, viewer]) assert.ok((await names(env)).includes("list_merge_queue"));
    assert.deepEqual(writes(await names(maint)), ["set_weight", "queue_pr", "cancel_queued"]);
    // Weights are set by operators: a maintainer's queue_pr takes none.
    assert.equal((await rpc(maint, "tools/list")).tools.find((t: { name: string }) => t.name === "queue_pr").inputSchema.properties.weight, undefined);
    assert.deepEqual(writes(await names(maintNoAnswer)), ["set_weight"]);
    assert.deepEqual(writes(await names(viewer)), []);
    assert.match((await rpc(op, "initialize", { protocolVersion: "2025-06-18" })).instructions, /queue_pr/);
    const maintText = (await rpc(maint, "initialize", { protocolVersion: "2025-06-18" })).instructions;
    assert.match(maintText, /queue_pr/);
    assert.match(maintText, /set_weight/);
    assert.doesNotMatch((await rpc(maintNoAnswer, "initialize", { protocolVersion: "2025-06-18" })).instructions, /queue_pr/);
  });

  test("an operator token that cannot answer is not offered queue_pr or cancel_queued, and a call is refused", async () => {
    const w = await (await fresh()).pr(1);
    const op = envOf(w, "operator", OPERATOR, { role: "operator" }, false);
    const list = await names(op);
    assert.deepEqual(list.filter((n) => ["queue_pr", "cancel_queued"].includes(n)), []);
    assert.ok(list.includes("set_weight") && list.includes("list_merge_queue"));
    assert.doesNotMatch((await rpc(op, "initialize", { protocolVersion: "2025-06-18" })).instructions, /queue_pr/);
    for (const name of ["queue_pr", "cancel_queued"]) {
      const r = await call(op, name, { item: "app#1" });
      assert.equal(r.isError, true, name);
      assert.match(r.text, /cannot answer decisions/);
    }
    assert.equal(await preapprovalOf(w.db, "app", 1), null);
    assert.equal((await call(op, "set_weight", { item: "app#1", weight: 3 })).isError, false);
  });

  test("queue_pr needs write permission on the repo and audits its writes as mcp", async () => {
    const w = await (await fresh()).pr(1);
    const noWrite: McpEnv = { ...envOf(w, "operator"), user: async () => ({ login: OPERATOR, role: "operator", canWrite: async () => false, canRead: async () => true }) };
    const r = await call(noWrite, "queue_pr", { item: "app#1" });
    assert.equal(r.isError, true);
    assert.match(r.text, /no write access to storytold\/app/);
    assert.equal(await preapprovalOf(w.db, "app", 1), null);
    const op = envOf(w, "operator");
    await call(op, "queue_pr", { item: "app#1" });
    await call(op, "cancel_queued", { item: "app#1" });
    await call(op, "set_weight", { item: "app#1", remove: true });
    const rows = await w.db.all(w.db.k.selectFrom("audit").select(["action", "details"]).where("action", "in", ["preapproval", "fasttrack-add", "fasttrack-remove"]));
    assert.ok(rows.length >= 4);
    for (const x of rows) assert.equal(JSON.parse(x.details ?? "{}").via, "mcp", `${x.action} ${x.details}`);
  });

  test("a viewer gets no batch summary, staff do", async () => {
    const w = await (await fresh()).pr(1);
    const id = await insertBatch(w.db, { repo: "app", branch: "batch-1", baseSha: "b", commitSha: "c", parentId: null, members: [{ number: 1, head_oid: "h" }], at: "2026-01-01T00:00:00Z" });
    await updateBatch(w.db, id, "2026-01-01T00:00:00Z", { summary: "push failed: remote said no" });
    await refreshPlanCache(w.db);
    for (const [role, want] of [["operator", "push failed: remote said no"], ["maintainer", "push failed: remote said no"], ["viewer", null]] as const) {
      const d = data((await call(envOf(w, role), "list_merge_queue", { repo: "app" })).text);
      assert.equal(d.batches.length, 1, role);
      assert.equal(d.batches[0].summary, want, role);
    }
  });

  test("a maintainer queues only where they answer decisions, with the default weight; set_weight and cancel_queued need write on the repo", async () => {
    const w = await (await (await fresh()).pr(1)).pr(2);
    const maint = envOf(w, "maintainer");
    const refusedAll = async () => {
      for (const [name, args, why] of [["queue_pr", { item: "app#1" }, /only an operator, or a maintainer with write access where the app acts for them/]] as const) {
        const r = await call(maint, name, args);
        assert.equal(r.isError, true, name);
        assert.match(r.text, why, name);
      }
      assert.equal(await preapprovalOf(w.db, "app", 1), null);
      assert.deepEqual(await weights(w), []);
    };
    // Maintainer writes off, then on without the app identity: nothing changes.
    await refusedAll();
    await w.db.run(w.db.k.insertInto("settings").values({ key: "maintainer_writes", value: "1" }).onConflict((oc) => oc.column("key").doUpdateSet(excluded(["value"]))));
    await refusedAll();
    await w.db.run(w.db.k.insertInto("settings").values({ key: "github_identity", value: "app" }).onConflict((oc) => oc.column("key").doUpdateSet(excluded(["value"]))));
    const r = await call(maint, "queue_pr", { item: "app#1", weight: 6 });
    assert.equal(r.isError, false, r.text);
    assert.match(r.text, /The weight was ignored: only operators set fast-track weights/);
    assert.equal((await preapprovalOf(w.db, "app", 1))?.by, "maint");
    assert.deepEqual(await weights(w), [[1, 5]]);
    // An item an operator fast-tracked keeps its weight, lighter or not.
    await addFastTrack(w.db, OPERATOR, "app#2", 3);
    assert.equal((await call(maint, "queue_pr", { item: "app#2", weight: 9 })).isError, false);
    assert.deepEqual(await weights(w), [[1, 5], [2, 3]]);
    // Weights and withdrawing a queued merge are steering, open to a maintainer with write on the repo.
    assert.equal((await call(maint, "set_weight", { item: "app#2", weight: 9 })).isError, false);
    assert.equal((await call(maint, "cancel_queued", { item: "app#1" })).isError, false);
  });

  test("an operator demoted after tools/list is refused on the next call, with the tools still in its client's list", async () => {
    const w = await (await (await fresh()).pr(1)).pr(2);
    const who: { role: Role } = { role: "operator" };
    const env = envOf(w, "operator", OPERATOR, who);
    assert.ok((await names(env)).includes("queue_pr"));
    assert.equal((await call(env, "queue_pr", { item: "app#2", weight: 4 })).isError, false);
    who.role = "maintainer";
    // A maintainer queues only where they answer decisions (maintainer writes are off here).
    for (const [name, args, why] of [["queue_pr", { item: "app#1" }, /only an operator, or a maintainer/]] as const) {
      const r = await call(env, name, args);
      assert.equal(r.isError, true, name);
      assert.match(r.text, why, name);
    }
    assert.equal(await preapprovalOf(w.db, "app", 1), null);
    assert.equal((await preapprovalOf(w.db, "app", 2))?.state, "review");
    assert.deepEqual(await weights(w), [[2, 4]]);
    // As a maintainer with an answering token it is offered the maintainer's set.
    assert.deepEqual((await names(env)).filter((n) => ["queue_pr", "set_weight", "cancel_queued"].includes(n)), ["set_weight", "queue_pr", "cancel_queued"]);
  });

  test("queue_pr pre-approves the PR at its head and fast-tracks it with the weight, as the Queue page does", async () => {
    const w = await (await (await fresh()).pr(1)).pr(2);
    const op = envOf(w, "operator");
    const r = await call(op, "queue_pr", { item: "app#1", weight: 8 });
    assert.equal(r.isError, false, r.text);
    assert.match(r.text, /Added to the merge queue/);
    assert.match(r.text, /weight 8, 1 of 1/);
    assert.equal((await preapprovalOf(w.db, "app", 1))?.state, "review");
    assert.equal((await preapprovalOf(w.db, "app", 1))?.by, OPERATOR);
    assert.deepEqual(await weights(w), [[1, 8]]);
    // The default weight; a URL works as the item too.
    assert.equal((await call(op, "queue_pr", { item: "https://github.com/storytold/app/pull/2" })).isError, false);
    assert.deepEqual(await weights(w), [[1, 8], [2, 5]]);
    // Queued twice: the pre-approval's own refusal comes back.
    const again = await call(op, "queue_pr", { item: "app#1" });
    assert.equal(again.isError, true);
    assert.match(again.text, /already has a pre-approved merge in progress/);
  });

  test("set_weight changes, adds without approving, and removes; its arguments are checked", async () => {
    const w = await (await (await fresh()).pr(1)).pr(2);
    const op = envOf(w, "operator");
    await call(op, "queue_pr", { item: "app#1", weight: 8 });
    assert.match((await call(op, "set_weight", { item: "app#1", weight: 3 })).text, /Set the weight\. app#1 is fast-tracked with weight 3/);
    assert.match((await call(op, "set_weight", { item: "app#2", weight: 6 })).text, /not approved by this/);
    assert.deepEqual(await weights(w), [[2, 6], [1, 3]]);
    assert.equal(await preapprovalOf(w.db, "app", 2), null);
    assert.match((await call(op, "set_weight", { item: "app#2", remove: true })).text, /Took app#2 off/);
    assert.match((await call(op, "set_weight", { item: "app#2", remove: true })).text, /was not fast-tracked/);
    assert.deepEqual(await weights(w), [[1, 3]]);
    for (const args of [{ item: "app#1", weight: 11 }, { item: "app#1", weight: 2.5 }, { item: "app#1", weight: 4, remove: true }, { item: "nothing here" }, { item: "app#1", remove: "yes" }]) {
      assert.equal((await call(op, "set_weight", args)).isError, true, JSON.stringify(args));
    }
    assert.deepEqual(await weights(w), [[1, 3]]);
  });

  test("cancel_queued withdraws the pre-approval and keeps the weight; a second cancel is refused", async () => {
    const w = await (await fresh()).pr(1);
    const op = envOf(w, "operator");
    await call(op, "queue_pr", { item: "app#1", weight: 7 });
    const r = await call(op, "cancel_queued", { item: "app#1" });
    assert.equal(r.isError, false, r.text);
    assert.match(r.text, /is cancelled\. app#1 is fast-tracked with weight 7.*remove: true/);
    assert.equal((await preapprovalOf(w.db, "app", 1))?.state, "cancelled");
    assert.deepEqual(await weights(w), [[1, 7]]);
    const again = await call(op, "cancel_queued", { item: "app#1" });
    assert.equal(again.isError, true);
    assert.match(again.text, /no pre-approved merge in progress/);
  });

  test("list_merge_queue shows what an agent queued, with its weight, to any role; viewers get no logins", async () => {
    const w = await (await (await fresh()).pr(1)).pr(2);
    await call(envOf(w, "operator"), "queue_pr", { item: "app#2", weight: 9 });
    for (const role of ["operator", "maintainer", "viewer"] as const) {
      const r = await call(envOf(w, role), "list_merge_queue", { repo: "app" });
      assert.equal(r.isError, false, r.text);
      const d = data(r.text);
      const row = d.prs.find((p: { item: string }) => p.item === "app#2");
      const pending = d.preapprovedBeforeReview.find((p: { item: string }) => p.item === "app#2");
      assert.ok(row || pending, r.text);
      if (row) {
        assert.equal(row.fastTrack.weight, 9);
        assert.equal(row.added.how, "queued");
        assert.equal(row.added.by, role === "viewer" ? null : OPERATOR);
      } else assert.equal(pending.by, role === "viewer" ? null : OPERATOR);
    }
    assert.equal((await call(envOf(w, "viewer"), "list_merge_queue", { cursor: "x" })).isError, true);
  });

  /** Every page of list_merge_queue at `limit`, following the cursor. */
  async function pages(env: McpEnv, limit: number): Promise<{ prs: string[]; pending: string[]; cursors: string[] }> {
    const out = { prs: [] as string[], pending: [] as string[], cursors: [] as string[] };
    let cursor: string | undefined;
    for (let k = 0; k < 20; k++) {
      const r = await call(env, "list_merge_queue", { repo: "app", limit, ...(cursor ? { cursor } : {}) });
      assert.equal(r.isError, false, r.text);
      const d = data(r.text);
      out.prs.push(...d.prs.map((p: { item: string }) => p.item));
      out.pending.push(...d.preapprovedBeforeReview.map((p: { item: string }) => p.item));
      if (d.next === null) return out;
      out.cursors.push(d.next);
      cursor = d.next;
    }
    throw new Error("the cursor never ended");
  }

  test("a queued PR that is a queue row on a later page is never listed as before its review", async () => {
    const w = await (await (await (await (await (await fresh()).pr(1)).review(1)).pr(2)).review(2)).pr(3);
    const op = envOf(w, "operator");
    for (const n of [1, 2, 3]) assert.equal((await call(op, "queue_pr", { item: `app#${n}` })).isError, false);
    await refreshPlanCache(w.db);
    const all = await pages(op, 1);
    assert.deepEqual(all.prs.sort(), ["app#1", "app#2"]);
    assert.deepEqual(all.pending, ["app#3"]);
  });

  test("pre-approvals before review page on with the cursor: none is skipped or shown twice", async () => {
    const w = await (await (await (await (await (await fresh()).pr(1)).pr(2)).pr(3)).pr(4)).pr(5);
    const op = envOf(w, "operator");
    for (const n of [1, 2, 3, 4, 5]) await call(op, "queue_pr", { item: `app#${n}` });
    const all = await pages(op, 2);
    assert.deepEqual(all.pending, ["app#1", "app#2", "app#3", "app#4", "app#5"]);
    assert.equal(all.cursors.length, 2);
    assert.ok(all.cursors.every((c) => /^x:\d+$/.test(c)), all.cursors.join());
    for (const cursor of ["7", "1:", "a:b", "1:2:3"]) assert.equal((await call(op, "list_merge_queue", { cursor })).isError, true, cursor);
  });
});
