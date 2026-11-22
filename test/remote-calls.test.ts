// The user-local inference mode: askClaude queues a remote call, a helper claims it, fetches the bundle and submits, and
// askClaude returns the reply. A lease without heartbeats goes back to the queue; the item's authors never get a judging call.
import assert from "node:assert/strict";
import { mkdirSync, mkdtempSync } from "node:fs";
import { join } from "node:path";
import { describe, test } from "node:test";
import { allocation, WaitLimitError } from "../src/agents/call.ts";
import { askClaude, setWorkspace } from "../src/classify/run.ts";
import type { Lint } from "../src/replylint.ts";
import { handleMcp, type McpEnv } from "../src/dash/mcp/index.ts";
import { noteLine } from "../src/github/note.ts";
import { createToken } from "../src/dash/mcp/tokens.ts";
import { RateLimiter } from "../src/dash/server/util.ts";
import type { User } from "../src/dash/types.ts";
import { SAMPLE_OPUS_PRS } from "../src/prompts/samples.ts";
import { bundleOf, claimableCalls, claimCall, CLAIM_RETRY_S, enqueueCall, expireLeases, heartbeat, leaseState, withdrawCall, submitCall, type Caller } from "../src/remote/calls.ts";
import { stampAlive } from "../src/remote/liveness.ts";
import { clearCooloff } from "../src/remote/cooloff.ts";
import * as userSettings from "../src/dash/api/user-settings.ts";
import type { Req } from "../src/dash/types.ts";
import { listHelpers, parseStatus, storeStatus } from "../src/remote/helpers.ts";
import { remoteRoom } from "../src/remote/room.ts";
import { setAcceptedModels, setInferenceMode } from "../src/remote/mode.ts";
import { PROTOCOL_VERSION, type Claim } from "../src/remote/protocol.ts";
import { SCRATCH } from "./scratch.ts";
import { mapSeq } from "../src/async-seq.ts";
import { connectDb } from "../src/db/open.ts";
import { excluded } from "../src/db/kx.ts";

mkdirSync(SCRATCH, { recursive: true });
const dir = mkdtempSync(join(SCRATCH, "remote-calls-"));

describe("user-local remote calls", async () => {
  const db = await connectDb(join(dir, "t.db"));
  allocation.pollMs = 10;
  await setInferenceMode(db, "user-local", "test");
  const T0 = new Date().toISOString();
  await db.run(db.k.insertInto("items").values({ repo: "photocraft", number: 1287, kind: "pr", state: "OPEN", title: "t", author: "alice", created_at: T0, updated_at: T0 }));
  await db.run(db.k.insertInto("pr_commits").values({ repo: "photocraft", number: 1287, oid: "abc", committed_at: T0, author: "carol", co_authors: JSON.stringify(["carol", "Dave"]) }));
  const callers: Record<string, Caller> = {};
  for (const login of ["alice", "bob", "carol", "dave"]) {
    await db.run(db.k.insertInto("access").values({ login, role: "maintainer" }));
    callers[login] = { tokenId: (await createToken(db, login, "helper", { kind: "runner" })).row.id, login, role: "maintainer" };
  }
  let claims = 0;
  /** A claim body with its own request id: a repeated id gets the first claim's reply. */
  const claim = (): Claim => ({ requestId: `test-claim-${String(++claims).padStart(4, "0")}`, protocol: PROTOCOL_VERSION, helperVersion: "1.2.0", harness: "claude", harnessVersion: "2.1", tiers: { top: "opus" } });
  const canWrite = async () => true;
  const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
  const leaseFor = async (login: string) => {
    for (let i = 0; i < 200; i++) {
      // A call queued before the claim and not leased by it: nothing for this login. One queued while the claim ran is tried again.
      const queued = !!await db.get(db.k.selectFrom("remote_calls").select((eb) => eb.lit(1).as("one")).where("state", "=", "queued"));
      const r = await claimCall(db, callers[login]!, claim(), canWrite);
      if (r.lease) return r.lease;
      if (queued) return null;
      await sleep(5);
    }
    throw new Error("no call was queued");
  };
  /** `login`'s helper asked for top-tier work a moment ago and found none; its write permission on the repo is cached. */
  const idle = async (login: string) => {
    await db.run(db.k.updateTable("mcp_tokens").set({ idle_claim_at: Date.now(), idle_tiers: '["top"]', idle_repos: null }).where("login", "=", login));
    await db.run(db.k.insertInto("repo_permissions").values({ login, repo: "photocraft", permission: "write", checked_at: new Date().toISOString() })
      .onConflict((oc) => oc.columns(["login", "repo"]).doUpdateSet(excluded(["permission", "checked_at"]))));
  };
  const req = (login: string, role: string, json?: unknown) => ({ user: { login, role, canWrite: async () => true }, json }) as unknown as Req;
  const done = async (secret: string, text: string) => await submitCall(db, secret, { ok: true, text, model: "claude-opus-4-5", usage: { inputTokens: 100, outputTokens: 20 }, modelUsage: { "claude-opus-4-5": { inputTokens: 90, outputTokens: 15 }, "claude-haiku-4-5": { inputTokens: 10, outputTokens: 5 } }, costUsd: 0.25,
    tools: [{ name: "mcp__items__item", args: '{"ref":"photocraft#1"}', resultBytes: 300, isError: false, startedAt: "2026-11-19T10:00:00.000Z", endedAt: "2026-11-19T10:00:01.000Z" }], durationMs: 1000 });

  test("a helper claims, fetches the bundle and submits; authors and co-authors give way while another helper is idle", async () => {
    const ask = askClaude("review.opus", "opus", SAMPLE_OPUS_PRS, db);
    await idle("bob");
    assert.equal(await leaseFor("alice"), null, "the PR's author");
    assert.equal(await leaseFor("carol"), null, "a committer on the PR");
    assert.equal(await leaseFor("dave"), null, "a co-author of a commit");
    await assert.rejects(claimCall(db, callers.bob!, { ...claim(), helperVersion: "0.9.0" }, canWrite), /older than/);
    const lease = (await leaseFor("bob"))!;
    assert.match(lease.title, /^review\.opus on photocraft#1287/);
    const b = (await bundleOf(db, lease.secret))!;
    assert.equal(b.tier, "top");
    assert.equal(b.callId, lease.id);
    assert.ok(b.stdin.length > 0 && b.system !== null);
    assert.deepEqual(b.mounts, []);
    assert.ok(b.hints.claude!.args.includes("--model"));
    assert.equal(await heartbeat(db, lease.secret, { progress: "reading" }), true);
    assert.deepEqual(await done(lease.secret, "the review"), { accepted: true });
    const r = await ask;
    assert.equal(r?.text, "the review");
    assert.equal(r?.model, "claude-opus-4-5", "the stage learns the model that ran");
    assert.equal(noteLine({ by: "model", kind: "review", model: r?.model }), "_AI-generated review (Claude Opus 4.5). Not a maintainer decision._");
    assert.equal(noteLine({ by: "model", kind: "review", model: "gpt-6.2-astra" }), "_AI-generated review (Codex Astra 6.2). Not a maintainer decision._");
    assert.equal(noteLine({ by: "model", kind: "review", model: "opus" }), "_AI-generated review (Opus). Not a maintainer decision._", "a server call names the prompt's model as before");
    assert.equal((await db.get(db.k.selectFrom("remote_calls").select("reply").where("id", "=", lease.id)))!.reply, null, "the raw reply is not kept");
    assert.equal(r?.usage.input, 90);
    assert.deepEqual(r?.usage.extra?.map((e) => [e.model, e.usage.input]), [["haiku", 10]]);
    const run = await db.get(db.k.selectFrom("agent_runs").select(["state", "user_login", "harness", "reported_model", "account_id", "cost_usd"])
      .where("id", "=", (eb) => eb.selectFrom("remote_calls").select("run_id").where("remote_calls.id", "=", lease.id)));
    assert.deepEqual({ ...run }, { state: "done", user_login: "bob", harness: "claude", reported_model: "claude-opus-4-5", account_id: null, cost_usd: 0.25 });
    const tool = await db.prepare("SELECT seq, tool, args, result_bytes, started_at FROM agent_run_tools WHERE run_id = (SELECT run_id FROM remote_calls WHERE id = ?)").get(lease.id) as Record<string, unknown>;
    assert.deepEqual({ ...tool }, { seq: 1, tool: "mcp__items__item", args: '{"ref":"photocraft#1"}', result_bytes: 300, started_at: "2026-11-19T10:00:00.000Z" });
  });

  test("with no other helper asking for work, the PR's author gets the review", async () => {
    await db.run(db.k.updateTable("mcp_tokens").set({ idle_claim_at: null }));
    const ask = askClaude("review.opus", "opus", SAMPLE_OPUS_PRS, db);
    const lease = (await leaseFor("alice"))!;
    assert.ok(lease, "the author's helper takes it");
    await done(lease.secret, "by the author");
    assert.equal((await ask)?.text, "by the author");
  });

  test("a lease without heartbeats goes back to the queue and the next claim gets it; the old secret is dead", async () => {
    const ask = askClaude("review.opus", "opus", SAMPLE_OPUS_PRS, db);
    const first = (await leaseFor("bob"))!;
    await db.run(db.k.updateTable("remote_calls").set((eb) => ({ heartbeat_at: eb("heartbeat_at", "-", 3 * 60000) })).where("id", "=", first.id));
    let second = null;
    for (let i = 0; i < 200 && !second; i++) { second = (await claimCall(db, callers.bob!, claim(), canWrite)).lease; if (!second) await sleep(5); }
    assert.equal(second!.id, first.id, "the same call, leased again");
    assert.equal(await heartbeat(db, first.secret, {}), false);
    assert.equal(await done(first.secret, "stale"), null, "the route answers 410");
    assert.deepEqual(await leaseState(db, first.secret), { id: first.id, live: false }, "a requeued lease's secret is known: 410, not 401");
    assert.equal(await leaseState(db, "aclease_never-issued"), null);
    assert.equal((await db.get(db.k.selectFrom("remote_calls").select("attempt").where("id", "=", first.id)))!.attempt, 2);
    assert.equal(await db.get(db.k.selectFrom("remote_lease_drops").select("token_id").where("login", "=", "bob")), undefined, "a lease that never had a heartbeat (its claim's reply was likely lost) is no drop");
    // A reply from another model family than the claim declared is refused.
    assert.equal((await submitCall(db, second!.secret, { ok: true, text: "x", model: "claude-sonnet-4-5", usage: { inputTokens: 1, outputTokens: 1 }, durationMs: 1 }))?.accepted, false);
    assert.equal(await ask, null, "both attempts are used up");
    const runs = await db.all(db.k.selectFrom("agent_runs").select(["state", "error"]).where("prompt_id", "=", "review.opus").orderBy("id", "desc").limit(2));
    assert.deepEqual(runs.map((x) => x.state), ["failed", "failed"]);
    assert.match(runs[1]!.error!, /lease expired/);
  });

  test("three lost leases of distinct calls of one login within two hours cool its helpers off for six hours; its next claim says why and until when; an operator clears it", async () => {
    await db.run(db.k.insertInto("access").values({ login: "erin", role: "maintainer" }));
    // A token's drops count once in 10 minutes: three helpers of the same user each lose one.
    const tokens = await mapSeq([1, 2, 3], async () => (await createToken(db, "erin", "helper", { kind: "runner" })).row.id);
    let erin: Caller = { tokenId: tokens[0]!, login: "erin", role: "maintainer" };
    const mine = async () => (await claimCall(db, erin, claim(), canWrite)).lease;
    const lose = async (id: number, now: number) => {
      // The helper heartbeat after its claim, then went quiet.
      await db.run(db.k.updateTable("remote_calls").set({ leased_at: now - 4 * 60000, heartbeat_at: now - 3 * 60000 }).where("id", "=", id));
      await expireLeases(db, now);
    };
    const ask = askClaude("review.opus", "opus", SAMPLE_OPUS_PRS, db);
    const t0 = Date.now();
    let lease = null;
    for (let i = 0; i < 200 && !lease; i++) { lease = await mine(); if (!lease) await sleep(5); }
    await lose(lease!.id, t0);
    erin = { ...erin, tokenId: tokens[1]! };
    const again = (await mine())!;
    assert.equal(again.id, lease!.id, "a lost lease is not a failed call: the call goes back to the queue");
    await lose(again.id, t0 + 1000);
    assert.equal((await db.get(db.k.selectFrom("remote_lease_drops").select((eb) => eb.fn.countAll<number>().as("n")).where("login", "=", "erin")))!.n, 1, "the same call lost again counts once");
    erin = { ...erin, tokenId: tokens[2]! };
    assert.equal(await ask, null, "the call used its attempts");
    const take = async () => { for (let i = 0; i < 200; i++) { const l = await mine(); if (l) return l; await sleep(5); } throw new Error("no lease"); };
    // A second call, lost twice: one more drop.
    const ask2 = askClaude("review.opus", "opus", SAMPLE_OPUS_PRS, db);
    const second = await take();
    await lose(second.id, t0 + 1500);
    assert.equal((await claimCall(db, erin, claim(), canWrite, t0 + 1600)).lease?.id, second.id, "two drops: no cool-off yet");
    await lose(second.id, t0 + 1700);
    assert.equal(await ask2, null);
    erin = { ...erin, tokenId: tokens[1]! };
    const ask3 = askClaude("review.opus", "opus", SAMPLE_OPUS_PRS, db);
    await lose((await take()).id, t0 + 2000);
    const reply = await claimCall(db, erin, claim(), canWrite, t0 + 3000);
    assert.equal(reply.lease, null);
    assert.equal(reply.lease === null && reply.until, t0 + 2000 + 6 * 3600_000);
    assert.match(reply.lease === null ? reply.reason ?? "" : "", /3 times within 2 hours/);
    assert.equal((await claimCall(db, { ...erin, tokenId: tokens[0]! }, claim(), canWrite, t0 + 3000)).lease, null, "every token of the login");
    assert.equal(await clearCooloff(db, "erin", "test"), true);
    assert.ok((await claimCall(db, erin, claim(), canWrite, t0 + 3000)).lease, "the call is back in the queue for the cleared helper");
    await withdrawCall(db, (await db.get(db.k.selectFrom("remote_calls").select("id").where("state", "in", ["queued", "leased"]).orderBy("id", "desc").limit(1)))!.id, "test over");
    await ask3;
  });

  test("a claim whose declared effort is below a tier's minimum gets nothing, and the refusal names the families and the floor", async () => {
    await setAcceptedModels(db, undefined, "test", { top: "high" });
    const claimant = callers.bob!;
    await assert.rejects(claimCall(db, claimant, claim(), canWrite), /opus\/astra at effort high or higher/);
    await assert.rejects(claimCall(db, claimant, { ...claim(), efforts: { top: "medium" } }, canWrite), /at effort high or higher/);
    assert.equal((await claimCall(db, claimant, { ...claim(), efforts: { top: "xhigh" } }, canWrite)).lease, null, "meets the floor; nothing is queued");
    await setAcceptedModels(db, undefined, "test", { top: "max" });
    const codex: Claim = { ...claim(), harness: "codex", tiers: { top: "gpt-6-astra" }, efforts: { top: "xhigh" } };
    assert.equal((await claimCall(db, claimant, codex, canWrite)).lease, null, "Codex's top level, xhigh, meets a max floor");
    await assert.rejects(claimCall(db, claimant, { ...claim(), efforts: { top: "xhigh" } }, canWrite), /effort max/);
    await setAcceptedModels(db, undefined, "test", { top: null });
    assert.equal((await claimCall(db, claimant, claim(), canWrite)).lease, null, "no floor: any helper");
  });

  test("the session tools count what the login may claim, and show a lease only to its holder", async () => {
    const env = (login: string, run = true): McpEnv => ({
      db, baseUrl: null, limiter: new RateLimiter(10, 60_000), canAnswer: false, expensive: () => 0,
      user: async () => ({ login, role: "maintainer" }) as unknown as User,
      ...(run && { run: { canWrite } }),
    });
    const call = async (e: McpEnv, name: string, args: unknown = {}) => {
      const r = (await handleMcp(e, { jsonrpc: "2.0", id: 1, method: "tools/call", params: { name, arguments: args } }))!.result as { content: { text: string }[]; isError?: boolean };
      return { text: r.content[0]!.text, isError: r.isError === true };
    };
    const names = async (e: McpEnv) => ((await handleMcp(e, { jsonrpc: "2.0", id: 1, method: "tools/list" }))!.result as { tools: { name: string }[] }).tools.map((t) => t.name);
    assert.ok((await names(env("bob"))).includes("next_task"));
    assert.ok(!(await names(env("bob", false))).includes("next_task"), "not offered without a Task Runner that reports in");
    const ask = askClaude("review.opus", "opus", SAMPLE_OPUS_PRS, db);
    while (!await db.get(db.k.selectFrom("remote_calls").select((eb) => eb.lit(1).as("one")).where("state", "=", "queued"))) await sleep(5);
    await idle("bob");
    assert.match((await call(env("alice"), "next_task")).text, /^No call is queued/, "the PR's author, while bob's helper is idle");
    assert.match((await call(env("bob"), "next_task", { tiers: ["mid"] })).text, /^No call is queued/, "wrong tier");
    assert.match((await call(env("bob"), "next_task", { models: { top: "gpt-5" } })).text, /^No call is queued/, "a model outside the accepted families");
    assert.match((await call(env("bob"), "next_task", { models: { top: "claude-opus-4-5" } })).text, /^1 call is queued/);
    const queued = await call(env("bob"), "next_task");
    assert.match(queued.text, /^1 call is queued/);
    assert.match(queued.text, /autocraft run --detach/);
    assert.ok(await db.get(db.k.selectFrom("remote_calls").select((eb) => eb.lit(1).as("one")).where("state", "=", "queued")), "next_task leases nothing");
    const lease = (await leaseFor("bob"))!;
    assert.match((await call(env("bob"), "task_status", { lease: String(lease.id) })).text, /: leased$/);
    assert.equal((await call(env("alice"), "task_status", { lease: String(lease.id) })).isError, true, "another login's lease");
    assert.match((await call(env("bob"), "my_work")).text, /review\.opus/);
    await done(lease.secret, "ok");
    await ask;
  });

  test("a user sets their own usage reserve, the claim reply carries it, and only a helper that reported ready is room", async () => {
    assert.deepEqual((await userSettings.settings(db, req("bob", "maintainer"))).reserve, { fiveHour: 20, weekly: 20 }, "the default");
    await userSettings.setSettings(db, req("bob", "maintainer", { reserve: { fiveHour: 35, weekly: 10 } }));
    assert.deepEqual((await userSettings.settings(db, req("bob", "maintainer"))).reserve, { fiveHour: 35, weekly: 10 });
    assert.deepEqual((await userSettings.settings(db, req("carol", "maintainer"))).reserve, { fiveHour: 20, weekly: 20 }, "another user's is their own");
    await assert.rejects(userSettings.setSettings(db, req("bob", "maintainer", { reserve: { fiveHour: 95, weekly: 10 } })), /0 to 90/);
    await assert.rejects(userSettings.setSettings(db, req("eve", "viewer", { reserve: { fiveHour: 30, weekly: 30 } })), /viewers/);
    assert.ok(await db.prepare("SELECT 1 FROM audit WHERE action = 'usage-reserve' AND login = 'bob'").get());
    assert.deepEqual(await claimCall(db, callers.bob!, claim(), canWrite), { lease: null, retryAfter: CLAIM_RETRY_S, reserve: { fiveHour: 35, weekly: 10 } });
    assert.equal(await remoteRoom(db, 0), 0, "an idle helper that never reported ready");
    const status = { slots: 1, busy: 0, harness: "claude", models: { top: "opus" }, reserve: { fiveHour: 35, weekly: 10 }, usage: { fiveHour: { usedPercent: 70, resetsAt: null }, weekly: null, at: new Date().toISOString() } };
    await storeStatus(db, callers.bob!.tokenId, "bob", parseStatus({ ...status, readiness: "ready" }));
    assert.equal(await remoteRoom(db, 0), 1);
    await storeStatus(db, callers.bob!.tokenId, "bob", parseStatus({ ...status, slots: 3, readiness: "ready" }));
    assert.equal(await remoteRoom(db, 0), 2, "--parallel 2 and the fast lane: the fast lane is no room");
    await storeStatus(db, callers.bob!.tokenId, "bob", parseStatus({ ...status, readiness: "quota-held", until: "2026-11-20T01:30:00.000Z", reason: "5-hour window 70% used" }));
    assert.equal(await remoteRoom(db, 0), 0, "held by its plan usage");
    const [h] = await listHelpers(db, "bob");
    assert.equal(h?.readiness, "quota-held");
    assert.equal(h?.fresh, true);
    assert.equal(h?.usage?.fiveHour?.usedPercent, 70);
    await db.run(db.k.insertInto("remote_cooloffs").values({ login: "bob", until: Date.now() + 3600_000, reason: "test" })
      .onConflict((oc) => oc.column("login").doUpdateSet((eb) => ({ ...excluded(["until", "reason"])(eb), failures: 0, failures_since: null }))));
    assert.equal((await listHelpers(db, "bob"))[0]?.readiness, "cooled-off", "the server's cool-off shows before the helper reports it");
    await db.run(db.k.deleteFrom("remote_cooloffs").where("login", "=", "bob"));
  });

  test("a user's repo picks limit their claims and next_task; the helper's own list narrows them, never widens them", async () => {
    await db.run(db.k.updateTable("mcp_tokens").set({ idle_claim_at: null }));
    await db.prepare("INSERT INTO tracked_repos (name, pipeline) VALUES ('photocraft', 1), ('fontcraft', 1), ('gridcraft', 0) ON CONFLICT (name) DO UPDATE SET pipeline = excluded.pipeline").run();
    const pick = (repos: string[]) => userSettings.setSettings(db, req("bob", "maintainer", { repos }));
    const got = await pick(["fontcraft", "gridcraft"]);
    assert.deepEqual([got.repos, got.repoChoices], [["fontcraft", "gridcraft"], ["fontcraft", "photocraft"]], "picks are kept; choices are the pipeline-on repos");
    await assert.rejects(pick(["nosuchrepo"]), /not tracked/);
    const ask = askClaude("review.opus", "opus", SAMPLE_OPUS_PRS, db);
    while (!await db.get(db.k.selectFrom("remote_calls").select((eb) => eb.lit(1).as("one")).where("state", "=", "queued"))) await sleep(5);
    assert.equal((await claimCall(db, callers.bob!, claim(), canWrite)).lease, null, "photocraft is not picked");
    assert.equal((await db.get(db.k.selectFrom("mcp_tokens").select("idle_repos").where("id", "=", callers.bob!.tokenId)))!.idle_repos, '["fontcraft","gridcraft"]', "the idle helper records what it takes");
    assert.equal(await claimableCalls(db, callers.bob!, {}, canWrite), 0);
    await pick(["photocraft"]);
    assert.equal(await claimableCalls(db, callers.bob!, {}, canWrite), 1);
    assert.equal((await claimCall(db, callers.bob!, { ...claim(), repos: ["fontcraft"] }, canWrite)).lease, null, "the helper's list narrows");
    assert.equal((await db.get(db.k.selectFrom("mcp_tokens").select("idle_repos").where("id", "=", callers.bob!.tokenId)))!.idle_repos, "[]");
    const lease = (await claimCall(db, callers.bob!, { ...claim(), repos: ["photocraft", "fontcraft"] }, canWrite)).lease!;
    assert.match(lease.title, /photocraft#1287/);
    await done(lease.secret, "picked");
    assert.equal((await ask)?.text, "picked");
    await pick([]);
  });

  const stateOf = async (id: number) => (await db.get(db.k.selectFrom("remote_calls").select("state").where("id", "=", id)))!.state;

  test("a repeated claim gets the same lease, and a dashboard outage ends no lease", async () => {
    await db.run(db.k.updateTable("mcp_tokens").set({ idle_claim_at: null }));
    const ask = askClaude("review.opus", "opus", SAMPLE_OPUS_PRS, db);
    while (!await db.get(db.k.selectFrom("remote_calls").select((eb) => eb.lit(1).as("one")).where("state", "=", "queued"))) await sleep(5);
    const c = claim();
    const [a, b] = await Promise.all([claimCall(db, callers.bob!, c, canWrite), claimCall(db, callers.bob!, c, canWrite)]);
    assert.ok(a.lease);
    assert.deepEqual(b, a, "a repeat while the first is still being answered");
    assert.deepEqual(await claimCall(db, callers.bob!, c, canWrite), a, "a repeat after the reply was lost");
    const id = a.lease!.id, now = Date.now();
    // The dashboard's last stamp is 5 minutes old; the lease's last heartbeat 3 minutes.
    await db.run(db.k.insertInto("remote_liveness").values({ id: 1, alive_at: now - 5 * 60_000, since: now - 3600_000 }).onConflict((oc) => oc.column("id").doUpdateSet(excluded(["alive_at", "since"]))));
    await db.run(db.k.updateTable("remote_calls").set({ heartbeat_at: now - 3 * 60_000 }).where("id", "=", id));
    await expireLeases(db, now);
    assert.equal(await stateOf(id), "leased", "nothing expires while the dashboard is down");
    await stampAlive(db, now);
    assert.equal((await db.get(db.k.selectFrom("remote_calls").select("heartbeat_at").where("id", "=", id)))!.heartbeat_at, now, "the dashboard came back: the heartbeat moved forward by the gap, up to now");
    await db.run(db.k.deleteFrom("remote_liveness"));
    await done(a.lease!.secret, "after the outage");
    assert.equal((await ask)?.text, "after the outage");
  });

  test("a call bound to a maintainer waits only firstWaitMs for their runner; its fix turn on the session waits past that and keeps the first reply", async () => {
    const restore = setWorkspace(async () => []);
    const checkout = join(SCRATCH, "checkouts", "photocraft", "c".repeat(40));
    mkdirSync(checkout, { recursive: true });
    const lint: Lint = (text, stage) => (stage === "first" && text.includes("leaky") ? [{ field: "public.points[0]", message: "mentions the reference folder", text: "a leaky point" }] : []);
    const reply = (secret: string, text: string) => submitCall(db, secret, { ok: true, text, model: "claude-opus-4-5", usage: { inputTokens: 10, outputTokens: 5 }, sessionId: "s1", durationMs: 10 });
    const wait = 100;
    const opts = { checkout, lint, itemTools: null, forLogin: "alice", firstWaitMs: wait };
    try {
      await assert.rejects(askClaude("review.opus", "opus", SAMPLE_OPUS_PRS, db, opts), WaitLimitError, "no runner took the first call in time");
      const ask = askClaude("review.opus", "opus", SAMPLE_OPUS_PRS, db, opts);
      const first = (await leaseFor("alice"))!;
      await reply(first.secret, JSON.stringify({ verdict: "take", summary: "s", why: "w", public: { summary: "ok", points: ["a leaky point", "second"] } }));
      // The fix turn on the session waits longer than the first call could.
      let fix: { id: number; secret: string } | null = null;
      for (let i = 0; i < 100 && !fix; i++) {
        await sleep(wait / 2);
        const queued = await db.get(db.k.selectFrom("remote_calls").select(["since"]).where("parent_id", "=", first.id).where("state", "=", "queued"));
        if (queued && Date.now() - Number(queued.since) > wait * 2) fix = (await claimCall(db, callers.alice!, { ...claim(), sessions: [first.id] }, canWrite)).lease;
      }
      assert.ok(fix, "the fix turn was still queued past firstWaitMs");
      await reply(fix.secret, JSON.stringify({ "public.points[0]": "clean point" }));
      assert.deepEqual(JSON.parse((await ask)!.text).public.points, ["clean point", "second"]);
    } finally {
      restore();
    }
  });

  test("a failed reply sends the call back to the queue with its next attempt, for another user's helper; the old secret answers 410", async () => {
    const ask = askClaude("review.opus", "opus", SAMPLE_OPUS_PRS, db);
    const first = (await leaseFor("bob"))!;
    assert.equal(await heartbeat(db, first.secret, { phase: "running" }), true);
    assert.deepEqual(await submitCall(db, first.secret, { ok: false, error: "harness crashed", durationMs: 5 }), { accepted: true });
    assert.deepEqual({ ...await db.get(db.k.selectFrom("remote_calls").select(["state", "attempt", "released_by", "started_at"]).where("id", "=", first.id)) }, { state: "queued", attempt: 2, released_by: "bob", started_at: null });
    assert.deepEqual(await leaseState(db, first.secret), { id: first.id, live: false });
    assert.equal((await claimCall(db, callers.bob!, claim(), canWrite)).lease, null, "not offered to the user whose helper failed it");
    assert.equal((await db.get(db.k.selectFrom("remote_cooloffs").select("failures").where("login", "=", "bob")))!.failures, 1, "counts toward bob's cool-off");
    await db.run(db.k.updateTable("mcp_tokens").set({ idle_claim_at: null }));
    const second = (await leaseFor("alice"))!;
    assert.equal(second.id, first.id);
    await done(second.secret, "second try");
    assert.equal((await ask)?.text, "second try");
  });

  test("gating calls go first, the fast lane takes only gating and small calls, and Max calls at once stops new leases but lets a session finish", async () => {
    await db.run(db.k.updateTable("remote_calls").set({ state: "failed" }).where("state", "in", ["queued", "leased"]));
    const q = async (promptId: string, tier: "top" | "small", opts: { ageMs?: number; parentId?: number } = {}) => {
      const id = await enqueueCall(db, { promptId, promptVersion: "1", tier, effort: "medium", askedModel: tier === "top" ? "opus" : "haiku", items: [{ repo: "photocraft", number: 1287 }], level: 0, maxAttempts: 1, ...(opts.parentId !== undefined && { parentId: opts.parentId }),
        bundle: { promptId, promptVersion: "1", tier, effort: "medium", timeoutSeconds: 60, maxTurns: null, stdin: "x", system: null, mounts: [], mcpServers: [], keepSession: true, hints: {} } });
      if (opts.ageMs) await db.run(db.k.updateTable("remote_calls").set((eb) => ({ since: eb("since", "-", opts.ageMs!) })).where("id", "=", id));
      return id;
    };
    const both = (extra: Partial<Claim> = {}): Claim => ({ ...claim(), tiers: { top: "opus", small: "haiku" }, ...extra });
    const opus = await q("review.opus", "top", { ageMs: 10 * 60_000 });
    const lint = await q("lint.reference.haiku", "small");
    const small = await q("classify.haiku", "small");
    assert.equal((await claimCall(db, callers.bob!, both(), canWrite)).lease?.id, lint, "the lint check before the older Opus review");
    assert.equal((await claimCall(db, callers.bob!, both({ fastLane: true }), canWrite)).lease?.id, small, "the fast lane takes the small call, not the review");
    assert.deepEqual(await claimCall(db, callers.bob!, both({ fastLane: true }), canWrite), { lease: null, retryAfter: CLAIM_RETRY_S, reserve: { fiveHour: 35, weekly: 10 } }, "nothing else fits the fast lane");

    await assert.rejects(userSettings.setSettings(db, req("bob", "maintainer", { maxCalls: 0 })), /1 to 64/);
    await userSettings.setSettings(db, req("bob", "maintainer", { maxCalls: 2 }));
    assert.equal((await userSettings.settings(db, req("bob", "maintainer"))).maxCalls, 2);
    assert.ok(await db.prepare("SELECT 1 FROM audit WHERE action = 'max-calls' AND login = 'bob'").get());
    const refused = await claimCall(db, callers.bob!, both(), canWrite);
    assert.equal(refused.lease, null);
    assert.match(("reason" in refused && refused.reason) || "", /limit of calls at once/, "two leases held: the review waits");
    const fix = await q("review.opus", "top", { parentId: lint });
    assert.equal((await claimCall(db, callers.bob!, both({ sessions: [lint] }), canWrite)).lease?.id, fix, "a follow-up of bob's own session, one past the limit");
    await q("review.opus", "top", { parentId: lint });
    assert.equal((await claimCall(db, callers.bob!, both({ sessions: [lint] }), canWrite)).lease, null, "but not two past it");
    await userSettings.setSettings(db, req("bob", "maintainer", { maxCalls: null }));
    assert.equal((await claimCall(db, callers.bob!, both(), canWrite)).lease?.id, opus, "no limit: the review");
    await db.run(db.k.updateTable("remote_calls").set({ state: "failed" }).where("state", "in", ["queued", "leased"]));
  });
});
