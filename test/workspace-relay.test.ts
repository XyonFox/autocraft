// The workspace relay on SQLite: a session's tool calls queued by the dashboard side reach the session of the job waiting
// for the remote call, run side by side, and come back with their images; a call still queued when the remote call ends
// is answered with a refusal.
import assert from "node:assert/strict";
import { mkdirSync, mkdtempSync, rmSync } from "node:fs";
import { join } from "node:path";
import { after, before, describe, test } from "node:test";
import { allocation } from "../src/agents/call.ts";
import { addLogin } from "../src/access.ts";
import { queueWorkspaceCall } from "../src/build/workspace-call.ts";
import { relayWorkspaceCall, type ToolSession } from "../src/build/workspace-relay.ts";
import { mcpReply } from "../src/dash/mcp/index.ts";
import { createToken } from "../src/dash/mcp/tokens.ts";
import { connectDb } from "../src/db/open.ts";
import { awaitCall, claimCall, submitCall, type Caller } from "../src/remote/calls.ts";
import { setInferenceMode } from "../src/remote/mode.ts";
import { PROTOCOL_VERSION, type TaskBundle } from "../src/remote/protocol.ts";
import { SCRATCH } from "./scratch.ts";

mkdirSync(SCRATCH, { recursive: true });
const dir = mkdtempSync(join(SCRATCH, "workspace-relay-"));
const PNG = Buffer.from("not really a png").toString("base64");

describe("workspace relay", async () => {
  const db = await connectDb(join(dir, "t.db"));
  let caller: Caller;
  let claims = 0;

  before(async () => {
    allocation.pollMs = 10;
    await setInferenceMode(db, "user-local", "test");
    await addLogin(db, "cli", "bob", "maintainer", null);
    const now = new Date().toISOString();
    await db.run(db.k.insertInto("repos").values({ name: "photocraft", owner: "storytold", visibility: "public" }));
    await db.run(db.k.insertInto("items").values({ repo: "photocraft", number: 7, kind: "issue", state: "OPEN", title: "t", body: "b", author: "alice", created_at: now, updated_at: now }));
    caller = { tokenId: (await createToken(db, "bob", "helper", { kind: "runner" })).row.id, login: "bob", role: "maintainer" };
  });
  after(async () => {
    await db.close();
    rmSync(dir, { recursive: true, force: true });
  });

  const request = (workspaceId: number) => ({ workspaceId, promptId: "workspace.try", promptVersion: "1", model: "opus", tier: "top" as const, effort: "medium" as const, stdin: "x", timeoutSeconds: 600, maxTurns: 50, items: [{ repo: "photocraft", number: 7 }], servers: ["items"] });

  /** A ready workspace with a leased session call bound to it. */
  const session = async (): Promise<{ ws: number; id: number; secret: string; runId: number | null }> => {
    const now = new Date().toISOString();
    const ws = Number((await db.get(db.k.insertInto("workspaces").values({ repo: "photocraft", base_sha: "a".repeat(40), env_id: "k", state: "ready", state_at: now, created_at: now, updated_at: now }).returning("id")))!.id);
    await queueWorkspaceCall(db, request(ws));
    const r = await claimCall(db, caller, { requestId: `ws-relay-claim-${++claims}`, protocol: PROTOCOL_VERSION, helperVersion: "1.3.6", harness: "claude", harnessVersion: "2", tiers: { top: "opus" } }, async () => true);
    assert.ok(r.lease, JSON.stringify(r));
    const lease = r.lease;
    const runId = (await db.get(db.k.selectFrom("remote_calls").select("run_id").where("id", "=", lease.id)))!.run_id;
    return { ws, id: lease.id, secret: lease.secret, runId };
  };

  test("tool calls reach the job's session side by side and their images come back", { timeout: 20_000 }, async () => {
    const s = await session();
    const b = JSON.parse((await db.get(db.k.selectFrom("remote_calls").select("bundle").where("id", "=", s.id)))!.bundle) as TaskBundle;
    assert.deepEqual(b.mcpServers, ["items", "work"]);
    const allowed = b.hints.claude!.args[b.hints.claude!.args.indexOf("--allowedTools") + 1];
    assert.ok(allowed!.split(",").includes("mcp__work__*"));
    await assert.rejects(queueWorkspaceCall(db, request(s.ws)), /in use by call/, "a workspace serves one live call at a time");

    // Each call waits until both have started: served one after the other, neither would finish.
    let started = 0, both: () => void = () => {};
    const bothStarted = new Promise<void>((r) => { both = r; });
    // Like WorkspaceSession, the fake settles its row itself.
    const fake: ToolSession = {
      call: async (name, args, row) => {
        if (++started === 2) both();
        await bothStarted;
        const result = { text: `${name} ran ${JSON.stringify(args)}`, images: [{ data: PNG, mimeType: "image/png" as const }] };
        await db.run(db.k.updateTable("workspace_calls").set({ state: "done", result: JSON.stringify({ ...result, acct: { ms: 1 } }), finished_at: new Date().toISOString() }).where("id", "=", row));
        return result;
      },
    };
    const waiting = awaitCall(db, s.id, { workspace: () => fake });
    const live = async () => true;
    const [a, c] = await Promise.all([
      relayWorkspaceCall(db, s, "shell", { command: "ls", stdin: "x".repeat(5000) }, { live }),
      relayWorkspaceCall(db, s, "screenshot", {}, { live }),
    ]);
    assert.equal(a.text, `shell ran ${JSON.stringify({ command: "ls", stdin: "x".repeat(5000) })}`, "the arguments reach the session whole");
    assert.deepEqual(c.images, [{ data: PNG, mimeType: "image/png" }]);
    assert.deepEqual(Object.keys(a).sort(), ["images", "text"], "the session's bookkeeping stays in the row");
    const rows = await db.all(db.k.selectFrom("workspace_calls").select(["args", "result"]).where("workspace_id", "=", s.ws).orderBy("id"));
    assert.ok(rows[0]!.args.length < 5000, "the row keeps the arguments cut once claimed");
    assert.ok(rows.every((r) => !r.result!.includes(PNG) && r.result!.includes('"sha256"')), "a delivered image is kept as its hash");

    await submitCall(db, s.secret, { ok: false, error: "done", durationMs: 1 });
    assert.equal((await waiting).state, "failed");

    const mcp = await mcpReply({ name: "t", title: "t", tools: [], call: async () => c }, { jsonrpc: "2.0", id: 1, method: "tools/call", params: { name: "screenshot" } });
    assert.deepEqual((mcp!.result as { content: unknown[] }).content.map((x) => (x as { type: string }).type), ["text", "image"]);
  });

  test("a tool call still queued when the remote call ends is refused", { timeout: 20_000 }, async () => {
    const s = await session();
    await submitCall(db, s.secret, { ok: false, error: "done", durationMs: 1 });
    // The dashboard request does not notice the end itself here, so only the job's end can answer it.
    const relayed = relayWorkspaceCall(db, s, "shell", { command: "ls" }, { live: async () => true });
    while (!await db.get(db.k.selectFrom("workspace_calls").select("id").where("workspace_id", "=", s.ws))) await new Promise((r) => setTimeout(r, 5));
    const fake: ToolSession = { call: async () => { throw new Error("nothing runs after the call ended"); } };
    assert.equal((await awaitCall(db, s.id, { workspace: () => fake })).state, "failed");
    const r = await relayed;
    assert.equal(r.isError, true);
    assert.match(r.text, /refused: the model call is over/);
  });
});
