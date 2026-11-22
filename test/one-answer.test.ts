// One answer per question: a running card locks its decision, a writing card is refused against an answered, unapplied decision,
// a disputed answer is not read by the pipeline, and undo ends once a GitHub write exists for the answer.
import assert from "node:assert/strict";
import { mkdirSync, mkdtempSync } from "node:fs";
import { join } from "node:path";
import { describe, test } from "node:test";
import { SCRATCH } from "./scratch.ts";
import { clearDispute, openDispute } from "../src/answers/hold.ts";
import { revertAnswer } from "../src/preapproval/index.ts";
import { sweepStaleAnswers } from "../src/answers/sweep.ts";
import { decisionStamp } from "../src/conversations/cards.ts";
import { doCard } from "../src/conversations/act.ts";
import { actionable, ask, getDecision } from "../src/decisions.ts";
import { applyAnswer, canUndo, keepAnswer, retry, undo } from "../src/dash/api/decisions.ts";
import type { Db } from "../src/db/index.ts";
import { connectDb } from "../src/db/open.ts";

mkdirSync(SCRATCH, { recursive: true });
const T0 = "2026-01-01T00:00:00Z";
const OPTIONS = [{ key: "take", label: "Wanted" }, { key: "close", label: "Out of scope" }];
const querier = { query: async () => { throw new Error("GitHub was called"); }, spent: 0, remaining: null } as never;

async function fresh(): Promise<Db> {
  const db = await connectDb(join(mkdtempSync(join(SCRATCH, "one-answer-")), "t.db"));
  await db.run(db.k.insertInto("repos").values({ name: "app", visibility: "public" }));
  await db.run(db.k.insertInto("items").values({ repo: "app", number: 1, kind: "issue", state: "OPEN", title: "An issue", body: "Body", author: "someone", created_at: T0, updated_at: T0, labels: "[]" }));
  await db.run(db.k.insertInto("access").values([{ login: "op", role: "operator" }, { login: "op2", role: "operator" }]));
  await ask(db, [{ id: "app#1:scope", kind: "scope", repo: "app", number: 1, question: "Scope?", options: OPTIONS }]);
  return db;
}

const card = async (db: Db, params: unknown): Promise<number> =>
  Number((await db.get(db.k.insertInto("conversation_cards").values({ turn_id: 0, repo: "app", number: 1, kind: "resolution", params: JSON.stringify(params), text: null, description: "x" }).returning("id")))!.id);

describe("one answer per question", () => {
  test("a lock whose card is gone or stale does not block answers", async () => {
    const db = await fresh();
    await db.run(db.k.updateTable("decisions").set({ exec_state: "settling", exec_reason: "999:op" }).where("id", "=", "app#1:scope"));
    await applyAnswer(db, (await getDecision(db, "app#1:scope"))!, "op2", { answer: "close" }, "dashboard");
    const d = (await getDecision(db, "app#1:scope"))!;
    assert.deepEqual({ answer: d.answer, by: d.answered_by, state: d.exec_state }, { answer: "close", by: "op2", state: null });
    const db2 = await fresh();
    const turn = 1;
    await db2.run(db2.k.insertInto("conversation_cards").values({ id: 7, turn_id: turn, repo: "app", number: 1, kind: "resolution", params: "{}", description: "x", state: "running", acted_at: "2020-01-01T00:00:00Z" }));
    await db2.run(db2.k.updateTable("decisions").set({ exec_state: "settling", exec_reason: "7:op" }).where("id", "=", "app#1:scope"));
    await applyAnswer(db2, (await getDecision(db2, "app#1:scope"))!, "op2", { answer: "close" }, "dashboard");
    assert.equal((await getDecision(db2, "app#1:scope"))!.answer, "close");
  });

  test("taking back a hand-over answer clears the exec state and the dispute", async () => {
    const db = await fresh();
    await ask(db, [{ id: "app#1:merge@abc", kind: "merge", repo: "app", number: 1, question: "Merge?", options: [{ key: "merge", label: "Merge" }, { key: "hold", label: "Hold" }], facts: ["head commit abc"] }]);
    await db.run(db.k.updateTable("decisions").set({ answer: "merge", answered_by: "op", answered_at: T0, exec_state: "disputed", dispute_by: "op2", dispute_reason: "x" }).where("id", "=", "app#1:merge@abc"));
    await db.run(db.k.insertInto("audit").values({ at: T0, login: "op", action: "answer", target: "app#1:merge@abc", details: '{"source":"preapproval"}' }));
    assert.equal(await revertAnswer(db, { repo: "app", number: 1, head: "abc", by: "op" } as never, "op"), true);
    const d = (await getDecision(db, "app#1:merge@abc"))!;
    assert.deepEqual({ answer: d.answer, state: d.exec_state, by: d.dispute_by }, { answer: null, state: null, by: null });
  });

  test("a card that settles the decision locks it while it runs, and a failed card releases it", async () => {
    const db = await fresh();
    const d = (await getDecision(db, "app#1:scope"))!;
    const settle = { kind: "answer", decision: d.id, option: "take", stamp: await decisionStamp(db, d) };
    let release!: () => void;
    const gate = new Promise<boolean>((r) => { release = () => r(true); });
    let claimed!: () => void;
    const claim = new Promise<void>((r) => { claimed = r; });
    const running = doCard(db, await card(db, { kind: "resolution", steps: [], settle }), "op", { querier, canWrite: () => gate, claimed });
    await claim;
    await assert.rejects(async () => await applyAnswer(db, (await getDecision(db, d.id))!, "op2", { answer: "close" }, "dashboard"), /op is applying their answer/);
    const second = doCard(db, await card(db, { kind: "resolution", steps: [], settle }), "op2", { querier });
    await assert.rejects(second, /op is applying their answer/);
    release();
    assert.equal((await running).card.state, "done");
    assert.equal((await getDecision(db, d.id))!.answer, "take");

    const db2 = await fresh();
    const d2 = (await getDecision(db2, "app#1:scope"))!;
    const failing = await doCard(db2, await card(db2, { kind: "resolution", steps: [], settle: { ...settle, stamp: await decisionStamp(db2, d2) } }), "op", { querier, canWrite: async () => false });
    assert.equal(failing.card.state, "failed");
    assert.equal((await getDecision(db2, d2.id))!.exec_state, null, "the lock is released");
    await applyAnswer(db2, (await getDecision(db2, d2.id))!, "op2", { answer: "close" }, "dashboard");
  });

  test("a card that closes the item is refused while the item has an answered, unapplied decision", async () => {
    const db = await fresh();
    await applyAnswer(db, (await getDecision(db, "app#1:scope"))!, "op", { answer: "close" }, "dashboard");
    const id = await card(db, { kind: "close", reason: "not_planned", pr: false });
    await assert.rejects(doCard(db, id, "op2", { querier }), /op already answered/);
    assert.equal((await db.get(db.k.selectFrom("conversation_cards").select("state").where("id", "=", id)))!.state, "proposed");
    await assert.rejects(doCard(db, id, "op", { querier }), /Undo your answer first/);
  });

  test("an answer without a write, or one nobody settles any more, does not stop a writing card", async () => {
    const db = await fresh();
    await applyAnswer(db, (await getDecision(db, "app#1:scope"))!, "op", { answer: "take" }, "dashboard");
    // The card gets past the refusal and fails at GitHub (the querier throws), not with a refusal.
    const r0 = await doCard(db, await card(db, { kind: "close", reason: "not_planned", pr: false }), "op2", { querier });
    assert.doesNotMatch(r0.card.error ?? "", /answered/);
    const db2 = await fresh();
    await db2.run(db2.k.insertInto("access").values({ login: "mt", role: "maintainer" }));
    await applyAnswer(db2, (await getDecision(db2, "app#1:scope"))!, "mt", { answer: "close" }, "dashboard");
    const r = await doCard(db2, await card(db2, { kind: "close", reason: "not_planned", pr: false }), "op2", { querier });
    assert.doesNotMatch(r.card.error ?? "", /answered/);
  });

  test("a disputed answer is not actionable until kept, and undo ends once a comment exists", async () => {
    const db = await fresh();
    await applyAnswer(db, (await getDecision(db, "app#1:scope"))!, "op", { answer: "close" }, "dashboard");
    assert.equal((await actionable(db)).length, 1);
    assert.equal(await openDispute(db, "app#1:scope", "op", "mine", T0), false, "the answerer does not dispute their own answer");
    assert.equal(await openDispute(db, "app#1:scope", "op2", "wrong item", T0), true);
    assert.equal((await actionable(db)).length, 0);
    assert.equal(await canUndo(db, (await getDecision(db, "app#1:scope"))!), true);
    await clearDispute(db, "app#1:scope");
    assert.equal((await actionable(db)).length, 1);
    await db.run(db.k.insertInto("actions").values({ decision_id: "app#1:scope", repo: "app", number: 1, kind: "comment-close", state: "failed", comment_url: "https://x.test/c", created_at: T0 }));
    assert.equal(await canUndo(db, (await getDecision(db, "app#1:scope"))!), false);
  });

  test("a merge answer whose PR merged at the answered head is closed out without a note", async () => {
    const db = await fresh();
    await ask(db, [{ id: "app#1:merge@abc", kind: "merge", repo: "app", number: 1, question: "Merge?", options: [{ key: "merge", label: "Merge" }, { key: "hold", label: "Hold" }], facts: ["head commit abc"] }]);
    await db.run(db.k.updateTable("decisions").set({ answer: "merge", answered_by: "op", answered_at: T0 }).where("id", "=", "app#1:merge@abc"));
    await db.run(db.k.insertInto("merges").values({ repo: "app", number: 1, head_oid: "abc", run_id: "r", at: T0, path: "operator", outcome: "merged" }));
    assert.equal(await sweepStaleAnswers(db, T0, "app"), 1);
    const d = (await getDecision(db, "app#1:merge@abc"))!;
    assert.deepEqual({ applied: !!d.applied_at, note: d.answer_note }, { applied: true, note: null });
  });

  test("an answer for an item that closed meanwhile is marked applied with a note", async () => {
    const db = await fresh();
    await applyAnswer(db, (await getDecision(db, "app#1:scope"))!, "op", { answer: "close" }, "dashboard");
    await db.run(db.k.updateTable("items").set({ state: "CLOSED" }).where("number", "=", 1));
    assert.equal(await sweepStaleAnswers(db, T0, "app"), 1);
    const d = (await getDecision(db, "app#1:scope"))!;
    assert.deepEqual({ applied: !!d.applied_at, note: d.answer_note }, { applied: true, note: "not applied: the item is closed" });
  });

  test("a maintainer who disputed another settler's answer cannot undo, keep or retry it", async () => {
    const db = await fresh();
    await db.run(db.k.insertInto("tracked_repos").values({ name: "app", added_by: "op", added_at: T0, pipeline: 1 }));
    await db.run(db.k.insertInto("access").values([{ login: "a", role: "maintainer" }, { login: "b", role: "maintainer" }]));
    await db.run(db.k.insertInto("settings").values([{ key: "maintainer_writes", value: "1" }, { key: "github_identity", value: "app" }]));
    await db.run(db.k.updateTable("decisions").set({ answer: "close", answered_by: "a", answered_at: T0, exec_state: "disputed", dispute_by: "b", dispute_reason: "no" }).where("id", "=", "app#1:scope"));
    const as = (login: string) => ({
      method: "POST", path: "", params: { id: "app#1:scope" }, query: new URLSearchParams(), form: new URLSearchParams(), csrf: "", nonce: "",
      user: { login, role: "maintainer", canWrite: async () => true, canRead: async () => true },
    }) as never;
    for (const f of [undo, keepAnswer, retry]) await assert.rejects(f(db, as("b")), { status: 403 });
    assert.equal((await getDecision(db, "app#1:scope"))!.answer, "close");
  });
});
