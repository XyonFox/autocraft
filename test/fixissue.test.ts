import assert from "node:assert/strict";
import { mkdirSync, mkdtempSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { describe, test } from "node:test";
import { spawnSync } from "node:child_process";
import { getTask } from "../src/build/tasks.ts";
import { exec, type Exec, type ExecResult } from "../src/fixup/git.ts";
import { setFixIssueApproval } from "../src/fixissue/approval.ts";
import { attemptOf, runFixIssue, type CollectDeps } from "../src/fixissue/index.ts";
import { loadItemFact } from "../src/core/issue-facts.ts";
import { parseTriage, validateFixPatch } from "../src/fixissue/parse.ts";
import type { Querier } from "../src/mirror.ts";
import { SCRATCH } from "./scratch.ts";
import { validating } from "./gql.ts";
import type { Db } from "../src/db/index.ts";
import { connectDb } from "../src/db/open.ts";
import { like } from "../src/db/kx.ts";

mkdirSync(SCRATCH, { recursive: true });
const dir = mkdtempSync(join(SCRATCH, "fixissue-test-"));
process.env.AUTOCRAFT_DIFF_CACHE = join(dir, "diffs");
const T0 = "2026-01-01T00:00:00Z";
const LOGIN = "pipeline-bot";
const LIB = "crates/app/src/lib.rs";
const TEST = "crates/app/tests/autocraft_regression_5.rs";

function git(cwd: string, ...args: string[]): string {
  const r = spawnSync("git", ["-c", "user.name=t", "-c", "user.email=t@t", ...args], { cwd, encoding: "utf8" });
  if (r.status !== 0) throw new Error(`git ${args.join(" ")}: ${r.stderr}`);
  return r.stdout.trim();
}

const FIX = `diff --git a/${LIB} b/${LIB}
--- a/${LIB}
+++ b/${LIB}
@@ -1,3 +1,3 @@
 pub fn first(v: &[u8]) -> u8 {
-    v[0]
+    v.first().copied().unwrap_or(0)
 }
`;
const NEWTEST = `diff --git a/${TEST} b/${TEST}
new file mode 100644
index 0000000..e69de29
--- /dev/null
+++ b/${TEST}
@@ -0,0 +1,4 @@
+#[test]
+fn empty_slice_does_not_panic() {
+    assert_eq!(app::first(&[]), 0);
+}
`;
const GOOD = FIX + NEWTEST;
const TRIAGE_OK = (files = [LIB]) => JSON.stringify([{ ref: "app#5", fixable: true, reason: "panics on an empty slice", crate: "app", files }]);

type Opts = { existingPr?: string; approved?: boolean; label?: string; flags?: string[]; needsDecision?: number; score?: number; level?: string };

async function fresh(o: Opts = {}): Promise<Db> {
  const db = await connectDb(join(mkdtempSync(join(dir, "db-")), "t.db"));
  await db.run(db.k.insertInto("repos").values({ name: "app", visibility: "public", default_branch: "main" }));
  await db.run(db.k.insertInto("tracked_repos").values({ name: "app", added_by: "op", added_at: T0, pipeline: 1 }));
  await issue(db, 5, "Panic when the slice is empty", "first() indexes element 0.", o);
  if (o.approved !== false) await setFixIssueApproval(db, "op", "app", true);
  return db;
}

async function issue(db: Db, n: number, title: string, body: string, o: Opts = {}): Promise<void> {
  await db.run(db.k.insertInto("items").values({ repo: "app", number: n, kind: "issue", state: "OPEN", title, body, author: "someone", created_at: T0, updated_at: T0 }));
  await db.run(db.k.insertInto("classifications").values({ repo: "app", number: n, classifier: "jev", run_id: "r1", at: T0, type_label: o.label ?? "bugfix", flags: JSON.stringify(o.flags ?? []),
    probs: JSON.stringify({ top: 0.97, ...(o.flags?.includes("needs decision") ? { needs_decision: o.needsDecision ?? 0.9 } : {}) }) }));
  if (o.level !== undefined) await db.run(db.k.insertInto("item_priority").values({ repo: "app", number: n, text_hash: "h", impact: o.level, score: 0, model: "jev", at: T0 }));
  if (o.score !== undefined) await db.run(db.k.insertInto("item_rank").values({ repo: "app", number: n, score: o.score, breakdown: "{}", computed_at: T0 }));
}

describe("candidates", () => {







});

describe("approval", () => {
});

describe("patch validation", () => {
  const ok = (text: string) => validateFixPatch(text, [LIB], TEST, "crates/app", "autocraft_regression_5");
  const reason = (text: string) => (ok(text) as { reason: string }).reason;

  test("accepts the fix and the test file, also inside a fence, and builds a test-only patch", () => {
    const r = ok("```diff\n" + GOOD + "```");
    assert.ok(r.ok);
    if (r.ok) {
      assert.deepEqual(r.files, [LIB]);
      assert.equal(r.testPatch.split("\n")[0], `diff --git a/${TEST} b/${TEST}`);
      assert.ok(r.patch.startsWith(`diff --git a/${LIB}`) && r.patch.endsWith(r.testPatch));
      assert.ok(!r.testPatch.includes("index "));
    }
  });

  test("refuses a path outside the list, an extra file, a missing test and a missing fix", () => {
    assert.match(reason(FIX.replaceAll(LIB, "crates/app/src/other.rs") + NEWTEST), /not a file named/);
    const extra = NEWTEST.replaceAll(TEST, "crates/app/tests/other.rs");
    assert.match(reason(GOOD + extra), /not a file named/);
    assert.match(reason(FIX), /does not add/);
    assert.match(reason(NEWTEST), /no source file/);
    assert.match(reason(FIX + NEWTEST.replace("new file mode 100644", "new file mode 100755")), /test file/);
  });

  test("refuses more than 300 changed lines", () => {
    const body = Array.from({ length: 151 }, (_, i) => `-o${i}\n+n${i}\n`).join("");
    const big = `diff --git a/${LIB} b/${LIB}\n--- a/${LIB}\n+++ b/${LIB}\n@@ -1,151 +1,151 @@\n${body}`;
    assert.match(reason(big + NEWTEST), /changed lines/);
  });

  test("refuses text that is not a diff", () => {
    assert.match(reason("I fixed it."), /not a unified diff/);
  });

  test("triage must name a workspace crate and listed editable Rust files", () => {
    const crates = [{ name: "app", dir: "crates/app" }];
    const paths = [LIB, "build.rs"];
    assert.ok(parseTriage(TRIAGE_OK(), "app#5", crates, paths).ok);
    assert.equal(parseTriage(TRIAGE_OK(["build.rs"]), "app#5", crates, paths).ok, false);
    assert.equal(parseTriage(TRIAGE_OK(["crates/app/src/x.rs"]), "app#5", crates, paths).ok, false);
    assert.equal(parseTriage(TRIAGE_OK().replace('"crate":"app"', '"crate":"nope"'), "app#5", crates, paths).ok, false);
  });
});

describe("texts", () => {

});

// ---- the whole step against real git, with GitHub and the models faked ----------

type World = { db: Db; deps: CollectDeps; log: string[]; calls: string[]; sha: string; replies: (string | null)[]; fork: string; asked: string[] };

const APPROVE = JSON.stringify({ verdict: "approve", reasons: "fixes the cause", problems: [] });

async function world(o: Opts & { check?: string | null; triage?: string; patches?: (string | null)[]; title?: string; lib?: string } = {}): Promise<World> {
  const root = mkdtempSync(join(dir, "w-"));
  const work = join(root, "work");
  mkdirSync(join(work, "crates/app/src"), { recursive: true });
  git(work, "init", "-q", "-b", "main");
  writeFileSync(join(work, LIB), o.lib ?? "pub fn first(v: &[u8]) -> u8 {\n    v[0]\n}\n");
  git(work, "add", "-A");
  git(work, "commit", "-q", "-m", "init");
  const sha = git(work, "rev-parse", "HEAD");
  const upstream = join(root, "upstream.git");
  git(root, "clone", "-q", "--bare", work, upstream);
  const fork = join(root, "fork.git");
  git(root, "init", "-q", "--bare", fork);
  const db = await fresh(o);
  if (o.title) await db.run(db.k.updateTable("items").set({ title: o.title }).where("number", "=", 5));
  const log: string[] = [], calls: string[] = [], asked: string[] = [];
  const replies = [...(o.patches ?? [GOOD])];
  let forked = false;
  const run: Exec = async (cmd, args, opt = {}): Promise<ExecResult> => {
    const out = (stdout: string, code = 0): ExecResult => ({ code, stdout, stderr: "" });
    if (cmd === "gh") {
      calls.push(`gh ${args.slice(0, 2).join(" ")}`);
      if (args[0] === "api" && args[1] === "user") return out(JSON.stringify({ login: LOGIN, id: 42, node_id: "U42" }));
      if (args[0] === "api" && args[1]!.startsWith(`repos/${LOGIN}/`)) return out("", forked ? 0 : 1);
      if (args[0] === "api" && args[1]!.includes("/forks")) return out(forked ? `${LOGIN}/app\n` : "");
      if (args[0] === "repo") { forked = true; return out(""); }
      if (args[0] === "pr" && args[1] === "list") return out(o.existingPr ? `${o.existingPr}\n` : "");
      if (args[0] === "pr") { log.push(`pr ${args.join(" ")} :: ${opt.input}`); return out("https://github.com/storytold/app/pull/12\n"); }
      if (args[0] === "issue") { log.push(`comment ${opt.input}`); return out(""); }
      return out("", 1);
    }
    const urlAt = args.findIndex((a) => a.startsWith("https://github.com/"));
    if (urlAt >= 0) {
      const target = args[urlAt]!.includes("/storytold/") ? upstream : fork;
      if (args.includes("push")) log.push(`push ${args.filter((a) => a.startsWith("--")).join(" ")} ${args.at(-1)}`);
      const a2 = args.map((a, i) => (i === urlAt ? target : a)).filter((a, i, all) => !(a === "-c" && all[i + 1] === "protocol.file.allow=never") && a !== "protocol.file.allow=never");
      return exec("git", a2, opt);
    }
    return exec("git", args, opt);
  };
  const deps: CollectDeps = {
    exec: run, now: () => new Date("2026-01-02T00:00:00Z"), root: join(root, "cache"),
    mainHead: async () => sha,
    fetchCrates: async () => ({ ok: true, rust: true, crates: [{ name: "app", dir: "crates/app" }] }),
    fetchPaths: async () => [LIB, "README.md", "build.rs"],
    ask: async (_db, id, input) => {
      asked.push(`${id}:${JSON.stringify(input)}`);
      if (id === "fixissue.triage") return o.triage ?? TRIAGE_OK();
      if (id === "fixissue.check") return o.check === undefined ? APPROVE : o.check;
      return replies.length > 1 ? replies.shift()! : (replies[0] ?? null);
    },
    summarise: async (_db, _r, _n, _id, step) => `step ${step} failed: assertion left 0 right 1`,
  };
  return { db, deps, log, calls, sha, replies, fork, asked };
}

type End = "pass" | "fail-test" | "fail-build";
async function finish(db: Db, id: number, end: End): Promise<void> {
  const t = (await getTask(db, id))!;
  const failAt = end === "pass" ? null : end === "fail-test" ? "test" : "build";
  const results = [];
  for (const s of t.plan) {
    const bad = s.name === failAt;
    results.push({ step: s.name, exitCode: bad ? 101 : 0, durationMs: 1, timedOut: false });
    if (bad) break;
  }
  await db.run(db.k.updateTable("build_tasks").set({ state: end === "pass" ? "done" : "failed", results: JSON.stringify(results), ended_at: T0 }).where("id", "=", id));
}
const finishBoth = async (db: Db, head: End, base: End) => {
  const a = (await attemptOf(db, "app", 5))!;
  await finish(db, a.head_task_id!, head);
  await finish(db, a.base_task_id!, base);
};
const quiet = () => {};

describe("opening the PR is gated and repeatable", () => {
  const passedWorld = async (o: Parameters<typeof world>[0] = {}) => {
    const w = await world(o);
    await runFixIssue(w.db, { repo: "app", dryRun: false }, quiet, w.deps);
    await finishBoth(w.db, "pass", "fail-test");
    return w;
  };
  const writes = (w: World) => w.log.length + w.calls.filter((c) => c === "gh repo").length;





});

describe("lane helpers", () => {
});

describe("fix step assignment", () => {
  function withQuerier(w: World, assignees: string[]) {
    const calls: string[] = [];
    let assigned = [...assignees];
    w.deps.querier = validating({
      spent: 0, remaining: 5000,
      async query(q: string) {
        if (q.includes("viewer")) return { viewer: { login: LOGIN, id: "U42" } };
        if (q.startsWith("mutation")) {
          const add = q.includes("addAssignees");
          calls.push(add ? "assign" : "unassign");
          assigned = add ? [...assigned, LOGIN] : assigned.filter((l) => l !== LOGIN);
          return {};
        }
        return { repository: { issue: { id: "I5", state: "OPEN", assignees: { nodes: assigned.map((login) => ({ login })) } } } };
      },
    } as Querier);
    return { calls, assigned: () => assigned };
  }




});

describe("claim comment of an attempt that ends without a PR", () => {
  const URL = "https://github.com/storytold/app/issues/5#issuecomment-777";
  async function claimed(state: string, prUrl: string | null, author = LOGIN) {
    const w = await world();
    await w.db.run(w.db.k.insertInto("fix_attempts").values({ repo: "app", number: 5, main_sha: w.sha, text_hash: "h", state, round: 1, pr_url: prUrl, created_at: T0, updated_at: T0 }));
    await w.db.run(w.db.k.insertInto("audit").values({ at: T0, login: "fixissue", action: "fixissue-claim", target: "app#5", details: JSON.stringify({ url: URL, state: "done" }) }));
    w.deps.querier = validating({ spent: 0, remaining: 5000, async query() { return { viewer: { login: LOGIN, id: "U42" } }; } } as Querier);
    const api: string[] = [];
    const inner = w.deps.exec;
    w.deps.exec = async (cmd, args, opt) => {
      if (cmd === "gh" && args.join(" ").includes("/issues/comments/777")) {
        api.push(args.includes("DELETE") ? "delete" : "get");
        return { code: 0, stdout: args.includes("DELETE") ? "" : `${author}\n`, stderr: "" };
      }
      return inner(cmd, args, opt);
    };
    return { w, api };
  }
  const run = (w: World) => runFixIssue(w.db, { repo: "app", dryRun: false }, quiet, w.deps);

  test("a held attempt's claim is deleted once; a taken attempt keeps it", async () => {
    const held = await claimed("failed", null);
    await run(held.w);
    await run(held.w);
    assert.deepEqual(held.api, ["get", "delete"]);
    assert.equal((await held.w.db.get(held.w.db.k.selectFrom("audit").select((eb) => eb.fn.countAll<number>().as("n")).where("action", "=", "fixissue-unclaim")
      .where((eb) => like(held.w.db, "details", eb.val('%"state":"done"%')))))!.n, 1);

    const bot = await claimed("failed", null, "app-bot[bot]");
    await bot.w.db.run(bot.w.db.k.insertInto("settings").values({ key: "app_slugs", value: '["app-bot"]' }));
    await run(bot.w);
    assert.deepEqual(bot.api, ["get", "delete"]);

    const taken = await claimed("opened", "https://github.com/storytold/app/pull/12");
    await run(taken.w);
    assert.deepEqual(taken.api, []);
  });
});

test("the fix PR we just opened counts as the issue's open PR until a sync mirrors it", async () => {
  const db = await fresh();
  await db.run(db.k.insertInto("fix_attempts").values({ repo: "app", number: 5, main_sha: "m", text_hash: "h", state: "pr-opened", pr_url: "https://github.com/o/app/pull/6", created_at: T0, updated_at: T0 }));
  assert.deepEqual((await loadItemFact(db, "app", 5))?.fix.openPr, { number: 6, closes: true });
  await db.run(db.k.insertInto("items").values({ repo: "app", number: 6, kind: "pr", state: "CLOSED", title: "Fix", body: "", author: "bot", created_at: T0, updated_at: T0 }));
  assert.equal((await loadItemFact(db, "app", 5))?.fix.openPr, null);
});
