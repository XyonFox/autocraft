import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";

const script = new URL("../deploy/container/ws", import.meta.url).pathname;

test("run reports exit codes, caps a large log, and background calls are waited for; kill ends a call; put checks the expected sha", () => {
  const root = mkdtempSync(join(tmpdir(), "ws-helper-"));
  const env = { ...process.env, WS_ROOT: root };
  const ws = (args: string[], input = "") => {
    const r = spawnSync("sh", [script, ...args], { env, input, encoding: "utf8", timeout: 20000 });
    const nl = r.stdout.indexOf("\n");
    return { status: r.status, header: r.stdout.slice(0, nl), body: r.stdout.slice(nl + 1) };
  };
  try {
    // Directories the init subcommand creates, without its wait loop.
    spawnSync("mkdir", ["-p", join(root, ".ws/calls"), join(root, "src"), join(root, "home")]);

    const fg = ws(["run", "1", "20", "--", "sh", "-c", "seq 1 20000; exit 3"]);
    assert.equal(fg.status, 0);
    const [w, state, code, , total] = fg.header.split(" ");
    assert.deepEqual([w, state, code], ["WS", "exited", "3"]);
    assert.equal(Number(total), readFileSync(join(root, ".ws/calls/1.log")).length);
    assert.match(fg.body, /\[ws: \d+ bytes cut; full log .*1\.log\]/);
    assert.ok(Buffer.byteLength(fg.body) < 31500);
    assert.ok(fg.body.endsWith("20000\n"));

    assert.equal(ws(["run", "1", "5", "--", "true"]).status, 4, "call numbers are not reused");

    assert.equal(ws(["run", "2", "20", "--background", "--", "sh", "-c", "sleep 0.3; echo done"]).header, "WS started 2");
    const st = ws(["status", "2", "--wait", "10"]);
    assert.match(st.header, /^WS exited 0 \d+ 5$/);
    assert.equal(st.body, "done\n");
    assert.equal(ws(["status", "2", "--since", "2"]).body, "ne\n");
    assert.equal(ws(["status", "99"]).status, 3);

    ws(["run", "3", "60", "--background", "--", "sleep", "60"]);
    assert.match(ws(["status", "3"]).header, /^WS running - /);
    assert.match(ws(["kill", "3"]).header, /^WS killed \d+ /);

    const target = join(root, "src/a/b.txt");
    const sha = (s: string) => createHash("sha256").update(s).digest("hex");
    assert.match(ws(["put", target, "--expect", "none"], "one").header, new RegExp(`^WS ok 3 ${sha("one")}$`));
    const bad = ws(["put", target, "--expect", sha("zzz")], "two");
    assert.equal(bad.status, 4);
    assert.equal(bad.header, `WS error conflict ${sha("one")}`);
    assert.equal(ws(["put", target, "--expect", "none"], "two").status, 4);
    assert.equal(ws(["put", target, "--expect", sha("one")], "two").status, 0);
    assert.equal(readFileSync(target, "utf8"), "two");
    assert.equal(ws(["put", `${root}/src/../x`], "x").status, 2);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("untar gives the same base commit for the same tree, and run prunes only ended calls older than a day", () => {
  const dirs: string[] = [];
  try {
    const tar = join(tmpdir(), `ws-helper-src-${process.pid}.tar`);
    const srcDir = mkdtempSync(join(tmpdir(), "ws-helper-src-"));
    dirs.push(srcDir);
    spawnSync("sh", ["-c", `echo hi > a.txt && tar -cf ${tar} a.txt`], { cwd: srcDir });
    const bases = [0, 1].map(() => {
      const root = mkdtempSync(join(tmpdir(), "ws-helper-"));
      dirs.push(root);
      const r = spawnSync("sh", [script, "untar"], { env: { ...process.env, WS_ROOT: root }, input: readFileSync(tar), encoding: "utf8", timeout: 20000 });
      return r.stdout.split("\n")[0];
    });
    rmSync(tar, { force: true });
    assert.match(bases[0]!, /^WS ok [0-9a-f]{40}$/);
    assert.equal(bases[0], bases[1]);

    const root = dirs[1]!;
    const calls = join(root, ".ws/calls");
    spawnSync("sh", ["-c", `mkdir -p ${calls} && cd ${calls} && for n in 1 2; do echo 0 > $n.exit; echo x > $n.log; echo 1 > $n.start; done; echo x > 3.log; echo 1 > 3.start; touch -d '3 days ago' 1.exit 1.log 1.start 3.start`]);
    const r = spawnSync("sh", [script, "run", "9", "10", "--", "true"], { env: { ...process.env, WS_ROOT: root }, encoding: "utf8", timeout: 20000 });
    assert.equal(r.status, 0);
    const left = spawnSync("ls", [calls], { encoding: "utf8" }).stdout.split("\n");
    assert.ok(!left.includes("1.log") && !left.includes("1.exit"), "an old ended call is pruned");
    assert.ok(left.includes("2.log") && left.includes("3.start"), "a recent and a running call stay");
  } finally {
    for (const d of dirs) rmSync(d, { recursive: true, force: true });
  }
});
