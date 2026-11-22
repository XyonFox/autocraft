import { test } from "node:test";
import assert from "node:assert/strict";
import { execFile, spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import { createServer, type IncomingMessage, type ServerResponse } from "node:http";
import { copyFileSync, existsSync, lstatSync, mkdirSync, mkdtempSync, readFileSync, readlinkSync, realpathSync, rmSync, statSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { promisify } from "node:util";
import * as helper from "../worker/autocraft.mjs";

const run = promisify(execFile);
const WORKER = realpathSync(new URL("../worker/autocraft.mjs", import.meta.url).pathname);
const SHIM = realpathSync(new URL("../worker/autocraft-task.mjs", import.meta.url).pathname);

/** A home with an autocraft-task 1.2.0 install: config with its agent token, cache, a systemd unit, and a fake systemctl that logs its calls. */
function legacyHome(scriptOf: (home: string) => string, failStart = false) {
  const dir = mkdtempSync(join(tmpdir(), "ac-migrate-"));
  const home = join(dir, "home"), cfg = join(home, ".config"), cache = join(home, ".cache"), bin = join(dir, "bin"), log = join(dir, "systemctl.log");
  for (const d of [join(cfg, "autocraft-task", "leases"), join(cache, "autocraft-task", "runs"), join(cfg, "systemd", "user"), bin]) mkdirSync(d, { recursive: true });
  writeFileSync(join(cfg, "autocraft-task", "config.json"), JSON.stringify({ server: "https://ac.example", agentToken: "acmcp_legacytoken1234", harness: "claude" }), { mode: 0o600 });
  writeFileSync(join(cache, "autocraft-task", "status.json"), JSON.stringify({ sent: { busy: 0 } }));
  const env = { PATH: "/usr/bin:/bin", CLAUDE_CONFIG_DIR: "/home/x/.claude 50%", ODD: "a$$b" };
  writeFileSync(join(cfg, "systemd", "user", "autocraft-task.service"), helper.systemdUnit({ argv: [process.execPath, scriptOf(home), "work", "--parallel", "3"], env }));
  const fail = failStart ? `[ "$*" = "--user start autocraft" ] && { echo "Job failed" >&2; exit 1; }\n` : "";
  writeFileSync(join(bin, "systemctl"), `#!/bin/sh\necho "$*" >> ${log}\n${fail}`, { mode: 0o755 });
  const calls = () => (existsSync(log) ? readFileSync(log, "utf8").trim().split("\n") : []);
  return { dir, home, cfg, cache, bin, env, calls, procEnv: { ...process.env, HOME: home, XDG_CONFIG_HOME: cfg, XDG_CACHE_HOME: cache, PATH: `${bin}:${process.env.PATH}` } };
}

test("a restart of the old unit moves the runner over: directories, credential, unit with the same work arguments", () => {
  // The production layout: the old unit runs the checkout's shim, and the new code starts inside that unit.
  const h = legacyHome(() => SHIM);
  const saved = { ...process.env };
  Object.assign(process.env, h.procEnv);
  try {
    const cgroup = join(h.dir, "cgroup");
    writeFileSync(cgroup, "0::/user.slice/user-1000.slice/user@1000.service/app.slice/autocraft-task.service\n");
    const r = helper.migrateLegacy({ cgroup });
    assert.equal(r.switched, true);
    for (const [base, legacy] of [[h.cfg, "autocraft-task"], [h.cache, "autocraft-task"]]) {
      assert.ok(lstatSync(join(base, legacy)).isSymbolicLink());
      assert.equal(readlinkSync(join(base, legacy)), join(base, "autocraft-runner"));
    }
    assert.ok(existsSync(join(h.cfg, "autocraft-runner", "leases")) && existsSync(join(h.cache, "autocraft-runner", "status.json")));
    const creds = JSON.parse(readFileSync(join(h.cfg, "autocraft-runner", "credentials.json"), "utf8"));
    assert.deepEqual([creds.credential, creds.server, creds.state], ["acmcp_legacytoken1234", "https://ac.example", "approved"]);
    assert.equal(statSync(join(h.cfg, "autocraft-runner", "credentials.json")).mode & 0o777, 0o600);
    assert.equal(JSON.parse(readFileSync(join(h.cfg, "autocraft-runner", "config.json"), "utf8")).agentToken, undefined);
    const unit = helper.legacyService(join(h.cfg, "systemd", "user", "autocraft.service"), false);
    assert.deepEqual(unit?.argv, [process.execPath, WORKER, "work", "--parallel", "3"]);
    assert.deepEqual(unit?.env, h.env);
    assert.ok(!existsSync(join(h.cfg, "systemd", "user", "autocraft-task.service")));
    const calls = h.calls();
    assert.ok(calls.indexOf("--user enable autocraft") < calls.indexOf("--user start autocraft"));
    assert.ok(calls.indexOf("--user start autocraft") < calls.indexOf("--user disable autocraft-task"));
    assert.equal(calls.at(-1), "--user --no-block stop autocraft-task");
    assert.ok(!existsSync(join(h.cfg, "autocraft-task.migrate.lock")), "the lock is gone before the old unit's stop ends the process");
  } finally {
    for (const k of Object.keys(process.env)) if (!(k in saved)) delete process.env[k];
    Object.assign(process.env, saved);
    rmSync(h.dir, { recursive: true, force: true });
  }
});

test("a lock of a dead process is taken over, and none is left after the migration", () => {
  const h = legacyHome(() => SHIM);
  const saved = { ...process.env };
  Object.assign(process.env, h.procEnv);
  try {
    const dead = spawnSync("true").pid;
    const lock = join(h.cfg, "autocraft-task.migrate.lock");
    writeFileSync(lock, String(dead));
    const r = helper.migrateLegacy({ manual: true });
    assert.ok(!r.lines.some((l) => l.includes("another process")));
    assert.ok(lstatSync(join(h.cfg, "autocraft-task")).isSymbolicLink());
    assert.ok(!existsSync(lock));
  } finally {
    for (const k of Object.keys(process.env)) if (!(k in saved)) delete process.env[k];
    Object.assign(process.env, saved);
    rmSync(h.dir, { recursive: true, force: true });
  }
});

test("a new unit that fails to start leaves the old unit installed, enabled and running", () => {
  const h = legacyHome(() => SHIM, true);
  const saved = { ...process.env };
  Object.assign(process.env, h.procEnv);
  try {
    const cgroup = join(h.dir, "cgroup");
    writeFileSync(cgroup, "0::/user.slice/user-1000.slice/user@1000.service/app.slice/autocraft-task.service\n");
    assert.throws(() => helper.migrateLegacy({ cgroup }), /autocraft service did not start; autocraft-task stays/);
    assert.ok(existsSync(join(h.cfg, "systemd", "user", "autocraft-task.service")));
    assert.ok(!existsSync(join(h.cfg, "systemd", "user", "autocraft.service")));
    const calls = h.calls();
    assert.ok(calls.includes("--user disable autocraft"));
    assert.ok(!calls.some((c) => /autocraft-task/.test(c)), calls.join("; "));
  } finally {
    for (const k of Object.keys(process.env)) if (!(k in saved)) delete process.env[k];
    Object.assign(process.env, saved);
    rmSync(h.dir, { recursive: true, force: true });
  }
});

test("migrate by hand waits while the old runner runs a call, then renames the installed copy and switches units", async () => {
  const h = legacyHome((home) => join(home, ".local", "bin", "autocraft-task"));
  const installed = join(h.home, ".local", "bin", "autocraft-task");
  try {
    mkdirSync(join(h.home, ".local", "bin"), { recursive: true });
    copyFileSync(WORKER, installed);
    // A call running in another live process (this test's) keeps everything where it is.
    const running = join(h.cache, "autocraft-task", "runs", "7.json");
    writeFileSync(running, JSON.stringify({ state: "running", pid: process.pid, at: Date.now(), line: "running" }));
    const busy = await run(process.execPath, [installed, "migrate"], { env: h.procEnv });
    assert.match(busy.stdout, /running calls/);
    assert.ok(lstatSync(join(h.cfg, "autocraft-task")).isDirectory() && !lstatSync(join(h.cfg, "autocraft-task")).isSymbolicLink());
    assert.ok(existsSync(join(h.cfg, "systemd", "user", "autocraft-task.service")) && !existsSync(join(h.cfg, "systemd", "user", "autocraft.service")));
    assert.ok(!h.calls().some((c) => /stop|start/.test(c)));

    rmSync(running);
    const bin = join(h.home, ".local", "bin", "autocraft");
    // The busy run above already renamed the copy (harmless while it runs); the alias still starts it.
    await run(process.execPath, [installed, "migrate"], { env: h.procEnv });
    assert.ok(lstatSync(join(h.cfg, "autocraft-task")).isSymbolicLink());
    assert.ok(lstatSync(installed).isSymbolicLink() && readlinkSync(installed) === "autocraft" && lstatSync(bin).isFile());
    const unit = helper.legacyService(join(h.cfg, "systemd", "user", "autocraft.service"), false);
    assert.deepEqual(unit?.argv, [process.execPath, bin, "work", "--parallel", "3"]);
    const calls = h.calls();
    assert.ok(calls.indexOf("--user stop autocraft-task") < calls.indexOf("--user start autocraft"));
    assert.ok(!calls.includes("--user --no-block stop autocraft-task"));
  } finally { rmSync(h.dir, { recursive: true, force: true }); }
});

/** A fake server for the login routes and claims; `handle` answers each request after its body is read. */
async function fakeServer(handle: (path: string, body: any, req: IncomingMessage, res: ServerResponse) => void) {
  const server = createServer((req, res) => {
    let raw = "";
    req.on("data", (c) => { raw += c; });
    req.on("end", () => handle(req.url ?? "", raw ? JSON.parse(raw) : null, req, res));
  });
  await new Promise<void>((done) => server.listen(0, "127.0.0.1", done));
  const port = (server.address() as { port: number }).port;
  return { url: `http://127.0.0.1:${port}`, close: () => new Promise((done) => server.close(done)) };
}
const json = (res: ServerResponse, status: number, body: unknown, headers: Record<string, string> = {}) => { res.writeHead(status, { "content-type": "application/json", ...headers }); res.end(JSON.stringify(body)); };

test("login --no-wait keeps the new credential and prints the approval link; login --wait stores it once approved", async () => {
  const dir = mkdtempSync(join(tmpdir(), "ac-login-"));
  const env = { ...process.env, HOME: dir, XDG_CONFIG_HOME: join(dir, "cfg"), XDG_CACHE_HOME: join(dir, "cache") };
  let started: any = null, polls = 0, bearer = "";
  const srv = await fakeServer((path, body, req, res) => {
    if (path === helper.ROUTES.loginStart) { started = { ...body, auth: req.headers.authorization }; return json(res, 200, { userCode: "BCDF-GHJK", verifyUrl: `${srv.url}/connect?runner=BCDF-GHJK`, interval: 1, expiresIn: 600 }); }
    if (path === helper.ROUTES.loginPoll) {
      bearer = String(req.headers.authorization);
      // One poll too early, then the approval.
      return ++polls === 1 ? json(res, 429, { error: "slow down" }, { "retry-after": "1" }) : json(res, 200, { state: "approved" });
    }
    json(res, 404, { error: "not found" });
  });
  try {
    const a = await run(process.execPath, [WORKER, "login", srv.url, "--label", "laptop", "--no-wait"], { env });
    assert.match(a.stdout, /connect\?runner=BCDF-GHJK/);
    assert.match(a.stdout, /BCDF-GHJK/);
    const credsFile = join(dir, "cfg", "autocraft-runner", "credentials.json");
    assert.equal(statSync(credsFile).mode & 0o777, 0o600);
    assert.equal(statSync(join(dir, "cfg", "autocraft-runner")).mode & 0o777, 0o700);
    const pending = JSON.parse(readFileSync(credsFile, "utf8")).pending;
    assert.match(pending.credential, /^acrun_[A-Za-z0-9_-]{43}$/);
    assert.equal(started.credentialHash, createHash("sha256").update(pending.credential).digest("hex"));
    assert.deepEqual([started.label, started.helperVersion, started.auth, typeof started.machine.hostname], ["laptop", helper.HELPER_VERSION, undefined, "string"]);
    assert.ok(!existsSync(join(dir, "cfg", "autocraft-runner", "config.json")) || !readFileSync(join(dir, "cfg", "autocraft-runner", "config.json"), "utf8").includes(pending.credential));

    const b = await run(process.execPath, [WORKER, "login", "--wait"], { env });
    assert.match(b.stdout, /approved/);
    assert.equal(polls, 2);
    assert.equal(bearer, `Bearer ${pending.credential}`);
    const creds = JSON.parse(readFileSync(credsFile, "utf8"));
    assert.deepEqual([creds.credential, creds.state, creds.label, creds.pending], [pending.credential, "approved", "laptop", undefined]);
    assert.equal(JSON.parse(readFileSync(join(dir, "cfg", "autocraft-runner", "config.json"), "utf8")).server, srv.url);
  } finally { await srv.close(); rmSync(dir, { recursive: true, force: true }); }
});

test("a server that refuses the current protocol is asked again with each older one, protocol 3 without a held wait", async () => {
  const seen: { protocol: number, wait?: number }[] = [];
  const srv = await fakeServer((_path, body, _req, res) => {
    seen.push({ protocol: body.protocol, ...(body.wait !== undefined && { wait: body.wait }) });
    if (body.protocol !== 3) return json(res, 409, { error: "this server speaks protocol 3; update the helper" });
    json(res, 200, { lease: null, retryAfter: 5, reserve: { fiveHour: 20, weekly: 20 } });
  });
  try {
    const base = { requestId: "r1", helperVersion: helper.HELPER_VERSION, harness: "claude" as const, harnessVersion: "2.0.0", tiers: { top: "opus" } };
    const r = await helper.claimRequest(srv.url, "acrun_x", base);
    assert.deepEqual(r, { lease: null, retryAfter: 5, reserve: { fiveHour: 20, weekly: 20 } });
    await helper.claimRequest(srv.url, "acrun_x", { ...base, requestId: "r2" });
    assert.deepEqual(seen, [{ protocol: helper.PROTOCOL_VERSION, wait: helper.CLAIM_WAIT_S }, { protocol: 4, wait: helper.CLAIM_WAIT_S }, { protocol: 3 }, { protocol: 3 }]);
  } finally { await srv.close(); }
});

test("autocraft-task 1.2.0's update accepts the runner served at its path", () => {
  // The check of 1.2.0's cmdUpdate, verbatim: the server serves worker/autocraft.mjs at /worker/autocraft-task.mjs.
  const text = readFileSync(WORKER, "utf8");
  assert.equal(/export const HELPER_VERSION = "([^"]+)"/.exec(text)?.[1], helper.HELPER_VERSION);
  assert.match(readFileSync(SHIM, "utf8"), /import \{ main \} from "\.\/autocraft\.mjs"/);
});
