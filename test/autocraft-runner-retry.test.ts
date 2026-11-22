import { test } from "node:test";
import assert from "node:assert/strict";
import { createServer } from "node:http";
import type { AddressInfo } from "node:net";
import { gunzipSync } from "node:zlib";
import * as helper from "../worker/autocraft.mjs";

test("lease requests retry 5xx and resets with backoff, keep the same body, and stop at a 410, another 4xx or the deadline", async () => {
  const hits: Record<string, string[]> = {};
  const server = createServer((req, res) => {
    let body = "";
    req.on("data", (d) => { body += d; });
    req.on("end", () => {
      const seen = (hits[req.url ?? ""] ??= []);
      seen.push(body);
      if (req.url === "/api/remote/submit") {
        if (seen.length === 1) { res.writeHead(502, { "Retry-After": "0" }).end("bad gateway"); return; }
        if (seen.length === 2) { req.socket.destroy(); return; }
        res.writeHead(200, { "Content-Type": "application/json" }).end(JSON.stringify({ accepted: true }));
      } else if (req.url === "/api/remote/tool/work/shell") {
        if (seen.length === 1) { res.writeHead(429, { "Retry-After": "0" }).end("slow down"); return; }
        res.writeHead(502).end("bad gateway");
      } else if (req.url === "/api/remote/heartbeat") res.writeHead(410).end();
      else if (req.url === "/api/remote/release") res.writeHead(400).end("bad body");
      else res.writeHead(503, { "Retry-After": "30" }).end();
    });
  });
  await new Promise<void>((r) => server.listen(0, "127.0.0.1", r));
  const base = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
  const opt = () => ({ deadline: Date.now() + 10_000, baseMs: 5 });
  try {
    const submit = { ok: true, text: "done" };
    assert.deepEqual(await helper.withRetry(() => helper.api(base, "/api/remote/submit", "s", submit), opt()), { accepted: true });
    assert.deepEqual(hits["/api/remote/submit"], Array(3).fill(JSON.stringify(submit)));

    // A workspace tool call that may have run is not sent again; one refused for the rate is.
    await assert.rejects(helper.withRetry(() => helper.api(base, "/api/remote/tool/work/shell", "s", { command: "make" }), { ...opt(), again: helper.notRun }), /HTTP 502/);
    assert.equal(hits["/api/remote/tool/work/shell"].length, 2);

    await assert.rejects(helper.withRetry(() => helper.api(base, "/api/remote/heartbeat", "s", { phase: "running" }), opt()), /the lease is gone/);
    assert.equal(hits["/api/remote/heartbeat"].length, 1);
    await assert.rejects(helper.withRetry(() => helper.api(base, "/api/remote/release", "s", { reason: "x" }), opt()), /HTTP 400/);
    assert.equal(hits["/api/remote/release"].length, 1);
    // Retry-After 30 s does not fit before a deadline 1 s away: one try.
    await assert.rejects(helper.withRetry(() => helper.api(base, "/api/remote/bundle", "s"), { deadline: Date.now() + 1000, baseMs: 5 }), /HTTP 503/);
    assert.equal(hits["/api/remote/bundle"].length, 1);
  } finally {
    server.close();
  }
});

test("after a status reply lists gzip+base64, bodies go as an envelope; an HTML 403 from a proxy is retried, not final", async () => {
  const bodies: string[] = [];
  const server = createServer((req, res) => {
    let body = "";
    req.on("data", (d) => { body += d; });
    req.on("end", () => {
      if (req.url === "/api/remote/status") { res.writeHead(200, { "Content-Type": "application/json" }).end(JSON.stringify({ ok: true, reserve: { fiveHour: 20, weekly: 20 }, encodings: ["gzip+base64"] })); return; }
      bodies.push(body);
      if (bodies.length === 1) { res.writeHead(403, { "Content-Type": "text/html" }).end("<!DOCTYPE html><html><body>Sorry, you have been blocked</body></html>"); return; }
      res.writeHead(200, { "Content-Type": "application/json" }).end(JSON.stringify({ accepted: true }));
    });
  });
  await new Promise<void>((r) => server.listen(0, "127.0.0.1", r));
  const base = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
  try {
    await helper.api(base, "/api/remote/status", "t", { readiness: "ready" });
    const submit = { ok: true, text: "<script>alert(1)</script>" };
    const blocked = await helper.api(base, "/api/remote/submit", "s", submit).catch((e: unknown) => e);
    assert.ok(blocked instanceof helper.ProxyBlocked && helper.transient(blocked));
    assert.equal((blocked as Error).message, "/api/remote/submit: blocked by a proxy in front of the server (HTTP 403, HTML reply)");
    assert.deepEqual(await helper.withRetry(() => helper.api(base, "/api/remote/submit", "s", submit), { deadline: Date.now() + 10_000, baseMs: 5 }), { accepted: true });
    const sent = JSON.parse(bodies.at(-1)!) as { encoding: string; body: string };
    assert.equal(sent.encoding, "gzip+base64");
    assert.deepEqual(JSON.parse(gunzipSync(Buffer.from(sent.body, "base64")).toString("utf8")), submit);
  } finally {
    server.close();
  }
});
