// Scratch space for test databases, checkouts and caches: one directory per test
// process (node --test runs each file in its own), removed when the process exits.
// It lives under ~/.cache, not /tmp: /tmp is a RAM disk shared by the whole box.
import { mkdirSync, mkdtempSync, readdirSync, rmSync, statSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";
import "./db-template.ts";

const BASE = join(homedir(), ".cache/autocraft-scratch");
mkdirSync(BASE, { recursive: true });
// Windows refuses to unlink a file that a handle in this process still has open (an SQLite database, say), where Linux
// unlinks it happily. The removal at exit then fails, so this run sweeps the leftovers of earlier ones: only directories
// old enough that no live test process can own them.
const STALE_MS = 6 * 3600_000;
for (const name of readdirSync(BASE)) {
  const dir = join(BASE, name);
  try {
    const age = Date.now() - statSync(dir).mtimeMs;
    if (age > STALE_MS) rmSync(dir, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
  } catch { /* a live test owns it, or it vanished: next run */ }
}
export const SCRATCH = mkdtempSync(join(BASE, "test-"));
// A removal that still fails here is not the test's to fail: the next run's sweep takes the directory.
process.on("exit", () => {
  try { rmSync(SCRATCH, { recursive: true, force: true, maxRetries: 20, retryDelay: 50 }); } catch { /* swept next run */ }
});
// The diff cache defaults to one shared directory under the home: test processes running at once would read, write and
// drop (dropCachedDiff) each other's diffs for the same fixture heads (app#3@sha3). A test may still point it elsewhere.
process.env.AUTOCRAFT_DIFF_CACHE = join(SCRATCH, "diffs");
// The sandbox checkout root, its sibling `sessions` (per-call session stores) and the generated agents-*.json files, and the
// fixed model-call working directory, all default to directories under ~/.cache/autocraft. A test may still point them elsewhere.
process.env.AUTOCRAFT_CHECKOUT_DIR = join(SCRATCH, "checkouts");
process.env.AUTOCRAFT_CLAUDE_CWD = join(SCRATCH, "claude-cwd");
