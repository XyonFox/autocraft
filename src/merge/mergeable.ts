// A PR whose mergeable state GitHub keeps reporting as unknown. The checks step reads every waiting PR each minute; when a read still
// shows UNKNOWN after UNKNOWN_AFTER_MS, the PR is read again (GitHub computes the merge state when a PR is requested), at most
// UNKNOWN_REREADS times with UNKNOWN_GAP_MS between tries. The merge queue then asks a person (src/core/decide.ts).
import type { Querier } from "../mirror.ts";
import { refreshChecks, type PrChecks } from "../review/checks.ts";

import { spawn } from "node:child_process";
import { slugOf } from "../owners.ts";
import { mergeDecisionId, UNKNOWN_REREADS } from "../core/rules.ts";
import { withdrawMergeQuestion } from "./withdraw.ts";
import { readEnvFor } from "../github/read.ts";
import type { Db } from "../db/index.ts";
import { batches, excluded, inList, like, rowid, tupleIn } from "../db/kx.ts";

const PREFIX = "mergeable_unknown:";
const keyOf = (repo: string, n: number) => `${PREFIX}${repo}#${n}`;

export type UnknownState = { head: string; since: string; reads: number; rereads: number; stuck: boolean };

const parse = (v: string | null): UnknownState | null => { try { if (v === null) return null; const x = JSON.parse(v) as UnknownState; return typeof x.since === "string" && typeof x.head === "string" ? x : null; } catch { return null; } };

/** GraphQL checks reads in a row that still showed an unknown state before GitHub's REST endpoint is asked (a count, not a clock). */
export const READS_BEFORE_REQUEST = 5;

/**
 * Follows PRs whose plan waits on GitHub's mergeable state: a take review at the head and CI success. Counts the consecutive checks reads
 * that still show an unknown state; a known state forgets the PR (and sets aside a Merge? question that was asked about the unknown state).
 */
export async function noteMergeable(db: Db, rows: PrChecks[], at: string): Promise<void> {
  // The take reviews and the noted states are read for all rows in one query per batch, and written in multi-row statements.
  const known = (r: PrChecks) => r.mergeable === "MERGEABLE" || r.mergeable === "CONFLICTING";
  const waiting = rows.filter((r) => !known(r) && r.state === "OPEN" && r.overall === "success");
  const taken = new Set<string>();
  for (const heads of batches(waiting.map((r) => [r.repo, r.number, r.head_oid] as const)))
    for (const t of await db.all(db.k.selectFrom("final_reviews as l").select(["l.repo", "l.number", "l.head_oid"])
      .where((eb) => tupleIn(db, [eb.ref("l.repo"), eb.ref("l.number"), eb.ref("l.head_oid")], heads, ["text", "bigint", "text"]))
      .where("l.verdict", "in", ["take", "take-fix"])
      .where((eb) => eb("l.head_oid", "=", eb.selectFrom("opus_reviews as r").select("r.head_oid").whereRef("r.repo", "=", "l.repo").whereRef("r.number", "=", "l.number")
        .where("r.verdict", "is not", null).orderBy("r.at", "desc").orderBy(rowid("opus_reviews", "r"), "desc").limit(1)))))
      taken.add(`${t.repo}#${t.number}@${t.head_oid}`);
  const withdraw: { repo: string; number: number; head: string }[] = [];
  // The notes are read and rewritten in one write transaction, so a concurrent writer of the same PR cannot lose a count.
  await db.write(async () => {
    const notes = new Map<string, string | null>();
    for (const keys of batches(rows.map((r) => keyOf(r.repo, r.number))))
      for (const n of await db.all(db.k.selectFrom("main.settings").select(["key", "value"]).where((eb) => inList(db, eb.ref("key"), keys)))) notes.set(n.key, n.value);
    const sets: { key: string; value: string }[] = [];
    const dels: string[] = [];
    for (const r of rows) {
      const k = keyOf(r.repo, r.number);
      const cur = notes.has(k) ? { value: notes.get(k)! } : undefined;
      if (known(r) || r.state !== "OPEN" || r.overall !== "success" || !taken.has(`${r.repo}#${r.number}@${r.head_oid}`)) {
        if (cur) {
          dels.push(k);
          const prev = parse(cur.value);
          if (prev && known(r)) withdraw.push({ repo: r.repo, number: r.number, head: prev.head });
        }
        continue;
      }
      const prev = cur ? parse(cur.value) : null;
      if (!prev || prev.head !== r.head_oid) sets.push({ key: k, value: JSON.stringify({ head: r.head_oid, since: at, reads: 1, rereads: 0, stuck: false } satisfies UnknownState) });
      else sets.push({ key: k, value: JSON.stringify({ ...prev, reads: prev.reads + 1 } satisfies UnknownState) });
    }
    for (const values of batches(sets)) await db.run(db.k.insertInto("main.settings").values(values).onConflict((oc) => oc.column("key").doUpdateSet(excluded(["value"]))));
    for (const keys of batches(dels)) await db.run(db.k.deleteFrom("main.settings").where((eb) => inList(db, eb.ref("key"), keys)));
  });
  // A Merge? question asked about the unknown state is set aside once GitHub reports the state (rare: one read per such PR).
  const open = (id: string) => db.get(db.k.selectFrom("decisions").select("id").where("id", "=", id).where("answer", "is", null)
    .where((eb) => like(db, "context", eb.val("%still reports the merge state as unknown%"))));
  for (const w of withdraw)
    if (await open(mergeDecisionId(w.repo, w.number, w.head))) await withdrawMergeQuestion(db, w.repo, w.number, w.head, at, "GitHub now reports the merge state");
}

/** What the planner needs for one PR at a head, or null when its state is not unknown (or not noted yet). */
export async function unknownOf(db: Db, repo: string, n: number, head: string): Promise<UnknownState | null> {
  const row = await db.get(db.k.selectFrom("main.settings").select("value").where("key", "=", keyOf(repo, n)));
  const s = row ? parse(row.value) : null;
  return s && s.head === head ? s : null;
}

/** The mergeable state of a PR as GitHub's REST endpoint reports it (reading it starts GitHub's mergeability job); null while GitHub has not computed it. */
export type RestRead = (repo: string, number: number) => Promise<string | null>;

export const restMergeable: RestRead = async (repo, number) => {
  const args = ["api", `repos/${slugOf(repo)}/pulls/${number}`, "--jq", ".mergeable"];
  const env = await readEnvFor("gh", args);
  return new Promise((resolve, reject) => {
  const p = spawn("gh", args, { stdio: ["ignore", "pipe", "pipe"], windowsHide: true, ...(env ? { env } : {}) });
  let out = "", err = "";
  p.stdout.on("data", (d) => (out += d));
  p.stderr.on("data", (d) => (err += d));
  p.on("error", reject);
  p.on("close", (code) => (code === 0 ? resolve(out.trim() === "true" ? "MERGEABLE" : out.trim() === "false" ? "CONFLICTING" : null) : reject(new Error(err.trim().slice(0, 200) || `gh exited ${code}`))));
  });
};

/**
 * Requests a PR whose state stayed unknown for READS_BEFORE_REQUEST checks reads from GitHub's REST endpoint, which starts the mergeability job.
 * A null answer marks the PR stuck, and the merge queue asks a person. No clock: one request per PR and head. A known answer is stored by reading the PR's checks again.
 */
export async function rereadUnknown(db: Db, client: Querier, repo: string, log: (line: string) => void = () => {}, read: RestRead = restMergeable): Promise<number[]> {
  const rows = await db.all(db.k.selectFrom("main.settings").select(["key", "value"]).where((eb) => like(db, "key", eb.val(`${PREFIX}${repo}#%`))));
  const noted = rows.flatMap((r) => { const n = Number(r.key.slice(r.key.lastIndexOf("#") + 1)); return Number.isInteger(n) ? [{ ...r, n, s: parse(r.value) }] : []; });
  const open = new Set<number>();
  for (const nums of batches(noted.map((r) => r.n)))
    for (const o of await db.all(db.k.selectFrom("main.items").select("number").where("repo", "=", repo).where("kind", "=", "pr").where("state", "=", "OPEN").where("number", "in", nums))) open.add(o.number);
  const done: number[] = [], known: number[] = [], gone: string[] = [];
  const stuck: { key: string; was: string; value: string }[] = [];
  for (const { key, value, n, s } of noted) {
    if (!s || !open.has(n)) { gone.push(key); continue; }
    if (s.stuck || s.rereads >= UNKNOWN_REREADS || s.reads < READS_BEFORE_REQUEST) continue;
    let state: string | null;
    try { state = await read(repo, n); } catch (e) { log(`checks: ${repo}#${n}: the REST read failed: ${String((e as Error).message ?? e)}`); continue; }
    done.push(n);
    if (state !== null) { known.push(n); continue; }
    stuck.push({ key, was: value!, value: JSON.stringify({ ...s, rereads: s.rereads + 1, stuck: true } satisfies UnknownState) });
  }
  // The REST reads run before the write lock is taken.
  if (gone.length || stuck.length) await db.write(async () => {
    for (const keys of batches(gone)) await db.run(db.k.deleteFrom("main.settings").where((eb) => inList(db, eb.ref("key"), keys)));
    // A note the checks step rewrote meanwhile (a new head, another read) keeps that newer value.
    for (const r of stuck) await db.run(db.k.updateTable("main.settings").set({ value: r.value }).where("key", "=", r.key).where("value", "=", r.was));
  });
  if (known.length) await refreshChecks(db, client, repo, known, log);
  if (done.length) log(`checks: ${repo}: requested ${done.length} PR${done.length === 1 ? "" : "s"} with an unknown mergeable state from GitHub (${done.map((n) => `#${n}`).join(", ")})`);
  return done;
}
