// Job logs in the database. A job's process writes its log file on the runner's disk; the runner copies the new whole
// lines into job_log_chunks every FLUSH_MS while the job runs and once more when it ends, so a dashboard without that
// disk can show the log. One copy writes the new lines of every running job in one INSERT (and, past the cap, one
// DELETE of the oldest chunks), never a transaction held across file reads. Only the last JOB_LOG_KEEP_BYTES of a job
// are kept, for JOB_LOG_KEEP_MS; the file keeps everything. Chunks are scrubbed before they are stored, and again when read.
import { closeSync, fstatSync, openSync, readSync } from "node:fs";
import type { Db } from "../db/index.ts";
import { scrub } from "./scrub.ts";

export const JOB_LOG_KEEP_BYTES = 256 * 1024;
/** Chunks are dropped once the kept text passes the cap by this much, so the DELETE runs once per this many bytes, not per copy. */
const DROP_SLACK_BYTES = 64 * 1024;
export const JOB_LOG_FLUSH_MS = 5_000;
/** Postgres text cannot hold NUL; the other C0 controls (bar tab, newline, carriage return) are terminal noise. */
// eslint-disable-next-line no-control-regex
const CONTROLS = /[\u0000-\u0008\u000b\u000c\u000e-\u001f]/g;

/** The bytes of `path` from `from` (an offset, or one computed from the size) to its end; null when the file cannot be read. */
function readRange(path: string, from: number | ((size: number) => number)): { buf: Buffer; start: number; size: number } | null {
  let fd: number;
  try {
    fd = openSync(path, "r");
  } catch {
    return null;
  }
  try {
    const size = fstatSync(fd).size;
    const start = typeof from === "function" ? from(size) : from;
    const buf = Buffer.alloc(Math.max(0, size - start));
    if (buf.length) readSync(fd, buf, 0, buf.length, start);
    return { buf, start, size };
  } finally {
    closeSync(fd);
  }
}

type Chunk = { job_id: number; seq: number; start: number; next_start: number; chunk: string; at: string };

/** Log writes of this process, one after another: a copy never reads a file while an earlier copy of it is being written. */
let queue: Promise<void> = Promise.resolve();

/** Copies one job's log file into job_log_chunks. One per running job, in the process that started it; written by `copyLogs`. */
export class JobLogCopy {
  readonly job: number;
  #path: string;
  #offset: number;
  #seq: number;
  #kept: { seq: number; bytes: number }[];
  #lastAt = 0;

  private constructor(job: number, path: string, offset: number, kept: { seq: number; bytes: number }[]) {
    this.job = job;
    this.#path = path;
    this.#offset = offset;
    this.#kept = kept;
    this.#seq = kept.at(-1)?.seq ?? 0;
  }

  /**
   * Starts after what an earlier run of the job (a resumed job) already copied, when the file still holds it; a file shorter
   * than that is another file (a new disk), copied from its start.
   */
  static async open(db: Db, job: number, path: string): Promise<JobLogCopy> {
    const rows = await db.all(db.k.selectFrom("job_log_chunks").select(["seq", "start", "chunk", "next_start"]).where("job_id", "=", job).orderBy("seq"));
    const kept = rows.map((r) => ({ seq: Number(r.seq), bytes: Buffer.byteLength(r.chunk) }));
    const end = rows.length ? Number(rows.at(-1)!.next_start) : 0;
    const size = readRange(path, (s) => s)?.size ?? 0;
    return new JobLogCopy(job, path, end <= size ? end : 0, kept);
  }

  /** The next chunk: the new whole lines (with `final`, a last partial line too), scrubbed; null when nothing is new or it is not yet time. */
  next(final: boolean, nowMs: number): Chunk | null {
    if (!final && nowMs - this.#lastAt < JOB_LOG_FLUSH_MS) return null;
    this.#lastAt = nowMs;
    // More new text than the cap keeps: only its end is read.
    const r = readRange(this.#path, (size) => Math.max(this.#offset, size - JOB_LOG_KEEP_BYTES));
    if (!r || !r.buf.length) return null;
    let buf = r.buf;
    if (r.start > this.#offset) buf = buf.subarray(buf.indexOf(10) + 1);
    const start = r.start + (r.buf.length - buf.length);
    if (!final) {
      const nl = buf.lastIndexOf(10);
      if (nl < 0) return null;
      buf = buf.subarray(0, nl + 1);
    }
    if (!buf.length) return null;
    return { job_id: this.job, seq: this.#seq + 1, start, next_start: start + buf.length, chunk: scrub(buf.toString("utf8").replace(CONTROLS, "")), at: new Date(nowMs).toISOString() };
  }

  /** Records a written chunk; the highest seq to drop once the kept text passes the cap, else 0. */
  wrote(c: Chunk): number {
    this.#seq = c.seq;
    this.#offset = c.next_start;
    this.#kept.push({ seq: c.seq, bytes: Buffer.byteLength(c.chunk) });
    let total = this.#kept.reduce((a, k) => a + k.bytes, 0);
    if (total <= JOB_LOG_KEEP_BYTES + DROP_SLACK_BYTES) return 0;
    let upTo = 0;
    while (this.#kept.length > 1 && total > JOB_LOG_KEEP_BYTES) {
      const k = this.#kept.shift()!;
      total -= k.bytes;
      upTo = k.seq;
    }
    return upTo;
  }
}

/**
 * Copies the new lines of `copies` in one INSERT (and, for the jobs past the cap, one DELETE), after the copies queued
 * before it. `final` copies everything now, partial last lines too, whatever the time since the last copy.
 */
export function copyLogs(db: Db, copies: JobLogCopy[], final = false): Promise<void> {
  const run = queue.then(async () => {
    const nowMs = Date.now();
    const chunks = copies.flatMap((c) => { const n = c.next(final, nowMs); return n ? [{ copy: c, n }] : []; });
    if (!chunks.length) return;
    let stored = chunks;
    try {
      await db.run(db.k.insertInto("job_log_chunks").values(chunks.map(({ n }) => n)));
    } catch (e) {
      // One bad row must not block the others: write each job's chunk on its own.
      if (chunks.length < 2) throw e;
      stored = [];
      for (const c of chunks) {
        try {
          await db.run(db.k.insertInto("job_log_chunks").values(c.n));
          stored.push(c);
        } catch (err) {
          console.error(`job log copy failed for job ${c.copy.job}: ${(err as Error).message}`);
        }
      }
      if (!stored.length) throw e;
    }
    const drops = stored.flatMap(({ copy, n }) => { const upTo = copy.wrote(n); return upTo ? [{ job: copy.job, upTo }] : []; });
    if (drops.length) await db.run(db.k.deleteFrom("job_log_chunks").where((eb) => eb.or(drops.map((d) => eb.and([eb("job_id", "=", d.job), eb("seq", "<=", d.upTo)])))));
  });
  queue = run.catch(() => { /* the caller sees the failure; the next copy reads the lines again */ });
  return run;
}

/** Stored log text is kept this long, apart from the job rows (pruneJobs keeps those 10 years). */
export const JOB_LOG_KEEP_MS = 90 * 24 * 3600_000;

/** Deletes stored log chunks older than JOB_LOG_KEEP_MS, a batch at a time; returns how many. */
export async function pruneJobLogs(db: Db, nowMs: number = Date.now()): Promise<number> {
  const before = new Date(nowMs - JOB_LOG_KEEP_MS).toISOString();
  let n = 0;
  for (;;) {
    const rows = await db.all(db.k.selectFrom("job_log_chunks").select(["job_id", "seq"]).where("at", "<", before).limit(500));
    if (!rows.length) return n;
    await db.run(db.k.deleteFrom("job_log_chunks").where((eb) => eb.or(rows.map((r) => eb.and([eb("job_id", "=", r.job_id), eb("seq", "=", r.seq)])))));
    n += rows.length;
  }
}

/** The last `bytes` of a job's stored log, from whole lines; truncated when earlier text is not shown. */
export async function storedLogTail(db: Db, job: number, bytes: number): Promise<{ log: string; logTruncated: boolean } | null> {
  const parts: string[] = [];
  let size = 0;
  let first: number | null = null;
  for (const c of await db.all(db.k.selectFrom("job_log_chunks").select(["chunk", "start"]).where("job_id", "=", job).orderBy("seq", "desc"))) {
    parts.push(c.chunk);
    size += Buffer.byteLength(c.chunk);
    first = Number(c.start);
    if (size >= bytes) break;
  }
  if (first === null) return null;
  const joined = parts.reverse().join("");
  if (size <= bytes) return { log: joined, logTruncated: first > 0 };
  const cut = Buffer.from(joined).subarray(-bytes).toString("utf8");
  return { log: cut.slice(cut.indexOf("\n") + 1), logTruncated: true };
}
