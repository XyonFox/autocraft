// Counts for the flowchart (docs/dashboard-spec.md, Flow), one node at a time,
// for one repo or every tracked repo. Each count uses the rule of the stage it
// describes: settled labels (src/classify/settled.ts) and the cached Plans
// (item_plans, src/core/plan-read.ts) for every count of what is due or queued.
//
// Counts are flat keys that add up across repos:
//   "<node>.<branch>"         items in a branch
//   "<node>.total"            items in the node
//   "<node>.recent.<label>"   movement in the last 24 hours
//   "edge.<from>.<to>"        items on an edge
//   "x.<name>"                other facts (open decisions, approved repos, ...)
// Keys starting "max." are combined with max, not summed, when repos add up.
// Item-backed branches also return their refs ("repo#n") for the Items
// browser's flow filter.
import type { Compilable, Expression, SqlBool } from "kysely";
import { latestRun, settledOnExpr, settledTable, verdictExpr } from "../classify/settled.ts";
import { isScope } from "../decide/select.ts";
import { cachedQueue, dueActions, dueRefs } from "../core/plan-read.ts";
import type { QueueState } from "../core/plan.ts";
import { commentApprovalOf } from "../act/approval.ts";
import { fixIssueApprovalOf } from "../fixissue/approval.ts";
import { featureOn } from "../build/features.ts";
import { ENDED_STATES, LIVE_STATES } from "../implement/types.ts";
import { approvalOf } from "../labels/approval.ts";
import { mergeApprovalOf } from "../merge/approval.ts";
import { pendingDefects } from "../postmerge/select.ts";
import { trackedNames } from "../repos.ts";
import { scopeAnswers } from "../review/candidates.ts";
import { BACKLOG_KIND } from "../decisions.ts";
import { filterSeq } from "../async-seq.ts";
import type { Db } from "../db/index.ts";
import * as kx from "../db/kx.ts";

export type Counts = Record<string, number>;
export type Refs = Set<string>;
export type NodeCount = { counts: Counts; members: Map<string, Refs> };

const DAY = 24 * 3600_000;
const ref = (r: { repo: string; number: number }) => `${r.repo}#${r.number}`;

/** Scope answers the step that applies them reads (docs/pipeline.md, Decide). */
export const SCOPE_ANSWERS = ["take", "small-extra", "plugin", "close", "need-info", "backlog"] as const;
export const VERDICTS = ["take", "take-fix", "hold", "close"] as const;

type OpenRow = { repo: string; number: number; kind: string; status: string | null; referee_sure: number | null; jev_label: string | null; haiku_label: string | null };

/** What the node counters share for one repo (or all repos) on one connection. */
export class Ctx {
  readonly since: string;
  private openRows?: OpenRow[];
  private openSet?: Refs;
  private answers?: Map<string, string>;
  readonly db: Db;
  readonly repo: string | null;
  /** The classification run every counter uses, resolved once. */
  readonly run: string | null;
  constructor(db: Db, repo: string | null, now: number, run: string | null) {
    this.run = run;
    this.db = db;
    this.repo = repo;
    this.since = new Date(now - DAY).toISOString();
  }
  andRepo(eb: kx.AnyEb, col = "repo"): Expression<SqlBool> {
    return this.repo ? eb(eb.ref(col), "=", this.repo) : eb.and([]);
  }
  /** Restricts a query on `col` to the repo, or to the tracked repos. */
  tracked(eb: kx.AnyEb, col = "repo"): Expression<SqlBool> {
    return this.repo ? eb(eb.ref(col), "=", this.repo) : eb(eb.ref(col), "in", eb.selectFrom("main.tracked_repos").select("name"));
  }
  async repos(): Promise<string[]> {
    return this.repo ? [this.repo] : await trackedNames(this.db);
  }
  /** Rows of `repo, number` as refs. */
  async refs(q: Compilable<{ repo: string; number: number }>): Promise<Refs> {
    return new Set((await this.db.all(q)).map(ref));
  }
  inRepo(k: string): boolean {
    return !this.repo || k.startsWith(`${this.repo}#`);
  }
  async n(q: Compilable<{ n: number }>): Promise<number> {
    return (await this.db.get(q))!.n;
  }
  /** Every open item with its settled status in the current run. */
  async open(): Promise<OpenRow[]> {
    if (!this.openRows) {
      await settledTable(this.db, this.run);
      this.openRows = await this.db.all(this.db.k.selectFrom("items as i")
        .leftJoin("item_settled as s", (j) => j.on((eb) => settledOnExpr(eb, this.run, "s")))
        .select(["i.repo", "i.number", "i.kind", "s.status", "s.referee_sure", "s.jev_label", "s.haiku_label"])
        .where("i.state", "=", "OPEN").where((eb) => this.andRepo(eb, "i.repo")));
    }
    return this.openRows;
  }
  async openRefs(): Promise<Refs> {
    return (this.openSet ??= new Set((await this.open()).map(ref)));
  }
  async scopeAnswers(): Promise<Map<string, string>> {
    return (this.answers ??= await scopeAnswers(this.db));
  }
  /** Items created or closed since `since`. Over every repo the named date index reads only the last day; for one repo its own rows are fewer. */
  async recentItems(col: "created_at" | "closed_at"): Promise<number> {
    const idx = this.repo ? null : col === "created_at" ? "items_created" : "items_closed";
    return await this.n(this.db.k.selectFrom(kx.indexedBy(this.db, "main.items", idx, "it")).select((eb) => eb.fn.countAll<number>().as("n"))
      .where(col, ">=", this.since).$if(col === "closed_at", (q) => q.where("state", "<>", "OPEN")).where((eb) => this.tracked(eb)));
  }
}

async function synced(c: Ctx): Promise<NodeCount> {
  const open = await c.open();
  const prs = open.filter((r) => r.kind === "pr").length;
  return {
    counts: {
      "synced.issues": open.length - prs, "synced.prs": prs, "synced.total": open.length,
      "synced.recent.new": await c.recentItems("created_at"), "synced.recent.closed": await c.recentItems("closed_at"),
      "edge.synced.dupes": open.length,
    },
    members: new Map(),
  };
}

async function classify(c: Ctx): Promise<NodeCount> {
  const members = new Map<string, Refs>(["settled", "waiting", "referee", "disagree", "unclassified"].map((k) => [`classify.${k}`, new Set()]));
  const by: Counts = { settled: 0, waiting: 0, referee: 0, disagree: 0, unclassified: 0 };
  for (const r of await c.open()) {
    // A label disagreement waits on the referee (Sonnet, then Opus when Sonnet is unsure); scope-vs-scope goes to Decide.
    const toReferee = r.status === "disagree" && !(isScope(r.jev_label) && isScope(r.haiku_label));
    const k = toReferee ? "referee" : (r.status ?? "unclassified");
    by[k]!++;
    members.get(`classify.${k}`)!.add(ref(r));
  }
  const run = c.run;
  const recent = run ? await c.n(c.db.k.selectFrom("main.classifications").select((eb) => eb.fn.countAll<number>().as("n"))
    .where("run_id", "=", run).where((eb) => verdictExpr(eb)).where("at", ">=", c.since)
    .where("repo", "in", (eb) => eb.selectFrom("main.tracked_repos").select("name")).where((eb) => c.andRepo(eb))) : 0;
  return {
    counts: {
      ...Object.fromEntries(Object.entries(by).map(([k, v]) => [`classify.${k}`, v])),
      "classify.total": (await c.open()).length, "classify.recent.classified": recent, "edge.decide.labels": by.settled!, "edge.classify.priority": (await c.open()).length,
    },
    members,
  };
}

async function decide(c: Ctx): Promise<NodeCount> {
  const rows = await c.db.all(c.db.k.selectFrom("decisions").select(["repo", "number", "items", "answer", "asked_at", "answered_at"]).where("kind", "=", "scope"));
  const openRefs = await c.openRefs();
  const waiting: Refs = new Set();
  let openDecisions = 0, asked = 0, answered = 0;
  for (const d of rows) {
    let refs: string[] = [];
    if (d.number !== null) refs = [`${d.repo}#${d.number}`];
    else try { refs = (JSON.parse(d.items ?? "[]") as { repo: string; number: number }[]).map(ref); } catch { /* malformed batch */ }
    refs = refs.filter((r) => c.inRepo(r));
    if (!refs.length) continue;
    if (d.asked_at >= c.since) asked++;
    if (d.answered_at && d.answered_at >= c.since) answered++;
    if (d.answer !== null) continue;
    openDecisions++;
    for (const r of refs) if (openRefs.has(r)) waiting.add(r);
  }
  const byAnswer = new Map<string, Refs>(SCOPE_ANSWERS.map((a) => [a, new Set()]));
  for (const [r, a] of await c.scopeAnswers()) if (openRefs.has(r) && !waiting.has(r) && c.inRepo(r)) byAnswer.get(a)?.add(r);
  // An item a person put on the Backlog without a question counts there, whatever its scope answer was.
  for (const b of await c.db.all(c.db.k.selectFrom("decisions").select(["repo", "number"]).where("kind", "=", BACKLOG_KIND).where("answer", "=", "backlog"))) {
    if (b.repo === null || b.number === null) continue;
    const r = ref({ repo: b.repo, number: b.number });
    if (!openRefs.has(r) || waiting.has(r) || !c.inRepo(r)) continue;
    for (const set of byAnswer.values()) set.delete(r);
    byAnswer.get("backlog")!.add(r);
  }
  const notAsked = await dueRefs(c.db, "decide", { bucket: "new", ...(c.repo ? { repo: c.repo } : {}) });
  const members = new Map<string, Refs>([["decide.not-asked", notAsked], ["decide.open", waiting], ...[...byAnswer].map(([k, v]) => [`decide.${k}`, v] as [string, Refs])]);
  const total = [...members.values()].reduce((a, s) => a + s.size, 0);
  return {
    counts: {
      ...Object.fromEntries([...members].map(([k, v]) => [k, v.size])),
      "decide.total": total, "decide.recent.asked": asked, "decide.recent.answered": answered,
      "x.openDecisions": openDecisions, "edge.classify.decide": total, "edge.priority.decide": (await c.open()).length,
    },
    members,
  };
}

async function review(c: Ctx): Promise<NodeCount> {
  const candidates = await dueRefs(c.db, "review", c.repo ? { repo: c.repo } : {});
  const last = await c.db.all(c.db.k
    .with("last", (qb) => qb.selectFrom("final_reviews").select((eb) => ["repo", "number", "verdict",
      eb.fn.agg<number>("row_number", []).over((o) => o.partitionBy(["repo", "number"]).orderBy("at", "desc").orderBy("head_oid", "desc")).as("k")])
      .where((eb) => c.andRepo(eb)))
    .selectFrom("last as l").innerJoin("items as i", (j) => j.onRef("i.repo", "=", "l.repo").onRef("i.number", "=", "l.number"))
    .select(["i.repo", "i.number", "l.verdict"]).$narrowType<{ verdict: string }>()
    .where("l.k", "=", 1).where("i.state", "=", "OPEN"));
  // A PR waiting for a review at its new head counts there, not under its old verdict.
  const by = new Map<string, Refs>(VERDICTS.map((v) => [v, new Set()]));
  for (const r of last) if (!candidates.has(ref(r))) by.get(r.verdict)?.add(ref(r));
  const members = new Map<string, Refs>([["review.not-reviewed", candidates], ...[...by].map(([k, v]) => [`review.${k}`, v] as [string, Refs])]);
  const all = [...members.values()].flatMap((s) => [...s]);
  const takeFix = by.get("take-fix")!.size;
  return {
    counts: {
      ...Object.fromEntries([...members].map(([k, v]) => [k, v.size])),
      "review.total": all.length,
      "review.recent.reviewed": await c.n(c.db.k.selectFrom("final_reviews").select((eb) => eb.fn.countAll<number>().as("n"))
        .where("at", ">=", c.since).where((eb) => c.andRepo(eb))),
      "edge.labels.review": all.length, "edge.review.verify": by.get("take")!.size, "edge.review.fixup": takeFix,
    },
    members,
  };
}

/** Verification states shown as branches of the Verify node; `stale` ones are not counted. */
export const VERIFY_BRANCHES = ["planned", "running", "passed", "ci-enough", "failed", "error"] as const;

async function verify(c: Ctx): Promise<NodeCount> {
  const by = new Map<string, Refs>([["verify.waiting", new Set()], ...VERIFY_BRANCHES.map((s) => [`verify.${s}`, new Set()] as [string, Refs])]);
  const tracked = new Set(await c.repos());
  for (const p of await dueActions(c.db, "verify", { ...(c.repo ? { repo: c.repo } : {}), bucket: "plan" }))
    if (tracked.has(p.repo)) by.get("verify.waiting")!.add(ref(p));
  // Verifications at the open PR's current head (same head rule as the queue); primary-key joins, no scan of closed PRs' rows.
  const rows = await c.db.all(c.db.k.selectFrom("main.verifications as v")
    .innerJoin("main.items as i", (j) => j.onRef("i.repo", "=", "v.repo").onRef("i.number", "=", "v.number").on("i.state", "=", "OPEN"))
    .leftJoin("main.pr_checks as c", (j) => j.onRef("c.repo", "=", "v.repo").onRef("c.number", "=", "v.number"))
    .select(["v.repo", "v.number", "v.state"])
    .where("v.state", "<>", "stale").where("v.repo", "in", (eb) => eb.selectFrom("main.tracked_repos").select("name")).where((eb) => c.andRepo(eb, "v.repo"))
    .where((eb) => eb("v.head_sha", "=", eb.fn.coalesce(
      eb.case().when(eb.and([eb("c.fetched_at", "is not", null), eb.or([eb("i.updated_at", "is", null), eb("c.fetched_at", ">", eb.ref("i.updated_at"))])])).then(eb.ref("c.head_oid")).end(),
      "i.head_oid", "c.head_oid"))));
  for (const r of rows) by.get(`verify.${r.state}`)?.add(ref(r));
  const total = [...by.values()].reduce((a, s) => a + s.size, 0);
  const passing = by.get("verify.passed")!.size + by.get("verify.ci-enough")!.size;
  return {
    counts: {
      ...Object.fromEntries([...by].map(([k, v]) => [k, v.size])),
      "verify.total": total,
      "verify.recent.finished": await c.n(c.db.k.selectFrom("main.verifications").select((eb) => eb.fn.countAll<number>().as("n")).where("finished_at", ">=", c.since)
        .where("state", "in", ["passed", "ci-enough", "failed", "error"]).where("repo", "in", (eb) => eb.selectFrom("main.tracked_repos").select("name")).where((eb) => c.andRepo(eb))),
      "edge.verify.merge": passing,
    },
    members: by,
  };
}

async function merge(c: Ctx): Promise<NodeCount> {
  const tracked = new Set(await trackedNames(c.db));
  const queue = (await cachedQueue(c.db, c.repo ? { repo: c.repo } : {})).filter((e) => tracked.has(e.repo));
  const approvedRepos = new Set(await filterSeq([...new Set(queue.map((e) => e.repo))], async (r) => await mergeApprovalOf(c.db, r)));
  const ready = queue.filter((e) => e.eligible);
  const of = (...states: QueueState[]): Refs => new Set(queue.filter((e) => states.includes(e.state)).map(ref));
  const members = new Map<string, Refs>([
    ["merge.eligible", new Set(ready.filter((e) => approvedRepos.has(e.repo)).map(ref))],
    ["merge.unapproved", new Set(ready.filter((e) => !approvedRepos.has(e.repo)).map(ref))],
    ["merge.in-batch", of("batch-checking", "in-batch")],
    ["merge.batch-failed", of("batch-failed")],
    ["merge.needs-decision", of("needs-decision", "held-duplicate", "held-backlog")],
    ["merge.waiting-ci", of("waiting-ci")],
    ["merge.waiting-main-ci", of("waiting-main-ci")],
    ["merge.ci-approval", new Set(queue.filter((e) => e.state === "ci-approval" && !e.runsBlocked).map(ref))],
    ["merge.ci-approval-person", new Set(queue.filter((e) => e.state === "ci-approval" && e.runsBlocked).map(ref))],
    ...(["ci-failed", "conflict", "needs-fix-up", "waiting-premerge", "waiting-verify", "verify-failed", "verify-error", "declined"] as const).map((s) => [`merge.${s}`, of(s)] as [string, Refs]),
    ["merge.merged", await c.refs(c.db.k.selectFrom("merges").select(["repo", "number"]).distinct().where("outcome", "=", "merged").where("at", ">=", c.since).where((eb) => c.andRepo(eb)))],
  ]);
  return {
    counts: {
      ...Object.fromEntries([...members].map(([k, v]) => [k, v.size])),
      "merge.total": queue.length - of("merged").size,
      "merge.recent.merged": await c.n(c.db.k.selectFrom((eb) => eb.selectFrom("merges").select(["repo", "number"]).distinct().where("outcome", "=", "merged").where("at", ">=", c.since)
        .where((e) => c.andRepo(e)).as("m")).select((eb) => eb.fn.countAll<number>().as("n"))),
      "merge.recent.tries": await c.n(c.db.k.selectFrom("merges").select((eb) => eb.fn.countAll<number>().as("n")).where("at", ">=", c.since).where((eb) => c.andRepo(eb))),
      "x.mergedAll": of("merged").size,
    },
    members,
  };
}

async function labels(c: Ctx): Promise<NodeCount> {
  const repos = c.repo ? [c.repo] : await trackedNames(c.db);
  const approved = new Set(await filterSeq(repos, async (r) => await approvalOf(c.db, r)));
  const openRefs = await c.openRefs();
  const labelled = new Set((await c.db.all(c.db.k.selectFrom("stages").select(["repo", "number"]).distinct().where("stage", "=", "labeled").where((eb) => c.andRepo(eb))))
    .map(ref).filter((r) => openRefs.has(r)));
  const pending: Refs = new Set(), unapproved: Refs = new Set();
  for (const r of await c.open()) {
    if (r.status !== "settled" || labelled.has(ref(r))) continue;
    (approved.has(r.repo) ? pending : unapproved).add(ref(r));
  }
  const members = new Map<string, Refs>([["labels.labelled", labelled], ["labels.pending", pending], ["labels.unapproved", unapproved]]);
  return {
    counts: {
      ...Object.fromEntries([...members].map(([k, v]) => [k, v.size])),
      "labels.total": labelled.size + pending.size + unapproved.size,
      "labels.recent.labelled": await c.n(c.db.k.selectFrom("stages").select((eb) => eb.fn.countAll<number>().as("n")).where("stage", "=", "labeled").where("at", ">=", c.since).where((eb) => c.andRepo(eb))),
      "x.repos": repos.length, "x.approved": approved.size,
    },
    members,
  };
}

const sizeOf = (m: Map<string, Refs>, key: string) => m.get(key)?.size ?? 0;

/** Duplicate pairs and the decisions about them (src/dupes). Pairs count by their open items. */
async function dupes(c: Ctx): Promise<NodeCount> {
  const members = new Map<string, Refs>(["checking", "waiting", "held", "closed"].map((k) => [`dupes.${k}`, new Set()]));
  // Unjudged pairs whose items still have the hashed text; duplicate_pairs_open reads only the rows without a verdict.
  // CROSS JOIN fixes the order: with a single repo the planner otherwise scans both items tables for the repo and probes the pairs
  // last, which took minutes on a large repo.
  const pairs = await c.db.all(c.db.k.selectFrom("main.duplicate_pairs as p").crossJoin("main.items as ia").crossJoin("main.items as ib")
    .select(["p.repo", "p.a", "p.b", "ia.state as sa", "ib.state as sb"])
    .whereRef("ia.repo", "=", "p.repo").whereRef("ia.number", "=", "p.a").whereRef("ia.embed_hash", "=", "p.a_hash")
    .whereRef("ib.repo", "=", "p.repo").whereRef("ib.number", "=", "p.b").whereRef("ib.embed_hash", "=", "p.b_hash")
    .where("p.verdict", "is", null).where((eb) => eb.or([eb("ia.state", "=", "OPEN"), eb("ib.state", "=", "OPEN")])).where((eb) => c.tracked(eb, "p.repo")));
  for (const p of pairs) {
    if (p.sa === "OPEN") members.get("dupes.checking")!.add(`${p.repo}#${p.a}`);
    if (p.sb === "OPEN") members.get("dupes.checking")!.add(`${p.repo}#${p.b}`);
  }
  // Open decisions and answered close (the held-out items, see src/dupes/held.ts); decisions_open covers both.
  const rows = await c.db.all(c.db.k.selectFrom("main.decisions as d")
    .innerJoin("main.items as i", (j) => j.onRef("i.repo", "=", "d.repo").onRef("i.number", "=", "d.number"))
    .select(["i.repo", "i.number", "d.answer", "i.state"])
    .where("d.kind", "=", "duplicate").where((eb) => eb.not(kx.like(c.db, "d.id", eb.val("%:dupegroup:%"))))
    .where((eb) => eb.or([eb("d.answer", "is", null), eb("d.answer", "=", "close")])).where((eb) => c.tracked(eb, "d.repo")));
  for (const r of rows) members.get(r.answer === null ? (r.state === "OPEN" ? "dupes.waiting" : "") : r.state === "OPEN" ? "dupes.held" : "dupes.closed")?.add(ref(r));
  // Group decisions hold the members they close.
  const groupRows = await c.db.all(c.db.k.selectFrom("main.dupe_group_items as gi")
    .innerJoin("main.decisions as d", "d.id", "gi.decision_id")
    .innerJoin("main.items as i", (j) => j.onRef("i.repo", "=", "gi.repo").onRef("i.number", "=", "gi.number"))
    .select(["gi.repo", "gi.number", "d.answer", "i.state"])
    .where("gi.role", "=", "close").where("d.kind", "=", "duplicate")
    .where((eb) => eb.or([eb("d.answer", "is", null), eb("d.answer", "=", "accept"), kx.like(c.db, "d.answer", eb.val("keep:%"))])).where((eb) => c.tracked(eb, "gi.repo")));
  for (const r of groupRows) members.get(r.answer === null ? (r.state === "OPEN" ? "dupes.waiting" : "") : r.state === "OPEN" ? "dupes.held" : "dupes.closed")?.add(ref(r));
  const open = (await c.open()).length;
  return {
    counts: {
      ...Object.fromEntries([...members].map(([k, v]) => [k, v.size])),
      "dupes.total": open, "x.pairsUnchecked": pairs.length,
      "edge.dupes.classify": Math.max(0, open - sizeOf(members, "dupes.waiting") - sizeOf(members, "dupes.held")),
    },
    members,
  };
}

/** Priority scores (src/priority) and the work rank (src/rank) of the open, embedded items. */
async function priority(c: Ctx): Promise<NodeCount> {
  const members = new Map<string, Refs>(["scored", "todo", "unsure"].map((k) => [`priority.${k}`, new Set()]));
  const rows = await c.db.all(c.db.k.selectFrom("main.items as i")
    .leftJoin("main.item_priority as p", (j) => j.onRef("p.repo", "=", "i.repo").onRef("p.number", "=", "i.number"))
    .select((eb) => ["i.repo", "i.number", eb.case().when(eb.or([eb("p.repo", "is", null), eb("p.text_hash", "<>", eb.ref("i.embed_hash"))])).then("todo")
      .when("p.model", "=", "jev-unsure").then("unsure").else("scored").end().as("s")])
    .where("i.state", "=", "OPEN").where("i.embed_hash", "is not", null).where((eb) => c.tracked(eb, "i.repo")));
  for (const r of rows) members.get(`priority.${r.s}`)!.add(ref(r));
  const rank = (await c.db.get(c.db.k.selectFrom("main.item_rank")
    .select((eb) => [eb.fn.countAll<number>().as("n"), eb.fn.coalesce(eb.fn.max<number | null>("score"), eb.lit(0)).as("top")]).where((eb) => c.tracked(eb))))!;
  return {
    counts: {
      ...Object.fromEntries([...members].map(([k, v]) => [k, v.size])),
      "priority.total": rows.length, "x.ranked": rank.n, "max.topRank": rank.top,
    },
    members,
  };
}

/** Where an implementation's state counts on the Implement node. */
const IMPLEMENT_NODE: Partial<Record<string, string>> = { asked: "asked", stuck: "stuck", "pr-opened": "pr-opened", ...Object.fromEntries([...LIVE_STATES].map((s) => [s, "working"])) };

/** The Implement stage (id fix): queued issues by where their implementation stands (src/implement). */
async function fix(c: Ctx): Promise<NodeCount> {
  const members = new Map<string, Refs>(["queued", "asked", "working", "pr-opened", "stuck", "off"].map((k) => [`fix.${k}`, new Set()]));
  const off = new Set<string>();
  for (const repo of await c.repos()) {
    if (await fixIssueApprovalOf(c.db, repo) && await featureOn(c.db, "implement", repo)) for (const r of await dueRefs(c.db, "implement", { repo, bucket: "plan" })) members.get("fix.queued")!.add(r);
    else off.add(repo);
  }
  for (const r of await c.db.all(c.db.k.selectFrom("main.fast_track as f")
    .innerJoin("main.items as i", (j) => j.onRef("i.repo", "=", "f.repo").onRef("i.number", "=", "f.number").on("i.state", "=", "OPEN").on("i.kind", "=", "issue"))
    .select(["f.repo", "f.number"]).where((eb) => c.tracked(eb, "f.repo")))) {
    if (off.has(r.repo)) members.get("fix.off")!.add(ref(r));
  }
  const rows = await c.db.all(c.db.k.selectFrom("main.implementations as m")
    .innerJoin("main.items as i", (j) => j.onRef("i.repo", "=", "m.repo").onRef("i.number", "=", "m.number").on("i.state", "=", "OPEN"))
    .select(["m.repo", "m.number", "m.state"]).where("m.state", "not in", [...ENDED_STATES]).where((eb) => c.tracked(eb, "m.repo")));
  for (const r of rows) {
    const node = IMPLEMENT_NODE[r.state];
    if (!node) continue;
    for (const s of members.values()) s.delete(ref(r));
    members.get(`fix.${node}`)!.add(ref(r));
  }
  const total = [...members.values()].reduce((a, s) => a + s.size, 0);
  return {
    counts: { ...Object.fromEntries([...members].map(([k, v]) => [k, v.size])), "fix.total": total, "edge.labels.fix": total, "edge.fix.review": sizeOf(members, "fix.pr-opened") },
    members,
  };
}

/** Fix-up: take-fix PRs by whether the step may push (src/fixup). */
async function fixup(c: Ctx): Promise<NodeCount> {
  const members = new Map<string, Refs>(["fix", "pushed", "hold", "unapproved", "asked", "cifix-queued", "cifix-rerun", "cifix-pushed", "cifix-failed"].map((k) => [`fixup.${k}`, new Set()]));
  const hold = members.get("fixup.hold")!;
  const tracked = new Set(await c.repos());
  // The buckets are part of each PR's cached view.
  const fixupOf = kx.jsonText(c.db, "view", "$.fixup"), cifixOf = kx.jsonText(c.db, "view", "$.cifix");
  const cached = await c.db.all(c.db.k.selectFrom("main.item_plans").select(["repo", "number", fixupOf.as("fixup"), cifixOf.as("cifix")])
    .where("kind", "=", "pr").where((eb) => eb.or([eb(fixupOf, "is not", null), eb(cifixOf, "is not", null)])).where((eb) => c.andRepo(eb)));
  const facts = cached.filter((f) => tracked.has(f.repo));
  for (const f of facts) {
    if (!f.fixup) continue;
    if (f.fixup === "fix" || f.fixup === "unapproved" || f.fixup === "asked") members.get(`fixup.${f.fixup}`)!.add(ref(f));
    else hold.add(ref(f));
  }
  const waiting = hold.size + sizeOf(members, "fixup.fix") + sizeOf(members, "fixup.unapproved") + sizeOf(members, "fixup.asked");
  const pushed = members.get("fixup.pushed")!;
  for (const r of await c.refs(c.db.k.selectFrom("main.fixups as f")
    .innerJoin("main.items as i", (j) => j.onRef("i.repo", "=", "f.repo").onRef("i.number", "=", "f.number").on("i.state", "=", "OPEN"))
    .select(["f.repo", "f.number"]).distinct().where("f.state", "=", "pushed").where((eb) => c.tracked(eb, "f.repo")))) if (![...members.values()].some((s) => s !== pushed && s.has(r))) pushed.add(r);
  // CI fix: PRs whose gate check fails, by what the step does next; a pushed CI fix counts like a pushed fix-up.
  for (const f of facts) if (f.cifix) members.get(`fixup.${f.cifix}`)!.add(ref(f));
  for (const r of await c.refs(c.db.k.selectFrom("main.ci_fixes as f")
    .innerJoin("main.items as i", (j) => j.onRef("i.repo", "=", "f.repo").onRef("i.number", "=", "f.number").on("i.state", "=", "OPEN"))
    .select(["f.repo", "f.number"]).distinct().where("f.state", "=", "pushed").where((eb) => c.tracked(eb, "f.repo")))) members.get("fixup.cifix-pushed")!.add(r);
  return {
    counts: {
      ...Object.fromEntries([...members].map(([k, v]) => [k, v.size])),
      "fixup.total": waiting + pushed.size, "edge.fixup.review": pushed.size,
      "fixup.recent.pushed": await c.n(c.db.k.selectFrom("main.fixups").select((eb) => eb.fn.countAll<number>().as("n")).where("state", "=", "pushed").where("created_at", ">=", c.since).where((eb) => c.tracked(eb))),
    },
    members,
  };
}

/** Act: comments and closes that follow an operator's answer (src/act). */
async function act(c: Ctx): Promise<NodeCount> {
  const members = new Map<string, Refs>(["todo", "failed", "unapproved", "done"].map((k) => [`act.${k}`, new Set()]));
  for (const repo of await c.repos()) {
    const failed = new Set((await c.db.all(c.db.k.selectFrom("main.actions").select(["decision_id", "number"]).where("repo", "=", repo).where("state", "=", "failed")))
      .map((r) => `${r.decision_id}\0${r.number}`));
    const approved = await commentApprovalOf(c.db, repo) !== null;
    for (const p of (await dueActions(c.db, "act", { repo })).filter((x) => !x.action.params?.ask))
      members.get(!approved ? "act.unapproved" : failed.has(`${p.action.params!.decisionId}\0${p.number}`) ? "act.failed" : "act.todo")!.add(`${repo}#${p.number}`);
  }
  const done = await c.n(c.db.k.selectFrom("main.actions").select((eb) => eb.fn.countAll<number>().as("n")).where("state", "=", "done").where("created_at", ">=", c.since).where((eb) => c.tracked(eb)));
  const waiting = sizeOf(members, "act.todo") + sizeOf(members, "act.failed") + sizeOf(members, "act.unapproved");
  return {
    counts: {
      ...Object.fromEntries([...members].map(([k, v]) => [k, v.size])),
      "act.done": done, "act.total": waiting, "edge.decide.act": waiting,
    },
    members,
  };
}

/** Post-merge review (src/postmerge): merged PRs to review, defects to file, and what the reviews of the last 24 hours led to. */
async function postmerge(c: Ctx): Promise<NodeCount> {
  const members = new Map<string, Refs>(["todo", "to-file", "unapproved", "filed", "nothing"].map((k) => [`postmerge.${k}`, new Set()]));
  const toFile = new Set<string>();
  for (const repo of await c.repos()) {
    for (const r of await dueRefs(c.db, "postmerge", { repo })) members.get("postmerge.todo")!.add(r);
    const approved = await commentApprovalOf(c.db, repo) !== null;
    for (const d of await pendingDefects(c.db, repo)) {
      toFile.add(`${repo}#${d.number}`);
      members.get(approved ? "postmerge.to-file" : "postmerge.unapproved")!.add(`${repo}#${d.number}`);
    }
  }
  const recent = await c.refs(c.db.k.selectFrom("main.postmerge_reviews").select(["repo", "number"]).where("error", "is", null).where("at", ">=", c.since).where((eb) => c.andRepo(eb)));
  const filed = await c.refs(c.db.k.selectFrom("main.postmerge_followups").select(["repo", "number"]).distinct().where("state", "=", "filed").where("at", ">=", c.since).where((eb) => c.andRepo(eb)));
  for (const r of filed) members.get("postmerge.filed")!.add(r);
  for (const r of recent) if (!filed.has(r) && !toFile.has(r)) members.get("postmerge.nothing")!.add(r);
  return {
    counts: {
      ...Object.fromEntries([...members].map(([k, v]) => [k, v.size])),
      "postmerge.total": sizeOf(members, "postmerge.todo") + toFile.size,
      "postmerge.recent.reviewed": recent.size,
      "edge.merge.postmerge": sizeOf(members, "postmerge.todo"),
      "edge.postmerge.fix": sizeOf(members, "postmerge.filed"),
    },
    members,
  };
}

export const COUNTERS = { synced, dupes, classify, priority, decide, labels, review, fix, fixup, verify, merge, postmerge, act } as const;
export type CountedNode = keyof typeof COUNTERS;

/** Every count for one repo, or for every tracked repo when `repo` is null. */
export async function countAll(db: Db, repo: string | null, now = Date.now(), run?: string | null): Promise<NodeCount> {
  if (run === undefined) run = await latestRun(db);
  const c = new Ctx(db, repo, now, run);
  const out: NodeCount = { counts: {}, members: new Map() };
  for (const f of Object.values(COUNTERS)) {
    const r = await f(c);
    Object.assign(out.counts, r.counts);
    for (const [k, v] of r.members) out.members.set(k, v);
  }
  return out;
}

/** The refs of one item-backed branch (`<node>.<branch>`) over every tracked repo, or null when the key names none. */
export async function branchMembers(db: Db, key: string): Promise<Refs | null> {
  const node = key.split(".")[0] as CountedNode;
  if (!Object.hasOwn(COUNTERS, node)) return null;
  return (await COUNTERS[node](new Ctx(db, null, Date.now(), await latestRun(db)))).members.get(key) ?? null;
}
