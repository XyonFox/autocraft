// Decisions over JSON: the queue, answers (operator only), feedback, claims
// and the thread. Feedback (picks and comments) is display-only: nothing the pipeline
// reads (src/decisions.ts `actionable`) touches their tables. Feedback reaches a prompt only when an operator includes
// it in a conversation (`<included>` blocks, src/conversations/store.ts).
import { preparedEarlier, preparing, prepareStates, type PrepareState } from "../../decide/prepared.ts";
import { priorityOf } from "../../priority/read.ts";
import type { Level } from "../../priority/score.ts";
import type {
  Claim, Classification, Decision, ExecState, ItemShown, DecisionComment, DecisionKind, DecisionList, DuplicateMember, ItemState, OpenDecisionCounts, Opinion, QueueSlot, RelatedItem, Relation,
} from "../../../web/src/api/types.ts";
import { resetGroupAnswer } from "../../dupes/decide.ts";
import { NOTE_MAX } from "../../conversations/types.ts";
import { conversationOf, insertTurn, systemBlock } from "../../conversations/store.ts";
import { blocksFacts, blocksIndex } from "../../review/blockers.ts";
import { BACKLOG, BACKLOG_KIND, BACKLOG_QUESTION, backlogDecisionId, HANDLED, isBacklog, snoozedExpr, replaceProposedCards, WITHDRAWN_OPTION, backlogOpen, exceptionsOf, factsOf, getDecision, isBatch, itemsOf, KINDS, optionsOf, reposOf, type DecisionRow, type Exception } from "../../decisions.ts";
import { commentPreview, writeEffect } from "../../act/index.ts";
import { optionTextsMany, optionTextsOf, textsFor, textsHash } from "../../dupes/texts.ts";
import { audit } from "../audit.ts";
import { mayRunRepo } from "../repo-access.ts";
import { heldLookup, type Held } from "../../runner/autopilot.ts";
import { queuePlace, queueReason } from "../../conversations/queue.ts";
import { initExecState, resetActTries, retryDecision } from "../../github/user-lane.ts";
import { inActiveBatch, mergeRunning } from "../../preapproval/index.ts";
import { clearDispute, DISPUTED, heldOpenExpr, lockLiveExpr, isDisputed, openDispute, queuedLock, lockMessage, settlingBy } from "../../answers/hold.ts";
import { randomToken } from "../server/crypto.ts";
import { HttpError, type RateLimiter } from "../server/util.ts";
import type { Req } from "../types.ts";
import { areaNameOf } from "../../areas/store.ts";
import { queueSlots } from "../../rank/position.ts";
import { allVisibleRepos, hiddenRepos, needVisible, visibleRepos } from "./visible.ts";
import { backlogHeld } from "../../dupes/held.ts";
import { filtered, hiddenTestExpr, type HiddenRepos } from "../../hidden-sql.ts";
import { pickDecision, shortRepo } from "../../../web/src/lib/links.ts";
import { canSeeInternals } from "./staff.ts";
import { canSettleCached, canSettleExpr, requireSettle, settleRefusal, settledBy, settleMemo, type SettleMemo } from "../../settlers.ts";
import { DEFAULT_BAND } from "../../rank/level.ts";
import { isQuiet, quietFixPrs } from "../../security/quiet.ts";

export const CLAIM_TTL = 10 * 60_000;
const COMMENT_MAX = 4000;
/** Hides every repo: the filter for a viewer whose visible repos were not read. */
const ALL_HIDDEN: HiddenRepos = Object.assign([], { visible: [] });

export const isOperator = (req: Req): boolean => req.user.role === "operator";

export { itemState, shownLookup, withShown } from "./shown.ts";
import { HEAD_FACT } from "../../merge/queue.ts";
import { itemState, shownMap, withShown } from "./shown.ts";
import { slugOf } from "../../owners.ts";
import { mapSeq, someSeq } from "../../async-seq.ts";
import type { AliasableExpression, Expression, ExpressionBuilder, NotNull, SqlBool } from "kysely";
import type { DB } from "../../db/types.ts";
import type { Db } from "../../db/index.ts";
import * as kx from "../../db/kx.ts";
import type { Eb } from "../../db/kx.ts";

const json = <T>(s: string | null | undefined, d: T): T => {
  if (!s) return d;
  try {
    return JSON.parse(s) as T;
  } catch {
    return d;
  }
};

// A decision that covers a repo the requester may not see does not exist for them.
async function needDecision(db: Db, req: Req, id: string): Promise<DecisionRow> {
  const d = await getDecision(db, id);
  if (!d) throw new HttpError(404, `no decision ${id}`);
  const visible = new Set(await visibleRepos(db, req));
  if (reposOf(d).some((r) => !visible.has(r))) throw new HttpError(404, `no decision ${id}`);
  return d;
}

function body(req: Req): Record<string, unknown> {
  const b = req.json;
  if (b === undefined) return {};
  if (!b || typeof b !== "object" || Array.isArray(b)) throw new HttpError(400, "the body must be a JSON object");
  return b as Record<string, unknown>;
}

function optionalText(v: unknown, max: number, what: string): string | null {
  if (v === undefined || v === null) return null;
  if (typeof v !== "string") throw new HttpError(400, `${what} must be a string`);
  const t = v.trim();
  if (t.length > max) throw new HttpError(400, `${what} is longer than ${max} characters`);
  return t || null;
}

// GitHub write on every repo the decision covers (always the live check on a
// POST: the server re-checks a cached value older than a minute).
async function blockedRepos(req: Req, d: DecisionRow): Promise<string[]> {
  const repos = reposOf(d);
  const ok = await Promise.all(repos.map((r) => req.user.canWrite(r).catch(() => false)));
  return repos.filter((_, i) => !ok[i]);
}

/** An operator settles with write access on every repo; a maintainer only under the rule of src/settlers.ts. */
const requireRepoSettler = (db: Db, req: Req, repos: string[]): Promise<void> => requireSettle(db, req.user, repos, "settles decisions");

/** `requireRepoSettler` on the decision's repos. `own`: the decision's answer must be this login's (undo, retry, reopen), unless an operator asks. */
async function requireSettler(db: Db, req: Req, d: DecisionRow, own = false): Promise<void> {
  await requireRepoSettler(db, req, reposOf(d));
  // Someone who can no longer settle has an orphaned answer; any settler takes it back. A dispute does not orphan it.
  if (own && !isOperator(req) && d.answered_by?.toLowerCase() !== req.user.login.toLowerCase() && await settledBy(db, { ...d, exec_state: null })) throw new HttpError(403, "only an operator takes back someone else's answer");
}

/** Snooze, wake and prepare again: whoever runs every repo the decision covers (src/dash/repo-access.ts). A decision with no repo is the operator's. */
async function requireRunner(req: Req, d: DecisionRow): Promise<void> {
  const repos = reposOf(d);
  const ok = await Promise.all(repos.map((r) => mayRunRepo(req.user, r)));
  if (req.user.role === "viewer" || (!isOperator(req) && repos.length === 0)) throw new HttpError(403, "no write access");
  const blocked = repos.filter((_, i) => !ok[i]);
  if (blocked.length) throw new HttpError(403, `no write access to ${blocked.map((r) => slugOf(r)).join(", ")}`);
}

// ---- reading -----------------------------------------------------------

type OpinionRow = { id: string; login: string; option: string | null; note: string | null; at: string; permission: string; via: string | null };
const toOpinion = (o: OpinionRow): Opinion => ({
  id: o.id, login: o.login, option: o.option, note: o.note, at: o.at, permission: o.permission === "write" ? "write" : "none", ...(o.via === "mcp" ? { via: "mcp" as const } : {}),
});

type ClassRow = {
  classifier: string; at: string; type_label: string | null; want: string | null; reference: string | null;
  reason: string | null; dup_of: number | null;
};

export async function classificationsOf(db: Db, repo: string, n: number): Promise<Classification[]> {
  // Latest verdict per classifier, picked here: a correlated MAX(at) subquery makes SQLite walk the (classifier, at) index.
  return latestClassifications(await db.all(db.k.selectFrom("classifications").select(["classifier", "at", "type_label", "want", "reference", "reason", "dup_of"])
    .where("repo", "=", repo).where("number", "=", n).orderBy("classifier")));
}

function latestClassifications(all: ClassRow[]): Classification[] {
  const latest = new Map<string, string>();
  for (const r of all) if (!latest.has(r.classifier) || r.at > latest.get(r.classifier)!) latest.set(r.classifier, r.at);
  const rows = all.filter((r) => r.at === latest.get(r.classifier));
  return rows.map((r) => ({
    classifier: r.classifier as Classification["classifier"], at: r.at,
    typeLabel: r.type_label as Classification["typeLabel"], want: (r.want ?? "unclear") as Classification["want"],
    reference: r.reference, reason: r.reason ?? "", dupOf: r.dup_of,
  }));
}

// The two relations that have no index, read once for a page: classifier duplicates by their original, and PRs by the issues they close ("repo#number" -> numbers).
export type RelatedIndex = { duplicatedBy: Map<string, number[]>; linkedBy: Map<string, number[]> };
export async function relatedIndex(db: Db): Promise<RelatedIndex> {
  const duplicatedBy = new Map<string, number[]>();
  for (const r of await db.all(db.k.selectFrom("classifications").select(["repo", "dup_of", "number"]).distinct().where("dup_of", "is not", null).$narrowType<{ dup_of: NotNull }>().orderBy("number"))) {
    const k = `${r.repo}#${r.dup_of}`;
    duplicatedBy.set(k, [...(duplicatedBy.get(k) ?? []), r.number]);
  }
  const linkedBy = new Map<string, number[]>();
  for (const p of await db.all(db.k.selectFrom(kx.indexedBy(db, "main.items", "items_closing", "i")).select(["i.repo", "i.number", "i.closing_issues"]).where("i.kind", "=", kx.literal("pr")).where("i.closing_issues", "is not", null)
    .$narrowType<{ closing_issues: NotNull }>().where("i.closing_issues", "<>", kx.literal("[]"))))
    for (const x of json<unknown[]>(p.closing_issues, [])) if (typeof x === "number") linkedBy.set(`${p.repo}#${x}`, [...(linkedBy.get(`${p.repo}#${x}`) ?? []), p.number]);
  return { duplicatedBy, linkedBy };
}

// Duplicates from the classifiers, issues a PR closes, and PRs that close this issue. `idx` answers the two unindexed lookups from a page-wide read.
export async function relatedOf(db: Db, repo: string, n: number, idx?: RelatedIndex, page?: DecisionPage): Promise<RelatedItem[]> {
  const res: RelatedItem[] = [];
  for (const { number, relation } of await relatedNumbers(db, repo, n, idx, page)) {
    const row = page ? page.items.get(`${repo}#${number}`) : (await db.get(db.k.selectFrom("items").select(["title", "state", "kind"]).where("repo", "=", repo).where("number", "=", number)));
    if (row) res.push({ repo, number, relation, title: row.title ?? "", state: itemState(row.state), kind: row.kind === "pr" ? "pr" : "issue" });
  }
  return res;
}

/** The numbers relatedOf shows, before their items are read; `page` answers the per-item reads from a page-wide one. */
async function relatedNumbers(db: Db, repo: string, n: number, idx?: RelatedIndex, page?: DecisionPage): Promise<{ number: number; relation: Relation }[]> {
  const out = new Map<string, { number: number; relation: Relation }>();
  const add = (number: number, relation: Relation) => {
    if (number !== n && Number.isInteger(number) && !out.has(String(number))) out.set(String(number), { number, relation });
  };
  for (const d of page ? (page.dupOf.get(`${repo}#${n}`) ?? []) : (await db.all(db.k.selectFrom("classifications").select("dup_of").distinct().where("repo", "=", repo).where("number", "=", n).where("dup_of", "is not", null).$narrowType<{ dup_of: NotNull }>())).map((r) => r.dup_of))
    add(d, "duplicate-of");
  for (const x of idx ? (idx.duplicatedBy.get(`${repo}#${n}`) ?? []).slice(0, 10) : (await db.all(db.k.selectFrom("classifications").select("number").distinct().where("repo", "=", repo).where("dup_of", "=", n).limit(10))).map((r) => r.number))
    add(x, "duplicated-by");
  const it = page ? page.items.get(`${repo}#${n}`) : (await db.get(db.k.selectFrom("items").select(["kind", "closing_issues"]).where("repo", "=", repo).where("number", "=", n)));
  for (const x of json<unknown[]>(it?.closing_issues, [])) if (typeof x === "number") add(x, "fixes");
  if (it?.kind === "issue") {
    if (idx) for (const p of (idx.linkedBy.get(`${repo}#${n}`) ?? []).slice(0, 50)) add(p, "links");
    else {
      const prs = await db.all(db.k.selectFrom("items").select(["number", "closing_issues"]).where("repo", "=", repo).where("kind", "=", "pr")
        .where((eb) => kx.like(db, "closing_issues", eb.val(`%${n}%`))).$narrowType<{ closing_issues: NotNull }>().limit(50));
      for (const p of prs) if (json<unknown[]>(p.closing_issues, []).includes(n)) add(p.number, "links");
    }
  }
  return [...out.values()].slice(0, 10);
}

// "One sentence. Another." or a JSON array of sentences, as the pipeline wrote it.
function contextOf(c: string | null): string[] {
  if (!c) return [];
  const arr = json<unknown>(c.trim().startsWith("[") ? c : null, null);
  if (Array.isArray(arr)) return arr.filter((x): x is string => typeof x === "string");
  return c.split(/\n+/).map((s) => s.trim()).filter(Boolean);
}

export type Viewer = { login: string; role: string; operator: boolean; staff: boolean; slots?: Map<string, QueueSlot>; held?: (repo: string, number: number, decisionId?: string) => Promise<Held | null>; memo?: SettleMemo;
  /** Per-page lookups a list fills once so toDecision does not query per row. */
  prep?: Map<string, PrepareState>; blocks?: Map<string, string[]>; related?: RelatedIndex; page?: DecisionPage;
  /** Repos the viewer may not see; facts naming their items are left out (without it, a non-operator gets none of those facts). */
  hidden?: HiddenRepos };
/** The reason a write waits or failed: the stored text (it may carry GitHub's error text) only for operators and maintainers, one fixed sentence per state for everyone else. */
export function execReasonFor(state: ExecState, stored: string | null, repo: string | null, staff: boolean): string | null {
  if (staff) return stored;
  if (state === "waiting-token") return "Waiting for the operator who answered to authorize AutoCraft";
  if (state === "waiting-permission") return repo ? `Waiting for write access to ${slugOf(repo)}` : "Waiting for write access";
  if (state === "failed") return "AutoCraft couldn't finish this.";
  return null;
}
/** Where an answer's write stands. A write the planner holds back (repo approval, pipeline switch, stage breaker, daily limit) is shown with its reason; operators also get the repo whose approvals to open. */
/** The press that waits for room under the daily GitHub write limit and holds the open decision's lock. The place is read now; a disputed press has none. */
async function queuedOf(db: Db, d: DecisionRow, viewer: Viewer): Promise<Decision["queued"]> {
  if (d.answer !== null) return null;
  const id = await queuedLock(db, d);
  if (id === null) return null;
  const c = (await db.get(db.k.selectFrom("main.conversation_cards").select(["acted_by", "dispute_by", "dispute_reason", "dispute_at"]).where("id", "=", id)))!;
  const place = await queuePlace(db, id);
  return { card: id, by: c.acted_by ?? "", place, reason: queueReason(place), dispute: c.dispute_by ? { by: c.dispute_by, reason: viewer.staff ? (c.dispute_reason ?? "") : "", at: c.dispute_at ?? "" } : null };
}

export async function execOf(db: Db, d: DecisionRow, repo: string | null, viewer: Viewer): Promise<Decision["exec"]> {
  if (!d.answer || d.applied_at || d.exec_state === "done" || d.exec_state === DISPUTED) return null;
  const state = (d.exec_state ?? "pending") as ExecState;
  if (state !== "pending") return { state, reason: execReasonFor(state, d.exec_reason ?? null, repo, viewer.staff) };
  let held = d.repo && d.number !== null && !isBatch(d) ? await (viewer.held ??= await heldLookup(db))(d.repo, d.number, d.id) : null;
  // Viewers read only the repo-approval and pipeline-off sentences; backoff and limit texts stay with staff.
  if (held && !viewer.staff && !held.approval && held.reason !== "The pipeline is off") held = null;
  if (!held) return d.exec_state ? { state, reason: null } : null;
  return { state, reason: held.reason, ...(held.approval && viewer.operator ? { approvalsRepo: d.repo! } : {}) };
}
export const viewerOf = (req: Req): Viewer => ({ login: req.user.login, role: req.user.role, operator: isOperator(req), staff: canSeeInternals(req) });
/** The viewer with the queue places of the items they may see, computed once for the response. */
const viewerWith = async (db: Db, req: Req): Promise<Viewer> => ({ ...viewerOf(req), slots: await queueSlots(db, await visibleRepos(db, req)), held: await heldLookup(db), memo: settleMemo(), hidden: await hiddenRepos(db, req) });

/** Whether Undo would succeed: answered, not acted on, and no GitHub write for the answer exists: no act comment, no group post, no write by the card that gave the answer. */
export const canUndo = async (db: Db, d: DecisionRow): Promise<boolean> =>
  !!d.answer && !d.id.includes("~replaced-") && (!d.applied_at || await directHandled(db, d)) && !await wroteToGithub(db, d);

/** Whether a GitHub write already exists for the answer: Act's comment, a duplicate group's post, or another step of the card that answered. */
export async function wroteToGithub(db: Db, d: DecisionRow): Promise<boolean> {
  if (await db.get(db.k.selectFrom("dupe_group_posts").select((eb) => eb.lit(1).as("one")).where("decision_id", "=", d.id).where("url", "is not", null).limit(1))) return true;
  if (await db.get(db.k.selectFrom("actions").select((eb) => eb.lit(1).as("one")).where("decision_id", "=", d.id).where("comment_url", "is not", null).limit(1))) return true;
  // Only the card that gave the answer counts: a later card on the item is another action.
  const row = await db.get(db.k.selectFrom("audit").select("details").where("target", "=", d.id).where("action", "=", "answer").orderBy(kx.rowid("audit"), "desc").limit(1));
  let card: number | undefined;
  try { card = (JSON.parse(row?.details ?? "{}") as { card?: number }).card; } catch { /* no card */ }
  if (card === undefined) return false;
  const c = await db.get(db.k.selectFrom("conversation_cards").select("result").where("id", "=", card).where("state", "=", "done"));
  return (c?.result ?? "").split("; ").some((seg) => seg && !seg.startsWith("Answered the decision") && !seg.startsWith("Decision closed as handled"));
}

/** An operator's own "handled" answer, recorded by its audit source: nothing was written anywhere, so Undo stays possible although it is marked applied (a conversation card's handled answer follows GitHub writes and stays final). A withdrawn question's renamed row (`~replaced-`) never reopens. */
const directHandled = async (db: Db, d: DecisionRow): Promise<boolean> =>
  d.answer === HANDLED.key && !d.id.includes("~replaced-") && !!await db.get(db.k.selectFrom("audit").select((eb) => eb.lit(1).as("one")).where("target", "=", d.id).where("action", "=", "answer")
    .where((eb) => kx.like(db, "details", eb.val('%"source":"dashboard"%'))).limit(1));

const ROLE_ORDER: Record<DuplicateMember["role"], number> = { keep: 0, close: 1, stay: 2 };
const strings = (v: unknown): string[] => (Array.isArray(v) ? v.filter((x): x is string => typeof x === "string" && !!x.trim()) : []);

const isGroup = (d: DecisionRow): boolean => d.kind === "duplicate" && d.id.includes(":dupegroup:");
type GroupRow = { number: number; kind: string; role: string; detail: string | null; title: string | null; state: string | null };
const groupRows = (db: Db) => db.k.selectFrom("dupe_group_items as gi").leftJoin("items as i", (j) => j.onRef("i.repo", "=", "gi.repo").onRef("i.number", "=", "gi.number"))
  .select(["gi.decision_id", "gi.number", "gi.kind", "gi.role", "gi.detail", "i.title", "i.state"]);

async function groupOf(db: Db, d: DecisionRow, page?: DecisionPage): Promise<DuplicateMember[]> {
  if (!isGroup(d)) return [];
  const rows = page ? (page.groups.get(d.id) ?? []) : (await db.all(groupRows(db).where("gi.decision_id", "=", d.id)));
  return rows.map((r) => {
    let detail: Record<string, unknown> = {};
    try { detail = (JSON.parse(r.detail ?? "{}") as Record<string, unknown>) ?? {}; } catch { /* no detail */ }
    const role: DuplicateMember["role"] = r.role === "keep" || r.role === "close" ? r.role : "stay";
    return { number: r.number, kind: r.kind === "pr" ? "pr" as const : "issue" as const, role, title: r.title, state: r.state ? itemState(r.state) : null, better: strings(detail.better), carry: strings(detail.carry), adds: strings(detail.adds) };
  }).sort((a, b) => ROLE_ORDER[a.role] - ROLE_ORDER[b.role] || a.number - b.number);
}

/** The review's risks for the PR at the head the Merge? or Close? decision was asked at. */
/** The head a Merge? or Close? decision was asked at, or null. */
function riskHead(d: DecisionRow): string | null {
  if ((d.kind !== "merge" && d.kind !== "close") || !d.repo || d.number === null) return null;
  return factsOf(d).find((f) => f.startsWith(HEAD_FACT))?.slice(HEAD_FACT.length).trim() || null;
}

async function risksOf(db: Db, d: DecisionRow, page?: DecisionPage): Promise<string[]> {
  const head = riskHead(d);
  if (!head) return [];
  const o = page ? page.risks.get(`${d.repo}#${d.number}@${head}`) : (await db.get(db.k.selectFrom("opus_reviews").select("risks").where("repo", "=", d.repo!).where("number", "=", d.number!).where("head_oid", "=", head).where("verdict", "is not", null)));
  try { return strings(JSON.parse(o?.risks ?? "[]")); } catch { return []; }
}

async function snoozedOf(db: Db, d: DecisionRow): Promise<{ by: string; at: string } | null> {
  if (d.answer || !d.snoozed_at) return null;
  const row = await db.get(db.k.selectFrom("decisions").select((eb) => snoozedExpr(eb).as("s")).where("id", "=", d.id));
  return row?.s ? { by: d.snoozed_by ?? "", at: d.snoozed_at } : null;
}

/** True when any item the decision concerns (its own, a batch member or a duplicate-group member) is a quiet pull request. */
export async function touchesQuiet(db: Db, d: DecisionRow, page?: DecisionPage): Promise<boolean> {
  const quiet = async (repo: string, number: number): Promise<boolean> => (page ? page.quiet!.has(`${repo}#${number}`) : await isQuiet(db, repo, number));
  if (d.repo !== null && d.number !== null && await quiet(d.repo, d.number)) return true;
  if (await someSeq(itemsOf(d), async (r) => await quiet(r.repo, r.number))) return true;
  const members = page ? (page.members.get(d.id) ?? []) : (await db.all(db.k.selectFrom("dupe_group_items").select(["repo", "number"]).where("decision_id", "=", d.id)));
  return await someSeq(members, async (m) => await quiet(m.repo, m.number));
}

type PageItem = { title: string | null; body: string | null; kind: string; state: string; closing_issues: string | null };
/**
 * What toDecision reads per decision, read once for a page of them: one query per table, keyed by decision id or `repo#number`.
 * `items` holds the decisions' own items and the items their related lists name. `quiet` and `members` are read only for viewers who are not staff.
 */
export type DecisionPage = {
  items: Map<string, PageItem>; shown: Map<string, ItemShown>; opinions: Map<string, OpinionRow[]>; adopted: Map<string, OpinionRow>;
  claims: Map<string, Claim>; comments: Map<string, number>; classes: Map<string, ClassRow[]>; dupOf: Map<string, number[]>;
  areas: Map<string, string | null>; priority: Map<string, NonNullable<Awaited<ReturnType<typeof priorityOf>>>>;
  risks: Map<string, { risks: string | null }>; groups: Map<string, GroupRow[]>; texts: Map<string, Awaited<ReturnType<typeof optionTextsOf>>>;
  quiet?: Set<string>; members: Map<string, { repo: string; number: number }[]>;
};

const pushTo = <K, V>(m: Map<K, V[]>, k: K, v: V): void => void (m.get(k) ?? m.set(k, []).get(k)!).push(v);
const keyOf = (r: { repo: string; number: number }): string => `${r.repo}#${r.number}`;

export async function decisionPage(db: Db, ds: DecisionRow[], viewer: Viewer): Promise<DecisionPage> {
  const ids = ds.map((d) => d.id);
  const singles = ds.filter((d) => !isBatch(d)).map((d) => ({ repo: d.repo!, number: d.number! }));
  const refs = singles.map((r) => [r.repo, r.number] as [string, number]);
  const itemRows = (of: [string, number][]) => db.all(db.k.selectFrom("items").select(["repo", "number", "title", "body", "kind", "state", "closing_issues"]).where(kx.refIn(db, "items", of)));
  const batchRefs = ds.filter(isBatch).flatMap((d) => itemsOf(d));
  const adoptedIds = ds.flatMap((d) => (d.adopted_opinion ? [d.adopted_opinion] : []));
  const heads = ds.flatMap((d) => { const h = riskHead(d); return h ? [[d.repo!, d.number!, h] as [string, number, string]] : []; });
  const groupIds = ds.filter(isGroup).map((d) => d.id);
  // The page's reads do not depend on each other, so they run together.
  const [itemsRead, dupRows, shown, opinions, adopted, claims, comments, classes, areas, priority, risks, groups, texts, quiet, members] = await Promise.all([
    itemRows(refs),
    // In dup_of order, as the (repo, dup_of) index gives them to the one-item read.
    db.all(db.k.selectFrom("classifications").select(["repo", "number", "dup_of"]).distinct().where(kx.refIn(db, "classifications", refs))
      .where("dup_of", "is not", null).$narrowType<{ dup_of: NotNull }>().orderBy("repo").orderBy("number").orderBy("dup_of")),
    batchRefs.length ? shownMap(db, batchRefs) : undefined,
    db.all(db.k.selectFrom("decision_opinions").select(["decision_id", "id", "login", "option", "note", "at", "permission", "via"])
      .where(kx.inList(db, "decision_id", ids)).where("retracted_at", "is", null).orderBy("decision_id").orderBy("at", "desc")),
    adoptedIds.length ? db.all(db.k.selectFrom("decision_opinions").select(["id", "login", "option", "note", "at", "permission", "via"]).where(kx.inList(db, "id", adoptedIds))) : [],
    db.all(db.k.selectFrom("decision_claims").select(["decision_id", "login", "since"]).where(kx.inList(db, "decision_id", ids))
      .where("at", ">", new Date(Date.now() - CLAIM_TTL).toISOString()).where("login", "<>", viewer.login).orderBy("decision_id").orderBy("at", "desc")),
    db.all(db.k.selectFrom("decision_comments").select(["decision_id", (eb) => eb.fn.countAll<number>().as("n")]).where(kx.inList(db, "decision_id", ids)).groupBy("decision_id")),
    // In the primary key's order within an item, as the one-item read walks it.
    singles.length ? db.all(db.k.selectFrom("classifications").select(["repo", "number", "classifier", "at", "type_label", "want", "reference", "reason", "dup_of"])
      .where(kx.refIn(db, "classifications", refs)).orderBy("repo").orderBy("number").orderBy("classifier").orderBy("run_id")) : [],
    singles.length ? db.all(db.k.selectFrom("item_areas").select(["repo", "number", "area"]).where(kx.refIn(db, "item_areas", refs))) : [],
    singles.length ? db.all(db.k.selectFrom("item_priority").select(["repo", "number", "impact", "model", "reason"]).$narrowType<{ impact: Level }>().where(kx.refIn(db, "item_priority", refs))) : [],
    heads.length ? db.all(db.k.selectFrom("opus_reviews").select(["repo", "number", "head_oid", "risks"])
      .where(kx.tupleIn(db, ["repo", "number", "head_oid"], heads, ["text", "bigint", "text"])).where("verdict", "is not", null)) : [],
    groupIds.length ? db.all(groupRows(db).where(kx.inList(db, "gi.decision_id", groupIds))) : [],
    groupIds.length ? optionTextsMany(db, groupIds) : undefined,
    viewer.staff ? undefined : quietFixPrs(db),
    viewer.staff ? [] : db.all(db.k.selectFrom("dupe_group_items").select(["decision_id", "repo", "number"]).where(kx.inList(db, "decision_id", ids))),
  ]);
  const items = new Map<string, PageItem>();
  for (const r of itemsRead) items.set(keyOf(r), r);
  const dupOf = new Map<string, number[]>();
  for (const r of dupRows) pushTo(dupOf, keyOf(r), r.dup_of);
  const page: DecisionPage = {
    items, shown: new Map(), opinions: new Map(), adopted: new Map(), claims: new Map(), comments: new Map(), classes: new Map(), dupOf,
    areas: new Map(), priority: new Map(), risks: new Map(), groups: new Map(), texts: new Map(), members: new Map(),
  };
  // The related lists' items, read once for every decision of the page.
  const related = new Map<string, [string, number]>();
  for (const r of singles) for (const x of await relatedNumbers(db, r.repo, r.number, viewer.related, page)) if (!items.has(`${r.repo}#${x.number}`)) related.set(`${r.repo}#${x.number}`, [r.repo, x.number]);
  if (related.size) for (const r of await itemRows([...related.values()])) items.set(keyOf(r), r);

  if (shown) page.shown = shown;
  for (const o of opinions) {
    const { decision_id, ...op } = o;
    pushTo(page.opinions, decision_id, op);
  }
  for (const o of adopted) page.adopted.set(o.id, o);
  for (const c of claims) if (!page.claims.has(c.decision_id)) page.claims.set(c.decision_id, { login: c.login, since: c.since });
  for (const c of comments) page.comments.set(c.decision_id, c.n);
  for (const c of classes) {
    const { repo, number, ...row } = c;
    pushTo(page.classes, `${repo}#${number}`, row);
  }
  for (const a of areas) page.areas.set(keyOf(a), a.area);
  for (const p of priority) page.priority.set(keyOf(p), { level: p.impact, model: p.model, reason: p.reason });
  for (const o of risks) page.risks.set(`${o.repo}#${o.number}@${o.head_oid}`, { risks: o.risks });
  for (const g of groups) {
    const { decision_id, ...row } = g;
    pushTo(page.groups, decision_id, row);
  }
  if (texts) page.texts = texts;
  if (quiet) page.quiet = quiet;
  for (const m of members) pushTo(page.members, m.decision_id, { repo: m.repo, number: m.number });
  return page;
}

export async function toDecision(db: Db, d: DecisionRow, viewer: Viewer): Promise<Decision> {
  const batch = isBatch(d);
  const refs = itemsOf(d);
  const page = viewer.page;
  const item = batch ? undefined : page ? page.items.get(`${d.repo}#${d.number}`) : (await db.get(db.k.selectFrom("items").select(["title", "body", "kind", "state"]).where("repo", "=", d.repo!).where("number", "=", d.number!)));
  const opinions = (page ? (page.opinions.get(d.id) ?? []) : (await db.all(db.k.selectFrom("decision_opinions").select(["id", "login", "option", "note", "at", "permission", "via"])
    .where("decision_id", "=", d.id).where("retracted_at", "is", null).orderBy("at", "desc")))).map(toOpinion);
  const adopted = d.adopted_opinion
    ? (page ? page.adopted.get(d.adopted_opinion) : (await db.get(db.k.selectFrom("decision_opinions").select(["id", "login", "option", "note", "at", "permission", "via"]).where("id", "=", d.adopted_opinion))))
    : undefined;
  const claimRow = page ? page.claims.get(d.id) : (await db.get(db.k.selectFrom("decision_claims").select(["login", "since"]).where("decision_id", "=", d.id)
    .where("at", ">", new Date(Date.now() - CLAIM_TTL).toISOString()).where("login", "<>", viewer.login).orderBy("at", "desc").limit(1)));
  const pstate = viewer.prep?.get(d.id) ?? (viewer.prep && (d.answer !== null || d.number === null) ? { preparing: false, preparedEarlier: false } : undefined);
  const commentCount = page ? (page.comments.get(d.id) ?? 0) : (await db.get(db.k.selectFrom("decision_comments").select((eb) => eb.fn.countAll<number>().as("n")).where("decision_id", "=", d.id)))!.n;
  // Viewers get none of the model text of a decision about a quiet pull request.
  const veil = !viewer.staff && await touchesQuiet(db, d, page?.quiet ? page : undefined);
  const out: Decision = {
    id: d.id, kind: d.kind as DecisionKind,
    repo: d.repo ?? refs[0]?.repo ?? "",
    number: d.number,
    items: batch ? await withShown(db, refs, page?.shown) : [],
    shown: batch ? null : { kind: item ? (item.kind === "pr" ? "pr" : "issue") : null, state: item ? itemState(item.state) : null, title: item?.title ?? null },
    title: batch ? d.question : (item?.title ?? `${d.repo}#${d.number}`),
    question: d.question,
    summary: d.summary ?? "", summaryBy: d.summary_by ?? "", summaryAt: d.summary_at ?? d.asked_at,
    body: batch ? null : (item?.body ?? null),
    context: contextOf(d.context),
    facts: [...factsOf(d), ...(d.answer !== null ? [] : viewer.blocks ? (viewer.blocks.get(`${d.repo}#${d.number}`) ?? []) : await blocksFacts(db, d.repo, d.number, viewer.hidden ?? (viewer.operator ? [] : ALL_HIDDEN)))],
    // An answer the pipeline or a conversation gave is not one of the asked options; it is listed so every view can name it.
    options: [...optionsOf(d), ...[HANDLED, WITHDRAWN_OPTION].filter((o) => o.key === d.answer)].map((o) => ({ ...o, writes: writeEffect(d.kind, o.key) ?? undefined })),
    recommended: d.recommended ?? "",
    recommendedReason: d.recommended ? (d.recommended_reason ?? "") : "",
    group: await groupOf(db, d, page), risks: await risksOf(db, d, page), undoable: await canUndo(db, d),
    canSettle: await canSettleCached(db, viewer.login, viewer.role, reposOf(d), viewer.memo),
    publicText: commentPreview(d),
    optionTexts: isGroup(d) ? (page ? (page.texts.get(d.id) ?? []) : await optionTextsOf(db, d.id)) : [],
    askedAt: d.asked_at, askedBy: d.asked_by ?? "the pipeline",
    docket: d.docket,
    answer: d.answer,
    answerNote: viewer.staff ? d.answer_note : null,
    answeredBy: d.answered_by, answeredAt: d.answered_at, appliedAt: d.applied_at,
    exec: await execOf(db, d, d.repo ?? refs[0]?.repo ?? null, viewer),
    settling: await settlingBy(db, d),
    queued: await queuedOf(db, d, viewer),
    dispute: isDisputed(d) ? { by: d.dispute_by ?? "", reason: viewer.staff ? (d.dispute_reason ?? "") : "", at: d.dispute_at ?? "" } : null,
    orphaned: !!d.answer && !d.applied_at && !isBacklog(d.answer) && !isDisputed(d) && !!d.answered_by && d.answered_by !== "pipeline" && writeEffect(d.kind, d.answer) !== null && !await settledBy(db, d, viewer.memo),
    adoptedOpinion: adopted ? toOpinion(adopted) : null,
    opinions,
    exceptions: exceptionsOf(d),
    snoozed: await snoozedOf(db, d),
    preparing: pstate ? pstate.preparing : await preparing(db, d),
    preparedEarlier: pstate ? pstate.preparedEarlier : await preparedEarlier(db, d),
    claim: d.answer ? null : (claimRow ? { login: claimRow.login, since: claimRow.since } : null),
    commentCount,
    related: batch ? [] : await relatedOf(db, d.repo!, d.number!, viewer.related, page),
    classifications: batch ? [] : page ? latestClassifications(page.classes.get(`${d.repo}#${d.number}`) ?? []) : await classificationsOf(db, d.repo!, d.number!),
    area: batch ? null : page ? (page.areas.get(`${d.repo}#${d.number}`) ?? null) : await areaNameOf(db, d.repo!, d.number!),
    priority: batch ? null : page ? (page.priority.get(`${d.repo}#${d.number}`) ?? null) : await priorityOf(db, d.repo!, d.number!),
    queue: batch ? null : (viewer.slots?.get(`${d.repo}#${d.number}`) ?? null),
  };
  if (!veil) return out;
  return {
    ...out, title: "", question: "", summary: "", body: null, shown: out.shown && { ...out.shown, title: null }, context: [], facts: [], recommendedReason: "", risks: [], publicText: null, optionTexts: [],
    group: out.group.map((m) => ({ ...m, better: [], carry: [], adds: [] })),
    opinions: out.opinions.map((o) => ({ ...o, note: "" })), adoptedOpinion: out.adoptedOpinion && { ...out.adoptedOpinion, note: "" },
    dispute: out.dispute && { ...out.dispute, reason: "" }, classifications: out.classifications.map((c) => ({ ...c, reason: "" })), priority: out.priority && { ...out.priority, reason: null },
  };
}

const concat = (eb: Eb, ...parts: Expression<unknown>[]): AliasableExpression<string> =>
  parts.slice(1).reduce<Expression<unknown>>((a, p) => eb(a, "||", p), parts[0]!) as AliasableExpression<string>;

const list = (req: Req, key: string): string[] => req.query.getAll(key).flatMap((v) => v.split(",")).map((s) => s.trim()).filter(Boolean);

/** Leaves out decisions touching a hidden repo (the decision's own, or an item of a batch), on decisions with unaliased `repo` and `items`; null when nothing is hidden. */
export function withoutHiddenExpr(db: Db, eb: Eb, hidden: HiddenRepos): AliasableExpression<SqlBool> | null {
  if (!filtered(hidden)) return null;
  const items = eb.case().when(kx.jsonValid(db, "items")).then(eb.ref("items")).else("[]").end();
  const member = (cond: (repo: Expression<string | null>) => Expression<SqlBool>) => {
    const repo = kx.jsonText(db, "j.value", "$.repo");
    return eb.exists(db.k.selectFrom(kx.jsonEach(db, items, "j")).select(eb.lit(1).as("one")).where(cond(repo)));
  };
  if (!hidden.visible) {
    return eb.not(eb.or([
      eb.fn.coalesce(kx.inList(db, "repo", hidden), eb.lit(false)) as Expression<SqlBool>,
      member((repo) => kx.inList(db, repo, hidden)),
    ]));
  }
  // Allow-list: a decision is shown only when every repo it names (its own, and each item of a batch) is one the viewer may see.
  return eb.not(eb.or([
    eb.and([eb("repo", "is not", null), hiddenTestExpr(db, "repo", hidden)]),
    member((repo) => eb.and([eb(repo, "is not", null), hiddenTestExpr(db, repo, hidden)])),
  ]));
}

/** Decisions touching any of the given repos: the decision's own repo, or an item of a batch. */
function inReposExpr(db: Db, eb: Eb, repos: string[]): AliasableExpression<SqlBool> {
  const items = eb.case().when(kx.jsonValid(db, "items")).then(eb.ref("items")).else("[]").end();
  return eb.or([
    eb.fn.coalesce(kx.inList(db, "repo", repos), eb.lit(false)) as Expression<SqlBool>,
    eb.exists(db.k.selectFrom(kx.jsonEach(db, items, "j")).select(eb.lit(1).as("one")).where(kx.inList(db, kx.jsonText(db, "j.value", "$.repo"), repos))),
  ]);
}

/**
 * The decisions a person has to look at (decisions unaliased): open ones they can settle, plus the disputes and unsettled answers they resolve
 * (src/answers/hold.ts). `everything` leaves out the can-settle test, for a view of all open questions; viewers always get it.
 */
export function needingExpr(db: Db, eb: Eb, req: Req, everything = false): AliasableExpression<SqlBool> {
  everything ||= req.user.role === "viewer";
  const open = [eb("decisions.answer", "is", null), eb.not(snoozedExpr(eb)), ...(everything ? [] : [canSettleExpr(db, eb, req.user.login, req.user.role)])];
  return eb.or([eb.and(open), heldOpenExpr(db, eb, req.user.login, isOperator(req))]);
}

/**
 * The requester's decisions inbox count (the Needs you page's), in total and per repo, over every repo they may see.
 * A batch counts once in the total and once for each repo it names.
 */
export async function openCounts(db: Db, req: Req): Promise<OpenDecisionCounts> {
  const hidden = await hiddenRepos(db, req);
  const rows = await db.all(db.k.selectFrom("decisions").select(["repo", "number", "items"]).where((eb) => needingExpr(db, eb, req))
    .$if(filtered(hidden), (q) => q.where((eb) => withoutHiddenExpr(db, eb, hidden)!)));
  const byRepo: Record<string, number> = {};
  for (const r of rows) for (const repo of reposOf(r as DecisionRow)) byRepo[repo] = (byRepo[repo] ?? 0) + 1;
  return { total: rows.length, byRepo };
}

// Pages are cut by keyset: `after` is the `next` of the previous page, "<sort key>|<id>".
// Open decisions run by priority level, then unclaimed before claimed, then oldest asked; answered ones newest answer first.
export async function listDecisions(db: Db, req: Req): Promise<DecisionList> {
  const q = req.query;
  const status = q.get("status") ?? "open";
  if (!["open", "opinions", "answered", "backlog", "snoozed"].includes(status)) throw new HttpError(400, "status must be open, opinions, answered, backlog or snoozed");
  const where: ((eb: ExpressionBuilder<DB, "decisions">) => Expression<SqlBool>)[] = [];
  if (status === "answered") {
    const since = new Date(Date.now() - 7 * 86400_000).toISOString();
    where.push((eb) => eb.and([eb("answer", "is not", null), eb("answer", "<>", BACKLOG.key), eb.or([eb("applied_at", "is", null), eb("applied_at", ">=", since)])]));
  } else if (status === "backlog") where.push((eb) => eb("answer", "=", BACKLOG.key));
  else if (status === "snoozed") where.push((eb) => snoozedExpr(eb));
  else if (status === "open" || status === "opinions") {
    // The person's own list unless asked for everything (`all=1`) or scoped to one item: only the questions they can settle, plus the disputes they resolve.
    where.push((eb) => needingExpr(db, eb, req, q.get("all") === "1" || q.get("number") !== null));
  }
  if (status === "opinions") where.push((eb) => eb.or([
    eb.exists(eb.selectFrom("decision_opinions as o").select(eb.lit(1).as("one")).whereRef("o.decision_id", "=", "decisions.id").where("o.retracted_at", "is", null)),
    eb.exists(eb.selectFrom("decision_comments as c").select(eb.lit(1).as("one")).whereRef("c.decision_id", "=", "decisions.id")),
  ]));
  const kinds = list(req, "kind").filter((k) => (KINDS as readonly string[]).includes(k));
  if (kinds.length) where.push((eb) => eb("kind", "in", kinds));
  const minAge = Math.min(Number(q.get("minAge") ?? 0), 36500);
  if (minAge > 0) {
    const before = new Date(Date.now() - minAge * 86400_000).toISOString();
    where.push((eb) => eb("asked_at", "<=", before));
  }
  const hidden = await hiddenRepos(db, req);
  if (filtered(hidden)) where.push((eb) => withoutHiddenExpr(db, eb, hidden)!);
  const repos = [...new Set(list(req, "repo"))];
  if (repos.length) where.push((eb) => inReposExpr(db, eb, repos));
  // One item: the decision's own, or a batch or duplicate group that lists it as a member.
  const itemNo = q.get("number");
  if (itemNo !== null && repos.length === 1) {
    const n = Number(itemNo);
    if (!Number.isInteger(n)) throw new HttpError(400, "number must be an item number");
    const itemRepo = repos[0]!;
    where.push((eb) => eb.or([
      eb.and([eb("repo", "=", itemRepo), eb("number", "=", n)]),
      eb.exists(db.k.selectFrom(kx.jsonEach(db, eb.case().when(kx.jsonValid(db, "items")).then(eb.ref("items")).else("[]").end(), "ij")).select(eb.lit(1).as("one"))
        .where(eb(kx.jsonText(db, "ij.value", "$.repo"), "=", itemRepo)).where(eb(kx.jsonNum(db, "ij.value", "$.number"), "=", n))),
      eb.exists(eb.selectFrom("dupe_group_items as gm").select(eb.lit(1).as("one")).whereRef("gm.decision_id", "=", "decisions.id").where("gm.repo", "=", itemRepo).where("gm.number", "=", n)),
    ]));
  }
  const matching = (eb: ExpressionBuilder<DB, "decisions">) => eb.and(where.map((w) => w(eb)));

  // Counts and the oldest ask cover every match, not just the page.
  const kindRows = await db.all(db.k.selectFrom("decisions").select(["kind", (eb) => eb.fn.countAll<number>().as("n"), (eb) => eb.fn.min<string>("asked_at").as("oldest")])
    .$narrowType<{ kind: DecisionKind }>().where(matching).groupBy("kind"));
  const byKind: Partial<Record<DecisionKind, number>> = {};
  let total = 0;
  let oldestAskedAt: string | null = null;
  for (const r of kindRows) {
    byKind[r.kind] = r.n;
    total += r.n;
    if (oldestAskedAt === null || r.oldest < oldestAskedAt) oldestAskedAt = r.oldest;
  }

  const answered = status === "answered" || status === "backlog";
  const after = q.get("after");
  // limit=0 asks for the counts only: no page, and no queue slots to compute.
  const rawLimit = q.get("limit");
  if (rawLimit !== null && rawLimit !== "" && Number(rawLimit) === 0) return { decisions: [], total, byKind, oldestAskedAt, next: null };
  const limit = Math.min(Math.max(Number(rawLimit) || 100, 1), 500);
  let ids: { id: string; cursor: string }[];
  if (answered) {
    // Answered ones: newest answer first.
    let cursor: { ck: string; cid: string } | undefined;
    if (after) {
      const k = after.indexOf("|");
      if (k < 0) throw new HttpError(400, "after must be a next cursor");
      cursor = { ck: after.slice(0, k), cid: after.slice(k + 1) };
    }
    ids = await db.all(db.k.selectFrom("decisions").select((eb) => ["id", concat(eb, eb.ref("answered_at"), eb.val("|"), eb.ref("id")).as("cursor")])
      .where(matching).$if(!!cursor, (qb) => qb.where((eb) => eb.or([eb("answered_at", "<", cursor!.ck), eb.and([eb("answered_at", "=", cursor!.ck), eb("id", ">", cursor!.cid)])])))
      .orderBy("answered_at", (o) => o.desc().nullsLast()).orderBy("id").limit(limit + 1));
  } else {
    // Open ones: priority level (security raised), then ones nobody else is looking at, then the oldest ask. The cursor carries all four keys.
    const fresh = new Date(Date.now() - CLAIM_TTL).toISOString();
    const keyed = db.k.selectFrom("decisions").select((eb) => [
      "id", "asked_at",
      eb.fn.coalesce(eb.selectFrom("main.item_rank as r").select((e) => e.cast<number>(e("r.score", "/", 10000), "bigint").as("lvl")).whereRef("r.repo", "=", "decisions.repo").whereRef("r.number", "=", "decisions.number"), eb.lit(DEFAULT_BAND)).as("lvl"),
      eb.case().when(eb.exists(eb.selectFrom("main.decision_claims as c").select(eb.lit(1).as("one")).whereRef("c.decision_id", "=", "decisions.id").where("c.at", ">", fresh).where("c.login", "<>", req.user.login))).then(1).else(0).end().as("claimed"),
    ]).where(matching);
    let cursor: { cl: number; cc: number; ca: string; ci: string } | undefined;
    if (after) {
      const m = /^(\d+)\|([01])\|([^|]*)\|(.*)$/s.exec(after);
      if (!m) throw new HttpError(400, "after must be a next cursor");
      cursor = { cl: Number(m[1]), cc: Number(m[2]), ca: m[3]!, ci: m[4]! };
    }
    ids = await db.all(db.k.selectFrom(keyed.as("keyed")).select((eb) => ["id", concat(eb, eb.ref("lvl"), eb.val("|"), eb.ref("claimed"), eb.val("|"), eb.ref("asked_at"), eb.val("|"), eb.ref("id")).as("cursor")])
      .$if(!!cursor, (qb) => qb.where((eb) => eb.or([
        eb("lvl", "<", cursor!.cl),
        eb.and([eb("lvl", "=", cursor!.cl), eb.or([
          eb("claimed", ">", cursor!.cc),
          eb.and([eb("claimed", "=", cursor!.cc), eb.or([eb("asked_at", ">", cursor!.ca), eb.and([eb("asked_at", "=", cursor!.ca), eb("id", ">", cursor!.ci)])])]),
        ])]),
      ])))
      .orderBy("lvl", "desc").orderBy("claimed").orderBy("asked_at").orderBy("id").limit(limit + 1));
  }
  const more = ids.length > limit;
  const shownIds = ids.slice(0, limit);
  const rowOf = new Map((shownIds.length ? await db.all(db.k.selectFrom("decisions").select(kx.cols("decisions")).where(kx.inList(db, "id", shownIds.map((r) => r.id)))) as DecisionRow[] : []).map((d) => [d.id, d]));
  const shown = shownIds.map((r) => rowOf.get(r.id)!);
  const viewer = shown.length ? await viewerWith(db, req) : viewerOf(req);
  const [prep, blocks, related] = await Promise.all([
    prepareStates(db, shown),
    shown.some((d) => d.answer === null) ? blocksIndex(db, viewer.hidden ?? (viewer.operator ? [] : ALL_HIDDEN)) : undefined,
    shown.some((d) => d.number !== null) ? relatedIndex(db) : undefined,
  ]);
  viewer.prep = prep;
  if (blocks) viewer.blocks = blocks;
  if (related) viewer.related = related;
  if (shown.length) viewer.page = await decisionPage(db, shown, viewer);
  return {
    decisions: await mapSeq(shown, async (d) => await toDecision(db, d, viewer)), total, byKind, oldestAskedAt,
    next: more ? shownIds[shownIds.length - 1]!.cursor : null,
  };
}

export async function getOne(db: Db, req: Req): Promise<Decision> {
  return await toDecision(db, await needDecision(db, req, req.params.id!), await viewerWith(db, req));
}

/** The decision a readable link `/decisions/<repo>/<n>` opens, or null when the item has none the viewer may see. */
export async function forItem(db: Db, req: Req): Promise<Decision | null> {
  const n = Number(req.params.n);
  if (!Number.isInteger(n)) throw new HttpError(400, "not an item number");
  const repo = shortRepo(req.params.repo!);
  const visible = new Set(await visibleRepos(db, req));
  const rows = await db.all(db.k.selectFrom("decisions").select(kx.cols("decisions")).where((eb) => eb.or([
    eb.and([eb("repo", "=", repo), eb("number", "=", n)]),
    eb.and([eb("number", "is", null), eb.exists(db.k.selectFrom(kx.jsonEach(db, eb.case().when(kx.jsonValid(db, "items")).then(eb.ref("items")).else("[]").end(), "j")).select(eb.lit(1).as("one"))
      .where(eb(kx.jsonText(db, "j.value", "$.repo"), "=", repo)).where(eb(kx.jsonNum(db, "j.value", "$.number"), "=", n)))]),
  ]))) as unknown as DecisionRow[];
  const viewer = await viewerWith(db, req);
  // A batch that also covers the item counts only when no single-item decision exists; it is the item's decision only by membership.
  const single = rows.filter((d) => d.number === n);
  const kind = req.query.get("kind");
  const shown = await mapSeq((single.length ? single : rows).filter((d) => !kind || d.kind === kind).filter((d) => reposOf(d).every((r) => visible.has(r))), async (d) => await toDecision(db, d, viewer));
  return pickDecision(shown) ?? null;
}

export async function thread(db: Db, req: Req): Promise<DecisionComment[]> {
  const d = await needDecision(db, req, req.params.id!);
  const veil = !canSeeInternals(req) && await touchesQuiet(db, d);
  return (await db.all(db.k.selectFrom("decision_comments").select(["id", "login", "at", "body", "via"]).where("decision_id", "=", req.params.id!).orderBy("at").orderBy(kx.rowid("decision_comments"))))
    .map(({ via, ...c }): DecisionComment => ({ ...c, ...(veil ? { body: "" } : {}), ...(via === "mcp" ? { via: "mcp" as const } : {}) }));
}

// ---- operator writes -----------------------------------------------------

async function settled(db: Db, req: Req, id: string): Promise<Decision> {
  await db.run(db.k.deleteFrom("decision_claims").where("decision_id", "=", id));
  return await toDecision(db, (await getDecision(db, id))!, await viewerWith(db, req));
}

/** Runs `fn` in one write transaction, so a check and the write it guards cannot be separated by another process. */
async function inTransaction<T>(db: Db, fn: () => Promise<T>): Promise<T> {
  return await db.write(async () => {
    const r = await fn();
    return r;
  });
}

/** `via` names the Inbox MCP server in the audit row when an operator's agent answered with an answering token. */
export async function answer(db: Db, req: Req, via?: "mcp"): Promise<Decision> {
  const d = await needDecision(db, req, req.params.id!);
  await requireSettler(db, req, d);
  await applyAnswer(db, d, req.user.login, body(req), "dashboard", via);
  return await settled(db, req, d.id);
}

/**
 * Validates and records an answer for `login`; the caller has already checked that the login may answer. `b` holds the
 * request fields (answer, note, exceptions, textsHash). `source` and `via` go to the audit row.
 */
export async function applyAnswer(db: Db, d: DecisionRow, login: string, b: Record<string, unknown>, source: string, via?: string): Promise<void> {
  if (d.answer) throw new HttpError(409, `already answered by ${d.answered_by}`);
  if (await settlingBy(db, d)) throw new HttpError(409, await lockMessage(db, d));
  const keys = new Set(optionsOf(d).map((o) => o.key));
  const handled = b.answer === HANDLED.key;
  if (typeof b.answer !== "string" || !(keys.has(b.answer) || (handled && !isBatch(d) && !d.id.includes(":dupegroup:")))) throw new HttpError(400, "answer must be one of the option keys");
  const ans = b.answer;
  const note = optionalText(b.note, NOTE_MAX, "note");
  if (handled && !note) throw new HttpError(400, "a handled answer needs a note");
  let exceptions: Exception[] | null = null;
  if (b.exceptions !== undefined && b.exceptions !== null) {
    if (!Array.isArray(b.exceptions)) throw new HttpError(400, "exceptions must be an array");
    if (b.exceptions.length && !isBatch(d)) throw new HttpError(400, "exceptions are only for a batch");
    const inBatch = new Set(itemsOf(d).map((r) => `${r.repo}#${r.number}`));
    const seen = new Set<string>();
    exceptions = [];
    for (const e of b.exceptions as Partial<Exception>[]) {
      const ref = `${e?.repo}#${e?.number}`;
      if (!e || typeof e.repo !== "string" || !Number.isInteger(e.number) || !inBatch.has(ref) || seen.has(ref) || typeof e.answer !== "string" || !keys.has(e.answer) || e.answer === BACKLOG.key)
        throw new HttpError(400, "each exception needs an item of the batch and an option key");
      seen.add(ref);
      if (e.answer !== ans) exceptions.push({ repo: e.repo, number: e.number!, answer: e.answer });
    }
  }
  if (isBatch(d) && (exceptions === null || ans === BACKLOG.key)) exceptions = [];
  // A group answer approves the texts stored for its option; a page showing other texts is stale. Checked and written in one transaction.
  await inTransaction(db, async () => {
    const stored = d.kind === "duplicate" && d.id.includes(":dupegroup:") ? await textsFor(db, d.id, ans) : [];
    if (stored.length && b.textsHash !== textsHash(stored)) throw new HttpError(409, "the comments for this answer changed; reload the decision");
    const at = new Date().toISOString();
    // Handled writes nothing and no step acts on it, so it counts as applied at once (Undo still works, see directHandled).
    const r = await db.run(db.k.updateTable("main.decisions").set({
      answer: ans, answer_note: note, exceptions: exceptions ? JSON.stringify(exceptions) : null, answered_by: login, answered_at: at, applied_at: handled ? at : null,
      adopted_opinion: null, snoozed_by: null, snoozed_at: null, exec_state: null, exec_reason: null,
    }).where("id", "=", d.id).where("answer", "is", null).where((eb) => eb.not(lockLiveExpr(db, eb, "decisions"))));
    if (r.changes !== 1) throw new HttpError(409, await settlingBy(db, await getDecision(db, d.id) ?? d) ? await lockMessage(db, await getDecision(db, d.id) ?? d) : "someone answered this a moment ago");
    await replaceProposedCards(db, d.repo, d.number, d.id);
    await resetActTries(db, d.id);
  });
  if (writeEffect(d.kind, ans)) await initExecState(db, d.id);
  await audit(db, login, "answer", d.id, { answer: ans, source, ...(via ? { via } : {}), exceptions: exceptions?.length ? exceptions : undefined, note: note ? true : undefined });
  // The thread keeps the choice (and the note) when the item has a conversation.
  if (d.repo && d.number !== null && await conversationOf(db, d.repo, d.number)) {
    const label = optionsOf(d).find((o) => o.key === ans)?.label ?? (handled ? HANDLED.label : ans);
    const said = `${login} answered "${d.question}" with "${label}"`;
    // A maintainer's note is stored and shown but never reaches the model: only an operator's words may.
    const operator = !!await db.get(db.k.selectFrom("access").select((eb) => eb.lit(1).as("one")).where("login", "=", login).where("role", "=", "operator"));
    await insertTurn(db, d.repo, d.number, { kind: "system", login: "system", body: note ? `${said}: ${note}` : said, block: systemBlock(`The ${operator ? "operator" : "maintainer"} ${login} answered the open question "${d.question.replace(/[\r\n<]+/g, " ")}" with "${label.replace(/[\r\n<]+/g, " ")}".`, note && operator ? { operator_note: note } : undefined) });
  }
}

/** Snooze (`on`) or wake an open decision. It stays open for the pipeline; only the inbox hides it until the item has new activity. */
export async function snooze(db: Db, req: Req): Promise<Decision> {
  const d = await needDecision(db, req, req.params.id!);
  await requireRunner(req, d);
  if (d.answer) throw new HttpError(409, "only an open decision can be snoozed");
  const b = body(req);
  if (typeof b.on !== "boolean") throw new HttpError(400, "on must be true or false");
  const at = new Date().toISOString();
  await db.run(db.k.updateTable("main.decisions").set({ snoozed_by: b.on ? req.user.login : null, snoozed_at: b.on ? at : null }).where("id", "=", d.id).where("answer", "is", null));
  await audit(db, req.user.login, b.on ? "snooze" : "unsnooze", d.id);
  return await toDecision(db, (await getDecision(db, d.id))!, await viewerWith(db, req));
}

/**
 * Prepare an open decision again. Clears what marks it prepared or failed, so the next pass runs the prepare step for
 * it before the other due ones; its current choices stay until the new ones replace them.
 */
export async function reprepare(db: Db, req: Req): Promise<Decision> {
  const d = await needDecision(db, req, req.params.id!);
  await requireRunner(req, d);
  if (d.answer) throw new HttpError(409, "only an open decision can be prepared again");
  if (!d.repo || d.number === null || !await db.get(db.k.selectFrom("items").select((eb) => eb.lit(1).as("one")).where("repo", "=", d.repo).where("number", "=", d.number))) throw new HttpError(409, "the decision has no item to prepare it for");
  await db.run(db.k.updateTable("main.decisions").set({
    prepared_at: null, prepared_head: null, recommend_failures: null, recommend_failed_at: null, prepare_failed_head: null, prepare_requested_at: new Date().toISOString(),
  }).where("id", "=", d.id).where("answer", "is", null));
  await audit(db, req.user.login, "reprepare", d.id);
  return await toDecision(db, (await getDecision(db, d.id))!, await viewerWith(db, req));
}

/** Null when the answer was a Backlog press on an item with no question: that decision is gone (see `releaseBacklogItem`). */
export async function undo(db: Db, req: Req, via?: "mcp"): Promise<Decision | null> {
  const d = await needDecision(db, req, req.params.id!);
  await requireSettler(db, req, d, true);
  if (!d.answer) throw new HttpError(409, "that decision has no answer to undo");
  if (d.kind === BACKLOG_KIND) return await releaseBacklogItem(db, d, req.user.login, "undo", via);
  if (d.id.includes("~replaced-")) throw new HttpError(409, "a withdrawn question stays closed");
  if (d.applied_at && !await directHandled(db, d)) throw new HttpError(409, "the pipeline already acted on this answer");
  await inTransaction(db, async () => {
    if (!await canUndo(db, d)) throw new HttpError(409, "the pipeline already wrote to GitHub for this answer");
    const r = await db.run(db.k.updateTable("main.decisions").set({
      answer: null, answer_note: null, exceptions: null, answered_by: null, answered_at: null, applied_at: null, adopted_opinion: null, snoozed_by: null, snoozed_at: null,
      exec_state: null, exec_reason: null, dispute_by: null, dispute_reason: null, dispute_at: null,
    }).where("id", "=", d.id).where("answer", "is not", null).where((eb) => eb.or([eb("applied_at", "is", null), eb("answer", "=", HANDLED.key)])));
    if (r.changes !== 1) throw new HttpError(409, "the pipeline acted on this a moment ago");
    await resetGroupAnswer(db, d.id);
    await resetActTries(db, d.id);
  });
  await audit(db, req.user.login, "undo", d.id, { answer: d.answer, adopted: d.adopted_opinion ?? undefined, ...(via ? { via } : {}) });
  if (d.exec_state === DISPUTED) await audit(db, req.user.login, "dispute-resolve", d.id, { resolution: "undo", disputedBy: d.dispute_by ?? undefined });
  return await toDecision(db, (await getDecision(db, d.id))!, await viewerWith(db, req));
}

/**
 * Someone who can settle, and did not give the answer, disputes an answer the pipeline has not applied. The pipeline stops reading it
 * until the answerer or an operator keeps it or takes it back. `reason` is required.
 */
export async function dispute(db: Db, req: Req): Promise<Decision> {
  const d = await needDecision(db, req, req.params.id!);
  await requireSettler(db, req, d);
  const reason = optionalText(body(req).reason, NOTE_MAX, "reason");
  if (!reason) throw new HttpError(400, "a dispute needs a reason");
  if (!d.answer || isBacklog(d.answer) || d.applied_at || d.id.includes("~replaced-")) throw new HttpError(409, "only an answer the pipeline has not applied yet can be disputed");
  if (d.answered_by?.toLowerCase() === req.user.login.toLowerCase()) throw new HttpError(409, "you gave this answer: undo it instead");
  if (d.exec_state === DISPUTED) throw new HttpError(409, `already disputed by ${d.dispute_by}`);
  if (await wroteToGithub(db, d)) throw new HttpError(409, "the pipeline already wrote to GitHub for this answer");
  if (d.kind === "merge" && d.repo && d.number !== null && (await mergeRunning(db, d.repo) || await inActiveBatch(db, d.repo, d.number))) throw new HttpError(409, "the merge is already in progress");
  if (!await openDispute(db, d.id, req.user.login, reason, new Date().toISOString())) throw new HttpError(409, "the pipeline acted on this a moment ago");
  return await toDecision(db, (await getDecision(db, d.id))!, await viewerWith(db, req));
}

/** The answerer, or an operator, keeps a disputed answer: the pipeline reads it again. */
export async function keepAnswer(db: Db, req: Req): Promise<Decision> {
  const d = await needDecision(db, req, req.params.id!);
  await requireSettler(db, req, d, true);
  if (!isDisputed(d)) throw new HttpError(409, "that answer is not disputed");
  await clearDispute(db, d.id);
  await resetActTries(db, d.id);
  await initExecState(db, d.id);
  await audit(db, req.user.login, "dispute-resolve", d.id, { resolution: "keep", answer: d.answer, disputedBy: d.dispute_by ?? undefined });
  return await toDecision(db, (await getDecision(db, d.id))!, await viewerWith(db, req));
}

/** Operator: tries the GitHub write of a waiting or failed answer again. Its merge or close is still made as the operator who answered. */
export async function retry(db: Db, req: Req): Promise<Decision> {
  const d = await needDecision(db, req, req.params.id!);
  await requireSettler(db, req, d, true);
  if (!d.answer || d.applied_at) throw new HttpError(409, "that decision has nothing waiting to run");
  if (!await retryDecision(db, d.id)) throw new HttpError(409, "that decision is not waiting or failed");
  await audit(db, req.user.login, "retry", d.id, { answer: d.answer });
  return await toDecision(db, (await getDecision(db, d.id))!, await viewerWith(db, req));
}

/** Null for a Backlog press on an item with no question: that decision is gone (see `releaseBacklogItem`). */
export async function reopen(db: Db, req: Req): Promise<Decision | null> {
  const d = await needDecision(db, req, req.params.id!);
  await requireSettler(db, req, d, true);
  if (d.answer !== BACKLOG.key) throw new HttpError(409, "that decision is not in the Backlog");
  if (d.kind === BACKLOG_KIND) return await releaseBacklogItem(db, d, req.user.login, "reopen");
  const r = await db.run(db.k.updateTable("main.decisions").set({
    answer: null, answer_note: null, exceptions: null, answered_by: null, answered_at: null, adopted_opinion: null, snoozed_by: null, snoozed_at: null,
  }).where("id", "=", d.id).where("answer", "=", BACKLOG.key));
  if (r.changes !== 1) throw new HttpError(409, "someone changed this a moment ago");
  await audit(db, req.user.login, "reopen", d.id, { answer: d.answer });
  return await toDecision(db, (await getDecision(db, d.id))!, await viewerWith(db, req));
}

/**
 * Puts an open item on the Backlog for `login`; the caller has checked that the login settles on the repo. An open question about the
 * item itself is answered Backlog. Without one a decision of kind `backlog` is written already answered Backlog, so the hold
 * (src/dupes/held.ts) reads the same answer; it is never asked or prepared. Nothing is written to GitHub.
 */
export async function backlogItem(db: Db, repo: string, n: number, login: string, source: string, via?: string): Promise<DecisionRow> {
  const it = await db.get(db.k.selectFrom("items").select("state").where("repo", "=", repo).where("number", "=", n));
  if (!it) throw new HttpError(404, `no item ${slugOf(repo)}#${n}`);
  if (it.state !== "OPEN") throw new HttpError(409, `${slugOf(repo)}#${n} is not open`);
  if (await backlogHeld(db, repo, n)) throw new HttpError(409, `${slugOf(repo)}#${n} is already in the Backlog`);
  const open = (await db.all(db.k.selectFrom("decisions").select(kx.cols("decisions")).where("repo", "=", repo).where("number", "=", n).where("answer", "is", null)
    .where((eb) => eb.not(kx.like(db, "id", eb.val("%~replaced-%")))) ) as DecisionRow[])
    .map((d) => ({ d, kind: d.kind as DecisionKind, answer: d.answer, askedAt: d.asked_at }));
  const first = pickDecision(open)?.d;
  if (first) {
    await applyAnswer(db, first, login, { answer: BACKLOG.key }, source, via);
    return (await getDecision(db, first.id))!;
  }
  const id = backlogDecisionId(repo, n);
  const at = new Date().toISOString();
  await inTransaction(db, async () => {
    if (await getDecision(db, id)) throw new HttpError(409, `${slugOf(repo)}#${n} is already in the Backlog`);
    await db.run(db.k.insertInto("main.decisions").values({ id, kind: BACKLOG_KIND, repo, number: n, question: BACKLOG_QUESTION, facts: "[]", options: JSON.stringify([BACKLOG]), asked_at: at, asked_by: login }));
    // Answered by an update, so the answered_role trigger records the answerer's role.
    await db.run(db.k.updateTable("main.decisions").set({ answer: BACKLOG.key, answered_by: login, answered_at: at }).where("id", "=", id));
  });
  await audit(db, login, "answer", id, { answer: BACKLOG.key, source, ...(via ? { via } : {}) });
  if (await conversationOf(db, repo, n)) {
    const operator = !!await db.get(db.k.selectFrom("access").select((eb) => eb.lit(1).as("one")).where("login", "=", login).where("role", "=", "operator"));
    await insertTurn(db, repo, n, { kind: "system", login: "system", body: `${login} put this item on the Backlog`, block: systemBlock(`The ${operator ? "operator" : "maintainer"} ${login} put the item on the Backlog.`) });
  }
  return (await getDecision(db, id))!;
}

/** POST /api/items/:repo/:n/backlog: `backlogItem` for the requester, who must settle on the repo. */
export async function backlogItemRoute(db: Db, req: Req): Promise<Decision> {
  const repo = req.params.repo!;
  const n = Number(req.params.n);
  if (!Number.isInteger(n)) throw new HttpError(400, "not an item number");
  await needVisible(db, req, repo);
  await requireRepoSettler(db, req, [repo]);
  const d = await backlogItem(db, repo, n, req.user.login, "dashboard");
  return await settled(db, req, d.id);
}

/** Reopen or Undo of a `backlog` decision deletes it with its feedback and claims, so the item counts as never held. */
async function releaseBacklogItem(db: Db, d: DecisionRow, login: string, action: "undo" | "reopen", via?: string): Promise<null> {
  await inTransaction(db, async () => {
    if ((await db.run(db.k.deleteFrom("main.decisions").where("id", "=", d.id).where("kind", "=", BACKLOG_KIND).where("answer", "=", BACKLOG.key))).changes !== 1) throw new HttpError(409, "someone changed this a moment ago");
    await db.run(db.k.deleteFrom("main.decision_opinions").where("decision_id", "=", d.id));
    await db.run(db.k.deleteFrom("main.decision_comments").where("decision_id", "=", d.id));
    await db.run(db.k.deleteFrom("main.decision_claims").where("decision_id", "=", d.id));
  });
  await audit(db, login, action, d.id, { answer: d.answer, repo: d.repo, number: d.number, ...(via ? { via } : {}) });
  return null;
}

/** Bulk: answers every open decision in scope (`outsidePilot`, `repo`) with Backlog. Dry run unless `apply` is true. */
export async function backlogBulk(db: Db, req: Req): Promise<{ applied: boolean; count: number; decisions: { id: string; kind: string; repo: string | null; number: number | null }[] }> {
  const b = body(req);
  const repo = typeof b.repo === "string" && b.repo ? b.repo : undefined;
  // Unscoped (the outside pilot) stays the operator's; a maintainer backlogs one repo they write to.
  if (repo ? !(await mayRunRepo(req.user, repo)) : !isOperator(req)) throw new HttpError(403, repo ? "no write access to this repo" : "only operators put decisions in the Backlog outside one repo");
  if (b.repo !== undefined && b.repo !== null && repo === undefined) throw new HttpError(400, "repo must be a repo name");
  if (b.apply !== undefined && typeof b.apply !== "boolean") throw new HttpError(400, "apply must be true or false");
  if (b.outsidePilot !== undefined && typeof b.outsidePilot !== "boolean") throw new HttpError(400, "outsidePilot must be true or false");
  if (!b.outsidePilot && !repo) throw new HttpError(400, "give outsidePilot: true or a repo");
  let only: ((repos: string[]) => Promise<boolean>) | undefined;
  if (repo) {
    // Scoped: the settle rule on the repo, and only decisions whose every repo the requester sees and settles on.
    await needVisible(db, req, repo);
    await requireRepoSettler(db, req, [repo]);
    const visible = new Set(await allVisibleRepos(db, req));
    only = async (repos) => repos.every((r) => visible.has(r)) && await settleRefusal(db, req.user, repos, "settles decisions") === null;
  }
  const res = await backlogOpen(db, { outsidePilot: b.outsidePilot === true, repo, only }, req.user.login, b.apply === true);
  return { applied: res.applied, count: res.decisions.length, decisions: res.decisions };
}

export async function adopt(db: Db, req: Req): Promise<Decision> {
  const d = await needDecision(db, req, req.params.id!);
  await requireSettler(db, req, d);
  if (d.answer) throw new HttpError(409, `already answered by ${d.answered_by}`);
  if (await settlingBy(db, d)) throw new HttpError(409, await lockMessage(db, d));
  const oid = body(req).opinionId;
  if (typeof oid !== "string") throw new HttpError(400, "opinionId is required");
  const o = await db.get(db.k.selectFrom("decision_opinions").select(["id", "login", "option"]).where("id", "=", oid).where("decision_id", "=", d.id).where("retracted_at", "is", null));
  if (!o) throw new HttpError(404, "no such live opinion on this decision");
  if (!o.option || !optionsOf(d).some((x) => x.key === o.option)) throw new HttpError(400, "that opinion picks no option");
  const option = o.option;
  await inTransaction(db, async () => {
    const stored = d.kind === "duplicate" && d.id.includes(":dupegroup:") ? await textsFor(db, d.id, option) : [];
    if (stored.length && body(req).textsHash !== textsHash(stored)) throw new HttpError(409, "the comments for this answer changed; reload the decision");
    const r = await db.run(db.k.updateTable("main.decisions").set({
      answer: option, answer_note: null, exceptions: isBatch(d) ? "[]" : null, answered_by: req.user.login, answered_at: new Date().toISOString(), adopted_opinion: o.id,
      snoozed_by: null, snoozed_at: null, exec_state: null, exec_reason: null,
    }).where("id", "=", d.id).where("answer", "is", null).where((eb) => eb.not(lockLiveExpr(db, eb, "decisions"))));
    if (r.changes !== 1) throw new HttpError(409, await settlingBy(db, await getDecision(db, d.id) ?? d) ? await lockMessage(db, await getDecision(db, d.id) ?? d) : "someone answered this a moment ago");
    await resetActTries(db, d.id);
  });
  if (writeEffect(d.kind, option)) await initExecState(db, d.id);
  await audit(db, req.user.login, "adopt", d.id, { answer: o.option, opinion: o.id, of: o.login });
  return await settled(db, req, d.id);
}

// ---- writes by operators and maintainers ------------------------------------------

// Viewers are read-only; this runs before any other check so a refused viewer learns nothing about the decision.
function requireWriter(req: Req): void {
  if (req.user.role === "viewer") throw new HttpError(403, "viewers cannot write");
}

// Maintainers: a fixed number of opinion and comment writes per login per hour.
// Operators and maintainers who may run the decision's repo are exempt.
async function limited(req: Req, limiter: RateLimiter, repo: string | null): Promise<void> {
  if (isOperator(req) || (repo && await mayRunRepo(req.user, repo))) return;
  if (!limiter.allow(req.user.login.toLowerCase())) throw new HttpError(429, "too many feedback posts this hour; try again later");
}

/**
 * One feedback box: an optional option pick and free text, at least one of them. A pick is the
 * login's live opinion (its text is the note); text alone is a comment in the thread. `via` names
 * the Inbox MCP server in the audit row when an agent sent it for its user.
 */
export async function feedback(db: Db, req: Req, limiter: RateLimiter, via?: "mcp"): Promise<Decision> {
  requireWriter(req);
  const d = await needDecision(db, req, req.params.id!);
  await limited(req, limiter, d.repo);
  const b = body(req);
  const option = b.option ?? null;
  if (option !== null && (typeof option !== "string" || !optionsOf(d).some((o) => o.key === option))) throw new HttpError(400, "option must be an option key or null");
  const text = optionalText(b.text, COMMENT_MAX, "text");
  if (option === null && text === null) throw new HttpError(400, "give an option, text, or both");
  const at = new Date().toISOString();
  if (option === null) {
    const id = randomToken(12);
    await db.run(db.k.insertInto("decision_comments").values({ id, decision_id: d.id, login: req.user.login, at, body: text!, via: via ?? null }));
    await audit(db, req.user.login, "comment", d.id, { comment: id, length: text!.length, ...(via ? { via } : {}) });
    return await toDecision(db, (await getDecision(db, d.id))!, await viewerWith(db, req));
  }
  if (d.answer) throw new HttpError(409, "the decision is answered; picks are closed");
  const permission = (await blockedRepos(req, d)).length ? "none" : "write";
  await db.write(async () => {
    // The permission check above can wait on GitHub; the operator may have answered meanwhile.
    if ((await db.get(db.k.selectFrom("decisions").select("answer").where("id", "=", d.id)))!.answer !== null)
      throw new HttpError(409, "the decision is answered; picks are closed");
    // A new pick without text keeps the login's earlier note.
    const earlier = await db.get(db.k.selectFrom("decision_opinions").select("note").where("decision_id", "=", d.id).where("login", "=", req.user.login).where("retracted_at", "is", null));
    const note = text ?? earlier?.note ?? null;
    await db.run(db.k.updateTable("decision_opinions").set({ retracted_at: at }).where("decision_id", "=", d.id).where("login", "=", req.user.login).where("retracted_at", "is", null));
    await db.run(db.k.insertInto("decision_opinions").values({ id: randomToken(12), decision_id: d.id, login: req.user.login, option, note, at, permission, via: via ?? null }));
    await audit(db, req.user.login, "opinion", d.id, { option, note: note ? true : undefined, ...(via ? { via } : {}) });
  });
  return await toDecision(db, (await getDecision(db, d.id))!, await viewerWith(db, req));
}

export async function retract(db: Db, req: Req, limiter: RateLimiter): Promise<Decision> {
  requireWriter(req);
  const d = await needDecision(db, req, req.params.id!);
  await limited(req, limiter, d.repo);
  if (d.answer) throw new HttpError(409, "the decision is answered; picks are closed");
  const r = await db.run(db.k.updateTable("decision_opinions").set({ retracted_at: new Date().toISOString() }).where("decision_id", "=", d.id).where("login", "=", req.user.login).where("retracted_at", "is", null));
  if (r.changes !== 1) throw new HttpError(404, "you have no opinion on this decision");
  await audit(db, req.user.login, "opinion-retract", d.id);
  return await toDecision(db, (await getDecision(db, d.id))!, await viewerWith(db, req));
}

// Presence, not content: refreshed while someone has the decision open, so
// it is neither audited nor counted against the hourly limit.
export async function claim(db: Db, req: Req): Promise<void> {
  requireWriter(req);
  const d = await needDecision(db, req, req.params.id!);
  if (d.answer) return;
  const now = new Date().toISOString();
  const lapsed = new Date(Date.now() - CLAIM_TTL).toISOString();
  await db.run(db.k.insertInto("decision_claims").values({ decision_id: d.id, login: req.user.login, since: now, at: now })
    .onConflict((oc) => oc.columns(["decision_id", "login"]).doUpdateSet((eb) => ({
      since: eb.case().when("decision_claims.at", "<=", lapsed).then(eb.ref("excluded.since")).else(eb.ref("decision_claims.since")).end(), at: eb.ref("excluded.at"),
    }))));
}
