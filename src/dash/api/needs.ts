// "Needs you": feedback people left that nobody has answered, and what the pipeline stopped. Decisions have their own API.
// Dashboard users' text is shown here and never read by a prompt.
import type { NeedsFeedbackPage, NeedsFeedbackRow, NeedsFixPage, NeedsPage, NeedsStoppedRow } from "../../../web/src/api/types.ts";
import { accountListOrNull } from "../../agents/account-actions.ts";
import { getDecision, reposOf } from "../../decisions.ts";
import { needingExpr, touchesQuiet, withoutHiddenExpr } from "./decisions.ts";
import { zeroLimits } from "./limits.ts";
import { decisionPath, itemPath } from "../../../web/src/lib/links.ts";
import { reviewBots } from "../../review/bots.ts";
import { HttpError } from "../server/util.ts";
import type { Req } from "../types.ts";
import { itemState } from "./shown.ts";
import { fixStoppedWait } from "../../core/decide-item.ts";
import { blankIfHidden, hiddenRepos } from "./visible.ts";
import { filtered, hiddenTestExpr, isHiddenRepo, type HiddenRepos } from "../../hidden-sql.ts";
import { allOwnLogins } from "../../github/identity.ts";
import { isQuiet } from "../../security/quiet.ts";
import { canSeeInternals } from "./staff.ts";
import type { SelectQueryBuilder, SqlBool } from "kysely";
import { filterSeq, mapSeq, reduceSeq } from "../../async-seq.ts";
import { mayResumeTrip } from "../../push-outcomes.ts";
import { parseTripKey } from "../../core/push-outcome.ts";
import { stepTitle } from "./trace.ts";
import type { Db } from "../../db/index.ts";
import * as kx from "../../db/kx.ts";
import { setting } from "../../settings-read.ts";

export const NEEDS_PAGE = 30;
/** Older feedback is not asked for any more; the window also bounds the scans. */
const WINDOW_MS = 60 * 86_400_000;

type Raw = { id: string; source: NeedsFeedbackRow["source"]; author: string; at: string; text: string; repo: string | null; number: number | null; decision: string | null; turn: number | null; comment: string | null; handled: number };

/** What the sources share: `since` (oldest time wanted), `cur` (newest), `own` (the box account), `owns` (every login that is ours) and `bots`. */
type Window = { since: string; cur: string; own: string; owns: string[]; bots: string[] };

const tag = (eb: kx.Eb, value: string) => eb.cast<string>(eb.val(value), "text");
const nullText = (eb: kx.Eb) => eb.cast<string | null>(eb.lit(null), "text");
const nullInt = (eb: kx.Eb) => eb.cast<number | null>(eb.lit(null), "bigint");
/** The person who wrote `col` has the operator role. */
const operatorLogin = (eb: kx.Eb, col: string) => eb.exists(eb.selectFrom("access as a").select(eb.lit(1).as("one")).whereRef("a.login", "=", col).where("a.role", "=", "operator"));
const idOf = (eb: kx.Eb, prefix: string, col: string) => eb(tag(eb, prefix), "||", eb.ref(col));

// One SELECT per source, each with a `handled` flag.
type Source = SelectQueryBuilder<any, any, Raw>;
const sources = (db: Db, w: Window): Source[] => {
  const k = db.k;
  const answeredAfter = (eb: kx.Eb, at: string) => eb.and([eb("d.answered_at", "is not", null), eb("d.answered_at", ">", eb.ref(at))]);
  const list: Source[] = [
    // Feedback on a decision from anyone but an operator; handled once the decision was answered afterwards.
    k.selectFrom("decision_comments as c").innerJoin("decisions as d", "d.id", "c.decision_id")
      .select((eb) => [idOf(eb, "decision-comment:", "c.id").as("id"), tag(eb, "decision").as("source"), "c.login as author", "c.at", "c.body as text", "d.repo", "d.number", "d.id as decision", nullInt(eb).as("turn"), "c.id as comment",
        answeredAfter(eb, "c.at").as("handled")])
      .where("c.at", ">=", w.since).where("c.at", "<=", w.cur).where((eb) => eb.not(operatorLogin(eb, "c.login"))).$castTo<Raw>(),
    k.selectFrom("decision_opinions as o").innerJoin("decisions as d", "d.id", "o.decision_id")
      .select((eb) => [idOf(eb, "decision-opinion:", "o.id").as("id"), tag(eb, "decision").as("source"), "o.login as author", "o.at", "o.note as text", "d.repo", "d.number", "d.id as decision", nullInt(eb).as("turn"), nullText(eb).as("comment"),
        answeredAfter(eb, "o.at").as("handled")])
      .where("o.at", ">=", w.since).where("o.at", "<=", w.cur).where("o.retracted_at", "is", null)
      .where((eb) => eb.and([eb(eb.fn("trim", [eb.fn.coalesce("o.note", eb.val(""))]), "<>", ""), eb.not(operatorLogin(eb, "o.login"))])).$castTo<Raw>(),
    // An operator's message to Opus on an item; handled once Opus answered it.
    k.selectFrom("conversation_turns as t")
      .select((eb) => [eb(tag(eb, "turn:"), "||", eb.cast<string>("t.id", "text")).as("id"), tag(eb, "operator").as("source"), "t.login as author", "t.at", "t.body as text", "t.repo", "t.number", nullText(eb).as("decision"), "t.id as turn", nullText(eb).as("comment"),
        eb.or([eb("t.status", "is", null), eb("t.status", "=", "answered")]).as("handled")])
      .where("t.kind", "=", "operator").where("t.at", ">=", w.since).where("t.at", "<=", w.cur).$castTo<Raw>(),
  ];
  // Nothing from GitHub without a pipeline login.
  if (w.own === "") return list;
  // A GitHub comment or review that names the pipeline login (flagged when written, so an index finds them); handled once the
  // pipeline login posted on the item afterwards.
  list.push(k.selectFrom("comments as c")
    .select((eb) => [idOf(eb, "comment:", "c.id").as("id"), tag(eb, "github").as("source"), "c.author", "c.created_at as at", "c.body as text", "c.repo", "c.number", nullText(eb).as("decision"), nullInt(eb).as("turn"), "c.id as comment",
      eb.exists(eb.selectFrom("comments as a").select(eb.lit(1).as("one")).whereRef("a.repo", "=", "c.repo").whereRef("a.number", "=", "c.number").where(kx.inList(db, "a.author", w.owns)).whereRef("a.created_at", ">", "c.created_at")).as("handled")])
    .where("c.mentions_pipeline", "=", 1).where("c.created_at", ">=", w.since).where("c.created_at", "<=", w.cur).where("c.author", "is not", null).where(kx.inList(db, "c.author", w.owns, "text", true)).$castTo<Raw>());
  // The comment right after one of AutoCraft's own (a comment the audit log recorded with its URL and text, or the review we posted (audit `review-post`); the account's older manual comments do not count), on an item it commented on; bots do not count as replies.
  list.push(k.selectFrom((eb) => eb.selectFrom("comments").select(["repo", "number"]).where(kx.inList(db, "author", w.owns)).distinct().as("o")).innerJoin("comments as c", (j) => j.onRef("c.repo", "=", "o.repo").onRef("c.number", "=", "o.number"))
    .select((eb) => [idOf(eb, "comment:", "c.id").as("id"), tag(eb, "github").as("source"), "c.author", "c.created_at as at", "c.body as text", "c.repo", "c.number", nullText(eb).as("decision"), nullInt(eb).as("turn"), "c.id as comment",
      eb.exists(eb.selectFrom("comments as a").select(eb.lit(1).as("one")).whereRef("a.repo", "=", "c.repo").whereRef("a.number", "=", "c.number").where(kx.inList(db, "a.author", w.owns)).whereRef("a.created_at", ">", "c.created_at")).as("handled")])
    .where("c.mentions_pipeline", "=", 0).where("c.created_at", ">=", w.since).where("c.created_at", "<=", w.cur).where("c.author", "is not", null).where(kx.inList(db, "c.author", w.owns, "text", true))
    .where((eb) => eb.and([
      eb.not(kx.like(db, "c.author", eb.val("%[bot]"))), kx.inList(db, eb.fn("lower", ["c.author"]), w.bots, "text", true), eb(eb.fn("trim", [eb.fn.coalesce("c.body", eb.val(""))]), "<>", ""),
      eb.exists(eb.selectFrom((e) => e.selectFrom("comments as p").select(["p.id", "p.author", "p.body"]).whereRef("p.repo", "=", "c.repo").whereRef("p.number", "=", "c.number").whereRef("p.created_at", "<", "c.created_at")
        .orderBy("p.created_at", (o) => o.desc().nullsLast()).limit(1).as("q")).select(eb.lit(1).as("one")).where(kx.inList(db, "q.author", w.owns))
        .where((e) => e.exists(e.selectFrom("audit as a").select(e.lit(1).as("one")).where("a.target", "=", e(e(e.ref("c.repo"), "||", e.val("#")), "||", e.cast<string>("c.number", "text")))
          .where((x) => x.or([
            x.and([x(kx.jsonText(db, "a.details", "$.url"), "is not", null), x(kx.trimChars(db, x.fn("replace", [kx.jsonText(db, "a.details", "$.text"), x.val("\r"), x.val("")]), " \n\t"), "=", kx.trimChars(db, x.fn("replace", ["q.body", x.val("\r"), x.val("")]), " \n\t"))]),
            x.and([x("a.action", "=", "review-post"), x(kx.jsonText(db, "a.details", "$.review"), "=", x.ref("q.id"))]),
          ]))))),
    ])).$castTo<Raw>());
  return list;
};

const feedbackUnion = (db: Db, w: Window): Source => {
  const [first, ...rest] = sources(db, w);
  let union = first!;
  for (const q of rest) union = union.unionAll(q);
  return union;
};

const ownLogin = async (db: Db): Promise<string> => (await setting(db, "pipeline_login")) ?? "";

/** True when a feedback row is about a quiet pull request: its item, or any item of its decision (batch and duplicate-group members too). */
async function quietRow(db: Db, r: Pick<Raw, "repo" | "number" | "decision">, memo: Map<string, boolean>): Promise<boolean> {
  if (r.repo !== null && r.number !== null && await isQuiet(db, r.repo, r.number)) return true;
  if (!r.decision) return false;
  let q = memo.get(r.decision);
  if (q === undefined) {
    const d = await getDecision(db, r.decision);
    memo.set(r.decision, (q = !!d && await touchesQuiet(db, d)));
  }
  return q;
}

async function feedback(db: Db, hidden: HiddenRepos, cursor: { at: string; id: string } | null, limit: number, staff: boolean): Promise<{ rows: Raw[]; more: boolean; unhandled: number }> {
  const [own, owns] = await Promise.all([ownLogin(db), allOwnLogins(db)]);
  const window = { since: new Date(Date.now() - WINDOW_MS).toISOString(), own, owns: [...owns], bots: reviewBots() };
  const unhiddenRepo = <QB extends SelectQueryBuilder<any, "u", any>>(q: QB): QB =>
    (filtered(hidden) ? q.where((eb) => eb.or([eb("repo", "is", null), eb.not(hiddenTestExpr(db, "repo", hidden))])) : q) as QB;
  const size = limit * 3 + 1;
  // Feedback on a batch decision is hidden when the batch holds an item of a hidden repo.
  const batches = new Map<string, boolean>();
  const visible = async (r: Pick<Raw, "source" | "number" | "decision">): Promise<boolean> => {
    if (r.source !== "decision" || r.number !== null || !r.decision) return true;
    let ok = batches.get(r.decision);
    if (ok === undefined) {
      const d = await getDecision(db, r.decision);
      ok = !d || !reposOf(d).some((x) => isHiddenRepo(hidden, x));
      batches.set(r.decision, ok);
    }
    return ok;
  };
  // The unhandled count reads alongside the page.
  const openRead = db.all(unhiddenRepo(db.k.selectFrom(feedbackUnion(db, { ...window, cur: "9999" }).as("u")).select((eb) => ["u.source", eb.fn.max<number | null>("u.number").as("number"), "u.decision", eb.fn.countAll<number>().as("n")]))
    .where((eb) => eb.not(kx.ref<SqlBool>("u.handled"))).groupBy(["u.source", (eb) => eb("u.number", "is", null), "u.decision"]));
  openRead.catch(() => {});
  // Time-ordered, so each source reads its index range; rows that are filtered out make it read on until the page is full.
  const rows: Raw[] = [];
  let pos = cursor ?? { at: "9999", id: "\uffff" };
  while (rows.length <= limit) {
    const cur = pos;
    const batch = await db.all(unhiddenRepo(db.k.selectFrom(feedbackUnion(db, { ...window, cur: cur.at }).as("u")).select(["u.id", "u.source", "u.author", "u.at", "u.text", "u.repo", "u.number", "u.decision", "u.turn", "u.comment", "u.handled"]))
      .where((eb) => eb.or([eb("at", "<", cur.at), eb.and([eb("at", "=", cur.at), eb(kx.bytewise(db, "id"), "<", cur.id)])])).orderBy("at", "desc").orderBy(kx.bytewise(db, "id"), "desc").limit(size));
    rows.push(...await filterSeq(batch, visible));
    const tail = batch[batch.length - 1];
    if (batch.length < size || !tail) break;
    pos = { at: tail.at, id: tail.id };
  }
  const open = await openRead;
  const unhandled = await reduceSeq(open, async (sum, r) => sum + (await visible(r) ? r.n : 0), 0);
  // What staff wrote about a quiet pull request reaches viewers as the row only, as the decision and conversation APIs give it.
  const quiet = new Map<string, boolean>();
  const out = rows.slice(0, limit);
  return { rows: staff ? out : await mapSeq(out, async (r) => (await quietRow(db, r, quiet) ? { ...r, text: "" } : r)), more: rows.length > limit, unhandled };
}

async function view(db: Db, r: Raw): Promise<NeedsFeedbackRow> {
  const it = r.repo && r.number !== null ? (await db.get(db.k.selectFrom("items").select(["kind", "state", "title"]).where("repo", "=", r.repo).where("number", "=", r.number))) : undefined;
  const item = r.repo && r.number !== null ? { repo: r.repo, number: r.number, kind: it ? (it.kind === "pr" ? "pr" as const : "issue" as const) : null, state: it ? itemState(it.state) : null, title: it?.title ?? null } : null;
  const trace = (event: string) => (item ? `${itemPath(item.repo, item.number)}#event=${encodeURIComponent(event)}` : "/needs");
  const [href, hrefLabel] = r.source === "decision"
    ? [decisionPath({ id: r.decision!, repo: r.repo ?? "", number: null }), "Decision"]
    : r.source === "operator" ? [trace(`turn:${r.turn}`), "Conversation"] : [trace(`comment:${r.comment}`), "Comment"];
  return { id: r.id, source: r.source, item, author: r.author, at: r.at, text: r.text, href, hrefLabel, handled: !!r.handled };
}


export async function stopped(db: Db, hidden: HiddenRepos, user: Req["user"]): Promise<NeedsStoppedRow[]> {
  const out: NeedsStoppedRow[] = [];
  // One row per tripped repo of a push stage, one per push stage tripped on every repo (no repo), and the outside-author surge breaker.
  for (const t of await db.all(db.k.selectFrom("stage_trips").select(["stage", "reason", "tripped_at"]).where("active", "=", 1).orderBy("tripped_at").orderBy("stage"))) {
    const k = parseTripKey(t.stage);
    if (k && isHiddenRepo(hidden, k.repo)) continue;
    out.push({ id: `breaker:${t.stage}`, kind: "breaker", title: stepTitle(k?.stage ?? t.stage), ...(k ? { repo: k.repo } : {}), reason: blankIfHidden(t.reason, hidden), since: t.tripped_at,
      resume: { type: "trip", stage: t.stage, canResume: await mayResumeTrip(user, t.stage) } });
  }
  const updated = new Map((await db.all(db.k.selectFrom("stage_limits").select(["step", "updated_at"]))).map((r) => [r.step, r.updated_at]));
  for (const l of await zeroLimits(db))
    out.push({ id: `limit:${l.step}`, kind: "limit", title: l.title, reason: "The daily limit is 0", since: updated.get(l.step) ?? null, resume: { type: "link", to: "/pipeline" } });
  for (const a of await accountListOrNull(db) ?? [])
    if (a.enabled && a.pausedReason) out.push({ id: `account:${a.id}`, kind: "account", title: a.label, reason: blankIfHidden(a.pausedReason, hidden), since: null, resume: { type: "link", to: "/accounts" } });
  // Targets Act stopped after repeated failures, while the answer still stands: an operator retries them from here.
  for (const a of await db.all(db.k.selectFrom("main.actions as a").innerJoin("main.decisions as d", "d.id", "a.decision_id").innerJoin("main.items as i", (j) => j.onRef("i.repo", "=", "a.repo").onRef("i.number", "=", "a.number"))
    .select(["a.decision_id", "a.repo", "a.number", "a.reason", "a.created_at"]).where("a.state", "=", "stopped").where("d.answer", "is not", null).where("d.applied_at", "is", null)
    .where("d.exec_state", "is distinct from", "disputed").where((eb) => eb.not(kx.like(db, "d.id", eb.val("%~replaced-%")))).where("i.state", "=", "OPEN").orderBy("a.created_at"))) {
    if (isHiddenRepo(hidden, a.repo)) continue;
    out.push({ id: `act:${a.decision_id}:${a.number}`, kind: "act", title: `${a.repo}#${a.number}`, reason: blankIfHidden(a.reason ?? "Act stopped after repeated failures", hidden), since: a.created_at, resume: { type: "retry", decision: a.decision_id } });
  }
  // Items held for an outside author's daily limit or a surge: one group row per author, listed by `/api/needs/outside/:author`.
  // One spelling of the author per group: the smallest.
  for (const g of await db.all(db.k.selectFrom("main.outside_holds as h").leftJoin("items as i", (j) => j.onRef("i.repo", "=", "h.repo").onRef("i.number", "=", "h.number"))
    .select((eb) => [eb.fn.min<string>("h.author").as("author"), eb.fn.countAll<number>().as("n"), eb.fn.sum<number>(eb.case().when("h.kind", "=", "pr").then(eb.lit(1)).else(eb.lit(0)).end()).as("prs"),
      eb.fn.min<string | null>("i.created_at").as("since"), eb.fn.max<number>(eb.case().when("h.reason", "=", "surge").then(eb.lit(1)).else(eb.lit(0)).end()).as("surge")])
    .$if(filtered(hidden), (q) => q.where((eb) => eb.not(hiddenTestExpr(db, "h.repo", hidden)))).groupBy(kx.nocase(db, "h.author")).orderBy("n", "desc").orderBy((eb) => eb.fn.min("h.author")))) {
    const parts = [g.prs && `${g.prs} PR${g.prs === 1 ? "" : "s"}`, g.n - g.prs && `${g.n - g.prs} issue${g.n - g.prs === 1 ? "" : "s"}`].filter(Boolean).join(", ");
    out.push({ id: `outside:${g.author}`, kind: "outside-group", title: g.author, reason: `${parts} waiting for ${g.surge ? "the outside-surge breaker or " : ""}the author's daily limit`, since: g.since, count: g.n, resume: { type: "outside", author: g.author } });
  }
  // Issues whose fix attempt was skipped or failed wait on a person: one group row per repo, listed by `fixes`.
  const groups = await db.all(db.k.selectFrom("main.item_plans as p").leftJoin("fix_attempts as a", (j) => j.onRef("a.repo", "=", "p.repo").onRef("a.number", "=", "p.number"))
    .select((eb) => ["p.repo", eb.fn.countAll<number>().as("n"), eb.fn.max<string | null>(eb.fn.coalesce("a.updated_at", "p.planned_at")).as("since")])
    .where("p.kind", "=", "issue").where("p.wait", "in", [fixStoppedWait("skipped"), fixStoppedWait("failed")]).groupBy("p.repo").orderBy("n", "desc").orderBy("p.repo"));
  for (const g of groups) {
    if (isHiddenRepo(hidden, g.repo)) continue;
    out.push({ id: `fixes:${g.repo}`, kind: "fix-group", title: g.repo, reason: `${g.n} issue${g.n === 1 ? "" : "s"} with a stopped fix`, since: g.since, count: g.n, resume: { type: "fixes", repo: g.repo } });
  }
  return out;
}

export const FIXES_PAGE = 25;

const STOP_WORDS: Record<string, string> = { skipped: "was skipped", failed: "failed" };

/** One page of a repo's issues with a stopped fix: higher priority first (unprioritized last), then the newest stop, then the number. The cursor is the last row's `score|since|number`. */
export async function fixes(db: Db, req: Req): Promise<NeedsFixPage> {
  const repo = req.params.repo ?? req.query.get("repo") ?? "";
  if (isHiddenRepo(await hiddenRepos(db, req), repo)) return { rows: [], nextCursor: null };
  const c = req.query.get("cursor");
  const m = c === null ? null : /^(-?\d{1,9})\|([^|]{1,40})\|(\d{1,9})$/.exec(c);
  if (c !== null && !m) throw new HttpError(400, "bad cursor");
  const after = m ? { score: Number(m[1]), since: m[2]!, number: Number(m[3]) } : { score: 2_000_000_000, since: "", number: 0 };
  const rows = await db.all(db.k.selectFrom((eb) => eb.selectFrom("main.item_plans as p").innerJoin("items as i", (j) => j.onRef("i.repo", "=", "p.repo").onRef("i.number", "=", "p.number"))
    .leftJoin("fix_attempts as a", (j) => j.onRef("a.repo", "=", "p.repo").onRef("a.number", "=", "p.number")).leftJoin("item_priority as pr", (j) => j.onRef("pr.repo", "=", "p.repo").onRef("pr.number", "=", "p.number"))
    .select((e) => ["p.number", "i.title", "a.state", "a.reason", "a.summary", e.fn.coalesce("a.updated_at", "p.planned_at").as("since"), "pr.impact", e.fn.coalesce("pr.score", e.lit(-1)).as("score")])
    .where("p.repo", "=", repo).where("p.kind", "=", "issue").where("p.wait", "in", [fixStoppedWait("skipped"), fixStoppedWait("failed")]).as("f"))
    .select(["f.number", "f.title", "f.state", "f.reason", "f.summary", "f.since", "f.impact", "f.score"])
    .$if(after.score !== 2_000_000_000, (q) => q.where((eb) => eb.or([eb("score", "<", after.score), eb.and([eb("score", "=", after.score), eb.or([eb("since", "<", after.since), eb.and([eb("since", "=", after.since), eb("number", "<", after.number)])])])])))
    .orderBy("score", "desc").orderBy("since", "desc").orderBy("number", "desc").limit(FIXES_PAGE + 1));
  const page = rows.slice(0, FIXES_PAGE);
  const last = page[page.length - 1];
  // The attempt's reason and summary are model text: staff only, as on the item page.
  const staff = canSeeInternals(req);
  return {
    rows: page.map((r) => ({ id: `fix:${repo}#${r.number}`, repo, number: r.number, title: r.title ?? "", reason: `The fix attempt ${STOP_WORDS[r.state ?? ""] ?? "stopped"}${staff && (r.reason ?? r.summary) ? `: ${r.reason ?? r.summary}` : ""}`, since: r.since, impact: r.impact })),
    nextCursor: rows.length > FIXES_PAGE && last ? `${last.score}|${last.since}|${last.number}` : null,
  };
}

export async function needs(db: Db, req: Req): Promise<NeedsPage> {
  const hidden = await hiddenRepos(db, req);
  const q = req.query;
  const limitRaw = q.get("limit");
  if (limitRaw !== null && !/^\d{1,3}$/.test(limitRaw)) throw new HttpError(400, "limit must be a whole number");
  const limit = Math.min(Math.max(Number(limitRaw ?? NEEDS_PAGE), 1), 100);
  const c = q.get("cursor");
  let cursor: { at: string; id: string } | null = null;
  if (c) {
    const i = c.indexOf("|");
    if (i < 1) throw new HttpError(400, "bad cursor");
    cursor = { at: c.slice(0, i), id: c.slice(i + 1) };
  }
  const fb = await feedback(db, hidden, cursor, q.get("feedback") === "1" ? limit : 1, canSeeInternals(req));
  // The same filter as the Needs you list: what this person settles, plus the disputes they resolve; a batch counts only when none of its items is in a hidden repo.
  const decisions = (await db.get(db.k.selectFrom("decisions").select((eb) => eb.fn.countAll<number>().as("n")).where((eb) => needingExpr(db, eb, req))
    .$if(filtered(hidden), (q) => q.where((eb) => withoutHiddenExpr(db, eb, hidden)!))))!.n;
  const rows = await stopped(db, hidden, req.user);
  const last = fb.rows[fb.rows.length - 1];
  return {
    counts: { decisions, feedback: fb.unhandled, stopped: rows.length },
    feedback: q.get("feedback") === "1" ? { rows: await mapSeq(fb.rows, async (r) => await view(db, r)), nextCursor: fb.more && last ? `${last.at}|${last.id}` : null } : null,
    stopped: rows,
  };
}
