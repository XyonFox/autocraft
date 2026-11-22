// Conversation storage: the item block (written once), the turns in prompt order, and the cards of each Opus turn.
// Every turn stores the exact text its prompt block carries, so later prompts repeat earlier ones byte for byte.
import { isBotLogin } from "../review/bots.ts";
import { createHash, randomUUID } from "node:crypto";
import { factsOf, getDecision, optionsOf, type DecisionRow, type Kind } from "../decisions.ts";
import { CLAUDE_PROMPTS, untrustedJson } from "../prompts/index.ts";
import { applicableSteps, cleanComment, openDecision, type NewCard } from "./cards.ts";
import { choicesJson, storeParsedChoices, type ParsedChoices } from "../decide/choices-parse.ts";
import { coreSummaries } from "../decide/choices.ts";
import { ITEM_BODY_LIMIT, ITEM_COMMENT_LIMIT, ITEM_COMMENTS, ITEM_DIFF_LIMIT, MESSAGE_MAX, OPENING_MAX, PERSON_KINDS, type PersonKind, type TurnKind } from "./types.ts";
import { flatMapSeq } from "../async-seq.ts";
import { withdrawCall } from "../remote/calls.ts";
import type { Db } from "../db/index.ts";
import * as kx from "../db/kx.ts";
import type { NotNull } from "kysely";

export class ConversationError extends Error {
  status: number;
  constructor(status: number, message: string) {
    super(message);
    this.status = status;
  }
}

/** The pull request diff for the item block, or null (an issue, an unavailable or oversized diff). Injected: the default asks GitHub. */
export type DiffSource = (repo: string, number: number, head: string) => Promise<string | null>;
export type StoreDeps = { diff?: DiffSource; now?: () => Date };
const nowOf = (d?: StoreDeps) => (d?.now?.() ?? new Date()).toISOString();

export type ConversationRow = { repo: string; number: number; item_block: string; item_sig: string; created_at: string; created_by: string };
export type TurnRow = { id: number; repo: string; number: number; kind: TurnKind; login: string; at: string; body: string; block: string; status: string | null; error: string | null; notes: string | null; seq: number | null; claim: string | null; claimed_at: string | null; progress: string | null };

export const conversationOf = async (db: Db, repo: string, n: number): Promise<ConversationRow | undefined> =>
  await db.get(db.k.selectFrom("conversations").select(kx.cols("conversations")).where("repo", "=", repo).where("number", "=", n));

/** Every column of a turn, its kind read as a TurnKind. */
const turns = (db: Db) => db.k.selectFrom("conversation_turns").select(kx.cols("conversation_turns")).$narrowType<{ kind: TurnKind }>();

/** All turns in display order: the prompt order, then the message Opus is answering, then turns held until its reply. */
export const turnsOf = async (db: Db, repo: string, n: number): Promise<TurnRow[]> =>
  await db.all(turns(db).where("repo", "=", repo).where("number", "=", n)
    .orderBy((eb) => eb("seq", "is", null)).orderBy((eb) => eb(eb.fn.coalesce("status", eb.val("")), "=", "held"))
    .orderBy("seq").orderBy("id"));

/** The person's message that waits for its reply: queued or claimed by a run. */
export const pendingTurn = async (db: Db, repo: string, n: number): Promise<TurnRow | undefined> =>
  await db.get(turns(db).where("repo", "=", repo).where("number", "=", n)
    .where("kind", "in", PERSON_KINDS).where("status", "in", ["pending", "answering"]));

/** A claim older than this is released: a run makes `attempts` calls of at most the prompt timeout each. */
const STALE_CLAIM_MS = (CLAUDE_PROMPTS["conversation.turn"].limits.timeoutSeconds * CLAUDE_PROMPTS["conversation.turn"].limits.attempts + 60) * 1000;

// ---- blocks ------------------------------------------------------------------

/** Text of people and Opus must not close or open the frame the prompt puts around turns. */
const frameSafe = (t: string): string => t.replace(/<(\/?)(turn|item|actions|choices|cores|untrusted|included|steps)\b/gi, "&lt;$1$2");

export type Included = { login: string; at: string; text: string; /** An agent sent it for the login. */ viaAgent?: boolean };

const attr = (v: string): string => v.replace(/[^\w.:-]/g, "");

/** A person's turn, framed as from an operator or a maintainer. `included`: maintainers' posts an operator chose to share, carried in an operator's turn once. */
export function personBlock(from: PersonKind, login: string, text: string, included: Included[] = [], steps?: string[], cores?: unknown[]): string {
  const extra = included.map((i) => `<included from="maintainer" login="${attr(i.login)}" at="${attr(i.at)}"${i.viaAgent ? ' via="agent"' : ""}>\n${frameSafe(i.text)}\n</included>\n`).join("");
  return `<turn from="${from}" login="${attr(login)}">\n${frameSafe(text)}\n${extra}${cores ? `<cores>${JSON.stringify(cores)}</cores>\n` : ""}${steps ? `<steps>${JSON.stringify(steps)}</steps>\n` : ""}</turn>\n`;
}

/** The steps list Opus last saw: the newest `<steps>` line of the stored turns, else the item block's. */
async function seenSteps(db: Db, repo: string, n: number): Promise<string[] | null> {
  const parse = (j: string): string[] | null => { try { const v = JSON.parse(j) as unknown; return Array.isArray(v) ? v.map(String) : null; } catch { return null; } };
  for (const t of [...await turnsOf(db, repo, n)].reverse()) {
    if (!PERSON_KINDS.includes(t.kind as PersonKind)) continue;
    const m = /\n<steps>(\[[^\n]*\])<\/steps>\n<\/turn>\n$/.exec(t.block);
    if (m) return parse(m[1]!);
  }
  const m = /"steps": (\[[^\]]*\])/.exec((await conversationOf(db, repo, n))?.item_block ?? "");
  return m ? parse(m[1]!) : null;
}

/** The cores Opus last saw for the item's open decision: the newest `<cores>` line of the stored people's turns, else the item block's. Null: none known. */
async function seenCores(db: Db, repo: string, n: number): Promise<unknown[] | null> {
  const parse = (j: string): unknown[] | null => { try { const v = JSON.parse(j) as unknown; return Array.isArray(v) ? v : null; } catch { return null; } };
  for (const t of [...await turnsOf(db, repo, n)].reverse()) {
    if (!PERSON_KINDS.includes(t.kind as PersonKind)) continue;
    const m = /\n<cores>(\[[^\n]*\])<\/cores>\n(?:<steps>[^\n]*<\/steps>\n)?<\/turn>\n$/.exec(t.block);
    if (m) return parse(m[1]!);
  }
  try {
    const decision = (JSON.parse((await conversationOf(db, repo, n))?.item_block ?? "null") as { decision?: { cores?: unknown } } | null)?.decision;
    return Array.isArray(decision?.cores) ? decision.cores : null;
  } catch { return null; }
}

export function opusBlock(text: string, cards: NewCard[], choices?: ParsedChoices | null): string {
  const list = cards.map((c) => ({ ...c.params, ...(c.params.kind === "resolution" && c.text !== null && { text: c.text }) }));
  return `<turn from="opus">\n${frameSafe(text)}\n${cards.length ? `<actions>\n${JSON.stringify(list)}\n</actions>\n` : ""}${choices ? `<choices>\n${choicesJson(choices)}\n</choices>\n` : ""}</turn>\n`;
}

/** A turn from our code: a plain sentence and, when GitHub text is quoted, that data as untrusted JSON. */
export function systemBlock(text: string, data?: unknown): string {
  return `<turn from="system">\n${text}\n${data === undefined ? "" : `<untrusted>\n${untrustedJson(data)}\n</untrusted>\n`}</turn>\n`;
}

// ---- the item ----------------------------------------------------------------

type ItemRow = { repo: string; number: number; kind: string; state: string; title: string | null; body: string | null; author: string | null; author_association: string | null; labels: string | null; head_oid: string | null; additions: number | null; deletions: number | null; changed_files: number | null };

const itemRow = async (db: Db, repo: string, n: number): Promise<ItemRow | undefined> =>
  await db.get(db.k.selectFrom("items").select(["repo", "number", "kind", "state", "title", "body", "author", "author_association", "labels", "head_oid", "additions", "deletions", "changed_files"])
    .where("repo", "=", repo).where("number", "=", n));

const hash = (v: unknown, n = 16): string => createHash("sha256").update(JSON.stringify(v)).digest("hex").slice(0, n);

const labelsOf = (i: { labels: string | null }): string[] => {
  try { const l = JSON.parse(i.labels ?? "[]") as unknown; return Array.isArray(l) ? l.filter((x): x is string => typeof x === "string") : []; } catch { return []; }
};

/** What a block or update turn described of the item; a change in any field sends an update turn. */
type Snapshot = { head: string | null; title: string; body: string; state: string; labels: string[]; comments: number; last_comment: string | null; decision: string | null; answer: string | null };

// The app's bot (the mirror stores its bare slug) is the pipeline itself: its posts are no news for the conversation.
const countedComments = (db: Db, repo: string, n: number) => db.k.selectFrom("comments").where("repo", "=", repo).where("number", "=", n)
  .where((eb) => {
    const author = eb.fn.coalesce("author", eb.val(""));
    const slugs = eb.fn.coalesce(eb.selectFrom("main.settings").select("value").where("key", "=", "app_slugs"), eb.val("[]"));
    return eb.and([
      eb(eb.fn.coalesce(eb.fn<string>("trim", ["body"]), eb.val("")), "<>", ""),
      eb.not(kx.like(db, author, eb.val("%[bot]"))),
      eb(author, "not in", eb.selectFrom(kx.jsonEach(db, slugs, "s")).select("s.value")),
    ]);
  });

async function snapshotOf(db: Db, i: ItemRow): Promise<Snapshot> {
  const cnt = (await db.get(countedComments(db, i.repo, i.number).select((eb) => eb.fn.countAll<number>().as("n"))))!;
  const last = await db.get(countedComments(db, i.repo, i.number).select("id").orderBy("created_at", (o) => o.desc().nullsLast()).orderBy("id", "desc").limit(1));
  const d = await openDecision(db, i.repo, i.number) ?? await latestDecision(db, i.repo, i.number);
  return { head: i.head_oid, title: hash(i.title), body: hash(i.body), state: i.state, labels: labelsOf(i).sort(), comments: Number(cnt.n), last_comment: last?.id ?? null, decision: d?.id ?? null, answer: d?.answer ?? null };
}

const sigOf = async (db: Db, i: ItemRow): Promise<string> => JSON.stringify(await snapshotOf(db, i));

const parseSig = (sig: string): Snapshot | null => {
  try { const v = JSON.parse(sig) as Snapshot; return v && typeof v === "object" ? v : null; } catch { return null; }
};

/** The head and newest comment Opus had been told about (the conversation's item snapshot), recorded on each card it proposes. */
async function seenOf(db: Db, repo: string, n: number): Promise<string | null> {
  const c = await conversationOf(db, repo, n);
  const s = c ? parseSig(c.item_sig) : null;
  return s ? JSON.stringify({ head: s.head, last_comment: s.last_comment }) : null;
}

/** Whether a card still proposed was written for an older item: a push since (it refuses to run) or new GitHub comments since (it still runs). */
export async function staleOf(db: Db, repo: string, n: number, seen: string | null): Promise<"pr-changed" | "new-comments" | undefined> {
  const i = await itemRow(db, repo, n);
  if (!seen || !i) return undefined;
  let was: { head: string | null; last_comment: string | null };
  try { was = JSON.parse(seen) as typeof was; } catch { return undefined; }
  if (i.kind === "pr" && (was.head ?? null) !== i.head_oid) return "pr-changed";
  if (was.last_comment === (await snapshotOf(db, i)).last_comment) return undefined;
  return await newerPersonComment(db, i.repo, i.number, was.last_comment) ? "new-comments" : undefined;
}

/**
 * Whether a person commented after the comment a card was written against. Stricter than commentsWhere, which also
 * drives update turns and must not change: review bots and our own posts (made through an account a person also
 * uses, so recognised by their note line, never by login) are news for Opus but do not make a decision card stale.
 * When the seen comment is gone, any change in the newest counted comment counts, as before.
 */
async function newerPersonComment(db: Db, repo: string, n: number, seenId: string | null): Promise<boolean> {
  const at = seenId === null ? null : await db.get(db.k.selectFrom("comments").select(["created_at", "id"]).where("repo", "=", repo).where("number", "=", n).where("id", "=", seenId));
  if (seenId !== null && !at) return true;
  const rows = await db.all(countedComments(db, repo, n).select(["author", "body"]).$narrowType<{ body: NotNull }>()
    .$if(!!at, (q) => q.where((eb) => eb.or([eb("created_at", ">", at!.created_at), eb.and([eb("created_at", "=", at!.created_at), eb("id", ">", at!.id)])]))));
  return rows.some((r) => !(r.author && isBotLogin(r.author)) && !OWN_NOTE_PREFIXES.some((p) => r.body.trimStart().startsWith(p)));
}

// Openings of the note lines in src/github/note.ts.
export const OWN_NOTE_PREFIXES = ["_AI-generated ", "_Automated "];

const cut = (s: string | null, max: number): string => (s ?? "").slice(0, max);

async function decisionPart(db: Db, d: DecisionRow | undefined): Promise<unknown> {
  if (!d) return null;
  return {
    question: d.question, facts: factsOf(d), summary: d.summary ?? "",
    options: optionsOf(d).map((o) => ({ key: o.key, label: o.label, hint: o.hint ?? "" })),
    cores: await coreSummaries(db, d),
    recommended: d.recommended ?? null, recommended_reason: d.recommended_reason ?? null, answer: d.answer ?? null,
  };
}

/** The item block: everything about the item the conversation starts from, as untrusted JSON. Written once. */
export async function buildItemBlock(db: Db, repo: string, n: number, deps?: StoreDeps): Promise<{ block: string; sig: string }> {
  const i = await itemRow(db, repo, n);
  if (!i) throw new ConversationError(404, `no item ${repo}#${n} in the mirror`);
  const pr = i.kind === "pr";
  const comments = await db.all(countedComments(db, repo, n).select(["author", "body"]).orderBy("created_at", (o) => o.asc().nullsFirst()).orderBy("id").limit(ITEM_COMMENTS));
  let diff: string | null = null;
  if (pr && i.head_oid && deps?.diff) diff = await deps.diff(repo, n, i.head_oid).catch(() => null);
  const data = {
    ref: `${repo}#${n}`, kind: pr ? "pull request" : "issue", state: i.state, title: i.title ?? "", body: cut(i.body, ITEM_BODY_LIMIT),
    author: i.author, author_association: i.author_association, labels: labelsOf(i),
    comments: comments.map((c) => ({ author: c.author, body: cut(c.body, ITEM_COMMENT_LIMIT) })),
    ...(pr ? { head: i.head_oid, lines: { additions: i.additions, deletions: i.deletions, files: i.changed_files }, diff: diff === null ? null : cut(diff, ITEM_DIFF_LIMIT) + (diff.length > ITEM_DIFF_LIMIT ? "\n[diff cut]" : "") } : {}),
    decision: await decisionPart(db, await openDecision(db, repo, n) ?? await latestDecision(db, repo, n)),
    steps: await applicableSteps(db, repo, n),
  };
  return { block: untrustedJson(data), sig: await sigOf(db, i) };
}

const latestDecision = async (db: Db, repo: string, n: number): Promise<DecisionRow | undefined> =>
  await db.get(db.k.selectFrom("decisions").select(kx.cols("decisions")).$narrowType<{ kind: Kind }>().where("repo", "=", repo).where("number", "=", n)
    .where((eb) => eb.not(kx.like(db, "id", eb.val("%:dupegroup:%")))).orderBy("asked_at", "desc").orderBy("id", "desc").limit(1));

/** Creates the conversation with its item block when the item has none. Returns whether it was created. */
export async function ensureConversation(db: Db, repo: string, n: number, by: string, deps?: StoreDeps): Promise<boolean> {
  if (await conversationOf(db, repo, n)) return false;
  const { block, sig } = await buildItemBlock(db, repo, n, deps);
  return (await db.run(db.k.insertInto("conversations").values({ repo, number: n, item_block: block, item_sig: sig, created_at: nowOf(deps), created_by: by })
    .onConflict((oc) => oc.doNothing()))).changes === 1;
}

// ---- turns -------------------------------------------------------------------

/** Validated choices an Opus turn carries; stored with the turn (a prepare replaces the decision's choices, a conversation turn merges into them). */
export type TurnChoices = { decision: DecisionRow; parsed: ParsedChoices; source: "prepare" | "conversation"; by?: string; stampThen?: string | null; /** The login whose message the reply answered. */ askedBy?: string };

type NewTurn = { kind: TurnKind; login: string; body: string; block: string; status?: string | null; notes?: string[]; cards?: NewCard[]; choices?: TurnChoices };

const nextSeq = async (db: Db, repo: string, n: number): Promise<number> =>
  Number((await db.get(db.k.selectFrom("conversation_turns").select((eb) => eb(eb.fn.coalesce(eb.fn.max("seq"), eb.lit(0)), "+", 1).as("s")).where("repo", "=", repo).where("number", "=", n)))!.s);

async function inTx<T>(db: Db, f: () => Promise<T>): Promise<T> {
  return await db.write(async () => {
    const r = await f();
    return r;
  });
}

async function insertRow(db: Db, repo: string, n: number, t: NewTurn, seq: number | null, status: string | null, deps?: StoreDeps, seenAt?: string | null): Promise<number> {
  const id = Number((await db.get(db.k.insertInto("conversation_turns")
    .values({ repo, number: n, kind: t.kind, login: t.login, at: nowOf(deps), body: t.body, block: t.block, status, error: null, notes: t.notes?.length ? JSON.stringify(t.notes) : null, seq })
    .returning("id")))!.id);
  // One current set: a new Opus turn replaces the proposals of the earlier ones.
  if (t.kind === "opus") await db.run(db.k.updateTable("conversation_cards").set({ state: "replaced" }).where("repo", "=", repo).where("number", "=", n).where("state", "=", "proposed"));
  // A reply's cards are stamped with what Opus was told when its prompt was built, not with a later refresh.
  const seen = seenAt !== undefined ? seenAt : await seenOf(db, repo, n);
  for (const c of t.cards ?? []) {
    await db.run(db.k.insertInto("conversation_cards")
      .values({ turn_id: id, repo, number: n, kind: c.params.kind, params: JSON.stringify(c.params), text: c.text, description: c.description, seen }));
  }
  if (t.choices) await storeParsedChoices(db, t.choices.decision, t.choices.parsed, { turnId: id, seen, source: t.choices.source, by: t.choices.by, stampThen: t.choices.stampThen, ...(t.choices.askedBy && { askedBy: t.choices.askedBy }) });
  return id;
}

/**
 * Appends a turn inside the caller's transaction. A turn has a stable id and takes the next seq. While a run is answering the
 * message, a system turn is held without a seq (the run already sent the prompt without it) and is placed after the reply.
 * An operator message has no seq until it is answered; only one may wait per conversation.
 */
async function appendTurn(db: Db, repo: string, n: number, t: NewTurn, deps?: StoreDeps): Promise<number> {
  const wait = await pendingTurn(db, repo, n);
  if (t.status === "pending") {
    if (wait) throw new ConversationError(409, "Opus has not answered the last message yet");
    return await insertRow(db, repo, n, t, null, "pending", deps);
  }
  if (wait?.status === "answering") return await insertRow(db, repo, n, t, null, "held", deps);
  return await insertRow(db, repo, n, t, await nextSeq(db, repo, n), t.status ?? null, deps);
}

/** Appends a turn (see appendTurn). Returns the turn id. */
export async function insertTurn(db: Db, repo: string, n: number, t: NewTurn, deps?: StoreDeps): Promise<number> {
  return await inTx(db, async () => await appendTurn(db, repo, n, t, deps));
}

const NOTHING: Snapshot = { head: null, title: "", body: "", state: "", labels: [], comments: -1, last_comment: null, decision: null, answer: null };

/**
 * Adds a system turn carrying what changed in the item since the stored block or the last update turn described it: head commit,
 * title or body, state, labels, comments, the open decision and its answer. The block itself is never rewritten.
 */
export async function refreshItem(db: Db, repo: string, n: number, deps?: StoreDeps): Promise<boolean> {
  const c = await conversationOf(db, repo, n), i = await itemRow(db, repo, n);
  if (!c || !i) return false;
  const sig = await sigOf(db, i);
  if (sig === c.item_sig) return false;
  const was = parseSig(c.item_sig) ?? NOTHING, now = JSON.parse(sig) as Snapshot;
  const pr = i.kind === "pr";
  const changed: string[] = [];
  const data: Record<string, unknown> = {};
  if (pr && was.head !== now.head) {
    changed.push("head commit");
    let diff: string | null = null;
    if (i.head_oid && deps?.diff) diff = await deps.diff(repo, n, i.head_oid).catch(() => null);
    Object.assign(data, { head: i.head_oid, lines: { additions: i.additions, deletions: i.deletions, files: i.changed_files }, diff: diff === null ? null : cut(diff, ITEM_DIFF_LIMIT) + (diff.length > ITEM_DIFF_LIMIT ? "\n[diff cut]" : "") });
  }
  if (was.title !== now.title) { changed.push("title"); data.title = i.title ?? ""; }
  if (was.body !== now.body) { changed.push("text"); data.body = cut(i.body, ITEM_BODY_LIMIT); }
  if (was.state !== now.state) { changed.push("state"); data.state = i.state; }
  if (JSON.stringify(was.labels) !== JSON.stringify(now.labels)) { changed.push("labels"); data.labels = labelsOf(i); }
  if (was.comments !== now.comments || was.last_comment !== now.last_comment) {
    changed.push("comments");
    const rows = await db.all(countedComments(db, repo, n).select(["author", "body"]).orderBy("created_at", (o) => o.desc().nullsLast()).orderBy("id", "desc").limit(3));
    data.comments = { total: now.comments, newest: rows.reverse().map((r) => ({ author: r.author, body: cut(r.body, ITEM_COMMENT_LIMIT) })) };
  }
  if (was.decision !== now.decision || was.answer !== now.answer) {
    changed.push("decision");
    data.decision = await decisionPart(db, await openDecision(db, repo, n) ?? await latestDecision(db, repo, n));
  }
  const text = `The ${pr ? "PR" : "issue"} was updated (${changed.join(", ")}). The current values of these fields follow; they replace the item block where they differ.`;
  return await inTx(db, async () => {
    // Another run may have written the same update while the diff was fetched.
    if ((await conversationOf(db, repo, n))?.item_sig !== c.item_sig) return false;
    await appendTurn(db, repo, n, { kind: "system", login: "system", body: text, block: systemBlock(text, data) }, deps);
    await db.run(db.k.updateTable("conversations").set({ item_sig: sig }).where("repo", "=", repo).where("number", "=", n));
    return true;
  });
}

/**
 * A person's message (`by.kind`: an operator's or a maintainer's): creates the conversation when the item has none and queues
 * the message for the Conversation step, which answers a maintainer's on the maintainer's own Task Runner. With `ask` false the
 * message is only stored (no pending state, no model call); Opus sees it among the turns at the next ask. The maintainer posts
 * an operator shared ride in the next asked operator message.
 */
export async function addMessage(db: Db, repo: string, n: number, by: { login: string; kind: PersonKind }, text: string, deps?: StoreDeps, ask = true): Promise<number> {
  const login = by.login;
  const body = text.trim();
  if (!body) throw new ConversationError(400, "the message is empty");
  if (body.length > MESSAGE_MAX) throw new ConversationError(400, `the message is longer than ${MESSAGE_MAX} characters`);
  if (!await itemRow(db, repo, n)) throw new ConversationError(404, `no item ${repo}#${n} in the mirror`);
  if (ask && await pendingTurn(db, repo, n)) throw new ConversationError(409, "Opus has not answered the last message yet");
  await ensureConversation(db, repo, n, login, deps);
  await refreshItem(db, repo, n, deps);
  const shared = ask && by.kind === "operator" ? await unsentIncludes(db, repo, n) : [];
  try {
    return await inTx(db, async () => {
      const now = await applicableSteps(db, repo, n), seen = await seenSteps(db, repo, n);
      const steps = seen && JSON.stringify(seen) === JSON.stringify(now) ? undefined : now;
      const d = await openDecision(db, repo, n), nowCores = d ? await coreSummaries(db, d) : [], seenC = await seenCores(db, repo, n);
      const cores = JSON.stringify(seenC ?? []) === JSON.stringify(nowCores) ? undefined : nowCores;
      const id = await appendTurn(db, repo, n, { kind: by.kind, login, body, block: personBlock(by.kind, login, body, shared.map((x) => x.post), steps, cores), ...(ask && { status: "pending" }) }, deps);
      for (const x of shared) await db.run(db.k.updateTable("conversation_includes").set({ sent_turn: id }).where("repo", "=", repo).where("number", "=", n).where("source", "=", x.source).where("sent_turn", "is", null));
      return id;
    });
  } catch (e) {
    if (/UNIQUE/i.test(String((e as Error).message))) throw new ConversationError(409, "Opus has not answered the last message yet");
    throw e;
  }
}

// ---- maintainer posts shared with Opus ---------------------------------------

export type IncludeRow = { source: string; by: string; at: string; sent: boolean };

/** The stored post behind an include source of this item, or null. Only decision comments and opinions on the item's own decisions can be shared. */
async function postOf(db: Db, repo: string, n: number, source: string): Promise<Included | null> {
  const m = /^(decision-comment|decision-opinion):(.+)$/.exec(source);
  if (!m) return null;
  if (m[1] === "decision-comment") {
    const row = await db.get(db.k.selectFrom("decision_comments as c").innerJoin("decisions as d", "d.id", "c.decision_id")
      .select(["c.login", "c.at", "c.body as text", "c.via"]).where("c.id", "=", m[2]!).where("d.repo", "=", repo).where("d.number", "=", n));
    const text = row?.text ? cleanComment(row.text).slice(0, ITEM_COMMENT_LIMIT) : "";
    return row && text ? { login: row.login, at: row.at, text, viaAgent: row.via === "mcp" } : null;
  }
  const o = await db.get(db.k.selectFrom("decision_opinions as o").innerJoin("decisions as d", "d.id", "o.decision_id")
    .select(["o.login", "o.at", "o.option", "o.note", "o.decision_id", "o.via"]).where("o.id", "=", m[2]!).where("d.repo", "=", repo).where("d.number", "=", n).where("o.retracted_at", "is", null));
  if (!o) return null;
  const d = await getDecision(db, o.decision_id);
  const label = o.option ? (d ? optionsOf(d).find((x) => x.key === o.option)?.label : undefined) ?? o.option : null;
  const note = o.note ? cleanComment(o.note) : "";
  const text = (label ? (note ? `Suggests ${label}: ${note}` : `Suggests ${label}`) : note).slice(0, ITEM_COMMENT_LIMIT);
  return text ? { login: o.login, at: o.at, text, viaAgent: o.via === "mcp" } : null;
}

async function unsentIncludes(db: Db, repo: string, n: number): Promise<{ source: string; post: Included }[]> {
  const rows = await db.all(db.k.selectFrom("conversation_includes").select("source").where("repo", "=", repo).where("number", "=", n).where("sent_turn", "is", null).orderBy("at").orderBy("source"));
  return await flatMapSeq(rows, async (r) => { const post = await postOf(db, repo, n, r.source); return post ? [{ source: r.source, post }] : []; });
}

export async function includesOf(db: Db, repo: string, n: number): Promise<IncludeRow[]> {
  const rows = await db.all(db.k.selectFrom("conversation_includes").select(["source", "by", "at", "sent_turn"]).where("repo", "=", repo).where("number", "=", n).orderBy("at").orderBy("source"));
  return rows.map((r) => ({ source: r.source, by: r.by, at: r.at, sent: r.sent_turn !== null }));
}

/** Shares (`on`) or stops sharing a maintainer post with Opus. A post already sent stays sent. */
export async function setInclude(db: Db, repo: string, n: number, source: string, on: boolean, by: string, deps?: StoreDeps): Promise<void> {
  const row = await db.get(db.k.selectFrom("conversation_includes").select("sent_turn").where("repo", "=", repo).where("number", "=", n).where("source", "=", source));
  if (row?.sent_turn != null) throw new ConversationError(409, "already sent to Opus");
  if (!on) { await db.run(db.k.deleteFrom("conversation_includes").where("repo", "=", repo).where("number", "=", n).where("source", "=", source)); return; }
  if (row) return;
  if (!await itemRow(db, repo, n)) throw new ConversationError(404, `no item ${repo}#${n} in the mirror`);
  if (!await postOf(db, repo, n, source)) throw new ConversationError(400, "no such post on this item's questions");
  await db.run(db.k.insertInto("conversation_includes").values({ repo, number: n, source, by, at: nowOf(deps) }));
}

/** Opus's opening turn (a question or proposal from the recommendation). Does nothing when the item already has a conversation. */
export async function startFromOpus(db: Db, repo: string, n: number, opening: string, deps?: StoreDeps): Promise<boolean> {
  const text = opening.replace(/\r\n/g, "\n").trim().slice(0, OPENING_MAX);
  if (!text || await conversationOf(db, repo, n) || !await itemRow(db, repo, n)) return false;
  if (!(await ensureConversation(db, repo, n, "opus", deps))) return false;
  await insertTurn(db, repo, n, { kind: "opus", login: "opus", body: text, block: opusBlock(text, []) }, deps);
  return true;
}

/** The message Opus is answering and the token of the run that holds it. */
export type Claim = { turn: TurnRow; claim: string };

/**
 * Takes the queued message of the conversation for one run, or null when none is queued or another run holds it.
 * A claim older than the prompt timeouts is released first, so a crashed run does not block the conversation.
 */
export async function claimPending(db: Db, repo: string, n: number, deps?: StoreDeps): Promise<Claim | null> {
  const at = nowOf(deps);
  const stale = new Date(Date.parse(at) - STALE_CLAIM_MS).toISOString();
  return await inTx(db, async () => {
    await db.run(db.k.updateTable("conversation_turns").set({ status: "pending", claim: null, claimed_at: null, progress: null })
      .where("repo", "=", repo).where("number", "=", n).where("kind", "in", PERSON_KINDS).where("status", "=", "answering").where("claimed_at", "<", stale));
    // A run that died while answering left system turns held without a seq; the next prompt must include them.
    if (!await db.get(db.k.selectFrom("conversation_turns").select("id").where("repo", "=", repo).where("number", "=", n).where("status", "=", "answering").limit(1))) {
      for (const h of await db.all(db.k.selectFrom("conversation_turns").select("id").where("repo", "=", repo).where("number", "=", n).where("status", "=", "held").orderBy("id")))
        await db.run(db.k.updateTable("conversation_turns").set({ status: null, seq: await nextSeq(db, repo, n) }).where("id", "=", h.id));
    }
    const claim = randomUUID();
    const r = await db.run(db.k.updateTable("conversation_turns").set({ status: "answering", claim, claimed_at: at })
      .where("repo", "=", repo).where("number", "=", n).where("kind", "in", PERSON_KINDS).where("status", "=", "pending"));
    if (r.changes !== 1) return null;
    return { turn: (await db.get(turns(db).where("claim", "=", claim)))!, claim };
  });
}

/** Stores what Opus is doing on the message the run holds (newest line last); the live topic pushes it. Cleared when the reply or the failure is stored. */
export async function setTurnProgress(db: Db, repo: string, n: number, claim: string, lines: string[]): Promise<void> {
  await db.run(db.k.updateTable("conversation_turns").set({ progress: JSON.stringify(lines) })
    .where("repo", "=", repo).where("number", "=", n).where("status", "=", "answering").where("claim", "=", claim));
}

/** The pieces of the next prompt: the stored item block and the turns in prompt order, the claimed message last. */
export async function promptParts(db: Db, repo: string, n: number, wait: TurnRow): Promise<{ item: string; turns: string; earlier: string; message: string; seen: string | null } | null> {
  const c = await conversationOf(db, repo, n);
  if (!c) return null;
  const earlier = (await db.all(db.k.selectFrom("conversation_turns").select("block").where("repo", "=", repo).where("number", "=", n).where("seq", "is not", null).orderBy("seq"))).map((t) => t.block).join("");
  return { item: c.item_block, turns: earlier + wait.block, earlier, message: wait.block, seen: await seenOf(db, repo, n) };
}

/** Gives the claimed message its place in the prompt order, then the turns held while it was answered, in the order they arrived. */
async function settleWait(db: Db, repo: string, n: number, wait: TurnRow, status: string, error: string | null, between?: () => Promise<void>): Promise<void> {
  await db.run(db.k.updateTable("conversation_turns").set({ status, error, seq: await nextSeq(db, repo, n), claim: null, progress: null }).where("id", "=", wait.id));
  await between?.();
  const held = await db.all(db.k.selectFrom("conversation_turns").select("id").where("repo", "=", repo).where("number", "=", n).where("status", "=", "held").orderBy("id"));
  for (const h of held) await db.run(db.k.updateTable("conversation_turns").set({ status: null, seq: await nextSeq(db, repo, n) }).where("id", "=", h.id));
}

const claimed = async (db: Db, repo: string, n: number, claim: string): Promise<TurnRow | undefined> =>
  await db.get(turns(db).where("repo", "=", repo).where("number", "=", n).where("status", "=", "answering").where("claim", "=", claim));

/**
 * Stores Opus's reply to the claimed message and marks the message answered, in one transaction. Returns the reply's turn id,
 * or null when the run no longer holds the message (its claim was released and taken by another run).
 */
export async function storeReply(db: Db, repo: string, n: number, claim: string, text: string, cards: NewCard[], notes: string[], deps?: StoreDeps, seen?: string | null, choices?: TurnChoices): Promise<number | null> {
  return await inTx(db, async () => {
    const wait = await claimed(db, repo, n, claim);
    if (!wait) return null;
    let id = 0;
    await settleWait(db, repo, n, wait, "answered", null, async () => { id = await insertRow(db, repo, n, { kind: "opus", login: "opus", body: text, block: opusBlock(text, cards, choices?.parsed), notes, cards, ...(choices && { choices }) }, await nextSeq(db, repo, n), null, deps, seen); });
    return id;
  });
}

/**
 * Withdraws the maintainer's message that waits for Opus, queued or being answered: it ends as failed, naming who withdrew it,
 * and the item takes messages again; a reply that arrives later is not stored. `by`: an operator, or the maintainer who wrote it.
 */
export async function withdrawTurn(db: Db, repo: string, n: number, by: { login: string; operator: boolean }): Promise<void> {
  await inTx(db, async () => {
    const wait = await pendingTurn(db, repo, n);
    if (!wait || wait.kind !== "maintainer") throw new ConversationError(409, "no maintainer's message waits for Opus here");
    if (!by.operator && wait.login.toLowerCase() !== by.login.toLowerCase()) throw new ConversationError(403, "only an operator, or the person who wrote it, withdraws a message");
    const why = `withdrawn by ${by.login}`;
    await settleWait(db, repo, n, wait, "failed", why);
    // The turn's model call (and its follow-ups) is bound to the writer's login: a queued one is dropped and a leased one is cancelled,
    // so the waiting job gets a failed outcome at its next poll instead of running Opus for a message nobody waits on.
    const live = await db.all(db.k.selectFrom("main.remote_calls").select(["id", "items", "for_login"]).where("prompt_id", "=", "conversation.turn").where("repo", "=", repo).where("state", "in", ["queued", "leased"]));
    for (const c of live) {
      if (c.for_login?.toLowerCase() !== wait.login.toLowerCase() || !(JSON.parse(c.items) as { repo: string; number: number }[]).some((i) => i.repo === repo && i.number === n)) continue;
      await withdrawCall(db, Number(c.id), why);
    }
  });
}

/** Gives the claimed message back: it waits, pending, for a later run. */
export async function releaseTurn(db: Db, repo: string, n: number, claim: string): Promise<boolean> {
  const r = await db.run(db.k.updateTable("conversation_turns").set({ status: "pending", claim: null, claimed_at: null, progress: null })
    .where("repo", "=", repo).where("number", "=", n).where("status", "=", "answering").where("claim", "=", claim));
  return Number(r.changes) === 1;
}

export async function failTurn(db: Db, repo: string, n: number, claim: string, error: string): Promise<boolean> {
  return await inTx(db, async () => {
    const wait = await claimed(db, repo, n, claim);
    if (!wait) return false;
    await settleWait(db, repo, n, wait, "failed", error.slice(0, 300));
    return true;
  });
}

/** Conversations with a message waiting for Opus (queued, or claimed by a run that went stale), oldest message first, with who wrote it. */
export async function pendingConversations(db: Db, deps?: StoreDeps, kind?: PersonKind): Promise<{ repo: string; number: number; turnId: number; kind: PersonKind; login: string }[]> {
  const stale = new Date(Date.parse(nowOf(deps)) - STALE_CLAIM_MS).toISOString();
  return await db.all(db.k.selectFrom("conversation_turns").select(["repo", "number", "id as turnId", "kind", "login"]).$narrowType<{ kind: PersonKind }>().where("kind", "in", kind ? [kind] : PERSON_KINDS)
    .where((eb) => eb.or([eb("status", "=", "pending"), eb.and([eb("status", "=", "answering"), eb("claimed_at", "<", stale)])])).orderBy("id"));
}
