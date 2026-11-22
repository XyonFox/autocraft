// The work of the item steps (classify, referee, prioritize, embed, dupes, decide, label, act, fix issue and their collect), taken from
// the item Plans: facts are loaded once, decideItem decides, and the items whose plan holds the action come back in the order the
// step works in. The steps keep their own work; none of them selects items on its own any more.
import type { Item } from "../classify/parse.ts";
import { decideItem } from "./decide-item.ts";
import { inFlightNow } from "./select.ts";
import { loadItemFacts, type ItemLoadOptions } from "./issue-facts.ts";
import type { PlanSet } from "./plan-set.ts";
import type { Action, ActionKind, ItemFacts, ItemPlan } from "./plan.ts";
import type { Db } from "../db/index.ts";
import * as kx from "../db/kx.ts";

export type PlannedItem = { facts: ItemFacts; plan: ItemPlan; action: Action };

export type PlannedOptions = ItemLoadOptions & {
  /** Only actions of this bucket (the action's `bucket` param, else its `source` param). */
  bucket?: string;
  /** Plans decided already (the planner's pass): no facts are loaded. */
  plans?: PlanSet;
};

const bucketOf = (a: Action): string | null => {
  const p = a.params ?? {};
  return typeof p.bucket === "string" ? p.bucket : typeof p.source === "string" ? p.source : null;
};

type Cmp = (a: PlannedItem, b: PlannedItem) => number;
type Keys = { created: string; updated: string };

const byRef: Cmp = (a, b) => (a.facts.repo < b.facts.repo ? -1 : a.facts.repo > b.facts.repo ? 1 : a.facts.number - b.facts.number);
const then = (...c: Cmp[]): Cmp => (a, b) => { for (const f of c) { const r = f(a, b); if (r) return r; } return 0; };
const str = (x: string, y: string) => (x < y ? -1 : x > y ? 1 : 0);
/** Highest score first, items without one last. */
const score: Cmp = (a, b) => (a.facts.score === null ? 1 : 0) - (b.facts.score === null ? 1 : 0) || (b.facts.score ?? 0) - (a.facts.score ?? 0);
const prFirst: Cmp = (a, b) => (b.facts.kind === "pr" ? 1 : 0) - (a.facts.kind === "pr" ? 1 : 0);

/** The sort keys of the items, keyed by `repo#number`: one query. */
async function sortKeysOf(db: Db, items: { repo: string; number: number }[]): Promise<Map<string, Keys>> {
  const rows = await db.all(db.k.selectFrom("items as i")
    .innerJoin(kx.refPairs(db, items.map((x) => [x.repo, x.number] as const), "j"), (j) => j.onRef("j.repo", "=", "i.repo").onRef("j.number", "=", "i.number"))
    .select((eb) => [eb(eb("i.repo", "||", eb.val("#")), "||", kx.exprOf(eb, "i.number")).as("ref"), eb.fn.coalesce("i.created_at", eb.val("")).as("created"), eb.fn.coalesce("i.updated_at", eb.val("")).as("updated")]));
  return new Map(rows.map((r) => [r.ref, { created: r.created, updated: r.updated }]));
}

/** The sort keys of a PlanSet's items, read on first use for every item with an action (one query for every kind) and kept with the set. */
const planKeys = new WeakMap<PlanSet, Map<string, Keys>>();
async function planSortKeys(db: Db, plans: PlanSet, items: { repo: string; number: number }[]): Promise<Map<string, Keys>> {
  let m = planKeys.get(plans);
  if (!m) planKeys.set(plans, (m = await sortKeysOf(db, plans.items.filter((p) => p.plan.actions.length).map((p) => p.facts))));
  const missing = new Map<string, { repo: string; number: number }>();
  for (const x of items) if (!m.has(`${x.repo}#${x.number}`)) missing.set(`${x.repo}#${x.number}`, x);
  if (missing.size) for (const [k, v] of await sortKeysOf(db, [...missing.values()])) m.set(k, v);
  return m;
}

/** Items the Plans say are due for `kind`, one entry per due action. */
export async function plannedItems(db: Db, kind: ActionKind, opts: PlannedOptions = {}): Promise<PlannedItem[]> {
  const out: PlannedItem[] = [];
  let source: Iterable<{ facts: ItemFacts; plan: ItemPlan }>;
  if (opts.plans) {
    const refs = opts.refs ? new Set(opts.refs) : null;
    source = opts.plans.items.filter((p) => (!opts.repo || p.facts.repo === opts.repo) && (!refs || refs.has(`${p.facts.repo}#${p.facts.number}`)));
  } else source = (await loadItemFacts(db, opts)).map((facts) => ({ facts, plan: decideItem(facts) }));
  for (const { facts, plan } of source) {
    for (const action of plan.actions) if (action.kind === kind && (opts.bucket === undefined || bucketOf(action) === opts.bucket)) out.push({ facts, plan, action });
  }
  if (out.length < 2) return out;
  const stored = opts.plans ? await planSortKeys(db, opts.plans, out.map((p) => p.facts)) : await sortKeysOf(db, out.map((p) => p.facts));
  const keys = new Map<PlannedItem, Keys>(out.map((p) => [p, stored.get(`${p.facts.repo}#${p.facts.number}`)!]));
  const created: Cmp = (a, b) => str(keys.get(a)!.created, keys.get(b)!.created);
  const updatedDesc: Cmp = (a, b) => str(keys.get(b)!.updated, keys.get(a)!.updated);
  let cmp: Cmp;
  switch (kind) {
    // Items without a Jev verdict first, then changed ones; newest first within each. Haiku: the newest Jev verdicts first.
    case "classify":
      cmp = then((a, b) => (a.facts.classification.jev ? 1 : 0) - (b.facts.classification.jev ? 1 : 0),
        (a, b) => (a.action.params?.source === "haiku" ? str(b.facts.classification.jev?.at ?? "", a.facts.classification.jev?.at ?? "") : updatedDesc(a, b)), byRef);
      break;
    case "referee": case "decide": cmp = then(score, prFirst, created, byRef); break;
    // Items without a priority row first, then changed ones; pull requests first, newest first.
    case "prioritize": cmp = then((a, b) => (a.facts.priorityHash === null ? 0 : 1) - (b.facts.priorityHash === null ? 0 : 1), prFirst, updatedDesc, byRef); break;
    case "implement": cmp = then(score, byRef); break;
    default: cmp = byRef;
  }
  // Work of ours that is still moving goes ahead of everything else.
  const flight: Cmp = (a, b) => (inFlightNow(b.facts, b.plan.state) ? 1 : 0) - (inFlightNow(a.facts, a.plan.state) ? 1 : 0);
  return out.sort(then(flight, cmp));
}

/** The item as the model steps read it. */
export async function itemOf(db: Db, repo: string, number: number): Promise<Item> {
  const r = await db.get(db.k.selectFrom("items").select((eb) => ["repo", "number", "kind", eb.fn.coalesce("title", eb.val("")).as("title"), eb.fn.coalesce("body", eb.val("")).as("body"),
    eb(eb.ref("additions"), "+", eb.ref("deletions")).as("lines_changed"), "changed_files as files_changed", "author_association"]).$narrowType<{ kind: Item["kind"] }>().where("repo", "=", repo).where("number", "=", number));
  if (!r) throw new Error(`${repo}#${number} is not in the database`);
  return r;
}
