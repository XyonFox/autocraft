// What a push of ours did to the PR's required checks, and when a stage stops itself because of it. Pure.

/** The result of the required checks at one commit. Optional checks and skipped ones do not count. */
export type Required = "green" | "red" | "pending" | "none";
export type Outcome = "kept-green" | "fixed" | "broke" | "still-red" | "unknown";

/** The stages that push or merge, as the circuit breaker names them. */
export const TRIP_STAGES = ["fixup", "cifix", "implement", "merge"] as const;
export type TripStage = (typeof TRIP_STAGES)[number];

/** Pushes the breaker looks at, and how many of them breaking a required check trips the stage. */
export const BREAKER_WINDOW = 10;
export const BREAKER_BROKE = 2;

export function requiredResult(checks: { name: string; state: string }[], optional: string[]): Required {
  const req = checks.filter((c) => !optional.includes(c.name) && c.state !== "skipped");
  if (req.some((c) => c.state === "failure")) return "red";
  if (req.some((c) => c.state === "pending")) return "pending";
  return req.some((c) => c.state === "success") ? "green" : "none";
}

/** The outcome once the checks after the push are final, or null while they are not (including before any required check exists; the 3-day expiry then gives `unknown`). A side without a known result gives `unknown`. */
export function classifyPush(before: Required | "unknown", after: Required | "unknown"): Outcome | null {
  if (after === "pending" || after === "none") return null;
  if (after === "red") return before === "green" ? "broke" : before === "red" ? "still-red" : "unknown";
  if (after === "green") return before === "red" ? "fixed" : before === "green" ? "kept-green" : "unknown";
  return "unknown";
}

/** True when `broke` is at least BREAKER_BROKE of the newest BREAKER_WINDOW outcomes (newest first; null: not known yet). */
export function breakerTripped(newestFirst: (Outcome | null)[]): boolean {
  return newestFirst.slice(0, BREAKER_WINDOW).filter((o) => o === "broke").length >= BREAKER_BROKE;
}

/** The breaker stage of an audit push action. */
export const PUSH_STAGE: Record<string, TripStage> = { "fixup-push": "fixup", "conflict-push": "fixup", "cifix-push": "cifix", "implement-push": "implement", "fixissue-push": "implement" };

/** The breaker stage a runner step belongs to. */
export const STEP_STAGE: Record<string, TripStage> = { fixup: "fixup", cifix: "cifix", implement: "implement", fixissue: "implement", "fixissue-collect": "implement", merge: "merge" };

/** The stage an action's push or merge belongs to, or null for an action that writes no commit (asks, comments, reruns). */
export function tripStageOf(a: { kind: string; bucket?: string }): TripStage | null {
  switch (a.kind) {
    case "fixup": return a.bucket === "fix" ? "fixup" : null;
    case "conflict": return a.bucket === "merge" ? "fixup" : null;
    case "cifix": return a.bucket === "work" || a.bucket === "collect" ? "cifix" : null;
    case "lint": return a.bucket === "cifix" ? "cifix" : "fixup";
    case "implement": case "fixissue": case "fixissue-collect": return "implement";
    case "merge": return "merge";
    default: return null;
  }
}

/**
 * Each push stage has two tiers of breaker: one per repo (`<stage>:<repo>`), which a repo's own broken pushes or merges trip and
 * which stops the stage on that repo only, and one instance-wide (the bare `<stage>`), which broken pushes on 2 or more repos trip
 * and which stops the stage everywhere: a fault in the step itself rather than in one repo.
 */
export const tripKey = (stage: TripStage, repo: string): string => `${stage}:${repo}`;

/** The stage and repo of a per-repo push-breaker key (`implement:deckcraft`), or null for any other key (`implement`, `outside-surge`). */
export function parseTripKey(key: string): { stage: TripStage; repo: string } | null {
  const i = key.indexOf(":");
  if (i < 1 || i === key.length - 1) return null;
  const stage = key.slice(0, i);
  return (TRIP_STAGES as readonly string[]).includes(stage) ? { stage: stage as TripStage, repo: key.slice(i + 1) } : null;
}

/** The stage of a push-breaker key of either tier, with its repo (null: the instance-wide breaker), or null for any other key (`outside-surge`). */
export function tripOf(key: string): { stage: TripStage; repo: string | null } | null {
  if ((TRIP_STAGES as readonly string[]).includes(key)) return { stage: key as TripStage, repo: null };
  return parseTripKey(key);
}

/** The repos a trip's evidence entries name ("repo#7@6df9331b", "repo@sha"). */
export function evidenceRepos(evidence: string[]): string[] {
  const repos = new Set<string>();
  for (const e of evidence) { const m = /^([^#@\s]+)[#@]/.exec(e); if (m) repos.add(m[1]!); }
  return [...repos];
}

/** The evidence entries that name `repo`. */
export const evidenceOf = (evidence: string[], repo: string): string[] => evidence.filter((e) => evidenceRepos([e])[0] === repo);

/** The trips of one repo's push stages, by stage, from active trips by key (an instance-wide trip first): what an item of `repo` is paused by. */
export function tripsOfRepo(trips: Readonly<Record<string, string>>, repo: string): Partial<Record<TripStage, string>> {
  const out: Partial<Record<TripStage, string>> = {};
  for (const s of TRIP_STAGES) { const r = trips[s] ?? trips[tripKey(s, repo)]; if (r !== undefined) out[s] = r; }
  return out;
}

export type StageTripRow = { stage: string; active: number; reason: string; evidence: string; tripped_at: string; cleared_at: string | null; cleared_by: string | null };

/** The push stages when every push stage came to trip per repo: the stage rows splitStageTrips splits are theirs. */
export const SPLIT_STAGES = ["fixup", "cifix", "fixissue", "merge"] as const;

/**
 * The per-repo rows that a stage row (key `fixup`, `cifix`, `fixissue` or `merge`, from before every push stage tripped per repo)
 * splits into. An active row becomes an active row for each repo its evidence names, with that repo's entries; with none named,
 * for each of `reposOf(stage)`, so nothing that was paused starts. The last resume of a stage row (`cleared_at`) carries to every
 * repo of `reposOf(stage)` that gets no active row, so pushes before it still do not count. Each reason names its repo.
 */
export function splitStageTrips(rows: StageTripRow[], reposOf: (stage: string) => string[]): StageTripRow[] {
  const out = new Map<string, StageTripRow>();
  const tripKey = (stage: string, repo: string): string => `${stage}:${repo}`;
  for (const r of rows) {
    if (!(SPLIT_STAGES as readonly string[]).includes(r.stage)) continue;
    const stage = r.stage;
    let evidence: string[] = [];
    try { const v = JSON.parse(r.evidence) as unknown; if (Array.isArray(v)) evidence = v.filter((e): e is string => typeof e === "string"); } catch { /* no evidence */ }
    const named = r.active ? evidenceRepos(evidence) : [];
    const active = new Set(r.active ? (named.length ? named : reposOf(stage)) : []);
    const row = (repo: string, on: number, ev: string[]): StageTripRow => ({ ...r, stage: tripKey(stage, repo), active: on, reason: `${r.reason} in ${repo}`, evidence: JSON.stringify(ev) });
    for (const repo of active) out.set(tripKey(stage, repo), row(repo, 1, named.length ? evidenceOf(evidence, repo) : evidence));
    if (!r.cleared_at) continue;
    for (const repo of reposOf(stage)) if (!active.has(repo)) out.set(tripKey(stage, repo), row(repo, 0, evidenceOf(evidence, repo)));
  }
  return [...out.values()];
}
