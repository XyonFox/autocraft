// The security deep scan API. Operators and maintainers read; operators start and continue scans; operators and maintainers with write access to the repo stop scans and decide findings. Everyone else gets 404, so the
// endpoints do not show that findings exist.
import { mayRunRepo } from "../repo-access.ts";
import type { FindingView, ScanDetail, ScanView, SecurityOverview } from "../../../web/src/api/types.ts";
import { actOnFindingAs, continueScanAs, SecurityError, startScanAs, stopScanAs } from "../../security/actions.ts";
import { listScans, overview, reconcileFindings, findingRow, scanDetail, toFinding, scanRow } from "../../security/store.ts";
import { HttpError } from "../server/util.ts";
import type { Req } from "../types.ts";
import { canSeeInternals } from "./staff.ts";
import { needVisible, visibleRepos } from "./visible.ts";
import { mapSeq } from "../../async-seq.ts";
import type { Db } from "../../db/index.ts";

const NOT_FOUND = "not found";
function needStaff(req: Req): void {
  if (!canSeeInternals(req)) throw new HttpError(404, NOT_FOUND);
}

/** Starting or continuing a scan: an operator with any cap or none, or a maintainer with write access to the repo with a positive cap. */
async function needStartScan(req: Req, repo: string, cap: unknown): Promise<void> {
  needStaff(req);
  if (req.user.role === "operator") return;
  if (!await mayRunRepo(req.user, repo)) throw new HttpError(403, "only an operator, or a maintainer with write access to the repo, starts or continues scans");
  if (typeof cap !== "number" || !(cap > 0)) throw new HttpError(403, "a spend cap is required: only operators run scans without one");
}

/** Stopping a scan and deciding findings: an operator, or a maintainer with write access to the repo. */
async function needRunRepo(req: Req, repo: string): Promise<void> {
  needStaff(req);
  if (!await mayRunRepo(req.user, repo)) throw new HttpError(403, "only an operator, or a maintainer with write access to the repo, stops scans and decides findings");
}

/** Finding actions that open or publish a security advisory. */
export const OPERATOR_ACTIONS: readonly string[] = ["advisory", "publish"];

function body(req: Req): Record<string, unknown> {
  const b = req.json;
  if (b === undefined) return {};
  if (!b || typeof b !== "object" || Array.isArray(b)) throw new HttpError(400, "the body must be a JSON object");
  return b as Record<string, unknown>;
}

function number(v: string | undefined, what: string): number {
  const n = Number(v);
  if (!Number.isInteger(n) || n < 1) throw new HttpError(400, `not a ${what} number`);
  return n;
}

/** Marks whether the caller may stop or continue the scan and decide its findings. */
async function withAct<T extends ScanView>(req: Req, s: T): Promise<T> {
  const may = await mayRunRepo(req.user, s.repo);
  return { ...s, canAct: may, canContinue: may };
}

export async function security(db: Db, req: Req): Promise<SecurityOverview> {
  needStaff(req);
  const repo = req.params.repo!;
  await needVisible(db, req, repo);
  const o = await overview(db, repo);
  return { ...o, canStart: await mayRunRepo(req.user, repo), scans: await mapSeq(o.scans, async (s) => await withAct(req, s)) };
}

/** Runs a security action and gives its refusals the HTTP status they carry. */
async function asHttp<T>(run: () => Promise<Promise<T> | T>): Promise<T> {
  try {
    return await run();
  } catch (e) {
    if (e instanceof SecurityError) throw new HttpError(e.status, e.message);
    throw e;
  }
}

export async function startScan(db: Db, req: Req): Promise<ScanView> {
  needStaff(req);
  const repo = req.params.repo!;
  await needVisible(db, req, repo);
  const b = body(req);
  await needStartScan(req, repo, b.capUsd);
  return await withAct(req, await asHttp(async () => startScanAs(db, repo, req.user.login, b.capUsd)));
}

export async function stopScan(db: Db, req: Req): Promise<ScanView> {
  needStaff(req);
  const id = number(req.params.id, "scan");
  const scan = await scanRow(db, id);
  if (!scan) throw new HttpError(404, NOT_FOUND);
  await needVisible(db, req, scan.repo);
  await needRunRepo(req, scan.repo);
  return await withAct(req, await asHttp(async () => await stopScanAs(db, id, req.user.login)));
}

export async function continueScan(db: Db, req: Req): Promise<ScanView> {
  needStaff(req);
  const id = number(req.params.id, "scan");
  const scan = await scanRow(db, id);
  if (!scan) throw new HttpError(404, NOT_FOUND);
  await needVisible(db, req, scan.repo);
  const cap = body(req).cap;
  await needStartScan(req, scan.repo, cap);
  return await withAct(req, await asHttp(async () => continueScanAs(db, id, req.user.login, cap)));
}

export async function finding(db: Db, req: Req): Promise<FindingView> {
  needStaff(req);
  const repo = req.params.repo!;
  await needVisible(db, req, repo);
  await reconcileFindings(db, repo);
  const row = await findingRow(db, repo, number(req.params.n, "finding"));
  if (!row) throw new HttpError(404, NOT_FOUND);
  return await toFinding(db, row);
}

export async function actOnFinding(db: Db, req: Req): Promise<FindingView> {
  needStaff(req);
  const repo = req.params.repo!;
  await needVisible(db, req, repo);
  await needRunRepo(req, repo);
  const n = number(req.params.n, "finding");
  const b = body(req);
  if (OPERATOR_ACTIONS.includes(b.action as string) && req.user.role !== "operator") throw new HttpError(403, "only an operator sends a finding the advisory way or publishes it");
  return asHttp(async () => actOnFindingAs(db, repo, n, req.user.login, b));
}

/** Scans of the repos the reader can see, newest first; `?repo=` narrows to one. */
export async function scans(db: Db, req: Req): Promise<ScanView[]> {
  needStaff(req);
  return await mapSeq(await listScans(db, await visibleRepos(db, req), req.query.get("repo") || undefined), async (s) => await withAct(req, s));
}

export async function scanOf(db: Db, req: Req): Promise<ScanDetail> {
  needStaff(req);
  const scan = await scanRow(db, number(req.params.id, "scan"));
  if (!scan) throw new HttpError(404, NOT_FOUND);
  await needVisible(db, req, scan.repo);
  const d = await scanDetail(db, scan);
  return { ...d, scan: await withAct(req, d.scan) };
}
