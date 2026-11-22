// The pipeline switch and the paused stages, as the health list and the Needs page show them: a few reads, not the Pipeline page's full stage state.
import { PIPELINE_DRAIN } from "../../core/rules.ts";
import { DEFAULT_PER_DAY, KEY_AT, KEY_BY, KEY_ENABLED, LIMIT_STEPS } from "../../runner/autopilot.ts";
import { loadSteps } from "../../runner/steps.ts";
import type { Db } from "../../db/index.ts";

export type PipelineSwitch = { enabled: boolean; draining: boolean; changedBy: string | null; changedAt: string | null };

export async function pipelineSwitch(db: Db): Promise<PipelineSwitch> {
  const v = new Map((await db.all(db.k.selectFrom("settings").select(["key", "value"]).where("key", "in", [KEY_ENABLED, KEY_BY, KEY_AT]))).map((r) => [r.key, r.value]));
  return { enabled: v.get(KEY_ENABLED) === "1", draining: v.get(KEY_ENABLED) === PIPELINE_DRAIN, changedBy: v.get(KEY_BY) ?? null, changedAt: v.get(KEY_AT) ?? null };
}

/** Stages whose daily limit is 0, in stage-limit order. A breaker trips one repo of a stage, so it is listed on its own beside the limit. */
export async function zeroLimits(db: Db): Promise<{ step: string; title: string }[]> {
  const steps = await loadSteps();
  const perDay = new Map((await db.all(db.k.selectFrom("stage_limits").select(["step", "per_day"]))).map((r) => [r.step, r.per_day]));
  return LIMIT_STEPS
    .filter((step) => (perDay.has(step) ? perDay.get(step) : (DEFAULT_PER_DAY[step] ?? null)) === 0)
    .map((step) => ({ step, title: steps.get(step)?.title ?? step }));
}
