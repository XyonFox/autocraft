// Process groups that a job's own process group does not reach: model calls run in a group of their own (detached) so
// their idle watchdog can stop everything they started. Each is registered here, in the process and, inside a job, in
// the job's groups file (AUTOCRAFT_JOB_GROUPS), so a runner that lost its leader lock kills them with the job (abandon).
import { appendFileSync, readFileSync, rmSync } from "node:fs";
import { join } from "node:path";
import { envVar } from "../env.ts";

const live = new Set<number>();

/** The groups file of job `id` under the runner's log directory. */
export const groupsFile = (logs: string, id: number): string => join(logs, `job-${id}.groups`);

export function registerGroup(pid: number | undefined): void {
  if (!pid) return;
  live.add(pid);
  const file = envVar("JOB_GROUPS");
  if (file) try { appendFileSync(file, `${pid}\n`); } catch { /* the job's own group is still killed */ }
}

export function unregisterGroup(pid: number | undefined): void {
  if (pid) live.delete(pid);
}

const kill = (pid: number): void => {
  try { process.kill(-pid, "SIGKILL"); } catch { /* gone */ }
};

/** SIGKILLs the groups this process registered and those listed in `files`. */
export function killGroups(files: string[]): void {
  for (const pid of live) kill(pid);
  for (const f of files) {
    let text = "";
    try { text = readFileSync(f, "utf8"); } catch { continue; }
    for (const line of text.split("\n")) if (/^[1-9][0-9]*$/.test(line)) kill(Number(line));
  }
}

export function removeGroupsFile(file: string): void {
  try { rmSync(file, { force: true }); } catch { /* best effort */ }
}
