// Who may run a repo: an operator, or a maintainer with GitHub write permission on it (read live, cached per session).
// Per-repo and per-item actions use this; instance-wide AutoCraft internals stay operator-only.
import type { User } from "./types.ts";

export async function mayRunRepo(user: Pick<User, "role" | "canWrite">, repo: string): Promise<boolean> {
  if (user.role === "operator") return true;
  return user.role === "maintainer" && await user.canWrite(repo).catch(() => false);
}
