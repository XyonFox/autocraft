import type { User } from "../api/types";
import type { Permission } from "./types";

export function can(user: User, p: Permission): boolean {
  if (p === "viewer") return true;
  if (p === "staff") return user.role !== "viewer";
  if (p === "operator") return user.role === "operator";
  return user.writeRepos.includes(p.slice("write:".length));
}

export { canComment } from "./roles.ts";

/** GitHub write on every repo involved, for an operator or a maintainer; whether the role settles a decision is the decision's `canSettle`. Everyone else leaves feedback. */
export function canAct(user: User, repos: string[]): boolean {
  return user.role !== "viewer" && [...new Set(repos)].every((r) => user.writeRepos.includes(r));
}

/** Whether the user runs the repo (server rule: operator, or maintainer with write). */
export const canRun = (user: User, repo: string): boolean => user.runRepos.includes(repo);

export const repoPermission = (user: User, repo: string): "write" | "none" => (user.writeRepos.includes(repo) ? "write" : "none");

export function roleLabel(user: User) {
  return user.role === "operator" ? "Operator" : user.role === "maintainer" ? "Maintainer" : "Viewer";
}
