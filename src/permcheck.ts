// Why a private file (secrets, store keys) is open to anyone but its owner, or null when it
// is private; the caller's message names the file. On POSIX the group/other mode bits answer.
// On Windows statSync reports 0o666 for every file, so the file's ACL answers instead, and
// the ACL is read by SID: display names are localized and two accounts can share a name, but
// a SID is one identity. The allowed identities are the well-known SIDs of SYSTEM (S-1-5-18)
// and Administrators (S-1-5-32-544), and the current account's own SID; every other SID,
// including one whose account no longer resolves, is too open. An ACL that cannot be read
// warns and passes, so a broken tool never blocks a start.
import { execFileSync } from "node:child_process";
import { statSync } from "node:fs";

const SID_SYSTEM = "S-1-5-18";
const SID_ADMINISTRATORS = "S-1-5-32-544";

/** The grantees that are not an allowed identity; SIDs are compared case-insensitively. */
export function tooOpenSids(sids: string[], allowed: string[]): string[] {
  const ok = new Set(allowed.map((s) => s.toLowerCase()));
  return sids.filter((s) => !ok.has(s.toLowerCase()));
}

/**
 * The SIDs the file's ACL grants access to, with the current account's SID first. PowerShell
 * reads the ACL as objects and translates every grantee to its SID, whatever the account's
 * display name or the installation's language; a grantee whose account no longer resolves
 * comes back as UNRESOLVED. The path travels in AUTOCRAFT_ACL_PATH and never in the script
 * text: PowerShell 5.1 also treats the curly apostrophes of other scripts as quote
 * delimiters, so a path such as C:\Users\O'Neil\secrets.json must be data, not source, and
 * no filename character can become script syntax.
 */
export const ACL_SCRIPT =
  "$ErrorActionPreference='Stop';" +
  "$me=[System.Security.Principal.WindowsIdentity]::GetCurrent().User.Value;" +
  "$acl=Get-Acl -LiteralPath $env:AUTOCRAFT_ACL_PATH;" +
  "$sids=@($acl.Access | ForEach-Object { try { $_.IdentityReference.Translate([System.Security.Principal.SecurityIdentifier]).Value } catch { 'UNRESOLVED' } } | Sort-Object -Unique);" +
  "$me; $sids";

function aclSids(path: string): { me: string; sids: string[] } {
  const out = execFileSync("powershell", ["-NoProfile", "-NonInteractive", "-Command", ACL_SCRIPT], {
    windowsHide: true, timeout: 20_000, encoding: "utf8",
    env: { ...process.env, AUTOCRAFT_ACL_PATH: path },
  });
  const lines = out.split(/\r?\n/).map((l) => l.trim()).filter(Boolean);
  const me = lines.shift() ?? "";
  return { me, sids: lines };
}

export function fileTooOpen(path: string): string | null {
  let mode: number;
  try {
    mode = statSync(path).mode;
  } catch {
    return "not found";
  }
  if (process.platform !== "win32") return mode & 0o077 ? "is readable or writable by group or others; chmod 600 it" : null;
  let ids: { me: string; sids: string[] };
  try {
    ids = aclSids(path);
  } catch (e) {
    console.warn(`the Windows ACL of ${path} could not be read (${(e as Error).message}); the access check is skipped`);
    return null;
  }
  if (!ids.me) {
    console.warn(`the current account's SID could not be read; the access check of ${path} is skipped`);
    return null;
  }
  const tooOpen = tooOpenSids(ids.sids, [SID_SYSTEM, SID_ADMINISTRATORS, ids.me]);
  return tooOpen.length ? `grants access to ${tooOpen.join(", ")}; only the current account, SYSTEM and Administrators may have access` : null;
}
