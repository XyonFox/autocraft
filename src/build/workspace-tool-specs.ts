// The workspace tools' arguments: one table that gives both the MCP input schemas and the checks the job runs on every
// call, plus the path rule and the output cuts the tools share.
import { posix } from "node:path";

export const WORK_ROOT = "/work";
export const SRC_ROOT = "/work/src";

/** Output a tool returns before it is cut: the first OUTPUT_HEAD bytes and the last OUTPUT_TAIL. */
export const OUTPUT_CAP = 30 * 1024;
export const OUTPUT_HEAD = 4 * 1024;
export const OUTPUT_TAIL = 26 * 1024;
/** Longest image edge a tool returns; the pod scales down to it. */
export const IMAGE_EDGE = 1568;
/** Bytes of one image a tool returns at most, so its base64 stays within the model API's 5 MB. The pod reads files up to 10 MiB. */
export const IMAGE_MAX_BYTES = 3_750_000;
/** Images one session gets back in all, as the design's screenshot limit. */
export const SESSION_IMAGES = 300;
export const READ_LINE_CHARS = 2000;
export const FILE_MAX_BYTES = 2 * 1024 * 1024;
export const GLOB_LIMIT = 500;

/** Apps one session runs at once (the helper counts them in the pod). */
export const APP_LIMIT = 4;
const APP_ARGS = 32;
const APP_ENV = 16;
const ARG_BYTES = 4096;
const RECT_MAX = 16384;
export type Rect = { x: number; y: number; w: number; h: number };
/** Names app_start's env takes; DISPLAY, HOME, the scale factor and the renderer's VK_*, LP_* and XDG_* stay the workspace's. */
const ENV_NAME = /^[A-Z][A-Z0-9_]{0,63}$/;
const ENV_KEPT = /^(DISPLAY|HOME|WINIT_X11_SCALE_FACTOR|VK_.*|LP_.*|XDG_.*)$/;

type Field =
  | { type: "string"; description: string; maxBytes: number; enum?: readonly string[] }
  | { type: "integer"; description: string; min: number; max: number }
  | { type: "boolean"; description: string }
  | { type: "strings"; description: string; maxItems: number; maxBytes: number }
  | { type: "env"; description: string; maxItems: number; maxBytes: number }
  | { type: "rect"; description: string };
type Spec = { description: string; readOnly: boolean; required: readonly string[]; fields: Record<string, Field> };

const PATH_NOTE = "an absolute path below /work, or a path relative to /work/src";
const path = (what: string): Field => ({ type: "string", description: `${what}: ${PATH_NOTE}.`, maxBytes: 4096 });
const id: Field = { type: "integer", description: "The id a shell call returned.", min: 1, max: Number.MAX_SAFE_INTEGER };
const app = (what: string): Field => ({ type: "integer", description: `${what}: the id app_start returned.`, min: 1, max: Number.MAX_SAFE_INTEGER });

const SPECS = {
  shell: {
    description: "Runs a bash command in the workspace (bash -c, in /work/src by default). Returns the exit code, the time it took and stdout and stderr merged; past 30 KiB, the first 4 KiB and last 26 KiB with the log path. With background, returns {\"id\"} at once: read it with shell_status, end it with shell_kill. The workspace has no network.",
    readOnly: false,
    required: ["command"],
    fields: {
      command: { type: "string", description: "The bash command.", maxBytes: 64 * 1024 },
      cwd: path("Directory to run in"),
      timeout_sec: { type: "integer", description: "Time limit in seconds (default 120). A background command's limit is the session's remaining shell time.", min: 1, max: 600 },
      stdin: { type: "string", description: "Text given to the command on stdin.", maxBytes: 1024 * 1024 },
      background: { type: "boolean", description: "Start the command and return its id without waiting (default false)." },
    },
  },
  shell_status: {
    description: "The state of a shell call (running, exited with its code, timed out, killed) and the output it wrote since the last shell_status, cut in the middle past 30 KiB.",
    readOnly: true,
    required: ["id"],
    fields: { id, wait_sec: { type: "integer", description: "Wait up to this many seconds for the command to end (default 0).", min: 0, max: 300 } },
  },
  shell_kill: {
    description: "Ends a running shell call: SIGTERM to its process group, SIGKILL 5 seconds later.",
    readOnly: false,
    required: ["id"],
    fields: { id },
  },
  read: {
    description: "Reads a file as numbered lines, each cut at 2000 characters. A PNG, JPEG, GIF or WebP file (up to 10 MiB) comes back as an image scaled to 1568 px on the long edge. Other binary files are refused.",
    readOnly: true,
    required: ["path"],
    fields: {
      path: path("The file"),
      offset: { type: "integer", description: "First line to read (default 1).", min: 1, max: Number.MAX_SAFE_INTEGER },
      limit: { type: "integer", description: "Number of lines (default 2000).", min: 1, max: 2000 },
    },
  },
  write: {
    description: "Writes a file, creating its parent directories and replacing what was there. Returns the size and sha256.",
    readOnly: false,
    required: ["path", "content"],
    fields: { path: path("The file"), content: { type: "string", description: "The whole new content.", maxBytes: FILE_MAX_BYTES } },
  },
  edit: {
    description: "Replaces an exact string in a file. Refused when old_string is not found, or found more than once without replace_all. Fails if the file changes while the edit runs.",
    readOnly: false,
    required: ["path", "old_string", "new_string"],
    fields: {
      path: path("The file"),
      old_string: { type: "string", description: "The exact text to replace.", maxBytes: FILE_MAX_BYTES },
      new_string: { type: "string", description: "The text to put in its place.", maxBytes: FILE_MAX_BYTES },
      replace_all: { type: "boolean", description: "Replace every occurrence (default false)." },
    },
  },
  glob: {
    description: "Lists files whose path matches a glob (ripgrep syntax: matched relative to path, a pattern without / matches at any depth, ** crosses directories), newest first, up to 500. Skips .git and git-ignored files.",
    readOnly: true,
    required: ["pattern"],
    fields: { pattern: { type: "string", description: "The glob, e.g. **/*.rs or src/**/test_*.py.", maxBytes: 4096 }, path: path("Directory to search (default /work/src)") },
  },
  grep: {
    description: "Searches file contents with ripgrep. Skips .git and git-ignored files. Output is cut at 30 KiB.",
    readOnly: true,
    required: ["pattern"],
    fields: {
      pattern: { type: "string", description: "A ripgrep (Rust regex) pattern.", maxBytes: 4096 },
      path: path("File or directory to search (default /work/src)"),
      glob: { type: "string", description: "Only files matching this glob, e.g. *.rs or src/**/*.ts.", maxBytes: 4096 },
      type: { type: "string", description: "Only files of this ripgrep type, e.g. rust, py, ts.", maxBytes: 64 },
      output_mode: { type: "string", description: "files_with_matches (default): matching paths, newest first; content: matching lines with numbers; count: matches per file.", maxBytes: 32, enum: ["files_with_matches", "content", "count"] },
      case_insensitive: { type: "boolean", description: "Ignore case (default false)." },
      context: { type: "integer", description: "Lines of context around each match, for content (default 0).", min: 0, max: 20 },
      multiline: { type: "boolean", description: "Let the pattern span lines, with . matching newlines (default false)." },
      head_limit: { type: "integer", description: "At most this many output lines (default 250).", min: 1, max: 2000 },
    },
  },
  app_start: {
    description: `Starts a GUI app on the workspace's virtual display (2880x1800, no window manager, software Vulkan) and waits up to 30 s for its window. Returns the app id, pid, window id and geometry and the log path; if the app exits first, its exit code and the last 4 KiB of its log. At most ${APP_LIMIT} apps run at once. Drive the app through its own CLI with shell; look at it with screenshot.`,
    readOnly: false,
    required: ["binary"],
    fields: {
      binary: path("The executable, e.g. a build in /work/target/debug"),
      args: { type: "strings", description: "Its arguments.", maxItems: APP_ARGS, maxBytes: ARG_BYTES },
      scale: { type: "integer", description: "Scale factor (default 1). 2 renders at twice the pixels; region then gives full-resolution crops.", min: 1, max: 2 },
      env: { type: "env", description: "Extra environment variables (names A-Z, 0-9, _; not DISPLAY, HOME, WINIT_X11_SCALE_FACTOR, VK_*, LP_* or XDG_*). WGPU_BACKEND=gl selects the GL fallback.", maxItems: APP_ENV, maxBytes: ARG_BYTES },
    },
  },
  app_stop: {
    description: "Stops an app: SIGTERM, SIGKILL 5 seconds later. Returns its exit code and the last 2 KiB of its log.",
    readOnly: false,
    required: ["app"],
    fields: { app: app("The app"), force: { type: "boolean", description: "SIGKILL at once (default false)." } },
  },
  screenshot: {
    description: `A PNG of an app's window as the display shows it (dialogs and menus over it included), or of the whole screen. The full-size image is kept at /work/.ws/shots/<n>.png, which read returns again. Counts toward the session's ${SESSION_IMAGES} images.`,
    readOnly: true,
    required: [],
    fields: {
      app: app("The app (default: the only running app, else the whole screen)"),
      region: { type: "rect", description: "Only this part, in window pixels: {x, y, w, h}." },
      max_edge: { type: "integer", description: `Longest edge of the image returned, scaled down to it (default ${IMAGE_EDGE}).`, min: 256, max: IMAGE_EDGE },
    },
  },
} satisfies Record<string, Spec>;

export type WorkspaceToolName = keyof typeof SPECS;
export const TOOL_NAMES = Object.keys(SPECS) as WorkspaceToolName[];
export const isToolName = (n: string): n is WorkspaceToolName => Object.hasOwn(SPECS, n);

/** The MCP tool definitions, as tools/list returns them. */
export const TOOL_DEFINITIONS: readonly unknown[] = TOOL_NAMES.map((name) => {
  const s: Spec = SPECS[name];
  const properties = Object.fromEntries(Object.entries(s.fields).map(([k, f]) => [k, fieldSchema(f)]));
  return {
    name, description: s.description,
    inputSchema: { type: "object", additionalProperties: false, required: [...s.required], properties },
    annotations: { readOnlyHint: s.readOnly },
  };
});

function fieldSchema(f: Field): unknown {
  const description = f.description;
  switch (f.type) {
    case "string": return { type: "string", description, ...(f.enum ? { enum: f.enum } : { maxLength: f.maxBytes }) };
    case "integer": return { type: "integer", description, minimum: f.min, ...(f.max < Number.MAX_SAFE_INTEGER ? { maximum: f.max } : {}) };
    case "boolean": return { type: "boolean", description };
    case "strings": return { type: "array", description, maxItems: f.maxItems, items: { type: "string", maxLength: f.maxBytes } };
    case "env": return { type: "object", description, maxProperties: f.maxItems, propertyNames: { pattern: ENV_NAME.source }, additionalProperties: { type: "string", maxLength: f.maxBytes } };
    case "rect": {
      const n = (min: number) => ({ type: "integer", minimum: min, maximum: RECT_MAX });
      return { type: "object", description, additionalProperties: false, required: ["x", "y", "w", "h"], properties: { x: n(0), y: n(0), w: n(1), h: n(1) } };
    }
  }
}

/** A call's arguments that break the table: the message says which and why. */
export class ArgError extends Error {}

export type Args = Record<string, string | number | boolean | string[] | Record<string, string> | Rect | undefined>;

/** `args` checked against tool `name`'s table row; throws ArgError. */
export function checkArgs(name: WorkspaceToolName, args: unknown): Args {
  const s: Spec = SPECS[name];
  const given: unknown = args ?? {};
  if (typeof given !== "object" || given === null || Array.isArray(given)) throw new ArgError("the arguments must be an object");
  const out: Args = {};
  for (const [k, v] of Object.entries(given)) {
    const f = s.fields[k];
    if (!f) throw new ArgError(`${name} takes no argument ${JSON.stringify(k.slice(0, 64))}`);
    if (v === undefined || v === null) continue;
    if (f.type === "string") {
      if (typeof v !== "string") throw new ArgError(`${k} must be a string`);
      if (Buffer.byteLength(v) > f.maxBytes) throw new ArgError(`${k} is longer than ${f.maxBytes} bytes`);
      if (f.enum && !f.enum.includes(v)) throw new ArgError(`${k} must be one of ${f.enum.join(", ")}`);
    } else if (f.type === "integer") {
      if (typeof v !== "number" || !Number.isInteger(v) || v < f.min || v > f.max) throw new ArgError(`${k} must be an integer from ${f.min}${f.max < Number.MAX_SAFE_INTEGER ? ` to ${f.max}` : " up"}`);
    } else if (f.type === "boolean") {
      if (typeof v !== "boolean") throw new ArgError(`${k} must be true or false`);
    } else {
      out[k] = checkCompound(k, f, v);
      continue;
    }
    out[k] = v;
  }
  for (const k of s.required) if (out[k] === undefined) throw new ArgError(`${name} needs ${k}`);
  return out;
}

/** An array, env or rect argument checked against its field. Strings go into argv, so they carry no NUL. */
function checkCompound(k: string, f: Extract<Field, { type: "strings" | "env" | "rect" }>, v: unknown): string[] | Record<string, string> | Rect {
  const str = (what: string, s: unknown, max: number): string => {
    if (typeof s !== "string") throw new ArgError(`${what} must be a string`);
    if (Buffer.byteLength(s) > max) throw new ArgError(`${what} is longer than ${max} bytes`);
    if (s.includes("\0")) throw new ArgError(`${what} contains a NUL byte`);
    return s;
  };
  if (f.type === "strings") {
    if (!Array.isArray(v)) throw new ArgError(`${k} must be an array of strings`);
    if (v.length > f.maxItems) throw new ArgError(`${k} has more than ${f.maxItems} items`);
    return v.map((s, i) => str(`${k}[${i}]`, s, f.maxBytes));
  }
  if (typeof v !== "object" || v === null || Array.isArray(v)) throw new ArgError(`${k} must be an object`);
  if (f.type === "env") {
    const e = Object.entries(v);
    if (e.length > f.maxItems) throw new ArgError(`${k} has more than ${f.maxItems} variables`);
    for (const [name] of e) {
      if (!ENV_NAME.test(name)) throw new ArgError(`${k} name ${JSON.stringify(name.slice(0, 64))} must match ${ENV_NAME.source}`);
      if (ENV_KEPT.test(name)) throw new ArgError(`${k} cannot set ${name}; the workspace sets it`);
    }
    return Object.fromEntries(e.map(([name, s]) => [name, str(`${k}.${name}`, s, f.maxBytes)]));
  }
  const r = v as Record<string, unknown>;
  for (const key of Object.keys(r)) if (!["x", "y", "w", "h"].includes(key)) throw new ArgError(`${k} takes only x, y, w and h`);
  const n = (key: "x" | "y" | "w" | "h", min: number): number => {
    const x = r[key];
    if (typeof x !== "number" || !Number.isInteger(x) || x < min || x > RECT_MAX) throw new ArgError(`${k}.${key} must be an integer from ${min} to ${RECT_MAX}`);
    return x;
  };
  return { x: n("x", 0), y: n("y", 0), w: n("w", 1), h: n("h", 1) };
}

/** A session path as the absolute, normalised path in the pod; throws ArgError for one outside /work. */
export function podPath(p: string, field = "path"): string {
  if (p.includes("\0")) throw new ArgError(`${field} contains a NUL byte`);
  if (!p) throw new ArgError(`${field} is empty`);
  const abs = posix.normalize(p.startsWith("/") ? p : posix.join(SRC_ROOT, p)).replace(/(.)\/+$/, "$1");
  if (abs !== WORK_ROOT && !abs.startsWith(`${WORK_ROOT}/`)) throw new ArgError(`${field} must be below ${WORK_ROOT}: ${PATH_NOTE}`);
  return abs;
}

/** `b` cut to its first OUTPUT_HEAD and last OUTPUT_TAIL bytes when longer than `cap`, with the cut noted. */
export function cutMiddle(b: Buffer, cap = OUTPUT_CAP): Buffer {
  if (b.length <= cap) return b;
  return Buffer.concat([b.subarray(0, OUTPUT_HEAD), Buffer.from(`\n[${b.length - OUTPUT_HEAD - OUTPUT_TAIL} bytes cut]\n`), b.subarray(b.length - OUTPUT_TAIL)]);
}

export type ImageMime = "image/png" | "image/jpeg" | "image/gif" | "image/webp";

/** The image type from its signature; null for anything else. Pixels are never decoded. */
export function imageMime(b: Buffer): ImageMime | null {
  if (b.length >= 24 && b.subarray(0, 8).equals(Buffer.from([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a])) && b.subarray(12, 16).toString("latin1") === "IHDR") return "image/png";
  if (b.length >= 3 && b[0] === 0xff && b[1] === 0xd8 && b[2] === 0xff) return "image/jpeg";
  if (b.length >= 6 && /^GIF8[79]a$/.test(b.subarray(0, 6).toString("latin1"))) return "image/gif";
  if (b.length >= 12 && b.subarray(0, 4).toString("latin1") === "RIFF" && b.subarray(8, 12).toString("latin1") === "WEBP") return "image/webp";
  return null;
}

/** Width and height from the image's header (PNG IHDR, GIF screen, WebP VP8/VP8L/VP8X, JPEG SOF); null when they cannot be read. */
export function imageSize(b: Buffer, mime: ImageMime): { w: number; h: number } | null {
  if (mime === "image/png") return { w: b.readUInt32BE(16), h: b.readUInt32BE(20) };
  if (mime === "image/gif") return b.length >= 10 ? { w: b.readUInt16LE(6), h: b.readUInt16LE(8) } : null;
  if (mime === "image/webp") {
    const chunk = b.length >= 30 ? b.toString("latin1", 12, 16) : "";
    if (chunk === "VP8 " && b[23] === 0x9d && b[24] === 0x01 && b[25] === 0x2a) return { w: b.readUInt16LE(26) & 0x3fff, h: b.readUInt16LE(28) & 0x3fff };
    if (chunk === "VP8L" && b[20] === 0x2f) {
      const bits = b.readUInt32LE(21);
      return { w: (bits & 0x3fff) + 1, h: ((bits >>> 14) & 0x3fff) + 1 };
    }
    if (chunk === "VP8X") return { w: 1 + b.readUIntLE(24, 3), h: 1 + b.readUIntLE(27, 3) };
    return null;
  }
  let i = 2;
  while (i + 9 <= b.length) {
    if (b[i] !== 0xff) return null;
    const m = b[i + 1]!;
    if (m === 0xff) { i++; continue; }
    if (m === 0x01 || (m >= 0xd0 && m <= 0xd8)) { i += 2; continue; }
    if (m >= 0xc0 && m <= 0xcf && m !== 0xc4 && m !== 0xc8 && m !== 0xcc) return { w: b.readUInt16BE(i + 7), h: b.readUInt16BE(i + 5) };
    const len = b.readUInt16BE(i + 2);
    if (len < 2) return null;
    i += 2 + len;
  }
  return null;
}

export const seconds = (ms: number): string => `${(ms / 1000).toFixed(ms < 10_000 ? 1 : 0)} s`;
