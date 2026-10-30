/**
 * The Inspector's thread state: the latest `STATE_SNAPSHOT` with the RFC 6902
 * `STATE_DELTA` patches after it applied. The hosted platform folds the same;
 * OpenBot reads it through `getThreadState`.
 */

const APPLIED_OPS = new Set(["add", "replace", "remove"]);

/**
 * One RFC 6902 patch, applied on a clone of the snapshot.
 *
 * `add`, `replace` and `remove` only: a `test`, `copy` or `move` is skipped rather
 * than guessed at, because a wrong fold here is a plausible-looking state panel.
 *
 * The last pointer segment names either a property or an array position, which is the
 * same lookup in JavaScript — but not the same write. `-` is the RFC's "the end of the
 * array", which is how a Bot appending to a list writes it; treating it as a property
 * name would create a `"-"` key, and `add` at an index inserts rather than overwrites.
 */
function applyJsonPatch(
  state: Record<string, unknown>,
  patch: { op: string; path: string; value?: unknown }[],
) {
  let next = structuredClone(state);
  for (const op of patch) {
    if (!APPLIED_OPS.has(op.op)) continue;
    const parts = op.path.split("/").filter(Boolean);
    if (parts.length === 0) {
      next = (op.value ?? {}) as Record<string, unknown>;
      continue;
    }
    let node: Record<string, unknown> = next;
    for (const part of parts.slice(0, -1)) {
      const child = (node[part] as Record<string, unknown> | undefined) ?? {};
      node[part] = child;
      node = child;
    }
    const last = parts[parts.length - 1];
    const parent: unknown = node;
    if (Array.isArray(parent)) {
      if (last === "-") {
        if (op.op !== "remove") parent.push(op.value);
        continue;
      }
      const at = Number(last);
      if (op.op === "remove") parent.splice(at, 1);
      else if (op.op === "add") parent.splice(at, 0, op.value);
      else parent[at] = op.value;
      continue;
    }
    if (op.op === "remove") delete node[last];
    else node[last] = op.value;
  }
  return next;
}

export type ThreadStateResult =
  | { kind: "no-snapshot" }
  | { kind: "snapshot"; state: unknown; skippedDeltas: number };

export function foldState(
  events: readonly Record<string, unknown>[],
): ThreadStateResult {
  let state: Record<string, unknown> | undefined;
  /*
   * Not reset by a snapshot. A delta is skipped for one reason only — no snapshot has
   * been seen yet — so counting them across the log is what tells the Inspector that
   * the state it is about to show is missing patches. Reset on snapshot, the count
   * could only ever be zero by the time it was returned.
   */
  let skippedDeltas = 0;
  for (const event of events) {
    if (event.type === "STATE_SNAPSHOT") {
      state = structuredClone(
        (event.snapshot ?? {}) as Record<string, unknown>,
      );
    } else if (event.type === "STATE_DELTA") {
      if (!state) {
        skippedDeltas += 1;
        continue;
      }
      state = applyJsonPatch(state, (event.delta ?? []) as never);
    }
  }
  if (!state) return { kind: "no-snapshot" };
  return { kind: "snapshot", state, skippedDeltas };
}
