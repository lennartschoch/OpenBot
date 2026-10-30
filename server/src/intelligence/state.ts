/**
 * The Inspector's thread state: the latest `STATE_SNAPSHOT` with the RFC 6902
 * `STATE_DELTA` patches after it applied. The hosted platform folds the same;
 * OpenBot reads it through `getThreadState`.
 */

function applyJsonPatch(
  state: Record<string, unknown>,
  patch: { op: string; path: string; value?: unknown }[],
) {
  const next = structuredClone(state);
  for (const op of patch) {
    const parts = op.path.split("/").filter(Boolean);
    if (parts.length === 0) {
      return (op.value ?? {}) as Record<string, unknown>;
    }
    let node: Record<string, unknown> = next;
    for (const part of parts.slice(0, -1)) {
      node = (node[part] ??= {}) as Record<string, unknown>;
    }
    const last = parts[parts.length - 1];
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
  let skippedDeltas = 0;
  for (const event of events) {
    if (event.type === "STATE_SNAPSHOT") {
      state = structuredClone(
        (event.snapshot ?? {}) as Record<string, unknown>,
      );
      skippedDeltas = 0;
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
