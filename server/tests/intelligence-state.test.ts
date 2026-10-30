import { describe, expect, test } from "bun:test";
import { foldState } from "../src/intelligence/state";

/**
 * The Inspector's state panel: the last `STATE_SNAPSHOT` with the `STATE_DELTA` patches
 * after it applied. Small enough to test directly, and worth it — a fold that is wrong
 * shows a state nobody can tell is wrong.
 */
const snapshot = (value: unknown) => ({
  type: "STATE_SNAPSHOT",
  snapshot: value,
});
const delta = (...patch: { op: string; path: string; value?: unknown }[]) => ({
  type: "STATE_DELTA",
  delta: patch,
});

describe("folding a thread's state", () => {
  test("no snapshot is its own answer, not an empty state", () => {
    expect(foldState([delta({ op: "add", path: "/a", value: 1 })])).toEqual({
      kind: "no-snapshot",
    });
  });

  test("deltas before the snapshot are counted, deltas after it are applied", () => {
    expect(
      foldState([
        delta({ op: "add", path: "/lost", value: 1 }),
        snapshot({ count: 1 }),
        delta({ op: "replace", path: "/count", value: 2 }),
      ]),
    ).toEqual({ kind: "snapshot", state: { count: 2 }, skippedDeltas: 1 });
  });

  test("remove deletes, and a later snapshot replaces everything before it", () => {
    expect(
      foldState([
        snapshot({ keep: 1, drop: 2 }),
        delta({ op: "remove", path: "/drop" }),
      ]),
    ).toEqual({ kind: "snapshot", state: { keep: 1 }, skippedDeltas: 0 });

    expect(
      foldState([
        snapshot({ old: true }),
        snapshot({ fresh: true }),
        delta({ op: "add", path: "/added", value: 1 }),
      ]),
    ).toEqual({
      kind: "snapshot",
      state: { fresh: true, added: 1 },
      skippedDeltas: 0,
    });
  });

  // `-` is the RFC's "end of the array". Read as a property name it created a `-` key
  // beside the list, which is the kind of wrong that looks deliberate.
  test("an array append lands at the end of the array", () => {
    expect(
      foldState([
        snapshot({ steps: ["one"] }),
        delta({ op: "add", path: "/steps/-", value: "two" }),
        delta({ op: "add", path: "/steps/-", value: "three" }),
      ]),
    ).toEqual({
      kind: "snapshot",
      state: { steps: ["one", "two", "three"] },
      skippedDeltas: 0,
    });
  });

  test("a numeric path inserts, replaces and removes inside an array", () => {
    expect(
      foldState([
        snapshot({ steps: ["one", "two"] }),
        delta({ op: "add", path: "/steps/1", value: "inserted" }),
        delta({ op: "replace", path: "/steps/0", value: "first" }),
        delta({ op: "remove", path: "/steps/2" }),
      ]),
    ).toEqual({
      kind: "snapshot",
      state: { steps: ["first", "inserted"] },
      skippedDeltas: 0,
    });
  });

  // Skipped rather than guessed: a `test` that failed, or a `copy` read as an `add`,
  // would both leave a state that reads as real.
  test("an op the fold does not implement changes nothing", () => {
    expect(
      foldState([
        snapshot({ count: 1 }),
        delta(
          { op: "test", path: "/count", value: 2 },
          { op: "copy", path: "/count", value: 3 },
          { op: "move", path: "/count", value: 4 },
        ),
      ]),
    ).toEqual({ kind: "snapshot", state: { count: 1 }, skippedDeltas: 0 });
  });

  test("a root path replaces the whole state", () => {
    expect(
      foldState([
        snapshot({ old: true }),
        delta({ op: "replace", path: "", value: { fresh: true } }),
      ]),
    ).toEqual({ kind: "snapshot", state: { fresh: true }, skippedDeltas: 0 });
  });
});
