export {};

/**
 * The test run, with a floor under how much of it must actually execute.
 *
 * A test file that throws while it is being imported never runs its tests and never reports them as
 * failures; the file is simply absent from the totals.
 *
 * This asserts the count as well as the result. A drop is treated as a failure, because a smaller
 * suite with no failure report gives false coverage.
 *
 * The floor is deliberately a floor and not an exact number. Tests are added constantly and a check
 * that has to be edited for every new test is a check people learn to edit without thinking.
 */

/**
 * What this run must contain, and the floor that keeps the count honest.
 *
 * The fork runs `server` and the root `tests/` on every push: that is the code it changes. The rest
 * of the repository — the app, the desktop, the browser extension, the six agent harnesses — belongs
 * to upstream and is not built or shipped differently here, and one flaky file in the extension
 * (which reproduces on CI and not locally) should not decide whether a release publishes. `bun test`
 * with no arguments still runs everything, which is what an upstream sync wants.
 *
 * The floor is per scope, because its whole job is to catch a file that fails to import and takes
 * its tests with it: a check that passes at 6,000 tests tells you nothing about whether the 6,300
 * you expected were there.
 */
const MINIMUM_TESTS = 5_500;
/**
 * Paths, not words. Bun treats these as SUBSTRING filters on the file path, so `tests` would also
 * match `app/tests/`, `extension/tests/` and `desktop/tests/` — which is the whole repository again.
 * `server/` matches the 426 files under `server/` and nothing else; the root `tests/` files that
 * guard this fork's own wiring are named individually.
 */
const SCOPE = [
  "server/",
  "tests/compose.test.ts",
  "tests/clean-checkout.test.ts",
  "tests/workspace.test.ts",
];

async function writeStderr(text: string) {
  if (!text) return;
  if (process.stderr.write(text)) return;
  await new Promise<void>((resolve) => process.stderr.once("drain", resolve));
}

// `bun run test` rather than `bun test`, so the pretest hook fires and the generated application
// config exists before route imports need it. The paths go through the script, so the hook still
// runs. `bun test` with no arguments is the full suite.
const proc = Bun.spawn(["bun", "run", "test", "--", ...SCOPE], {
  stdout: "inherit",
  stderr: "pipe",
});

// Bun writes its summary to stderr, so it is captured and echoed rather than inherited.
const stderr = await new Response(proc.stderr).text();
await writeStderr(stderr);

const exitStatus = await proc.exited;
if (exitStatus !== 0) {
  process.exitCode = exitStatus;
} else {
  const ran = stderr.match(/Ran (\d+) tests? across/);
  const count = ran ? Number.parseInt(ran[1] as string, 10) : 0;

  if (!ran) {
    await writeStderr(
      "\nCould not read how many tests ran from bun's output. Refusing to report a pass on a run that cannot be counted.\n",
    );
    process.exitCode = 1;
  } else if (count < MINIMUM_TESTS) {
    await writeStderr(
      `\n${count} tests ran, and at least ${MINIMUM_TESTS} were expected.\n\n` +
        "Every test passed, so this is not a failing test, it is a suite that got smaller. The usual\n" +
        "cause is a file that threw while being imported, which takes its tests with it and reports\n" +
        "nothing. Run `bun test` and look for an unhandled error between the file groups.\n\n" +
        `If tests were deliberately removed, lower MINIMUM_TESTS in scripts/test-ci.ts and say why.\n`,
    );
    process.exitCode = 1;
  } else {
    await writeStderr(`\n${count} tests ran (floor ${MINIMUM_TESTS}).\n`);
  }
}
