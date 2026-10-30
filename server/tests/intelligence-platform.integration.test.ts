import { beforeAll, describe, expect, test } from "bun:test";
import { createDatabase } from "../src/db/client";
import { intelligenceThreadEvents } from "../src/db/schema/intelligence";
import { createLocalIntelligence } from "../src/intelligence/index";
import { TEST_POOL, testDatabaseUrl } from "./support/database";

/**
 * The local Intelligence platform as the SDK's platform client speaks it:
 * threads, locks, join codes, memories, entitlements, the inspector, learning
 * containers and channel files, over the REST surface plus one Phoenix relay
 * round-trip. The wire shapes are what `CopilotKitIntelligence` parses, so
 * these are the contract tests for the whole fork's storage story.
 */

const API_KEY = "integration-key";
const SERVICE_TOKEN = "service-token";

function intelligence() {
  return createLocalIntelligence({
    database: createDatabase(testDatabaseUrl(), TEST_POOL),
    apiKey: API_KEY,
    serviceToken: SERVICE_TOKEN,
  });
}

async function call(
  app: ReturnType<typeof intelligence>,
  path: string,
  init?: RequestInit,
) {
  const response = await app.rest.request(path, {
    ...init,
    headers: {
      authorization: `Bearer ${API_KEY}`,
      "content-type": "application/json",
      ...(init?.headers ?? {}),
    },
  });
  const text = await response.text();
  return { status: response.status, body: text ? JSON.parse(text) : null };
}

const unique = () => `${Date.now()}-${Math.random().toString(36).slice(2)}`;

describe("threads, locks and join codes", () => {
  let app: ReturnType<typeof intelligence>;
  beforeAll(() => (app = intelligence()));

  test("creates, reads, renames and lists a thread", async () => {
    const threadId = unique();
    const created = await call(app, "/api/threads", {
      method: "POST",
      body: JSON.stringify({ threadId, userId: "u1", agentId: "a1" }),
    });
    expect(created.status).toBe(201);
    expect(created.body.thread.id).toBe(threadId);
    expect(created.body.thread.createdById).toBe("u1");

    const duplicate = await call(app, "/api/threads", {
      method: "POST",
      body: JSON.stringify({ threadId, userId: "u1", agentId: "a1" }),
    });
    expect(duplicate.status).toBe(409);

    const renamed = await call(app, `/api/threads/${threadId}`, {
      method: "PATCH",
      body: JSON.stringify({ userId: "u1", agentId: "a1", name: "Renamed" }),
    });
    expect(renamed.body.thread.name).toBe("Renamed");

    const listed = await call(app, `/api/threads?userId=u1&agentId=a1`);
    expect(listed.body.threads.some((t: any) => t.id === threadId)).toBe(true);
    // The thread-list meta channel's credential rides the page.
    expect(typeof listed.body.joinCode).toBe("string");

    const gone = await call(app, `/api/threads/${threadId}`, {
      method: "DELETE",
      body: JSON.stringify({ userId: "u1", agentId: "a1" }),
    });
    expect(gone.status).toBe(200);
    expect(gone.body.deleted.id).toBe(threadId);
  });

  test("the lock is exclusive until its run finishes", async () => {
    const { body } = await call(app, "/api/threads", {
      method: "POST",
      body: JSON.stringify({ threadId: unique(), userId: "u1", agentId: "a1" }),
    });
    const threadId = body.thread.id;
    const first = await call(app, `/api/threads/${threadId}/lock`, {
      method: "POST",
      body: JSON.stringify({ runId: "run-a", userId: "u1", agentId: "a1" }),
    });
    expect(first.status).toBe(200);
    expect(typeof first.body.joinToken).toBe("string");

    const second = await call(app, `/api/threads/${threadId}/lock`, {
      method: "POST",
      body: JSON.stringify({ runId: "run-b", userId: "u1", agentId: "a1" }),
    });
    expect(second.status).toBe(409);
    expect(second.body.runId).toBe("run-a");

    const renewed = await call(app, `/api/threads/${threadId}/lock`, {
      method: "PATCH",
      body: JSON.stringify({ runId: "run-a", ttlSeconds: 120 }),
    });
    expect(renewed.body.status).toBe("renewed");

    await call(app, `/api/threads/${threadId}/lock`, {
      method: "DELETE",
      body: JSON.stringify({ runId: "run-a" }),
    });
    const after = await call(app, `/api/threads/${threadId}/lock`, {
      method: "POST",
      body: JSON.stringify({ runId: "run-b", userId: "u1", agentId: "a1" }),
    });
    expect(after.status).toBe(200);
  });

  test("join codes mint a token the gateway accepts only for its thread", async () => {
    const { body } = await call(app, "/api/threads", {
      method: "POST",
      body: JSON.stringify({ threadId: unique(), userId: "u1", agentId: "a1" }),
    });
    const minted = await call(
      app,
      `/api/threads/${body.thread.id}/join-code?userId=u1`,
    );
    expect(minted.status).toBe(200);
    const store = app.gateway.store;
    expect(
      (
        await store.authorizeTopic(
          minted.body.joinToken,
          `thread:${body.thread.id}`,
        )
      ).ok,
    ).toBe(true);
    expect(
      (await store.authorizeTopic(minted.body.joinToken, "thread:other")).ok,
    ).toBe(false);
  });
});

describe("events, fold and the inspector", () => {
  let app: ReturnType<typeof intelligence>;
  beforeAll(() => (app = intelligence()));

  test("a run's events persist, fold to messages and replay", async () => {
    const threadId = unique();
    await call(app, "/api/threads", {
      method: "POST",
      body: JSON.stringify({ threadId, userId: "u1", agentId: "a1" }),
    });
    const runId = `run-${unique()}`;
    const events = [
      {
        type: "RUN_STARTED",
        threadId,
        runId,
        timestamp: 1,
        metadata: { cpki_event_id: "e1", cpki_event_seq: 1 },
      },
      {
        type: "TEXT_MESSAGE_START",
        threadId,
        runId,
        timestamp: 2,
        messageId: "m1",
        role: "user",
        metadata: { cpki_event_id: "e2", cpki_event_seq: 2 },
      },
      {
        type: "TEXT_MESSAGE_CONTENT",
        threadId,
        runId,
        timestamp: 3,
        messageId: "m1",
        delta: "hello",
        metadata: { cpki_event_id: "e3", cpki_event_seq: 3 },
      },
      {
        type: "TEXT_MESSAGE_END",
        threadId,
        runId,
        timestamp: 4,
        messageId: "m1",
        metadata: { cpki_event_id: "e4", cpki_event_seq: 4 },
      },
      {
        type: "RUN_FINISHED",
        threadId,
        runId,
        timestamp: 5,
        metadata: { cpki_event_id: "e5", cpki_event_seq: 5 },
      },
    ];
    const gateway = app.gateway;
    const socket = captureSocket();
    gateway.handleClose(socket); // no-op: proves close tolerates an unjoined socket
    for (const event of events) {
      // The runner's durability contract is batch, ack, next — mirror it so
      // the log's insertion order matches the run's seq order.
      gateway.handle(
        socket,
        JSON.stringify([
          null,
          "1",
          `ingestion:${runId}`,
          "events",
          { events: [event] },
        ]),
      );
      await new Promise((r) => setTimeout(r, 30));
    }
    await new Promise((r) => setTimeout(r, 100));

    const inspect = await call(app, `/api/_inspect/threads/${threadId}/events`);
    expect(inspect.body.events.length).toBe(events.length);
    expect(inspect.body.events[0].type).toBe("RUN_STARTED");

    const messages = await call(
      app,
      `/api/threads/${threadId}/messages?userId=u1`,
    );
    expect(messages.body.messages.at(-1).content).toBe("hello");

    // The state fold answers with a snapshot shape (no STATE events: the documented empty answer).
    const state = await call(app, `/api/_inspect/threads/${threadId}/state`);
    expect(state.status).toBe(200);
  });

  test("repeated pushes are idempotent on (run_id, seq)", async () => {
    const threadId = unique();
    await call(app, "/api/threads", {
      method: "POST",
      body: JSON.stringify({ threadId, userId: "u1", agentId: "a1" }),
    });
    const gateway = app.gateway;
    const socket = captureSocket();
    const runId = `run-${unique()}`;
    const event = {
      type: "RUN_STARTED",
      threadId,
      runId,
      timestamp: 1,
      metadata: { cpki_event_id: "d1", cpki_event_seq: 1 },
    };
    gateway.handle(
      socket,
      JSON.stringify([
        null,
        "1",
        `ingestion:${runId}`,
        "events",
        { events: [event] },
      ]),
    );
    gateway.handle(
      socket,
      JSON.stringify([
        null,
        "1",
        `ingestion:${runId}`,
        "events",
        { events: [event] },
      ]),
    );
    await new Promise((r) => setTimeout(r, 250));
    const inspect = await call(app, `/api/_inspect/threads/${threadId}/events`);
    expect(inspect.body.events.length).toBe(1);
  });
});

describe("memories", () => {
  let app: ReturnType<typeof intelligence>;
  beforeAll(() => (app = intelligence()));

  const userId = () => `u-${unique()}`;

  test("creates, absorbs duplicates, supersedes, retires and recalls", async () => {
    const user = userId();
    const headers = { "x-cpki-user-id": user };
    const created = await call(app, "/api/memories", {
      method: "POST",
      headers,
      body: JSON.stringify({
        content: "prefers terse answers",
        kind: "topical",
        scope: "user",
      }),
    });
    expect(created.status).toBe(200);
    expect(created.body.absorbed).toBe(false);

    const duplicate = await call(app, "/api/memories", {
      method: "POST",
      headers,
      body: JSON.stringify({
        content: "prefers terse answers",
        kind: "topical",
        scope: "user",
      }),
    });
    expect(duplicate.body.absorbed).toBe(true);
    expect(duplicate.body.id).toBe(created.body.id);

    const superseded = await call(app, `/api/memories/${created.body.id}`, {
      method: "PATCH",
      headers,
      body: JSON.stringify({
        content: "prefers very terse answers",
        kind: "topical",
        scope: "user",
      }),
    });
    expect(superseded.body.retiredId).toBe(created.body.id);

    const listed = await call(app, "/api/memories", { headers });
    expect(listed.body.memories.length).toBe(1);

    const recalled = await call(app, "/api/memories/recall", {
      method: "POST",
      headers,
      body: JSON.stringify({ query: "terse answers", limit: 5 }),
    });
    expect(recalled.body.memories.length).toBe(1);

    const retired = await call(app, `/api/memories/${superseded.body.id}`, {
      method: "DELETE",
      headers,
    });
    expect(retired.status).toBe(200);
  });
});

describe("entitlements, learning and files", () => {
  let app: ReturnType<typeof intelligence>;
  beforeAll(() => (app = intelligence()));

  test("entitlements are ready for a self-hosted deployment", async () => {
    const result = await call(app, "/api/entitlements/runtime");
    expect(result.status).toBe(200);
    expect(result.body.entitlement.active).toBe(true);
  });

  test("a service token creates a container; skills publish, snapshot and revalidate", async () => {
    const containerId = `c-${unique()}`;
    const created = await app.rest.request(
      `/api/learning/projects/1/containers`,
      {
        method: "POST",
        headers: {
          "x-copilotkit-ei-service-token": "service-token",
          "content-type": "application/json",
        },
        body: JSON.stringify({ id: containerId, name: "Integration" }),
      },
    );
    expect(created.status).toBe(201);
    const present = await app.rest.request(
      `/api/learning/projects/1/containers`,
      {
        method: "POST",
        headers: {
          "x-copilotkit-ei-service-token": "service-token",
          "content-type": "application/json",
        },
        body: JSON.stringify({ id: containerId, name: "Integration" }),
      },
    );
    expect(present.status).toBe(409);

    const page = await call(app, "/api/v1/learning/containers");
    expect(page.body.containers.some((c: any) => c.id === containerId)).toBe(
      true,
    );

    const published = await call(
      app,
      `/api/v1/learning/containers/${containerId}/skills`,
      {
        method: "PUT",
        body: "zip-bytes",
      },
    );
    expect(published.body.revision).toMatch(/^[a-f0-9]{64}$/);

    const snapshot = await app.rest.request(
      `/api/v1/learning/containers/${containerId}/skills`,
      {
        headers: {
          authorization: `Bearer ${API_KEY}`,
          accept: "application/zip",
        },
      },
    );
    expect(snapshot.status).toBe(200);
    expect(snapshot.headers.get("x-copilotkit-skills-revision")).toBe(
      published.body.revision,
    );
    expect(snapshot.headers.get("content-type")).toBe("application/zip");

    const revalidated = await app.rest.request(
      `/api/v1/learning/containers/${containerId}/skills`,
      {
        headers: {
          authorization: `Bearer ${API_KEY}`,
          accept: "application/zip",
          "if-none-match": snapshot.headers.get("etag")!,
        },
      },
    );
    expect(revalidated.status).toBe(304);

    const missing = await app.rest.request(
      `/api/v1/learning/containers/nope/skills`,
      {
        headers: {
          authorization: `Bearer ${API_KEY}`,
          accept: "application/zip",
        },
      },
    );
    expect(missing.status).toBe(404);
  });

  test("a container reads back, a thread records its assignment, and the batch serves several snapshots", async () => {
    const containerId = `c-${unique()}`;
    const created = await call(app, "/api/learning/projects/1/containers", {
      method: "POST",
      headers: { "x-copilotkit-ei-service-token": SERVICE_TOKEN },
      body: JSON.stringify({ id: containerId, name: "Batch" }),
    });
    expect(created.status).toBe(201);

    const container = await call(
      app,
      `/api/v1/learning/containers/${containerId}`,
    );
    expect(container.status).toBe(200);
    expect(container.body.container.name).toBe("Batch");

    const unavailable = await call(app, "/api/v1/learning/containers", {
      method: "GET",
    });
    expect(unavailable.body.nextCursor).toBeNull();

    const published = await call(
      app,
      `/api/v1/learning/containers/${containerId}/skills`,
      {
        method: "PUT",
        body: "zip-bytes",
      },
    );
    expect(published.status).toBe(200);
    const revision = published.body.revision;

    const batch = await call(app, "/api/v1/learning/skills/batch", {
      method: "POST",
      body: JSON.stringify({
        containers: [{ containerId }, { containerId: "missing" }],
      }),
    });
    expect(batch.status).toBe(200);
    const [snapshot, missing] = batch.body.containers;
    expect(snapshot.status).toBe("snapshot");
    expect(snapshot.revision).toBe(revision);
    expect(Buffer.from(snapshot.bytesBase64, "base64").toString()).toBe(
      "zip-bytes",
    );
    expect(missing.error.code).toBe("CONTAINER_NOT_FOUND");

    const unchanged = await call(app, "/api/v1/learning/skills/batch", {
      method: "POST",
      body: JSON.stringify({
        containers: [{ containerId, ifNoneMatch: snapshot.etag }],
      }),
    });
    expect(unchanged.body.containers[0]).toEqual({
      containerId,
      status: "unchanged",
      revision,
      etag: snapshot.etag,
    });

    const unknownRevision = await call(app, "/api/v1/learning/skills/batch", {
      method: "POST",
      body: JSON.stringify({ containers: [{ containerId, revision: "nope" }] }),
    });
    expect(unknownRevision.body.containers[0].error.code).toBe(
      "REVISION_NOT_FOUND",
    );

    const threadId = unique();
    const withContainer = await call(app, "/api/threads", {
      method: "POST",
      body: JSON.stringify({
        threadId,
        userId: "u1",
        agentId: "a1",
        learningContainerId: containerId,
      }),
    });
    expect(withContainer.status).toBe(201);
    expect(withContainer.body.thread.learningContainerId).toBe(containerId);
    expect(withContainer.body.thread.learningContainerIds).toEqual([
      containerId,
    ]);

    const unknownContainer = await call(app, "/api/threads", {
      method: "POST",
      body: JSON.stringify({
        threadId: unique(),
        userId: "u1",
        agentId: "a1",
        learningContainerId: "missing",
      }),
    });
    expect(unknownContainer.status).toBe(404);
    expect(unknownContainer.body.error.code).toBe(
      "LEARNING_CONTAINER_NOT_FOUND",
    );

    const lockUnknown = await call(app, `/api/threads/${unique()}/lock`, {
      method: "POST",
      body: JSON.stringify({
        runId: unique(),
        userId: "u1",
        agentId: "a1",
        learningContainerId: "missing",
      }),
    });
    expect(lockUnknown.status).toBe(404);
    expect(lockUnknown.body.error.code).toBe("LEARNING_CONTAINER_NOT_FOUND");

    const second = `c-${unique()}`;
    await call(app, "/api/learning/projects/1/containers", {
      method: "POST",
      headers: { "x-copilotkit-ei-service-token": SERVICE_TOKEN },
      body: JSON.stringify({ id: second, name: "Second" }),
    });
    const assigned = await call(app, `/api/threads/${threadId}/lock`, {
      method: "POST",
      body: JSON.stringify({
        runId: unique(),
        userId: "u1",
        agentId: "a1",
        learningContainerId: second,
      }),
    });
    expect(assigned.status).toBe(200);
    const reread = await call(app, `/api/threads/${threadId}?userId=u1`);
    expect(reread.body.thread.learningContainerIds.sort()).toEqual(
      [containerId, second].sort(),
    );
    expect(reread.body.thread.learningContainerId).toBeNull();

    const start = await call(
      app,
      `/api/v1/learning/containers/${containerId}/runs`,
      {
        method: "POST",
        body: "{}",
      },
    );
    expect(start.status).toBe(501);
    expect(start.body.error.code).toBe("LEARNING_ENGINE_UNAVAILABLE");

    const inspector = await call(app, "/api/inspector/learning");
    expect(inspector.status).toBe(200);
    expect(inspector.body.schemaVersion).toBe(1);
    expect(inspector.body.skillsPage.total).toBe(0);
  });

  test("a delivery file uploads and hydrates back", async () => {
    const uploaded = await call(
      app,
      `/api/channels/deliveries/dlv-1/files?operationId=op&filename=a.png`,
      {
        method: "POST",
        headers: { "content-type": "image/png" },
        body: "PNG",
      },
    );
    expect(typeof uploaded.body.handle).toBe("string");
    const asset = await app.rest.request(
      `/api/channels/files/${uploaded.body.handle}`,
      {
        headers: { authorization: `Bearer ${API_KEY}` },
      },
    );
    expect(asset.status).toBe(200);
    expect(asset.headers.get("content-type")).toBe("image/png");
    expect(await asset.text()).toBe("PNG");
  });

  test("the inspector answers unknown threads with an empty page, not an error", async () => {
    const events = await call(app, `/api/_inspect/threads/missing/events`);
    expect(events.status).toBe(200);
    expect(events.body.events).toEqual([]);
  });
});

describe("the gateway relays between planes", () => {
  let app: ReturnType<typeof intelligence>;
  beforeAll(() => (app = intelligence()));

  test("a runner push echoes on ingestion and lands on the thread channel", async () => {
    const { body } = await call(app, "/api/threads", {
      method: "POST",
      body: JSON.stringify({ threadId: unique(), userId: "u1", agentId: "a1" }),
    });
    const gateway = app.gateway;
    const runner = captureSocket();
    const minted = await call(
      app,
      `/api/threads/${body.thread.id}/join-code?userId=u1`,
    );
    const viewer = captureSocket({
      phoenix: "client",
      joinToken: minted.body.joinToken,
    });
    gateway.handle(
      viewer,
      JSON.stringify([
        null,
        "1",
        `thread:${body.thread.id}`,
        "phx_join",
        { stream_mode: "run" },
      ]),
    );
    await new Promise((r) => setTimeout(r, 100)); // the join's replay settles first
    const runId = `run-${unique()}`;
    gateway.handle(
      runner,
      JSON.stringify([
        null,
        "1",
        `ingestion:${runId}`,
        "phx_join",
        { thread_id: body.thread.id, run_id: runId },
      ]),
    );
    gateway.handle(
      runner,
      JSON.stringify([
        null,
        "2",
        `ingestion:${runId}`,
        "events",
        {
          events: [
            {
              type: "RUN_STARTED",
              threadId: body.thread.id,
              runId,
              timestamp: 1,
              metadata: { cpki_event_id: "r1", cpki_event_seq: 1 },
            },
          ],
        },
      ]),
    );
    await new Promise((r) => setTimeout(r, 250));

    const runnerEvents = runner.frames.map((f: string) => JSON.parse(f)[3]);
    expect(runnerEvents).toContain("phx_reply"); // the join ok
    expect(runnerEvents).toContain("ag-ui"); // the echo that feeds the run stream
    const viewerFrame = JSON.parse(viewer.frames.at(-1));
    expect(viewerFrame[2]).toBe(`thread:${body.thread.id}`);
    expect(viewerFrame[3]).toBe("ag_ui_event");
    expect(viewerFrame[4].type).toBe("RUN_STARTED");
  });

  test("control pushes relay to the topic's other members", async () => {
    const gateway = app.gateway;
    const a = captureSocket({ phoenix: "channels" });
    const b = captureSocket({ phoenix: "channels" });
    gateway.handle(a, JSON.stringify([null, "1", "control", "phx_join", {}]));
    gateway.handle(b, JSON.stringify([null, "1", "control", "phx_join", {}]));
    gateway.handle(
      a,
      JSON.stringify([
        null,
        "2",
        "control",
        "delivery_invitation",
        { deliveryId: "d1" },
      ]),
    );
    const received = JSON.parse(b.frames.at(-1));
    expect(received[3]).toBe("delivery_invitation");
    expect(received[4].deliveryId).toBe("d1");
  });
});

/** A socket the gateway can drive, recording what it sends. */
function captureSocket(
  options: {
    phoenix?: "runner" | "client" | "channels";
    joinToken?: string;
    token?: string | null;
  } = {},
) {
  const frames: string[] = [];
  const socket = {
    data: {
      phoenix: options.phoenix ?? "runner",
      joined: new Map<string, string | null>(),
      streamModes: new Map<string, string | undefined>(),
      authorized: new Set<string>(),
      joinToken: options.joinToken,
      token:
        options.token !== undefined
          ? (options.token ?? undefined)
          : (options.phoenix ?? "runner") === "client"
            ? undefined
            : API_KEY,
    },
    send: (data: string) => void frames.push(data),
    close: () => {},
    frames,
  };
  return socket as typeof socket & { frames: string[] };
}

describe("the hosted realtime control contract", () => {
  let app: ReturnType<typeof intelligence>;
  beforeAll(() => (app = intelligence()));

  const wait = (ms: number) =>
    new Promise((resolve) => setTimeout(resolve, ms));
  const framesOf = (socket: ReturnType<typeof captureSocket>, event: string) =>
    socket.frames
      .map(
        (frame) => JSON.parse(frame) as [unknown, unknown, string, string, any],
      )
      .filter((frame) => frame[3] === event);

  async function thread(userId = "u1") {
    const threadId = unique();
    await call(app, "/api/threads", {
      method: "POST",
      body: JSON.stringify({ threadId, userId, agentId: "a1" }),
    });
    return threadId;
  }

  function join(
    socket: ReturnType<typeof captureSocket>,
    topic: string,
    params: Record<string, unknown>,
  ) {
    app.gateway.handle(
      socket,
      JSON.stringify([null, "1", topic, "phx_join", params]),
    );
  }

  test("a run join replays no history", async () => {
    const threadId = await thread();
    const runId = `run-${unique()}`;
    await app.store.appendEvents(threadId, runId, [
      {
        seq: 1,
        payload: {
          type: "RUN_STARTED",
          threadId,
          runId,
          metadata: { cpki_event_id: "h1", cpki_event_seq: 1 },
        },
      },
      {
        seq: 2,
        payload: {
          type: "RUN_FINISHED",
          threadId,
          runId,
          metadata: { cpki_event_id: "h2", cpki_event_seq: 2 },
        },
      },
    ]);
    const minted = await call(
      app,
      `/api/threads/${threadId}/join-code?userId=u1`,
    );
    const client = captureSocket({
      phoenix: "client",
      joinToken: minted.body.joinToken,
    });
    join(client, `thread:${threadId}`, { stream_mode: "run", run_id: runId });
    await wait(150);
    expect(framesOf(client, "ag_ui_event")).toEqual([]);
    expect(framesOf(client, "replay_complete")).toEqual([]);
    expect(framesOf(client, "stream_idle")).toEqual([]);
  });

  test("connect does not idle a run that has not emitted an event yet", async () => {
    const threadId = await thread();
    const runId = `run-${unique()}`;
    await call(app, `/api/threads/${threadId}/lock`, {
      method: "POST",
      body: JSON.stringify({ runId, userId: "u1", agentId: "a1" }),
    });
    const minted = await call(
      app,
      `/api/threads/${threadId}/join-code?userId=u1`,
    );
    const client = captureSocket({
      phoenix: "client",
      joinToken: minted.body.joinToken,
    });
    join(client, `thread:${threadId}`, {
      stream_mode: "connect",
      last_seen_event_id: null,
    });
    await wait(150);
    expect(framesOf(client, "replay_complete").length).toBe(1);
    expect(framesOf(client, "stream_idle")).toEqual([]);

    const runner = captureSocket();
    app.gateway.handle(
      runner,
      JSON.stringify([
        null,
        "1",
        `ingestion:${runId}`,
        "phx_join",
        { thread_id: threadId, run_id: runId },
      ]),
    );
    app.gateway.handle(
      runner,
      JSON.stringify([
        null,
        "2",
        `ingestion:${runId}`,
        "events",
        {
          events: [
            {
              type: "RUN_STARTED",
              threadId,
              runId,
              metadata: { cpki_event_id: "i1", cpki_event_seq: 1 },
            },
            {
              type: "RUN_FINISHED",
              threadId,
              runId,
              metadata: { cpki_event_id: "i2", cpki_event_seq: 2 },
            },
          ],
        },
      ]),
    );
    await wait(400);
    const idle = framesOf(client, "stream_idle");
    expect(idle.length).toBe(1);
    expect(idle[0][4].latestEventId).toBe("i2");
  });

  test("an idle thread replays and then idles", async () => {
    const threadId = await thread();
    const runId = `run-${unique()}`;
    await app.store.appendEvents(threadId, runId, [
      {
        seq: 1,
        payload: {
          type: "RUN_STARTED",
          threadId,
          runId,
          metadata: { cpki_event_id: "j1", cpki_event_seq: 1 },
        },
      },
    ]);
    const minted = await call(
      app,
      `/api/threads/${threadId}/join-code?userId=u1`,
    );
    const client = captureSocket({
      phoenix: "client",
      joinToken: minted.body.joinToken,
    });
    join(client, `thread:${threadId}`, {
      stream_mode: "connect",
      last_seen_event_id: null,
    });
    await wait(200);
    expect(framesOf(client, "ag_ui_event").length).toBe(1);
    expect(framesOf(client, "replay_complete").length).toBe(1);
    expect(framesOf(client, "stream_idle").length).toBe(1);
  });

  test("RUN_STARTED merges the input into the transcript instead of replacing it", async () => {
    const threadId = await thread();
    const first = `run-${unique()}`;
    await app.store.appendEvents(threadId, first, [
      {
        seq: 1,
        payload: {
          type: "RUN_STARTED",
          threadId,
          runId: first,
          input: { messages: [{ id: "u1", role: "user", content: "one" }] },
          metadata: { cpki_event_id: "k1", cpki_event_seq: 1 },
        },
      },
      {
        seq: 2,
        payload: {
          type: "TEXT_MESSAGE_START",
          threadId,
          runId: first,
          messageId: "a1",
          role: "assistant",
          metadata: { cpki_event_id: "k2", cpki_event_seq: 2 },
        },
      },
      {
        seq: 3,
        payload: {
          type: "TEXT_MESSAGE_CONTENT",
          threadId,
          runId: first,
          messageId: "a1",
          delta: "answer one",
          metadata: { cpki_event_id: "k3", cpki_event_seq: 3 },
        },
      },
    ]);
    let messages = await call(
      app,
      `/api/threads/${threadId}/messages?userId=u1`,
    );
    expect(messages.body.messages.map((m: any) => m.id)).toEqual(["u1", "a1"]);

    const second = `run-${unique()}`;
    await app.store.appendEvents(threadId, second, [
      {
        seq: 1,
        payload: {
          type: "RUN_STARTED",
          threadId,
          runId: second,
          input: { messages: [{ id: "u2", role: "user", content: "two" }] },
          metadata: { cpki_event_id: "l1", cpki_event_seq: 1 },
        },
      },
      {
        seq: 2,
        payload: {
          type: "TEXT_MESSAGE_START",
          threadId,
          runId: second,
          messageId: "a2",
          role: "assistant",
          metadata: { cpki_event_id: "l2", cpki_event_seq: 2 },
        },
      },
      {
        seq: 3,
        payload: {
          type: "TEXT_MESSAGE_CONTENT",
          threadId,
          runId: second,
          messageId: "a2",
          delta: "answer two",
          metadata: { cpki_event_id: "l3", cpki_event_seq: 3 },
        },
      },
    ]);
    messages = await call(app, `/api/threads/${threadId}/messages?userId=u1`);
    expect(messages.body.messages.map((m: any) => m.id)).toEqual([
      "u1",
      "a1",
      "u2",
      "a2",
    ]);

    const third = `run-${unique()}`;
    await app.store.appendEvents(threadId, third, [
      {
        seq: 1,
        payload: {
          type: "RUN_STARTED",
          threadId,
          runId: third,
          input: { messages: [] },
          metadata: { cpki_event_id: "m1", cpki_event_seq: 1 },
        },
      },
    ]);
    messages = await call(app, `/api/threads/${threadId}/messages?userId=u1`);
    expect(messages.body.messages.map((m: any) => m.id)).toEqual([
      "u1",
      "a1",
      "u2",
      "a2",
    ]);
  });

  test("run start and finish publish thread_run_activity", async () => {
    const threadId = await thread();
    const listed = await call(app, "/api/threads?userId=u1&agentId=a1");
    const subscribed = await call(app, "/api/threads/subscribe", {
      method: "POST",
      body: JSON.stringify({ userId: "u1" }),
    });
    const meta = captureSocket({
      phoenix: "client",
      joinToken: subscribed.body.joinToken,
    });
    join(meta, `user_meta:${listed.body.joinCode}`, {});
    await wait(50);

    const runId = `run-${unique()}`;
    const runner = captureSocket();
    app.gateway.handle(
      runner,
      JSON.stringify([
        null,
        "1",
        `ingestion:${runId}`,
        "phx_join",
        { thread_id: threadId, run_id: runId },
      ]),
    );
    app.gateway.handle(
      runner,
      JSON.stringify([
        null,
        "2",
        `ingestion:${runId}`,
        "events",
        {
          events: [
            {
              type: "RUN_STARTED",
              threadId,
              runId,
              metadata: { cpki_event_id: "n1", cpki_event_seq: 1 },
            },
            {
              type: "RUN_FINISHED",
              threadId,
              runId,
              metadata: { cpki_event_id: "n2", cpki_event_seq: 2 },
            },
          ],
        },
      ]),
    );
    await wait(400);
    const activity = framesOf(meta, "thread_run_activity");
    expect(activity.map((frame) => frame[4].event_type)).toEqual([
      "RUN_STARTED",
      "RUN_FINISHED",
    ]);
    expect(activity[0][4].thread_id).toBe(threadId);
    expect(activity[0][4].run_id).toBe(runId);
  });
});

describe("platform authentication and thread ownership", () => {
  let app: ReturnType<typeof intelligence>;
  beforeAll(() => (app = intelligence()));

  const wait = (ms: number) =>
    new Promise((resolve) => setTimeout(resolve, ms));
  const replyOf = (socket: ReturnType<typeof captureSocket>) =>
    socket.frames
      .map(
        (frame) => JSON.parse(frame) as [unknown, unknown, string, string, any],
      )
      .filter((frame) => frame[3] === "phx_reply")
      .at(-1)?.[4];

  async function threadFor(userId: string) {
    const threadId = unique();
    const created = await call(app, "/api/threads", {
      method: "POST",
      body: JSON.stringify({ threadId, userId, agentId: "a1" }),
    });
    expect(created.status).toBe(201);
    return threadId;
  }

  test("the socket params carry the runner key and the client join token", () => {
    const bearer = Buffer.from(API_KEY).toString("base64").replace(/=/g, "");
    const runner = app.gateway.socketData(
      "runner",
      new Request(
        "http://localhost/api/intelligence-gateway/runner/websocket?vsn=2.0.0",
        {
          headers: {
            "sec-websocket-protocol": `phoenix, base64url.bearer.phx.${bearer}`,
          },
        },
      ),
    );
    expect(runner.token).toBe(API_KEY);
    expect(runner.subprotocol).toBe("phoenix");

    const client = app.gateway.socketData(
      "client",
      new Request(
        "http://localhost/api/intelligence-gateway/client/websocket?vsn=2.0.0&join_token=a-token",
      ),
    );
    expect(client.joinToken).toBe("a-token");
  });

  test("the service token opens only the Learning container route", async () => {
    const id = `c-${unique()}`;
    const created = await call(app, "/api/learning/projects/1/containers", {
      method: "POST",
      headers: {
        Authorization: "",
        "x-copilotkit-ei-service-token": SERVICE_TOKEN,
      },
      body: JSON.stringify({ id, name: "Service" }),
    });
    expect(created.status).toBe(201);

    const wrongRoute = await call(app, "/api/threads?userId=u1", {
      headers: {
        Authorization: "",
        "x-copilotkit-ei-service-token": SERVICE_TOKEN,
      },
    });
    expect(wrongRoute.status).toBe(401);

    const wrongToken = await call(app, "/api/learning/projects/1/containers", {
      method: "POST",
      headers: {
        Authorization: "",
        "x-copilotkit-ei-service-token": "not-the-service-token",
      },
      body: JSON.stringify({ id: `c-${unique()}`, name: "Nope" }),
    });
    expect(wrongToken.status).toBe(401);
  });

  test("the runner plane needs the platform key and only joins ingestion topics", async () => {
    const bad = captureSocket({ phoenix: "runner", token: "wrong" });
    app.gateway.handle(
      bad,
      JSON.stringify([null, "1", "ingestion:run-1", "phx_join", {}]),
    );
    await wait(100);
    expect(replyOf(bad)?.response.reason).toBe("unauthorized");

    const runner = captureSocket({ phoenix: "runner" });
    app.gateway.handle(
      runner,
      JSON.stringify([null, "1", "thread:some-thread", "phx_join", {}]),
    );
    await wait(100);
    expect(replyOf(runner)?.response.reason).toBe("unmatched topic");
  });

  test("a client token joins one thread once and cannot reach ingestion", async () => {
    const threadId = await threadFor("u1");
    const minted = await call(
      app,
      `/api/threads/${threadId}/join-code?userId=u1`,
    );
    const first = captureSocket({
      phoenix: "client",
      joinToken: minted.body.joinToken,
    });
    app.gateway.handle(
      first,
      JSON.stringify([
        null,
        "1",
        `thread:${threadId}`,
        "phx_join",
        { stream_mode: "connect" },
      ]),
    );
    await wait(100);
    expect(JSON.parse(first.frames[0])[3]).toBe("phx_reply");
    expect(JSON.parse(first.frames[0])[4].status).toBe("ok");

    const second = captureSocket({
      phoenix: "client",
      joinToken: minted.body.joinToken,
    });
    app.gateway.handle(
      second,
      JSON.stringify([
        null,
        "1",
        `thread:${threadId}`,
        "phx_join",
        { stream_mode: "connect" },
      ]),
    );
    await wait(100);
    expect(replyOf(second)?.response.reason).toBe("unauthorized");

    const extra = await call(
      app,
      `/api/threads/${threadId}/join-code?userId=u1`,
    );
    const client = captureSocket({
      phoenix: "client",
      joinToken: extra.body.joinToken,
    });
    app.gateway.handle(
      client,
      JSON.stringify([null, "1", "ingestion:any", "phx_join", {}]),
    );
    await wait(100);
    expect(replyOf(client)?.response.reason).toBe("unmatched topic");
  });

  test("a join code is stable and a token only joins its own code", async () => {
    const first = await call(app, "/api/threads?userId=u1&agentId=a1");
    const again = await call(app, "/api/threads?userId=u1&agentId=a1");
    expect(first.body.joinCode).toBe(again.body.joinCode);
    expect(first.body.joinToken).toBeUndefined();

    const other = await call(app, "/api/threads?userId=u2&agentId=a1");
    expect(other.body.joinCode).not.toBe(first.body.joinCode);

    const subscribed = await call(app, "/api/threads/subscribe", {
      method: "POST",
      body: JSON.stringify({ userId: "u1" }),
    });
    const socket = captureSocket({
      phoenix: "client",
      joinToken: subscribed.body.joinToken,
    });
    app.gateway.handle(
      socket,
      JSON.stringify([
        null,
        "1",
        `user_meta:${other.body.joinCode}`,
        "phx_join",
        {},
      ]),
    );
    await wait(100);
    expect(replyOf(socket)?.response.reason).toBe("unauthorized");
  });

  test("another person's thread is a 404 on every route", async () => {
    const threadId = await threadFor("u1");
    const runId = `run-${unique()}`;

    expect((await call(app, `/api/threads/${threadId}`)).status).toBe(400);
    expect((await call(app, `/api/threads/${threadId}?userId=u2`)).status).toBe(
      404,
    );
    expect(
      (await call(app, `/api/threads/${threadId}/messages?userId=u2`)).status,
    ).toBe(404);
    expect(
      (await call(app, `/api/threads/${threadId}/join-code?userId=u2`)).status,
    ).toBe(404);
    expect(
      (
        await call(app, `/api/threads/${threadId}`, {
          method: "PATCH",
          body: JSON.stringify({
            userId: "u2",
            agentId: "a1",
            name: "Mine now",
          }),
        })
      ).status,
    ).toBe(404);
    expect(
      (
        await call(app, `/api/threads/${threadId}`, {
          method: "DELETE",
          body: JSON.stringify({ userId: "u2", agentId: "a1" }),
        })
      ).status,
    ).toBe(404);
    expect(
      (
        await call(app, `/api/threads/${threadId}/lock`, {
          method: "POST",
          body: JSON.stringify({ runId, userId: "u2", agentId: "a1" }),
        })
      ).status,
    ).toBe(404);
    expect(
      (
        await call(app, `/api/threads/${threadId}/connect`, {
          method: "POST",
          body: JSON.stringify({ userId: "u2", agentId: "a1" }),
        })
      ).status,
    ).toBe(404);

    const renamed = await call(app, `/api/threads/${threadId}`, {
      method: "PATCH",
      body: JSON.stringify({ userId: "u1", agentId: "a1", name: "Still mine" }),
    });
    expect(renamed.body.thread.name).toBe("Still mine");
    expect((await call(app, "/api/threads?userId=u1")).status).toBe(200);
    expect((await call(app, "/api/threads")).status).toBe(400);
    expect(
      (
        await call(app, `/api/threads/${threadId}`, {
          method: "DELETE",
          body: JSON.stringify({ userId: "u1", agentId: "a1" }),
        })
      ).status,
    ).toBe(200);
  });
});

describe("the hosted lock and thread lifecycle contract", () => {
  let app: ReturnType<typeof intelligence>;
  beforeAll(() => (app = intelligence()));

  async function threadFor(userId = "u1") {
    const threadId = unique();
    await call(app, "/api/threads", {
      method: "POST",
      body: JSON.stringify({ threadId, userId, agentId: "a1" }),
    });
    return threadId;
  }

  test("the lock lifetime is the platform's 120 seconds", async () => {
    const threadId = await threadFor();
    const runId = `run-${unique()}`;
    for (const ttlSeconds of [5, 3600]) {
      await call(app, `/api/threads/${threadId}/lock`, {
        method: "DELETE",
        body: JSON.stringify({ runId }),
      });
      const acquired = await call(app, `/api/threads/${threadId}/lock`, {
        method: "POST",
        body: JSON.stringify({
          runId,
          userId: "u1",
          agentId: "a1",
          ttlSeconds,
        }),
      });
      expect(acquired.status).toBe(200);
      expect(acquired.body.ttlSeconds).toBe(120);
    }
    const renewed = await call(app, `/api/threads/${threadId}/lock`, {
      method: "PATCH",
      body: JSON.stringify({ runId, ttlSeconds: 1 }),
    });
    expect(renewed.body).toEqual({ ttlSeconds: 120, status: "renewed" });
  });

  test("renewing a lock another run holds is a 409, not a finish", async () => {
    const threadId = await threadFor();
    const runId = `run-${unique()}`;
    await call(app, `/api/threads/${threadId}/lock`, {
      method: "POST",
      body: JSON.stringify({ runId, userId: "u1", agentId: "a1" }),
    });
    const wrong = await call(app, `/api/threads/${threadId}/lock`, {
      method: "PATCH",
      body: JSON.stringify({ runId: "another-run", ttlSeconds: 120 }),
    });
    expect(wrong.status).toBe(409);
    expect(wrong.body.error.code).toBe("THREAD_LOCK_RENEWAL_FAILED");
    expect(wrong.body.error.retryable).toBe(false);

    // Once the run releases it, the same renewal reports completion.
    await call(app, `/api/threads/${threadId}/lock`, {
      method: "DELETE",
      body: JSON.stringify({ runId }),
    });
    const completed = await call(app, `/api/threads/${threadId}/lock`, {
      method: "PATCH",
      body: JSON.stringify({ runId, ttlSeconds: 120 }),
    });
    expect(completed.body).toEqual({ ttlSeconds: 0, status: "completed" });
  });

  test("connect and lock create the thread for the caller on first use", async () => {
    const threadId = unique();
    const idle = await call(app, `/api/threads/${threadId}/connect`, {
      method: "POST",
      body: JSON.stringify({ userId: "u1", agentId: "a1" }),
    });
    expect(idle.status).toBe(200);
    expect(typeof idle.body.joinToken).toBe("string");
    expect(idle.body.runId).toBeUndefined();
    expect((await call(app, `/api/threads/${threadId}?userId=u1`)).status).toBe(
      200,
    );

    const other = unique();
    const runId = `run-${unique()}`;
    const locked = await call(app, `/api/threads/${other}/lock`, {
      method: "POST",
      body: JSON.stringify({ runId, userId: "u1", agentId: "a1" }),
    });
    expect(locked.status).toBe(200);
    expect((await call(app, `/api/threads/${other}?userId=u1`)).status).toBe(
      200,
    );
    const connect = await call(app, `/api/threads/${other}/connect`, {
      method: "POST",
      body: JSON.stringify({ userId: "u1", agentId: "a1" }),
    });
    expect(connect.body.runId).toBe(runId);
  });

  test("deleting an absent thread is a 404", async () => {
    const threadId = unique();
    const first = await call(app, `/api/threads/${threadId}`, {
      method: "DELETE",
      body: JSON.stringify({ userId: "u1", agentId: "a1" }),
    });
    expect(first.status).toBe(404);
  });
});

describe("thread list pagination", () => {
  let app: ReturnType<typeof intelligence>;
  beforeAll(() => (app = intelligence()));

  test("pages by an opaque cursor and keeps the summary to the hosted shape", async () => {
    const userId = `page-${unique()}`;
    const ids: string[] = [];
    for (let index = 0; index < 3; index += 1) {
      const threadId = `page-${unique()}`;
      ids.push(threadId);
      const created = await call(app, "/api/threads", {
        method: "POST",
        body: JSON.stringify({ threadId, userId, agentId: "a1" }),
      });
      expect(created.status).toBe(201);
      await new Promise((resolve) => setTimeout(resolve, 5));
    }

    const first = await call(
      app,
      `/api/threads?userId=${userId}&agentId=a1&limit=2`,
    );
    expect(first.status).toBe(200);
    expect(first.body.threads.length).toBe(2);
    expect(typeof first.body.nextCursor).toBe("string");
    expect(first.body.threads[0].lastRunAt).toBeUndefined();
    expect(first.body.threads[0].lastUpdatedAt).toBeUndefined();
    expect(first.body.threads[0].learningContainerId).toBeNull();

    const second = await call(
      app,
      `/api/threads?userId=${userId}&agentId=a1&limit=2&cursor=${encodeURIComponent(first.body.nextCursor)}`,
    );
    expect(second.status).toBe(200);
    expect(second.body.threads.length).toBe(1);
    expect(second.body.nextCursor).toBeNull();
    expect(
      [
        ...first.body.threads.map((thread: any) => thread.id),
        ...second.body.threads.map((thread: any) => thread.id),
      ].sort(),
    ).toEqual([...ids].sort());

    const invalid = await call(
      app,
      `/api/threads?userId=${userId}&agentId=a1&cursor=not-a-cursor`,
    );
    expect(invalid.status).toBe(400);
  });

  test("a rename moves a thread to the front of the next page", async () => {
    const userId = `page-${unique()}`;
    const firstId = `page-${unique()}`;
    const secondId = `page-${unique()}`;
    for (const threadId of [firstId, secondId]) {
      await call(app, "/api/threads", {
        method: "POST",
        body: JSON.stringify({ threadId, userId, agentId: "a1" }),
      });
      await new Promise((resolve) => setTimeout(resolve, 5));
    }
    await call(app, `/api/threads/${firstId}`, {
      method: "PATCH",
      body: JSON.stringify({ userId, agentId: "a1", name: "Newest" }),
    });
    const listed = await call(app, `/api/threads?userId=${userId}&agentId=a1`);
    expect(listed.body.threads[0].id).toBe(firstId);
  });
});

describe("long-thread replay and inspector pages", () => {
  let app: ReturnType<typeof intelligence>;
  beforeAll(() => (app = intelligence()));

  test("a cursor near the end replays only what follows it", async () => {
    const threadId = unique();
    const userId = `long-${unique()}`;
    await call(app, "/api/threads", {
      method: "POST",
      body: JSON.stringify({ threadId, userId, agentId: "a1" }),
    });

    const database = createDatabase(testDatabaseUrl(), TEST_POOL);
    const runId = `run-${unique()}`;
    const rows = Array.from({ length: 2105 }, (_, index) => ({
      threadId,
      runId,
      seq: index + 1,
      event: {
        type:
          index + 1 === 1500
            ? "STATE_SNAPSHOT"
            : index + 1 === 2000
              ? "STATE_DELTA"
              : "TEXT_MESSAGE_CONTENT",
        ...(index + 1 === 1500 ? { snapshot: { count: 1 } } : {}),
        ...(index + 1 === 2000
          ? { delta: [{ op: "add", path: "/count", value: 2 }] }
          : {}),
        metadata: {
          cpki_event_id: `bulk-${index + 1}`,
          cpki_event_seq: index + 1,
        },
      },
    }));
    await database.insert(intelligenceThreadEvents).values(rows);

    const after = await app.store.listEventsAfter(threadId, "bulk-1500");
    expect(after.length).toBe(605);
    expect((after[0].metadata as { cpki_event_id: string }).cpki_event_id).toBe(
      "bulk-1501",
    );
    expect(
      (after.at(-1)!.metadata as { cpki_event_id: string }).cpki_event_id,
    ).toBe("bulk-2105");

    const inspector = await call(
      app,
      `/api/_inspect/threads/${threadId}/events`,
    );
    expect(inspector.body.events.length).toBe(1000);
    expect(inspector.body.truncated).toBe(true);

    const state = await call(app, `/api/_inspect/threads/${threadId}/state`);
    expect(state.body).toEqual({
      kind: "snapshot",
      state: { count: 2 },
      skippedDeltas: 0,
    });

    await call(app, `/api/threads/${threadId}`, {
      method: "DELETE",
      body: JSON.stringify({ userId, agentId: "a1" }),
    });
  });
});

describe("memory scope, grants and absorption", () => {
  let app: ReturnType<typeof intelligence>;
  beforeAll(() => (app = intelligence()));

  test("a grant that excludes project scope cannot read or write it", async () => {
    const user = `grant-${unique()}`;
    const userHeaders = { "x-cpki-user-id": user };
    const grant = { user: "read-write", project: "none" };
    const grantHeaders = {
      ...userHeaders,
      "x-cpki-memory-grant": JSON.stringify(grant),
    };

    await call(app, "/api/memories", {
      method: "POST",
      headers: userHeaders,
      body: JSON.stringify({
        content: "Vacation policy allows 30 days",
        kind: "topical",
        scope: "project",
      }),
    });
    await call(app, "/api/memories", {
      method: "POST",
      headers: userHeaders,
      body: JSON.stringify({
        content: "Ada prefers concise reports",
        kind: "topical",
        scope: "user",
      }),
    });

    const listWithGrant = await call(app, "/api/memories", {
      headers: grantHeaders,
    });
    expect(
      listWithGrant.body.memories.every(
        (memory: any) => memory.scope === "user",
      ),
    ).toBe(true);

    const recallUser = await call(app, "/api/memories/recall", {
      method: "POST",
      headers: grantHeaders,
      body: JSON.stringify({ query: "vacation", scope: "user" }),
    });
    expect(recallUser.body.memories).toEqual([]);

    const recallProject = await call(app, "/api/memories/recall", {
      method: "POST",
      headers: grantHeaders,
      body: JSON.stringify({ query: "vacation", scope: "project" }),
    });
    expect(recallProject.status).toBe(403);
    expect(recallProject.body.error.code).toBe("MEMORY_NOT_ENTITLED");

    const createProject = await call(app, "/api/memories", {
      method: "POST",
      headers: grantHeaders,
      body: JSON.stringify({
        content: "Another project fact",
        kind: "topical",
        scope: "project",
      }),
    });
    expect(createProject.status).toBe(403);

    const project = (
      await call(app, "/api/memories", { headers: userHeaders })
    ).body.memories.find((memory: any) => memory.scope === "project");
    const patchProject = await call(app, `/api/memories/${project.id}`, {
      method: "PATCH",
      headers: grantHeaders,
      body: JSON.stringify({ content: "Changed", kind: "topical" }),
    });
    expect(patchProject.status).toBe(403);
  });

  test("supersede keeps the scope and sources the caller left out", async () => {
    const user = `scope-${unique()}`;
    const headers = { "x-cpki-user-id": user };
    const created = await call(app, "/api/memories", {
      method: "POST",
      headers,
      body: JSON.stringify({
        content: "Vacation policy allows 30 days",
        kind: "topical",
        scope: "project",
        sourceThreadIds: ["thread-a"],
      }),
    });
    const replaced = await call(app, `/api/memories/${created.body.id}`, {
      method: "PATCH",
      headers,
      body: JSON.stringify({ content: "Vacation policy allows 25 days" }),
    });
    expect(replaced.status).toBe(200);
    expect(replaced.body.scope).toBe("project");
    expect(replaced.body.kind).toBe("topical");
    expect(replaced.body.sourceThreadIds).toEqual(["thread-a"]);
  });

  test("near-duplicates absorb on normalised content and merge their sources", async () => {
    const user = `absorb-${unique()}`;
    const headers = { "x-cpki-user-id": user };
    const first = await call(app, "/api/memories", {
      method: "POST",
      headers,
      body: JSON.stringify({
        content: "Ada prefers concise reports",
        kind: "topical",
        scope: "user",
        sourceThreadIds: ["thread-a"],
      }),
    });
    const second = await call(app, "/api/memories", {
      method: "POST",
      headers,
      body: JSON.stringify({
        content: "  Ada   PREFERS concise reports.  ",
        kind: "topical",
        scope: "user",
        sourceThreadIds: ["thread-b"],
      }),
    });
    expect(second.body.absorbed).toBe(true);
    expect(second.body.id).toBe(first.body.id);
    expect(second.body.sourceThreadIds.sort()).toEqual([
      "thread-a",
      "thread-b",
    ]);

    const project = await call(app, "/api/memories", {
      method: "POST",
      headers,
      body: JSON.stringify({
        content: "Ada prefers concise reports",
        kind: "topical",
        scope: "project",
      }),
    });
    expect(project.body.absorbed).toBe(false);
    expect(project.body.id).not.toBe(first.body.id);
  });
});

describe("event batch writes", () => {
  let app: ReturnType<typeof intelligence>;
  beforeAll(() => (app = intelligence()));

  test("two batches on one thread fold without losing either", async () => {
    const threadId = unique();
    await call(app, "/api/threads", {
      method: "POST",
      body: JSON.stringify({ threadId, userId: "u1", agentId: "a1" }),
    });
    const batch = (runId: string, suffix: string) => [
      {
        seq: 1,
        payload: {
          type: "RUN_STARTED",
          threadId,
          runId,
          input: {
            messages: [{ id: `u-${suffix}`, role: "user", content: suffix }],
          },
          metadata: { cpki_event_id: `e-${suffix}-1`, cpki_event_seq: 1 },
        },
      },
      {
        seq: 2,
        payload: {
          type: "TEXT_MESSAGE_START",
          threadId,
          runId,
          messageId: `a-${suffix}`,
          role: "assistant",
          metadata: { cpki_event_id: `e-${suffix}-2`, cpki_event_seq: 2 },
        },
      },
      {
        seq: 3,
        payload: {
          type: "TEXT_MESSAGE_CONTENT",
          threadId,
          runId,
          messageId: `a-${suffix}`,
          delta: `answer ${suffix}`,
          metadata: { cpki_event_id: `e-${suffix}-3`, cpki_event_seq: 3 },
        },
      },
    ];
    await Promise.all([
      app.store.appendEvents(
        threadId,
        `run-${unique()}`,
        batch(`run-${unique()}`, "one"),
      ),
      app.store.appendEvents(
        threadId,
        `run-${unique()}`,
        batch(`run-${unique()}`, "two"),
      ),
    ]);
    const messages = await call(
      app,
      `/api/threads/${threadId}/messages?userId=u1`,
    );
    expect(
      messages.body.messages.map((message: any) => message.id).sort(),
    ).toEqual(["a-one", "a-two", "u-one", "u-two"]);
  });

  test("a batch is one insert and a replayed batch stores nothing new", async () => {
    const threadId = unique();
    await call(app, "/api/threads", {
      method: "POST",
      body: JSON.stringify({ threadId, userId: "u1", agentId: "a1" }),
    });
    const runId = `run-${unique()}`;
    const events = Array.from({ length: 50 }, (_, index) => ({
      seq: index + 1,
      payload: {
        type: "TEXT_MESSAGE_CONTENT",
        threadId,
        runId,
        messageId: "a1",
        delta: "x",
        metadata: {
          cpki_event_id: `b-${runId}-${index + 1}`,
          cpki_event_seq: index + 1,
        },
      },
    }));
    const first = await app.store.appendEvents(threadId, runId, events);
    expect(first.events.length).toBe(50);
    const replay = await app.store.appendEvents(threadId, runId, events);
    expect(replay.events.length).toBe(0);
    const inspect = await call(app, `/api/_inspect/threads/${threadId}/events`);
    expect(inspect.body.events.length).toBe(50);
  });
});
