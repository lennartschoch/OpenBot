import { beforeAll, describe, expect, test } from "bun:test";
import { createDatabase } from "../src/db/client";
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

function intelligence() {
  return createLocalIntelligence({
    database: createDatabase(testDatabaseUrl(), TEST_POOL),
    apiKey: API_KEY,
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
    expect(created.status).toBe(200);
    expect(created.body.thread.id).toBe(threadId);
    expect(created.body.thread.createdById).toBe("u1");

    const duplicate = await call(app, "/api/threads", {
      method: "POST",
      body: JSON.stringify({ threadId, userId: "u1", agentId: "a1" }),
    });
    expect(duplicate.status).toBe(409);

    const renamed = await call(app, `/api/threads/${threadId}`, {
      method: "PATCH",
      body: JSON.stringify({ name: "Renamed" }),
    });
    expect(renamed.body.thread.name).toBe("Renamed");

    const listed = await call(
      app,
      `/api/threads?userId=u1&agentId=a1`,
    );
    expect(listed.body.threads.some((t: any) => t.id === threadId)).toBe(true);
    // The thread-list meta channel's credential rides the page.
    expect(typeof listed.body.joinCode).toBe("string");

    const gone = await call(app, `/api/threads/${threadId}`, { method: "DELETE" });
    expect(gone.status).toBe(200);
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
    const minted = await call(app, `/api/threads/${body.thread.id}/join-code`);
    expect(minted.status).toBe(200);
    const store = app.gateway.store;
    expect((await store.authorizeTopic(minted.body.joinToken, `thread:${body.thread.id}`)).ok).toBe(true);
    expect((await store.authorizeTopic(minted.body.joinToken, "thread:other")).ok).toBe(false);
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
      { type: "RUN_STARTED", threadId, runId, timestamp: 1, metadata: { cpki_event_id: "e1", cpki_event_seq: 1 } },
      { type: "TEXT_MESSAGE_START", threadId, runId, timestamp: 2, messageId: "m1", role: "user", metadata: { cpki_event_id: "e2", cpki_event_seq: 2 } },
      { type: "TEXT_MESSAGE_CONTENT", threadId, runId, timestamp: 3, messageId: "m1", delta: "hello", metadata: { cpki_event_id: "e3", cpki_event_seq: 3 } },
      { type: "TEXT_MESSAGE_END", threadId, runId, timestamp: 4, messageId: "m1", metadata: { cpki_event_id: "e4", cpki_event_seq: 4 } },
      { type: "RUN_FINISHED", threadId, runId, timestamp: 5, metadata: { cpki_event_id: "e5", cpki_event_seq: 5 } },
    ];
    const gateway = app.gateway;
    const socket = captureSocket();
    gateway.handleClose(socket); // no-op: proves close tolerates an unjoined socket
    for (const event of events) {
      // The runner's durability contract is batch, ack, next — mirror it so
      // the log's insertion order matches the run's seq order.
      gateway.handle(socket, JSON.stringify([null, "1", `ingestion:${runId}`, "events", { events: [event] }]));
      await new Promise((r) => setTimeout(r, 30));
    }
    await new Promise((r) => setTimeout(r, 100));

    const inspect = await call(app, `/api/_inspect/threads/${threadId}/events`);
    expect(inspect.body.events.length).toBe(events.length);
    expect(inspect.body.events[0].type).toBe("RUN_STARTED");

    const messages = await call(app, `/api/threads/${threadId}/messages`);
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
    const event = { type: "RUN_STARTED", threadId, runId, timestamp: 1, metadata: { cpki_event_id: "d1", cpki_event_seq: 1 } };
    gateway.handle(socket, JSON.stringify([null, "1", `ingestion:${runId}`, "events", { events: [event] }]));
    gateway.handle(socket, JSON.stringify([null, "1", `ingestion:${runId}`, "events", { events: [event] }]));
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
      method: "POST", headers,
      body: JSON.stringify({ content: "prefers terse answers", kind: "topical", scope: "user" }),
    });
    expect(created.status).toBe(200);
    expect(created.body.absorbed).toBe(false);

    const duplicate = await call(app, "/api/memories", {
      method: "POST", headers,
      body: JSON.stringify({ content: "prefers terse answers", kind: "topical", scope: "user" }),
    });
    expect(duplicate.body.absorbed).toBe(true);
    expect(duplicate.body.id).toBe(created.body.id);

    const superseded = await call(app, `/api/memories/${created.body.id}`, {
      method: "PATCH", headers,
      body: JSON.stringify({ content: "prefers very terse answers", kind: "topical", scope: "user" }),
    });
    expect(superseded.body.retiredId).toBe(created.body.id);

    const listed = await call(app, "/api/memories", { headers });
    expect(listed.body.memories.length).toBe(1);

    const recalled = await call(app, "/api/memories/recall", {
      method: "POST", headers,
      body: JSON.stringify({ query: "terse answers", limit: 5 }),
    });
    expect(recalled.body.memories.length).toBe(1);

    const retired = await call(app, `/api/memories/${superseded.body.id}`, { method: "DELETE", headers });
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
    const created = await app.rest.request(`/api/learning/projects/1/containers`, {
      method: "POST",
      headers: { "x-copilotkit-ei-service-token": "service-token", "content-type": "application/json" },
      body: JSON.stringify({ id: containerId, name: "Integration" }),
    });
    expect(created.status).toBe(201);
    const present = await app.rest.request(`/api/learning/projects/1/containers`, {
      method: "POST",
      headers: { "x-copilotkit-ei-service-token": "service-token", "content-type": "application/json" },
      body: JSON.stringify({ id: containerId, name: "Integration" }),
    });
    expect(present.status).toBe(409);

    const page = await call(app, "/api/v1/learning/containers");
    expect(page.body.containers.some((c: any) => c.id === containerId)).toBe(true);

    const published = await call(app, `/api/v1/learning/containers/${containerId}/skills`, {
      method: "PUT",
      body: "zip-bytes",
    });
    expect(published.body.revision).toMatch(/^[a-f0-9]{64}$/);

    const snapshot = await app.rest.request(`/api/v1/learning/containers/${containerId}/skills`, {
      headers: { authorization: `Bearer ${API_KEY}`, accept: "application/zip" },
    });
    expect(snapshot.status).toBe(200);
    expect(snapshot.headers.get("x-copilotkit-skills-revision")).toBe(published.body.revision);
    expect(snapshot.headers.get("content-type")).toBe("application/zip");

    const revalidated = await app.rest.request(`/api/v1/learning/containers/${containerId}/skills`, {
      headers: { authorization: `Bearer ${API_KEY}`, accept: "application/zip", "if-none-match": snapshot.headers.get("etag")! },
    });
    expect(revalidated.status).toBe(304);

    const missing = await app.rest.request(`/api/v1/learning/containers/nope/skills`, {
      headers: { authorization: `Bearer ${API_KEY}`, accept: "application/zip" },
    });
    expect(missing.status).toBe(403);
  });

  test("a delivery file uploads and hydrates back", async () => {
    const uploaded = await call(app, `/api/channels/deliveries/dlv-1/files?operationId=op&filename=a.png`, {
      method: "POST",
      headers: { "content-type": "image/png" },
      body: "PNG",
    });
    expect(typeof uploaded.body.handle).toBe("string");
    const asset = await app.rest.request(`/api/channels/files/${uploaded.body.handle}`, {
      headers: { authorization: `Bearer ${API_KEY}` },
    });
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
    const viewer = captureSocket();
    gateway.handle(viewer, JSON.stringify([null, "1", `thread:${body.thread.id}`, "phx_join", { stream_mode: "run" }]));
    await new Promise((r) => setTimeout(r, 100)); // the join's replay settles first
    const runId = `run-${unique()}`;
    gateway.handle(runner, JSON.stringify([null, "1", `ingestion:${runId}`, "phx_join", { thread_id: body.thread.id, run_id: runId }]));
    gateway.handle(runner, JSON.stringify([null, "2", `ingestion:${runId}`, "events", { events: [{ type: "RUN_STARTED", threadId: body.thread.id, runId, timestamp: 1, metadata: { cpki_event_id: "r1", cpki_event_seq: 1 } }] }]));
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
    const a = captureSocket();
    const b = captureSocket();
    gateway.handle(a, JSON.stringify([null, "1", "control", "phx_join", {}]));
    gateway.handle(b, JSON.stringify([null, "1", "control", "phx_join", {}]));
    gateway.handle(a, JSON.stringify([null, "2", "control", "delivery_invitation", { deliveryId: "d1" }]));
    const received = JSON.parse(b.frames.at(-1));
    expect(received[3]).toBe("delivery_invitation");
    expect(received[4].deliveryId).toBe("d1");
  });
});

/** A socket the gateway can drive, recording what it sends. */
function captureSocket() {
  const frames: string[] = [];
  const socket = {
    data: { phoenix: "runner", joined: new Map<string, string | null>() },
    send: (data: string) => void frames.push(data),
    close: () => {},
    frames,
  };
  return socket as typeof socket & { frames: string[] };
}
