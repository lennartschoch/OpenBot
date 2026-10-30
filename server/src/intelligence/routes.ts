import { Hono } from "hono";
import { IntelligenceStore, toMemorySummary, toThreadSummary } from "./store";
import { foldState } from "./state";

/**
 * The Intelligence REST surface, as `CopilotKitIntelligence` speaks it.
 *
 * Mounted under one base path (`INTELLIGENCE_API_URL` points here); every response
 * is the shape the runtime's platform client parses, so no OpenBot code changes to
 * read from it. `userId` arrives as query parameter on thread routes and as the
 * `x-cpki-user-id` header on memory routes — exactly the hosted platform's split.
 */

export type IntelligenceRestDependencies = {
  store: IntelligenceStore;
  /** Shared secret checked against `Authorization: Bearer …`. */
  apiKey: string;
  /** Fan-out to the user's live meta channels (thread list, memories). */
  meta?: {
    threads: (userId: string, payload: Record<string, unknown>) => void;
    memories: (userId: string, payload: Record<string, unknown>) => void;
  };
};

/** The `thread_metadata` delta shape the client thread store applies. */
const threadDelta = (
  operation: "created" | "updated" | "archived" | "deleted",
  thread: Record<string, unknown>,
) => ({
  operation,
  thread,
  threadId: thread.id,
  ...(operation === "deleted" ? { deleted: { id: thread.id } } : {}),
});

/** The `memory_metadata` delta shape the client memory store applies. */
const memoryDelta = (
  operation: "upserted" | "invalidated",
  memory: Record<string, unknown>,
) => ({
  operation,
  memory,
  ...(operation === "invalidated" ? { invalidated: { id: memory.id } } : {}),
});

const requireUser = (c: { req: { header: (name: string) => string | undefined } }) =>
  c.req.header("x-cpki-user-id");

export function createIntelligenceRestRoutes(
  deps: IntelligenceRestDependencies,
): Hono {
  const { store, apiKey } = deps;
  const app = new Hono();

  app.use("*", async (c, next) => {
    const header = c.req.header("authorization") ?? "";
    if (header !== `Bearer ${apiKey}`) {
      return c.json({ error: { code: "UNAUTHORIZED", retryable: false } }, 401);
    }
    await next();
  });

  // ---- threads ------------------------------------------------------------

  app.get("/api/threads", async (c) => {
    const userId = c.req.query("userId");
    const agentId = c.req.query("agentId");
    if (!userId || !agentId)
      return c.json({ error: { code: "BAD_REQUEST", retryable: false } }, 400);
    const threads = await store.listThreads({
      userId,
      agentId,
      includeArchived: c.req.query("includeArchived") === "true",
      limit: c.req.query("limit") ? Number(c.req.query("limit")) : undefined,
    });
    // The `user_meta` join credentials for this user's thread-list channel.
    const joinToken = await store.mintToken("user_threads", userId);
    return c.json({ threads, joinCode: joinToken, joinToken, nextCursor: null });
  });

  app.post("/api/threads", async (c) => {
    const body = await c.req.json<{
      threadId: string;
      userId: string;
      agentId: string;
      name?: string;
    }>();
    const existing = await store.getThread(body.threadId);
    if (existing)
      return c.json({ error: { code: "CONFLICT", retryable: false } }, 409);
    const row = await store.createThread(body);
    const thread = row ? summary(row) : null;
    if (!thread)
      return c.json({ error: { code: "CREATE_FAILED", retryable: true } }, 500);
    deps.meta?.threads(body.userId, threadDelta("created", thread as never));
    return c.json({ thread });
  });

  app.get("/api/threads/:id", async (c) => {
    const row = await store.getThread(c.req.param("id"));
    if (!row) return c.json({ error: { code: "NOT_FOUND", retryable: false } }, 404);
    return c.json({ thread: summary(row) });
  });

  app.patch("/api/threads/:id", async (c) => {
    const body = await c.req.json<{ name?: string | null; archived?: boolean }>();
    const row = await store.updateThread(c.req.param("id"), body);
    if (!row) return c.json({ error: { code: "NOT_FOUND", retryable: false } }, 404);
    const thread = summary(row);
    deps.meta?.threads(
      row.userId,
      threadDelta(body.archived ? "archived" : "updated", thread as never),
    );
    return c.json({ thread });
  });

  app.delete("/api/threads/:id", async (c) => {
    const body = await c.req
      .json<{ userId?: string; agentId?: string }>()
      .catch(() => ({}) as { userId?: string; agentId?: string });
    const existing = await store.getThread(c.req.param("id"));
    await store.deleteThread(c.req.param("id"));
    if (existing)
      deps.meta?.threads(
        existing.userId,
        threadDelta(
          "deleted",
          summary(existing) as unknown as Record<string, unknown>,
        ),
      );
    return c.json({ ok: true });
  });

  app.get("/api/threads/:id/messages", async (c) => {
    const messages = await store.getMessages(c.req.param("id"));
    if (c.req.query("fields") === "id")
      return c.json({ messages: messages.map((m) => ({ id: m.id })) });
    return c.json({ messages });
  });

  // ---- locks, join codes, connect ------------------------------------------

  app.post("/api/threads/:id/lock", async (c) => {
    const body = await c.req.json<{
      runId: string;
      userId: string;
      agentId: string;
      ttlSeconds?: number;
    }>();
    const result = await store.acquireLock({
      threadId: c.req.param("id"),
      runId: body.runId,
      userId: body.userId,
      agentId: body.agentId,
      ttlSeconds: body.ttlSeconds,
    });
    if (!result.acquired)
      return c.json(
        { error: { code: "THREAD_LOCKED", retryable: false }, runId: result.runId },
        409,
      );
    return c.json({
      threadId: c.req.param("id"),
      runId: result.runId,
      joinToken: result.joinToken,
      ttlSeconds: body.ttlSeconds ?? 90,
    });
  });

  app.patch("/api/threads/:id/lock", async (c) => {
    const body = await c.req.json<{ runId: string; ttlSeconds: number }>();
    const { renewed } = await store.renewLock(
      c.req.param("id"),
      body.runId,
      body.ttlSeconds,
    );
    if (!renewed) {
      // The run already reached a terminal event: "completed" is the platform's answer.
      return c.json({ ttlSeconds: 0, status: "completed" });
    }
    return c.json({ ttlSeconds: body.ttlSeconds, status: "renewed" });
  });

  app.delete("/api/threads/:id/lock", async (c) => {
    const body = await c.req
      .json<{ runId: string }>()
      .catch(() => ({ runId: "" }));
    await store.releaseLock(c.req.param("id"), body.runId);
    return c.json({ ok: true });
  });

  app.get("/api/threads/:id/join-code", async (c) => {
    const threadId = c.req.param("id");
    const joinToken = await store.mintToken("thread", threadId);
    const lock = await store.getLock(threadId);
    const running = lock && lock.expiresAt.getTime() > Date.now();
    return c.json({
      threadId,
      joinToken,
      ...(running ? { runId: lock.runId } : {}),
    });
  });

  app.post("/api/threads/:id/connect", async (c) => {
    const threadId = c.req.param("id");
    const body = await c.req.json<{ userId: string; agentId: string }>().catch(
      () => ({ userId: "", agentId: "" }) as { userId: string; agentId: string },
    );
    const existing = await store.getThread(threadId);
    if (!existing) await store.createThread({ threadId, ...body });
    const lock = await store.getLock(threadId);
    const running = lock && lock.expiresAt.getTime() > Date.now();
    if (!running) return c.body(null, 204);
    const joinToken = await store.mintToken("thread", threadId);
    return c.json({ threadId, joinToken, runId: lock.runId });
  });

  app.post("/api/threads/subscribe", async (c) => {
    const body = await c.req.json<{ userId: string }>();
    const joinToken = await store.mintToken("user_threads", body.userId);
    return c.json({ joinToken });
  });

  // ---- memories -------------------------------------------------------------

  app.get("/api/memories", async (c) => {
    const userId = requireUser(c);
    if (!userId)
      return c.json({ error: { code: "BAD_REQUEST", retryable: false } }, 400);
    const rows = await store.listMemories(
      userId,
      c.req.query("includeInvalidated") === "true",
    );
    return c.json({ memories: rows.map((r) => memory(r)) });
  });

  app.post("/api/memories", async (c) => {
    const userId = requireUser(c);
    if (!userId)
      return c.json({ error: { code: "BAD_REQUEST", retryable: false } }, 400);
    const body = await c.req.json<{
      content: string;
      kind: string;
      scope?: string;
      sourceThreadIds?: string[];
    }>();
    const { row, absorbed } = await store.createMemory({
      userId,
      content: body.content,
      kind: body.kind,
      scope: body.scope ?? "user",
      sourceThreadIds: body.sourceThreadIds ?? [],
    });
    deps.meta?.memories(userId, memoryDelta("upserted", memory(row) as never));
    return c.json({ ...memory(row), absorbed });
  });

  app.patch("/api/memories/:id", async (c) => {
    const userId = requireUser(c);
    if (!userId)
      return c.json({ error: { code: "BAD_REQUEST", retryable: false } }, 400);
    const body = await c.req.json<{
      content: string;
      kind: string;
      scope?: string;
      sourceThreadIds?: string[];
    }>();
    const result = await store.supersedeMemory({
      id: c.req.param("id"),
      userId,
      content: body.content,
      kind: body.kind,
      scope: body.scope ?? "user",
      sourceThreadIds: body.sourceThreadIds ?? [],
    });
    if (!result)
      return c.json({ error: { code: "NOT_FOUND", retryable: false } }, 404);
    deps.meta?.memories(
      userId,
      memoryDelta("upserted", memory(result.row) as never),
    );
    return c.json({ ...memory(result.row), retiredId: result.retiredId });
  });

  app.delete("/api/memories/:id", async (c) => {
    const userId = requireUser(c);
    if (!userId)
      return c.json({ error: { code: "BAD_REQUEST", retryable: false } }, 400);
    const row = await store.retireMemory(c.req.param("id"), userId);
    if (!row)
      return c.json({ error: { code: "NOT_FOUND", retryable: false } }, 404);
    deps.meta?.memories(
      userId,
      memoryDelta("invalidated", memory(row) as never),
    );
    return c.json({ ok: true });
  });

  app.post("/api/memories/recall", async (c) => {
    const userId = requireUser(c);
    if (!userId)
      return c.json({ error: { code: "BAD_REQUEST", retryable: false } }, 400);
    const body = await c.req.json<{ query: string; limit?: number; scope?: string }>();
    const rows = await store.recallMemories(userId, body.query, body.limit ?? 10);
    return c.json({
      memories: rows.map(({ row, score }) => memory(row, Number(score))),
    });
  });

  app.post("/api/memories/subscribe", async (c) => {
    const userId = requireUser(c);
    if (!userId)
      return c.json({ error: { code: "BAD_REQUEST", retryable: false } }, 400);
    const joinToken = await store.mintToken("user_memories", userId);
    return c.json({ joinToken, joinCode: joinToken });
  });

  // ---- entitlements, inspector, annotate -------------------------------------

  app.get("/api/entitlements/runtime", (c) =>
    c.json({
      status: "ready",
      entitlement: {
        active: true,
        source: "selfHostedDeploymentLicense",
        features: { memory: true, threads: true, learning: true },
        limits: {},
      },
    }),
  );

  app.get("/api/inspector/metadata", (c) => c.body(null, 204));

  app.get("/api/_inspect/threads/:id/events", async (c) => {
    const rows = await store.listEvents(c.req.param("id"));
    return c.json({
      events: rows.map((row) => row.event),
      decodeErrorRowIds: [],
      truncated: false,
    });
  });

  app.get("/api/_inspect/threads/:id/state", async (c) => {
    const rows = await store.listEvents(c.req.param("id"));
    const state = foldState(rows.map((row) => row.event));
    return c.json(state ?? { kind: "no-snapshot" });
  });

  app.put("/connector/annotate/:clientEventId", async (c) => {
    // Idempotent on the client event id, which is all the annotation loop needs.
    return c.json({ id: c.req.param("clientEventId"), duplicate: false });
  });

  // ---- trajectory capture (Phase 2) ---------------------------------------

  /** Mint a Trajectory capture join; the browser streams on `trajectory:{id}`. */
  app.post("/api/trajectories/join", async (c) => {
    const body = await c.req.json<{
      trajectoryId: string;
      appUserId?: string;
    }>();
    const joinToken = await store.joinTrajectory(body.trajectoryId);
    return c.json({ joinToken, trajectoryId: body.trajectoryId });
  });

  // ---- managed-channel files (Phase 2) -------------------------------------

  /** One stored asset, for history hydration (`ɵgetManagedChannelAsset`). */
  app.get("/api/channels/files/:assetId", async (c) => {
    const file = await store.getChannelFile(c.req.param("assetId"));
    if (!file) return c.body(null, 404);
    return new Response(new Uint8Array(file.bytes), {
      headers: { "content-type": file.mimeType },
    });
  });

  /** A delivery's outbound file, stored for the provider relay to pick up. */
  app.post("/api/channels/deliveries/:deliveryId/files", async (c) => {
    const filename = c.req.query("filename");
    const handle = await store.saveChannelFile({
      deliveryId: c.req.param("deliveryId"),
      operationId: c.req.query("operationId") ?? undefined,
      filename: filename ?? undefined,
      title: c.req.query("title") ?? undefined,
      altText: c.req.query("altText") ?? undefined,
      mimeType: c.req.header("content-type") ?? undefined,
      bytes: Buffer.from(await c.req.arrayBuffer()),
    });
    return c.json({ handle });
  });

  // ---- learning (Phase 3: containers from the table, runs/candidates pages) --

  app.get("/api/v1/learning/containers", async (c) => {
    const rows = await store.listContainers();
    return c.json({
      containers: rows.map((row) => ({
        id: row.id,
        name: row.name,
        createdAt: row.createdAt.toISOString(),
      })),
      nextCursor: null,
    });
  });

  app.get("/api/v1/learning/containers/:id/runs", (c) =>
    c.json({ runs: [] }),
  );

  app.get("/api/v1/learning/containers/:id/candidates", (c) =>
    c.json({ candidates: [] }),
  );

  return app;
}

const summary = toThreadSummary;

const memory = toMemorySummary;
