import { createHash, randomUUID, timingSafeEqual } from "node:crypto";
import { Hono } from "hono";
import { foldState } from "./state";
import {
  decodeThreadCursor,
  type IntelligenceStore,
  toMemorySummary,
  toThreadSummary,
} from "./store";

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
  /** The project service token, accepted only where a Learning container is created. */
  serviceToken?: string;
  /** Fan-out to the user's live meta channels (thread list, memories). */
  meta?: {
    threads: (userId: string, payload: Record<string, unknown>) => void;
    memories: (userId: string, payload: Record<string, unknown>) => void;
  };
};

/**
 * A body field the platform promises, or the request is a 400.
 *
 * Every route here is called by the runtime, which sends complete bodies — but a
 * missing field reached Postgres and came back as a 500 with a driver stack, which
 * reads as a broken server rather than a broken caller.
 */
const text = (value: unknown): string | undefined =>
  typeof value === "string" && value.trim() ? value.trim() : undefined;

const badRequest = (c: { json: (body: unknown, status: 400) => Response }) =>
  c.json({ error: { code: "BAD_REQUEST", retryable: false } }, 400);

/** A handle minted by `gen_random_uuid()`; anything else cannot name a stored file. */
const isUuid = (value: string) =>
  /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(value);

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

const requireUser = (c: {
  req: { header: (name: string) => string | undefined };
}) => c.req.header("x-cpki-user-id");

export function createIntelligenceRestRoutes(
  deps: IntelligenceRestDependencies,
): Hono {
  const { store, apiKey } = deps;
  const app = new Hono();

  app.use("*", async (c, next) => {
    const header = c.req.header("authorization") ?? "";
    if (secretsMatch(header, `Bearer ${apiKey}`)) return next();
    // The project service token has its own header because the Learning setup
    // job has no platform key. It is accepted for exactly one route — creating
    // a Learning container — and is a credential, not a bypass: the header's
    // presence alone authorizes nothing.
    if (
      deps.serviceToken &&
      c.req.method === "POST" &&
      /^\/api\/learning\/projects\/[^/]+\/containers$/.test(c.req.path) &&
      secretsMatch(
        c.req.header("x-copilotkit-ei-service-token") ?? "",
        deps.serviceToken,
      )
    ) {
      return next();
    }
    return c.json({ error: { code: "UNAUTHORIZED", retryable: false } }, 401);
  });

  // ---- threads ------------------------------------------------------------

  app.get("/api/threads", async (c) => {
    const userId = c.req.query("userId");
    if (!userId)
      return c.json({ error: { code: "BAD_REQUEST", retryable: false } }, 400);
    const rawCursor = c.req.query("cursor");
    const cursor = rawCursor ? decodeThreadCursor(rawCursor) : undefined;
    if (rawCursor && !cursor)
      return c.json({ error: { code: "BAD_REQUEST", retryable: false } }, 400);
    const page = await store.listThreads({
      userId,
      agentId: c.req.query("agentId") || undefined,
      includeArchived: c.req.query("includeArchived") === "true",
      limit: c.req.query("limit") ? Number(c.req.query("limit")) : undefined,
      cursor: cursor ?? undefined,
    });
    // The stable `user_meta` channel suffix; the join token for it comes from
    // `/threads/subscribe`, as it does on the hosted platform.
    const joinCode = await store.ensureJoinCode(userId);
    return c.json({
      threads: page.threads,
      joinCode,
      nextCursor: page.nextCursor,
    });
  });

  app.post("/api/threads", async (c) => {
    const body = await c.req
      .json<{
        threadId: string;
        userId: string;
        agentId: string;
        name?: string;
        learningContainerId?: string;
      }>()
      .catch(() => null);
    const threadId = text(body?.threadId);
    const userId = text(body?.userId);
    const agentId = text(body?.agentId);
    if (!body || !threadId || !userId || !agentId) return badRequest(c);
    const existing = await store.getThread(threadId);
    if (existing)
      return c.json({ error: { code: "CONFLICT", retryable: false } }, 409);
    if (
      body.learningContainerId &&
      !(await store.containerExists(body.learningContainerId))
    ) {
      return c.json(
        { error: { code: "LEARNING_CONTAINER_NOT_FOUND", retryable: false } },
        404,
      );
    }
    const row = await store.createThread({
      ...body,
      threadId,
      userId,
      agentId,
    });
    const thread = row ? summary(row) : null;
    if (!thread)
      return c.json({ error: { code: "CREATE_FAILED", retryable: true } }, 500);
    deps.meta?.threads(userId, threadDelta("created", thread as never));
    return c.json({ thread }, 201);
  });

  app.get("/api/threads/:id", async (c) => {
    const userId = c.req.query("userId");
    if (!userId)
      return c.json({ error: { code: "BAD_REQUEST", retryable: false } }, 400);
    const row = await store.getThread(c.req.param("id"));
    // Another person's thread does not exist for this caller.
    if (!row || row.userId !== userId) return notFound(c);
    return c.json({ thread: summary(row) });
  });

  app.patch("/api/threads/:id", async (c) => {
    const body = await c.req.json<{
      userId?: string;
      agentId?: string;
      name?: string | null;
      archived?: boolean;
    }>();
    if (!body.userId)
      return c.json({ error: { code: "BAD_REQUEST", retryable: false } }, 400);
    const existing = await store.getThread(c.req.param("id"));
    if (!existing || existing.userId !== body.userId) return notFound(c);
    const row = await store.updateThread(c.req.param("id"), body);
    if (!row) return notFound(c);
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
    if (!body.userId)
      return c.json({ error: { code: "BAD_REQUEST", retryable: false } }, 400);
    const existing = await store.getThread(c.req.param("id"));
    if (!existing || existing.userId !== body.userId) return notFound(c);
    const deleted = await store.deleteThread(c.req.param("id"));
    if (!deleted) return notFound(c);
    deps.meta?.threads(
      existing.userId,
      threadDelta(
        "deleted",
        summary(existing) as unknown as Record<string, unknown>,
      ),
    );
    return c.json({ deleted: { id: existing.id } });
  });

  app.get("/api/threads/:id/messages", async (c) => {
    const userId = c.req.query("userId");
    if (!userId)
      return c.json({ error: { code: "BAD_REQUEST", retryable: false } }, 400);
    const row = await store.getThread(c.req.param("id"));
    if (!row || row.userId !== userId) return notFound(c);
    const messages = await store.getMessages(c.req.param("id"));
    if (c.req.query("fields") === "id")
      return c.json({ messages: messages.map((m) => ({ id: m.id })) });
    return c.json({ messages });
  });

  // ---- locks, join codes, connect ------------------------------------------

  app.post("/api/threads/:id/lock", async (c) => {
    const threadId = c.req.param("id");
    const body = await c.req.json<{
      runId: string;
      userId: string;
      agentId: string;
      learningContainerId?: string;
    }>();
    if (!body.runId || !body.userId || !body.agentId)
      return c.json({ error: { code: "BAD_REQUEST", retryable: false } }, 400);
    if (
      body.learningContainerId &&
      !(await store.containerExists(body.learningContainerId))
    ) {
      return c.json(
        { error: { code: "LEARNING_CONTAINER_NOT_FOUND", retryable: false } },
        404,
      );
    }
    // Hosted lock and connect create the thread for the caller on first use,
    // and refuse a thread somebody else owns.
    const thread = await store.ensureThreadForUser({
      threadId,
      userId: body.userId,
      agentId: body.agentId,
    });
    if (!thread) return notFound(c);
    if (body.learningContainerId) {
      await store.assignLearningContainer(threadId, body.learningContainerId);
    }
    const result = await store.acquireLock({
      threadId,
      runId: body.runId,
      userId: body.userId,
      agentId: body.agentId,
    });
    if (!result.acquired)
      return c.json(
        {
          error: { code: "THREAD_LOCK_FAILED", retryable: true },
          runId: result.runId,
        },
        409,
      );
    return c.json({
      threadId,
      runId: result.runId,
      joinToken: result.joinToken,
      ttlSeconds: result.ttlSeconds,
    });
  });

  app.patch("/api/threads/:id/lock", async (c) => {
    const body = await c.req.json<{ runId: string; ttlSeconds?: number }>();
    if (!body.runId)
      return c.json({ error: { code: "BAD_REQUEST", retryable: false } }, 400);
    const result = await store.renewLock(c.req.param("id"), body.runId);
    if (result.status === "conflict") {
      // Another run holds the thread now. The runner must abort, not carry on
      // without the lock, so this is a denial rather than a normal finish.
      return c.json(
        { error: { code: "THREAD_LOCK_RENEWAL_FAILED", retryable: false } },
        409,
      );
    }
    if (result.status === "completed") {
      return c.json({ ttlSeconds: 0, status: "completed" });
    }
    return c.json({ ttlSeconds: result.ttlSeconds, status: "renewed" });
  });

  app.delete("/api/threads/:id/lock", async (c) => {
    const body = await c.req
      .json<{ runId: string }>()
      .catch(() => ({ runId: "" }));
    const released = await store.releaseLock(c.req.param("id"), body.runId);
    return c.json({ threadId: c.req.param("id"), runId: body.runId, released });
  });

  app.get("/api/threads/:id/join-code", async (c) => {
    const threadId = c.req.param("id");
    const userId = c.req.query("userId");
    if (!userId)
      return c.json({ error: { code: "BAD_REQUEST", retryable: false } }, 400);
    const thread = await store.getThread(threadId);
    if (!thread || thread.userId !== userId) return notFound(c);
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
    const body = await c.req
      .json<{ userId: string; agentId: string }>()
      .catch(
        () =>
          ({ userId: "", agentId: "" }) as { userId: string; agentId: string },
      );
    if (!body.userId || !body.agentId)
      return c.json({ error: { code: "BAD_REQUEST", retryable: false } }, 400);
    const thread = await store.ensureThreadForUser({
      threadId,
      userId: body.userId,
      agentId: body.agentId,
    });
    if (!thread) return notFound(c);
    const lock = await store.getLock(threadId);
    const running = lock && lock.expiresAt.getTime() > Date.now();
    // Hosted answers with a join grant even when nothing is running; a 204 is
    // reserved for a caller that asked to connect to something absent.
    const joinToken = await store.mintToken("thread", threadId);
    return c.json({
      threadId,
      joinToken,
      ...(running ? { runId: lock.runId } : {}),
    });
  });

  app.post("/api/threads/subscribe", async (c) => {
    const body = await c.req.json<{ userId?: string }>().catch(() => null);
    const userId = text(body?.userId);
    if (!userId) return badRequest(c);
    const joinToken = await store.mintToken("user_threads", userId);
    return c.json({ joinToken });
  });

  // ---- memories -------------------------------------------------------------

  app.get("/api/memories", async (c) => {
    const userId = requireUser(c);
    if (!userId)
      return c.json({ error: { code: "BAD_REQUEST", retryable: false } }, 400);
    const grant = memoryGrant(c);
    const scope = scopeForGrant(grant);
    if (scope === "none") return memoryDenied(c);
    const rows = await store.listMemories(
      userId,
      c.req.query("includeInvalidated") === "true",
      scope,
    );
    return c.json({ memories: rows.map((r) => memory(r)) });
  });

  app.post("/api/memories", async (c) => {
    const userId = requireUser(c);
    if (!userId)
      return c.json({ error: { code: "BAD_REQUEST", retryable: false } }, 400);
    const body = await c.req
      .json<{
        content: string;
        kind: string;
        scope?: string;
        sourceThreadIds?: string[];
      }>()
      .catch(() => null);
    const content = text(body?.content);
    if (!body || !content) return badRequest(c);
    const scope = body.scope ?? "user";
    if (!grantAllows(memoryGrant(c), scope)) return memoryDenied(c);
    const { row, absorbed } = await store.createMemory({
      userId,
      content,
      kind: body.kind,
      scope,
      sourceThreadIds: body.sourceThreadIds ?? [],
    });
    deps.meta?.memories(userId, memoryDelta("upserted", memory(row) as never));
    return c.json({ ...memory(row), absorbed });
  });

  app.patch("/api/memories/:id", async (c) => {
    const userId = requireUser(c);
    if (!userId)
      return c.json({ error: { code: "BAD_REQUEST", retryable: false } }, 400);
    const body = await c.req
      .json<{
        content: string;
        kind: string;
        scope?: string;
        sourceThreadIds?: string[];
      }>()
      .catch(() => null);
    if (!body || !text(body.content)) return badRequest(c);
    const current = await store.getMemory(c.req.param("id"), userId);
    if (!current) return notFound(c);
    const grant = memoryGrant(c);
    if (!grantAllows(grant, body.scope ?? current.scope))
      return memoryDenied(c);
    const result = await store.supersedeMemory({
      id: c.req.param("id"),
      userId,
      content: body.content,
      kind: body.kind,
      scope: body.scope,
      sourceThreadIds: body.sourceThreadIds,
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
    const current = await store.getMemory(c.req.param("id"), userId);
    if (!current) return notFound(c);
    if (!grantAllows(memoryGrant(c), current.scope)) return memoryDenied(c);
    const row = await store.retireMemory(c.req.param("id"), userId);
    if (!row) return notFound(c);
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
    const body = await c.req.json<{
      query: string;
      limit?: number;
      scope?: string;
    }>();
    const grant = memoryGrant(c);
    const scope =
      body.scope ??
      (grant && !grantAllows(grant, "project") ? "user" : undefined);
    if (scope && !grantAllows(grant, scope)) return memoryDenied(c);
    const rows = await store.recallMemories(
      userId,
      body.query,
      body.limit ?? 10,
      scope,
    );
    return c.json({
      memories: rows.map(({ row, score }) => memory(row, Number(score))),
    });
  });

  app.post("/api/memories/subscribe", async (c) => {
    const userId = requireUser(c);
    if (!userId)
      return c.json({ error: { code: "BAD_REQUEST", retryable: false } }, 400);
    const joinCode = await store.ensureJoinCode(userId);
    const joinToken = await store.mintToken("user_memories", userId);
    return c.json({ joinToken, joinCode });
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

  /**
   * The Inspector's Learning panel reads a versioned snapshot, not a 404. This
   * deployment runs no Learning engine, so the snapshot is empty; its links use
   * the request's own origin when that origin is a safe dashboard transport.
   */
  app.get("/api/inspector/learning", (c) => {
    const url = new URL(c.req.url);
    const local =
      url.hostname === "localhost" ||
      url.hostname === "127.0.0.1" ||
      url.hostname === "[::1]";
    if (url.protocol !== "https:" && !(url.protocol === "http:" && local)) {
      return c.json({ error: { code: "NOT_FOUND", retryable: false } }, 404);
    }
    const origin = url.origin;
    return c.json({
      schemaVersion: 1,
      projectKey: "local",
      snapshotVersion: randomUUID(),
      webAppOrigin: origin,
      configuration: { state: "not_configured" },
      pendingThreadCount: 0,
      run: { hasActiveRun: false, hasEverSucceeded: false, latest: null },
      pendingCandidateCount: 0,
      skillsPage: { page: 1, pageSize: 3, total: 0, totalPages: 0, items: [] },
      insightsPage: {
        page: 1,
        pageSize: 4,
        total: 0,
        totalPages: 0,
        items: [],
      },
      links: {
        learning: `${origin}/o/local/local/learning`,
        candidates: null,
        runs: null,
      },
    });
  });

  app.get("/api/_inspect/threads/:id/events", async (c) => {
    const page = await store.listEventsPage(c.req.param("id"));
    return c.json({
      events: page.events,
      decodeErrorRowIds: [],
      truncated: page.truncated,
    });
  });

  app.get("/api/_inspect/threads/:id/state", async (c) => {
    const events = await store.listStateEvents(c.req.param("id"));
    return c.json(foldState(events));
  });

  /**
   * The annotation loop's arrival point, answered without recording anything.
   *
   * This deployment keeps no annotation analytics, so there is no row to be a duplicate
   * of and `duplicate` is always false. The route exists because the client posts here
   * on every annotation and treats a non-null body as acceptance; a 404 would make it
   * retry forever.
   */
  app.put("/connector/annotate/:clientEventId", (c) =>
    c.json({ id: c.req.param("clientEventId"), duplicate: false }),
  );

  // ---- trajectory capture (Phase 2) ---------------------------------------

  /** Mint a Trajectory capture join; the browser streams on `trajectory:{id}`. */
  app.post("/api/trajectories/join", async (c) => {
    const body = await c.req
      .json<{ trajectoryId?: string; appUserId?: string }>()
      .catch(() => null);
    const trajectoryId = text(body?.trajectoryId);
    if (!trajectoryId) return badRequest(c);
    const joinToken = await store.joinTrajectory(trajectoryId);
    return c.json({ joinToken, trajectoryId });
  });

  // ---- managed-channel files (Phase 2) -------------------------------------

  /** One stored asset, for history hydration (`ɵgetManagedChannelAsset`). */
  app.get("/api/channels/files/:assetId", async (c) => {
    const handle = c.req.param("assetId");
    // The column is a uuid, so a malformed id has to be answered here rather than
    // sent to Postgres, where it came back as an unhandled 500 instead of "absent".
    if (!isUuid(handle)) return c.body(null, 404);
    const file = await store.getChannelFile(handle);
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

  // ---- learning (Phase 3: containers, skill-ZIP snapshots) ----------------

  const containerSummary = (row: {
    id: string;
    name: string;
    projectId: number;
    promptContext: string | null;
    createdAt: Date;
    updatedAt: Date;
  }) => ({
    id: row.id,
    name: row.name,
    projectId: row.projectId,
    promptContext: row.promptContext,
    createdAt: row.createdAt.toISOString(),
    updatedAt: row.updatedAt.toISOString(),
  });

  app.get("/api/v1/learning/containers", async (c) => {
    const rows = await store.listContainers();
    return c.json({
      containers: rows.map(containerSummary),
      nextCursor: null,
    });
  });

  app.get("/api/v1/learning/containers/:id", async (c) => {
    const rows = await store.listContainers();
    const row = rows.find((container) => container.id === c.req.param("id"));
    if (!row) return c.json({ error: { code: "CONTAINER_NOT_FOUND" } }, 404);
    return c.json({ container: containerSummary(row) });
  });

  /**
   * Several Skill snapshots in one authorized request, as the runtime's
   * multi-container registry asks for them. Each entry is a snapshot, an
   * unchanged answer for a matching If-None-Match, or a per-container error.
   */
  app.post("/api/v1/learning/skills/batch", async (c) => {
    const body = await c.req.json<{
      containers?: {
        containerId?: string;
        revision?: string;
        ifNoneMatch?: string;
      }[];
    }>();
    const requests = Array.isArray(body.containers)
      ? body.containers.slice(0, 50)
      : [];
    const containers = [];
    for (const request of requests) {
      const containerId = request?.containerId;
      if (!containerId || !(await store.containerExists(containerId))) {
        containers.push({
          containerId: containerId ?? "",
          status: "error",
          error: { code: "CONTAINER_NOT_FOUND", retryable: false },
        });
        continue;
      }
      const found = await store.skillRevision(containerId, request.revision);
      if (!found) {
        containers.push({
          containerId,
          status: "error",
          error: { code: "REVISION_NOT_FOUND", retryable: false },
        });
        continue;
      }
      const etag = `"${createHash("sha256").update(found.archive).digest("hex")}"`;
      if (request.ifNoneMatch === etag) {
        containers.push({
          containerId,
          status: "unchanged",
          revision: found.revision,
          etag,
        });
        continue;
      }
      containers.push({
        containerId,
        status: "snapshot",
        revision: found.revision,
        etag,
        contentType: "application/zip",
        bytesBase64: found.archive.toString("base64"),
      });
    }
    return c.json({ containers });
  });

  /** A project's service token creates its Learning container (idempotent). */
  app.post("/api/learning/projects/:projectId/containers", async (c) => {
    const body = await c.req.json<{ id: string; name: string }>();
    const projectId = Number(c.req.param("projectId"));
    const outcome = await store.createContainer({
      id: body.id,
      name: body.name,
      projectId: Number.isInteger(projectId) ? projectId : 1,
    });
    if (outcome === "present")
      return c.json(
        { error: { code: "LEARNING_CONTAINER_ALREADY_EXISTS" } },
        409,
      );
    return c.json(
      {
        container: {
          id: body.id,
          projectId: Number.isInteger(projectId) ? projectId : 1,
        },
      },
      201,
    );
  });

  /**
   * One Skill snapshot as the runtime's registry reads it: the ZIP, its
   * content hash as the revision, and the revision's digest as the ETag
   * (304 on a matching If-None-Match).
   */
  app.get("/api/v1/learning/containers/:id/skills", async (c) => {
    const containerId = c.req.param("id");
    const revision = c.req.query("revision");
    if (!(await store.containerExists(containerId)))
      return c.json({ error: { code: "CONTAINER_NOT_FOUND" } }, 404);
    const found = await store.skillRevision(containerId, revision);
    if (!found) return c.json({ error: { code: "REVISION_NOT_FOUND" } }, 403);
    const etag = `"${createHash("sha256").update(found.archive).digest("hex")}"`;
    if (c.req.header("if-none-match") === etag)
      return new Response(null, {
        status: 304,
        headers: { etag, "x-copilotkit-skills-revision": found.revision },
      });
    return new Response(new Uint8Array(found.archive), {
      headers: {
        "content-type": "application/zip",
        etag,
        "x-copilotkit-skills-revision": found.revision,
      },
    });
  });

  /** Publish one Skill revision: the ZIP, stored under its content hash. */
  app.put("/api/v1/learning/containers/:id/skills", async (c) => {
    const revision = await store.publishSkillRevision({
      containerId: c.req.param("id"),
      archive: Buffer.from(await c.req.arrayBuffer()),
    });
    // Publishing into a container nobody created used to store an archive the read
    // route then refused, so the revision was silently unreachable.
    if (!revision)
      return c.json({ error: { code: "CONTAINER_NOT_FOUND" } }, 404);
    return c.json({ revision });
  });

  app.post("/api/v1/learning/containers/:id/runs", (c) =>
    c.json(
      { error: { code: "LEARNING_ENGINE_UNAVAILABLE", retryable: false } },
      501,
    ),
  );

  app.get("/api/v1/learning/containers/:id/runs", (c) => c.json({ runs: [] }));

  app.get("/api/v1/learning/containers/:id/candidates", (c) =>
    c.json({ candidates: [] }),
  );

  return app;
}

const summary = toThreadSummary;

const memory = toMemorySummary;

/** Another person's thread (or a thread that is not there) is the same 404. */
function notFound(c: { json: (body: unknown, status: 404) => Response }) {
  return c.json({ error: { code: "NOT_FOUND", retryable: false } }, 404);
}

type MemoryGrant = { user?: string; project?: string };

function memoryGrant(c: {
  req: { header: (name: string) => string | undefined };
}): MemoryGrant | undefined {
  const raw = c.req.header("x-cpki-memory-grant");
  if (!raw) return undefined;
  try {
    const parsed = JSON.parse(raw) as { user?: unknown; project?: unknown };
    return {
      user: typeof parsed.user === "string" ? parsed.user : undefined,
      project: typeof parsed.project === "string" ? parsed.project : undefined,
    };
  } catch {
    return undefined;
  }
}

/** A scope is allowed when the immutable grant names read or read-write for it. */
function grantAllows(grant: MemoryGrant | undefined, scope: string): boolean {
  if (!grant) return true;
  const value = scope === "project" ? grant.project : grant.user;
  return value === "read" || value === "read-write";
}

/** The one scope a list may read when the grant allows only one; "none" for neither. */
function scopeForGrant(grant: MemoryGrant | undefined): string | undefined {
  if (!grant) return undefined;
  const user = grantAllows(grant, "user");
  const project = grantAllows(grant, "project");
  if (user && !project) return "user";
  if (!user && project) return "project";
  if (!user && !project) return "none";
  return undefined;
}

function memoryDenied(c: { json: (body: unknown, status: 403) => Response }) {
  return c.json(
    { error: { code: "MEMORY_NOT_ENTITLED", retryable: false } },
    403,
  );
}

/** Compare two secrets without an early exit that leaks their common prefix. */
function secretsMatch(presented: string, expected: string): boolean {
  const left = Buffer.from(presented);
  const right = Buffer.from(expected);
  return left.length === right.length && timingSafeEqual(left, right);
}
