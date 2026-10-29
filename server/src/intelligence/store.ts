import { randomUUID } from "node:crypto";
import { and, asc, desc, eq, gt, isNull, or, sql } from "drizzle-orm";
import type { Database } from "../db/client";
import {
  intelligenceJoinTokens,
  intelligenceLearningContainers,
  intelligenceMemories,
  intelligenceThreadEvents,
  intelligenceThreadLocks,
  intelligenceThreads,
} from "../db/schema/intelligence";
import { foldEventsToMessages } from "./messages";

/**
 * The storage behind the local Intelligence platform.
 *
 * Single instance, one Postgres: no cache layer, no fan-out, no sharding — the hosted
 * platform's tables, one row each where a row suffices. Durable state lives here; the
 * realtime gateway's only state is the socket registry on top of these rows.
 */

export type ThreadRow = typeof intelligenceThreads.$inferSelect;
export type MemoryRow = typeof intelligenceMemories.$inferSelect;

/** The platform's `ThreadSummary` projection, from this deployment's row. */
export function toThreadSummary(row: ThreadRow) {
  return {
    id: row.id,
    name: row.name,
    agentId: row.agentId,
    createdById: row.userId,
    archived: row.archived,
    learningContainerIds: row.learningContainerIds,
    createdAt: row.createdAt.toISOString(),
    updatedAt: row.updatedAt.toISOString(),
    ...(row.lastRunAt ? { lastRunAt: row.lastRunAt.toISOString() } : {}),
    ...(row.lastUpdatedAt
      ? { lastUpdatedAt: row.lastUpdatedAt.toISOString() }
      : {}),
  };
}

/** The platform's `MemorySummary` projection. */
export function toMemorySummary(row: MemoryRow, score?: number) {
  return {
    id: row.id,
    kind: row.kind,
    scope: row.scope,
    content: row.content,
    sourceThreadIds: row.sourceThreadIds,
    invalidatedAt: row.invalidatedAt ? row.invalidatedAt.toISOString() : null,
    ...(score === undefined ? {} : { score }),
  };
}

const JOIN_TOKEN_TTL_MS = 60 * 60_000;

export class IntelligenceStore {
  constructor(private readonly db: Database) {}

  // ---- threads ------------------------------------------------------------

  async listThreads(input: {
    userId: string;
    agentId: string;
    includeArchived?: boolean;
    limit?: number;
  }) {
    const conditions = [
      eq(intelligenceThreads.userId, input.userId),
      eq(intelligenceThreads.agentId, input.agentId),
    ];
    if (!input.includeArchived) conditions.push(eq(intelligenceThreads.archived, false));
    const rows = await this.db
      .select()
      .from(intelligenceThreads)
      .where(and(...conditions))
      .orderBy(desc(intelligenceThreads.lastUpdatedAt), desc(intelligenceThreads.createdAt))
      .limit(Math.min(input.limit ?? 200, 500));
    return rows.map(toThreadSummary);
  }

  async getThread(threadId: string) {
    const [row] = await this.db
      .select()
      .from(intelligenceThreads)
      .where(eq(intelligenceThreads.id, threadId))
      .limit(1);
    return row;
  }

  async createThread(input: {
    threadId: string;
    userId: string;
    agentId: string;
    name?: string;
  }) {
    const [row] = await this.db
      .insert(intelligenceThreads)
      .values({
        id: input.threadId,
        userId: input.userId,
        agentId: input.agentId,
        name: input.name ?? null,
      })
      .onConflictDoNothing()
      .returning();
    return row ?? (await this.getThread(input.threadId));
  }

  async updateThread(
    threadId: string,
    updates: { name?: string | null; archived?: boolean },
  ) {
    const [row] = await this.db
      .update(intelligenceThreads)
      .set({
        ...(updates.name !== undefined ? { name: updates.name } : {}),
        ...(updates.archived !== undefined ? { archived: updates.archived } : {}),
        updatedAt: new Date(),
        ...(updates.name !== undefined ? { lastUpdatedAt: new Date() } : {}),
      })
      .where(eq(intelligenceThreads.id, threadId))
      .returning();
    return row;
  }

  async deleteThread(threadId: string) {
    await this.db
      .delete(intelligenceThreads)
      .where(eq(intelligenceThreads.id, threadId));
    await this.db
      .delete(intelligenceThreadEvents)
      .where(eq(intelligenceThreadEvents.threadId, threadId));
  }

  // ---- events and messages ------------------------------------------------

  /**
   * One appended event, ordered by its per-run seq.
   *
   * The runner retries event pushes; insert is idempotent on `(run_id, seq)` so a
   * replayed batch stores each event once and broadcasts them in seq order.
   */
  async appendEvents(
    threadId: string,
    runId: string,
    events: { seq: number; payload: Record<string, unknown> }[],
  ): Promise<{ ids: Map<number, string>; events: Record<string, unknown>[] }> {
    const ids = new Map<number, string>();
    const stored: Record<string, unknown>[] = [];
    for (const event of events.sort((a, b) => a.seq - b.seq)) {
      const [row] = await this.db
        .insert(intelligenceThreadEvents)
        .values({
          threadId,
          runId,
          seq: event.seq,
          event: event.payload as Record<string, unknown>,
        })
        .onConflictDoNothing()
        .returning();
      if (!row) {
        const [existing] = await this.db
          .select()
          .from(intelligenceThreadEvents)
          .where(
            and(
              eq(intelligenceThreadEvents.runId, runId),
              eq(intelligenceThreadEvents.seq, event.seq),
            ),
          )
          .limit(1);
        if (existing) ids.set(event.seq, String(existing.id));
        continue;
      }
      ids.set(event.seq, String(row.id));
      stored.push(event.payload);
    }
    // The fold keeps the thread's transcript current for `GET …/messages`.
    if (stored.length > 0) {
      const thread = await this.getThread(threadId);
      if (thread) {
        const messages = foldEventsToMessages(
          (thread.messages ?? []) as never,
          stored as never,
        );
        await this.db
          .update(intelligenceThreads)
          .set({
            messages: messages as never,
            lastRunAt: new Date(),
            lastUpdatedAt: new Date(),
            updatedAt: new Date(),
          })
          .where(eq(intelligenceThreads.id, threadId));
      }
    }
    return { ids, events: stored };
  }

  /** The thread's event log, in replay order. */
  async listEvents(threadId: string, limit = 1000) {
    const rows = await this.db
      .select()
      .from(intelligenceThreadEvents)
      .where(eq(intelligenceThreadEvents.threadId, threadId))
      .orderBy(asc(intelligenceThreadEvents.id))
      .limit(limit);
    return rows;
  }

  /**
   * The events a reconnecting client has not seen, keyed on the runner's
   * `cpki_event_id` cursor (the durable id the client tracks). A cursor this
   * log has not seen replays from the beginning, which is what a cold browser
   * needs.
   */
  async listEventsAfter(
    threadId: string,
    cpkiEventId: string | null,
    limit = 2000,
  ): Promise<Record<string, unknown>[]> {
    const rows = await this.db
      .select()
      .from(intelligenceThreadEvents)
      .where(eq(intelligenceThreadEvents.threadId, threadId))
      .orderBy(asc(intelligenceThreadEvents.id))
      .limit(limit);
    const eventOf = (row: typeof intelligenceThreadEvents.$inferSelect) =>
      row.event as Record<string, unknown>;
    if (!cpkiEventId) return rows.map(eventOf);
    const index = rows.findIndex(
      (row) =>
        (row.event as { metadata?: { cpki_event_id?: string } }).metadata
          ?.cpki_event_id === cpkiEventId,
    );
    if (index < 0) return rows.map(eventOf);
    return rows.slice(index + 1).map(eventOf);
  }

  /** The newest event of the thread and whether its run is still active. */
  async latestEvent(
    threadId: string,
  ): Promise<{ cpkiEventId: string; running: boolean } | null> {
    const [row] = await this.db
      .select()
      .from(intelligenceThreadEvents)
      .where(eq(intelligenceThreadEvents.threadId, threadId))
      .orderBy(desc(intelligenceThreadEvents.id))
      .limit(1);
    if (!row) return null;
    const lock = await this.getLock(threadId);
    return {
      cpkiEventId:
        (row.event as { metadata?: { cpki_event_id?: string } }).metadata
          ?.cpki_event_id ?? String(row.id),
      running: Boolean(lock && lock.expiresAt.getTime() > Date.now()),
    };
  }

  /** The run reached its terminal event: the thread lock is no longer needed. */
  async markRunFinished(threadId: string, runId: string) {
    await this.releaseLock(threadId, runId);
  }

  async getMessages(threadId: string) {
    const thread = await this.getThread(threadId);
    return (thread?.messages ?? []) as Record<string, unknown>[];
  }

  // ---- locks --------------------------------------------------------------

  /**
   * Take the thread's run lock, or report the run that holds it.
   *
   * A lock is taken by its previous expiry, one compare: a single server cannot fight
   * itself, so no fencing token. A lock whose run never finished expires on its TTL.
   */
  async acquireLock(input: {
    threadId: string;
    runId: string;
    userId: string;
    agentId: string;
    ttlSeconds?: number;
  }): Promise<{ acquired: boolean; runId?: string; joinToken?: string }> {
    const expiresAt = new Date(
      Date.now() + (input.ttlSeconds ?? 90) * 1000,
    );
    const [existing] = await this.db
      .select()
      .from(intelligenceThreadLocks)
      .where(eq(intelligenceThreadLocks.threadId, input.threadId))
      .limit(1);
    if (existing && existing.expiresAt.getTime() > Date.now() && existing.runId !== input.runId) {
      return { acquired: false, runId: existing.runId };
    }
    await this.db
      .insert(intelligenceThreadLocks)
      .values({
        threadId: input.threadId,
        runId: input.runId,
        userId: input.userId,
        agentId: input.agentId,
        expiresAt,
      })
      .onConflictDoUpdate({
        target: intelligenceThreadLocks.threadId,
        set: { runId: input.runId, userId: input.userId, agentId: input.agentId, expiresAt },
      });
    return {
      acquired: true,
      runId: input.runId,
      joinToken: await this.mintToken("thread", input.threadId),
    };
  }

  async renewLock(threadId: string, runId: string, ttlSeconds?: number) {
    const [row] = await this.db
      .update(intelligenceThreadLocks)
      .set({ expiresAt: new Date(Date.now() + (ttlSeconds ?? 90) * 1000) })
      .where(
        and(
          eq(intelligenceThreadLocks.threadId, threadId),
          eq(intelligenceThreadLocks.runId, runId),
        ),
      )
      .returning();
    return { renewed: Boolean(row) };
  }

  async releaseLock(threadId: string, runId: string) {
    await this.db
      .delete(intelligenceThreadLocks)
      .where(
        and(
          eq(intelligenceThreadLocks.threadId, threadId),
          eq(intelligenceThreadLocks.runId, runId),
        ),
      );
  }

  /** The lock held right now, past its TTL or not. */
  async getLock(threadId: string) {
    const [row] = await this.db
      .select()
      .from(intelligenceThreadLocks)
      .where(eq(intelligenceThreadLocks.threadId, threadId))
      .limit(1);
    return row;
  }

  // ---- join tokens --------------------------------------------------------

  /**
   * Subscribe to a thread's events and state: mint the join token the runtime
   * passes to the gateway as the client's subscription credential.
   */
  async connect(threadId: string, runId: string): Promise<string | null> {
    const [row] = await this.db
      .select({ id: intelligenceThreads.id })
      .from(intelligenceThreads)
      .where(eq(intelligenceThreads.id, threadId))
      .limit(1);
    if (!row) return null;
    return this.mintToken("thread", threadId);
  }

  async mintToken(
    kind: "thread" | "user_threads" | "user_memories",
    scope: string,
  ): Promise<string> {
    const token = randomUUID();
    await this.db
      .insert(intelligenceJoinTokens)
      .values({
        token,
        kind,
        threadId: kind === "thread" ? scope : null,
        userId: kind === "thread" ? null : scope,
        expiresAt: new Date(Date.now() + JOIN_TOKEN_TTL_MS),
      })
      .onConflictDoNothing();
    return token;
  }

  /** Whether a join token authorizes a topic. Tokens name their scope, so a token
   * minted for one thread cannot join another's channel, and a thread-list token
   * cannot join the memories channel. */
  async authorizeTopic(
    token: string | null,
    topic: string,
  ): Promise<{ ok: boolean; userId?: string }> {
    if (!token) return { ok: false };
    const [row] = await this.db
      .select()
      .from(intelligenceJoinTokens)
      .where(eq(intelligenceJoinTokens.token, token))
      .limit(1);
    if (!row || row.expiresAt.getTime() < Date.now()) return { ok: false };
    if (row.kind === "thread") {
      return { ok: row.threadId === topic.replace(/^thread:/, "") };
    }
    if (topic.startsWith("user_meta:memories:")) {
      return { ok: row.kind === "user_memories", userId: row.userId ?? undefined };
    }
    if (topic.startsWith("user_meta:")) {
      return { ok: row.kind === "user_threads", userId: row.userId ?? undefined };
    }
    return { ok: true };
  }

  /** Not-yet-expired join tokens of a user's meta channel of a kind. */
  async liveTokens(
    userId: string,
    kind: "user_threads" | "user_memories",
  ): Promise<string[]> {
    const rows = await this.db
      .select({ token: intelligenceJoinTokens.token })
      .from(intelligenceJoinTokens)
      .where(
        and(
          eq(intelligenceJoinTokens.userId, userId),
          eq(intelligenceJoinTokens.kind, kind),
          sql`${intelligenceJoinTokens.expiresAt} > now()`,
        ),
      );
    return rows.map((row) => row.token);
  }

  // ---- memories -----------------------------------------------------------

  async listMemories(userId: string, includeInvalidated = false) {
    const rows = await this.db
      .select()
      .from(intelligenceMemories)
      .where(
        and(
          eq(intelligenceMemories.userId, userId),
          ...(includeInvalidated ? [] : [isNull(intelligenceMemories.invalidatedAt)]),
        ),
      )
      .orderBy(desc(intelligenceMemories.createdAt))
      .limit(200);
    return rows;
  }

  async createMemory(input: {
    userId: string;
    content: string;
    kind: string;
    scope: string;
    sourceThreadIds: string[];
  }) {
    // Near-duplicate absorption: the hosted platform's create can merge into an
    // existing row; exact-content match is the whole contract OpenBot relies on.
    const [existing] = await this.db
      .select()
      .from(intelligenceMemories)
      .where(
        and(
          eq(intelligenceMemories.userId, input.userId),
          eq(intelligenceMemories.content, input.content),
          isNull(intelligenceMemories.invalidatedAt),
        ),
      )
      .limit(1);
    if (existing) {
      return { row: existing, absorbed: true } as const;
    }
    const [row] = await this.db
      .insert(intelligenceMemories)
      .values({
        id: randomUUID(),
        userId: input.userId,
        kind: input.kind,
        scope: input.scope,
        content: input.content,
        sourceThreadIds: input.sourceThreadIds,
      })
      .returning();
    return { row, absorbed: false } as const;
  }

  /** Retire `id` and insert `content` as its successor, atomically. */
  async supersedeMemory(input: {
    id: string;
    userId: string;
    content: string;
    kind: string;
    scope: string;
    sourceThreadIds: string[];
  }) {
    return this.db.transaction(async (tx) => {
      const [retired] = await tx
        .update(intelligenceMemories)
        .set({ invalidatedAt: new Date() })
        .where(
          and(
            eq(intelligenceMemories.id, input.id),
            eq(intelligenceMemories.userId, input.userId),
            isNull(intelligenceMemories.invalidatedAt),
          ),
        )
        .returning();
      if (!retired) return undefined;
      const [created] = await tx
        .insert(intelligenceMemories)
        .values({
          id: randomUUID(),
          userId: input.userId,
          kind: input.kind,
          scope: input.scope,
          content: input.content,
          sourceThreadIds: input.sourceThreadIds,
        })
        .returning();
      return { retiredId: retired.id, row: created };
    });
  }

  async retireMemory(id: string, userId: string) {
    const [row] = await this.db
      .update(intelligenceMemories)
      .set({ invalidatedAt: new Date() })
      .where(
        and(
          eq(intelligenceMemories.id, id),
          eq(intelligenceMemories.userId, userId),
          isNull(intelligenceMemories.invalidatedAt),
        ),
      )
      .returning();
    return row;
  }

  /**
   * Ranked recall over the person's live memories: Postgres full text, ranked and
   * intersected with a trigram-free containment pass so short queries that miss the
   * tsvector config still find their rows.
   */
  async recallMemories(userId: string, query: string, limit: number) {
    const rows = await this.db
      .select({
        row: intelligenceMemories,
        score: sql<number>`ts_rank(to_tsvector('english', ${intelligenceMemories.content}), plainto_tsquery('english', ${query}))`,
      })
      .from(intelligenceMemories)
      .where(
        and(
          eq(intelligenceMemories.userId, userId),
          isNull(intelligenceMemories.invalidatedAt),
          or(
            sql`to_tsvector('english', ${intelligenceMemories.content}) @@ plainto_tsquery('english', ${query})`,
            sql`lower(${intelligenceMemories.content}) LIKE '%' || lower(${query}) || '%'`,
          ),
        ),
      )
      .orderBy(desc(sql`ts_rank(to_tsvector('english', ${intelligenceMemories.content}), plainto_tsquery('english', ${query}))`))
      .limit(limit);
    return rows;
  }

  // ---- learning -----------------------------------------------------------

  async listContainers() {
    return this.db.select().from(intelligenceLearningContainers);
  }
}
