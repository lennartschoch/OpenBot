import { createHash, randomUUID } from "node:crypto";
import { inflateRawSync } from "node:zlib";
import { and, asc, desc, eq, gt, isNull, lt, or, sql } from "drizzle-orm";
import type { Database } from "../db/client";
import {
  intelligenceChannelFiles,
  intelligenceJoinTokens,
  intelligenceLearningContainers,
  intelligenceMemories,
  intelligenceSkillRevisions,
  intelligenceThreadEvents,
  intelligenceThreadLocks,
  intelligenceThreads,
  intelligenceUserJoinCodes,
} from "../db/schema/intelligence";
import { foldEventsToMessages } from "./messages";

/**
 * The revision a skills ZIP declares: `manifest.json` names it (the publisher
 * stamps the archive, the platform stores it under that id, and the runtime's
 * registry checks `manifest.revision` against the revision header). Read
 * straight from the ZIP's local file headers, inflating the manifest entry.
 */
export function readArchiveRevision(archive: Buffer): string | null {
  let offset = 0;
  while (
    offset + 30 <= archive.length &&
    archive.readUInt32LE(offset) === 0x04034b50
  ) {
    const method = archive.readUInt16LE(offset + 8);
    const compressedSize = archive.readUInt32LE(offset + 18);
    const nameLength = archive.readUInt16LE(offset + 26);
    const extraLength = archive.readUInt16LE(offset + 28);
    const name = archive
      .subarray(offset + 30, offset + 30 + nameLength)
      .toString();
    const dataStart = offset + 30 + nameLength + extraLength;
    if (dataStart + compressedSize > archive.length) return null;
    if (name === "manifest.json") {
      const entry = archive.subarray(dataStart, dataStart + compressedSize);
      const bytes = method === 8 ? inflateRawSync(entry) : entry;
      try {
        const manifest = JSON.parse(bytes.toString("utf8")) as {
          revision?: unknown;
        };
        return typeof manifest.revision === "string" ? manifest.revision : null;
      } catch {
        return null;
      }
    }
    offset = dataStart + compressedSize;
  }
  return null;
}

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
  const learningContainerIds = row.learningContainerIds ?? [];
  return {
    id: row.id,
    name: row.name,
    agentId: row.agentId,
    createdById: row.userId,
    archived: row.archived,
    learningContainerIds,
    learningContainerId:
      learningContainerIds.length === 1 ? learningContainerIds[0] : null,
    createdAt: row.createdAt.toISOString(),
    updatedAt: row.updatedAt.toISOString(),
  };
}

/** A page's keyset position: the exact database timestamp and the row id. */
export type ThreadCursor = { value: string; id: string };

export function encodeThreadCursor(cursor: ThreadCursor): string {
  return Buffer.from(JSON.stringify(cursor)).toString("base64url");
}

export function decodeThreadCursor(raw: string): ThreadCursor | null {
  try {
    const parsed = JSON.parse(Buffer.from(raw, "base64url").toString("utf8"));
    return typeof parsed?.value === "string" && typeof parsed?.id === "string"
      ? { value: parsed.value, id: parsed.id }
      : null;
  } catch {
    return null;
  }
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

/** A memory's identity for near-duplicate absorption, not its stored text. */
function memoryKey(content: string): string {
  return content
    .trim()
    .toLowerCase()
    .replace(/[\s]+/g, " ")
    .replace(/[.!?;:,]+$/g, "")
    .trim();
}

const JOIN_TOKEN_TTL_MS = 60 * 60_000;
/** The hosted platform answers every lock acquisition with this TTL, whatever was asked. */
const LOCK_TTL_SECONDS = 120;

export class IntelligenceStore {
  constructor(private readonly db: Database) {}

  // ---- threads ------------------------------------------------------------

  /**
   * One page of a person's threads, newest activity first, with the keyset a
   * client sends back for the next page. The cursor carries the database's
   * microsecond `updated_at`, not the millisecond ISO string in the summary, so
   * a page boundary cannot duplicate or skip a row written in the same
   * millisecond.
   */
  async listThreads(input: {
    userId: string;
    agentId?: string;
    includeArchived?: boolean;
    limit?: number;
    cursor?: ThreadCursor;
  }): Promise<{
    threads: ReturnType<typeof toThreadSummary>[];
    nextCursor: string | null;
  }> {
    const requested = input.limit ?? 0;
    const limit =
      Number.isFinite(requested) && requested > 0
        ? Math.min(Math.floor(requested), 500)
        : 200;
    const conditions = [eq(intelligenceThreads.userId, input.userId)];
    if (input.agentId) {
      conditions.push(eq(intelligenceThreads.agentId, input.agentId));
    }
    if (!input.includeArchived) {
      conditions.push(eq(intelligenceThreads.archived, false));
    }
    if (input.cursor) {
      conditions.push(
        sql`(${intelligenceThreads.updatedAt}, ${intelligenceThreads.id}) < (${input.cursor.value}::timestamptz, ${input.cursor.id})`,
      );
    }
    // One extra row tells us whether there is a next page without a count.
    const rows = await this.db
      .select({
        row: intelligenceThreads,
        cursorValue: sql<string>`to_char(${intelligenceThreads.updatedAt} AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"')`,
      })
      .from(intelligenceThreads)
      .where(and(...conditions))
      .orderBy(
        desc(intelligenceThreads.updatedAt),
        desc(intelligenceThreads.id),
      )
      .limit(limit + 1);
    const page = rows.slice(0, limit);
    const last = page.at(-1);
    return {
      threads: page.map(({ row }) => toThreadSummary(row)),
      nextCursor:
        rows.length > limit && last
          ? encodeThreadCursor({ value: last.cursorValue, id: last.row.id })
          : null,
    };
  }

  async getThread(threadId: string) {
    const [row] = await this.db
      .select()
      .from(intelligenceThreads)
      .where(eq(intelligenceThreads.id, threadId))
      .limit(1);
    return row;
  }

  /**
   * The thread, created for this user when it does not exist yet; null when it
   * belongs to somebody else. Hosted connect and lock both create the thread
   * for the caller on first use, and both refuse another user's thread.
   */
  async ensureThreadForUser(input: {
    threadId: string;
    userId: string;
    agentId: string;
  }): Promise<ThreadRow | null> {
    const existing = await this.getThread(input.threadId);
    if (existing) return existing.userId === input.userId ? existing : null;
    const [created] = await this.db
      .insert(intelligenceThreads)
      .values({
        id: input.threadId,
        userId: input.userId,
        agentId: input.agentId,
      })
      .onConflictDoNothing()
      .returning();
    if (created) return created;
    const raced = await this.getThread(input.threadId);
    return raced && raced.userId === input.userId ? raced : null;
  }

  async createThread(input: {
    threadId: string;
    userId: string;
    agentId: string;
    name?: string;
    learningContainerId?: string;
  }) {
    const [row] = await this.db
      .insert(intelligenceThreads)
      .values({
        id: input.threadId,
        userId: input.userId,
        agentId: input.agentId,
        name: input.name ?? null,
        learningContainerIds: input.learningContainerId
          ? [input.learningContainerId]
          : [],
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
        ...(updates.archived !== undefined
          ? { archived: updates.archived }
          : {}),
        updatedAt: new Date(),
        ...(updates.name !== undefined ? { lastUpdatedAt: new Date() } : {}),
      })
      .where(eq(intelligenceThreads.id, threadId))
      .returning();
    return row;
  }

  async deleteThread(threadId: string): Promise<boolean> {
    return this.db.transaction(async (tx) => {
      const [row] = await tx
        .delete(intelligenceThreads)
        .where(eq(intelligenceThreads.id, threadId))
        .returning({ id: intelligenceThreads.id });
      if (!row) return false;
      await tx
        .delete(intelligenceThreadEvents)
        .where(eq(intelligenceThreadEvents.threadId, threadId));
      await tx
        .delete(intelligenceThreadLocks)
        .where(eq(intelligenceThreadLocks.threadId, threadId));
      await tx
        .delete(intelligenceJoinTokens)
        .where(eq(intelligenceJoinTokens.threadId, threadId));
      return true;
    });
  }

  // ---- events and messages ------------------------------------------------

  /**
   * One appended batch, ordered by its per-run seq.
   *
   * The runner retries event pushes; the insert is idempotent on `(run_id, seq)`
   * so a replayed batch stores each event once. The batch and the transcript it
   * folds into are one transaction, and the thread row is locked first, so two
   * batches cannot read the same messages and write over each other.
   */
  async appendEvents(
    threadId: string,
    runId: string,
    events: { seq: number; payload: Record<string, unknown> }[],
  ): Promise<{ ids: Map<number, string>; events: Record<string, unknown>[] }> {
    const sorted = [...events].sort((a, b) => a.seq - b.seq);
    return this.db.transaction(async (tx) => {
      const [thread] = await tx
        .select()
        .from(intelligenceThreads)
        .where(eq(intelligenceThreads.id, threadId))
        .for("update")
        .limit(1);
      const inserted =
        sorted.length === 0
          ? []
          : await tx
              .insert(intelligenceThreadEvents)
              .values(
                sorted.map((event) => ({
                  threadId,
                  runId,
                  seq: event.seq,
                  event: event.payload as Record<string, unknown>,
                })),
              )
              .onConflictDoNothing()
              .returning({
                id: intelligenceThreadEvents.id,
                seq: intelligenceThreadEvents.seq,
                event: intelligenceThreadEvents.event,
              });
      const stored = [...inserted]
        .sort((a, b) => a.seq - b.seq)
        .map((row) => row.event as Record<string, unknown>);
      if (stored.length > 0 && thread) {
        const messages = foldEventsToMessages(
          (thread.messages ?? []) as never,
          stored as never,
        );
        await tx
          .update(intelligenceThreads)
          .set({
            messages: messages as never,
            lastRunAt: new Date(),
            lastUpdatedAt: new Date(),
            updatedAt: new Date(),
          })
          .where(eq(intelligenceThreads.id, threadId));
      }
      return {
        ids: new Map(inserted.map((row) => [row.seq, String(row.id)])),
        events: stored,
      };
    });
  }

  /** One inspector page of the thread's event log, and whether it was cut short. */
  async listEventsPage(
    threadId: string,
    limit = 1000,
  ): Promise<{ events: Record<string, unknown>[]; truncated: boolean }> {
    const rows = await this.db
      .select()
      .from(intelligenceThreadEvents)
      .where(eq(intelligenceThreadEvents.threadId, threadId))
      .orderBy(asc(intelligenceThreadEvents.id))
      .limit(limit + 1);
    return {
      events: rows
        .slice(0, limit)
        .map((row) => row.event as Record<string, unknown>),
      truncated: rows.length > limit,
    };
  }

  /** The state events only, which is all the state fold reads. */
  async listStateEvents(threadId: string): Promise<Record<string, unknown>[]> {
    const rows = await this.db
      .select({ event: intelligenceThreadEvents.event })
      .from(intelligenceThreadEvents)
      .where(
        and(
          eq(intelligenceThreadEvents.threadId, threadId),
          sql`${intelligenceThreadEvents.event}->>'type' in ('STATE_SNAPSHOT', 'STATE_DELTA')`,
        ),
      )
      .orderBy(asc(intelligenceThreadEvents.id));
    return rows.map((row) => row.event as Record<string, unknown>);
  }

  /**
   * The events a reconnecting client has not seen, keyed on the runner's
   * `cpki_event_id` cursor (the durable id the client tracks).
   *
   * The cursor is resolved to its row and the page is read after it in SQL. The
   * earlier version read the first 2000 events and looked for the cursor among
   * them, so on a longer thread a recent cursor was never found and the oldest
   * page was replayed instead. A cursor this log has never seen falls back to
   * the whole log, which is what a browser whose history was rebuilt needs.
   */
  async listEventsAfter(
    threadId: string,
    cpkiEventId: string | null,
    limit = 2000,
  ): Promise<Record<string, unknown>[]> {
    const eventOf = (row: typeof intelligenceThreadEvents.$inferSelect) =>
      row.event as Record<string, unknown>;
    let afterId = 0;
    if (cpkiEventId) {
      const [anchor] = await this.db
        .select({ id: intelligenceThreadEvents.id })
        .from(intelligenceThreadEvents)
        .where(
          and(
            eq(intelligenceThreadEvents.threadId, threadId),
            sql`${intelligenceThreadEvents.event}->'metadata'->>'cpki_event_id' = ${cpkiEventId}`,
          ),
        )
        .orderBy(asc(intelligenceThreadEvents.id))
        .limit(1);
      if (anchor) afterId = anchor.id;
    }
    const rows = await this.db
      .select()
      .from(intelligenceThreadEvents)
      .where(
        and(
          eq(intelligenceThreadEvents.threadId, threadId),
          afterId > 0 ? gt(intelligenceThreadEvents.id, afterId) : undefined,
        ),
      )
      .orderBy(asc(intelligenceThreadEvents.id))
      .limit(limit);
    return rows.map(eventOf);
  }

  /** Whether a run holds the thread right now, whether or not it has emitted an event. */
  async isThreadRunning(threadId: string): Promise<boolean> {
    const lock = await this.getLock(threadId);
    return Boolean(lock && lock.expiresAt.getTime() > Date.now());
  }

  /**
   * The newest event of the thread and whether its run is still active.
   *
   * `running` comes from the lock, not from the presence of an event: a run that
   * has just been locked and has not produced its first event is running, and a
   * stream that treated it as idle would close before the run said anything.
   */
  async latestEvent(
    threadId: string,
  ): Promise<{ cpkiEventId: string | null; running: boolean }> {
    const [row] = await this.db
      .select()
      .from(intelligenceThreadEvents)
      .where(eq(intelligenceThreadEvents.threadId, threadId))
      .orderBy(desc(intelligenceThreadEvents.id))
      .limit(1);
    return {
      cpkiEventId: row
        ? ((row.event as { metadata?: { cpki_event_id?: string } }).metadata
            ?.cpki_event_id ?? String(row.id))
        : null,
      running: await this.isThreadRunning(threadId),
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
   * One statement, so two concurrent requests cannot both read "no lock" and
   * both take it: the conflict clause only rewrites a row this run already
   * holds or one whose TTL has passed. The TTL is the platform's, not the
   * caller's: hosted answers every acquisition with 120 seconds.
   */
  async acquireLock(input: {
    threadId: string;
    runId: string;
    userId: string;
    agentId: string;
  }): Promise<
    | { acquired: true; runId: string; joinToken: string; ttlSeconds: number }
    | { acquired: false; runId?: string }
  > {
    const expiresAt = new Date(Date.now() + LOCK_TTL_SECONDS * 1000);
    const result = await this.db.execute(sql`
      insert into intelligence_thread_locks
        (thread_id, run_id, user_id, agent_id, expires_at)
      values
        (${input.threadId}, ${input.runId}, ${input.userId}, ${input.agentId}, ${expiresAt})
      on conflict (thread_id) do update
        set run_id = excluded.run_id,
            user_id = excluded.user_id,
            agent_id = excluded.agent_id,
            expires_at = excluded.expires_at
        where intelligence_thread_locks.run_id = excluded.run_id
           or intelligence_thread_locks.expires_at <= now()
      returning run_id
    `);
    const rows = Array.isArray(result)
      ? result
      : ((result as { rows?: unknown[] }).rows ?? []);
    if (rows.length === 0) {
      const lock = await this.getLock(input.threadId);
      return { acquired: false, runId: lock?.runId };
    }
    return {
      acquired: true,
      runId: input.runId,
      joinToken: await this.mintToken("thread", input.threadId),
      ttlSeconds: LOCK_TTL_SECONDS,
    };
  }

  /**
   * Renew the run's lock. `completed` is the platform's answer once the run
   * released it; `conflict` means another run holds the thread now, which the
   * caller must treat as a lost lock rather than a normal finish.
   */
  async renewLock(
    threadId: string,
    runId: string,
  ): Promise<
    | { status: "renewed"; ttlSeconds: number }
    | { status: "completed" }
    | { status: "conflict" }
  > {
    const [row] = await this.db
      .update(intelligenceThreadLocks)
      .set({ expiresAt: new Date(Date.now() + LOCK_TTL_SECONDS * 1000) })
      .where(
        and(
          eq(intelligenceThreadLocks.threadId, threadId),
          eq(intelligenceThreadLocks.runId, runId),
        ),
      )
      .returning({ threadId: intelligenceThreadLocks.threadId });
    if (row) return { status: "renewed", ttlSeconds: LOCK_TTL_SECONDS };
    const lock = await this.getLock(threadId);
    return lock ? { status: "conflict" } : { status: "completed" };
  }

  /** Release the lock if this run holds it; false when it did not. */
  async releaseLock(threadId: string, runId: string): Promise<boolean> {
    const [row] = await this.db
      .delete(intelligenceThreadLocks)
      .where(
        and(
          eq(intelligenceThreadLocks.threadId, threadId),
          eq(intelligenceThreadLocks.runId, runId),
        ),
      )
      .returning({ threadId: intelligenceThreadLocks.threadId });
    return Boolean(row);
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

  // ---- join tokens and meta channels --------------------------------------

  /**
   * The person's stable meta-channel suffix. Hosted returns the same joinCode
   * on every list request; a code minted per request made the client tear down
   * and rejoin its thread-list channel on every page load.
   */
  async ensureJoinCode(userId: string): Promise<string> {
    const [existing] = await this.db
      .select({ code: intelligenceUserJoinCodes.code })
      .from(intelligenceUserJoinCodes)
      .where(eq(intelligenceUserJoinCodes.userId, userId))
      .limit(1);
    if (existing) return existing.code;
    const code = randomUUID();
    const [created] = await this.db
      .insert(intelligenceUserJoinCodes)
      .values({ userId, code })
      .onConflictDoNothing()
      .returning({ code: intelligenceUserJoinCodes.code });
    if (created) return created.code;
    const [raced] = await this.db
      .select({ code: intelligenceUserJoinCodes.code })
      .from(intelligenceUserJoinCodes)
      .where(eq(intelligenceUserJoinCodes.userId, userId))
      .limit(1);
    return raced.code;
  }

  /** Drop tokens nobody can use any more; every mint is a chance to do it. */
  private async pruneExpiredJoinTokens(): Promise<void> {
    await this.db
      .delete(intelligenceJoinTokens)
      .where(lt(intelligenceJoinTokens.expiresAt, new Date()));
  }

  async mintToken(
    kind: "thread" | "user_threads" | "user_memories" | "trajectory",
    scope: string,
  ): Promise<string> {
    await this.pruneExpiredJoinTokens();
    const isUser = kind === "user_threads" || kind === "user_memories";
    const token = randomUUID();
    await this.db
      .insert(intelligenceJoinTokens)
      .values({
        token,
        kind,
        threadId: kind === "thread" || kind === "trajectory" ? scope : null,
        userId: isUser ? scope : null,
        code: isUser ? await this.ensureJoinCode(scope) : null,
        expiresAt: new Date(Date.now() + JOIN_TOKEN_TTL_MS),
      })
      .onConflictDoNothing();
    return token;
  }

  /** Mint a Trajectory capture join: the browser streams events on the
   * gateway's `trajectory:{id}` topic, scoped to this trajectory. */
  async joinTrajectory(trajectoryId: string): Promise<string> {
    return this.mintToken("trajectory", trajectoryId);
  }

  /**
   * Whether a join token authorizes a topic. Tokens name their scope, so a token
   * minted for one thread cannot join another's channel, and a user_meta token
   * names the one person whose channel it may join.
   */
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
      return { ok: row.threadId === topic.slice("thread:".length) };
    }
    if (row.kind === "trajectory") {
      return { ok: row.threadId === topic.slice("trajectory:".length) };
    }
    if (row.kind === "user_threads") {
      return {
        ok: Boolean(row.code) && topic === `user_meta:${row.code}`,
        userId: row.userId ?? undefined,
      };
    }
    if (row.kind === "user_memories") {
      return {
        ok: Boolean(row.code) && topic === `user_meta:memories:${row.code}`,
        userId: row.userId ?? undefined,
      };
    }
    return { ok: false };
  }

  /**
   * Authorize and burn the token in one step. Hosted join tokens are single
   * use: a second socket presenting one is refused. The gateway remembers the
   * topics a socket has already authorized, so a Phoenix rejoin on the same
   * socket keeps working after its token row is gone.
   */
  async authorizeAndConsumeTopic(
    token: string | null,
    topic: string,
  ): Promise<{ ok: boolean; userId?: string }> {
    const authorized = await this.authorizeTopic(token, topic);
    if (!authorized.ok || !token) return authorized;
    const [row] = await this.db
      .delete(intelligenceJoinTokens)
      .where(eq(intelligenceJoinTokens.token, token))
      .returning({ token: intelligenceJoinTokens.token });
    return row ? authorized : { ok: false };
  }

  // ---- memories -----------------------------------------------------------

  async listMemories(
    userId: string,
    includeInvalidated = false,
    scope?: string,
  ) {
    const rows = await this.db
      .select()
      .from(intelligenceMemories)
      .where(
        and(
          eq(intelligenceMemories.userId, userId),
          ...(includeInvalidated
            ? []
            : [isNull(intelligenceMemories.invalidatedAt)]),
          ...(scope ? [eq(intelligenceMemories.scope, scope)] : []),
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
    // Near-duplicate absorption. Hosted merges a near-duplicate into the row it
    // already holds; without an embedding pipeline, the closest local answer is
    // a normalised content comparison (case, whitespace, trailing punctuation).
    // The candidate set is one person's live rows, which the list endpoint caps
    // at 200, so this stays a small read.
    const key = memoryKey(input.content);
    const candidates = await this.db
      .select()
      .from(intelligenceMemories)
      .where(
        and(
          eq(intelligenceMemories.userId, input.userId),
          eq(intelligenceMemories.scope, input.scope),
          isNull(intelligenceMemories.invalidatedAt),
        ),
      )
      .limit(500);
    const existing = candidates.find((row) => memoryKey(row.content) === key);
    if (existing) {
      const sourceThreadIds = [
        ...new Set([...existing.sourceThreadIds, ...input.sourceThreadIds]),
      ];
      if (sourceThreadIds.length !== existing.sourceThreadIds.length) {
        const [updated] = await this.db
          .update(intelligenceMemories)
          .set({ sourceThreadIds, updatedAt: new Date() })
          .where(eq(intelligenceMemories.id, existing.id))
          .returning();
        return { row: updated ?? existing, absorbed: true } as const;
      }
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

  /**
   * Retire `id` and insert `content` as its successor, atomically. Fields the
   * caller omitted are carried from the retired row: a caller that changes only
   * the content must not silently turn a project memory into a private one.
   */
  /** One memory the person owns, live or retired. */
  async getMemory(id: string, userId: string) {
    const [row] = await this.db
      .select()
      .from(intelligenceMemories)
      .where(
        and(
          eq(intelligenceMemories.id, id),
          eq(intelligenceMemories.userId, userId),
        ),
      )
      .limit(1);
    return row;
  }

  async supersedeMemory(input: {
    id: string;
    userId: string;
    content: string;
    kind?: string;
    scope?: string;
    sourceThreadIds?: string[];
  }) {
    return this.db.transaction(async (tx) => {
      const [current] = await tx
        .select()
        .from(intelligenceMemories)
        .where(
          and(
            eq(intelligenceMemories.id, input.id),
            eq(intelligenceMemories.userId, input.userId),
            isNull(intelligenceMemories.invalidatedAt),
          ),
        )
        .limit(1);
      if (!current) return undefined;
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
          kind: input.kind ?? current.kind,
          scope: input.scope ?? current.scope,
          content: input.content,
          sourceThreadIds: input.sourceThreadIds ?? current.sourceThreadIds,
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
  async recallMemories(
    userId: string,
    query: string,
    limit: number,
    scope?: string,
  ) {
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
          ...(scope ? [eq(intelligenceMemories.scope, scope)] : []),
          or(
            sql`to_tsvector('english', ${intelligenceMemories.content}) @@ plainto_tsquery('english', ${query})`,
            sql`lower(${intelligenceMemories.content}) LIKE '%' || lower(${query}) || '%'`,
          ),
        ),
      )
      .orderBy(
        desc(
          sql`ts_rank(to_tsvector('english', ${intelligenceMemories.content}), plainto_tsquery('english', ${query}))`,
        ),
      )
      .limit(limit);
    return rows;
  }

  // ---- learning -----------------------------------------------------------

  async listContainers() {
    return this.db.select().from(intelligenceLearningContainers);
  }

  async containerExists(containerId: string): Promise<boolean> {
    const [row] = await this.db
      .select({ id: intelligenceLearningContainers.id })
      .from(intelligenceLearningContainers)
      .where(eq(intelligenceLearningContainers.id, containerId))
      .limit(1);
    return Boolean(row);
  }

  /** Record a run's Learning Container on the thread, keeping earlier memberships. */
  async assignLearningContainer(
    threadId: string,
    containerId: string,
  ): Promise<void> {
    await this.db
      .update(intelligenceThreads)
      .set({
        learningContainerIds: sql`(
          select array_agg(distinct member)
          from unnest(array_append(${intelligenceThreads.learningContainerIds}, ${containerId})) as member
        )`,
      })
      .where(eq(intelligenceThreads.id, threadId));
  }

  /** Create a Learning Container with the project service token; idempotent. */
  async createContainer(input: {
    id: string;
    name: string;
    projectId: number;
  }): Promise<"created" | "present"> {
    const [row] = await this.db
      .insert(intelligenceLearningContainers)
      .values({ id: input.id, name: input.name, projectId: input.projectId })
      .onConflictDoNothing()
      .returning({ id: intelligenceLearningContainers.id });
    return row ? "created" : "present";
  }

  /** One published Skill revision's ZIP: the newest, or the one named. */
  async skillRevision(
    containerId: string,
    revision?: string,
  ): Promise<{ revision: string; archive: Buffer } | null> {
    const [row] = await this.db
      .select()
      .from(intelligenceSkillRevisions)
      .where(
        and(
          eq(intelligenceSkillRevisions.containerId, containerId),
          ...(revision
            ? [eq(intelligenceSkillRevisions.revision, revision)]
            : []),
        ),
      )
      .orderBy(desc(intelligenceSkillRevisions.publishedAt))
      .limit(1);
    if (!row) return null;
    return { revision: row.revision, archive: row.archive };
  }

  /**
   * Publish one Skill revision. The revision is the id the archive's
   * `manifest.json` declares (what the runtime's registry pins and checks);
   * an archive without a readable manifest is stored under its content hash.
   */
  async publishSkillRevision(input: {
    containerId: string;
    archive: Buffer;
  }): Promise<string> {
    const revision =
      readArchiveRevision(input.archive) ??
      createHash("sha256").update(input.archive).digest("hex");
    await this.db
      .insert(intelligenceSkillRevisions)
      .values({
        containerId: input.containerId,
        revision,
        archive: input.archive,
      })
      .onConflictDoNothing();
    return revision;
  }

  // ---- managed-channel files ----------------------------------------------

  /** Store one managed-channel file; the handle is its asset id. */
  async saveChannelFile(input: {
    deliveryId?: string;
    operationId?: string;
    filename?: string;
    title?: string;
    altText?: string;
    mimeType?: string;
    bytes: Buffer;
  }): Promise<string> {
    const [row] = await this.db
      .insert(intelligenceChannelFiles)
      .values({
        deliveryId: input.deliveryId ?? null,
        operationId: input.operationId ?? null,
        filename: input.filename ?? null,
        title: input.title ?? null,
        altText: input.altText ?? null,
        mimeType: input.mimeType ?? null,
        bytes: input.bytes,
      })
      .returning({ handle: intelligenceChannelFiles.handle });
    return row.handle;
  }

  /** Read one managed-channel file back (the runtime's history hydration). */
  async getChannelFile(handle: string): Promise<{
    bytes: Buffer;
    mimeType: string;
    filename: string | null;
  } | null> {
    const [row] = await this.db
      .select()
      .from(intelligenceChannelFiles)
      .where(eq(intelligenceChannelFiles.handle, handle))
      .limit(1);
    if (!row) return null;
    return {
      bytes: row.bytes,
      mimeType: row.mimeType ?? "application/octet-stream",
      filename: row.filename,
    };
  }
}
