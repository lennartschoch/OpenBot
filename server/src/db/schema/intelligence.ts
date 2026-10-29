import {
  boolean,
  index,
  integer,
  pgTable,
  serial,
  text,
  timestamp,
  uniqueIndex,
} from "drizzle-orm/pg-core";
import { customType } from "drizzle-orm/pg-core";
import { jsonb } from "./json";

/** `bytea` column, same custom type as `core.ts` uses for its blobs. */
const bytea = customType<{ data: Buffer; driverData: Buffer }>({
  dataType: () => "bytea",
});

/**
 * The local Intelligence platform: the tables behind OpenBot's self-contained
 * replacement for CopilotKit's hosted Intelligence service.
 *
 * Single-instance by design: one deployment, one server, one Postgres. The shapes mirror the
 * hosted platform's REST contract (`@copilotkit/runtime`'s `CopilotKitIntelligence` client is
 * the protocol), which is what lets OpenBot run without any CopilotKit-hosted service.
 */

/** A conversation thread — the platform's `ThreadSummary` row. */
export const intelligenceThreads = pgTable(
  "intelligence_threads",
  {
    id: text("id").primaryKey(),
    userId: text("user_id").notNull(),
    agentId: text("agent_id").notNull(),
    name: text("name"),
    archived: boolean("archived").notNull().default(false),
    /** The folded conversation, as AG-UI messages; the event log is the source. */
    messages: jsonb("messages").$type<unknown[]>().notNull().default([]),
    learningContainerIds: text("learning_container_ids").array().notNull().default([]),
    createdAt: timestamp("created_at", { withTimezone: true })
      .notNull()
      .defaultNow(),
    updatedAt: timestamp("updated_at", { withTimezone: true })
      .notNull()
      .defaultNow(),
    lastRunAt: timestamp("last_run_at", { withTimezone: true }),
    lastUpdatedAt: timestamp("last_updated_at", { withTimezone: true }),
  },
  (table) => [
    index("intelligence_threads_user_idx").on(table.userId, table.agentId),
  ],
);

/**
 * The AG-UI event log, one row per event, per run, ordered by the runner's
 * `cpki_event_seq`. Doubles as the Inspector's replay source.
 */
export const intelligenceThreadEvents = pgTable(
  "intelligence_thread_events",
  {
    id: serial("id").primaryKey(),
    threadId: text("thread_id").notNull(),
    runId: text("run_id").notNull(),
    seq: integer("seq").notNull(),
    event: jsonb("event").$type<Record<string, unknown>>().notNull(),
    createdAt: timestamp("created_at", { withTimezone: true })
      .notNull()
      .defaultNow(),
  },
  (table) => [
    uniqueIndex("intelligence_thread_events_run_seq_idx").on(
      table.runId,
      table.seq,
    ),
    index("intelligence_thread_events_thread_idx").on(table.threadId, table.id),
  ],
);

/** The per-thread run lock: one run at a time, with a TTL the runner renews. */
export const intelligenceThreadLocks = pgTable(
  "intelligence_thread_locks",
  {
    threadId: text("thread_id").primaryKey(),
    runId: text("run_id").notNull(),
    userId: text("user_id").notNull(),
    agentId: text("agent_id").notNull(),
    expiresAt: timestamp("expires_at", { withTimezone: true }).notNull(),
  },
);

/** Single-use join credentials minted by connect / join-code / subscribe. */
export const intelligenceJoinTokens = pgTable(
  "intelligence_join_tokens",
  {
    token: text("token").primaryKey(),
    /** What the token authorizes a Phoenix join onto: a thread, a user's meta channel. */
    kind: text("kind").notNull(),
    threadId: text("thread_id"),
    userId: text("user_id"),
    expiresAt: timestamp("expires_at", { withTimezone: true }).notNull(),
  },
  (table) => [index("intelligence_join_tokens_thread_idx").on(table.threadId)],
);

/**
 * A person's long-term memory, the platform row behind `/api/memories`.
 *
 * Supersede retires the old row (`invalidatedAt`) and inserts a new one, which is
 * the hosted platform's non-lossy contract and what `memory/intelligence.ts` consumes.
 */
export const intelligenceMemories = pgTable(
  "intelligence_memories",
  {
    id: text("id").primaryKey(),
    userId: text("user_id").notNull(),
    kind: text("kind").notNull().default("topical"),
    scope: text("scope").notNull().default("user"),
    content: text("content").notNull(),
    sourceThreadIds: text("source_thread_ids").array().notNull().default([]),
    invalidatedAt: timestamp("invalidated_at", { withTimezone: true }),
    createdAt: timestamp("created_at", { withTimezone: true })
      .notNull()
      .defaultNow(),
    updatedAt: timestamp("updated_at", { withTimezone: true })
      .notNull()
      .defaultNow(),
  },
  (table) => [
    index("intelligence_memories_user_idx").on(table.userId, table.createdAt),
  ],
);

/** A user's Learning membership (Phase 3 serves these; rows are stable from here). */
export const intelligenceLearningContainers = pgTable(
  "intelligence_learning_containers",
  {
    id: text("id").primaryKey(),
    name: text("name").notNull(),
    createdAt: timestamp("created_at", { withTimezone: true })
      .notNull()
      .defaultNow(),
  },
);

/** One published Skill revision of a Learning Container: the ZIP and its content hash. */
export const intelligenceSkillRevisions = pgTable(
  "intelligence_skill_revisions",
  {
    containerId: text("container_id").notNull(),
    revision: text("revision").notNull(),
    archive: bytea("archive").notNull(),
    publishedAt: timestamp("published_at", { withTimezone: true })
      .notNull()
      .defaultNow(),
  },
  (table) => [
    uniqueIndex("intelligence_skill_revisions_pk").on(
      table.containerId,
      table.revision,
    ),
  ],
);
