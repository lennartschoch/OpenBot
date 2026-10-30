CREATE TABLE "intelligence_channel_files" (
	"handle" uuid PRIMARY KEY DEFAULT gen_random_uuid() NOT NULL,
	"delivery_id" text,
	"operation_id" text,
	"filename" text,
	"title" text,
	"alt_text" text,
	"mime_type" text,
	"bytes" "bytea" NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL
);
--> statement-breakpoint
CREATE TABLE "intelligence_join_tokens" (
	"token" text PRIMARY KEY NOT NULL,
	"kind" text NOT NULL,
	"thread_id" text,
	"user_id" text,
	"code" text,
	"expires_at" timestamp with time zone NOT NULL
);
--> statement-breakpoint
CREATE TABLE "intelligence_learning_containers" (
	"id" text PRIMARY KEY NOT NULL,
	"name" text NOT NULL,
	"project_id" integer DEFAULT 1 NOT NULL,
	"prompt_context" text,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	"updated_at" timestamp with time zone DEFAULT now() NOT NULL
);
--> statement-breakpoint
CREATE TABLE "intelligence_memories" (
	"id" text PRIMARY KEY NOT NULL,
	"user_id" text NOT NULL,
	"kind" text DEFAULT 'topical' NOT NULL,
	"scope" text DEFAULT 'user' NOT NULL,
	"content" text NOT NULL,
	"source_thread_ids" text[] DEFAULT '{}' NOT NULL,
	"invalidated_at" timestamp with time zone,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	"updated_at" timestamp with time zone DEFAULT now() NOT NULL
);
--> statement-breakpoint
CREATE TABLE "intelligence_skill_revisions" (
	"container_id" text NOT NULL,
	"revision" text NOT NULL,
	"archive" "bytea" NOT NULL,
	"published_at" timestamp with time zone DEFAULT now() NOT NULL
);
--> statement-breakpoint
CREATE TABLE "intelligence_thread_events" (
	"id" serial PRIMARY KEY NOT NULL,
	"thread_id" text NOT NULL,
	"run_id" text NOT NULL,
	"seq" integer NOT NULL,
	"event" jsonb NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL
);
--> statement-breakpoint
CREATE TABLE "intelligence_thread_locks" (
	"thread_id" text PRIMARY KEY NOT NULL,
	"run_id" text NOT NULL,
	"user_id" text NOT NULL,
	"agent_id" text NOT NULL,
	"expires_at" timestamp with time zone NOT NULL
);
--> statement-breakpoint
CREATE TABLE "intelligence_threads" (
	"id" text PRIMARY KEY NOT NULL,
	"user_id" text NOT NULL,
	"agent_id" text NOT NULL,
	"name" text,
	"archived" boolean DEFAULT false NOT NULL,
	"messages" jsonb DEFAULT '[]'::jsonb NOT NULL,
	"learning_container_ids" text[] DEFAULT '{}' NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	"updated_at" timestamp with time zone DEFAULT now() NOT NULL,
	"last_run_at" timestamp with time zone,
	"last_updated_at" timestamp with time zone
);
--> statement-breakpoint
CREATE TABLE "intelligence_user_join_codes" (
	"user_id" text PRIMARY KEY NOT NULL,
	"code" text NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL
);
--> statement-breakpoint
CREATE INDEX "intelligence_channel_files_delivery_idx" ON "intelligence_channel_files" USING btree ("delivery_id");--> statement-breakpoint
CREATE INDEX "intelligence_join_tokens_thread_idx" ON "intelligence_join_tokens" USING btree ("thread_id");--> statement-breakpoint
CREATE INDEX "intelligence_join_tokens_expires_idx" ON "intelligence_join_tokens" USING btree ("expires_at");--> statement-breakpoint
CREATE INDEX "intelligence_memories_user_idx" ON "intelligence_memories" USING btree ("user_id","created_at");--> statement-breakpoint
CREATE UNIQUE INDEX "intelligence_skill_revisions_pk" ON "intelligence_skill_revisions" USING btree ("container_id","revision");--> statement-breakpoint
CREATE UNIQUE INDEX "intelligence_thread_events_run_seq_idx" ON "intelligence_thread_events" USING btree ("run_id","seq");--> statement-breakpoint
CREATE INDEX "intelligence_thread_events_thread_idx" ON "intelligence_thread_events" USING btree ("thread_id","id");--> statement-breakpoint
CREATE INDEX "intelligence_thread_events_cpki_idx" ON "intelligence_thread_events" USING btree ((("event" -> 'metadata') ->> 'cpki_event_id'));--> statement-breakpoint
CREATE INDEX "intelligence_threads_user_idx" ON "intelligence_threads" USING btree ("user_id","agent_id");--> statement-breakpoint
CREATE UNIQUE INDEX "intelligence_user_join_codes_code_idx" ON "intelligence_user_join_codes" USING btree ("code");
--> statement-breakpoint
-- The container a deployment's Bots contribute to by default. Created here rather than left to
-- `scripts/setup-learning.ts`, which needs a CopilotKit CLI login and a project key: this platform
-- is the project.
INSERT INTO "intelligence_learning_containers" ("id", "name", "project_id")
VALUES ('openbot', 'OpenBot', 1);
