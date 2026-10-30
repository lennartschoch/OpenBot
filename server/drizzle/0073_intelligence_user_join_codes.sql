-- One stable join code per person, so the thread-list channel has an address that does not change
-- every time a page loads, and a minted join token can name the channel it is for.
CREATE TABLE IF NOT EXISTS "intelligence_user_join_codes" (
	"user_id" text PRIMARY KEY NOT NULL,
	"code" text NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL
);
--> statement-breakpoint
CREATE UNIQUE INDEX IF NOT EXISTS "intelligence_user_join_codes_code_idx" ON "intelligence_user_join_codes" USING btree ("code");
--> statement-breakpoint
-- A join token minted for a user's meta channel carries that channel's code, so a token for one
-- person's list cannot join another's.
ALTER TABLE "intelligence_join_tokens" ADD COLUMN IF NOT EXISTS "code" text;
--> statement-breakpoint
-- Replay asks for the event after a cpki_event_id cursor; without this it scans the thread's log.
CREATE INDEX IF NOT EXISTS "intelligence_thread_events_cpki_idx" ON "intelligence_thread_events" USING btree ((("event" -> 'metadata') ->> 'cpki_event_id'));
