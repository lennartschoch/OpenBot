ALTER TABLE "intelligence_learning_containers" ADD COLUMN IF NOT EXISTS "project_id" integer DEFAULT 1 NOT NULL;
--> statement-breakpoint
ALTER TABLE "intelligence_learning_containers" ADD COLUMN IF NOT EXISTS "prompt_context" text;
--> statement-breakpoint
ALTER TABLE "intelligence_learning_containers" ADD COLUMN IF NOT EXISTS "updated_at" timestamp with time zone DEFAULT now() NOT NULL;
--> statement-breakpoint
INSERT INTO "intelligence_learning_containers" ("id", "name", "project_id")
VALUES ('openbot', 'OpenBot', 1)
ON CONFLICT ("id") DO NOTHING;
