CREATE TABLE "intelligence_channel_files" (
	"handle" uuid PRIMARY KEY DEFAULT gen_random_uuid() NOT NULL,
	"delivery_id" text,
	"operation_id" text,
	"filename" text,
	"title" text,
	"alt_text" text,
	"mime_type" text,
	"bytes" bytea NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL
);
--> statement-breakpoint
CREATE INDEX "intelligence_channel_files_delivery_idx" ON "intelligence_channel_files" USING btree ("delivery_id");
