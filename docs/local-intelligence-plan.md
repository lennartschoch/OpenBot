# Local-first OpenBot: replacing the CopilotKit-hosted services

## Why

OpenBot currently depends on three CopilotKit-hosted services:

1. **Intelligence REST API** — `https://api.intelligence.copilotkit.ai`
   (thread history, messages, locks, join tokens, memories + recall, entitlements,
   inspector, learning containers + skill ZIPs, trajectory join, annotations,
   managed-channel files)
2. **Intelligence realtime gateway** — `wss://realtime.intelligence.copilotkit.ai`
   (Phoenix-protocol WebSockets: `/runner`, `/client`, `/channels`)
3. **License/entitlement token** — gates memory features

Everything runs fully locally when one in-repo module speaks the same wire protocol,
backed by the Postgres OpenBot already ships. The `@copilotkit/*` npm packages stay —
they're OSS and run in-process; only their hosted peers are replaced.

**Deployment assumption: single instance.** One server process, one Postgres.
In-process pub/sub, no Redis, no multi-replica fan-out, no presence tracking.

## Wire protocol (verified against `@copilotkit/runtime` 1.83.0 + `@copilotkit/core` 1.83.0)

### REST (Bearer apiKey; memories scope by `x-cpki-user-id` header)

| Endpoint | Contract |
| --- | --- |
| `GET /api/threads?userId&agentId&includeArchived&limit&cursor` | `{threads, joinCode, joinToken?, nextCursor?}` |
| `POST /api/threads` `{threadId,userId,agentId,name?}` | 409 on dup → `{thread: ThreadSummary}` |
| `GET /api/threads/:id?userId` | `{thread}` (404 when absent) |
| `PATCH /api/threads/:id` `{userId,agentId,name?,archived?}` | `{thread}` |
| `DELETE /api/threads/:id` | 200 |
| `GET/POST /api/threads/:id/messages?userId` (`?fields=id`) | `{messages}` |
| `POST /api/threads/:id/lock` `{runId,userId,agentId,ttlSeconds?}` | `{threadId,runId,joinToken}`; 409 when locked |
| `PATCH /api/threads/:id/lock` `{runId,ttlSeconds}` (renew), `DELETE` (cleanup) | |
| `GET /api/threads/:id/join-code?userId`, `POST /api/threads/:id/connect` | `{threadId,joinToken,runId?}` |
| `POST /api/threads/subscribe` `{userId}` | `{joinToken}` → `user_meta:{joinCode}` Phoenix topic |
| `GET/POST /api/memories`, `PATCH/DELETE /api/memories/:id`, `POST /api/memories/recall` | `{memories}` / MemorySummary; create absorbs near-dups (`absorbed`), PATCH retires+inserts (`retiredId`); recall scores |
| `POST /api/memories/subscribe` | `{joinToken, joinCode}` → `user_meta:memories:{joinCode}` |
| `GET /api/entitlements/runtime` | `{status:"ready", entitlement:{active:true}}` |
| `GET /api/inspector/metadata`, `/api/_inspect/threads/:id/events\|state` | thin reads over the event log |
| `PUT /connector/annotate/:clientEventId` | idempotent insert |
| `GET /api/v1/learning/containers` (+ skills ZIP, runs/candidates pages) | Phase 3 |
| `POST /api/trajectories/join`, `GET /api/channels/files/:assetId` | Phase 2 |

`ThreadSummary = {id, name, agentId, createdById, archived, learningContainerIds?, createdAt, updatedAt, lastRunAt, lastUpdatedAt}`.

### Realtime (Phoenix channel framing: `[join_ref, ref, topic, event, payload]`)

- `/runner` (runtime's `IntelligenceAgentRunner`, `authToken` in socket params):
  joins `ingestion:{runId}` (params `{thread_id, run_id}`); join reply carries
  `capabilities:["runner_event_batch_v1"]`; pushes `events` `{events:[payload]}` (or
  `event` single) — payloads carry `threadId`/`runId`/`thread_id`/`run_id` and
  `metadata.cpki_event_id`/`cpki_event_seq`; gateway appends (idempotent by event id)
  and fans out as `ag_ui_event`. Also joins `thread:{threadId}` to receive browser
  input. Receives `ag_ui_event` relayed from the browser (`CUSTOM: stop`, tool results).
  Event pushes must be acked (`phx_reply ok`) — the runner's durability contract.
- `/client` (browser; socket param `join_token`, validated against minted tokens):
  topic `thread:{threadId}` with join params `{stream_mode:"run"|"connect", run_id?,
  last_seen_event_id?}`. Receives `ag_ui_event`; control events `replay_complete`,
  `replay_failed`, `stream_idle` (payload `{latestEventId}`) — `stream_idle` closes
  connect-mode streams. Pushes `stop_run {run_id}` (gateway relays as CUSTOM stop).
  Meta topics: `user_meta:{joinCode}` (thread list), `user_meta:memories:{joinCode}`
  (`memory_metadata` deltas).
- `/channels` — live-screen screencast relay (Phase 2).

Replay: on join, stream persisted events after `last_seen_event_id`, then
`replay_complete {latestEventId}`; if no active run, `stream_idle`.

## The re-implementation

One new module, `server/src/intelligence-local/`, mounted on the existing Bun server
(`http-server.ts` already dispatches WS upgrades; Hono serves REST). Zero new processes,
zero new containers.

### Tables (drizzle, existing Postgres)

- `intelligence_threads` — summary fields + folded `messages` JSONB (event log is source)
- `intelligence_thread_events` — `(run_id, seq)` unique; the replay + inspector source
- `intelligence_thread_locks` — one row per thread, TTL renewal
- `intelligence_join_tokens` — random tokens with expiry, minted by connect/join-code/subscribe
- `intelligence_memories` — user-scoped rows; recall = tsvector + pgvector hybrid
- `intelligence_learning_containers` + `intelligence_skill_revisions` — Phase 3

Messages are folded from the event log (AG-UI event reducer) at append time, so
`GET …/messages` is a single row read.

### Kept as-is

Composio + Arcade (tool integrations, env-gated), Parallel/Exa/Firecrawl (search; add a
local SearXNG default), Twilio/Slack/Teams, model providers, the 15 agent adapters,
Chrome extension, `@copilotkit/*` npm packages.

### Dropped

`COPILOTKIT_LICENSE_TOKEN`, CopilotKit-side annotation analytics, Inspector UI extras.

## Phases

1. **Core loop** — threads/messages/locks/join-code/connect/subscribe REST, memories
   REST (+ pgvector recall), entitlements, annotate, inspector reads; Phoenix relay
   `/runner` + `/client` incl. meta topics, replay, `stream_idle`.
   → full local chat with streaming, thread list + live updates, memory.
2. **Channels plane** — `/channels` screencast relay, `trajectories/join`, managed-channel files.
3. **Learning** — containers, skill-ZIP snapshots (sha256 revision, ETag), empty
   runs/candidates pages; engine later.
4. **Packaging** — `.env.example`, docker-compose, docs; no external keys beyond a model key.

Fork `lennartschoch/OpenBot`, branch `local-intelligence`.
