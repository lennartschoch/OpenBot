# The Intelligence platform

Threads, message history, memories, Learning containers and the realtime gateway used to be three
CopilotKit-hosted services. They run inside this server instead — one process, the same Postgres,
speaking the same wire protocol, so `@copilotkit/runtime` and `@copilotkit/core` talk to it without
knowing the difference.

Nothing here is a separate service: the REST surface is mounted on the API server under
`/api/intelligence` and the realtime planes under `/api/intelligence-gateway/{runner,client,channels}`.
`INTELLIGENCE_API_URL` and `INTELLIGENCE_GATEWAY_WS_URL` default to those addresses (see
[Configuration](configuration.md)); naming them points the runtime at managed Intelligence, or at
your own copy of the hosted service, and this one stays mounted but unused.

**Deployment assumption: single instance.** One server process, one Postgres. The realtime fan-out is
in-process — no Redis, no multi-replica broadcast, no presence tracking. That is the shape a laptop
deployment has, and it is what this is for.

The wire contract below was verified against `@copilotkit/runtime` 1.83.0 and `@copilotkit/core`
1.83.0. Those packages are the client: when they change, this changes with them.

## REST

Every request presents `Authorization: Bearer <platform key>`. Thread routes take `userId` as a query
parameter, memory routes take it as the `x-cpki-user-id` header — the hosted platform's split, not an
invention here.

| Endpoint | Contract |
| --- | --- |
| `GET /api/threads?userId&agentId&includeArchived&limit&cursor` | `{threads, joinCode, nextCursor}` |
| `POST /api/threads` `{threadId,userId,agentId,name?,learningContainerId?}` | 201 `{thread}`; 409 on a duplicate |
| `GET /api/threads/:id?userId` | `{thread}`; 404 when absent |
| `PATCH /api/threads/:id` `{userId,agentId?,name?,archived?}` | `{thread}` |
| `DELETE /api/threads/:id` `{userId}` | `{deleted:{id}}` |
| `GET /api/threads/:id/messages?userId` (`?fields=id`) | `{messages}`, already folded |
| `POST /api/threads/:id/lock` `{runId,userId,agentId,learningContainerId?}` | `{threadId,runId,joinToken,ttlSeconds}`; 409 when another run holds it |
| `PATCH /api/threads/:id/lock` `{runId}` (renew), `DELETE` (release) | `{ttlSeconds,status}` / `{released}` |
| `GET /api/threads/:id/join-code?userId` | `{threadId,joinToken,runId?}` |
| `POST /api/threads/:id/connect` `{userId,agentId}` | `{threadId,joinToken,runId?}` |
| `POST /api/threads/subscribe` `{userId}` | `{joinToken}` for `user_meta:{joinCode}` |
| `GET/POST /api/memories`, `PATCH/DELETE /api/memories/:id` | `{memories}` / `MemorySummary` |
| `POST /api/memories/recall` `{query,limit?,scope?}` | `{memories}` with `score` |
| `POST /api/memories/subscribe` | `{joinToken,joinCode}` for `user_meta:memories:{joinCode}` |
| `GET /api/entitlements/runtime` | `{status:"ready",entitlement:{active:true}}` |
| `GET /api/inspector/metadata` | 204 — the runtime treats a 204 as "no metadata", which is the answer here |
| `GET /api/inspector/learning` | a versioned snapshot with no engine behind it |
| `GET /api/_inspect/threads/:id/events\|state` | one page of the event log, and the folded state |
| `PUT /connector/annotate/:clientEventId` | `{id,duplicate:false}`; nothing is recorded |
| `POST /api/trajectories/join` `{trajectoryId}` | `{joinToken,trajectoryId}` |
| `GET /api/channels/files/:assetId` | the stored bytes, by uuid handle |
| `POST /api/channels/deliveries/:deliveryId/files` | `{handle}` |
| `GET /api/v1/learning/containers`, `GET /api/v1/learning/containers/:id` | containers |
| `GET/PUT /api/v1/learning/containers/:id/skills` | the Skills ZIP, with `ETag` + `If-None-Match` |
| `POST /api/v1/learning/skills/batch` | several snapshots, unchanged answers or per-container errors |
| `POST /api/learning/projects/:projectId/containers` | the project service token's one route |

Ownership is part of the contract: another person's thread answers 404 on every route — the same
answer as a thread that does not exist — and `lock` and `connect` create the thread for the caller on
first use, because the browser's first message can arrive before anything else has made the row.

## Realtime

Phoenix channel framing — a frame is `[join_ref, ref, topic, event, payload]`, a joined channel is a
string, and every client push is answered with one `phx_reply`. Nothing else of Phoenix is
implemented: no Presence, no Tracking.

- `/runner` — the runtime's `IntelligenceAgentRunner`. Presents the platform key as a
  `base64url.bearer.phx.…` subprotocol and may join `ingestion:*` only. The join reply carries
  `capabilities:["runner_event_batch_v1"]`. Event pushes (`events`, or a single `event`) are appended
  and acked; the ack is the runner's durability signal, and a push it does not receive sends the batch
  again. The batch is idempotent on `(run_id, seq)` and folds into the thread's transcript in one
  transaction under the thread's row lock.
- `/client` — the browser. Presents a single-use join token in the connect URL and may join
  `thread:*`, `trajectory:*` and `user_meta:*`. A `thread:` join names a `stream_mode`:
  **connect** replays the persisted events after `last_seen_event_id` (paged to the end of the log,
  not to one page), then sends `replay_complete {latestEventId}` and, when no run is active,
  `stream_idle`. **run** replays nothing — a historical `RUN_FINISHED` would end the run that is
  starting — and is never sent `stream_idle`, which would end that stream too. When a terminal event
  arrives, connect-mode subscribers get `stream_idle` and the user's meta channel gets
  `thread_run_activity`.
- `/channels` — the live-screen relay. Presents the platform key; relays control and delivery frames
  between the members of a topic and stores nothing.

Join tokens are single-use and name one topic. The gateway remembers what a socket already joined, so
a Phoenix rejoin on the same socket works after its token row is gone. Each person's `joinCode` is
stable, so the thread-list channel has an address that does not change on every page load.

## Storage

Nine tables, created by `0070_local_intelligence`:

- `intelligence_threads` — the summary fields and the folded transcript. The event log is the source;
  `GET …/messages` is one row read because the fold happens at append time.
- `intelligence_thread_events` — one row per event, unique on `(run_id, seq)`. The replay and
  Inspector source. Indexed on the `cpki_event_id` cursor replay resolves.
- `intelligence_thread_locks` — one row per thread, with the TTL the runner renews. Acquisition is a
  single `insert … on conflict do update … where` so two runs cannot both read "no lock".
- `intelligence_join_tokens` — single-use grants with an expiry, minted on connect, join-code and
  subscribe. Pruned on every mint.
- `intelligence_user_join_codes` — one stable meta-channel suffix per person.
- `intelligence_memories` — user-scoped rows; nothing is deleted, a supersede retires the old row.
- `intelligence_learning_containers` — the container registry, `openbot` seeded by the migration.
- `intelligence_skill_revisions` — published Skills archives, keyed on their declared revision.
- `intelligence_channel_files` — managed-channel attachments, by uuid handle.

There are no foreign keys between them: single-instance and one owning process, so the checks live in
the store where they can answer with a reason (`appendEvents` refuses a batch for a thread that is not
there; `publishSkillRevision` refuses a container that is not there) rather than in the database's
error text.

## Credentials

The REST surface's credential is the platform key, compared exactly. It is a credential, not a
default: behind it every thread and memory route trusts `x-cpki-user-id`, so one key covers every
person's conversations. `.env.example` ships a placeholder so a clone runs, and a deployment with a
sign-in provider **refuses to start** with it — see [Configuration](configuration.md).

`INTELLIGENCE_SERVICE_TOKEN` is a second credential, accepted for exactly one route: creating a
Learning container, which the setup job does on an installation with no platform key. It
authenticates nothing else.

The memory routes additionally honour `x-cpki-memory-grant`, and a scope the grant does not name is
refused with 403 — a caller holding a read grant for a person's own memory cannot read the project's.

## What it does not do

Worth knowing before relying on it:

- **The Learning engine.** Containers and Skill snapshots are served, so published skills work.
  `POST .../runs` answers `LEARNING_ENGINE_UNAVAILABLE`, and the runs and candidates pages are empty.
- **Semantic memory recall.** Recall is lexical — Postgres full text plus a substring pass for short
  queries — and near-duplicate absorption compares normalised text rather than embeddings.
- **Trajectory persistence.** `/trajectories/join` mints the token and the `trajectory:` topic relays,
  but the events are not stored.
- **Annotation analytics.** `PUT /connector/annotate/:id` accepts and records nothing.
- **The hosted error envelope and tombstones.** Errors are `{error:{code,retryable}}`; the hosted
  `message`/`category`/`requestId` envelope is not reproduced, and a deleted thread id is not
  tombstoned.
- **More than one instance.** See the top of this page.

## Where the code is

`server/src/intelligence/`: `routes.ts` (REST), `gateway.ts` (Phoenix), `store.ts` (the tables),
`messages.ts` (the transcript fold), `state.ts` (the Inspector's state fold). Mounted by
`server/src/http-server.ts`, packaged by `server/src/intelligence/index.ts`. Covered by
`server/tests/intelligence-platform.integration.test.ts` and `server/tests/intelligence-state.test.ts`.
