import type { IntelligenceStore } from "./store";
import { EventType } from "@ag-ui/core";

/**
 * The realtime gateway: a Phoenix-protocol WebSocket server speaking the same
 * wire format as CopilotKit's hosted gateway, so `@copilotkit/runtime`'s
 * `IntelligenceAgentRunner` (the runner plane) and `@copilotkit/core` (the
 * client plane) connect to it unchanged.
 *
 * Phoenix's wire frame is a JSON array `[join_ref, ref, topic, event, payload]`,
 * joined channels are strings, and every client push is answered with one
 * `phx_reply`. That is the whole protocol this implements; Presence and Tracking
 * are not used by the SDK.
 *
 * Single-instance by design: the fan-out is one in-process registry. Durable
 * state is the event log the runner appends to; a socket that reconnects replays
 * from there.
 */

/** The slice of a Bun server websocket the gateway uses. */
export type GatewaySocket = {
  data: PhoenixData;
  send: (data: string) => void;
  close: (code?: number, reason?: string) => void;
};

/** The per-socket state an upgraded Phoenix connection carries. */
export type PhoenixData = {
  phoenix: "runner" | "client" | "channels";
  joined: Map<string, string | null>;
  /** The socket-level `join_token` the Phoenix client puts on the connect URL. */
  joinToken?: string;
  /** The runner plane's bearer key, carried as the socket's `token` param. */
  token?: string;
};
type SocketData = PhoenixData;

const RUNNER_EVENT_BATCH_CAPABILITY = "runner_event_batch_v1";
const STOP_RUN_EVENT = "stop_run";
const REPLAY_COMPLETE_EVENT = "replay_complete";
const REPLAY_FAILED_EVENT = "replay_failed";
const STREAM_IDLE_EVENT = "stream_idle";
const AG_UI_EVENT = "ag_ui_event";

export class IntelligenceGateway {
  private readonly subscribers = new Map<string, Set<GatewaySocket>>();

  constructor(private readonly store: IntelligenceStore) {}

  /** Build the socket state for a fresh upgrade, from its connect params. */
  socketData(plane: PhoenixData["phoenix"], request: Request): PhoenixData {
    const url = new URL(request.url);
    // Phoenix's transport appends `/websocket` to the connect URL, and a query
    // param written before it can carry the suffix into its value.
    const clean = (value: string | null) =>
      value ? value.replace(/\/websocket$/, "") : undefined;
    return {
      phoenix: plane,
      joined: new Map(),
      joinToken: clean(url.searchParams.get("join_token")),
      token: clean(url.searchParams.get("token")),
    };
  }

  /** One inbound frame. */
  handle(ws: GatewaySocket, raw: string | Buffer): void {
    const message = parseFrame(raw);
    if (!message) return;
    const { joinRef, ref, topic, event, payload } = message;
    const data = ws.data as SocketData;
    if (process.env.INTELLIGENCE_GATEWAY_DEBUG)
      console.log(`[gateway] ${data.phoenix} ${topic} -> ${event}`);
    switch (event) {
      case "phx_join":
        void this.join(ws, topic, joinRef, ref, payload);
        break;
      case "phx_leave":
        data.joined.delete(topic);
        this.subscribers.get(topic)?.delete(ws);
        reply(ws, joinRef, ref, topic, {});
        break;
      case "heartbeat":
        reply(ws, joinRef, ref, topic, {});
        break;
      case "event":
      case "events":
        void this.ingest(ws, topic, ref, payload);
        break;
      case STOP_RUN_EVENT:
        this.relayStop(topic, payload);
        reply(ws, joinRef, ref, topic, {});
        break;
      default:
        reply(ws, joinRef, ref, topic, {});
        break;
    }
  }

  handleClose(ws: GatewaySocket): void {
    const data = ws.data as SocketData;
    for (const topic of data.joined.keys()) {
      this.subscribers.get(topic)?.delete(ws);
    }
    data.joined.clear();
  }

  /** Push one event to every socket on a topic (the channel broadcast). */
  broadcast(topic: string, event: string, payload: unknown): void {
    const sockets = this.subscribers.get(topic);
    if (!sockets) return;
    const frame = JSON.stringify([null, null, topic, event, payload]);
    for (const socket of sockets) socket.send(frame);
  }

  /**
   * Push one event to this person's meta channels: the thread-list channel
   * (`user_meta:{joinCode}`) and the memories channel
   * (`user_meta:memories:{joinCode}`). Join codes are the minted tokens, so the
   * token table is the channel registry.
   */
  async broadcastUser(
    userId: string,
    channel: "threads" | "memories",
    event: string,
    payload: unknown,
  ): Promise<void> {
    const tokens = await this.store.liveTokens(
      userId,
      channel === "threads" ? "user_threads" : "user_memories",
    );
    for (const token of tokens) {
      const topic =
        channel === "threads" ? `user_meta:${token}` : `user_meta:memories:${token}`;
      this.broadcast(topic, event, payload);
    }
  }

  // ---- joins --------------------------------------------------------------

  private async join(
    ws: GatewaySocket,
    topic: string,
    joinRef: string | null,
    ref: string | null,
    params: Record<string, unknown>,
  ): Promise<void> {
    const data = ws.data as SocketData;
    if (process.env.INTELLIGENCE_GATEWAY_DEBUG)
      console.log(
        `[gateway] join ${topic} plane=${data.phoenix} token=${data.joinToken ?? "none"}`,
      );
    // Authorize: runner joins need no per-thread token (the socket's bearer
    // key is the deployment's); client joins carry the minted join token, put
    // on the socket URL by the Phoenix client's connect params.
    if (data.phoenix === "client" && topic.startsWith("thread:")) {
      const token = data.joinToken;
      const authorized = await this.store.authorizeTopic(
        typeof token === "string" ? token : null,
        topic,
      );
      if (!authorized.ok) {
        replyError(ws, joinRef, ref, topic, { reason: "unauthorized" });
        return;
      }
    }
    // The meta channels (thread list, memories) join with the socket's token too.
    if (data.phoenix === "client" && topic.startsWith("user_meta:")) {
      const token = data.joinToken;
      if (typeof token !== "string" || !(await this.store.authorizeTopic(token, topic)).ok) {
        replyError(ws, joinRef, ref, topic, { reason: "unauthorized" });
        return;
      }
    }
    data.joined.set(topic, joinRef);
    const set = this.subscribers.get(topic) ?? new Set<GatewaySocket>();
    set.add(ws);
    this.subscribers.set(topic, set);

    // The runner's ingestion channel negotiates the event-batch capability;
    // every plane answers the join, and replies carry the platform's shape.
    const response: Record<string, unknown> =
      topic.startsWith("ingestion:")
        ? { capabilities: [RUNNER_EVENT_BATCH_CAPABILITY] }
        : {};
    reply(ws, joinRef, ref, topic, response);

    // A thread join replays the events after the caller's cursor, then says so
    // (and idles the stream when no run is active to fill it).
    if (topic.startsWith("thread:")) {
      await this.replay(ws, topic.slice("thread:".length), params);
    }
  }

  /**
   * Replay the events a joining client has not seen, then the control events:
   * `replay_complete` closes a client-initiated stream, `stream_idle` tells a
   * connect-mode subscriber there is no active run to wait for.
   */
  private async replay(
    ws: GatewaySocket,
    threadId: string,
    params: Record<string, unknown>,
  ): Promise<void> {
    const cursor =
      typeof params.last_seen_event_id === "string"
        ? params.last_seen_event_id
        : null;
    const rows = await this.store.listEventsAfter(threadId, cursor);
    for (const row of rows) {
      send(ws, `thread:${threadId}`, AG_UI_EVENT, row);
    }
    const latest = await this.store.latestEvent(threadId);
    const frame = JSON.stringify([
      null,
      null,
      `thread:${threadId}`,
      REPLAY_COMPLETE_EVENT,
      latest ? { latest_event_id: latest.cpkiEventId } : {},
    ]);
    ws.send(frame);
    if (params.stream_mode === "connect" && !latest?.running) {
      const idle = JSON.stringify([
        null,
        null,
        `thread:${threadId}`,
        STREAM_IDLE_EVENT,
        latest ? { latest_event_id: latest.cpkiEventId } : {},
      ]);
      ws.send(idle);
    }
  }

  // ---- runner events ------------------------------------------------------

  /**
   * One `event`/`events` push from the runner on its ingestion channel.
   *
   * The payload is an AG-UI event stamped with `threadId`/`runId` and
   * `metadata.cpki_event_id`/`cpki_event_seq`; the run identity comes from the
   * channel join params the platform keeps per connection.
   */
  private async ingest(
    ws: GatewaySocket,
    ingestionTopic: string,
    ref: string | null,
    payload: Record<string, unknown>,
  ): Promise<void> {
    const data = ws.data as SocketData;
    const joinRef = data.joined.get(ingestionTopic) ?? null;
    const runId = ingestionTopic.slice("ingestion:".length);
    const events = Array.isArray(payload.events)
      ? (payload.events as Record<string, unknown>[])
      : [payload];
    const threadId = threadIdOf(events[0]);
    if (!threadId) {
      replyError(ws, joinRef, ref, ingestionTopic, { reason: "invalid_batch" });
      return;
    }
    const stamped = events.map((event) => ({
      payload: event,
      seq: seqOf(event),
    }));
    const { events: stored } = await this.store.appendEvents(
      threadId,
      runId,
      stamped,
    );
    for (const event of stored) {
      this.broadcast(`thread:${threadId}`, AG_UI_EVENT, event);
    }
    const finished = stored.some((event) => event.type === EventType.RUN_FINISHED);
    if (finished) await this.store.markRunFinished(threadId, runId);
    reply(ws, joinRef, ref, ingestionTopic, {});
  }

  /**
   * A `stop_run` from the browser, relayed to the runner.
   *
   * The hosted gateway hands the runner an AG-UI `CUSTOM` event named `stop` on
   * its ingestion channel; the runner stops the agent, and its terminal events
   * come back through the event push.
   */
  private relayStop(threadTopic: string, payload: Record<string, unknown>): void {
    const threadId = threadTopic.slice("thread:".length);
    this.broadcast(`ingestion:${String(payload.run_id ?? payload.runId ?? "")}`, AG_UI_EVENT, {
      type: "CUSTOM",
      name: "stop",
      timestamp: Date.now(),
      threadId,
    });
  }
}

// ---- Phoenix frames ---------------------------------------------------------

type Frame = {
  joinRef: string | null;
  ref: string | null;
  topic: string;
  event: string;
  payload: Record<string, unknown>;
};

function parseFrame(raw: string | Buffer): Frame | null {
  let parsed: unknown;
  try {
    parsed = JSON.parse(typeof raw === "string" ? raw : raw.toString("utf8"));
  } catch {
    return null;
  }
  if (!Array.isArray(parsed) || parsed.length < 5) return null;
  const [joinRef, ref, topic, event, payload] = parsed;
  if (typeof topic !== "string" || typeof event !== "string") return null;
  return {
    joinRef: typeof joinRef === "string" ? joinRef : null,
    ref: typeof ref === "string" ? ref : null,
    topic,
    event,
    payload: (payload ?? {}) as Record<string, unknown>,
  };
}

function send(ws: GatewaySocket, topic: string, event: string, payload: unknown): void {
  ws.send(JSON.stringify([null, null, topic, event, payload]));
}

function reply(
  ws: GatewaySocket,
  joinRef: string | null,
  ref: string | null,
  topic: string,
  response: Record<string, unknown>,
): void {
  ws.send(
    JSON.stringify([joinRef, ref, topic, "phx_reply", { status: "ok", response }]),
  );
}

function replyError(
  ws: GatewaySocket,
  joinRef: string | null,
  ref: string | null,
  topic: string,
  response: Record<string, unknown>,
): void {
  ws.send(
    JSON.stringify([joinRef, ref, topic, "phx_reply", { status: "error", response }]),
  );
}

const seqOf = (event: Record<string, unknown>): number => {
  const metadata = event.metadata as { cpki_event_seq?: number } | undefined;
  return typeof metadata?.cpki_event_seq === "number" ? metadata.cpki_event_seq : 0;
};

const threadIdOf = (event: Record<string, unknown>): string | null => {
  const threadId = event.threadId ?? event.thread_id;
  return typeof threadId === "string" && threadId ? threadId : null;
};
