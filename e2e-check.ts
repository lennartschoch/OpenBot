// End-to-end check: REST thread CRUD + join token, Phoenix client relay of a
// live agent run, ingestion persistence, folded transcript.
import { Socket } from "/home/agent/workspace/OpenBot/node_modules/.bun/phoenix@1.8.15/node_modules/phoenix/priv/static/phoenix.mjs";

const REST = "http://localhost:3101/api/intelligence";
const KEY = "local-intelligence";
const USER = "dev-local-user";
const THREAD = `e2e-${Date.now()}`;
const RUN_ID = `e2e-run-${Date.now()}`;

async function rest(path: string, init?: RequestInit) {
  const res = await fetch(`${REST}${path}`, {
    ...init,
    headers: {
      authorization: `Bearer ${KEY}`,
      "content-type": "application/json",
      ...(init?.headers ?? {}),
    },
  });
  const text = await res.text();
  return text ? JSON.parse(text) : null;
}

const client = await rest("/api/threads", {
  method: "POST",
  body: JSON.stringify({
    threadId: THREAD,
    userId: USER,
    agentId: "general-assistant",
  }),
});
if (!client.thread) throw new Error("thread create failed");
const { joinToken } = await rest(
  `/api/threads/${THREAD}/join-code?userId=${USER}`,
);

const socket = new Socket(
  `ws://localhost:3101/api/intelligence-gateway/client?join_token=${joinToken}`,
  {},
);
socket.connect();
await new Promise((r) => setTimeout(r, 700));
const chan = socket.channel(`thread:${THREAD}`, {});
const relayed: string[] = [];
chan.on("ag_ui_event", (p: any) => relayed.push((p.event ?? p).type));
const joined = chan.join();
const ok = await new Promise<boolean>((resolve) => {
  joined.receive("ok", () => resolve(true));
  joined.receive("error", () => resolve(false));
  setTimeout(() => resolve(false), 5000);
});
if (!ok) throw new Error("client join failed");
console.log("1. REST create + join token + Phoenix join: OK");

const run = await fetch(
  "http://localhost:3101/api/copilotkit/agent/general-assistant/run",
  {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      threadId: THREAD,
      runId: RUN_ID,
      state: {},
      messages: [
        {
          id: "m-user",
          role: "user",
          content: "Reply with exactly one short friendly sentence.",
        },
      ],
      tools: [],
      context: [],
      forwardedProps: {},
    }),
  },
);
if (run.status !== 200) throw new Error(`run accepted with ${run.status}`);
// Intelligence mode answers with the realtime connection info, not an event
// stream: events arrive on the Phoenix channel this client already joined.
const ack = (await run.json()) as {
  realtime?: { topic: string; clientUrl: string };
};
if (!ack.realtime?.topic)
  throw new Error(`no realtime info: ${JSON.stringify(ack)}`);
console.log("2. run accepted, realtime topic", ack.realtime.topic);
const finished = await new Promise<boolean>((resolve) => {
  const started = Date.now();
  const poll = setInterval(() => {
    if (relayed.includes("RUN_FINISHED")) {
      clearInterval(poll);
      resolve(true);
    } else if (Date.now() - started > 200_000) {
      clearInterval(poll);
      resolve(false);
    }
  }, 500);
});
if (!finished) throw new Error("run did not finish in budget");

await new Promise((r) => setTimeout(r, 1500));
if (relayed.length === 0)
  throw new Error("no events relayed to the Phoenix client");
console.log(
  "3. events relayed live to the Phoenix client:",
  new Set(relayed).size,
  "types",
);

const stored = await rest(`/api/threads/${THREAD}/messages?userId=${USER}`);
const roles = (stored.messages ?? []).map((m: any) => m.role).join(",");
if (!stored.messages?.length) throw new Error("transcript not persisted");
console.log("4. transcript folded + persisted:", roles);

const inspect = await rest(
  `/api/_inspect/threads/${THREAD}/events?limit=5&sort=desc`,
);
if (!inspect.events?.length) throw new Error("inspect events empty");
console.log(
  "5. event log serves the inspector:",
  inspect.events.length,
  "of latest run",
);

const connect = await rest(`/api/threads/${THREAD}/connect`, {
  method: "POST",
  body: JSON.stringify({ userId: USER, agentId: "general-assistant" }),
});
const idle = typeof connect?.joinToken === "string" && !connect.runId;
console.log(
  "6. lock released after finish:",
  idle ? "OK" : JSON.stringify(connect).slice(0, 80),
);
if (!idle) throw new Error("lock not released");

socket.disconnect();
console.log("\nALL CHECKS PASSED");
process.exit(0);
