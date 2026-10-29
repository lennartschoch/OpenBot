import { Socket } from "/home/agent/workspace/OpenBot/node_modules/.bun/phoenix@1.8.15/node_modules/phoenix/priv/static/phoenix.mjs";

const REST = "http://localhost:3101/api/intelligence";
const KEY = "local-intelligence";
const THREAD = `ql-${Date.now()}`;
const RUN_ID = `ql-run-${Date.now()}`;

async function rest(path: string, init?: RequestInit) {
  const res = await fetch(`${REST}${path}`, {
    ...init,
    headers: { authorization: `Bearer ${KEY}`, "content-type": "application/json", ...(init?.headers ?? {}) },
  });
  const t = await res.text();
  return t ? JSON.parse(t) : null;
}

// Create the thread first, then mint a join token for it.
await rest("/api/threads", { method: "POST", body: JSON.stringify({ threadId: THREAD, userId: "u-ql", agentId: "general-assistant" }) });
const { joinToken } = await rest(`/api/threads/${THREAD}/join-code`);

const socket = new Socket(`ws://localhost:3101/api/intelligence-gateway/client?join_token=${joinToken}`, {});
socket.connect();
await new Promise((r) => setTimeout(r, 600));
const chan = socket.channel(`thread:${THREAD}`, {});
const seen: string[] = [];
chan.on("event", (p: any) => {
  const e = p?.event ?? p;
  seen.push(e.type);
  console.log("client event:", e.type);
});
chan.on("events", (p: any) => {
  for (const e of p.events ?? []) {
    seen.push(e.type);
    console.log("client batch:", e.type);
  }
});
chan.join();
await new Promise((r) => setTimeout(r, 300));

console.log("starting run on", THREAD);
const run = await fetch("http://localhost:3101/api/copilotkit/agent/general-assistant/run", {
  method: "POST",
  headers: { "content-type": "application/json" },
  body: JSON.stringify({
    threadId: THREAD,
    runId: RUN_ID,
    state: {},
    messages: [{ id: "m-user", role: "user", content: "Reply with exactly one short friendly sentence." }],
    tools: [],
    context: [],
    forwardedProps: {},
  }),
});
console.log("run http:", run.status);
const reader = run.body!.getReader();
const dec = new TextDecoder();
let buf = "";
let finished = false;
const deadline = Date.now() + 90_000;
while (!finished && Date.now() < deadline) {
  const { value, done } = await reader.read();
  if (done) break;
  buf += dec.decode(value, { stream: true });
  if (buf.includes("RUN_FINISHED")) finished = true;
  if (buf.includes('"type":"RUN_ERROR"')) {
    console.log("RUN_ERROR:", buf.slice(0, 400));
    break;
  }
}
console.log("run finished:", finished, "sse bytes:", buf.length);
await new Promise((r) => setTimeout(r, 2000));
console.log("client saw", seen.length, "events:", [...new Set(seen)].join(","));
const folded = await rest(`/api/threads/${THREAD}/messages?limit=20&sort=asc`);
console.log("folded:", JSON.stringify(folded.messages ?? folded).slice(0, 400));
process.exit(0);
