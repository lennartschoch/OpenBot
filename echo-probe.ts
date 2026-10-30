import { Socket } from "/home/agent/workspace/OpenBot/node_modules/.bun/phoenix@1.8.15/node_modules/phoenix/priv/static/phoenix.mjs";
const socket = new Socket("ws://localhost:3101/api/intelligence-gateway/runner", { authToken: "local-intelligence" });
socket.connect();
await new Promise((r) => setTimeout(r, 700));
const chan = socket.channel("ingestion:echo-run-1", { thread_id: "echo-1", run_id: "echo-run-1" });
chan.on("ag-ui", (p: any) => console.log("echo ag-ui:", p.type));
const j = chan.join();
await new Promise((r) => (j.receive("ok", () => r(0)), j.receive("timeout", () => r(0))));
// push + expect echo
chan.push("events", { threadId: "echo-1", events: [{ type: "RUN_STARTED", threadId: "echo-1", runId: "echo-run-1", timestamp: 1, rawEvent: null, metadata: { cpki_event_seq: 1 } }] }, 4000);
await new Promise((r) => setTimeout(r, 1500));
socket.disconnect();
process.exit(0);
