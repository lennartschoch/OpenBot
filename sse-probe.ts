const run = await fetch("http://localhost:3101/api/copilotkit/agent/general-assistant/run", {
  method: "POST", headers: { "content-type": "application/json" },
  body: JSON.stringify({ threadId: "sse-probe-1", runId: "sse-probe-run-1", state: {}, messages: [{ id: "m1", role: "user", content: "Reply with one short friendly sentence." }], tools: [], context: [], forwardedProps: {} }),
});
console.log("status:", run.status);
const reader = run.body!.getReader();
const dec = new TextDecoder();
let buf = "";
const t0 = Date.now();
for (;;) {
  const { value, done } = await reader.read();
  if (done) break;
  buf += dec.decode(value, { stream: true });
  console.log(`+${Date.now() - t0}ms chunk ${value?.length}B total ${buf.length}`);
  if (buf.includes("RUN_FINISHED")) break;
}
console.log("stream text (first 400):", buf.slice(0, 400).replace(/\n/g, " "));
process.exit(0);
