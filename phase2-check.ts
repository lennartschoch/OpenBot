/**
 * Phase-2 smoke: the channels plane relay, Trajectory capture join + relay,
 * and managed-channel files, against a running server (default :3101).
 */
import { Socket } from "/home/agent/workspace/OpenBot/node_modules/.bun/phoenix@1.8.15/node_modules/phoenix/priv/static/phoenix.mjs";
const BASE = process.env.OPENBOT_URL ?? "http://localhost:3101";
const KEY = { authorization: "Bearer local-intelligence" };

// 1+2. channels plane: two sockets relay on the shared `control` topic.
const GW = BASE.replace("http", "ws") + "/api/intelligence-gateway/channels";
const a = new Socket(GW, { authToken: "local-intelligence" });
const b = new Socket(GW, { authToken: "local-intelligence" });
a.connect(); b.connect();
await new Promise((r) => setTimeout(r, 800));
const ca = a.channel("control", { channels: [{ name: "demo" }], runtimeInstanceId: "r1" });
const j1 = ca.join(3000);
await new Promise((r) => (j1.receive("ok", () => r(0)), j1.receive("error", (p: any) => { throw new Error(`control join: ${JSON.stringify(p)}`); })));
const cb = b.channel("control", {});
const j2 = cb.join(3000);
await new Promise((r) => (j2.receive("ok", () => r(0)), j2.receive("timeout", () => r(0))));
const relayed = new Promise<void>((resolve, reject) => {
  cb.on("delivery_invitation", () => resolve());
  setTimeout(() => reject(new Error("control relay silent")), 5000);
});
ca.push("delivery_invitation", { deliveryId: "dlv-check" });
await relayed;
console.log("1. channels plane relays control events: OK");
a.disconnect(); b.disconnect();

// 3. Trajectory join grant + live relay to a second subscriber.
const TID = crypto.randomUUID();
const grant = (await (
  await fetch(`${BASE}/api/intelligence/api/trajectories/join`, {
    method: "POST", headers: { ...KEY, "content-type": "application/json" },
    body: JSON.stringify({ trajectoryId: TID, appUserId: "u-check" }),
  })
).json()) as { joinToken: string; trajectoryId: string };
if (grant.trajectoryId !== TID || !grant.joinToken) throw new Error("bad grant");
const CW = BASE.replace("http", "ws") + "/api/intelligence-gateway/client";
const cap = new Socket(`${CW}?join_token=${grant.joinToken}`, { authToken: "local-intelligence" });
const view = new Socket(`${CW}?join_token=${grant.joinToken}`, { authToken: "local-intelligence" });
cap.connect(); view.connect();
await new Promise((r) => setTimeout(r, 800));
const capture = cap.channel(`trajectory:${TID}`, {});
const j3 = capture.join(3000);
await new Promise((r) => (j3.receive("ok", () => r(0)), j3.receive("error", (p: any) => { throw new Error(`trajectory join: ${JSON.stringify(p)}`); })));
const viewer = view.channel(`trajectory:${TID}`, {});
const streamed = new Promise<void>((resolve, reject) => {
  viewer.on("capture_event", () => resolve());
  setTimeout(() => reject(new Error("trajectory relay silent")), 5000);
});
const j4 = viewer.join(3000);
await new Promise((r) => (j4.receive("ok", () => r(0)), j4.receive("timeout", () => r(0))));
capture.push("capture_event", { kind: "click" });
await streamed;
console.log("2. trajectory join grants, events relay live: OK");
cap.disconnect(); view.disconnect();

// 4. Managed-channel file: upload a delivery file, read the asset back.
const upload = (await (
  await fetch(`${BASE}/api/intelligence/api/channels/deliveries/dlv-check/files?operationId=op-check&filename=chart.png`, {
    method: "POST", headers: { ...KEY, "content-type": "image/png" },
    body: "PNG-check-bytes",
  })
).json()) as { handle: string };
if (!upload.handle) throw new Error("upload returned no handle");
const asset = await fetch(`${BASE}/api/intelligence/api/channels/files/${upload.handle}`, { headers: KEY });
const text = await asset.text();
if (!asset.ok || text !== "PNG-check-bytes" || !asset.headers.get("content-type")?.startsWith("image/png"))
  throw new Error(`asset readback failed: ${asset.status} ${text}`);
console.log("3. channel files: upload + asset hydration round-trip: OK");
console.log("\nALL PHASE-2 CHECKS PASSED");
process.exit(0);
