import { Socket } from "/home/agent/workspace/OpenBot/node_modules/.bun/phoenix@1.8.15/node_modules/phoenix/priv/static/phoenix.mjs";
const K = { authorization: "Bearer local-intelligence" };
const TID = crypto.randomUUID();
const res = await fetch("http://localhost:3101/api/intelligence/api/trajectories/join", {
  method: "POST", headers: { ...K, "content-type": "application/json" },
  body: JSON.stringify({ trajectoryId: TID, appUserId: "u1" }),
});
const grant = (await res.json()) as { joinToken: string; trajectoryId: string };
console.log("1. join grant:", res.status, grant.trajectoryId === TID ? "trajectoryId echoes" : "MISMATCH");
const cap = new Socket(`ws://localhost:3101/api/intelligence-gateway/client?join_token=${grant.joinToken}`, { authToken: "local-intelligence" });
cap.connect();
const viewer = new Socket(`ws://localhost:3101/api/intelligence-gateway/client?join_token=${grant.joinToken}`, { authToken: "local-intelligence" });
viewer.connect();
await new Promise((r) => setTimeout(r, 700));
const cj = cap.channel(`trajectory:${TID}`, {});
const j1 = cj.join(2000);
await new Promise((r) => (j1.receive("ok", () => { console.log("2. capture joined"); r(0); }), j1.receive("error", (p: any) => { console.log("2. capture join error", JSON.stringify(p)); r(0); })));
const vj = viewer.channel(`trajectory:${TID}`, {});
vj.on("capture_event", (p: any) => { console.log("3. viewer received capture_event:", JSON.stringify(p)); process.exit(0); });
const j2 = vj.join(2000);
await new Promise((r) => (j2.receive("ok", () => r(0)), j2.receive("timeout", () => r(0))));
cj.push("capture_event", { kind: "click", x: 10 });
await new Promise((r) => setTimeout(r, 1500));
console.log("(no relay)"); process.exit(1);
