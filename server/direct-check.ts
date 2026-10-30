import { createDatabase } from "./src/db/client.ts";
import { createLocalIntelligence } from "./src/intelligence/index.ts";
const db = createDatabase(process.env.DATABASE_URL!);
const i = createLocalIntelligence({ database: db, apiKey: "local-intelligence" });
const res = await i.rest.request("/entitlements", { headers: { authorization: "Bearer local-intelligence" } });
console.log("direct hono:", res.status, (await res.text()).slice(0, 80));
const t = await i.rest.request("/threads", { method: "POST", headers: { authorization: "Bearer local-intelligence", "content-type": "application/json" }, body: "{}" });
console.log("create thread:", t.status, (await t.text()).slice(0, 120));
process.exit(0);
