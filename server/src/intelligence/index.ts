import type { Database } from "../db/client";
import { IntelligenceGateway } from "./gateway";
import { createIntelligenceRestRoutes } from "./routes";
import { IntelligenceStore } from "./store";

/**
 * The local Intelligence platform: the REST surface, the Phoenix realtime
 * gateway, and the Postgres behind both. `config.runtime.intelligence` points
 * the runtime's platform client at this (REST under `/api/intelligence`, the
 * gateway planes under `/api/intelligence-gateway/{runner,client,channels}`),
 * and nothing else in OpenBot knows the difference.
 */
export function createLocalIntelligence(deps: {
  database: Database;
  apiKey: string;
}) {
  const store = new IntelligenceStore(deps.database);
  const gateway = new IntelligenceGateway(store);
  const rest = createIntelligenceRestRoutes({
    store,
    apiKey: deps.apiKey,
    meta: {
      threads: (userId, payload) => {
        void gateway.broadcastUser(userId, "threads", "thread_metadata", payload);
      },
      memories: (userId, payload) => {
        void gateway.broadcastUser(
          userId,
          "memories",
          "memory_metadata",
          payload,
        );
      },
    },
  });
  return { rest, gateway, store };
}

export { IntelligenceGateway } from "./gateway";
export { IntelligenceStore } from "./store";
