import Typesense from "typesense";

import type { AppConfig } from "../../core/types.js";

export function createTypesenseClient(config: AppConfig) {
  return new Typesense.Client({
    nodes: [
      {
        host: config.typesense.host,
        port: config.typesense.port,
        protocol: config.typesense.protocol
      }
    ],
    apiKey: config.typesense.apiKey,
    connectionTimeoutSeconds: 10
  });
}