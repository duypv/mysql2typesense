import type { Logger } from "pino";
import { createClient } from "redis";
import type { Server } from "node:http";

import { loadConfig } from "../config/env.js";
import { createLogger } from "../config/logger.js";
import type { AppConfig, CheckpointStore, TableSyncConfig } from "../core/types.js";
import { FileCheckpointStore } from "../modules/checkpoint/file-checkpoint-store.js";
import { RedisCheckpointStore } from "../modules/checkpoint/redis-checkpoint-store.js";
import { MySqlBinlogListener } from "../modules/mysql/binlog-listener.js";
import { createMysqlPool } from "../modules/mysql/connection.js";
import { MysqlSchemaIntrospector } from "../modules/mysql/schema-introspector.js";
import { InMemorySyncMonitor } from "../modules/monitoring/sync-monitor.js";
import { MySqlSourceReader } from "../modules/mysql/source-reader.js";
import { InitialSyncService } from "../modules/sync/initial-sync.service.js";
import { RealtimeSyncService } from "../modules/sync/realtime-sync.service.js";
import { resolveTableConfigs } from "../modules/sync/table-config-resolver.js";
import { ConfigDrivenTransformer } from "../modules/transform/transformer.js";
import { createTypesenseClient } from "../modules/typesense/client.js";
import { TypesenseCollectionManager } from "../modules/typesense/collection-manager.js";
import { TypesenseDocumentIndexer } from "../modules/typesense/document-indexer.js";
import { startMonitoringServer } from "./monitoring-server.js";

export interface AppContext {
  config: AppConfig;
  tables: TableSyncConfig[];
  logger: Logger;
  initialSyncService: InitialSyncService;
  realtimeSyncService: RealtimeSyncService;
  monitor: InMemorySyncMonitor;
  dispose(): Promise<void>;
}

async function createCheckpointStore(config: AppConfig): Promise<CheckpointStore> {
  if (config.checkpoint.driver === "file") {
    return new FileCheckpointStore(config.checkpoint.filePath ?? "storage/checkpoints/binlog.json");
  }

  const client = createClient({ url: config.checkpoint.redisUrl });
  await client.connect();
  return new RedisCheckpointStore(client, config.checkpoint.redisKey ?? "mysql2typesense:binlog");
}

export async function bootstrap(): Promise<AppContext> {
  const config = loadConfig();
  const logger = createLogger(config.logLevel);
  const mysqlPool = createMysqlPool(config);
  const sourceReader = new MySqlSourceReader(mysqlPool);
  const introspector = new MysqlSchemaIntrospector(mysqlPool);
  const resolvedTables = await resolveTableConfigs(introspector, config.mysql.database, config.sync.tables);
  const monitor = new InMemorySyncMonitor();
  monitor.setTables(resolvedTables);

  const checkpointStore = await createCheckpointStore(config);
  const typesenseClient = createTypesenseClient(config);
  const collectionManager = new TypesenseCollectionManager(typesenseClient);
  const documentIndexer = new TypesenseDocumentIndexer(typesenseClient);
  const transformer = new ConfigDrivenTransformer();
  const binlogListener = new MySqlBinlogListener(config, resolvedTables, checkpointStore, logger);
  const reindexInFlight = new Set<string>();
  let monitoringServer: Server | null = null;

  const initialSyncService = new InitialSyncService(
    sourceReader,
    collectionManager,
    documentIndexer,
    transformer,
    config.sync.batchSize,
    config.sync.retry,
    logger,
    monitor
  );

  if (config.monitoring.enabled) {
    monitoringServer = startMonitoringServer({
      host: config.monitoring.host,
      port: config.monitoring.port,
      logger,
      monitor,
      typesenseClient,
      authToken: config.monitoring.authToken,
      reindexCollection: async (collectionName) => {
        const table = resolvedTables.find((item) => item.collection === collectionName);
        if (!table) {
          return { ok: false, reason: "Collection is not mapped to any table" };
        }
        if (reindexInFlight.has(collectionName)) {
          return { ok: false, reason: "Reindex already in progress" };
        }

        reindexInFlight.add(collectionName);
        try {
          await initialSyncService.run([table]);
          return { ok: true };
        } catch (error) {
          monitor.recordError(error, `reindex:${collectionName}`);
          return {
            ok: false,
            reason: error instanceof Error ? error.message : String(error)
          };
        } finally {
          reindexInFlight.delete(collectionName);
        }
      }
    });
  }

  return {
    config,
    tables: resolvedTables,
    logger,
    monitor,
    initialSyncService,
    realtimeSyncService: new RealtimeSyncService(
      binlogListener,
      collectionManager,
      documentIndexer,
      transformer,
      checkpointStore,
      config.sync.retry,
      logger,
      monitor
    ),
    async dispose() {
      monitor.markMode("idle");
      await mysqlPool.end();
      await binlogListener.stop();
      await checkpointStore.close();
      if (monitoringServer) {
        await new Promise<void>((resolve) => {
          monitoringServer?.close(() => resolve());
        });
      }
    }
  };
}