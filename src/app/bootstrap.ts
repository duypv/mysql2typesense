import type { Logger } from "pino";
import { createClient } from "redis";
import type { Server } from "node:http";

import { loadConfig } from "../config/env.js";
import { createLogger } from "../config/logger.js";
import type { AppConfig, BinlogCheckpoint, CheckpointStore, TableSyncConfig } from "../core/types.js";
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
  const resolvedTables = await resolveTableConfigs(
    introspector,
    config.mysql.database,
    config.sync.tables,
    config.sync.database
  );
  const startupDiscoveredTables = new Set(resolvedTables.map((table) => `${table.database}.${table.table}`));
  const runtimeDiscoveredTables = new Set<string>();
  const monitor = new InMemorySyncMonitor();
  monitor.setTables(resolvedTables);

  const checkpointStore = await createCheckpointStore(config);
  const typesenseClient = createTypesenseClient(config);
  const collectionManager = new TypesenseCollectionManager(typesenseClient);
  const documentIndexer = new TypesenseDocumentIndexer(typesenseClient);
  const transformer = new ConfigDrivenTransformer();
  const binlogListener = new MySqlBinlogListener(config, resolvedTables, checkpointStore, logger);
  const reindexInFlight = new Set<string>();
  const autoDatabaseMode = Boolean(config.sync.database && config.sync.tables.length === 0);
  let tableRefreshTimer: NodeJS.Timeout | null = null;
  let refreshInProgress = false;
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

  if (autoDatabaseMode) {
    tableRefreshTimer = setInterval(async () => {
      if (refreshInProgress) {
        return;
      }

      refreshInProgress = true;
      try {
        const database = config.sync.database?.name ?? config.mysql.database;
        const discoveredTables = await introspector.listTables(database);
        const knownTables = new Set(resolvedTables.map((table) => `${table.database}.${table.table}`));

        for (const tableName of discoveredTables) {
          const key = `${database}.${tableName}`;
          if (knownTables.has(key)) {
            continue;
          }

          const [resolved] = await resolveTableConfigs(
            introspector,
            config.mysql.database,
            [{ database, table: tableName }],
            config.sync.database
          );

          if (!resolved) {
            continue;
          }

          resolvedTables.push(resolved);
          monitor.setTables(resolvedTables);
          binlogListener.registerTable?.(resolved);
          runtimeDiscoveredTables.add(key);
          logger.info({ table: key }, "Discovered new table in database mode");

          try {
            await initialSyncService.run([resolved]);
            monitor.markMode("realtime");
            logger.info({ table: key }, "Initial backfill completed for newly discovered table");
          } catch (error) {
            monitor.recordError(error, `auto-discovery:${key}`);
            logger.error({ error, table: key }, "Initial backfill failed for newly discovered table");
          }
        }
      } catch (error) {
        logger.error({ error }, "Periodic table discovery failed");
      } finally {
        refreshInProgress = false;
      }
    }, 15000);
  }

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
      },
      updateCollectionSchema: async (collectionName) => {
        const tableIndex = resolvedTables.findIndex((item) => item.collection === collectionName);
        if (tableIndex === -1) {
          return { ok: false, reason: "Collection is not mapped to any table" };
        }
        if (reindexInFlight.has(collectionName)) {
          return { ok: false, reason: "Reindex already in progress" };
        }

        const existingTable = resolvedTables[tableIndex];
        const [freshConfig] = await resolveTableConfigs(
          introspector,
          config.mysql.database,
          [{ database: existingTable.database, table: existingTable.table, collection: existingTable.collection }],
          config.sync.database
        );

        if (!freshConfig) {
          return { ok: false, reason: "Failed to resolve fresh table config from database" };
        }

        const fields = freshConfig.typesense.fields.some((f) => f.name === "id")
          ? freshConfig.typesense.fields
          : [{ name: "id", type: "string" as const }, ...freshConfig.typesense.fields];

        reindexInFlight.add(collectionName);
        try {
          try {
            await typesenseClient.collections(collectionName).update({ fields });
          } catch {
            // Schema update failed (e.g. incompatible type change) — delete and re-sync
            logger.warn({ collection: collectionName }, "Schema update failed, deleting collection and re-syncing");
            try {
              await typesenseClient.collections(collectionName).delete();
            } catch { /* ignore if already gone */ }
            resolvedTables[tableIndex] = freshConfig;
            await initialSyncService.run([freshConfig]);
            return { ok: true };
          }
          resolvedTables[tableIndex] = freshConfig;
          return { ok: true };
        } catch (error) {
          monitor.recordError(error, `update-schema:${collectionName}`);
          return {
            ok: false,
            reason: error instanceof Error ? error.message : String(error)
          };
        } finally {
          reindexInFlight.delete(collectionName);
        }
      },
      resetTypesense: async () => {
        try {
          // Delete all Typesense collections
          const collections = await typesenseClient.collections().retrieve();
          for (const col of collections) {
            try {
              await typesenseClient.collections(col.name).delete();
            } catch { /* ignore */ }
          }

          // Reset checkpoint so binlog listener starts from current position on next restart
          const emptyCheckpoint: BinlogCheckpoint = { updatedAt: new Date().toISOString() };
          await checkpointStore.save(emptyCheckpoint);

          // Re-run initial sync table-by-table; don't abort all if one table fails.
          for (const table of resolvedTables) {
            try {
              await initialSyncService.run([table]);
            } catch (error) {
              monitor.recordError(error, `reset:${table.database}.${table.table}`);
              logger.error(
                { error, table: `${table.database}.${table.table}` },
                "Reset sync failed for table, continuing with remaining tables"
              );
            }
          }
          return { ok: true };
        } catch (error) {
          monitor.recordError(error, "reset");
          return {
            ok: false,
            reason: error instanceof Error ? error.message : String(error)
          };
        }
      },
      getDiscoveredTables: () => ({
        autoDiscoveryEnabled: autoDatabaseMode,
        startupDiscovered: Array.from(startupDiscoveredTables).sort(),
        runtimeDiscovered: Array.from(runtimeDiscoveredTables).sort(),
        currentTables: resolvedTables.map((table) => `${table.database}.${table.table}`).sort()
      })
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
      if (tableRefreshTimer) {
        clearInterval(tableRefreshTimer);
        tableRefreshTimer = null;
      }
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