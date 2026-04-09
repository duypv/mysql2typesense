import type { Logger } from "pino";
import { createClient } from "redis";
import type { Server } from "node:http";
import type { RowDataPacket } from "mysql2/promise";

import { loadConfig } from "../config/env.js";
import { createLogger } from "../config/logger.js";
import type { AppConfig, BinlogCheckpoint, CheckpointStore, ResetStatusSnapshot, TableSyncConfig } from "../core/types.js";
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
  alignCheckpointToCurrentBinlog(context?: string): Promise<void>;
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
    config.sync.database,
    config.sync.joinConfigs
  );
  const startupDiscoveredTables = new Set(resolvedTables.map((table) => `${table.database}.${table.table}`));
  const runtimeDiscoveredTables = new Set<string>();
  const monitor = new InMemorySyncMonitor();
  monitor.setTables(resolvedTables);

  const checkpointStore = await createCheckpointStore(config);
  const typesenseClient = createTypesenseClient(config);
  const collectionManager = new TypesenseCollectionManager(typesenseClient, logger);
  const documentIndexer = new TypesenseDocumentIndexer(typesenseClient);
  const transformer = new ConfigDrivenTransformer();
  const binlogListener = new MySqlBinlogListener(config, resolvedTables, checkpointStore, logger);
  const reindexInFlight = new Set<string>();
  const autoDatabaseMode = Boolean(config.sync.database && config.sync.tables.length === 0);
  let tableRefreshTimer: NodeJS.Timeout | null = null;
  let refreshInProgress = false;
  let monitoringServer: Server | null = null;
  let resetInProgress = false;
  let resetStatus: ResetStatusSnapshot = {
    status: "idle",
    phase: "idle",
    currentPhase: 0,
    totalPhases: 2,
    updatedAt: new Date().toISOString(),
    message: "No reset has been started yet"
  };

  const setResetStatus = (patch: Partial<ResetStatusSnapshot>) => {
    resetStatus = {
      ...resetStatus,
      ...patch,
      updatedAt: new Date().toISOString()
    };
  };

  if (autoDatabaseMode && resolvedTables.length === 0) {
    logger.warn(
      { configuredDatabase: config.sync.database?.name, mysqlDatabase: config.mysql.database },
      "No tables found at startup — check that database.name in sync.config.json matches the MySQL database. Tables will be discovered via the 15s interval."
    );
  }

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

  const realtimeSyncService = new RealtimeSyncService(
    binlogListener,
    collectionManager,
    documentIndexer,
    transformer,
    checkpointStore,
    config.sync.retry,
    logger,
    monitor
  );

  const alignCheckpointToCurrentBinlog = async (context = "runtime") => {
    try {
      let rows: RowDataPacket[] = [];
      let statusQuery = "SHOW BINARY LOG STATUS";
      try {
        const [binaryStatusRows] = await mysqlPool.query<RowDataPacket[]>("SHOW BINARY LOG STATUS");
        rows = binaryStatusRows;
      } catch {
        // Compatibility fallback for older MySQL versions.
        statusQuery = "SHOW MASTER STATUS";
        const [masterStatusRows] = await mysqlPool.query<RowDataPacket[]>("SHOW MASTER STATUS");
        rows = masterStatusRows;
      }

      const first = rows[0];
      const filename = first?.File ? String(first.File) : undefined;
      const position = first?.Position !== undefined ? Number(first.Position) : undefined;
      if (!filename || Number.isNaN(position)) {
        logger.warn({ context, statusQuery, rows: rows.length }, "Checkpoint align skipped: status query returned no file/position");
        return;
      }

      const checkpoint: BinlogCheckpoint = {
        filename,
        position,
        updatedAt: new Date().toISOString()
      };
      await checkpointStore.save(checkpoint);
      logger.info({ context, statusQuery, filename, position }, "Aligned binlog checkpoint to current master position");
    } catch (error) {
      logger.warn({ error, context }, "Failed to align binlog checkpoint to current master position");
    }
  };

  if (autoDatabaseMode) {
    const runDiscoveryWave = async (trigger: "startup" | "interval") => {
      if (refreshInProgress) {
        return;
      }

      if (resetInProgress) {
        logger.info({ trigger }, "Skipping discovery wave while reset is in progress");
        return;
      }

      refreshInProgress = true;
      try {
        const configuredDatabase = config.sync.database?.name ?? config.mysql.database;
        let database = configuredDatabase;
        let discoveredTables = await introspector.listTables(database);

        // If configured database name finds nothing, fall back to the MySQL connection database
        if (discoveredTables.length === 0 && configuredDatabase !== config.mysql.database) {
          discoveredTables = await introspector.listTables(config.mysql.database);
          if (discoveredTables.length > 0) {
            database = config.mysql.database;
            logger.warn(
              { configuredDatabase, fallbackDatabase: database },
              "Configured database name found no tables, using MySQL connection database as fallback"
            );
          }
        }

        const knownTables = new Set(resolvedTables.map((table) => `${table.database}.${table.table}`));
        const newlyDiscovered: string[] = [];

        for (const tableName of discoveredTables) {
          const key = `${database}.${tableName}`;
          if (knownTables.has(key)) {
            continue;
          }

          const allSeeds = [
            ...resolvedTables.map((table) => ({
              database: table.database,
              table: table.table,
              collection: table.collection,
              primaryKey: table.primaryKey
            })),
            { database, table: tableName }
          ];

          const refreshedConfigs = await resolveTableConfigs(
            introspector,
            config.mysql.database,
            allSeeds,
            config.sync.database,
            config.sync.joinConfigs
          );

          const refreshedByKey = new Map(
            refreshedConfigs.map((table) => [`${table.database}.${table.table}`, table])
          );

          for (let i = 0; i < resolvedTables.length; i += 1) {
            const existingKey = `${resolvedTables[i].database}.${resolvedTables[i].table}`;
            const refreshed = refreshedByKey.get(existingKey);
            if (refreshed) {
              resolvedTables[i] = refreshed;
            }
          }

          const resolved = refreshedByKey.get(key);

          if (!resolved) {
            continue;
          }

          resolvedTables.push(resolved);
          knownTables.add(key);
          monitor.setTables(resolvedTables);
          binlogListener.registerTable?.(resolved);
          runtimeDiscoveredTables.add(key);
          newlyDiscovered.push(key);
          logger.info({ table: key }, "Discovered new table in database mode");
        }

        if (newlyDiscovered.length > 0) {
          try {
            // Backfill all currently known tables together so join dependencies are
            // always created/imported in one coherent run, regardless of discovery order.
            await initialSyncService.run(resolvedTables);
            monitor.markMode("realtime");
            logger.info(
              {
                trigger,
                discoveredTables: newlyDiscovered,
                totalKnownTables: resolvedTables.length
              },
              "Initial backfill completed for newly discovered tables"
            );
          } catch (error) {
            monitor.recordError(error, "auto-discovery");
            logger.error(
              { error, trigger, discoveredTables: newlyDiscovered, totalKnownTables: resolvedTables.length },
              "Initial backfill failed after discovery wave"
            );
          }
        } else if (trigger === "startup") {
          logger.info({ trigger, totalKnownTables: resolvedTables.length }, "Startup discovery wave completed with no new tables");
        }
      } catch (error) {
        logger.error({ error, trigger }, "Periodic table discovery failed");
      } finally {
        refreshInProgress = false;
      }
    };

    // Startup guard: run one discovery wave immediately to avoid the initial 15s blind window.
    await runDiscoveryWave("startup");

    tableRefreshTimer = setInterval(() => {
      runDiscoveryWave("interval").catch((error) => {
        logger.error({ error }, "Periodic table discovery runner failed unexpectedly");
      });
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
          monitor.markMode("realtime");
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
        const refreshedConfigs = await resolveTableConfigs(
          introspector,
          config.mysql.database,
          resolvedTables.map((table) => ({
            database: table.database,
            table: table.table,
            collection: table.collection,
            primaryKey: table.primaryKey
          })),
          config.sync.database,
          config.sync.joinConfigs
        );

        const refreshedByKey = new Map(
          refreshedConfigs.map((table) => [`${table.database}.${table.table}`, table])
        );

        for (let i = 0; i < resolvedTables.length; i += 1) {
          const key = `${resolvedTables[i].database}.${resolvedTables[i].table}`;
          const refreshed = refreshedByKey.get(key);
          if (refreshed) {
            resolvedTables[i] = refreshed;
          }
        }

        const freshConfig = resolvedTables.find((item) => item.collection === collectionName);

        if (!freshConfig) {
          return { ok: false, reason: "Failed to resolve fresh table config from database" };
        }

        const dependencyCollections = new Set<string>();
        for (const field of freshConfig.typesense.fields) {
          if (!field.reference) continue;
          const targetCollection = field.reference.split(".", 2)[0];
          if (targetCollection) {
            dependencyCollections.add(targetCollection);
          }
        }

        const dependencyTables = resolvedTables.filter(
          (table) => dependencyCollections.has(table.collection) && table.collection !== freshConfig.collection
        );
        const backfillTables = [...dependencyTables, freshConfig];

        reindexInFlight.add(collectionName);
        try {
          await initialSyncService.run(backfillTables);
          monitor.markMode("realtime");
          logger.info(
            {
              collection: collectionName,
              dependencyTables: dependencyTables.map((table) => `${table.database}.${table.table}`)
            },
            "Force update schema completed with join-aware backfill"
          );
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
        let realtimeStopped = false;
        try {
          if (resetInProgress) {
            return { ok: false, reason: "Reset already in progress" };
          }

          resetInProgress = true;
          const startedAt = new Date().toISOString();
          setResetStatus({
            status: "running",
            phase: "phase-1-initial-sync",
            currentPhase: 1,
            totalPhases: 2,
            startedAt,
            finishedAt: undefined,
            error: undefined,
            message: "Reset started: refreshing configs and running phase 1 initial sync"
          });

          logger.info("Reset: started");

          try {
            logger.info("Reset: stopping realtime listener");
            await binlogListener.stop();
            realtimeStopped = true;
            setResetStatus({
              status: "running",
              phase: "phase-1-initial-sync",
              currentPhase: 1,
              message: "Reset started: realtime listener paused, refreshing configs and running phase 1 initial sync"
            });
          } catch (error) {
            logger.error({ error }, "Reset: failed to stop realtime listener");
            return {
              ok: false,
              reason: error instanceof Error ? error.message : String(error)
            };
          }

          // Delete all Typesense collections
          const collections = await typesenseClient.collections().retrieve();
          for (const col of collections) {
            try {
              await typesenseClient.collections(col.name).delete();
            } catch { /* ignore */ }
          }

          // Clear manager schema cache so the next ensureCollection rebuilds each collection fresh
          collectionManager.clearSyncedCache();

          // Reset checkpoint so binlog listener starts from current position on next restart
          const emptyCheckpoint: BinlogCheckpoint = { updatedAt: new Date().toISOString() };
          await checkpointStore.save(emptyCheckpoint);

          // Always refresh table configs before reset sync so we never rely on stale in-memory schemas.
          let freshTables: TableSyncConfig[] = [];
          if (autoDatabaseMode) {
            const configuredDatabase = config.sync.database?.name ?? config.mysql.database;
            let discoveryDatabase = configuredDatabase;
            freshTables = await resolveTableConfigs(introspector, config.mysql.database, [], config.sync.database, config.sync.joinConfigs);

            // If configured database.name found nothing, retry with MySQL connection DB
            if (freshTables.length === 0 && configuredDatabase !== config.mysql.database) {
              const databaseOnlyConfig = config.sync.database ? { ...config.sync.database, name: config.mysql.database } : undefined;
              freshTables = await resolveTableConfigs(introspector, config.mysql.database, [], databaseOnlyConfig, config.sync.joinConfigs);
              if (freshTables.length > 0) {
                discoveryDatabase = config.mysql.database;
                logger.warn(
                  { configuredDatabase, fallbackDatabase: discoveryDatabase },
                  "Reset: configured database found no tables, using MySQL connection database"
                );
              }
            }
          } else {
            const seedTables = config.sync.tables.length > 0
              ? config.sync.tables
              : resolvedTables.map((table) => ({
                  database: table.database,
                  table: table.table,
                  collection: table.collection,
                  primaryKey: table.primaryKey
                }));
            freshTables = await resolveTableConfigs(
              introspector,
              config.mysql.database,
              seedTables,
              config.sync.database,
              config.sync.joinConfigs
            );
          }

          if (freshTables.length === 0) {
            const reason = "Reset aborted: no tables resolved from current database/config";
            logger.error({ autoDatabaseMode }, reason);
            return { ok: false, reason };
          }

          const knownKeysBefore = new Set(resolvedTables.map((t) => `${t.database}.${t.table}`));
          resolvedTables.splice(0, resolvedTables.length, ...freshTables);

          for (const table of freshTables) {
            const key = `${table.database}.${table.table}`;
            binlogListener.registerTable?.(table);
            if (!knownKeysBefore.has(key)) {
              runtimeDiscoveredTables.add(key);
            }
          }
          monitor.setTables(resolvedTables);
          logger.info({ tableCount: resolvedTables.length }, "Reset: refreshed table configs for initial sync");

          // Run a full initial sync for all tables at once. run() handles:
          //   1. Force-recreating all collections (removes stale data, ensures clean schema)
          //   2. Creating all schemas before any data import (avoids join reference errors)
          //   3. Importing all data from MySQL
          try {
            logger.info({ tableCount: resolvedTables.length }, "Reset: running full initial sync");
            await initialSyncService.run(resolvedTables);
            logger.info({ tableCount: resolvedTables.length }, "Reset: full initial sync completed");
          } catch (error) {
            setResetStatus({
              status: "failed",
              phase: "phase-1-initial-sync",
              currentPhase: 1,
              finishedAt: new Date().toISOString(),
              message: "Reset failed during phase 1 initial sync",
              error: error instanceof Error ? error.message : String(error)
            });
            monitor.recordError(error, "reset");
            logger.error({ error }, "Reset initial sync failed");
            return {
              ok: false,
              reason: error instanceof Error ? error.message : String(error)
            };
          }

          // Extra safety pass requested: force update schema for all collections after reset completes.
          // This runs one more global initial sync to ensure all schemas are reconciled in a single pass.
          try {
            setResetStatus({
              status: "running",
              phase: "phase-2-force-schema",
              currentPhase: 2,
              message: "Reset phase 1 completed. Running phase 2 force schema update for all collections"
            });
            logger.info({ tableCount: resolvedTables.length }, "Reset: force update schema for all collections (post-reset pass)");
            await initialSyncService.run(resolvedTables);
            logger.info({ tableCount: resolvedTables.length }, "Reset: post-reset force schema update completed");
          } catch (error) {
            setResetStatus({
              status: "failed",
              phase: "phase-2-force-schema",
              currentPhase: 2,
              finishedAt: new Date().toISOString(),
              message: "Reset failed during phase 2 force schema update",
              error: error instanceof Error ? error.message : String(error)
            });
            monitor.recordError(error, "reset:force-update-all");
            logger.error({ error }, "Reset post-pass force schema update failed");
            return {
              ok: false,
              reason: error instanceof Error ? error.message : String(error)
            };
          }

          // Restore realtime mode after reset completes
          monitor.markMode("realtime");
          await alignCheckpointToCurrentBinlog("reset-post-phase2");
          setResetStatus({
            status: "completed",
            phase: "phase-2-force-schema",
            currentPhase: 2,
            finishedAt: new Date().toISOString(),
            message: "Reset completed successfully and switched back to realtime mode",
            error: undefined
          });
          logger.info({ tableCount: resolvedTables.length }, "Reset: completed and switched back to realtime mode");
          return { ok: true };
        } catch (error) {
          setResetStatus({
            status: "failed",
            phase: resetStatus.phase,
            currentPhase: resetStatus.currentPhase,
            finishedAt: new Date().toISOString(),
            message: "Reset failed with unexpected error",
            error: error instanceof Error ? error.message : String(error)
          });
          monitor.recordError(error, "reset");
          return {
            ok: false,
            reason: error instanceof Error ? error.message : String(error)
          };
        } finally {
          if (realtimeStopped) {
            try {
              logger.info("Reset: restarting realtime listener");
              await realtimeSyncService.run();
              logger.info("Reset: realtime listener restarted");
            } catch (error) {
              setResetStatus({
                status: "failed",
                phase: resetStatus.phase,
                currentPhase: resetStatus.currentPhase,
                finishedAt: new Date().toISOString(),
                message: "Reset finished but failed to restart realtime listener",
                error: error instanceof Error ? error.message : String(error)
              });
              logger.error({ error }, "Reset: failed to restart realtime listener");
            }
          }
          resetInProgress = false;
        }
      },
      getJoinReferenceDiagnostics: async () => {
        return collectionManager.getJoinReferenceIntegrityReport(resolvedTables);
      },
      getResetStatus: () => ({ ...resetStatus }),
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
    realtimeSyncService,
    alignCheckpointToCurrentBinlog,
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