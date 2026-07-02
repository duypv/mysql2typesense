import type { Logger } from "pino";
import type { Client } from "typesense";

import type { RetryConfig, SyncMonitor, TableSyncConfig } from "../../core/types.js";
import { withRetry } from "../../utils/retry.js";
import type { MySqlSourceReader } from "../mysql/source-reader.js";
import type { TypesenseDocumentIndexer } from "../typesense/document-indexer.js";

/**
 * Reconciles the set of documents in each Typesense collection against the live
 * primary keys in MySQL. Any document present in Typesense but missing from
 * MySQL is deleted.
 *
 * Why this exists: deletes can be lost when
 *   - a row is deleted DURING initial sync after it was already scanned/imported,
 *     and the checkpoint is then advanced past the delete event,
 *   - the binlog listener is disconnected/expired and the checkpoint is forced
 *     forward by the recovery path,
 *   - any other path where a `delete` binlog event never reaches the listener.
 *
 * Reconcile catches these stale documents by direct comparison.
 */
export class ReconciliationService {
  constructor(
    private readonly sourceReader: MySqlSourceReader,
    private readonly typesenseClient: Client,
    private readonly documentIndexer: TypesenseDocumentIndexer,
    private readonly batchSize: number,
    private readonly retryConfig: RetryConfig,
    private readonly logger: Logger,
    private readonly monitor?: SyncMonitor
  ) {}

  async reconcileTable(table: TableSyncConfig): Promise<number> {
    const tableKey = `${table.database}.${table.table}`;
    const startedAt = Date.now();

    const mysqlIds = new Set<string>();
    try {
      for await (const idBatch of this.sourceReader.scanPrimaryKeys(table, this.batchSize)) {
        for (const id of idBatch) {
          mysqlIds.add(String(id));
        }
      }
    } catch (error) {
      this.logger.error({ error, table: tableKey }, "reconcile: failed to scan MySQL primary keys, aborting reconcile for this table");
      this.monitor?.recordError(error, `reconcile:${tableKey}:mysql-scan`);
      return 0;
    }

    let typesenseTotal = 0;
    const staleIds: string[] = [];
    try {
      const exportPayload = await withRetry(
        () =>
          this.typesenseClient
            .collections(table.collection)
            .documents()
            .export({ include_fields: "id" }),
        this.retryConfig
      );

      if (typeof exportPayload === "string" && exportPayload.length > 0) {
        for (const rawLine of exportPayload.split("\n")) {
          const line = rawLine.trim();
          if (line.length === 0) continue;
          let parsed: { id?: unknown } | null = null;
          try {
            parsed = JSON.parse(line) as { id?: unknown };
          } catch {
            continue;
          }
          if (parsed?.id === undefined || parsed.id === null) continue;
          typesenseTotal += 1;
          const idStr = String(parsed.id);
          if (!mysqlIds.has(idStr)) {
            staleIds.push(idStr);
          }
        }
      }
    } catch (error) {
      const httpStatus = (error as { httpStatus?: number })?.httpStatus;
      if (httpStatus === 404) {
        this.logger.debug({ table: tableKey, collection: table.collection }, "reconcile: collection missing in Typesense, skipping");
        return 0;
      }
      this.logger.error({ error, table: tableKey }, "reconcile: failed to export Typesense IDs");
      this.monitor?.recordError(error, `reconcile:${tableKey}:typesense-export`);
      return 0;
    }

    if (staleIds.length === 0) {
      this.logger.info(
        { table: tableKey, mysqlCount: mysqlIds.size, typesenseCount: typesenseTotal, durationMs: Date.now() - startedAt },
        "reconcile: no stale documents"
      );
      return 0;
    }

    let deleted = 0;
    for (const id of staleIds) {
      try {
        await withRetry(() => this.documentIndexer.deleteDocument(table, id), this.retryConfig);
        deleted += 1;
      } catch (error) {
        this.logger.error({ error, table: tableKey, id }, "reconcile: failed to delete stale document");
        this.monitor?.recordError(error, `reconcile:${tableKey}:delete:${id}`);
      }
    }

    this.logger.warn(
      {
        table: tableKey,
        mysqlCount: mysqlIds.size,
        typesenseCount: typesenseTotal,
        stale: staleIds.length,
        deleted,
        durationMs: Date.now() - startedAt
      },
      "reconcile: deleted stale documents"
    );

    return deleted;
  }

  async reconcileAll(tables: TableSyncConfig[]): Promise<number> {
    let totalDeleted = 0;
    for (const table of tables) {
      try {
        totalDeleted += await this.reconcileTable(table);
      } catch (error) {
        const tableKey = `${table.database}.${table.table}`;
        this.logger.error({ error, table: tableKey }, "reconcile: unexpected failure");
        this.monitor?.recordError(error, `reconcile:${tableKey}`);
      }
    }
    return totalDeleted;
  }
}
