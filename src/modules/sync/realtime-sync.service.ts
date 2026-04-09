import type { Logger } from "pino";

import type { BinlogListener, CheckpointStore, DocumentTransformer, SyncMonitor } from "../../core/types.js";
import { withRetry } from "../../utils/retry.js";
import type { TypesenseCollectionManager } from "../typesense/collection-manager.js";
import type { TypesenseDocumentIndexer } from "../typesense/document-indexer.js";

export class RealtimeSyncService {
  private readonly joinSchemaValidatedTables = new Set<string>();

  constructor(
    private readonly listener: BinlogListener,
    private readonly collectionManager: TypesenseCollectionManager,
    private readonly documentIndexer: TypesenseDocumentIndexer,
    private readonly transformer: DocumentTransformer,
    private readonly checkpointStore: CheckpointStore,
    private readonly retryConfig: { maxAttempts: number; baseDelayMs: number },
    private readonly logger: Logger,
    private readonly monitor: SyncMonitor
  ) {}

  async run(): Promise<void> {
    this.monitor.markMode("realtime");

    await this.listener.start(async (event) => {
      const tableKey = `${event.table.database}.${event.table.table}`;
      this.logger.debug({ tableKey, operation: event.operation }, "Received realtime event from binlog");

      try {
        await withRetry(() => this.collectionManager.ensureCollection(event.table), this.retryConfig);

        if (!this.joinSchemaValidatedTables.has(tableKey)) {
          await withRetry(() => this.collectionManager.validateJoinReferenceIntegrity([event.table]), this.retryConfig);
          this.joinSchemaValidatedTables.add(tableKey);
        }

        if (event.operation === "delete") {
          const primaryValue = event.before?.[event.table.primaryKey];
          if (primaryValue !== undefined && primaryValue !== null) {
            await withRetry(
              () => this.documentIndexer.deleteDocument(event.table, String(primaryValue)),
              this.retryConfig
            );
          }
        } else if (event.after) {
          const previousPrimaryValue = event.before?.[event.table.primaryKey];
          const currentPrimaryValue = event.after[event.table.primaryKey];

          if (
            previousPrimaryValue !== undefined &&
            previousPrimaryValue !== null &&
            currentPrimaryValue !== undefined &&
            currentPrimaryValue !== null &&
            String(previousPrimaryValue) !== String(currentPrimaryValue)
          ) {
            await withRetry(
              () => this.documentIndexer.deleteDocument(event.table, String(previousPrimaryValue)),
              this.retryConfig
            );
          }

          const document = await this.transformer.toDocument(event.after, event.table);
          await withRetry(() => this.documentIndexer.upsertDocument(event.table, document), this.retryConfig);
        }

        if (event.checkpoint) {
          await this.checkpointStore.save(event.checkpoint);
        }

        this.monitor.recordRealtimeEvent(tableKey, event.operation);
        this.logger.info({ table: event.table.table, operation: event.operation }, "Realtime change processed");
      } catch (error) {
        this.monitor.recordError(error, `realtime:${tableKey}`, (event.after ?? event.before) as Record<string, unknown> | undefined);
        this.logger.error({ error, table: event.table.table, operation: event.operation }, "Realtime change failed, skipping event");
      }
    });
  }
}