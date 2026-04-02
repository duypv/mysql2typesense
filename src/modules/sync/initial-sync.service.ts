import type { Logger } from "pino";

import type { DocumentTransformer, SyncMonitor, TableSyncConfig } from "../../core/types.js";
import { withRetry } from "../../utils/retry.js";
import type { MySqlSourceReader } from "../mysql/source-reader.js";
import type { TypesenseCollectionManager } from "../typesense/collection-manager.js";
import type { TypesenseDocumentIndexer } from "../typesense/document-indexer.js";

export class InitialSyncService {
  constructor(
    private readonly sourceReader: MySqlSourceReader,
    private readonly collectionManager: TypesenseCollectionManager,
    private readonly documentIndexer: TypesenseDocumentIndexer,
    private readonly transformer: DocumentTransformer,
    private readonly batchSize: number,
    private readonly retryConfig: { maxAttempts: number; baseDelayMs: number },
    private readonly logger: Logger,
    private readonly monitor: SyncMonitor
  ) {}

  async run(tables: TableSyncConfig[]): Promise<void> {
    this.monitor.markMode("initial");

    for (const table of tables) {
      const tableKey = `${table.database}.${table.table}`;
      await withRetry(() => this.collectionManager.ensureCollection(table), this.retryConfig);

      for await (const batch of this.sourceReader.scanTable(table, table.batchSize ?? this.batchSize)) {
        try {
          const documents = await Promise.all(batch.map((row) => this.transformer.toDocument(row, table)));
          let imported = documents.length;

          try {
            await this.documentIndexer.importDocuments(table, documents);
          } catch (bulkError) {
            this.logger.warn(
              { error: bulkError, table: table.table, batchSize: documents.length },
              "Bulk import failed, falling back to per-document upsert"
            );

            imported = 0;
            for (const document of documents) {
              try {
                await withRetry(() => this.documentIndexer.upsertDocument(table, document), this.retryConfig);
                imported += 1;
              } catch (documentError) {
                this.monitor.recordError(documentError, `initial:${tableKey}:document:${document.id}`);
              }
            }
          }

          this.monitor.recordInitialBatch(tableKey, imported);
          this.logger.info({ table: table.table, imported }, "Initial sync batch imported");
        } catch (error) {
          this.monitor.recordError(error, `initial:${tableKey}`);
          throw error;
        }
      }
    }

    this.monitor.markMode("idle");
  }
}