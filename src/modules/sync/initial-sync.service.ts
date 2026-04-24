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

    // Phase 1: drop+recreate ALL collection schemas before importing any data.
    // Every collection is force-recreated to:
    //   a) Remove stale documents deleted from MySQL since the last sync.
    //   b) Guarantee canonical schema (fixes corrupted optional flags, type mismatches
    //      from prior patch-in operations that Typesense v30 join validation rejects).
    //   c) Ensure referenced/parent collections exist before child collections import
    //      their documents (avoids "Referenced field X not found in collection Y").
    // This is safe because Phase 2 re-imports all data from MySQL.
    for (const table of tables) {
      await withRetry(() => this.collectionManager.ensureCollection(table, /* forceRecreate */ true), this.retryConfig);
    }

    // Phase 1b: scan ACTUAL Typesense schemas for reference fields and ensure all
    // target fields are non-optional. This catches references created by any code
    // path (including previous deployments) that the in-memory config may not know about.
    // Typesense v30 rejects imports with "Referenced field X not found in collection Y"
    // when the target field is optional — this auto-repair prevents that error.
    await withRetry(() => this.collectionManager.repairReferenceTargets(), this.retryConfig);

    await withRetry(() => this.collectionManager.validateJoinReferenceIntegrity(tables), this.retryConfig);

    // Phase 2: import data for all tables now that all schemas are in place.
    for (const table of tables) {
      const tableKey = `${table.database}.${table.table}`;

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
                this.monitor.recordError(documentError, `initial:${tableKey}:document:${document.id}`, document as Record<string, unknown>);
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