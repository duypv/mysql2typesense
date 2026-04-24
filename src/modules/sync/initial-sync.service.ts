import type { Logger } from "pino";

import type { DocumentTransformer, SyncMonitor, TableSyncConfig } from "../../core/types.js";
import { withRetry } from "../../utils/retry.js";
import type { MySqlSourceReader } from "../mysql/source-reader.js";
import type { TypesenseCollectionManager } from "../typesense/collection-manager.js";
import type { TypesenseDocumentIndexer } from "../typesense/document-indexer.js";

/**
 * Topologically sorts tables so that referenced (parent) collections come before
 * referencing (child) collections. This ensures Typesense has fully propagated
 * parent schemas before child collections are created or populated.
 *
 * Tables with no outgoing references come first. Tables that reference other
 * tables come after all their dependencies. Ties are broken by original order.
 */
function sortByDependencyOrder(tables: TableSyncConfig[]): TableSyncConfig[] {
  // Build a map of collection name → set of collections it depends on
  const collectionSet = new Set(tables.map((t) => t.collection));
  const deps = new Map<string, Set<string>>();

  for (const table of tables) {
    const tableDeps = new Set<string>();
    for (const field of table.typesense.fields) {
      if (!field.reference) continue;
      const dotIdx = field.reference.indexOf(".");
      if (dotIdx === -1) continue;
      const targetCollection = field.reference.slice(0, dotIdx);
      if (collectionSet.has(targetCollection) && targetCollection !== table.collection) {
        tableDeps.add(targetCollection);
      }
    }
    deps.set(table.collection, tableDeps);
  }

  const sorted: TableSyncConfig[] = [];
  const visited = new Set<string>();
  const visiting = new Set<string>();
  const tableByCollection = new Map(tables.map((t) => [t.collection, t]));

  const visit = (name: string) => {
    if (visited.has(name)) return;
    if (visiting.has(name)) return; // cycle — break it
    visiting.add(name);
    for (const dep of deps.get(name) ?? []) {
      visit(dep);
    }
    visiting.delete(name);
    visited.add(name);
    const table = tableByCollection.get(name);
    if (table) sorted.push(table);
  };

  for (const table of tables) {
    visit(table.collection);
  }

  return sorted;
}

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

    // Sort tables so parent/referenced collections are processed before children.
    // This prevents Typesense "Referenced field X not found in collection Y" errors
    // caused by child collections being created before their parent schemas are
    // fully propagated in Typesense's internal reference graph.
    const sorted = sortByDependencyOrder(tables);

    if (sorted.length !== tables.length) {
      this.logger.warn(
        { original: tables.length, sorted: sorted.length },
        "Dependency sort dropped tables — falling back to original order"
      );
    }

    const orderedTables = sorted.length === tables.length ? sorted : tables;

    // Phase 1: drop+recreate ALL collection schemas before importing any data.
    // Every collection is force-recreated to:
    //   a) Remove stale documents deleted from MySQL since the last sync.
    //   b) Guarantee canonical schema (fixes corrupted optional flags, type mismatches
    //      from prior patch-in operations that Typesense v30 join validation rejects).
    //   c) Ensure referenced/parent collections exist before child collections import
    //      their documents (avoids "Referenced field X not found in collection Y").
    // This is safe because Phase 2 re-imports all data from MySQL.
    for (const table of orderedTables) {
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
    // Uses the same dependency order so parent data is available before child imports.
    for (const table of orderedTables) {
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
