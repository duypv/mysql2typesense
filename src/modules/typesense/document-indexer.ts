import type { Client } from "typesense";

import type { SyncDocument, TableSyncConfig } from "../../core/types.js";

export class TypesenseDocumentIndexer {
  constructor(private readonly client: Client) {}

  async importDocuments(table: TableSyncConfig, documents: SyncDocument[]): Promise<void> {
    if (documents.length === 0) {
      return;
    }

    await this.client
      .collections(table.collection)
      .documents()
      .import(documents, { action: "upsert", dirty_values: "coerce_or_drop" });
  }

  async upsertDocument(table: TableSyncConfig, document: SyncDocument): Promise<void> {
    try {
      await this.client
        .collections(table.collection)
        .documents()
        .import([document], { action: "emplace", dirty_values: "coerce_or_drop" });
    } catch (error: unknown) {
      // import() throws when any document fails. Extract the per-document result
      // to decide whether to propagate. If the single document simply had
      // uncoercible values that were dropped, treat it as a warning not a crash.
      if (error instanceof Error && "importResults" in error) {
        const results = (error as any).importResults as Array<{ success: boolean; error?: string }>;
        const failed = results?.filter((r) => !r.success);
        if (failed?.length) {
          const reason = failed[0]?.error ?? "unknown";
          throw new Error(`Typesense import failed for document ${document.id}: ${reason}`);
        }
      }
      throw error;
    }
  }

  async deleteDocument(table: TableSyncConfig, documentId: string): Promise<void> {
    await this.client.collections(table.collection).documents(documentId).delete();
  }
}