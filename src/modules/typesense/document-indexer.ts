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
      if (!(error instanceof Error && "importResults" in error)) {
        throw error;
      }

      const results = (error as any).importResults as Array<{ success: boolean; error?: string }>;
      const reason = results?.find((r) => !r.success)?.error ?? "";

      // Missing required field → partial binlog event. Fall back to partial update
      // on the existing document; if it doesn't exist yet (404), skip silently —
      // the document will be created during the next initial sync or full-row event.
      if (reason.includes("not found in the document")) {
        try {
          await this.client.collections(table.collection).documents(document.id).update(document);
        } catch (updateError: unknown) {
          const status = (updateError as any)?.httpStatus;
          if (status === 404) {
            return; // document not yet indexed — skip partial event
          }
          throw updateError;
        }
        return;
      }

      throw new Error(`Typesense import failed for document ${document.id}: ${reason}`);
    }
  }

  async deleteDocument(table: TableSyncConfig, documentId: string): Promise<void> {
    await this.client.collections(table.collection).documents(documentId).delete();
  }
}