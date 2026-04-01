import type { Client } from "typesense";

import type { SyncDocument, TableSyncConfig } from "../../core/types.js";

export class TypesenseDocumentIndexer {
  constructor(private readonly client: Client) {}

  async importDocuments(table: TableSyncConfig, documents: SyncDocument[]): Promise<void> {
    if (documents.length === 0) {
      return;
    }

    await this.client.collections(table.collection).documents().import(documents, { action: "upsert" });
  }

  async upsertDocument(table: TableSyncConfig, document: SyncDocument): Promise<void> {
    try {
      await this.client.collections(table.collection).documents().upsert(document);
    } catch (error: unknown) {
      // Partial binlog event (e.g. UPDATE with binlog-row-image != FULL) may omit
      // unchanged columns, causing a 400 from Typesense for missing required fields.
      // Fall back to partial update which only touches the provided fields.
      if (error instanceof Error && "httpStatus" in error && (error as any).httpStatus === 400) {
        await this.client.collections(table.collection).documents(document.id).update(document);
        return;
      }
      throw error;
    }
  }

  async deleteDocument(table: TableSyncConfig, documentId: string): Promise<void> {
    await this.client.collections(table.collection).documents(documentId).delete();
  }
}