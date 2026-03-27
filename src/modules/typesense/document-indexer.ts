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
    await this.client.collections(table.collection).documents().upsert(document);
  }

  async deleteDocument(table: TableSyncConfig, documentId: string): Promise<void> {
    await this.client.collections(table.collection).documents(documentId).delete();
  }
}