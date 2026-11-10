import type { Logger } from "pino";
import type { Client } from "typesense";

import type { SyncDocument, TableSyncConfig } from "../../core/types.js";

export class TypesenseDocumentIndexer {
  constructor(
    private readonly client: Client,
    private readonly logger?: Logger
  ) {}

  async importDocuments(table: TableSyncConfig, documents: SyncDocument[]): Promise<void> {
    if (documents.length === 0) {
      return;
    }

    await this.client
      .collections(table.collection)
      .documents()
      .import(documents, { action: "upsert", dirty_values: "coerce_or_drop" });
  }

  /**
   * Indexes a realtime change. `nullFields` lists target fields whose source column
   * was explicitly set to NULL: emplace is a partial update, so omitting them would
   * leave the previous value in place. They are patched to null in a follow-up call.
   *
   * An optional field whose value does not match the type Typesense already holds
   * for it (typically an `auto` JSON column with a different shape) is dropped and
   * the document retried, so one bad column does not block every other column.
   * Returns the dropped field names so callers can report them.
   */
  async upsertDocument(table: TableSyncConfig, document: SyncDocument, nullFields: string[] = []): Promise<string[]> {
    const droppedFields: string[] = [];
    let current = document;

    for (;;) {
      try {
        await this.client
          .collections(table.collection)
          .documents()
          .import([current], { action: "emplace", dirty_values: "coerce_or_drop" });
        break;
      } catch (error: unknown) {
        if (!(error instanceof Error && "importResults" in error)) {
          throw error;
        }

        const results = (error as any).importResults as Array<{ success: boolean; error?: string }>;
        const reason = results?.find((r) => !r.success)?.error ?? "";

        const mistypedField = droppableMistypedField(table, current, reason);
        if (mistypedField) {
          droppedFields.push(mistypedField);
          const { [mistypedField]: _dropped, ...rest } = current;
          current = rest as SyncDocument;
          this.logger?.warn(
            { collection: table.collection, documentId: document.id, field: mistypedField, reason },
            "Dropping field with mismatched type and retrying document"
          );
          continue;
        }

        // Missing required field → partial binlog event. Fall back to partial update
        // on the existing document; if it doesn't exist yet (404), skip silently —
        // the document will be created during the next initial sync or full-row event.
        if (reason.includes("not found in the document")) {
          try {
            await this.client
              .collections(table.collection)
              .documents(current.id)
              .update({ ...current, ...nullPayload(nullFields) });
          } catch (updateError: unknown) {
            const status = (updateError as any)?.httpStatus;
            if (status === 404) {
              return droppedFields; // document not yet indexed — skip partial event
            }
            throw updateError;
          }
          return droppedFields;
        }

        throw new Error(`Typesense import failed for document ${document.id}: ${reason}`);
      }
    }

    await this.resetNullFields(table, document.id, nullFields);
    return droppedFields;
  }

  /** Patches explicitly-nulled fields to null so Typesense drops their previous value. */
  private async resetNullFields(table: TableSyncConfig, documentId: string, nullFields: string[]): Promise<void> {
    if (nullFields.length === 0) {
      return;
    }

    try {
      await this.client.collections(table.collection).documents(documentId).update(nullPayload(nullFields));
    } catch (error: unknown) {
      const status = (error as { httpStatus?: number })?.httpStatus;
      if (status === 404) {
        return; // document not yet indexed — nothing to reset
      }
      if (status === 400) {
        // Typesense refuses null on a non-optional field (e.g. a join reference
        // target). The emplace already succeeded, so keep it rather than failing
        // the whole event; the stale field value is reported instead.
        this.logger?.warn(
          { collection: table.collection, documentId, nullFields, reason: (error as Error).message },
          "Could not reset NULL fields in Typesense (field is not optional)"
        );
        return;
      }
      throw error;
    }
  }

  async deleteDocument(table: TableSyncConfig, documentId: string): Promise<void> {
    try {
      await this.client.collections(table.collection).documents(documentId).delete();
    } catch (error: unknown) {
      const status = (error as { httpStatus?: number })?.httpStatus;
      if (status === 404) {
        // Delete is idempotent: if the document is already gone, treat as success.
        return;
      }
      throw error;
    }
  }
}

function nullPayload(fields: string[]): Record<string, null> {
  return Object.fromEntries(fields.map((field) => [field, null]));
}

const TYPE_MISMATCH = /^Field `([^`]+)` (?:has an incorrect type|must be )/;

/**
 * Returns the top-level field to drop for a Typesense type-mismatch error, or
 * undefined when the error is something else or the field is required (dropping
 * it would only turn the error into "not found in the document").
 */
function droppableMistypedField(table: TableSyncConfig, document: SyncDocument, reason: string): string | undefined {
  const fieldPath = TYPE_MISMATCH.exec(reason)?.[1];
  if (!fieldPath) {
    return undefined;
  }

  const field = fieldPath.split(".")[0];
  if (field === "id" || !(field in document)) {
    return undefined;
  }

  const config = table.typesense.fields.find((candidate) => candidate.name === field);
  return config && !config.optional ? undefined : field;
}
