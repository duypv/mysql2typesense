import type { Client } from "typesense";

import type { TableSyncConfig, TypesenseFieldConfig } from "../../core/types.js";

export class TypesenseCollectionManager {
  private readonly synced = new Set<string>();

  constructor(private readonly client: Client) {}

  async ensureCollection(table: TableSyncConfig): Promise<void> {
    const fields = table.typesense.fields.some((field) => field.name === "id")
      ? table.typesense.fields
      : [{ name: "id", type: "string" as const }, ...table.typesense.fields];

    try {
      const existing = await this.client.collections(table.collection).retrieve();

      if (!this.synced.has(table.collection)) {
        const updates = this.diffOptionalFlags(existing.fields ?? [], fields);
        if (updates.length > 0) {
          try {
            await this.client.collections(table.collection).update({ fields: updates as any });
          } catch {
            // Schema update may not be supported for some field types; continue anyway.
            // The document-indexer fallback to partial update handles missing fields.
          }
        }
        this.synced.add(table.collection);
      }
    } catch {
      await this.client.collections().create({
        name: table.collection,
        fields,
        default_sorting_field: table.typesense.defaultSortingField,
        enable_nested_fields: table.typesense.enableNestedFields,
        token_separators: table.typesense.tokenSeparators,
        symbols_to_index: table.typesense.symbolsToIndex
      });
      this.synced.add(table.collection);
    }
  }

  /**
   * Clears the internal cache of synced collections.
   * Call this after deleting collections (e.g. during a full reset) so that
   * the next `ensureCollection` call treats each collection as new.
   */
  clearSyncedCache(): void {
    this.synced.clear();
  }

  /**
   * Returns field patches for fields that should be optional in the desired config
   * but are currently required in the existing collection.
   */
  private diffOptionalFlags(
    existing: Array<{ name?: string; type?: string; optional?: boolean }>,
    desired: TypesenseFieldConfig[]
  ): Array<{ name: string; type: string; optional: boolean }> {
    const desiredByName = new Map(desired.map((f) => [f.name, f]));
    const patches: Array<{ name: string; type: string; optional: boolean }> = [];

    for (const field of existing) {
      if (!field.name || !field.type) continue;
      const want = desiredByName.get(field.name);
      if (want?.optional === true && !field.optional) {
        patches.push({ name: field.name, type: field.type, optional: true });
      }
    }

    return patches;
  }
}