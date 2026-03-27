import type { Client } from "typesense";

import type { TableSyncConfig } from "../../core/types.js";

export class TypesenseCollectionManager {
  constructor(private readonly client: Client) {}

  async ensureCollection(table: TableSyncConfig): Promise<void> {
    const fields = table.typesense.fields.some((field) => field.name === "id")
      ? table.typesense.fields
      : [{ name: "id", type: "string" as const }, ...table.typesense.fields];

    try {
      await this.client.collections(table.collection).retrieve();
    } catch {
      await this.client.collections().create({
        name: table.collection,
        fields,
        default_sorting_field: table.typesense.defaultSortingField,
        enable_nested_fields: table.typesense.enableNestedFields,
        token_separators: table.typesense.tokenSeparators,
        symbols_to_index: table.typesense.symbolsToIndex
      });
    }
  }
}