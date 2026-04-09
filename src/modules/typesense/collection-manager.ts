import type { Logger } from "pino";
import type { Client } from "typesense";

import type { TableSyncConfig, TypesenseFieldConfig } from "../../core/types.js";

export class TypesenseCollectionManager {
  private readonly synced = new Set<string>();

  constructor(
    private readonly client: Client,
    private readonly logger?: Logger
  ) {}

  /**
   * Ensures the Typesense collection for `table` matches the desired schema.
   *
   * @param forceRecreate When true, always drop+recreate the collection even if the
   *   schema looks structurally correct. Use this for join-target collections during
   *   initial sync: a field can appear non-optional in the API response yet still fail
   *   Typesense v30 join validation (e.g. if it was patched in from a very old run with
   *   an incompatible type). Forced recreation guarantees a clean, canonical schema.
   */
  async ensureCollection(table: TableSyncConfig, forceRecreate = false): Promise<void> {
    const fields = table.typesense.fields.some((field) => field.name === "id")
      ? table.typesense.fields
      : [{ name: "id", type: "string" as const }, ...table.typesense.fields];

    const createCollection = async () => {
      await this.client.collections().create({
        name: table.collection,
        fields,
        default_sorting_field: table.typesense.defaultSortingField,
        enable_nested_fields: table.typesense.enableNestedFields,
        token_separators: table.typesense.tokenSeparators,
        symbols_to_index: table.typesense.symbolsToIndex
      });
      this.synced.add(table.collection);
    };

    let existing: Awaited<ReturnType<ReturnType<(typeof this.client)["collections"]>["retrieve"]>> | null = null;
    try {
      existing = await this.client.collections(table.collection).retrieve();
    } catch {
      // Collection does not exist yet — create it below.
    }

    if (!existing) {
      this.logger?.info({ collection: table.collection }, "ensureCollection: creating (new)");
      await createCollection();
      return;
    }

    if (this.synced.has(table.collection)) {
      return;
    }

    // Forced recreation requested (e.g. join target during initial sync).
    if (forceRecreate) {
      this.logger?.info({ collection: table.collection }, "ensureCollection: drop+recreate (forced — join target schema refresh)");
      await this.client.collections(table.collection).delete();
      await createCollection();
      return;
    }

    // Typesense only allows patching in new fields as optional=true.
    // If the desired schema has non-optional fields that are missing from the existing
    // collection (e.g. a primary-key field used as a join reference target), patching
    // would leave them as optional, causing Typesense v30 join validation to reject them
    // with "Referenced field X not found in collection Y". In that case, drop and recreate
    // so the field is defined correctly from the start.
    //
    // We also drop+recreate when a required field already EXISTS in the collection but
    // was previously patched in as optional=true (Typesense forces optional on all
    // patch-added fields). Typesense does not support changing optional→required via
    // PATCH, so a full drop+recreate is the only fix.
    const existingByName = new Map(
      (existing.fields ?? [])
        .filter((f) => !!f.name)
        .map((f) => [f.name!, f])
    );
    const existingFieldNames = new Set(existingByName.keys());

    const missingRequiredField = fields.find(
      (want) => !want.optional && !existingFieldNames.has(want.name)
    );
    const requiredButOptionalField = fields.find((want) => {
      if (want.optional) return false;
      const ex = existingByName.get(want.name);
      return ex !== undefined && ex.optional === true;
    });

    if (missingRequiredField ?? requiredButOptionalField) {
      const reason = missingRequiredField
        ? `required field "${missingRequiredField.name}" is missing from existing collection`
        : `required field "${requiredButOptionalField!.name}" exists but is optional in existing collection (was force-patched in a prior run)`;
      this.logger?.info({ collection: table.collection, reason }, "ensureCollection: drop+recreate");
      await this.client.collections(table.collection).delete();
      await createCollection();
      return;
    }

    const updates = this.diffSchemaChanges(existing.fields ?? [], fields);
    if (updates.length > 0) {
      this.logger?.info({ collection: table.collection, patchCount: updates.length }, "ensureCollection: patching schema");
      try {
        await this.client.collections(table.collection).update({ fields: updates as any });
      } catch {
        // Schema update may not be supported for some field types; continue anyway.
        // The document-indexer fallback to partial update handles missing fields.
      }
    } else {
      this.logger?.info({ collection: table.collection }, "ensureCollection: schema up-to-date, kept as-is");
    }
    this.synced.add(table.collection);
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
   * Returns field patches needed to bring the existing Typesense collection schema in line
   * with the desired config. Handles two cases:
   *
   * 1. `optional` flag mismatch — patch with the same type + optional=true.
   * 2. Field type mismatch (e.g. int64 → string caused by a join reference config) —
   *    drop the old field then re-add it with the new type.  Typesense supports both
   *    operations in a single PATCH via `{ name, drop: true }` + the new field object.
   */
  private diffSchemaChanges(
    existing: Array<{
      name?: string;
      type?: string;
      optional?: boolean;
      reference?: string;
      async_reference?: boolean;
      facet?: boolean;
      index?: boolean;
      sort?: boolean;
      infix?: boolean;
    }>,
    desired: TypesenseFieldConfig[]
  ): Array<Record<string, unknown>> {
    const desiredByName = new Map(desired.map((f) => [f.name, f]));
    const patches: Array<Record<string, unknown>> = [];

    for (const field of existing) {
      if (!field.name || !field.type) continue;
      const want = desiredByName.get(field.name);
      if (!want) continue;

      const typeMismatch = want.type !== field.type;
      const optionalMismatch = want.optional === true && !field.optional;
      // Becoming required when currently optional cannot be fixed by PATCH — needs drop+recreate.
      // The collection-level check in ensureCollection triggers the recreate; this flag is here
      // for completeness so the diff result is consistent if diffSchemaChanges is called directly.
      const requiredOptionalMismatch = !want.optional && field.optional === true;
      const referenceMismatch = want.reference !== undefined && want.reference !== field.reference;
      const asyncReferenceMismatch =
        want.async_reference !== undefined && want.async_reference !== field.async_reference;
      const facetMismatch = want.facet !== undefined && want.facet !== field.facet;
      const indexMismatch = want.index !== undefined && want.index !== field.index;
      const sortMismatch = want.sort !== undefined && want.sort !== field.sort;
      const infixMismatch = want.infix !== undefined && want.infix !== field.infix;

      const requiresDropRecreate = typeMismatch || referenceMismatch || asyncReferenceMismatch || requiredOptionalMismatch;
      const requiresPatchUpdate = optionalMismatch || facetMismatch || indexMismatch || sortMismatch || infixMismatch;

      if (requiresDropRecreate) {
        // Drop the old field and re-add with the correct type.
        patches.push({ name: field.name, drop: true });
        patches.push({
          name: want.name,
          type: want.type,
          optional: want.optional ?? true,
          ...(want.reference !== undefined && { reference: want.reference }),
          ...(want.async_reference !== undefined && { async_reference: want.async_reference }),
          ...(want.facet !== undefined && { facet: want.facet }),
          ...(want.index !== undefined && { index: want.index }),
          ...(want.sort !== undefined && { sort: want.sort }),
          ...(want.infix !== undefined && { infix: want.infix })
        });
      } else if (requiresPatchUpdate) {
        patches.push({
          name: field.name,
          type: field.type,
          optional: want.optional ?? field.optional,
          ...(want.facet !== undefined && { facet: want.facet }),
          ...(want.index !== undefined && { index: want.index }),
          ...(want.sort !== undefined && { sort: want.sort }),
          ...(want.infix !== undefined && { infix: want.infix })
        });
      }
    }

    // Also add entirely new fields that exist in desired but not in existing.
    const existingNames = new Set(existing.map((f) => f.name).filter(Boolean));
    for (const want of desired) {
      if (!existingNames.has(want.name)) {
        patches.push({
          name: want.name,
          type: want.type,
          optional: want.optional ?? true,
          ...(want.reference !== undefined && { reference: want.reference }),
          ...(want.async_reference !== undefined && { async_reference: want.async_reference }),
          ...(want.facet !== undefined && { facet: want.facet }),
          ...(want.index !== undefined && { index: want.index }),
          ...(want.sort !== undefined && { sort: want.sort }),
          ...(want.infix !== undefined && { infix: want.infix })
        });
      }
    }

    return patches;
  }}
