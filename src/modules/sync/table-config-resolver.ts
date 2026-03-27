import type { TableSyncConfig, TableSyncConfigSeed, TypesenseFieldConfig } from "../../core/types.js";
import { MysqlSchemaIntrospector } from "../mysql/schema-introspector.js";

function mergeTypesenseFields(
  inferred: TypesenseFieldConfig[],
  configured: TypesenseFieldConfig[] | undefined
): TypesenseFieldConfig[] {
  if (!configured || configured.length === 0) {
    return inferred;
  }

  const byName = new Map(inferred.map((field) => [field.name, field]));
  for (const field of configured) {
    byName.set(field.name, { ...byName.get(field.name), ...field });
  }

  return Array.from(byName.values());
}

function inferDefaultSortingField(fields: TypesenseFieldConfig[]): string | undefined {
  const candidate = fields.find((field) => field.sort && (field.type === "int64" || field.type === "int32"));
  return candidate?.name;
}

export async function resolveTableConfigs(
  introspector: MysqlSchemaIntrospector,
  defaultDatabase: string,
  seeds: TableSyncConfigSeed[]
): Promise<TableSyncConfig[]> {
  const effectiveSeeds: TableSyncConfigSeed[] =
    seeds.length > 0
      ? seeds
      : (await introspector.listTables(defaultDatabase)).map((table) => ({
          table,
          database: defaultDatabase
        }));

  const resolved: TableSyncConfig[] = [];

  for (const seed of effectiveSeeds) {
    const database = seed.database ?? defaultDatabase;
    const table = seed.table;
    const collection = seed.collection ?? table;
    const columns = await introspector.getColumns(database, table);

    if (columns.length === 0) {
      continue;
    }

    const inferredPrimary = columns.find((column) => column.primary)?.name ?? columns[0].name;
    const primaryKey = seed.primaryKey ?? inferredPrimary;

    const configuredMappings = seed.transform?.fieldMappings;
    const fieldMappings =
      configuredMappings && configuredMappings.length > 0
        ? configuredMappings
        : columns.map((column) => {
            const inferred = introspector.inferTypesenseType(column.mysqlType);
            return {
              source: column.name,
              target: column.name,
              type: inferred.type,
              optional: column.nullable || undefined,
              sourceFormat: inferred.sourceFormat,
              timestampResolution: inferred.sourceFormat === "datetime" ? ("seconds" as const) : undefined
            };
          });

    const configuredFields = seed.typesense?.fields;
    let fields: TypesenseFieldConfig[];

    if (configuredFields && configuredFields.length > 0) {
      // If configured fields are explicitly provided, use only those + fields referenced in transform mappings.
      const mappingTargets = new Set(fieldMappings.map((m) => m.target));
      const configuredNames = new Set(configuredFields.map((f) => f.name));

      fields = configuredFields;

      // Add any target fields from mappings that are not already in configured fields.
      for (const mapping of fieldMappings) {
        if (!configuredNames.has(mapping.target) && !fields.some((f) => f.name === mapping.target)) {
          fields.push({
            name: mapping.target,
            type: mapping.type,
            optional: mapping.optional
          });
        }
      }
    } else {
      // No configured fields: infer from all columns.
      const inferredFields: TypesenseFieldConfig[] = columns.map((column) => {
        const inferredType = introspector.inferTypesenseType(column.mysqlType);
        return {
          name: column.name,
          type: inferredType.type,
          optional: column.nullable || undefined,
          sort: column.name === primaryKey ? true : undefined
        };
      });
      fields = mergeTypesenseFields(inferredFields, undefined);
    }

    if (!fields.some((field) => field.name === "id")) {
      fields.unshift({ name: "id", type: "string" });
    }

    resolved.push({
      database,
      table,
      primaryKey,
      collection,
      batchSize: seed.batchSize,
      typesense: {
        fields,
        defaultSortingField: seed.typesense?.defaultSortingField ?? inferDefaultSortingField(fields),
        enableNestedFields: seed.typesense?.enableNestedFields ?? true,
        tokenSeparators: seed.typesense?.tokenSeparators,
        symbolsToIndex: seed.typesense?.symbolsToIndex
      },
      transform: {
        fieldMappings,
        dropNulls: seed.transform?.dropNulls ?? true
      }
    });
  }

  return resolved;
}
