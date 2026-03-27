import type { DatabaseSyncConfig, TableSyncConfig, TableSyncConfigSeed, TypesenseFieldConfig } from "../../core/types.js";
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
  const sortableNumeric = fields.filter(
    (field) => field.name !== "id" && (field.type === "int64" || field.type === "int32" || field.type === "float")
  );

  const preferred = sortableNumeric.find((field) => field.name === "updated_at_ts" || field.name === "created_at_ts");
  if (preferred) {
    return preferred.name;
  }

  const explicitSort = sortableNumeric.find((field) => field.sort);
  if (explicitSort) {
    return explicitSort.name;
  }

  return sortableNumeric[0]?.name;
}

function withStringInfixDefaults(fields: TypesenseFieldConfig[], enabled: boolean): TypesenseFieldConfig[] {
  if (!enabled) {
    return fields;
  }

  return fields.map((field) => {
    if ((field.type === "string" || field.type === "string*") && field.infix === undefined) {
      return { ...field, infix: true };
    }

    return field;
  });
}

export async function resolveTableConfigs(
  introspector: MysqlSchemaIntrospector,
  defaultDatabase: string,
  seeds: TableSyncConfigSeed[],
  databaseConfig?: DatabaseSyncConfig
): Promise<TableSyncConfig[]> {
  const resolvedDefaultDatabase = databaseConfig?.name ?? defaultDatabase;
  const excludedFieldNames = new Set((databaseConfig?.excludeFields ?? []).map((name) => name.toLowerCase()));
  const infixStringEnabled = databaseConfig?.infixString === true;

  const effectiveSeeds: TableSyncConfigSeed[] =
    seeds.length > 0
      ? seeds
      : (await introspector.listTables(resolvedDefaultDatabase)).map((table) => ({
          table,
          database: resolvedDefaultDatabase
        }));

  const resolved: TableSyncConfig[] = [];

  for (const seed of effectiveSeeds) {
    const database = seed.database ?? resolvedDefaultDatabase;
    const table = seed.table;
    const collection = seed.collection ?? table;
    const columns = (await introspector.getColumns(database, table)).filter(
      (column) => !excludedFieldNames.has(column.name.toLowerCase())
    );

    if (columns.length === 0) {
      continue;
    }

    const inferredPrimary = columns.find((column) => column.primary)?.name ?? columns[0].name;
    const primaryKey = seed.primaryKey ?? inferredPrimary;

    const configuredMappings = seed.transform?.fieldMappings;
    const fieldMappings =
      configuredMappings && configuredMappings.length > 0
        ? configuredMappings.filter(
            (mapping) =>
              !excludedFieldNames.has(mapping.source.toLowerCase()) &&
              !excludedFieldNames.has(mapping.target.toLowerCase())
          )
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

    const configuredFields = seed.typesense?.fields?.filter(
      (field) => !excludedFieldNames.has(field.name.toLowerCase())
    );
    let fields: TypesenseFieldConfig[];

    if (configuredFields && configuredFields.length > 0) {
      // If configured fields are explicitly provided, use only those + fields referenced in transform mappings.
      const mappingTargets = new Set(fieldMappings.map((m) => m.target));
      const configuredNames = new Set(configuredFields.map((f) => f.name));

      fields = [...configuredFields];

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
          sort: column.name === primaryKey && column.name !== "id" ? true : undefined
        };
      });
      fields = mergeTypesenseFields(inferredFields, undefined);
    }

    if (!fields.some((field) => field.name === "id")) {
      fields.unshift({ name: "id", type: "string" });
    }

    fields = withStringInfixDefaults(fields, infixStringEnabled);

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
