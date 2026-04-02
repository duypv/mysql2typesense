import type {
  DatabaseSyncConfig,
  JoinFieldConfig,
  TableJoinConfig,
  TableSyncConfig,
  TableSyncConfigSeed,
  TransformFieldMapping,
  TypesenseFieldConfig
} from "../../core/types.js";
import { MysqlSchemaIntrospector } from "../mysql/schema-introspector.js";

function matchesPattern(input: string, pattern: string): boolean {
  const value = input.toLowerCase();
  const rule = pattern.toLowerCase();

  const startsWithStar = rule.startsWith("*");
  const endsWithStar = rule.endsWith("*");
  const token = rule.replace(/^\*|\*$/g, "");

  if (!startsWithStar && !endsWithStar) {
    return value === rule;
  }

  if (startsWithStar && endsWithStar) {
    return token.length > 0 ? value.includes(token) : false;
  }

  if (startsWithStar) {
    return token.length > 0 ? value.endsWith(token) : false;
  }

  return token.length > 0 ? value.startsWith(token) : false;
}

function matchesAnyPattern(input: string, patterns: string[]): boolean {
  return patterns.some((pattern) => matchesPattern(input, pattern));
}

function applyJsonStringifyMappings(
  mappings: Array<{
    source: string;
    target: string;
    type: TypesenseFieldConfig["type"];
    optional?: boolean;
    sourceFormat?: "plain" | "json" | "csv" | "datetime";
    timestampResolution?: "seconds" | "milliseconds";
  }>,
  patterns: string[]
) {
  if (patterns.length === 0) {
    return mappings;
  }

  return mappings.map((mapping) => {
    if (!matchesAnyPattern(mapping.source, patterns)) {
      return mapping;
    }

    return {
      ...mapping,
      type: "auto" as const,
      sourceFormat: "json" as const
    };
  });
}

function applyFacetRules(fields: TypesenseFieldConfig[], facetPatterns: string[]): TypesenseFieldConfig[] {
  return fields.map((field) => {
    const facet = facetPatterns.length > 0 && matchesAnyPattern(field.name, facetPatterns);

    if (!facet) {
      return field;
    }

    return {
      ...field,
      facet: true
    };
  });
}

function applyMappingTypeOverrides(
  fields: TypesenseFieldConfig[],
  mappings: Array<{ target: string; type: TypesenseFieldConfig["type"]; optional?: boolean }>
): TypesenseFieldConfig[] {
  if (mappings.length === 0) {
    return fields;
  }

  const mappingByTarget = new Map(mappings.map((mapping) => [mapping.target, mapping]));

  return fields.map((field) => {
    const mapping = mappingByTarget.get(field.name);
    if (!mapping) {
      return field;
    }

    return {
      ...field,
      type: mapping.type,
      optional: mapping.optional ?? field.optional
    };
  });
}

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
    (field) =>
      field.name !== "id" &&
      field.optional !== true &&
      (field.type === "int64" || field.type === "int32" || field.type === "float")
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

function resolveValidatedDefaultSortingField(
  fields: TypesenseFieldConfig[],
  configuredDefault?: string
): string | undefined {
  if (configuredDefault) {
    const field = fields.find((item) => item.name === configuredDefault);
    if (
      field &&
      field.optional !== true &&
      (field.type === "int64" || field.type === "int32" || field.type === "float")
    ) {
      return configuredDefault;
    }
  }

  return inferDefaultSortingField(fields);
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

/**
 * Apply join reference configs: for fields listed in joinConfigs for this table,
 * set `reference` (and optionally `async_reference` and `type`) on the matching field.
 * If the field doesn't exist in the schema yet, add it as an optional int64 field.
 */
function applyJoinFieldConfigs(
  fields: TypesenseFieldConfig[],
  table: string,
  joinConfigs: TableJoinConfig[]
): TypesenseFieldConfig[] {
  const tableJoin = joinConfigs.find(
    (jc) => jc.table.toLowerCase() === table.toLowerCase()
  );

  if (!tableJoin || tableJoin.fields.length === 0) {
    return fields;
  }

  const result = [...fields];

  for (const joinField of tableJoin.fields) {
    const idx = result.findIndex((f) => f.name.toLowerCase() === joinField.name.toLowerCase());
    // Typesense v26 join reference fields MUST be type string regardless of MySQL column type.
    // Only override if the user explicitly sets a type in join_configs.
    const resolvedType = joinField.type ?? "string";
    const patch: Partial<JoinFieldConfig> = {
      reference: joinField.reference,
      type: resolvedType,
      ...(joinField.async_reference !== undefined && { async_reference: joinField.async_reference })
    };

    if (idx !== -1) {
      result[idx] = { ...result[idx], ...patch };
    } else {
      result.push({
        name: joinField.name,
        type: resolvedType,
        optional: true,
        reference: joinField.reference,
        ...(joinField.async_reference !== undefined && { async_reference: joinField.async_reference })
      });
    }
  }

  return result;
}

/**
 * Apply join reference configs to field mappings so the transformer coerces values
 * to the correct type (e.g. string) before indexing.
 */
function applyJoinMappingOverrides(
  mappings: TransformFieldMapping[],
  table: string,
  joinConfigs: TableJoinConfig[]
): TransformFieldMapping[] {
  const tableJoin = joinConfigs.find(
    (jc) => jc.table.toLowerCase() === table.toLowerCase()
  );

  if (!tableJoin || tableJoin.fields.length === 0) {
    return mappings;
  }

  return mappings.map((mapping) => {
    const joinField = tableJoin.fields.find(
      (jf) => jf.name.toLowerCase() === mapping.source.toLowerCase()
    );
    if (!joinField) return mapping;
    // Default to string — Typesense join reference fields must be strings.
    const resolvedType = joinField.type ?? "string";
    return { ...mapping, type: resolvedType };
  });
}

export async function resolveTableConfigs(
  introspector: MysqlSchemaIntrospector,
  defaultDatabase: string,
  seeds: TableSyncConfigSeed[],
  databaseConfig?: DatabaseSyncConfig,
  joinConfigs: TableJoinConfig[] = []
): Promise<TableSyncConfig[]> {
  const resolvedDefaultDatabase = databaseConfig?.name ?? defaultDatabase;
  const excludedFieldNames = new Set((databaseConfig?.excludeFields ?? []).map((name) => name.toLowerCase()));
  const infixStringEnabled = databaseConfig?.infixString === true;
  const jsonStringifyPatterns = databaseConfig?.jsonStringify ?? [];
  const facetPatterns = databaseConfig?.facetFields ?? [];

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
    let fieldMappings =
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

    fieldMappings = applyJsonStringifyMappings(fieldMappings, jsonStringifyPatterns);

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
      // Mark all fields as optional because binlog UPDATE events may only contain
      // changed columns (when MySQL binlog-row-image != FULL), producing partial documents.
      const inferredFields: TypesenseFieldConfig[] = columns.map((column) => {
        const inferredType = introspector.inferTypesenseType(column.mysqlType);
        return {
          name: column.name,
          type: inferredType.type,
          optional: column.name !== primaryKey ? true : undefined,
          sort: column.name === primaryKey && column.name !== "id" ? true : undefined
        };
      });
      fields = mergeTypesenseFields(inferredFields, undefined);
    }

    if (!fields.some((field) => field.name === "id")) {
      fields.unshift({ name: "id", type: "string" });
    }

    fields = applyMappingTypeOverrides(fields, fieldMappings);

    fields = applyJoinFieldConfigs(fields, table, joinConfigs);
    fieldMappings = applyJoinMappingOverrides(fieldMappings, table, joinConfigs);

    fields = applyFacetRules(fields, facetPatterns);
    fields = withStringInfixDefaults(fields, infixStringEnabled);

    resolved.push({
      database,
      table,
      primaryKey,
      collection,
      batchSize: seed.batchSize,
      typesense: {
        fields,
        defaultSortingField: resolveValidatedDefaultSortingField(fields, seed.typesense?.defaultSortingField),
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
