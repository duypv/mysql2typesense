import { readFileSync } from "node:fs";
import { resolve } from "node:path";

import { z } from "zod";

import type { DatabaseSyncConfig, JoinFieldConfig, SyncConfigFile, TableJoinConfig, TableSyncConfigSeed } from "../core/types.js";

/** Maps common MySQL / Java type aliases to their Typesense equivalents. */
const TYPE_ALIASES: Record<string, string> = {
  // integer variants
  long: "int64",
  bigint: "int64",
  mediumint: "int64",
  smallint: "int32",
  tinyint: "int32",
  integer: "int32",
  int: "int32",
  // float variants
  double: "float",
  decimal: "float",
  numeric: "float",
  real: "float",
  // string variants
  varchar: "string",
  text: "string",
  char: "string",
  // bool variants
  boolean: "bool",
  bit: "bool"
};

function normalizeFieldType(v: unknown): unknown {
  if (typeof v !== "string") return v;
  const lower = v.toLowerCase();
  return TYPE_ALIASES[lower] ?? v;
}

const typesenseFieldTypeSchema = z.preprocess(
  normalizeFieldType,
  z.enum([
    "string",
    "int32",
    "int64",
    "float",
    "bool",
    "geopoint",
    "geopolygon",
    "geopoint[]",
    "string[]",
    "int32[]",
    "int64[]",
    "float[]",
    "bool[]",
    "object",
    "object[]",
    "auto",
    "string*",
    "image"
  ])
);

const fieldSchema = z.object({
  name: z.string().min(1),
  type: typesenseFieldTypeSchema,
  optional: z.boolean().optional(),
  facet: z.boolean().optional(),
  index: z.boolean().optional(),
  sort: z.boolean().optional(),
  locale: z.string().optional(),
  infix: z.boolean().optional(),
  stem: z.boolean().optional(),
  num_dim: z.number().int().positive().optional(),
  store: z.boolean().optional(),
  range_index: z.boolean().optional(),
  reference: z.string().optional(),
  async_reference: z.boolean().optional()
});

const joinFieldSchema = z.object({
  name: z.string().min(1),
  reference: z
    .string()
    .min(1)
    .regex(/^[^.]+\.[^.]+$/, 'reference must be in format "CollectionName.fieldName"')
    .optional(),
  async_reference: z.boolean().optional(),
  type: typesenseFieldTypeSchema.optional()
});

const tableJoinConfigSchema = z.object({
  table: z.string().min(1),
  fields: z.array(joinFieldSchema).min(1)
});

const mappingSchema = z.object({
  source: z.string().min(1),
  target: z.string().min(1),
  type: typesenseFieldTypeSchema,
  optional: z.boolean().optional(),
  defaultValue: z.unknown().optional(),
  sourceFormat: z.enum(["plain", "json", "csv", "datetime"]).optional(),
  arraySeparator: z.string().optional(),
  timestampResolution: z.enum(["seconds", "milliseconds"]).optional()
});

const tableSchema = z.object({
  database: z.string().min(1).optional(),
  table: z.string().min(1),
  primaryKey: z.string().min(1).optional(),
  collection: z.string().min(1).optional(),
  batchSize: z.number().int().positive().optional(),
  typesense: z
    .object({
      fields: z.array(fieldSchema).optional(),
    defaultSortingField: z.string().optional(),
    enableNestedFields: z.boolean().optional(),
    tokenSeparators: z.array(z.string()).optional(),
    symbolsToIndex: z.array(z.string()).optional()
    })
    .optional(),
  transform: z
    .object({
      fieldMappings: z.array(mappingSchema).optional(),
    dropNulls: z.boolean().optional()
    })
    .optional()
});

const syncConfigSchema = z.object({
  database: z
    .object({
      name: z.string().min(1).optional(),
      excludeFields: z.array(z.string().min(1)).optional(),
      infix_string: z.boolean().optional(),
      json_stringify: z.array(z.string().min(1)).optional(),
      facet_fields: z.array(z.string().min(1)).optional()
    })
    .optional(),
  tables: z.array(tableSchema).optional(),
  join_configs: z.array(tableJoinConfigSchema).optional()
});

export interface LoadedSyncConfig {
  database?: DatabaseSyncConfig;
  tables: TableSyncConfigSeed[];
  joinConfigs: TableJoinConfig[];
}

export function loadSyncConfig(configPath: string): LoadedSyncConfig {
  try {
    const absolutePath = resolve(configPath);
    const content = readFileSync(absolutePath, "utf8");
    const parsed = JSON.parse(content) as SyncConfigFile;
    const result = syncConfigSchema.parse(parsed);
    const joinConfigs: TableJoinConfig[] = (result.join_configs ?? []).map((jc) => ({
      table: jc.table,
      fields: jc.fields.map(
        (f): JoinFieldConfig => ({
          name: f.name,
          reference: f.reference,
          async_reference: f.async_reference,
          type: f.type
        })
      )
    }));

    return {
      database: result.database
        ? {
            name: result.database.name,
            excludeFields: result.database.excludeFields,
            infixString: result.database.infix_string,
            jsonStringify: result.database.json_stringify,
            facetFields: result.database.facet_fields
          }
        : undefined,
      tables: result.tables ?? [],
      joinConfigs
    };
  } catch (error) {
    const nodeError = error as NodeJS.ErrnoException;
    if (nodeError.code === "ENOENT") {
      return { tables: [], joinConfigs: [] };
    }

    throw error;
  }
}
