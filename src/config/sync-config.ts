import { readFileSync } from "node:fs";
import { resolve } from "node:path";

import { z } from "zod";

import type { SyncConfigFile, TableSyncConfigSeed } from "../core/types.js";

const typesenseFieldTypeSchema = z.enum([
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
]);

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
  range_index: z.boolean().optional()
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
  tables: z.array(tableSchema).optional()
});

export function loadTableSyncConfig(configPath: string): TableSyncConfigSeed[] {
  try {
    const absolutePath = resolve(configPath);
    const content = readFileSync(absolutePath, "utf8");
    const parsed = JSON.parse(content) as SyncConfigFile;
    const result = syncConfigSchema.parse(parsed);
    return result.tables ?? [];
  } catch (error) {
    const nodeError = error as NodeJS.ErrnoException;
    if (nodeError.code === "ENOENT") {
      return [];
    }

    throw error;
  }
}
