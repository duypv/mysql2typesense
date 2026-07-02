import { wallClockUtcToInstant } from "../../utils/timezone.js";

/**
 * Shape of the column schema rows zongji attaches to tablemap events
 * (queried from information_schema.COLUMNS).
 */
export interface BinlogColumnSchema {
  COLUMN_NAME: string;
  COLUMN_TYPE: string;
}

// DATETIME and DATE are timezone-naive wall-clock types that zongji decodes as
// Date.UTC(wall components). TIMESTAMP is stored as a true epoch and is decoded
// correctly, so it must NOT be shifted.
const WALL_CLOCK_TYPE = /^(datetime|date)\b/i;

/** Returns the names of DATETIME/DATE columns from a tablemap's column schemas. */
export function collectWallClockColumns(columnSchemas: BinlogColumnSchema[] | undefined): string[] {
  if (!columnSchemas) {
    return [];
  }

  return columnSchemas
    .filter((schema) => WALL_CLOCK_TYPE.test(schema.COLUMN_TYPE))
    .map((schema) => schema.COLUMN_NAME);
}

/**
 * Mutates a binlog row in place: shifts wall-clock Date values (DATETIME/DATE)
 * from "wall clock read as UTC" to the true absolute instant in the given
 * source timezone. Null/non-Date values are left untouched.
 */
export function normalizeBinlogRow(
  row: Record<string, unknown>,
  wallClockColumns: string[],
  timezone: string
): void {
  for (const column of wallClockColumns) {
    const value = row[column];
    if (value instanceof Date) {
      row[column] = wallClockUtcToInstant(value, timezone);
    }
  }
}
