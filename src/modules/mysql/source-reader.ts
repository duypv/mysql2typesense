import type { Pool, RowDataPacket } from "mysql2/promise";

import type { TableSyncConfig } from "../../core/types.js";

function quoteIdentifier(value: string) {
  return `\`${value.replace(/`/g, "``")}\``;
}

export class MySqlSourceReader {
  constructor(private readonly pool: Pool) {}

  async *scanTable(table: TableSyncConfig, batchSize: number): AsyncGenerator<Record<string, unknown>[]> {
    let cursor: string | number | null = null;
    const tableName = `${quoteIdentifier(table.database)}.${quoteIdentifier(table.table)}`;
    const primaryKey = quoteIdentifier(table.primaryKey);

    while (true) {
      const [rows] = await this.pool.query<RowDataPacket[]>(
        cursor === null
          ? `SELECT * FROM ${tableName} ORDER BY ${primaryKey} ASC LIMIT ?`
          : `SELECT * FROM ${tableName} WHERE ${primaryKey} > ? ORDER BY ${primaryKey} ASC LIMIT ?`,
        cursor === null ? [batchSize] : [cursor, batchSize]
      );

      if (rows.length === 0) {
        break;
      }

      yield rows as Record<string, unknown>[];

      const lastRow = rows[rows.length - 1] as Record<string, unknown>;
      const nextCursor = lastRow[table.primaryKey];
      cursor = typeof nextCursor === "bigint" ? Number(nextCursor) : (nextCursor as string | number);
    }
  }
}