import type { Pool, RowDataPacket } from "mysql2/promise";

import type { TypesenseFieldType } from "../../core/types.js";

export interface MysqlColumnMeta {
  name: string;
  mysqlType: string;
  nullable: boolean;
  primary: boolean;
}

export class MysqlSchemaIntrospector {
  constructor(private readonly pool: Pool) {}

  async listTables(database: string): Promise<string[]> {
    const [rows] = await this.pool.query<RowDataPacket[]>(
      `SELECT TABLE_NAME AS tableName
       FROM information_schema.tables
       WHERE TABLE_SCHEMA = ? AND TABLE_TYPE = 'BASE TABLE'
       ORDER BY TABLE_NAME ASC`,
      [database]
    );

    return rows.map((row) => String(row.tableName));
  }

  async getColumns(database: string, table: string): Promise<MysqlColumnMeta[]> {
    const [rows] = await this.pool.query<RowDataPacket[]>(
      `SELECT
         COLUMN_NAME AS columnName,
         DATA_TYPE AS dataType,
         IS_NULLABLE AS isNullable,
         COLUMN_KEY AS columnKey,
         COLUMN_TYPE AS columnType
       FROM information_schema.columns
       WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
       ORDER BY ORDINAL_POSITION ASC`,
      [database, table]
    );

    return rows.map((row) => {
      const dataType = String(row.dataType).toLowerCase();
      const columnType = String(row.columnType).toLowerCase();
      return {
        name: String(row.columnName),
        mysqlType: columnType || dataType,
        nullable: String(row.isNullable).toUpperCase() === "YES",
        primary: String(row.columnKey).toUpperCase() === "PRI"
      };
    });
  }

  inferTypesenseType(mysqlType: string): { type: TypesenseFieldType; sourceFormat?: "datetime" | "json" } {
    const type = mysqlType.toLowerCase();

    if (/^tinyint\(1\)/.test(type) || type === "boolean" || type === "bool") {
      return { type: "bool" };
    }

    if (/(int|bigint|smallint|mediumint)/.test(type)) {
      return { type: "int64" };
    }

    if (/(decimal|numeric|float|double)/.test(type)) {
      return { type: "float" };
    }

    if (/(datetime|timestamp|date)/.test(type)) {
      return { type: "int64", sourceFormat: "datetime" };
    }

    if (/(json)/.test(type)) {
      return { type: "object", sourceFormat: "json" };
    }

    if (/(set)/.test(type)) {
      return { type: "string[]" };
    }

    return { type: "string" };
  }
}
