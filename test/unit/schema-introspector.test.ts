import { describe, expect, it } from "vitest";

import { MysqlSchemaIntrospector } from "../../src/modules/mysql/schema-introspector.js";

// inferTypesenseType is a pure method — no DB connection needed
const introspector = new MysqlSchemaIntrospector(null as never);

describe("MysqlSchemaIntrospector.inferTypesenseType", () => {
  it.each([
    // Boolean
    ["tinyint(1)", { type: "bool" }],
    ["bool", { type: "bool" }],
    ["boolean", { type: "bool" }],
    // Integer
    ["int(11)", { type: "int64" }],
    ["int", { type: "int64" }],
    ["bigint(20) unsigned", { type: "int64" }],
    ["smallint(6)", { type: "int64" }],
    ["mediumint(8)", { type: "int64" }],
    ["tinyint(4)", { type: "int64" }],
    // Float
    ["decimal(10,2)", { type: "float" }],
    ["numeric(5,2)", { type: "float" }],
    ["float", { type: "float" }],
    ["double", { type: "float" }],
    // Datetime
    ["datetime", { type: "int64", sourceFormat: "datetime" }],
    ["timestamp", { type: "int64", sourceFormat: "datetime" }],
    ["date", { type: "int64", sourceFormat: "datetime" }],
    // JSON
    ["json", { type: "auto", sourceFormat: "json" }],
    // Set
    ["set('a','b','c')", { type: "string[]" }],
    // String fallback
    ["varchar(255)", { type: "string" }],
    ["text", { type: "string" }],
    ["longtext", { type: "string" }],
    ["char(10)", { type: "string" }],
    ["enum('x','y')", { type: "string" }],
  ] as [string, { type: string; sourceFormat?: string }][])(
    "maps %s → %o",
    (mysqlType, expected) => {
      expect(introspector.inferTypesenseType(mysqlType)).toEqual(expected);
    }
  );
});
