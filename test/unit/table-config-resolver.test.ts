import { describe, expect, it, vi } from "vitest";

import type { TableJoinConfig, TableSyncConfigSeed } from "../../src/core/types.js";
import { MysqlSchemaIntrospector } from "../../src/modules/mysql/schema-introspector.js";
import { resolveTableConfigs } from "../../src/modules/sync/table-config-resolver.js";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

type ColumnSpec = {
  name: string;
  mysqlType: string;
  nullable?: boolean;
  primary?: boolean;
};

function makeIntrospector(columns: ColumnSpec[]): MysqlSchemaIntrospector {
  const real = new MysqlSchemaIntrospector(null as never);
  vi.spyOn(real, "listTables").mockResolvedValue([]);
  vi.spyOn(real, "getColumns").mockResolvedValue(
    columns.map((c) => ({
      name: c.name,
      mysqlType: c.mysqlType,
      nullable: c.nullable ?? false,
      primary: c.primary ?? false
    }))
  );
  return real;
}

const DEFAULT_COLS: ColumnSpec[] = [
  { name: "id", mysqlType: "int(11)", primary: true },
  { name: "name", mysqlType: "varchar(255)" },
  { name: "score", mysqlType: "int(11)" },
  { name: "created_at", mysqlType: "datetime" }
];

const SEED: TableSyncConfigSeed = { table: "Users", database: "app" };

// ---------------------------------------------------------------------------
// Basic resolution
// ---------------------------------------------------------------------------
describe("resolveTableConfigs — basic", () => {
  it("resolves a table from column introspection", async () => {
    const introspector = makeIntrospector(DEFAULT_COLS);
    const configs = await resolveTableConfigs(introspector, "app", [SEED]);
    expect(configs).toHaveLength(1);
    expect(configs[0].table).toBe("Users");
    expect(configs[0].primaryKey).toBe("id");
  });

  it("produces one field mapping per column", async () => {
    const introspector = makeIntrospector(DEFAULT_COLS);
    const [cfg] = await resolveTableConfigs(introspector, "app", [SEED]);
    expect(cfg.transform.fieldMappings).toHaveLength(DEFAULT_COLS.length);
  });

  it("adds a string 'id' field when no column named 'id' exists", async () => {
    // When the PK column is named differently, resolver must inject `id: string`
    const introspector = makeIntrospector([
      { name: "pk", mysqlType: "int(11)", primary: true },
      { name: "name", mysqlType: "varchar(255)" }
    ]);
    const [cfg] = await resolveTableConfigs(introspector, "app", [
      { table: "Users", database: "app", primaryKey: "pk" }
    ]);
    const idField = cfg.typesense.fields.find((f) => f.name === "id");
    expect(idField).toBeDefined();
    expect(idField?.type).toBe("string");
  });

  it("sets collection name to table name when not overridden", async () => {
    const introspector = makeIntrospector(DEFAULT_COLS);
    const [cfg] = await resolveTableConfigs(introspector, "app", [SEED]);
    expect(cfg.collection).toBe("Users");
  });

  it("uses custom collection name from seed", async () => {
    const introspector = makeIntrospector(DEFAULT_COLS);
    const [cfg] = await resolveTableConfigs(introspector, "app", [{ ...SEED, collection: "users_v2" }]);
    expect(cfg.collection).toBe("users_v2");
  });

  it("returns empty array when no columns", async () => {
    const introspector = makeIntrospector([]);
    const configs = await resolveTableConfigs(introspector, "app", [SEED]);
    expect(configs).toHaveLength(0);
  });

  it("uses seed primaryKey if provided", async () => {
    const introspector = makeIntrospector([
      { name: "uuid", mysqlType: "varchar(36)", primary: false },
      { name: "name", mysqlType: "varchar(255)" }
    ]);
    const [cfg] = await resolveTableConfigs(introspector, "app", [{ ...SEED, primaryKey: "uuid" }]);
    expect(cfg.primaryKey).toBe("uuid");
  });
});

// ---------------------------------------------------------------------------
// jsonStringify patterns
// ---------------------------------------------------------------------------
describe("resolveTableConfigs — jsonStringify patterns", () => {
  it("applies json sourceFormat to fields matching pattern", async () => {
    const cols: ColumnSpec[] = [
      { name: "id", mysqlType: "int(11)", primary: true },
      { name: "tagIDs", mysqlType: "varchar(255)" },
      { name: "name", mysqlType: "varchar(255)" }
    ];
    const introspector = makeIntrospector(cols);
    const [cfg] = await resolveTableConfigs(
      introspector,
      "app",
      [SEED],
      { name: "app", jsonStringify: ["*IDs"] }
    );
    const tagMapping = cfg.transform.fieldMappings.find((m) => m.source === "tagIDs");
    expect(tagMapping?.sourceFormat).toBe("json");
    expect(tagMapping?.type).toBe("auto");

    const nameMapping = cfg.transform.fieldMappings.find((m) => m.source === "name");
    expect(nameMapping?.sourceFormat).toBeUndefined();
  });

  it("does not override non-matching fields", async () => {
    const introspector = makeIntrospector(DEFAULT_COLS);
    const [cfg] = await resolveTableConfigs(
      introspector,
      "app",
      [SEED],
      { name: "app", jsonStringify: ["nonExistentPattern"] }
    );
    const nameMapping = cfg.transform.fieldMappings.find((m) => m.source === "name");
    expect(nameMapping?.sourceFormat).toBeUndefined();
  });
});

// ---------------------------------------------------------------------------
// excludeFields
// ---------------------------------------------------------------------------
describe("resolveTableConfigs — excludeFields", () => {
  it("removes excluded fields from mappings and schema", async () => {
    const introspector = makeIntrospector(DEFAULT_COLS);
    const [cfg] = await resolveTableConfigs(
      introspector,
      "app",
      [SEED],
      { name: "app", excludeFields: ["score"] }
    );
    const scoreMapping = cfg.transform.fieldMappings.find((m) => m.source === "score");
    const scoreField = cfg.typesense.fields.find((f) => f.name === "score");
    expect(scoreMapping).toBeUndefined();
    expect(scoreField).toBeUndefined();
  });

  it("is case-insensitive for excludeFields", async () => {
    const introspector = makeIntrospector(DEFAULT_COLS);
    const [cfg] = await resolveTableConfigs(
      introspector,
      "app",
      [SEED],
      { name: "app", excludeFields: ["SCORE"] }
    );
    expect(cfg.transform.fieldMappings.find((m) => m.source === "score")).toBeUndefined();
  });
});

// ---------------------------------------------------------------------------
// facetFields
// ---------------------------------------------------------------------------
describe("resolveTableConfigs — facetFields", () => {
  it("adds facet=true to matching fields", async () => {
    const introspector = makeIntrospector(DEFAULT_COLS);
    const [cfg] = await resolveTableConfigs(
      introspector,
      "app",
      [SEED],
      { name: "app", facetFields: ["name"] }
    );
    const nameField = cfg.typesense.fields.find((f) => f.name === "name");
    expect(nameField?.facet).toBe(true);
  });

  it("does not add facet to non-matching fields", async () => {
    const introspector = makeIntrospector(DEFAULT_COLS);
    const [cfg] = await resolveTableConfigs(
      introspector,
      "app",
      [SEED],
      { name: "app", facetFields: ["name"] }
    );
    const scoreField = cfg.typesense.fields.find((f) => f.name === "score");
    expect(scoreField?.facet).toBeUndefined();
  });

  it("supports wildcard suffix pattern *_at for facet", async () => {
    const introspector = makeIntrospector(DEFAULT_COLS);
    const [cfg] = await resolveTableConfigs(
      introspector,
      "app",
      [SEED],
      { name: "app", facetFields: ["*_at"] }
    );
    const createdField = cfg.typesense.fields.find((f) => f.name === "created_at");
    expect(createdField?.facet).toBe(true);
    const nameField = cfg.typesense.fields.find((f) => f.name === "name");
    expect(nameField?.facet).toBeUndefined();
  });
});

// ---------------------------------------------------------------------------
// infixString
// ---------------------------------------------------------------------------
describe("resolveTableConfigs — infixString", () => {
  it("sets infix=true on string fields when enabled", async () => {
    const introspector = makeIntrospector(DEFAULT_COLS);
    const [cfg] = await resolveTableConfigs(
      introspector,
      "app",
      [SEED],
      { name: "app", infixString: true }
    );
    const nameField = cfg.typesense.fields.find((f) => f.name === "name");
    expect(nameField?.infix).toBe(true);
  });

  it("does not set infix on non-string fields", async () => {
    const introspector = makeIntrospector(DEFAULT_COLS);
    const [cfg] = await resolveTableConfigs(
      introspector,
      "app",
      [SEED],
      { name: "app", infixString: true }
    );
    const scoreField = cfg.typesense.fields.find((f) => f.name === "score");
    expect(scoreField?.infix).toBeUndefined();
  });

  it("does not set infix when disabled", async () => {
    const introspector = makeIntrospector(DEFAULT_COLS);
    const [cfg] = await resolveTableConfigs(
      introspector,
      "app",
      [SEED],
      { name: "app", infixString: false }
    );
    const nameField = cfg.typesense.fields.find((f) => f.name === "name");
    expect(nameField?.infix).toBeUndefined();
  });
});

// ---------------------------------------------------------------------------
// defaultSortingField
// ---------------------------------------------------------------------------
describe("resolveTableConfigs — defaultSortingField", () => {
  // Note: auto-inferred non-PK fields are marked optional:true so they cannot
  // be selected as defaultSortingField. Use explicit typesense.fields to test.
  it("selects updated_at_ts as preferred sorting field", async () => {
    const introspector = makeIntrospector([
      { name: "uuid", mysqlType: "varchar(36)", primary: true }
    ]);
    const [cfg] = await resolveTableConfigs(introspector, "app", [
      {
        table: "Users",
        database: "app",
        typesense: {
          fields: [
            { name: "id", type: "string" },
            { name: "updated_at_ts", type: "int64" },
            { name: "score", type: "int64" }
          ]
        }
      }
    ]);
    expect(cfg.typesense.defaultSortingField).toBe("updated_at_ts");
  });

  it("falls back to first sortable numeric when no preferred field", async () => {
    const introspector = makeIntrospector([
      { name: "uuid", mysqlType: "varchar(36)", primary: true }
    ]);
    const [cfg] = await resolveTableConfigs(introspector, "app", [
      {
        table: "Users",
        database: "app",
        typesense: {
          fields: [
            { name: "id", type: "string" },
            { name: "score", type: "int64" }
          ]
        }
      }
    ]);
    expect(cfg.typesense.defaultSortingField).toBe("score");
  });

  it("returns undefined when no sortable numeric field exists", async () => {
    const cols: ColumnSpec[] = [
      { name: "id", mysqlType: "varchar(36)", primary: true },
      { name: "name", mysqlType: "varchar(255)" }
    ];
    const introspector = makeIntrospector(cols);
    const [cfg] = await resolveTableConfigs(introspector, "app", [{ table: "Users", database: "app" }]);
    expect(cfg.typesense.defaultSortingField).toBeUndefined();
  });
});

// ---------------------------------------------------------------------------
// applyJoinFieldConfigs
// ---------------------------------------------------------------------------
describe("resolveTableConfigs — join configs (applyJoinFieldConfigs)", () => {
  const JOIN_CONFIGS: TableJoinConfig[] = [
    {
      table: "Users",
      fields: [{ name: "deptId", reference: "Department.id", async_reference: true }]
    }
  ];

  it("adds reference to existing field", async () => {
    const cols: ColumnSpec[] = [
      { name: "id", mysqlType: "int(11)", primary: true },
      { name: "deptId", mysqlType: "int(11)" }
    ];
    const introspector = makeIntrospector(cols);
    const [cfg] = await resolveTableConfigs(introspector, "app", [SEED], undefined, JOIN_CONFIGS);
    const field = cfg.typesense.fields.find((f) => f.name === "deptId");
    expect(field?.reference).toBe("Department.id");
    expect(field?.async_reference).toBe(true);
  });

  it("coerces reference field type to string by default", async () => {
    const cols: ColumnSpec[] = [
      { name: "id", mysqlType: "int(11)", primary: true },
      { name: "deptId", mysqlType: "int(11)" } // MySQL int → but join makes it string
    ];
    const introspector = makeIntrospector(cols);
    const [cfg] = await resolveTableConfigs(introspector, "app", [SEED], undefined, JOIN_CONFIGS);
    const field = cfg.typesense.fields.find((f) => f.name === "deptId");
    expect(field?.type).toBe("string");
  });

  it("respects explicit type override in join config", async () => {
    const joinConfigs: TableJoinConfig[] = [
      {
        table: "Users",
        fields: [{ name: "deptId", reference: "Department.id", type: "int64" }]
      }
    ];
    const introspector = makeIntrospector(DEFAULT_COLS);
    const [cfg] = await resolveTableConfigs(introspector, "app", [SEED], undefined, joinConfigs);
    const field = cfg.typesense.fields.find((f) => f.name === "deptId");
    expect(field?.type).toBe("int64");
  });

  it("adds a new optional field if not present in columns", async () => {
    const cols: ColumnSpec[] = [{ name: "id", mysqlType: "int(11)", primary: true }];
    const introspector = makeIntrospector(cols);
    const [cfg] = await resolveTableConfigs(introspector, "app", [SEED], undefined, JOIN_CONFIGS);
    const field = cfg.typesense.fields.find((f) => f.name === "deptId");
    expect(field).toBeDefined();
    expect(field?.optional).toBe(true);
    expect(field?.reference).toBe("Department.id");
  });

  it("does not affect tables not listed in joinConfigs", async () => {
    const introspector = makeIntrospector(DEFAULT_COLS);
    const [cfg] = await resolveTableConfigs(introspector, "app", [{ table: "Orders", database: "app" }], undefined, JOIN_CONFIGS);
    expect(cfg.typesense.fields.every((f) => !f.reference)).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// applyJoinMappingOverrides
// ---------------------------------------------------------------------------
describe("resolveTableConfigs — applyJoinMappingOverrides", () => {
  it("coerces mapping type to string for reference field", async () => {
    const cols: ColumnSpec[] = [
      { name: "id", mysqlType: "int(11)", primary: true },
      { name: "deptId", mysqlType: "int(11)" }
    ];
    const joinConfigs: TableJoinConfig[] = [
      { table: "Users", fields: [{ name: "deptId", reference: "Department.id" }] }
    ];
    const introspector = makeIntrospector(cols);
    const [cfg] = await resolveTableConfigs(introspector, "app", [SEED], undefined, joinConfigs);
    const mapping = cfg.transform.fieldMappings.find((m) => m.source === "deptId");
    expect(mapping?.type).toBe("string");
  });

  it("leaves other mappings unchanged", async () => {
    const cols: ColumnSpec[] = [
      { name: "id", mysqlType: "int(11)", primary: true },
      { name: "deptId", mysqlType: "int(11)" },
      { name: "score", mysqlType: "int(11)" }
    ];
    const joinConfigs: TableJoinConfig[] = [
      { table: "Users", fields: [{ name: "deptId", reference: "Department.id" }] }
    ];
    const introspector = makeIntrospector(cols);
    const [cfg] = await resolveTableConfigs(introspector, "app", [SEED], undefined, joinConfigs);
    const scoreMapping = cfg.transform.fieldMappings.find((m) => m.source === "score");
    expect(scoreMapping?.type).toBe("int64");
  });
});

// ---------------------------------------------------------------------------
// Cross-table join reference target enforcement
// All non-PK columns are inferred as optional=true by default. If such a column
// is the TARGET of a join reference from another table (e.g. Stock.StockID is
// referenced by DrugBatchOutMaster.StockOutID), it must be made non-optional
// in the target collection schema. Otherwise Typesense v30 rejects the join
// with "Referenced field X not found in collection Y".
// ---------------------------------------------------------------------------
describe("resolveTableConfigs — cross-table join reference target enforcement", () => {
  it("makes the referenced target field non-optional even when it is not the MySQL PK", async () => {
    // Stock table: MySQL PK is 'id', StockID is a non-PK unique column → inferred as optional
    const stockCols: ColumnSpec[] = [
      { name: "id", mysqlType: "int(11)", primary: true },
      { name: "StockID", mysqlType: "int(11)" }, // non-nullable, but not PK → optional by default
      { name: "StockCode", mysqlType: "varchar(100)" }
    ];
    // DrugBatchOutMaster references Stock.StockID
    const drugCols: ColumnSpec[] = [
      { name: "id", mysqlType: "int(11)", primary: true },
      { name: "StockOutID", mysqlType: "int(11)" }
    ];
    const introspector = makeIntrospector(stockCols);
    vi.spyOn(introspector, "getColumns")
      .mockResolvedValueOnce(
        stockCols.map((c) => ({ name: c.name, mysqlType: c.mysqlType, nullable: false, primary: c.primary ?? false }))
      )
      .mockResolvedValueOnce(
        drugCols.map((c) => ({ name: c.name, mysqlType: c.mysqlType, nullable: false, primary: c.primary ?? false }))
      );

    const joinConfigs: TableJoinConfig[] = [
      {
        table: "DrugBatchOutMaster",
        fields: [{ name: "StockOutID", reference: "Stock.StockID", type: "int64" }]
      }
    ];

    const [stockCfg, _drugCfg] = await resolveTableConfigs(
      introspector,
      "app",
      [
        { table: "Stock", database: "app" },
        { table: "DrugBatchOutMaster", database: "app" }
      ],
      undefined,
      joinConfigs
    );

    const stockIdField = stockCfg.typesense.fields.find((f) => f.name === "StockID");
    expect(stockIdField).toBeDefined();
    // Must NOT be optional — it is a join reference target
    expect(stockIdField?.optional).toBeUndefined();
  });

  it("leaves non-referenced optional fields unchanged", async () => {
    const stockCols: ColumnSpec[] = [
      { name: "id", mysqlType: "int(11)", primary: true },
      { name: "StockID", mysqlType: "int(11)" },
      { name: "StockCode", mysqlType: "varchar(100)" }  // not a join target
    ];
    const drugCols: ColumnSpec[] = [
      { name: "id", mysqlType: "int(11)", primary: true },
      { name: "StockOutID", mysqlType: "int(11)" }
    ];
    const introspector = makeIntrospector(stockCols);
    vi.spyOn(introspector, "getColumns")
      .mockResolvedValueOnce(
        stockCols.map((c) => ({ name: c.name, mysqlType: c.mysqlType, nullable: false, primary: c.primary ?? false }))
      )
      .mockResolvedValueOnce(
        drugCols.map((c) => ({ name: c.name, mysqlType: c.mysqlType, nullable: false, primary: c.primary ?? false }))
      );
    const joinConfigs: TableJoinConfig[] = [
      { table: "DrugBatchOutMaster", fields: [{ name: "StockOutID", reference: "Stock.StockID", type: "int64" }] }
    ];
    const [stockCfg] = await resolveTableConfigs(
      introspector,
      "app",
      [{ table: "Stock", database: "app" }, { table: "DrugBatchOutMaster", database: "app" }],
      undefined,
      joinConfigs
    );
    const codeField = stockCfg.typesense.fields.find((f) => f.name === "StockCode");
    expect(codeField?.optional).toBe(true); // not a join target → stays optional
  });

  it("adds missing join target field and mapping when target table uses strict custom fields", async () => {
    const stockCols: ColumnSpec[] = [
      { name: "id", mysqlType: "int(11)", primary: true },
      { name: "StockID", mysqlType: "int(11)" },
      { name: "StockCode", mysqlType: "varchar(100)" }
    ];
    const drugCols: ColumnSpec[] = [
      { name: "id", mysqlType: "int(11)", primary: true },
      { name: "StockOutID", mysqlType: "int(11)" }
    ];

    const introspector = makeIntrospector(stockCols);
    vi.spyOn(introspector, "getColumns")
      .mockResolvedValueOnce(
        stockCols.map((c) => ({ name: c.name, mysqlType: c.mysqlType, nullable: false, primary: c.primary ?? false }))
      )
      .mockResolvedValueOnce(
        drugCols.map((c) => ({ name: c.name, mysqlType: c.mysqlType, nullable: false, primary: c.primary ?? false }))
      );

    const joinConfigs: TableJoinConfig[] = [
      {
        table: "DrugBatchOutMaster",
        fields: [{ name: "StockOutID", reference: "Stock.StockID", type: "int64" }]
      }
    ];

    const [stockCfg] = await resolveTableConfigs(
      introspector,
      "app",
      [
        {
          table: "Stock",
          database: "app",
          typesense: {
            fields: [
              { name: "id", type: "string" },
              { name: "StockCode", type: "string", optional: true }
            ]
          },
          transform: {
            fieldMappings: [
              { source: "id", target: "id", type: "int64" },
              { source: "StockCode", target: "StockCode", type: "string", optional: true }
            ]
          }
        },
        { table: "DrugBatchOutMaster", database: "app" }
      ],
      undefined,
      joinConfigs
    );

    const stockIdField = stockCfg.typesense.fields.find((f) => f.name === "StockID");
    expect(stockIdField).toBeDefined();
    expect(stockIdField?.optional).toBeUndefined();

    const stockIdMapping = stockCfg.transform.fieldMappings.find((m) => m.target === "StockID");
    expect(stockIdMapping).toBeDefined();
    expect(stockIdMapping?.source).toBe("StockID");
  });
});
