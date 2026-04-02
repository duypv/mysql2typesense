import { mkdirSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { loadSyncConfig } from "../../src/config/sync-config.js";

// ---------------------------------------------------------------------------
// Temp directory for config files
// ---------------------------------------------------------------------------
const TMP = join(tmpdir(), `sync-config-test-${process.pid}`);

beforeAll(() => mkdirSync(TMP, { recursive: true }));
afterAll(() => rmSync(TMP, { recursive: true, force: true }));

function writeCfg(name: string, content: unknown): string {
  const path = join(TMP, `${name}.json`);
  writeFileSync(path, JSON.stringify(content), "utf8");
  return path;
}

// ---------------------------------------------------------------------------
// loadSyncConfig — missing file
// ---------------------------------------------------------------------------
describe("loadSyncConfig — missing file", () => {
  it("returns empty tables and joinConfigs when file is missing", () => {
    const result = loadSyncConfig(join(TMP, "does-not-exist.json"));
    expect(result).toEqual({ tables: [], joinConfigs: [] });
  });
});

// ---------------------------------------------------------------------------
// loadSyncConfig — minimal valid config
// ---------------------------------------------------------------------------
describe("loadSyncConfig — minimal config", () => {
  it("parses empty object as valid config", () => {
    const path = writeCfg("empty", {});
    const result = loadSyncConfig(path);
    expect(result.tables).toEqual([]);
    expect(result.joinConfigs).toEqual([]);
    expect(result.database).toBeUndefined();
  });

  it("parses database section", () => {
    const path = writeCfg("with-db", {
      database: { name: "mydb" }
    });
    const result = loadSyncConfig(path);
    expect(result.database?.name).toBe("mydb");
  });

  it("maps infix_string → infixString", () => {
    const path = writeCfg("infix", {
      database: { name: "mydb", infix_string: true }
    });
    expect(loadSyncConfig(path).database?.infixString).toBe(true);
  });

  it("maps json_stringify → jsonStringify", () => {
    const path = writeCfg("json-stringify", {
      database: { name: "mydb", json_stringify: ["*IDs"] }
    });
    expect(loadSyncConfig(path).database?.jsonStringify).toEqual(["*IDs"]);
  });

  it("maps facet_fields → facetFields", () => {
    const path = writeCfg("facet", {
      database: { name: "mydb", facet_fields: ["category"] }
    });
    expect(loadSyncConfig(path).database?.facetFields).toEqual(["category"]);
  });
});

// ---------------------------------------------------------------------------
// loadSyncConfig — tables
// ---------------------------------------------------------------------------
describe("loadSyncConfig — tables", () => {
  it("parses table list", () => {
    const path = writeCfg("tables", {
      tables: [{ table: "Users" }, { table: "Orders", database: "shop" }]
    });
    const result = loadSyncConfig(path);
    expect(result.tables).toHaveLength(2);
    expect(result.tables[0].table).toBe("Users");
    expect(result.tables[1].database).toBe("shop");
  });

  it("parses transform field mappings", () => {
    const path = writeCfg("mappings", {
      tables: [
        {
          table: "Users",
          transform: {
            fieldMappings: [{ source: "id", target: "id", type: "string" }]
          }
        }
      ]
    });
    const result = loadSyncConfig(path);
    expect(result.tables[0].transform?.fieldMappings?.[0].source).toBe("id");
  });
});

// ---------------------------------------------------------------------------
// loadSyncConfig — join_configs
// ---------------------------------------------------------------------------
describe("loadSyncConfig — join_configs", () => {
  it("parses join_configs", () => {
    const path = writeCfg("joins", {
      join_configs: [
        { table: "Orders", fields: [{ name: "userId", reference: "Users.id" }] }
      ]
    });
    const result = loadSyncConfig(path);
    expect(result.joinConfigs).toHaveLength(1);
    expect(result.joinConfigs[0].table).toBe("Orders");
    expect(result.joinConfigs[0].fields[0].reference).toBe("Users.id");
  });

  it("defaults async_reference to undefined when not provided", () => {
    const path = writeCfg("joins-no-async", {
      join_configs: [
        { table: "Orders", fields: [{ name: "userId", reference: "Users.id" }] }
      ]
    });
    expect(loadSyncConfig(path).joinConfigs[0].fields[0].async_reference).toBeUndefined();
  });

  it("parses async_reference=true", () => {
    const path = writeCfg("joins-async", {
      join_configs: [
        { table: "Orders", fields: [{ name: "userId", reference: "Users.id", async_reference: true }] }
      ]
    });
    expect(loadSyncConfig(path).joinConfigs[0].fields[0].async_reference).toBe(true);
  });

  it("returns empty joinConfigs when join_configs is absent", () => {
    const path = writeCfg("no-joins", { tables: [{ table: "Users" }] });
    expect(loadSyncConfig(path).joinConfigs).toEqual([]);
  });
});

// ---------------------------------------------------------------------------
// loadSyncConfig — Zod validation errors
// ---------------------------------------------------------------------------
describe("loadSyncConfig — validation errors", () => {
  it("throws ZodError for invalid field type", () => {
    const path = writeCfg("invalid-type", {
      tables: [
        {
          table: "Users",
          transform: {
            fieldMappings: [{ source: "id", target: "id", type: "invalid_type" }]
          }
        }
      ]
    });
    expect(() => loadSyncConfig(path)).toThrow();
  });

  it("throws ZodError when join reference is missing dot separator", () => {
    const path = writeCfg("bad-reference", {
      join_configs: [{ table: "Orders", fields: [{ name: "userId", reference: "Users" }] }]
    });
    expect(() => loadSyncConfig(path)).toThrow();
  });

  it("throws ZodError when join fields array is empty", () => {
    const path = writeCfg("empty-join-fields", {
      join_configs: [{ table: "Orders", fields: [] }]
    });
    expect(() => loadSyncConfig(path)).toThrow();
  });

  it("throws on malformed JSON", () => {
    const path = join(TMP, "bad.json");
    writeFileSync(path, "{ invalid json }", "utf8");
    expect(() => loadSyncConfig(path)).toThrow();
  });
});
