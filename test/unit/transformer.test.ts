import { describe, expect, it } from "vitest";

import type { TableSyncConfig } from "../../src/core/types.js";
import { ConfigDrivenTransformer } from "../../src/modules/transform/transformer.js";

const transformer = new ConfigDrivenTransformer();

function makeTable(overrides: Partial<TableSyncConfig> = {}): TableSyncConfig {
  return {
    database: "db",
    table: "test",
    primaryKey: "id",
    collection: "test",
    typesense: { fields: [], enableNestedFields: true },
    transform: { fieldMappings: [], dropNulls: false },
    ...overrides
  };
}

// ---------------------------------------------------------------------------
// Basic document building
// ---------------------------------------------------------------------------
describe("ConfigDrivenTransformer.toDocument — basic", () => {
  it("builds document from row", async () => {
    const table = makeTable({
      transform: {
        dropNulls: false,
        fieldMappings: [
          { source: "id", target: "id", type: "string" },
          { source: "name", target: "name", type: "string" }
        ]
      }
    });
    expect(await transformer.toDocument({ id: 1, name: "Alice" }, table)).toEqual({ id: "1", name: "Alice" });
  });

  it("id field is always coerced to string", async () => {
    const table = makeTable({
      transform: {
        dropNulls: false,
        fieldMappings: [{ source: "id", target: "id", type: "int64" }]
      }
    });
    const doc = await transformer.toDocument({ id: 99 }, table);
    expect(doc.id).toBe("99");
    expect(typeof doc.id).toBe("string");
  });

  it("throws when primary key is missing from row", async () => {
    const table = makeTable({ primaryKey: "id", transform: { fieldMappings: [], dropNulls: false } });
    await expect(transformer.toDocument({ name: "Alice" }, table)).rejects.toThrow('Missing primary key "id"');
  });

  it("throws when required field is null", async () => {
    const table = makeTable({
      transform: {
        dropNulls: false,
        fieldMappings: [
          { source: "id", target: "id", type: "string" },
          { source: "status", target: "status", type: "string" }
        ]
      }
    });
    await expect(transformer.toDocument({ id: 1, status: null }, table)).rejects.toThrow(
      'Field "status" is required'
    );
  });

  it("skips null for optional field", async () => {
    const table = makeTable({
      transform: {
        dropNulls: false,
        fieldMappings: [
          { source: "id", target: "id", type: "string" },
          { source: "note", target: "note", type: "string", optional: true }
        ]
      }
    });
    expect(await transformer.toDocument({ id: 1, note: null }, table)).toEqual({ id: "1" });
  });

  it("skips null field when dropNulls=true", async () => {
    const table = makeTable({
      transform: {
        dropNulls: true,
        fieldMappings: [
          { source: "id", target: "id", type: "string" },
          { source: "score", target: "score", type: "int64" }
        ]
      }
    });
    expect(await transformer.toDocument({ id: 1, score: null }, table)).toEqual({ id: "1" });
  });

  it("uses defaultValue when raw value is null", async () => {
    const table = makeTable({
      transform: {
        dropNulls: false,
        fieldMappings: [
          { source: "id", target: "id", type: "string" },
          { source: "count", target: "count", type: "int64", defaultValue: 0, optional: true }
        ]
      }
    });
    const doc = await transformer.toDocument({ id: 1, count: null }, table);
    expect(doc["count"]).toBe(0);
  });
});

// ---------------------------------------------------------------------------
// Type coercions
// ---------------------------------------------------------------------------
async function coerce(value: unknown, type: string): Promise<unknown> {
  const table = makeTable({
    transform: {
      dropNulls: false,
      fieldMappings: [
        { source: "id", target: "id", type: "string" },
        { source: "v", target: "v", type: type as never }
      ]
    }
  });
  const doc = await transformer.toDocument({ id: 1, v: value }, table);
  return doc["v"];
}

describe("ConfigDrivenTransformer — type coercions", () => {
  it("number → string", async () => expect(await coerce(42, "string")).toBe("42"));
  it("string → int64", async () => expect(await coerce("7", "int64")).toBe(7));
  it("float string → int64 (truncation)", async () => expect(await coerce("3.9", "int64")).toBe(3));
  it("string → float", async () => expect(await coerce("3.14", "float")).toBeCloseTo(3.14));
  it('"1" → bool true', async () => expect(await coerce("1", "bool")).toBe(true));
  it('"true" → bool true', async () => expect(await coerce("true", "bool")).toBe(true));
  it('"yes" → bool true', async () => expect(await coerce("yes", "bool")).toBe(true));
  it('"false" → bool false', async () => expect(await coerce("false", "bool")).toBe(false));
  it('"0" → bool false', async () => expect(await coerce("0", "bool")).toBe(false));
  it("number 0 → bool false", async () => expect(await coerce(0, "bool")).toBe(false));
  it("scalar → string[]", async () => expect(await coerce("x", "string[]")).toEqual(["x"]));
  it("array → int64[]", async () => expect(await coerce(["1", "2"], "int64[]")).toEqual([1, 2]));
  it("array → float[]", async () => expect(await coerce(["1.5", "2.5"], "float[]")).toEqual([1.5, 2.5]));
  it("array → bool[]", async () => expect(await coerce(["true", "0"], "bool[]")).toEqual([true, false]));
  it("auto passes value through unchanged", async () => expect(await coerce({ a: 1 }, "auto")).toEqual({ a: 1 }));
});

// ---------------------------------------------------------------------------
// json sourceFormat
// ---------------------------------------------------------------------------
async function jsonField(source: unknown): Promise<unknown> {
  // dropNulls:true so that null-returning paths (e.g. empty string) produce
  // undefined (field omitted) rather than a required-field error.
  const table = makeTable({
    transform: {
      dropNulls: true,
      fieldMappings: [
        { source: "id", target: "id", type: "string" },
        { source: "data", target: "data", type: "auto", sourceFormat: "json" }
      ]
    }
  });
  return (await transformer.toDocument({ id: 1, data: source }, table))["data"];
}

describe("ConfigDrivenTransformer — json sourceFormat", () => {
  it("parses valid JSON string to object", async () => {
    expect(await jsonField('{"a":1}')).toEqual({ a: 1 });
  });

  it("parses valid JSON string to array", async () => {
    expect(await jsonField("[1,2,3]")).toEqual([1, 2, 3]);
  });

  it("passes through already-parsed object", async () => {
    expect(await jsonField({ a: 1 })).toEqual({ a: 1 });
  });

  it("splits non-JSON CSV string into number array", async () => {
    expect(await jsonField("1,2,3")).toEqual([1, 2, 3]);
  });

  it("splits non-JSON CSV string with mixed types", async () => {
    expect(await jsonField("a,b,c")).toEqual(["a", "b", "c"]);
  });

  it("returns undefined for empty string (null dropped by dropNulls)", async () => {
    expect(await jsonField("")).toBeUndefined();
  });

  it("wraps single numeric JSON value in array", async () => {
    expect(await jsonField("1")).toEqual([1]);
  });

  it("wraps single boolean JSON value in array", async () => {
    expect(await jsonField("true")).toEqual([true]);
  });

  it("wraps already-parsed primitive number in array", async () => {
    expect(await jsonField(42)).toEqual([42]);
  });

  it("keeps homogeneous number array unchanged", async () => {
    expect(await jsonField([1, 2, 3])).toEqual([1, 2, 3]);
  });

  it("keeps homogeneous string array unchanged", async () => {
    expect(await jsonField(["a", "b"])).toEqual(["a", "b"]);
  });

  it("normalizes mixed-type array to string[]", async () => {
    const result = await jsonField([1, "two", false]);
    // strings pass through as-is; numbers/booleans are JSON.stringify'd
    expect(result).toEqual(["1", "two", "false"]);
  });

  it("normalizes mixed array containing object to string[]", async () => {
    const result = await jsonField([1, { nested: true }]);
    expect(Array.isArray(result)).toBe(true);
    expect((result as string[]).every((v) => typeof v === "string")).toBe(true);
  });

  it("recursively sanitizes nested object values", async () => {
    const result = await jsonField({ scores: [1, 2, 3] });
    expect(result).toEqual({ scores: [1, 2, 3] });
  });

  // -------------------------------------------------------------------------
  // DepartmentIDs-style: MySQL stores JSON arrays or CSV in VARCHAR/TEXT
  // -------------------------------------------------------------------------
  it("parses JSON array string '[1,2,3,4]' to number array", async () => {
    expect(await jsonField("[1,2,3,4]")).toEqual([1, 2, 3, 4]);
  });

  it("parses single-element JSON array '[1]' to array", async () => {
    expect(await jsonField("[1]")).toEqual([1]);
  });

  it("parses CSV '1,17,12,14' to number array (legacy rows)", async () => {
    expect(await jsonField("1,17,12,14")).toEqual([1, 17, 12, 14]);
  });

  it("parses single CSV value '1' to single-element array", async () => {
    expect(await jsonField("1")).toEqual([1]);
  });

  it("parses CSV with spaces '1, 2, 3' to trimmed number array", async () => {
    expect(await jsonField("1, 2, 3")).toEqual([1, 2, 3]);
  });
});

// ---------------------------------------------------------------------------
// csv sourceFormat
// ---------------------------------------------------------------------------
describe("ConfigDrivenTransformer — csv sourceFormat", () => {
  it("splits comma-separated string by default", async () => {
    const table = makeTable({
      transform: {
        dropNulls: false,
        fieldMappings: [
          { source: "id", target: "id", type: "string" },
          { source: "tags", target: "tags", type: "string[]", sourceFormat: "csv" }
        ]
      }
    });
    expect((await transformer.toDocument({ id: 1, tags: "a,b,c" }, table))["tags"]).toEqual(["a", "b", "c"]);
  });

  it("trims whitespace from each item", async () => {
    const table = makeTable({
      transform: {
        dropNulls: false,
        fieldMappings: [
          { source: "id", target: "id", type: "string" },
          { source: "tags", target: "tags", type: "string[]", sourceFormat: "csv" }
        ]
      }
    });
    expect((await transformer.toDocument({ id: 1, tags: " a , b , c " }, table))["tags"]).toEqual(["a", "b", "c"]);
  });

  it("respects custom arraySeparator", async () => {
    const table = makeTable({
      transform: {
        dropNulls: false,
        fieldMappings: [
          { source: "id", target: "id", type: "string" },
          { source: "tags", target: "tags", type: "string[]", sourceFormat: "csv", arraySeparator: "|" }
        ]
      }
    });
    expect((await transformer.toDocument({ id: 1, tags: "a|b|c" }, table))["tags"]).toEqual(["a", "b", "c"]);
  });

  it("passes through array as-is", async () => {
    const table = makeTable({
      transform: {
        dropNulls: false,
        fieldMappings: [
          { source: "id", target: "id", type: "string" },
          { source: "tags", target: "tags", type: "string[]", sourceFormat: "csv" }
        ]
      }
    });
    expect((await transformer.toDocument({ id: 1, tags: ["x", "y"] }, table))["tags"]).toEqual(["x", "y"]);
  });
});

// ---------------------------------------------------------------------------
// datetime sourceFormat
// ---------------------------------------------------------------------------
describe("ConfigDrivenTransformer — datetime sourceFormat", () => {
  const ISO = "2024-01-01T00:00:00.000Z";
  const EPOCH_S = 1704067200;
  const EPOCH_MS = 1704067200000;

  it("converts ISO string to epoch seconds (default)", async () => {
    const table = makeTable({
      transform: {
        dropNulls: false,
        fieldMappings: [
          { source: "id", target: "id", type: "string" },
          { source: "ts", target: "ts", type: "int64", sourceFormat: "datetime", timestampResolution: "seconds" }
        ]
      }
    });
    expect((await transformer.toDocument({ id: 1, ts: ISO }, table))["ts"]).toBe(EPOCH_S);
  });

  it("converts ISO string to epoch milliseconds", async () => {
    const table = makeTable({
      transform: {
        dropNulls: false,
        fieldMappings: [
          { source: "id", target: "id", type: "string" },
          {
            source: "ts",
            target: "ts",
            type: "int64",
            sourceFormat: "datetime",
            timestampResolution: "milliseconds"
          }
        ]
      }
    });
    expect((await transformer.toDocument({ id: 1, ts: ISO }, table))["ts"]).toBe(EPOCH_MS);
  });

  it("converts Date object to epoch seconds", async () => {
    const table = makeTable({
      transform: {
        dropNulls: false,
        fieldMappings: [
          { source: "id", target: "id", type: "string" },
          { source: "ts", target: "ts", type: "int64", sourceFormat: "datetime", timestampResolution: "seconds" }
        ]
      }
    });
    expect((await transformer.toDocument({ id: 1, ts: new Date(ISO) }, table))["ts"]).toBe(EPOCH_S);
  });

  it("coerces empty datetime string to 0 via Number(null)", async () => {
    // normalizeSourceValue returns null for empty datetime, then coerceValue(null, int64)
    // calls Number(null) === 0. This is the current behaviour.
    const table = makeTable({
      transform: {
        dropNulls: false,
        fieldMappings: [
          { source: "id", target: "id", type: "string" },
          { source: "ts", target: "ts", type: "int64", sourceFormat: "datetime" }
        ]
      }
    });
    const doc = await transformer.toDocument({ id: 1, ts: "" }, table);
    expect(doc["ts"]).toBe(0);
  });

  it("coerces invalid date string to 0 via Number(null)", async () => {
    // normalizeSourceValue returns null for invalid dates, same coercion path.
    const table = makeTable({
      transform: {
        dropNulls: false,
        fieldMappings: [
          { source: "id", target: "id", type: "string" },
          { source: "ts", target: "ts", type: "int64", sourceFormat: "datetime" }
        ]
      }
    });
    const doc = await transformer.toDocument({ id: 1, ts: "not-a-date" }, table);
    expect(doc["ts"]).toBe(0);
  });
});
