import { describe, expect, it, vi } from "vitest";

import type { TableSyncConfig } from "../../src/core/types.js";
import { TypesenseDocumentIndexer } from "../../src/modules/typesense/document-indexer.js";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function makeTable(): TableSyncConfig {
  return {
    database: "db",
    table: "test",
    primaryKey: "id",
    collection: "test_collection",
    typesense: { fields: [], enableNestedFields: true },
    transform: { fieldMappings: [], dropNulls: true }
  };
}

function makeClient(overrides: { update?: ReturnType<typeof vi.fn>; import?: ReturnType<typeof vi.fn> } = {}) {
  const importMock = overrides.import ?? vi.fn().mockResolvedValue([{ success: true }]);
  const updateMock = overrides.update ?? vi.fn().mockResolvedValue({});

  const documentsCall = vi.fn((id?: string) => (id ? { update: updateMock, delete: vi.fn() } : { import: importMock }));

  return {
    client: { collections: vi.fn(() => ({ documents: documentsCall })) },
    importMock,
    updateMock
  };
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("TypesenseDocumentIndexer.upsertDocument — null field reset", () => {
  it("patches nulled fields to null after the emplace succeeds", async () => {
    const { client, importMock, updateMock } = makeClient();
    const indexer = new TypesenseDocumentIndexer(client as never);

    await indexer.upsertDocument(makeTable(), { id: "1", name: "Alice" }, ["note", "score"]);

    expect(importMock).toHaveBeenCalledOnce();
    expect(updateMock).toHaveBeenCalledWith({ note: null, score: null });
  });

  it("does not patch when no field was nulled", async () => {
    const { client, updateMock } = makeClient();
    const indexer = new TypesenseDocumentIndexer(client as never);

    await indexer.upsertDocument(makeTable(), { id: "1", name: "Alice" }, []);

    expect(updateMock).not.toHaveBeenCalled();
  });

  it("keeps the emplace result when a field cannot be reset because it is non-optional", async () => {
    // Typesense rejects null on a non-optional field with 400 "must be a string".
    const update = vi
      .fn()
      .mockRejectedValue(Object.assign(new Error("Field `code` must be a string."), { httpStatus: 400 }));
    const { client } = makeClient({ update });
    const warn = vi.fn();
    const indexer = new TypesenseDocumentIndexer(client as never, { warn } as never);

    await expect(indexer.upsertDocument(makeTable(), { id: "1" }, ["code"])).resolves.toEqual([]);
    expect(warn).toHaveBeenCalledOnce();
  });

  it("propagates unexpected errors from the null patch", async () => {
    const update = vi.fn().mockRejectedValue(Object.assign(new Error("boom"), { httpStatus: 503 }));
    const { client } = makeClient({ update });
    const indexer = new TypesenseDocumentIndexer(client as never);

    await expect(indexer.upsertDocument(makeTable(), { id: "1" }, ["note"])).rejects.toThrow("boom");
  });

  it("ignores a 404 from the null patch (document not indexed yet)", async () => {
    const update = vi.fn().mockRejectedValue(Object.assign(new Error("Not Found"), { httpStatus: 404 }));
    const { client } = makeClient({ update });
    const indexer = new TypesenseDocumentIndexer(client as never);

    await expect(indexer.upsertDocument(makeTable(), { id: "1" }, ["note"])).resolves.toEqual([]);
  });
});

// Regression: a JSON column whose shape differed from the type Typesense had
// already inferred (e.g. RefundDetails as string[] vs object[]) rejected the whole
// document, so unrelated columns such as Status were never synced either.
describe("TypesenseDocumentIndexer.upsertDocument — field type mismatch", () => {
  function importError(reason: string) {
    return Object.assign(new Error("import failed"), { importResults: [{ success: false, error: reason }] });
  }

  function makeTableWithFields(): TableSyncConfig {
    const table = makeTable();
    table.typesense.fields = [
      { name: "id", type: "string" },
      { name: "Status", type: "int64", optional: true },
      { name: "RefundDetails", type: "auto", optional: true },
      { name: "Code", type: "string" }
    ];
    return table;
  }

  it("retries without the mistyped optional field and reports it", async () => {
    const importMock = vi
      .fn()
      .mockRejectedValueOnce(importError("Field `RefundDetails` has an incorrect type."))
      .mockResolvedValueOnce([{ success: true }]);
    const { client } = makeClient({ import: importMock });
    const indexer = new TypesenseDocumentIndexer(client as never, { warn: vi.fn() } as never);

    const dropped = await indexer.upsertDocument(makeTableWithFields(), { id: "1", Status: 7, RefundDetails: ["x"] });

    expect(dropped).toEqual(["RefundDetails"]);
    expect(importMock).toHaveBeenCalledTimes(2);
    expect(importMock.mock.calls[1][0]).toEqual([{ id: "1", Status: 7 }]);
  });

  it("drops the top-level field when the mismatch is on a nested path", async () => {
    const importMock = vi
      .fn()
      .mockRejectedValueOnce(importError("Field `RefundDetails.Amount` must be an array of int64."))
      .mockResolvedValueOnce([{ success: true }]);
    const { client } = makeClient({ import: importMock });
    const indexer = new TypesenseDocumentIndexer(client as never, { warn: vi.fn() } as never);

    const dropped = await indexer.upsertDocument(makeTableWithFields(), { id: "1", Status: 7, RefundDetails: [{ Amount: "a" }] });

    expect(dropped).toEqual(["RefundDetails"]);
    expect(importMock.mock.calls[1][0]).toEqual([{ id: "1", Status: 7 }]);
  });

  it("still fails when the mistyped field is required", async () => {
    const importMock = vi.fn().mockRejectedValue(importError("Field `Code` must be a string."));
    const { client } = makeClient({ import: importMock });
    const indexer = new TypesenseDocumentIndexer(client as never);

    await expect(indexer.upsertDocument(makeTableWithFields(), { id: "1", Code: 5 })).rejects.toThrow("Field `Code` must be a string.");
    expect(importMock).toHaveBeenCalledOnce();
  });

  it("returns no dropped fields on a clean import", async () => {
    const { client } = makeClient();
    const indexer = new TypesenseDocumentIndexer(client as never);

    await expect(indexer.upsertDocument(makeTableWithFields(), { id: "1", Status: 7 })).resolves.toEqual([]);
  });
});
