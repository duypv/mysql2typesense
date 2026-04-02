import { describe, it, expect, vi } from "vitest";
import { TypesenseCollectionManager } from "../../src/modules/typesense/collection-manager.js";
import type { TableSyncConfig } from "../../src/core/types.js";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function makeClient(existingFields: Array<{ name: string; type: string; optional?: boolean }>) {
  const updateMock = vi.fn().mockResolvedValue({});
  const createMock = vi.fn().mockResolvedValue({});

  const collectionsCall = vi.fn((name?: string) => {
    if (name) {
      return {
        retrieve: vi.fn().mockResolvedValue({ name, fields: existingFields }),
        update: updateMock,
      };
    }
    return { create: createMock };
  });

  return { collections: collectionsCall, updateMock, createMock };
}

function makeTableConfig(fields: Array<{ name: string; type: string; optional?: boolean; reference?: string; async_reference?: boolean }>): TableSyncConfig {
  return {
    table: "test_table",
    collection: "test_collection",
    typesense: { fields, enableNestedFields: false } as any,
    mappings: [],
    filters: [],
    watch: true,
    skip: false,
  } as any;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("TypesenseCollectionManager.ensureCollection", () => {
  describe("when collection does not exist", () => {
    it("creates the collection", async () => {
      const createMock = vi.fn().mockResolvedValue({});
      const client = {
        collections: vi.fn((name?: string) => {
          if (name) {
            return {
              retrieve: vi.fn().mockRejectedValue(new Error("Not Found")),
              update: vi.fn(),
            };
          }
          return { create: createMock };
        }),
      };

      const manager = new TypesenseCollectionManager(client as any);
      const config = makeTableConfig([{ name: "service_id", type: "string" }]);
      await manager.ensureCollection(config);

      expect(createMock).toHaveBeenCalledOnce();
    });
  });

  describe("when collection exists with matching schema", () => {
    it("does not call update when nothing changed", async () => {
      const existing = [{ name: "id", type: "string" }, { name: "price", type: "int64" }];
      const { collections, updateMock } = makeClient(existing);
      const manager = new TypesenseCollectionManager({ collections } as any);
      const config = makeTableConfig([{ name: "id", type: "string" }, { name: "price", type: "int64" }]);

      await manager.ensureCollection(config);

      expect(updateMock).not.toHaveBeenCalled();
    });
  });

  describe("when a field's optional flag needs updating", () => {
    it("patches the field with optional=true", async () => {
      const existing = [{ name: "id", type: "string" }, { name: "service_id", type: "int64", optional: false }];
      const updateMock = vi.fn().mockResolvedValue({});
      const client = {
        collections: vi.fn((name?: string) => {
          if (name) {
            return {
              retrieve: vi.fn().mockResolvedValue({ name, fields: existing }),
              update: updateMock,
            };
          }
          return { create: vi.fn() };
        }),
      };
      const manager = new TypesenseCollectionManager(client as any);
      const config = makeTableConfig([
        { name: "id", type: "string" },
        { name: "service_id", type: "int64", optional: true },
      ]);

      await manager.ensureCollection(config);

      expect(updateMock).toHaveBeenCalledOnce();
      const { fields } = updateMock.mock.calls[0][0];
      expect(fields).toContainEqual(expect.objectContaining({ name: "service_id", optional: true }));
      expect(fields).not.toContainEqual(expect.objectContaining({ drop: true }));
    });
  });

  describe("type mismatch (join reference scenario)", () => {
    it("drops the old field and re-adds with the new type when type changes from int64 to string", async () => {
      // Simulate: ServicePriceHistory.ServiceID was int64 in existing collection,
      // but join config changed it to string in the desired schema.
      const existing = [
        { name: "id", type: "string" },
        { name: "ServiceID", type: "int64", optional: true },
        { name: "amount", type: "float", optional: true },
      ];
      const updateMock = vi.fn().mockResolvedValue({});
      const client = {
        collections: vi.fn((name?: string) => {
          if (name) {
            return {
              retrieve: vi.fn().mockResolvedValue({ name, fields: existing }),
              update: updateMock,
            };
          }
          return { create: vi.fn() };
        }),
      };
      const manager = new TypesenseCollectionManager(client as any);
      const config = makeTableConfig([
        { name: "id", type: "string" },
        { name: "ServiceID", type: "string", optional: true, reference: "services.id" },
        { name: "amount", type: "float", optional: true },
      ]);

      await manager.ensureCollection(config);

      expect(updateMock).toHaveBeenCalledOnce();
      const { fields } = updateMock.mock.calls[0][0];

      // Must have a drop entry for the old field
      expect(fields).toContainEqual({ name: "ServiceID", drop: true });

      // Must have the re-add entry with the new type
      expect(fields).toContainEqual(
        expect.objectContaining({ name: "ServiceID", type: "string", reference: "services.id" })
      );

      // 'amount' type matches — should not appear in patches
      expect(fields).not.toContainEqual(expect.objectContaining({ name: "amount" }));
    });

    it("adds new fields that are present in desired but absent in existing", async () => {
      const existing = [{ name: "id", type: "string" }];
      const updateMock = vi.fn().mockResolvedValue({});
      const client = {
        collections: vi.fn((name?: string) => {
          if (name) {
            return {
              retrieve: vi.fn().mockResolvedValue({ name, fields: existing }),
              update: updateMock,
            };
          }
          return { create: vi.fn() };
        }),
      };
      const manager = new TypesenseCollectionManager(client as any);
      const config = makeTableConfig([
        { name: "id", type: "string" },
        { name: "new_field", type: "string", optional: true },
      ]);

      await manager.ensureCollection(config);

      expect(updateMock).toHaveBeenCalledOnce();
      const { fields } = updateMock.mock.calls[0][0];
      expect(fields).toContainEqual(expect.objectContaining({ name: "new_field", type: "string" }));
    });

    it("continues silently when schema update throws", async () => {
      const existing = [{ name: "id", type: "string" }, { name: "score", type: "int64" }];
      const client = {
        collections: vi.fn((name?: string) => {
          if (name) {
            return {
              retrieve: vi.fn().mockResolvedValue({ name, fields: existing }),
              update: vi.fn().mockRejectedValue(new Error("Schema update not supported")),
            };
          }
          return { create: vi.fn() };
        }),
      };
      const manager = new TypesenseCollectionManager(client as any);
      const config = makeTableConfig([
        { name: "id", type: "string" },
        { name: "score", type: "float" }, // type mismatch
      ]);

      // Should not throw
      await expect(manager.ensureCollection(config)).resolves.toBeUndefined();
    });
  });

  describe("clearSyncedCache", () => {
    it("forces re-check of schema after cache is cleared", async () => {
      const existing = [{ name: "id", type: "string" }];
      const updateMock = vi.fn().mockResolvedValue({});
      const client = {
        collections: vi.fn((name?: string) => {
          if (name) {
            return {
              retrieve: vi.fn().mockResolvedValue({ name, fields: existing }),
              update: updateMock,
            };
          }
          return { create: vi.fn() };
        }),
      };
      const manager = new TypesenseCollectionManager(client as any);
      const config = makeTableConfig([
        { name: "id", type: "string" },
        { name: "extra", type: "string", optional: true },
      ]);

      await manager.ensureCollection(config);
      expect(updateMock).toHaveBeenCalledTimes(1); // first check — adds extra field

      await manager.ensureCollection(config);
      expect(updateMock).toHaveBeenCalledTimes(1); // cached — no second update

      manager.clearSyncedCache();
      await manager.ensureCollection(config);
      expect(updateMock).toHaveBeenCalledTimes(2); // cache cleared — re-checks
    });
  });
});
