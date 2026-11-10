import { describe, expect, it, vi } from "vitest";

import type { ChangeEvent, TableSyncConfig } from "../../src/core/types.js";
import { RealtimeSyncService } from "../../src/modules/sync/realtime-sync.service.js";
import { ConfigDrivenTransformer } from "../../src/modules/transform/transformer.js";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function makeTable(): TableSyncConfig {
  return {
    database: "db",
    table: "orders",
    primaryKey: "id",
    collection: "orders",
    typesense: { fields: [], enableNestedFields: true },
    transform: {
      dropNulls: true,
      fieldMappings: [
        { source: "id", target: "id", type: "string" },
        { source: "note", target: "note", type: "string", optional: true }
      ]
    }
  };
}

const silentLogger = { debug: vi.fn(), info: vi.fn(), error: vi.fn(), warn: vi.fn() };

/** Runs one binlog event through the service and returns the indexer spies. */
async function runEvent(event: ChangeEvent, droppedFields: string[] = []) {
  const upsertDocument = vi.fn().mockResolvedValue(droppedFields);
  const listener = {
    start: async (onChange: (e: ChangeEvent) => Promise<void>) => {
      await onChange(event);
    },
    stop: async () => {}
  };
  const collectionManager = {
    ensureCollection: vi.fn().mockResolvedValue(undefined),
    validateJoinReferenceIntegrity: vi.fn().mockResolvedValue(undefined)
  };
  const monitor = {
    setTables: vi.fn(),
    markMode: vi.fn(),
    recordInitialBatch: vi.fn(),
    recordRealtimeEvent: vi.fn(),
    recordError: vi.fn(),
    snapshot: vi.fn(),
    toPrometheusMetrics: vi.fn()
  };

  const service = new RealtimeSyncService(
    listener as never,
    collectionManager as never,
    { upsertDocument, deleteDocument: vi.fn() } as never,
    new ConfigDrivenTransformer(),
    { load: vi.fn(), save: vi.fn(), close: vi.fn() } as never,
    { maxAttempts: 1, baseDelayMs: 0 },
    silentLogger as never,
    monitor as never
  );

  await service.run();
  return { upsertDocument, monitor };
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("RealtimeSyncService — columns updated to NULL", () => {
  it("passes the nulled target fields to the indexer", async () => {
    const { upsertDocument } = await runEvent({
      operation: "update",
      table: makeTable(),
      before: { id: 1, note: "old" },
      after: { id: 1, note: null }
    });

    expect(upsertDocument).toHaveBeenCalledWith(expect.anything(), { id: "1" }, ["note"]);
  });

  it("passes no nulled fields when every column carries a value", async () => {
    const { upsertDocument } = await runEvent({
      operation: "update",
      table: makeTable(),
      before: { id: 1, note: "old" },
      after: { id: 1, note: "new" }
    });

    expect(upsertDocument).toHaveBeenCalledWith(expect.anything(), { id: "1", note: "new" }, []);
  });
});

describe("RealtimeSyncService — fields dropped for a type mismatch", () => {
  it("reports dropped fields on the dashboard while counting the event as processed", async () => {
    const { monitor } = await runEvent(
      { operation: "update", table: makeTable(), before: { id: 1, note: "old" }, after: { id: 1, note: "new" } },
      ["note"]
    );

    expect(monitor.recordError).toHaveBeenCalledWith(
      expect.objectContaining({ message: expect.stringContaining("note") }),
      "realtime:db.orders:dropped-fields",
      expect.objectContaining({ id: "1" })
    );
    expect(monitor.recordRealtimeEvent).toHaveBeenCalledWith("db.orders", "update");
  });

  it("reports nothing when no field was dropped", async () => {
    const { monitor } = await runEvent({
      operation: "update",
      table: makeTable(),
      before: { id: 1, note: "old" },
      after: { id: 1, note: "new" }
    });

    expect(monitor.recordError).not.toHaveBeenCalled();
  });
});
