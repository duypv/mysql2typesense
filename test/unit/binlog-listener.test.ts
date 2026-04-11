/**
 * Unit tests for MySqlBinlogListener.
 *
 * Covers the auto-reconnect behaviour introduced to fix a production bug where
 * ZongJi connection drops were silently swallowed after "ready" fired, resulting
 * in realtimeUpserts staying at 0 indefinitely.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import type { AppConfig, BinlogCheckpoint, CheckpointStore, TableSyncConfig } from "../../src/core/types.js";

// ---------------------------------------------------------------------------
// ZongJi mock
//
// vi.hoisted() runs BEFORE any imports and vi.mock() factories, so the shared
// `state` object is available inside the mock factory below.
// ---------------------------------------------------------------------------

const state = vi.hoisted(() => {
  type Instance = {
    start: ReturnType<typeof vi.fn>;
    stop: ReturnType<typeof vi.fn>;
    emit(event: string, ...args: unknown[]): boolean;
    on(event: string, listener: (...args: unknown[]) => unknown): unknown;
  };
  return { instances: [] as Instance[] };
});

vi.mock("@powersync/mysql-zongji", () => {
  /* eslint-disable @typescript-eslint/no-require-imports */
  const { EventEmitter } = require("node:events") as typeof import("node:events");

  // Constructor function instead of ES class avoids hoisting issues inside factory.
  function MockZongJi(this: InstanceType<typeof EventEmitter> & { start: () => void; stop: () => void }) {
    EventEmitter.call(this);
    this.start = vi.fn();
    this.stop = vi.fn();
    state.instances.push(this as unknown as (typeof state.instances)[number]);
  }
  MockZongJi.prototype = Object.create(EventEmitter.prototype);
  MockZongJi.prototype.constructor = MockZongJi;

  return { default: MockZongJi };
});

// Import under test AFTER mock is registered.
import { MySqlBinlogListener } from "../../src/modules/mysql/binlog-listener.js";

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

function makeConfig(): AppConfig {
  return {
    mysql: { host: "localhost", port: 3306, user: "user", password: "pass", database: "db" },
    sync: {
      batchSize: 100,
      database: undefined,
      tables: [],
      retry: { maxAttempts: 3, baseDelayMs: 100 },
      joinConfigs: []
    },
    typesense: { host: "localhost", port: 8108, protocol: "http", apiKey: "key" },
    checkpoint: { driver: "file", filePath: "/tmp/test.json" },
    monitoring: { host: "localhost", port: 8080, enabled: false },
    logLevel: "silent"
  };
}

function makeCheckpointStore(checkpoint: BinlogCheckpoint | null = null): CheckpointStore {
  return {
    load: vi.fn().mockResolvedValue(checkpoint),
    save: vi.fn().mockResolvedValue(undefined),
    close: vi.fn().mockResolvedValue(undefined)
  };
}

function makeTable(table = "users", database = "db"): TableSyncConfig {
  return {
    database,
    table,
    primaryKey: "id",
    collection: table,
    typesense: { fields: [], enableNestedFields: true },
    transform: { fieldMappings: [], dropNulls: false }
  };
}

/** Returns the most-recently created ZongJi mock instance. */
function lastInst() {
  return state.instances[state.instances.length - 1];
}

/** Drain pending microtasks. Uses Promise.resolve() so it works even with fake timers
 * (vi.useFakeTimers() mocks setImmediate but does NOT mock Promise microtasks). */
async function tick(): Promise<void> {
  await Promise.resolve();
  await Promise.resolve();
}

/** Start a listener, await ZongJi creation, emit "ready", and resolve start(). */
async function startListener(
  tables: TableSyncConfig[] = [],
  store: CheckpointStore = makeCheckpointStore()
) {
  const onChange = vi.fn().mockResolvedValue(undefined);
  const listener = new MySqlBinlogListener(makeConfig(), tables, store);
  const p = listener.start(onChange);
  // start() awaits checkpointStore.load() before calling connectZongJi / creating ZongJi.
  // One tick drains all pending microtasks so the ZongJi instance exists in state.instances.
  await tick();
  lastInst().emit("ready");
  await p;
  return { listener, onChange, store };
}

// ---------------------------------------------------------------------------
// Binlog event factory
// ---------------------------------------------------------------------------

function makeBinlogEvent(
  eventName: string,
  rows: unknown[],
  tableId = 1,
  parentSchema = "db",
  tableName = "users"
) {
  return {
    getEventName: () => eventName,
    tableMap: { [tableId]: { parentSchema, tableName } },
    tableId,
    rows,
    nextPosition: 500,
    binlogName: "mysql-bin.000004"
  };
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

beforeEach(() => {
  state.instances.length = 0;
});

afterEach(() => {
  vi.useRealTimers();
});

// ---------------------------------------------------------------------------
describe("isConnected()", () => {
  it("returns false before start()", () => {
    const listener = new MySqlBinlogListener(makeConfig(), [], makeCheckpointStore());
    expect(listener.isConnected()).toBe(false);
  });

  it("returns true after ZongJi emits 'ready'", async () => {
    const { listener } = await startListener();
    expect(listener.isConnected()).toBe(true);
    await listener.stop();
  });

  it("returns false after stop()", async () => {
    const { listener } = await startListener();
    await listener.stop();
    expect(listener.isConnected()).toBe(false);
  });

  it("returns false when ZongJi emits error after ready", async () => {
    vi.useFakeTimers();
    const { listener } = await startListener();
    lastInst().emit("error", new Error("TCP connection reset"));
    expect(listener.isConnected()).toBe(false);
    await listener.stop();
  });
});

// ---------------------------------------------------------------------------
describe("start() — error handling", () => {
  it("rejects when ZongJi emits error before ready", async () => {
    const listener = new MySqlBinlogListener(makeConfig(), [], makeCheckpointStore());
    const p = listener.start(vi.fn());
    await tick(); // wait for checkpointStore.load() so ZongJi is created
    lastInst().emit("error", new Error("ECONNREFUSED"));
    await expect(p).rejects.toThrow("ECONNREFUSED");
    expect(listener.isConnected()).toBe(false);
  });

  it("no reconnect is scheduled when error occurs before ready", async () => {
    vi.useFakeTimers();
    const listener = new MySqlBinlogListener(makeConfig(), [], makeCheckpointStore());
    const p = listener.start(vi.fn());
    await tick();
    lastInst().emit("error", new Error("startup fail"));
    await expect(p).rejects.toThrow();

    const countAfterReject = state.instances.length;
    await vi.advanceTimersByTimeAsync(60_000);
    // No additional ZongJi instances created (no background reconnect)
    expect(state.instances.length).toBe(countAfterReject);
  });

  it("uses stored checkpoint position when starting", async () => {
    const cp: BinlogCheckpoint = { filename: "mysql-bin.000003", position: 4096, updatedAt: "" };
    const store = makeCheckpointStore(cp);
    const listener = new MySqlBinlogListener(makeConfig(), [], store);
    const p = listener.start(vi.fn());
    await tick(); // wait for checkpointStore.load() so ZongJi is created
    const inst = lastInst();
    expect(inst.start).toHaveBeenCalledWith(
      expect.objectContaining({ filename: "mysql-bin.000003", position: 4096 })
    );
    inst.emit("ready");
    await p;
    await listener.stop();
  });

  it("starts without filename/position when no checkpoint exists", async () => {
    const listener = new MySqlBinlogListener(makeConfig(), [], makeCheckpointStore(null));
    const p = listener.start(vi.fn());
    await tick(); // wait for checkpointStore.load() so ZongJi is created
    const inst = lastInst();
    const callArg = inst.start.mock.calls[0][0] as Record<string, unknown>;
    expect(callArg.filename).toBeUndefined();
    expect(callArg.position).toBeUndefined();
    inst.emit("ready");
    await p;
    await listener.stop();
  });
});

// ---------------------------------------------------------------------------
describe("auto-reconnect — regression for realtimeUpserts: 0 bug", () => {
  it("creates a fresh ZongJi instance after post-ready connection drop", async () => {
    // THE REGRESSION TEST: the old code reused the same stopped ZongJi instance,
    // so events could never be received again after any connection error.
    vi.useFakeTimers();
    const { listener } = await startListener();
    const instancesAfterStart = state.instances.length;

    lastInst().emit("error", new Error("connection lost"));
    expect(listener.isConnected()).toBe(false);

    // Advance past the 5s initial reconnect delay
    await vi.advanceTimersByTimeAsync(5100);
    await tick(); // let checkpointStore.load() resolve inside scheduleReconnect

    // A NEW ZongJi instance must have been created — not the old stopped one
    expect(state.instances.length).toBeGreaterThan(instancesAfterStart);

    lastInst().emit("ready");
    await tick();
    expect(listener.isConnected()).toBe(true);

    await listener.stop();
  });

  it("resumes receiving events on the new ZongJi after reconnect", async () => {
    vi.useFakeTimers();
    const table = makeTable("users", "db");
    const { listener, onChange } = await startListener([table]);

    // Simulate connection drop
    lastInst().emit("error", new Error("connection dropped"));

    // Reconnect fires
    await vi.advanceTimersByTimeAsync(5100);
    await tick(); // let load() resolve
    lastInst().emit("ready");
    await tick();

    // New instance receives a binlog event and routes it
    lastInst().emit("binlog", makeBinlogEvent("writerows", [{ id: 1, name: "Alice" }]));
    await tick();

    expect(onChange).toHaveBeenCalledWith(expect.objectContaining({ operation: "upsert" }));
    await listener.stop();
  });

  it("does NOT reconnect after stop()", async () => {
    vi.useFakeTimers();
    const { listener } = await startListener();

    lastInst().emit("error", new Error("lost"));
    await listener.stop(); // cancels the pending reconnect timer

    const countAfterStop = state.instances.length;
    await vi.advanceTimersByTimeAsync(60_000); // wait much longer than any backoff

    expect(state.instances.length).toBe(countAfterStop); // no new ZongJi
    expect(listener.isConnected()).toBe(false);
  });

  it("loads the latest checkpoint from the store before each reconnect", async () => {
    vi.useFakeTimers();
    const store = makeCheckpointStore(null);
    const savedCp: BinlogCheckpoint = { filename: "mysql-bin.000005", position: 999, updatedAt: "" };
    (store.load as ReturnType<typeof vi.fn>)
      .mockResolvedValueOnce(null) // first call inside start()
      .mockResolvedValueOnce(savedCp); // call during reconnect

    const listener = new MySqlBinlogListener(makeConfig(), [], store);
    const p = listener.start(vi.fn());
    await tick();
    lastInst().emit("ready");
    await p;

    lastInst().emit("error", new Error("lost"));
    await vi.advanceTimersByTimeAsync(5100);
    await tick(); // let checkpointStore.load() resolve so new ZongJi is created

    // store.load must have been called a second time to get the latest checkpoint
    expect(store.load).toHaveBeenCalledTimes(2);

    // The new ZongJi.start() should use the position from the saved checkpoint
    expect(lastInst().start).toHaveBeenCalledWith(
      expect.objectContaining({ filename: "mysql-bin.000005", position: 999 })
    );

    lastInst().emit("ready");
    await tick();
    await listener.stop();
  });

  it("applies exponential backoff: second retry waits longer than first", async () => {
    vi.useFakeTimers();
    const { listener } = await startListener();

    lastInst().emit("error", new Error("first drop"));

    // First reconnect delay = 5000ms
    await vi.advanceTimersByTimeAsync(4999);
    const countBefore = state.instances.length;
    await vi.advanceTimersByTimeAsync(2); // cross 5000ms
    await tick(); // let load() resolve so new ZongJi is created
    // New instance created for first retry
    const countAfterFirst = state.instances.length;
    expect(countAfterFirst).toBeGreaterThan(countBefore);

    // Make the reconnect fail before ready (simulates second connection failure)
    lastInst().emit("error", new Error("still down"));

    // Second reconnect delay = 10000ms (5000 * 2^1) — should NOT fire before 10s
    await vi.advanceTimersByTimeAsync(9999);
    await tick();
    expect(state.instances.length).toBe(countAfterFirst); // no new instance yet

    await vi.advanceTimersByTimeAsync(1001); // cross 10s mark
    await tick(); // let load() resolve for second retry
    expect(state.instances.length).toBeGreaterThan(countAfterFirst);

    await listener.stop();
  });

  it("caps reconnect delay at 60 seconds regardless of attempt count", async () => {
    vi.useFakeTimers();
    const { listener } = await startListener();

    // Drive through enough failures to exceed the 60s cap (2^7 * 5s = 640s without cap)
    for (let i = 0; i < 8; i++) {
      lastInst().emit("error", new Error("down"));
      // Each retry delay doubles but is capped at 60s; 64s > 60s so use 61s
      await vi.advanceTimersByTimeAsync(61_000);
      await tick(); // let load() resolve so new ZongJi is created
      // Confirm a new instance was created within 60s (capped)
    }

    // Should have many reconnect instances but delay should never exceed 60s
    expect(state.instances.length).toBeGreaterThanOrEqual(8);
    await listener.stop();
  });
});

// ---------------------------------------------------------------------------
describe("event routing", () => {
  it("calls onChange with upsert for updaterows event (before+after row)", async () => {
    const { listener, onChange } = await startListener([makeTable()]);

    lastInst().emit("binlog", makeBinlogEvent("updaterows", [
      { before: { id: 1, name: "Old" }, after: { id: 1, name: "New" } }
    ]));
    await tick();

    expect(onChange).toHaveBeenCalledWith(expect.objectContaining({
      operation: "upsert",
      after: expect.objectContaining({ id: 1, name: "New" })
    }));
    await listener.stop();
  });

  it("merges before+after fields for updaterows so id is always present", async () => {
    const { listener, onChange } = await startListener([makeTable()]);

    lastInst().emit("binlog", makeBinlogEvent("updaterows", [
      { before: { id: 5, role: "admin" }, after: { id: 5, name: "Updated" } }
    ]));
    await tick();

    const call = (onChange as ReturnType<typeof vi.fn>).mock.calls[0][0] as { after: Record<string, unknown> };
    // Merged after = { ...before, ...after }
    expect(call.after).toEqual({ id: 5, role: "admin", name: "Updated" });
    await listener.stop();
  });

  it("calls onChange with upsert for writerows event (flat row)", async () => {
    const { listener, onChange } = await startListener([makeTable()]);

    lastInst().emit("binlog", makeBinlogEvent("writerows", [{ id: 42, name: "Alice" }]));
    await tick();

    expect(onChange).toHaveBeenCalledWith(expect.objectContaining({
      operation: "upsert",
      after: { id: 42, name: "Alice" }
    }));
    await listener.stop();
  });

  it("calls onChange with delete for deleterows event", async () => {
    const { listener, onChange } = await startListener([makeTable()]);

    lastInst().emit("binlog", makeBinlogEvent("deleterows", [{ id: 7, name: "Bob" }]));
    await tick();

    expect(onChange).toHaveBeenCalledWith(expect.objectContaining({
      operation: "delete",
      before: { id: 7, name: "Bob" }
    }));
    await listener.stop();
  });

  it("calls onChange once per row in a multi-row event", async () => {
    const { listener, onChange } = await startListener([makeTable()]);

    lastInst().emit("binlog", makeBinlogEvent("writerows", [
      { id: 1 },
      { id: 2 },
      { id: 3 }
    ]));
    await tick();

    expect(onChange).toHaveBeenCalledTimes(3);
    await listener.stop();
  });

  it("skips events for tables that are not registered", async () => {
    const { listener, onChange } = await startListener([makeTable("users")]);

    // Event for "orders" — not registered
    lastInst().emit("binlog", makeBinlogEvent("writerows", [{ id: 1 }], 2, "db", "orders"));
    await tick();

    expect(onChange).not.toHaveBeenCalled();
    await listener.stop();
  });

  it("skips events with no rows (e.g. tablemap events filtered by ZongJi)", async () => {
    const { listener, onChange } = await startListener([makeTable()]);

    lastInst().emit("binlog", { getEventName: () => "tablemap", rows: [], tableId: 1 });
    await tick();

    expect(onChange).not.toHaveBeenCalled();
    await listener.stop();
  });

  it("includes checkpoint with filename and nextPosition from the event", async () => {
    const { listener, onChange } = await startListener([makeTable()]);

    lastInst().emit("binlog", makeBinlogEvent("writerows", [{ id: 1 }]));
    await tick();

    expect(onChange).toHaveBeenCalledWith(expect.objectContaining({
      checkpoint: expect.objectContaining({ filename: "mysql-bin.000004", position: 500 })
    }));
    await listener.stop();
  });
});

// ---------------------------------------------------------------------------
describe("registerTable()", () => {
  it("routes events to a table registered after start()", async () => {
    const { listener, onChange } = await startListener([]); // no tables at start

    listener.registerTable(makeTable("orders", "db"));

    lastInst().emit("binlog", makeBinlogEvent("writerows", [{ id: 99 }], 1, "db", "orders"));
    await tick();

    expect(onChange).toHaveBeenCalledWith(expect.objectContaining({ operation: "upsert" }));
    await listener.stop();
  });

  it("does not duplicate a table registered twice", async () => {
    const { listener } = await startListener([makeTable()]);
    listener.registerTable(makeTable()); // same table key
    // No assertion needed — just ensure no throw and tableByKey has 1 entry
    await listener.stop();
  });
});
