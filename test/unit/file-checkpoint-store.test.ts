import { mkdirSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";

import { FileCheckpointStore } from "../../src/modules/checkpoint/file-checkpoint-store.js";

const TMP_BASE = join(tmpdir(), `checkpoint-test-${process.pid}`);
let tmpDir: string;
let counter = 0;

beforeEach(() => {
  tmpDir = join(TMP_BASE, String(counter++));
  mkdirSync(tmpDir, { recursive: true });
});

afterEach(() => {
  rmSync(TMP_BASE, { recursive: true, force: true });
});

describe("FileCheckpointStore", () => {
  it("load() returns null when file does not exist", async () => {
    const store = new FileCheckpointStore(join(tmpDir, "checkpoint.json"));
    expect(await store.load()).toBeNull();
  });

  it("save() then load() round-trips checkpoint data", async () => {
    const path = join(tmpDir, "checkpoint.json");
    const store = new FileCheckpointStore(path);
    const checkpoint = { binlogName: "mysql-bin.000001", binlogPosition: 12345 };
    await store.save(checkpoint);
    expect(await store.load()).toEqual(checkpoint);
  });

  it("save() creates intermediate directories", async () => {
    const path = join(tmpDir, "deep", "nested", "checkpoint.json");
    const store = new FileCheckpointStore(path);
    const checkpoint = { binlogName: "mysql-bin.000002", binlogPosition: 999 };
    await expect(store.save(checkpoint)).resolves.not.toThrow();
    expect(await store.load()).toEqual(checkpoint);
  });

  it("save() overwrites previous checkpoint", async () => {
    const path = join(tmpDir, "checkpoint.json");
    const store = new FileCheckpointStore(path);
    await store.save({ binlogName: "mysql-bin.000001", binlogPosition: 100 });
    await store.save({ binlogName: "mysql-bin.000002", binlogPosition: 999 });
    expect(await store.load()).toEqual({ binlogName: "mysql-bin.000002", binlogPosition: 999 });
  });

  it("close() resolves without error", async () => {
    const store = new FileCheckpointStore(join(tmpDir, "checkpoint.json"));
    await expect(store.close()).resolves.not.toThrow();
  });
});
