/**
 * Unit tests for binlog wall-clock datetime normalization.
 *
 * zongji decodes binlog values with two different conventions:
 *   - TIMESTAMP: true epoch (absolute instant) — already correct.
 *   - DATETIME / DATE: Date.UTC(wall components) — the DB wall clock read as UTC,
 *     off by the source timezone offset.
 *
 * The normalizer must shift only DATETIME/DATE Dates back to true instants,
 * using the table's column schemas from the binlog tablemap event.
 */
import { describe, expect, it } from "vitest";

import {
  collectWallClockColumns,
  normalizeBinlogRow
} from "../../src/modules/mysql/binlog-row-normalizer.js";

const columnSchemas = [
  { COLUMN_NAME: "id", COLUMN_TYPE: "bigint unsigned" },
  { COLUMN_NAME: "name", COLUMN_TYPE: "varchar(255)" },
  { COLUMN_NAME: "created_at", COLUMN_TYPE: "datetime" },
  { COLUMN_NAME: "updated_at", COLUMN_TYPE: "datetime(3)" },
  { COLUMN_NAME: "synced_at", COLUMN_TYPE: "timestamp" },
  { COLUMN_NAME: "birthday", COLUMN_TYPE: "date" }
];

describe("collectWallClockColumns", () => {
  it("returns DATETIME and DATE columns but not TIMESTAMP or others", () => {
    expect(collectWallClockColumns(columnSchemas)).toEqual([
      "created_at",
      "updated_at",
      "birthday"
    ]);
  });

  it("returns an empty array for undefined schemas", () => {
    expect(collectWallClockColumns(undefined)).toEqual([]);
  });
});

describe("normalizeBinlogRow", () => {
  it("shifts DATETIME Dates by the source offset and leaves TIMESTAMP untouched", () => {
    const row = {
      id: 1,
      name: "Alice",
      // DB wall clock 18:00 (+07) decoded by zongji as 18:00Z
      created_at: new Date("2026-07-02T18:00:00Z"),
      // TIMESTAMP decoded as the true instant already
      synced_at: new Date("2026-07-02T11:00:00Z")
    };

    normalizeBinlogRow(row, ["created_at", "updated_at", "birthday"], "+07:00");

    expect((row.created_at as Date).toISOString()).toBe("2026-07-02T11:00:00.000Z");
    expect((row.synced_at as Date).toISOString()).toBe("2026-07-02T11:00:00.000Z");
    expect(row.id).toBe(1);
    expect(row.name).toBe("Alice");
  });

  it("ignores null and non-Date values in wall-clock columns", () => {
    const row: Record<string, unknown> = {
      created_at: null,
      updated_at: "0000-00-00 00:00:00",
      birthday: undefined
    };

    normalizeBinlogRow(row, ["created_at", "updated_at", "birthday"], "+07:00");

    expect(row.created_at).toBeNull();
    expect(row.updated_at).toBe("0000-00-00 00:00:00");
    expect(row.birthday).toBeUndefined();
  });

  it("is a no-op when the offset is zero", () => {
    const created = new Date("2026-07-02T18:00:00Z");
    const row = { created_at: new Date(created) };

    normalizeBinlogRow(row, ["created_at"], "Z");

    expect((row.created_at as Date).getTime()).toBe(created.getTime());
  });
});
