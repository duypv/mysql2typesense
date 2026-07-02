/**
 * Unit tests for source-timezone handling.
 *
 * Background: MySQL DATETIME/DATE columns are wall-clock values with no timezone.
 * The binlog reader (zongji) decodes them as Date.UTC(wall components), i.e. the
 * wall-clock read as UTC. To store true absolute instants in Typesense, that Date
 * must be shifted by the source database's UTC offset (e.g. +07:00 for
 * Asia/Ho_Chi_Minh).
 */
import { describe, expect, it } from "vitest";

import { parseTimezoneOffsetMs, wallClockUtcToInstant } from "../../src/utils/timezone.js";

describe("parseTimezoneOffsetMs", () => {
  it("parses positive offsets like +07:00", () => {
    expect(parseTimezoneOffsetMs("+07:00")).toBe(7 * 3600 * 1000);
  });

  it("parses negative offsets like -03:30", () => {
    expect(parseTimezoneOffsetMs("-03:30")).toBe(-(3.5 * 3600 * 1000));
  });

  it("parses Z as zero offset", () => {
    expect(parseTimezoneOffsetMs("Z")).toBe(0);
  });

  it("parses +00:00 as zero offset", () => {
    expect(parseTimezoneOffsetMs("+00:00")).toBe(0);
  });

  it("returns null for 'local' (no fixed offset)", () => {
    expect(parseTimezoneOffsetMs("local")).toBeNull();
  });

  it("throws on garbage input", () => {
    expect(() => parseTimezoneOffsetMs("saigon")).toThrow();
    expect(() => parseTimezoneOffsetMs("+7:00")).toThrow();
    expect(() => parseTimezoneOffsetMs("")).toThrow();
  });
});

describe("wallClockUtcToInstant", () => {
  it("shifts a wall-clock-as-UTC Date back by the source offset", () => {
    // DB (Asia/Ho_Chi_Minh, +07:00) wall clock: 2026-07-02 18:00:00
    // zongji decodes it as 2026-07-02T18:00:00Z
    const wallAsUtc = new Date("2026-07-02T18:00:00Z");
    const instant = wallClockUtcToInstant(wallAsUtc, "+07:00");
    // True instant: 2026-07-02T11:00:00Z
    expect(instant.toISOString()).toBe("2026-07-02T11:00:00.000Z");
  });

  it("keeps the Date unchanged for Z / +00:00", () => {
    const wallAsUtc = new Date("2026-07-02T18:00:00Z");
    expect(wallClockUtcToInstant(wallAsUtc, "Z").getTime()).toBe(wallAsUtc.getTime());
    expect(wallClockUtcToInstant(wallAsUtc, "+00:00").getTime()).toBe(wallAsUtc.getTime());
  });

  it("preserves milliseconds", () => {
    const wallAsUtc = new Date("2026-07-02T18:00:00.123Z");
    const instant = wallClockUtcToInstant(wallAsUtc, "+07:00");
    expect(instant.toISOString()).toBe("2026-07-02T11:00:00.123Z");
  });

  it("interprets the wall clock in the process timezone when timezone is 'local'", () => {
    const wallAsUtc = new Date(Date.UTC(2026, 6, 2, 18, 0, 0));
    const instant = wallClockUtcToInstant(wallAsUtc, "local");
    // Equivalent of constructing the same wall-clock in the local timezone
    const expected = new Date(2026, 6, 2, 18, 0, 0);
    expect(instant.getTime()).toBe(expected.getTime());
  });
});
