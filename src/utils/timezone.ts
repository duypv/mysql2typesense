/**
 * Source-timezone helpers.
 *
 * MySQL DATETIME/DATE columns are wall-clock values with no timezone attached.
 * Depending on the read path they arrive in Node as a Date whose UTC fields hold
 * the raw wall clock (zongji decodes via Date.UTC). These helpers convert such
 * "wall-clock as UTC" Dates into true absolute instants using the source
 * database's UTC offset (e.g. "+07:00" for Asia/Ho_Chi_Minh).
 */

const OFFSET_PATTERN = /^([+-])(\d{2}):(\d{2})$/;

/**
 * Parses a timezone spec into a UTC offset in milliseconds.
 * Accepts "Z", "+HH:MM", "-HH:MM" (same format as the mysql2 `timezone` option).
 * Returns null for "local" — the offset then depends on the process timezone
 * (and possibly DST) at the moment being converted.
 */
export function parseTimezoneOffsetMs(timezone: string): number | null {
  if (timezone === "local") {
    return null;
  }

  if (timezone === "Z") {
    return 0;
  }

  const match = OFFSET_PATTERN.exec(timezone);
  if (!match) {
    throw new Error(
      `Invalid timezone "${timezone}" — expected "local", "Z", "+HH:MM" or "-HH:MM"`
    );
  }

  const sign = match[1] === "-" ? -1 : 1;
  const hours = Number(match[2]);
  const minutes = Number(match[3]);
  return sign * (hours * 60 + minutes) * 60 * 1000;
}

/**
 * Converts a Date whose UTC fields carry the source wall clock into the true
 * absolute instant, interpreting that wall clock in the given timezone.
 */
export function wallClockUtcToInstant(wallAsUtc: Date, timezone: string): Date {
  const offsetMs = parseTimezoneOffsetMs(timezone);

  if (offsetMs === null) {
    // Rebuild the wall clock in the process-local timezone (handles DST per date).
    return new Date(
      wallAsUtc.getUTCFullYear(),
      wallAsUtc.getUTCMonth(),
      wallAsUtc.getUTCDate(),
      wallAsUtc.getUTCHours(),
      wallAsUtc.getUTCMinutes(),
      wallAsUtc.getUTCSeconds(),
      wallAsUtc.getUTCMilliseconds()
    );
  }

  return new Date(wallAsUtc.getTime() - offsetMs);
}
