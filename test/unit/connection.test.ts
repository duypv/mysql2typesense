/**
 * Unit tests for createMysqlPool — source timezone wiring.
 *
 * The mysql2 `timezone` option controls how timezone-naive DATETIME/DATE strings
 * are parsed into JS Dates. It must match the MySQL server's offset so the
 * initial-sync path produces true absolute instants.
 */
import { describe, expect, it } from "vitest";

import type { AppConfig } from "../../src/core/types.js";
import { createMysqlPool } from "../../src/modules/mysql/connection.js";

function makeConfig(timezone: string): AppConfig {
  return {
    mysql: { host: "localhost", port: 3306, user: "user", password: "pass", database: "db", timezone },
    sync: {
      batchSize: 100,
      database: undefined,
      tables: [],
      retry: { maxAttempts: 3, baseDelayMs: 100 },
      joinConfigs: [],
      reconcileIntervalMs: 0
    },
    typesense: { host: "localhost", port: 8108, protocol: "http", apiKey: "key" },
    checkpoint: { driver: "file", filePath: "/tmp/test.json" },
    monitoring: { host: "localhost", port: 8080, enabled: false },
    logLevel: "silent"
  };
}

describe("createMysqlPool timezone", () => {
  it("passes the configured offset to mysql2 so DATETIME parses as true instants", async () => {
    const pool = createMysqlPool(makeConfig("+07:00"));
    const connectionConfig = (pool.pool.config as unknown as { connectionConfig: { timezone?: string } })
      .connectionConfig;
    expect(connectionConfig.timezone).toBe("+07:00");
    await pool.end();
  });

  it("aligns the session time_zone with the configured offset for TIMESTAMP columns", async () => {
    const pool = createMysqlPool(makeConfig("+07:00"));
    // A per-connection init hook must be registered so every pooled connection
    // runs SET time_zone — otherwise TIMESTAMP rendering depends on the server default.
    expect(pool.pool.listenerCount("connection")).toBeGreaterThanOrEqual(1);
    await pool.end();
  });

  it("keeps legacy behaviour with 'local': process tz parsing, no session override", async () => {
    const pool = createMysqlPool(makeConfig("local"));
    const connectionConfig = (pool.pool.config as unknown as { connectionConfig: { timezone?: string } })
      .connectionConfig;
    expect(connectionConfig.timezone).toBe("local");
    expect(pool.pool.listenerCount("connection")).toBe(0);
    await pool.end();
  });
});
