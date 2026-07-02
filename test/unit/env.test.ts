/**
 * Unit tests for environment config loading — DB_TIMEZONE.
 */
import { afterEach, beforeEach, describe, expect, it } from "vitest";

import { loadConfig } from "../../src/config/env.js";

const REQUIRED_ENV = {
  DB_HOST: "localhost",
  DB_USER: "user",
  TS_NODE_HOST: "localhost",
  TS_API_KEY: "key",
  SYNC_CONFIG_PATH: "config/__missing__.json"
};

let savedEnv: NodeJS.ProcessEnv;

beforeEach(() => {
  savedEnv = { ...process.env };
  Object.assign(process.env, REQUIRED_ENV);
  delete process.env.DB_TIMEZONE;
});

afterEach(() => {
  process.env = savedEnv;
});

describe("DB_TIMEZONE", () => {
  it("defaults to +07:00 when unset (Asia/Ho_Chi_Minh source databases)", () => {
    const config = loadConfig();
    expect(config.mysql.timezone).toBe("+07:00");
  });

  it("accepts 'local' explicitly", () => {
    process.env.DB_TIMEZONE = "local";
    const config = loadConfig();
    expect(config.mysql.timezone).toBe("local");
  });

  it("accepts a fixed offset like +07:00", () => {
    process.env.DB_TIMEZONE = "+07:00";
    const config = loadConfig();
    expect(config.mysql.timezone).toBe("+07:00");
  });

  it("accepts Z", () => {
    process.env.DB_TIMEZONE = "Z";
    const config = loadConfig();
    expect(config.mysql.timezone).toBe("Z");
  });

  it("rejects invalid values", () => {
    process.env.DB_TIMEZONE = "Asia/Ho_Chi_Minh";
    expect(() => loadConfig()).toThrow();
  });
});
