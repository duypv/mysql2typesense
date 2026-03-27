import "dotenv/config";

import { z } from "zod";

import type { AppConfig } from "../core/types.js";
import { loadSyncConfig } from "./sync-config.js";

const envSchema = z.object({
  DB_HOST: z.string().min(1),
  DB_PORT: z.coerce.number().int().positive().default(3306),
  DB_USER: z.string().min(1),
  DB_PASS: z.string().default(""),
  DB_NAME: z.string().min(1).default("app"),
  SYNC_CONFIG_PATH: z.string().min(1).default("config/sync.config.json"),
  SYNC_BATCH_SIZE: z.coerce.number().int().positive().default(1000),
  TS_NODE_HOST: z.string().min(1),
  TS_NODE_PORT: z.coerce.number().int().positive().default(8108),
  TS_NODE_PROTOCOL: z.enum(["http", "https"]).default("http"),
  TS_API_KEY: z.string().min(1),
  CHECKPOINT_DRIVER: z.enum(["file", "redis"]).default("redis"),
  CHECKPOINT_FILE: z.string().min(1).default("storage/checkpoints/binlog.json"),
  CHECKPOINT_REDIS_KEY: z.string().min(1).default("mysql2typesense:binlog"),
  REDIS_URL: z.string().min(1).default("redis://127.0.0.1:6379"),
  MONITORING_ENABLED: z
    .string()
    .default("true")
    .transform((value) => !["0", "false", "no", "off"].includes(value.trim().toLowerCase())),
  MONITORING_HOST: z.string().min(1).default("0.0.0.0"),
  MONITORING_PORT: z.coerce.number().int().positive().default(8080),
  MONITORING_AUTH_TOKEN: z.string().optional(),
  RETRY_MAX_ATTEMPTS: z.coerce.number().int().positive().default(5),
  RETRY_BASE_DELAY_MS: z.coerce.number().int().positive().default(500),
  LOG_LEVEL: z.string().default("info")
});

export function loadConfig(): AppConfig {
  const env = envSchema.parse(process.env);
  const syncConfig = loadSyncConfig(env.SYNC_CONFIG_PATH);

  return {
    mysql: {
      host: env.DB_HOST,
      port: env.DB_PORT,
      user: env.DB_USER,
      password: env.DB_PASS,
      database: env.DB_NAME
    },
    sync: {
      batchSize: env.SYNC_BATCH_SIZE,
      database: syncConfig.database,
      tables: syncConfig.tables,
      retry: {
        maxAttempts: env.RETRY_MAX_ATTEMPTS,
        baseDelayMs: env.RETRY_BASE_DELAY_MS
      }
    },
    typesense: {
      host: env.TS_NODE_HOST,
      port: env.TS_NODE_PORT,
      protocol: env.TS_NODE_PROTOCOL,
      apiKey: env.TS_API_KEY
    },
    checkpoint: {
      driver: env.CHECKPOINT_DRIVER,
      filePath: env.CHECKPOINT_FILE,
      redisUrl: env.REDIS_URL,
      redisKey: env.CHECKPOINT_REDIS_KEY
    },
    monitoring: {
      enabled: env.MONITORING_ENABLED,
      host: env.MONITORING_HOST,
      port: env.MONITORING_PORT,
      authToken: env.MONITORING_AUTH_TOKEN
    },
    logLevel: env.LOG_LEVEL
  };
}