import mysql, { type Pool, type PoolOptions } from "mysql2/promise";

import type { AppConfig } from "../../core/types.js";

export function createMysqlPool(config: AppConfig): Pool {
  const options: PoolOptions = {
    host: config.mysql.host,
    port: config.mysql.port,
    user: config.mysql.user,
    password: config.mysql.password,
    database: config.mysql.database,
    waitForConnections: true,
    connectionLimit: 10,
    namedPlaceholders: true
  };

  return mysql.createPool(options);
}