import mysql, { type Pool, type PoolOptions } from "mysql2/promise";

import type { AppConfig } from "../../core/types.js";

export function createMysqlPool(config: AppConfig): Pool {
  const timezone = config.mysql.timezone ?? "local";

  const options: PoolOptions = {
    host: config.mysql.host,
    port: config.mysql.port,
    user: config.mysql.user,
    password: config.mysql.password,
    database: config.mysql.database,
    waitForConnections: true,
    connectionLimit: 10,
    namedPlaceholders: true,
    // Parse timezone-naive DATETIME/DATE values in the source DB's offset so
    // JS Dates are true absolute instants (not the wall clock re-read in the
    // container's timezone).
    timezone
  };

  const pool = mysql.createPool(options);

  // TIMESTAMP columns are rendered by the server in the session time_zone.
  // Pin it to the configured offset so rendering matches the parsing offset
  // above regardless of the server's default time_zone.
  if (timezone !== "local") {
    const sessionTz = timezone === "Z" ? "+00:00" : timezone;
    pool.pool.on("connection", (connection) => {
      connection.query(`SET time_zone = '${sessionTz}'`);
    });
  }

  return pool;
}
