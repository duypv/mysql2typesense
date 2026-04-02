/**
 * Shared E2E test helpers.
 *
 * Prerequisites: docker compose up -d
 * Skip all E2E tests with: SKIP_E2E=1 npm test
 */

import { createPool, type Pool } from "mysql2/promise";

// ---------------------------------------------------------------------------
// Environment
// ---------------------------------------------------------------------------
export const SKIP = process.env.SKIP_E2E === "1" || process.env.SKIP_INTEGRATION === "1";

export const TS_HOST = process.env.TS_HOST ?? "localhost";
export const TS_PORT = process.env.TS_PORT ?? "8108";
export const TS_API_KEY = process.env.TS_API_KEY ?? "xyz";
export const TS_BASE = `http://${TS_HOST}:${TS_PORT}`;
export const TS_HEADERS = { "X-TYPESENSE-API-KEY": TS_API_KEY };

export const MON_HOST = process.env.MON_HOST ?? "localhost";
export const MON_PORT = process.env.MON_PORT ?? "8080";
export const MON_AUTH = process.env.MON_AUTH ?? "changeme";
export const MON_BASE = `http://${MON_HOST}:${MON_PORT}`;

export const MYSQL_HOST = process.env.MYSQL_HOST ?? "localhost";
export const MYSQL_PORT = parseInt(process.env.MYSQL_PORT ?? "3306");
export const MYSQL_USER = process.env.MYSQL_USER ?? "root";
export const MYSQL_PASS = process.env.MYSQL_PASS ?? "root";
export const MYSQL_DB = process.env.MYSQL_DB ?? "app";

// ---------------------------------------------------------------------------
// Typesense helpers
// ---------------------------------------------------------------------------

export async function tsGet(path: string): Promise<Response> {
  return fetch(`${TS_BASE}${path}`, { headers: TS_HEADERS });
}

export async function tsGetJson<T = unknown>(path: string): Promise<T> {
  const res = await tsGet(path);
  return res.json() as Promise<T>;
}

export interface SearchHit<T = Record<string, unknown>> {
  document: T;
}

export interface SearchResult<T = Record<string, unknown>> {
  found: number;
  hits: SearchHit<T>[];
}

export async function tsSearch<T = Record<string, unknown>>(
  collection: string,
  queryBy: string,
  q = "*",
  extra = ""
): Promise<SearchResult<T>> {
  const path = `/collections/${collection}/documents/search?q=${encodeURIComponent(q)}&query_by=${queryBy}${extra ? `&${extra}` : ""}`;
  return tsGetJson<SearchResult<T>>(path);
}

// ---------------------------------------------------------------------------
// Monitoring server helpers
// ---------------------------------------------------------------------------

export function monHeaders(withAuth = true): HeadersInit {
  if (!withAuth) return {};
  const token = Buffer.from(`any:${MON_AUTH}`).toString("base64");
  return { Authorization: `Basic ${token}` };
}

export async function monGet(path: string, withAuth = true): Promise<Response> {
  return fetch(`${MON_BASE}${path}`, { headers: monHeaders(withAuth) });
}

export async function monPost(path: string, withAuth = true): Promise<Response> {
  return fetch(`${MON_BASE}${path}`, {
    method: "POST",
    headers: monHeaders(withAuth),
  });
}

// ---------------------------------------------------------------------------
// MySQL helpers
// ---------------------------------------------------------------------------

let _pool: Pool | null = null;

export function getMysqlPool(): Pool {
  if (!_pool) {
    _pool = createPool({
      host: MYSQL_HOST,
      port: MYSQL_PORT,
      user: MYSQL_USER,
      password: MYSQL_PASS,
      database: MYSQL_DB,
    });
  }
  return _pool;
}

export async function mysqlQuery<T = Record<string, unknown>>(sql: string, values?: unknown[]): Promise<T[]> {
  const pool = getMysqlPool();
  const [rows] = await pool.query(sql, values);
  return rows as T[];
}

/** Execute an INSERT/UPDATE/DELETE and return insertId + affectedRows. */
export async function mysqlWrite(sql: string, values?: unknown[]): Promise<{ insertId: number; affectedRows: number }> {
  const pool = getMysqlPool();
  const [result] = await pool.query(sql, values);
  return result as { insertId: number; affectedRows: number };
}

export async function closeMysqlPool(): Promise<void> {
  if (_pool) {
    await _pool.end();
    _pool = null;
  }
}

// ---------------------------------------------------------------------------
// Polling helper
// ---------------------------------------------------------------------------

export interface PollOptions {
  /** Max total wait time in ms (default: 10_000) */
  timeout?: number;
  /** Interval between checks in ms (default: 500) */
  interval?: number;
  /** Human-readable description for timeout error */
  label?: string;
}

/**
 * Repeatedly calls `fn` until it returns a truthy value or the timeout elapses.
 * Returns the last truthy value on success; throws on timeout.
 */
export async function poll<T>(fn: () => Promise<T | null | undefined | false>, opts: PollOptions = {}): Promise<T> {
  const { timeout = 10_000, interval = 500, label = "condition" } = opts;
  const deadline = Date.now() + timeout;

  while (Date.now() < deadline) {
    const result = await fn();
    if (result) return result as T;
    await new Promise((resolve) => setTimeout(resolve, interval));
  }

  throw new Error(`Timed out waiting for ${label} after ${timeout}ms`);
}
