/**
 * E2E tests: Monitoring API
 *
 * Verifies all monitoring server endpoints exposed on port 8080.
 *
 * Endpoints tested:
 *   GET  /health                  — public, returns { ok, mode }
 *   GET  /metrics                 — public, Prometheus text format
 *   GET  /api/status              — public, sync snapshot
 *   GET  /api/collections         — admin (auth required), returns Typesense collections
 *   GET  /api/discovered-tables   — admin (auth required)
 *   GET  /                        — admin (auth required), dashboard HTML
 *   401 behaviour on admin routes without auth
 *
 * Skip with: SKIP_E2E=1 npm test
 */

import { describe, expect, it } from "vitest";
import { monGet, monPost, SKIP } from "./helpers.js";

// ---------------------------------------------------------------------------
// Public endpoints (no auth required)
// ---------------------------------------------------------------------------
describe.skipIf(SKIP)("monitoring API — public endpoints", () => {
  it("GET /health returns 200 with ok:true", async () => {
    const res = await monGet("/health", false);
    expect(res.status).toBe(200);
    const body = (await res.json()) as { ok: boolean; mode: string };
    expect(body.ok).toBe(true);
    expect(typeof body.mode).toBe("string");
  });

  it("GET /health mode is a valid sync state", async () => {
    const res = await monGet("/health", false);
    const body = (await res.json()) as { mode: string };
    expect(["idle", "initial", "realtime"]).toContain(body.mode);
  });

  it("GET /metrics returns 200 with Prometheus text", async () => {
    const res = await monGet("/metrics", false);
    expect(res.status).toBe(200);
    const contentType = res.headers.get("content-type") ?? "";
    expect(contentType).toContain("text/plain");
    const text = await res.text();
    expect(text).toContain("mysql2typesense_");
  });

  it("GET /api/status returns 200 with mode and tables", async () => {
    const res = await monGet("/api/status", false);
    expect(res.status).toBe(200);
    const body = (await res.json()) as {
      mode: string;
      tables: string[];
      counters: { errors: number };
      startedAt: string;
    };
    expect(["idle", "initial", "realtime"]).toContain(body.mode);
    expect(Array.isArray(body.tables)).toBe(true);
    expect(body.tables.length).toBe(5);
    expect(body.counters.errors).toBe(0);
    expect(typeof body.startedAt).toBe("string");
  });

  it("GET /api/status tables include all 5 synced tables", async () => {
    const res = await monGet("/api/status", false);
    const body = (await res.json()) as { tables: string[] };
    const expected = ["app.users", "app.products", "app.ParentRef", "app.ChildJoin", "app.json_case_test"];
    for (const table of expected) {
      expect(body.tables).toContain(table);
    }
  });

  it("GET /api/status counters.initialDocuments > 0", async () => {
    const res = await monGet("/api/status", false);
    const body = (await res.json()) as { counters: { initialDocuments: number } };
    expect(body.counters.initialDocuments).toBeGreaterThan(0);
  });

  it("GET /api/status perTable has entries for all tables", async () => {
    const res = await monGet("/api/status", false);
    const body = (await res.json()) as { perTable: Record<string, unknown> };
    expect(Object.keys(body.perTable).length).toBe(5);
    expect(body.perTable["app.users"]).toBeDefined();
    expect(body.perTable["app.products"]).toBeDefined();
  });
});

// ---------------------------------------------------------------------------
// Auth enforcement on admin routes
// ---------------------------------------------------------------------------
describe.skipIf(SKIP)("monitoring API — authentication", () => {
  it("GET / without auth returns 401", async () => {
    const res = await monGet("/", false);
    expect(res.status).toBe(401);
    const wwwAuth = res.headers.get("www-authenticate");
    expect(wwwAuth).toContain("Basic");
  });

  it("GET /api/collections without auth returns 401", async () => {
    const res = await monGet("/api/collections", false);
    expect(res.status).toBe(401);
  });

  it("GET /api/discovered-tables without auth returns 401", async () => {
    const res = await monGet("/api/discovered-tables", false);
    expect(res.status).toBe(401);
  });

  it("POST /api/reset without auth returns 401", async () => {
    const res = await monPost("/api/reset", false);
    expect(res.status).toBe(401);
  });
});

// ---------------------------------------------------------------------------
// Admin endpoints with auth
// ---------------------------------------------------------------------------
describe.skipIf(SKIP)("monitoring API — admin endpoints (with auth)", () => {
  it("GET / with auth returns 200 HTML dashboard", async () => {
    const res = await monGet("/", true);
    expect(res.status).toBe(200);
    const contentType = res.headers.get("content-type") ?? "";
    expect(contentType).toContain("text/html");
    const html = await res.text();
    expect(html.toLowerCase()).toContain("mysql2typesense");
  });

  it("GET /api/collections returns array of collections", async () => {
    const res = await monGet("/api/collections", true);
    expect(res.status).toBe(200);
    const body = (await res.json()) as { collections: { name: string }[] };
    expect(Array.isArray(body.collections)).toBe(true);
    const names = body.collections.map((c) => c.name);
    expect(names).toContain("users");
    expect(names).toContain("products");
    expect(names).toContain("ParentRef");
    expect(names).toContain("ChildJoin");
  });

  it("GET /api/discovered-tables returns discovery info", async () => {
    const res = await monGet("/api/discovered-tables", true);
    expect(res.status).toBe(200);
    const body = (await res.json()) as {
      autoDiscoveryEnabled: boolean;
      currentTables: string[];
    };
    expect(typeof body.autoDiscoveryEnabled).toBe("boolean");
    expect(Array.isArray(body.currentTables)).toBe(true);
  });

  it("GET /dashboard with auth returns 200 HTML", async () => {
    const res = await monGet("/dashboard", true);
    expect(res.status).toBe(200);
    const contentType = res.headers.get("content-type") ?? "";
    expect(contentType).toContain("text/html");
  });

  it("POST /api/reindex/users returns 200 or 409 (conflict if in-flight)", async () => {
    const res = await monPost("/api/reindex/users", true);
    const body = (await res.json()) as { ok: boolean };
    // API returns 200 if reindex started successfully, 409 if already in progress
    expect([200, 409]).toContain(res.status);
    expect(typeof body.ok).toBe("boolean");
  });

  it("GET /health still works without auth (not admin route)", async () => {
    const res = await monGet("/health", false);
    expect(res.status).toBe(200);
  });

  it("unknown route returns 404", async () => {
    const res = await monGet("/api/nonexistent-route", false);
    expect(res.status).toBe(404);
  });
});
