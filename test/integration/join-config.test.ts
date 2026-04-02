/**
 * Integration tests for the join_configs feature.
 *
 * Prerequisites (must be running):
 *   docker compose up -d
 *
 * These tests call the live Typesense instance at localhost:8108 to verify
 * that the join_configs in sync.config.json are correctly materialised:
 *   - ChildJoin.parentId has type "string" and reference "ParentRef.id"
 *   - Typesense join query returns embedded ParentRef sub-documents
 *   - parentId values are stored as strings (not ints)
 *
 * Run with: npm test  (vitest picks up test/integration/*.test.ts)
 * Skip when stack is not running — tests are guarded by SKIP_INTEGRATION env.
 */

import { describe, expect, it } from "vitest";

const TS_HOST = process.env.TS_HOST ?? "localhost";
const TS_PORT = process.env.TS_PORT ?? "8108";
const TS_API_KEY = process.env.TS_API_KEY ?? "xyz";
const BASE = `http://${TS_HOST}:${TS_PORT}`;
const HEADERS = { "X-TYPESENSE-API-KEY": TS_API_KEY };

const SKIP = process.env.SKIP_INTEGRATION === "1";

async function tsGet(path: string): Promise<Response> {
  return fetch(`${BASE}${path}`, { headers: HEADERS });
}

// ---------------------------------------------------------------------------
// Schema verification
// ---------------------------------------------------------------------------
describe.skipIf(SKIP)("join_configs — ChildJoin schema", () => {
  it("ChildJoin collection exists", async () => {
    const res = await tsGet("/collections/ChildJoin");
    expect(res.status).toBe(200);
  });

  it("parentId field has type 'string'", async () => {
    const res = await tsGet("/collections/ChildJoin");
    const col = (await res.json()) as { fields: { name: string; type: string }[] };
    const field = col.fields.find((f) => f.name === "parentId");
    expect(field).toBeDefined();
    expect(field?.type).toBe("string");
  });

  it("parentId field has reference 'ParentRef.id'", async () => {
    const res = await tsGet("/collections/ChildJoin");
    const col = (await res.json()) as { fields: { name: string; reference?: string }[] };
    const field = col.fields.find((f) => f.name === "parentId");
    expect(field?.reference).toBe("ParentRef.id");
  });

  it("ParentRef collection exists", async () => {
    const res = await tsGet("/collections/ParentRef");
    expect(res.status).toBe(200);
  });

  it("ParentRef has id field of type 'string'", async () => {
    // Typesense always returns 'id' as string
    const res = await tsGet("/collections/ParentRef/documents/search?q=*&query_by=code");
    const data = (await res.json()) as { hits: { document: { id: string } }[] };
    expect(data.hits.length).toBeGreaterThan(0);
    data.hits.forEach((hit) => {
      expect(typeof hit.document.id).toBe("string");
    });
  });
});

// ---------------------------------------------------------------------------
// Document data verification
// ---------------------------------------------------------------------------
describe.skipIf(SKIP)("join_configs — ChildJoin documents", () => {
  it("has 2 synced documents", async () => {
    const res = await tsGet("/collections/ChildJoin/documents/search?q=*&query_by=note");
    const data = (await res.json()) as { found: number };
    expect(data.found).toBe(2);
  });

  it("parentId values are stored as strings", async () => {
    const res = await tsGet("/collections/ChildJoin/documents/search?q=*&query_by=note");
    const data = (await res.json()) as { hits: { document: { parentId: unknown } }[] };
    data.hits.forEach((hit) => {
      expect(typeof hit.document.parentId).toBe("string");
    });
  });

  it("parentId '1' references ParentRef document id '1'", async () => {
    const res = await tsGet(
      "/collections/ChildJoin/documents/search?q=first&query_by=note"
    );
    const data = (await res.json()) as { hits: { document: { parentId: string } }[] };
    expect(data.hits.length).toBeGreaterThan(0);
    expect(data.hits[0].document.parentId).toBe("1");
  });
});

// ---------------------------------------------------------------------------
// Join query verification
// ---------------------------------------------------------------------------
describe.skipIf(SKIP)("join_configs — Typesense join query", () => {
  type JoinDoc = {
    parentId: string;
    note: string;
    amount: number;
    ParentRef?: { id: string; code: string; price: number };
  };

  async function searchWithJoin(): Promise<JoinDoc[]> {
    const params = new URLSearchParams({
      q: "*",
      query_by: "note",
      include_fields: "$ParentRef(id,code,price),parentId,note,amount"
    });
    const res = await tsGet(`/collections/ChildJoin/documents/search?${params}`);
    const data = (await res.json()) as { hits: { document: JoinDoc }[] };
    return data.hits.map((h) => h.document);
  }

  it("join query returns 2 results", async () => {
    const docs = await searchWithJoin();
    expect(docs).toHaveLength(2);
  });

  it("each document contains embedded ParentRef sub-document", async () => {
    const docs = await searchWithJoin();
    docs.forEach((doc) => {
      expect(doc.ParentRef).toBeDefined();
      expect(typeof doc.ParentRef?.code).toBe("string");
      expect(typeof doc.ParentRef?.price).toBe("number");
    });
  });

  it("ChildJoin id=1 joins to ParentRef code=SVC-001 price=99.5", async () => {
    const docs = await searchWithJoin();
    const first = docs.find((d) => d.note === "first");
    expect(first?.ParentRef?.code).toBe("SVC-001");
    expect(first?.ParentRef?.price).toBeCloseTo(99.5);
  });

  it("ChildJoin id=2 joins to ParentRef code=SVC-002 price=149", async () => {
    const docs = await searchWithJoin();
    const second = docs.find((d) => d.note === "second");
    expect(second?.ParentRef?.code).toBe("SVC-002");
    expect(second?.ParentRef?.price).toBeCloseTo(149);
  });

  it("parentId matches the joined ParentRef.id", async () => {
    const docs = await searchWithJoin();
    docs.forEach((doc) => {
      expect(doc.parentId).toBe(doc.ParentRef?.id);
    });
  });
});

// ---------------------------------------------------------------------------
// Monitoring endpoint
// ---------------------------------------------------------------------------
describe.skipIf(SKIP)("monitoring API", () => {
  const MON_BASE = `http://localhost:${process.env.MONITORING_PORT ?? "8080"}`;
  const MON_TOKEN = process.env.MONITORING_AUTH_TOKEN ?? "changeme";

  it("GET /api/status returns 200", async () => {
    const res = await fetch(`${MON_BASE}/api/status`, {
      headers: { Authorization: `Basic ${Buffer.from(`any:${MON_TOKEN}`).toString("base64")}` }
    });
    expect(res.status).toBe(200);
  });

  it("status response contains synced tables", async () => {
    const res = await fetch(`${MON_BASE}/api/status`, {
      headers: { Authorization: `Basic ${Buffer.from(`any:${MON_TOKEN}`).toString("base64")}` }
    });
    const body = (await res.json()) as { tables?: unknown[] };
    expect(body.tables).toBeDefined();
  });
});
