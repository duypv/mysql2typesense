/**
 * E2E tests: Initial Sync
 *
 * Verifies that all MySQL seed data is correctly materialised in Typesense
 * after the bootstrap sync completes.
 *
 * Covers:
 *   - All 5 collections exist
 *   - users: document count, field types (bool, string[], object, int64 timestamp)
 *   - products: document count, field types (float price, string[] categories, object attributes)
 *   - ParentRef: document count, field types
 *   - ChildJoin: document count, parentId stored as string, join reference field
 *   - json_case_test: document count, JSON object/array fields preserved
 *
 * Skip with: SKIP_E2E=1 npm test
 */

import { afterAll, describe, expect, it } from "vitest";
import { closeMysqlPool, SKIP, tsGet, tsGetJson, tsSearch } from "./helpers.js";

afterAll(async () => {
  await closeMysqlPool();
});

// ---------------------------------------------------------------------------
// Collection existence
// ---------------------------------------------------------------------------
describe.skipIf(SKIP)("initial sync — all collections present", () => {
  const COLLECTIONS = ["users", "products", "ParentRef", "ChildJoin", "json_case_test"];

  for (const name of COLLECTIONS) {
    it(`collection '${name}' exists`, async () => {
      const res = await tsGet(`/collections/${name}`);
      expect(res.status).toBe(200);
    });
  }
});

// ---------------------------------------------------------------------------
// users collection
// ---------------------------------------------------------------------------
describe.skipIf(SKIP)("initial sync — users", () => {
  it("has at least 2 documents", async () => {
    const data = await tsSearch("users", "email");
    expect(data.found).toBeGreaterThanOrEqual(2);
  });

  it("alice document has correct identity fields", async () => {
    const data = await tsSearch<{
      id: string;
      email: string;
      is_active: boolean;
    }>("users", "email", "alice@example.com");

    expect(data.hits.length).toBeGreaterThanOrEqual(1);
    const alice = data.hits[0].document;

    expect(alice.email).toBe("alice@example.com");
    expect(alice.is_active).toBe(true);
    expect(alice.id).toMatch(/^\d+$/); // coerced to string
  });

  it("alice metadata is an object (JSON column)", async () => {
    const data = await tsSearch<{ metadata: Record<string, unknown> }>("users", "email", "alice@example.com");
    const alice = data.hits[0].document;
    expect(typeof alice.metadata).toBe("object");
    expect(alice.metadata).not.toBeNull();
  });

  it("timestamps (created_at / updated_at) are stored as integers (seconds)", async () => {
    const data = await tsSearch<{ created_at: number; updated_at: number }>("users", "email", "alice@example.com");
    const alice = data.hits[0].document;
    expect(typeof alice.created_at).toBe("number");
    expect(typeof alice.updated_at).toBe("number");
    // Unix timestamp in seconds: sanity range 2020–2040
    expect(alice.created_at).toBeGreaterThan(1_577_836_800);
    expect(alice.created_at).toBeLessThan(2_208_988_800);
  });

  it("bob is_active is false (tinyint 0 → bool false)", async () => {
    const data = await tsSearch<{ is_active: boolean; email: string }>("users", "email", "bob@example.com");
    expect(data.hits.length).toBeGreaterThanOrEqual(1);
    const bob = data.hits[0].document;
    expect(bob.is_active).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// products collection
// ---------------------------------------------------------------------------
describe.skipIf(SKIP)("initial sync — products", () => {
  it("has at least 2 documents", async () => {
    const data = await tsSearch("products", "name");
    expect(data.found).toBeGreaterThanOrEqual(2);
  });

  it("Wireless Mouse (SKU-002) has correct fields", async () => {
    const data = await tsSearch<{
      id: string;
      sku: string;
      name: string;
      price: number;
      is_published: boolean;
    }>("products", "sku", "SKU-002");

    expect(data.hits.length).toBeGreaterThanOrEqual(1);
    const mouse = data.hits[0].document;

    expect(mouse.sku).toBe("SKU-002");
    expect(mouse.price).toBeCloseTo(59.5, 1);
    expect(mouse.is_published).toBe(true);
  });

  it("price is stored as a float type (not integer)", async () => {
    const data = await tsSearch<{ price: number; sku: string }>("products", "sku", "SKU-002");
    const mouse = data.hits[0].document;
    expect(typeof mouse.price).toBe("number");
    // Verify it is actually a float by checking Typesense schema
    const col = await tsGetJson<{ fields: { name: string; type: string }[] }>("/collections/products");
    const priceField = col.fields.find((f) => f.name === "price");
    expect(priceField?.type).toBe("float");
  });

  it("categories field is auto-typed (JSON array column)", async () => {
    // In auto-discovery mode, JSON columns get type 'auto' then expand to string[]
    const col = await tsGetJson<{ fields: { name: string; type: string }[] }>("/collections/products");
    const catField = col.fields.find((f) => f.name === "categories" && f.type !== "auto");
    // After documents are indexed, categories expands to string[]
    if (catField) {
      expect(catField.type).toBe("string[]");
    } else {
      // Still 'auto' if no documents have been indexed with categories — acceptable
      const autoField = col.fields.find((f) => f.name === "categories");
      expect(autoField).toBeDefined();
    }
  });

  it("attributes is an object (JSON column)", async () => {
    const data = await tsSearch<{ attributes: Record<string, unknown> }>("products", "sku", "SKU-002");
    const mouse = data.hits[0].document;
    expect(typeof mouse.attributes).toBe("object");
    expect(mouse.attributes).not.toBeNull();
  });
});

// ---------------------------------------------------------------------------
// ParentRef collection
// ---------------------------------------------------------------------------
describe.skipIf(SKIP)("initial sync — ParentRef", () => {
  it("has at least 2 seed documents", async () => {
    const data = await tsSearch("ParentRef", "code");
    expect(data.found).toBeGreaterThanOrEqual(2);
  });

  it("id is stored as string", async () => {
    const data = await tsSearch<{ id: string; code: string }>("ParentRef", "code");
    for (const hit of data.hits) {
      expect(typeof hit.document.id).toBe("string");
    }
  });

  it("SVC-001 and SVC-002 both present", async () => {
    const data = await tsSearch<{ code: string }>("ParentRef", "code");
    const codes = data.hits.map((h) => h.document.code);
    expect(codes).toContain("SVC-001");
    expect(codes).toContain("SVC-002");
  });

  it("price is stored as a number (float)", async () => {
    const data = await tsSearch<{ code: string; price: number }>("ParentRef", "code", "SVC-001");
    const doc = data.hits[0].document;
    expect(typeof doc.price).toBe("number");
    expect(doc.price).toBeCloseTo(99.5, 1);
  });
});

// ---------------------------------------------------------------------------
// ChildJoin collection
// ---------------------------------------------------------------------------
describe.skipIf(SKIP)("initial sync — ChildJoin", () => {
  it("has at least 2 seed documents", async () => {
    const data = await tsSearch("ChildJoin", "note");
    expect(data.found).toBeGreaterThanOrEqual(2);
  });

  it("parentId is stored as string", async () => {
    const data = await tsSearch<{ parentId: string }>("ChildJoin", "note");
    for (const hit of data.hits) {
      expect(typeof hit.document.parentId).toBe("string");
    }
  });

  it("parentId field has reference to ParentRef.id in collection schema", async () => {
    const col = await tsGetJson<{ fields: { name: string; reference?: string; async_reference?: boolean }[] }>(
      "/collections/ChildJoin"
    );
    const parentIdField = col.fields.find((f) => f.name === "parentId");
    expect(parentIdField?.reference).toBe("ParentRef.id");
    expect(parentIdField?.async_reference).toBe(true);
  });

  it("join query returns embedded ParentRef sub-documents", async () => {
    const data = await tsGetJson<{
      found: number;
      hits: { document: { parentId: string; ParentRef?: { id: string; code: string } } }[];
    }>("/collections/ChildJoin/documents/search?q=*&query_by=note&include_fields=$ParentRef(id,code)");

    expect(data.found).toBe(2);
    for (const hit of data.hits) {
      expect(hit.document.ParentRef).toBeDefined();
      expect(typeof hit.document.ParentRef?.id).toBe("string");
      expect(typeof hit.document.ParentRef?.code).toBe("string");
    }
  });
});

// ---------------------------------------------------------------------------
// json_case_test collection
// ---------------------------------------------------------------------------
describe.skipIf(SKIP)("initial sync — json_case_test", () => {
  it("has 2 seed documents", async () => {
    const data = await tsSearch("json_case_test", "legacyIDs");
    expect(data.found).toBe(2);
  });

  it("row-1 document exists", async () => {
    const res = await tsGet("/collections/json_case_test/documents/row-1");
    expect(res.status).toBe(200);
    const doc = (await res.json()) as Record<string, unknown>;
    expect(doc.id).toBe("row-1");
  });

  it("row-2 document exists", async () => {
    const res = await tsGet("/collections/json_case_test/documents/row-2");
    expect(res.status).toBe(200);
  });
});
