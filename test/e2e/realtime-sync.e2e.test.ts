/**
 * E2E tests: Realtime Sync (binlog CDC)
 *
 * Verifies that INSERT, UPDATE, and DELETE operations in MySQL are reflected
 * in Typesense via the binlog listener, typically within a few seconds.
 *
 * Each test:
 *   1. Performs a MySQL write
 *   2. Polls Typesense until the change appears (or times out at 12s)
 *   3. Cleans up the row in afterEach to keep the DB in a known state
 *
 * Skip with: SKIP_E2E=1 npm test
 */

import { afterAll, afterEach, beforeEach, describe, expect, it } from "vitest";
import { closeMysqlPool, mysqlQuery, mysqlWrite, poll, SKIP, TS_BASE, TS_HEADERS, tsGet, tsSearch } from "./helpers.js";

// ---------------------------------------------------------------------------
// Realtime sync — INSERT
// ---------------------------------------------------------------------------
describe.skipIf(SKIP)("realtime sync — INSERT", () => {
  // Track inserted IDs for cleanup
  const insertedUserIds: number[] = [];
  const insertedProductIds: number[] = [];

  afterEach(async () => {
    if (insertedUserIds.length > 0) {
      const ids = insertedUserIds.splice(0);
      await mysqlQuery(`DELETE FROM users WHERE id IN (${ids.map(() => "?").join(",")})`, ids);
    }
    if (insertedProductIds.length > 0) {
      const ids = insertedProductIds.splice(0);
      await mysqlQuery(`DELETE FROM products WHERE id IN (${ids.map(() => "?").join(",")})`, ids);
    }
  });

  afterAll(async () => {
    await closeMysqlPool();
  });

  it(
    "new user appears in Typesense after INSERT",
    { timeout: 20_000 },
    async () => {
      const email = `e2e-insert-${Date.now()}@test.invalid`;

      const result = await mysqlWrite(
        `INSERT INTO users (email, full_name, status, is_active, created_at, updated_at)
         VALUES (?, 'E2E Insert User', 'active', 1, NOW(), NOW())`,
        [email]
      );
      insertedUserIds.push(result.insertId);

      const found = await poll(
        async () => {
          const data = await tsSearch("users", "email", email);
          return data.found > 0 ? data : null;
        },
        { timeout: 12_000, label: `user ${email} to appear in Typesense` }
      );

      expect(found.found).toBe(1);
      expect(found.hits[0].document.email).toBe(email);
    }
  );

  it(
    "inserted user id stored as string in Typesense",
    { timeout: 20_000 },
    async () => {
      const email = `e2e-id-${Date.now()}@test.invalid`;

      const result = await mysqlWrite(
        `INSERT INTO users (email, full_name, status, is_active, created_at, updated_at)
         VALUES (?, 'E2E ID User', 'active', 1, NOW(), NOW())`,
        [email]
      );
      insertedUserIds.push(result.insertId);

      const found = await poll(
        async () => {
          const data = await tsSearch<{ id: string }>("users", "email", email);
          return data.found > 0 ? data : null;
        },
        { timeout: 12_000, label: "inserted user with string id" }
      );

      const doc = found.hits[0].document;
      expect(typeof doc.id).toBe("string");
      expect(doc.id).toMatch(/^\d+$/);
    }
  );

  it(
    "new product appears in Typesense after INSERT",
    { timeout: 20_000 },
    async () => {
      const sku = `SKU-E2E-${Date.now()}`;

      const result = await mysqlWrite(
        `INSERT INTO products (sku, name, price, is_published, created_at, updated_at)
         VALUES (?, 'E2E Test Product', 42.99, 1, NOW(), NOW())`,
        [sku]
      );
      insertedProductIds.push(result.insertId);

      const found = await poll(
        async () => {
          const data = await tsSearch("products", "sku", sku);
          return data.found > 0 ? data : null;
        },
        { timeout: 12_000, label: `product ${sku} to appear in Typesense` }
      );

      expect(found.found).toBe(1);
      const doc = found.hits[0].document as { sku: string; price: number; is_published: boolean };
      expect(doc.sku).toBe(sku);
      expect(doc.price).toBeCloseTo(42.99, 2);
      expect(doc.is_published).toBe(true);
    }
  );

  it(
    "inserted product timestamp (created_at) stored as integer seconds",
    { timeout: 20_000 },
    async () => {
      const sku = `SKU-TS-${Date.now()}`;
      const before = Math.floor(Date.now() / 1000);

      const result = await mysqlWrite(
        `INSERT INTO products (sku, name, price, is_published, created_at, updated_at)
         VALUES (?, 'E2E TS Product', 9.99, 1, NOW(), NOW())`,
        [sku]
      );
      insertedProductIds.push(result.insertId);

      const found = await poll(
        async () => {
          const data = await tsSearch<{ created_at: number }>("products", "sku", sku);
          return data.found > 0 ? data : null;
        },
        { timeout: 12_000, label: "timestamp product to appear" }
      );

      const ts = found.hits[0].document.created_at;
      const after = Math.floor(Date.now() / 1000) + 5;
      expect(typeof ts).toBe("number");
      expect(ts).toBeGreaterThanOrEqual(before);
      expect(ts).toBeLessThanOrEqual(after);
    }
  );
});

// ---------------------------------------------------------------------------
// Realtime sync — UPDATE
// ---------------------------------------------------------------------------
describe.skipIf(SKIP)("realtime sync — UPDATE", () => {
  let testUserId: number | null = null;
  const testEmail = `e2e-update-${Date.now()}@test.invalid`;

  beforeEach(async () => {
    // Insert a user to update
    const result = await mysqlWrite(
      `INSERT INTO users (email, full_name, status, is_active, created_at, updated_at)
       VALUES (?, 'E2E Update User', 'active', 1, NOW(), NOW())`,
      [testEmail]
    );
    testUserId = result.insertId;

    // Wait for the insert to appear in Typesense
    await poll(
      async () => {
        const data = await tsSearch("users", "email", testEmail);
        return data.found > 0 ? true : null;
      },
      { timeout: 12_000, label: "test user to appear before update test" }
    );
  });

  afterEach(async () => {
    if (testUserId !== null) {
      await mysqlQuery("DELETE FROM users WHERE id = ?", [testUserId]);
      testUserId = null;
    }
  });

  it(
    "updated user status reflected in Typesense",
    { timeout: 20_000 },
    async () => {
      await mysqlQuery("UPDATE users SET status = 'suspended', updated_at = NOW() WHERE id = ?", [testUserId]);

      const updated = await poll(
        async () => {
          const data = await tsSearch<{ status: string }>("users", "email", testEmail);
          if (data.found > 0 && data.hits[0].document.status === "suspended") {
            return data;
          }
          return null;
        },
        { timeout: 12_000, label: "user status update to appear in Typesense" }
      );

      expect(updated.hits[0].document.status).toBe("suspended");
    }
  );

  it(
    "updated user is_active reflected in Typesense",
    { timeout: 20_000 },
    async () => {
      await mysqlQuery("UPDATE users SET is_active = 0, updated_at = NOW() WHERE id = ?", [testUserId]);

      const updated = await poll(
        async () => {
          const data = await tsSearch<{ is_active: boolean }>("users", "email", testEmail);
          if (data.found > 0 && data.hits[0].document.is_active === false) {
            return data;
          }
          return null;
        },
        { timeout: 12_000, label: "user is_active=false to appear in Typesense" }
      );

      expect(updated.hits[0].document.is_active).toBe(false);
    }
  );

  it(
    "updated_at timestamp changes after UPDATE",
    { timeout: 25_000 },
    async () => {
      // Use a dedicated fresh email to avoid race with afterEach/beforeEach delete-reinsert
      const freshEmail = `e2e-ts-upd-${Date.now()}@test.invalid`;
      const ins = await mysqlWrite(
        `INSERT INTO users (email, full_name, status, is_active, created_at, updated_at)
         VALUES (?, 'E2E TS Upd', 'active', 1, NOW(), NOW())`,
        [freshEmail]
      );
      const freshId = ins.insertId;
      try {
        await poll(
          async () => { const d = await tsSearch<{ updated_at: number }>("users", "email", freshEmail); return d.found > 0 ? d : null; },
          { timeout: 12_000, label: "fresh user before ts test" }
        );

        const beforeData = await tsSearch<{ updated_at: number }>("users", "email", freshEmail);
        const beforeTs = beforeData.hits[0].document.updated_at;

        await new Promise((r) => setTimeout(r, 1100));
        await mysqlQuery("UPDATE users SET full_name = 'E2E TS Updated', updated_at = NOW() WHERE id = ?", [freshId]);

        const updated = await poll(
          async () => {
            const data = await tsSearch<{ updated_at: number; full_name: string }>("users", "email", freshEmail);
            if (data.found > 0 && data.hits[0].document.full_name === "E2E TS Updated") return data;
            return null;
          },
          { timeout: 12_000, label: "name update in Typesense" }
        );

        expect(typeof updated.hits[0].document.updated_at).toBe("number");
        expect(updated.hits[0].document.updated_at).toBeGreaterThanOrEqual(beforeTs);
      } finally {
        await mysqlQuery("DELETE FROM users WHERE id = ?", [freshId]);
      }
    }
  );

  it(
    "primary key change removes old document id and creates new id",
    { timeout: 30_000 },
    async () => {
      const oldId = testUserId as number;
      let newId = oldId + 100_000;

      // Avoid PK collision if this range is already occupied.
      for (let i = 0; i < 20; i += 1) {
        const existing = await mysqlQuery<{ c: number }>("SELECT COUNT(*) AS c FROM users WHERE id = ?", [newId]);
        if ((existing[0]?.c ?? 0) === 0) break;
        newId += 1;
      }

      await poll(
        async () => {
          const res = await tsGet(`/collections/users/documents/${encodeURIComponent(String(oldId))}`);
          return res.status === 200 ? true : null;
        },
        { timeout: 12_000, label: `Typesense doc ${oldId} before PK update` }
      );

      await mysqlQuery("UPDATE users SET id = ?, updated_at = NOW() WHERE id = ?", [newId, oldId]);
      testUserId = newId;

      await poll(
        async () => {
          const oldRes = await tsGet(`/collections/users/documents/${encodeURIComponent(String(oldId))}`);
          const newRes = await tsGet(`/collections/users/documents/${encodeURIComponent(String(newId))}`);
          return oldRes.status === 404 && newRes.status === 200 ? true : null;
        },
        { timeout: 12_000, label: "old id removed and new id created in Typesense" }
      );

      const oldFinal = await tsGet(`/collections/users/documents/${encodeURIComponent(String(oldId))}`);
      const newFinal = await tsGet(`/collections/users/documents/${encodeURIComponent(String(newId))}`);
      expect(oldFinal.status).toBe(404);
      expect(newFinal.status).toBe(200);
    }
  );
});

// ---------------------------------------------------------------------------
// Realtime sync — DELETE
// ---------------------------------------------------------------------------
describe.skipIf(SKIP)("realtime sync — DELETE", () => {
  afterAll(async () => {
    await closeMysqlPool();
  });

  it(
    "deleted user removed from Typesense",
    { timeout: 30_000 },
    async () => {
      const email = `e2e-delete-${Date.now()}@test.invalid`;

      // Insert
      const result = await mysqlWrite(
        `INSERT INTO users (email, full_name, status, is_active, created_at, updated_at)
         VALUES (?, 'E2E Delete User', 'active', 1, NOW(), NOW())`,
        [email]
      );
      const userId = result.insertId;

      // Wait for it to appear
      await poll(
        async () => {
          const data = await tsSearch("users", "email", email);
          return data.found > 0 ? true : null;
        },
        { timeout: 12_000, label: "inserted user before delete test" }
      );

      // Delete from MySQL
      await mysqlQuery("DELETE FROM users WHERE id = ?", [userId]);

      // Poll until removed from Typesense
      await poll(
        async () => {
          const data = await tsSearch("users", "email", email);
          return data.found === 0 ? true : null;
        },
        { timeout: 12_000, label: "deleted user to be removed from Typesense" }
      );

      // Final assertion
      const data = await tsSearch("users", "email", email);
      expect(data.found).toBe(0);
    }
  );

  it(
    "deleted product removed from Typesense",
    { timeout: 30_000 },
    async () => {
      const sku = `SKU-DEL-${Date.now()}`;

      const result = await mysqlWrite(
        `INSERT INTO products (sku, name, price, is_published, created_at, updated_at)
         VALUES (?, 'E2E Delete Product', 9.99, 1, NOW(), NOW())`,
        [sku]
      );
      const productId = result.insertId;

      await poll(
        async () => {
          const data = await tsSearch("products", "sku", sku);
          return data.found > 0 ? true : null;
        },
        { timeout: 12_000, label: "inserted product before delete test" }
      );

      await mysqlQuery("DELETE FROM products WHERE id = ?", [productId]);

      await poll(
        async () => {
          const data = await tsSearch("products", "sku", sku);
          return data.found === 0 ? true : null;
        },
        { timeout: 12_000, label: "deleted product to be removed from Typesense" }
      );

      const data = await tsSearch("products", "sku", sku);
      expect(data.found).toBe(0);
    }
  );

  it(
    "delete event is idempotent when document was already removed in Typesense",
    { timeout: 35_000 },
    async () => {
      const email = `e2e-idempotent-del-${Date.now()}@test.invalid`;
      const followUpEmail = `e2e-idempotent-del-followup-${Date.now()}@test.invalid`;

      const inserted = await mysqlWrite(
        `INSERT INTO users (email, full_name, status, is_active, created_at, updated_at)
         VALUES (?, 'E2E Idempotent Delete User', 'active', 1, NOW(), NOW())`,
        [email]
      );
      const userId = inserted.insertId;

      let followUpUserId: number | null = null;

      try {
        // Wait for initial sync into Typesense.
        await poll(
          async () => {
            const data = await tsSearch<{ id: string }>("users", "email", email);
            return data.found > 0 ? data : null;
          },
          { timeout: 12_000, label: "idempotent delete test user to appear in Typesense" }
        );

        // Manually remove the document first to simulate already-deleted state.
        const preDeleteRes = await fetch(
          `${TS_BASE}/collections/users/documents/${encodeURIComponent(String(userId))}`,
          { method: "DELETE", headers: TS_HEADERS }
        );
        expect([200, 404]).toContain(preDeleteRes.status);

        // Now delete in MySQL; realtime delete should not fail when Typesense returns 404.
        await mysqlQuery("DELETE FROM users WHERE id = ?", [userId]);

        // Confirm it stays absent.
        await poll(
          async () => {
            const data = await tsSearch("users", "email", email);
            return data.found === 0 ? true : null;
          },
          { timeout: 12_000, label: "already-deleted user remains absent after MySQL delete" }
        );

        // Companion assertion: realtime pipeline still processes subsequent events.
        const followUp = await mysqlWrite(
          `INSERT INTO users (email, full_name, status, is_active, created_at, updated_at)
           VALUES (?, 'E2E Idempotent Delete Follow-up', 'active', 1, NOW(), NOW())`,
          [followUpEmail]
        );
        followUpUserId = followUp.insertId;

        await poll(
          async () => {
            const data = await tsSearch("users", "email", followUpEmail);
            return data.found > 0 ? true : null;
          },
          { timeout: 12_000, label: "follow-up insert appears after idempotent delete event" }
        );
      } finally {
        await mysqlQuery("DELETE FROM users WHERE id = ?", [userId]);
        if (followUpUserId !== null) {
          await mysqlQuery("DELETE FROM users WHERE id = ?", [followUpUserId]);
        }
      }
    }
  );
});

// ---------------------------------------------------------------------------
// Realtime sync — field type coercions
// ---------------------------------------------------------------------------
describe.skipIf(SKIP)("realtime sync — field type coercions", () => {
  const insertedIds: number[] = [];

  afterAll(async () => {
    if (insertedIds.length > 0) {
      const ids = insertedIds.splice(0);
      await mysqlQuery(`DELETE FROM users WHERE id IN (${ids.map(() => "?").join(",")})`, ids);
    }
    await closeMysqlPool();
  });

  it(
    "is_active=0 synced as bool false (not integer) via binlog",
    { timeout: 20_000 },
    async () => {
      const email = `e2e-bool-f-${Date.now()}@test.invalid`;

      const result = await mysqlWrite(
        `INSERT INTO users (email, full_name, status, is_active, created_at, updated_at)
         VALUES (?, 'E2E Bool False', 'inactive', 0, NOW(), NOW())`,
        [email]
      );
      insertedIds.push(result.insertId);

      const found = await poll(
        async () => {
          const data = await tsSearch<{ is_active: boolean }>("users", "email", email);
          return data.found > 0 ? data : null;
        },
        { timeout: 12_000, label: "bool=false user to appear" }
      );

      expect(found.hits[0].document.is_active).toBe(false);
    }
  );

  it(
    "is_active=1 synced as bool true via binlog",
    { timeout: 20_000 },
    async () => {
      const email = `e2e-bool-t-${Date.now()}@test.invalid`;

      const result = await mysqlWrite(
        `INSERT INTO users (email, full_name, status, is_active, created_at, updated_at)
         VALUES (?, 'E2E Bool True', 'active', 1, NOW(), NOW())`,
        [email]
      );
      insertedIds.push(result.insertId);

      const found = await poll(
        async () => {
          const data = await tsSearch<{ is_active: boolean }>("users", "email", email);
          return data.found > 0 ? data : null;
        },
        { timeout: 12_000, label: "bool=true user to appear" }
      );

      expect(found.hits[0].document.is_active).toBe(true);
    }
  );
});
