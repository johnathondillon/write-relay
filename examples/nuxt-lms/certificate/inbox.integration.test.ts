import assert from "node:assert/strict";
import { createHash, randomUUID } from "node:crypto";
import { after, before, test } from "node:test";
import { setTimeout as delay } from "node:timers/promises";
import pg from "pg";
import {
  withInbox,
  inboxTableSQL,
  InboxConflictError,
  InboxTransactionError,
} from "@writerelay/node";

// Disposable tables in the example receiver database; never alter its real inbox.
const suffix = randomUUID().replaceAll("-", "");
const table = `inbox_test_${suffix}`;
const effects = `effects_test_${suffix}`;
const pool = new pg.Pool({
  connectionString: process.env.CERTIFICATE_DATABASE_URL,
  max: 5,
  statement_timeout: 8000,
  connectionTimeoutMillis: 3000,
});
pool.on("error", () => {});
const receipt = () => ({
  table,
  key: createHash("sha256").update(randomUUID()).digest("hex"),
  body: Buffer.from('{"ok":true}'),
});
async function counts(key: string) {
  const inbox = await pool.query(
    `SELECT count(*)::int AS n FROM "${table}" WHERE idempotency_key=$1`,
    [key],
  );
  const business = await pool.query(
    `SELECT count(*)::int AS n FROM "${effects}" WHERE id=$1`,
    [key],
  );
  return [inbox.rows[0].n, business.rows[0].n];
}
before(async () => {
  assert.ok(
    process.env.CERTIFICATE_DATABASE_URL,
    "run inside the certificate container",
  );
  const version = await pool.query(
    "SELECT current_setting('server_version') AS version, current_setting('server_version_num')::int AS number",
  );
  console.log(`TypeScript receiver inbox: PostgreSQL ${version.rows[0].version}`);
  const expected = process.env.WRITERELAY_INBOX_POSTGRES_MAJOR;
  if (expected) {
    assert.equal(Math.floor(version.rows[0].number / 10000), Number(expected));
  }
  await pool.query(inboxTableSQL({ table }));
  await pool.query(`CREATE TABLE "${effects}" (id text PRIMARY KEY)`);
});
after(async () => {
  try {
    await pool.query(`DROP TABLE IF EXISTS "${effects}"`);
    await pool.query(`DROP TABLE IF EXISTS "${table}"`);
  } finally {
    await pool.end();
  }
});

test("commits one effect, skips duplicate, and rejects conflicting bytes", async () => {
  const message = receipt();
  const result = await withInbox(pool, message, async (tx) => {
    const isolation = await tx.query("SHOW transaction_isolation");
    assert.equal(isolation.rows[0].transaction_isolation, "read committed");
    await tx.query(`INSERT INTO "${effects}" VALUES($1)`, [message.key]);
    return 42;
  });
  assert.deepEqual(result, { status: "processed", value: 42 });
  assert.deepEqual(await counts(message.key), [1, 1]);
  assert.deepEqual(
    await withInbox(pool, message, async () => assert.fail("duplicate")),
    { status: "duplicate" },
  );
  await assert.rejects(
    withInbox(
      pool,
      { ...message, body: Buffer.from('{"ok":false}') },
      async () => assert.fail("conflict"),
    ),
    InboxConflictError,
  );
  assert.deepEqual(await counts(message.key), [1, 1]);
});

test("callback failure rolls back both records so the same delivery can retry", async () => {
  const message = receipt();
  const failure = new Error("business write failed");
  await assert.rejects(
    withInbox(pool, message, async (tx) => {
      await tx.query(`INSERT INTO "${effects}" VALUES($1)`, [message.key]);
      throw failure;
    }),
    (e) => e === failure,
  );
  assert.deepEqual(await counts(message.key), [0, 0]);
  await withInbox(pool, message, async (tx) => {
    await tx.query(`INSERT INTO "${effects}" VALUES($1)`, [message.key]);
  });
  assert.deepEqual(await counts(message.key), [1, 1]);
});

test("a swallowed SQL error cannot turn PostgreSQL COMMIT/ROLLBACK into success", async () => {
  const message = receipt();
  await assert.rejects(
    withInbox(pool, message, async (tx) => {
      await tx.query(`INSERT INTO "${effects}" VALUES($1)`, [message.key]);
      try {
        await tx.query("SELECT 1/0");
      } catch {
        /* Simulate a buggy callback. */
      }
    }),
    InboxTransactionError,
  );
  assert.deepEqual(await counts(message.key), [0, 0]);
});

for (const rollbackFirst of [false, true]) {
  test(
    `concurrent duplicate waits for first transaction to ${rollbackFirst ? "roll back" : "commit"}`,
    { timeout: 15000 },
    async () => {
      const message = receipt();
      let entered!: () => void;
      let finish!: () => void;
      const entry = new Promise<void>((resolve) => {
        entered = resolve;
      });
      const gate = new Promise<void>((resolve) => {
        finish = resolve;
      });
      const failure = new Error("first transaction rolled back");
      let secondCalls = 0;
      const first = withInbox(pool, message, async (tx) => {
        await tx.query(`INSERT INTO "${effects}" VALUES($1)`, [message.key]);
        entered();
        await gate;
        if (rollbackFirst) throw failure;
      }).then(
        (value) => ({ value, error: undefined }),
        (error) => ({ value: undefined, error }),
      );
      let second: ReturnType<typeof withInbox> | undefined;
      try {
        await Promise.race([
          entry,
          first.then((result) => {
            if (result.error) throw result.error;
          }),
        ]);
        second = withInbox(pool, message, async (tx) => {
          secondCalls++;
          await tx.query(`INSERT INTO "${effects}" VALUES($1)`, [message.key]);
        });
        void second.catch(() => {}); // Observe early failures while checking its lock wait.
        // Observe an actual PostgreSQL lock wait, rather than relying on a sleep
        // to assume the competing INSERT has reached the database.
        const deadline = Date.now() + 5000;
        while (true) {
          const blocked = await pool.query(
            "SELECT count(*)::int AS n FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE $1",
            [`INSERT INTO "public"."${table}"%`],
          );
          if (blocked.rows[0].n > 0) break;
          assert.ok(
            Date.now() < deadline,
            "second delivery did not reach its lock wait",
          );
          await delay(25);
        }
        assert.equal(secondCalls, 0);
        finish();
        const firstResult = await first;
        assert.equal(firstResult.error, rollbackFirst ? failure : undefined);
        assert.equal(
          (await second).status,
          rollbackFirst ? "processed" : "duplicate",
        );
        assert.equal(secondCalls, rollbackFirst ? 1 : 0);
        assert.deepEqual(await counts(message.key), [1, 1]);
      } finally {
        finish();
        await first;
        await second?.catch(() => {});
      }
    },
  );
}
