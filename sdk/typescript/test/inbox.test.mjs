import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import test from "node:test";
import {
  withInbox,
  inboxTableSQL,
  InboxConflictError,
  InboxTransactionError,
} from "@writerelay/node";

const key = "a".repeat(64);
const body = Buffer.from('{"orderId":"one"}');
const digest = createHash("sha256").update(body).digest("hex");

function fixture(options = {}) {
  const calls = [];
  const client = {
    calls,
    async query(sql, values) {
      this.calls.push([sql, values]);
      if (options.fail?.(sql)) throw options.error;
      if (sql === "ROLLBACK" && options.rollbackError)
        throw options.rollbackError;
      if (sql.startsWith("INSERT INTO"))
        return {
          command: "INSERT",
          rowCount: options.duplicate ? 0 : 1,
          rows: [],
        };
      if (sql.startsWith("SELECT payload_sha256"))
        return {
          command: "SELECT",
          rowCount: 1,
          rows: options.missing
            ? []
            : [{ payload_sha256: options.digest ?? digest }],
        };
      if (sql === "COMMIT" && options.commit) return options.commit();
      return {
        command: sql === "COMMIT" ? "COMMIT" : "OK",
        rowCount: null,
        rows: [],
      };
    },
    release(discard) {
      calls.push(["release", discard]);
    },
  };
  return {
    calls,
    pool: {
      async connect() {
        calls.push(["connect"]);
        return client;
      },
    },
  };
}

test("inbox snapshots raw bytes, parameterizes values, and commits callback writes before success", async () => {
  const { pool, calls } = fixture();
  const raw = Buffer.from(body);
  let finishCommit, enteredCommit;
  const commitEntered = new Promise((resolve) => {
    enteredCommit = resolve;
  });
  const commitFinished = new Promise((resolve) => {
    finishCommit = resolve;
  });
  const held = fixture({
    commit: async () => {
      enteredCommit();
      await commitFinished;
      return { command: "COMMIT" };
    },
  });
  let settled = false;
  const pending = withInbox(
    held.pool,
    { key, body: raw, schema: "Receiver", table: "Inbox" },
    async (tx) => {
      assert.equal(tx.release, undefined);
      await tx.query("UPDATE business SET complete=$1", [true]);
      return { orderId: "one" };
    },
  ).then((value) => {
    settled = true;
    return value;
  });
  raw.fill(0);
  await commitEntered;
  assert.equal(settled, false);
  assert.equal(
    held.calls.some(([sql]) => sql === "release"),
    false,
  );
  finishCommit();
  assert.deepEqual(await pending, {
    status: "processed",
    value: { orderId: "one" },
  });
  assert.deepEqual(held.calls[2][1], [key, digest]);
  assert.match(held.calls[2][0], /^INSERT INTO "Receiver"\."Inbox"/);
  assert.deepEqual(
    held.calls.map(([sql]) => sql),
    [
      "connect",
      "BEGIN ISOLATION LEVEL READ COMMITTED",
      held.calls[2][0],
      "UPDATE business SET complete=$1",
      "COMMIT",
      "release",
    ],
  );
  assert.deepEqual(held.calls.at(-1), ["release", false]);
  await withInbox(pool, { key, body }, async () => undefined);
  assert.equal(calls[2][1][1], digest);
});

test("matching duplicate skips callback; changed content rolls back and conflicts", async () => {
  const duplicate = fixture({ duplicate: true });
  assert.deepEqual(
    await withInbox(duplicate.pool, { key, body }, async () =>
      assert.fail("duplicate callback"),
    ),
    { status: "duplicate" },
  );
  assert.deepEqual(duplicate.calls[3][1], [key]);
  assert.match(duplicate.calls[3][0], /FOR SHARE$/);
  assert.deepEqual(duplicate.calls.at(-2), ["COMMIT", undefined]);

  const conflict = fixture({ duplicate: true, digest: "b".repeat(64) });
  await assert.rejects(
    withInbox(conflict.pool, { key, body }, async () =>
      assert.fail("conflict callback"),
    ),
    InboxConflictError,
  );
  assert.equal(
    conflict.calls.some(([sql]) => sql === "COMMIT"),
    false,
  );
  assert.deepEqual(conflict.calls.at(-2), ["ROLLBACK", undefined]);
});

test("callback and database failures roll back, release, and preserve the original error", async () => {
  for (const stage of ["BEGIN", "INSERT", "SELECT", "callback", "COMMIT"]) {
    const error = new Error("original failure");
    const f = fixture({
      duplicate: stage === "SELECT",
      fail: (sql) => sql.startsWith(stage),
      error,
    });
    let handles = 0;
    await assert.rejects(
      withInbox(f.pool, { key, body }, async () => {
        handles++;
        if (stage === "callback") throw error;
      }),
      (e) => e === error,
    );
    assert.deepEqual(f.calls.at(-2), ["ROLLBACK", undefined]);
    assert.deepEqual(f.calls.at(-1), ["release", stage === "COMMIT"]);
    assert.equal(handles, stage === "callback" || stage === "COMMIT" ? 1 : 0);
  }
  const original = new Error("business error");
  const failedRollback = fixture({
    rollbackError: new Error("connection lost"),
  });
  await assert.rejects(
    withInbox(failedRollback.pool, { key, body }, async () => {
      throw original;
    }),
    (e) => e === original,
  );
  assert.deepEqual(failedRollback.calls.at(-1), ["release", true]);
  await assert.rejects(
    withInbox(
      {
        async connect() {
          throw original;
        },
      },
      { key, body },
      async () => {},
    ),
    (e) => e === original,
  );
});

test("missing receipts and a rolled-back COMMIT cannot report success", async () => {
  const missing = fixture({ duplicate: true, missing: true });
  await assert.rejects(
    withInbox(missing.pool, { key, body }, async () =>
      assert.fail("missing callback"),
    ),
    InboxTransactionError,
  );
  const aborted = fixture({ commit: async () => ({ command: "ROLLBACK" }) });
  await assert.rejects(
    withInbox(aborted.pool, { key, body }, async () => {}),
    InboxTransactionError,
  );
  assert.deepEqual(aborted.calls.at(-1), ["release", true]);
});

test("retained callback transactions cannot query a released pooled connection", async () => {
  const f = fixture();
  let retained;
  await withInbox(f.pool, { key, body }, async (tx) => {
    retained = tx;
  });
  const calls = f.calls.length;
  await assert.rejects(
    retained.query("UPDATE business SET complete=false"),
    InboxTransactionError,
  );
  assert.equal(f.calls.length, calls);
});

test("invalid keys, raw bodies, handlers, and identifiers are rejected before checkout", async () => {
  const pool = {
    async connect() {
      assert.fail("invalid input checked out a connection");
    },
  };
  for (const delivery of [
    null,
    {},
    { key: "A".repeat(64), body },
    { key: "a", body },
    { key, body: "{}" },
    { key, body: {} },
    { key, body: new Uint8Array() },
    { key, body, table: 'inbox"; DROP TABLE users; --' },
    { key, body, schema: "public.other" },
    { key, body, table: "x".repeat(64) },
  ]) {
    await assert.rejects(
      withInbox(pool, delivery, async () => {}),
      TypeError,
    );
  }
  await assert.rejects(withInbox(pool, { key, body }, null), TypeError);
  assert.match(inboxTableSQL(), /CREATE TABLE "public"\."writerelay_inbox"/);
  assert.match(
    inboxTableSQL({ schema: "Receiver", table: "Inbox" }),
    /CREATE TABLE "Receiver"\."Inbox"/,
  );
  assert.throws(() => inboxTableSQL({ table: "inbox; SELECT 1" }), TypeError);
});
