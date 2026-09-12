# Receive webhooks with a PostgreSQL inbox

`withInbox` saves a delivery key and your receiver's database changes in one
transaction. After that transaction commits, another attempt with the same key
and original body skips your callback. If the callback fails, both changes roll
back so a later delivery can try again.

This helper is part of `@writerelay/node`; see the [SDK README](README.md) for
local installation. It supports Node 22+, ESM, and the promise-based `pg.Pool`
API. No additional runtime dependencies are included.

## Create the inbox once

Run this through your receiver application's migration process:

```ts
import { inboxTableSQL } from "@writerelay/node";

await migrationClient.query(inboxTableSQL());
```

The returned SQL creates `"public"."writerelay_inbox"` with:

| Column | Purpose |
| --- | --- |
| `idempotency_key` | Primary key; 64 lowercase hexadecimal characters. |
| `payload_sha256` | SHA-256 of the original request bytes. |
| `received_at` | Receipt timestamp, defaulting to PostgreSQL `now()`. |

`inboxTableSQL` only returns SQL. It does not connect, execute a migration, create
a schema, or silently accept an existing table. Run the migration once with a
role allowed to create tables. The runtime role needs `SELECT`, `INSERT`, and
`UPDATE` on the inbox: PostgreSQL requires `UPDATE` permission for the helper's
`SELECT ... FOR SHARE` row lock. It also needs permission for your business SQL.

For a custom location, use the same options for the migration and every call:

```ts
const inbox = { schema: "receivers", table: "certificate_inbox" };
await migrationClient.query(inboxTableSQL(inbox)); // Schema must already exist.
// Later: withInbox(pool, { ...inbox, key, body: rawBody }, callback)
```

Identifiers are quoted separately and must be ASCII SQL identifiers of at most
63 characters. Use separate tables for independent consumers. Keep the table
location stable across deployments; switching tables loses access to prior keys.
The Nuxt certificate example uses its existing `public.inbox` table and preserves
its existing keys and hashes without a schema migration.

## Process a validated request

Authenticate the webhook, limit its size, parse and validate the event, and
validate the `Idempotency-Key` header before calling this function. Preserve the
**original request bytes**, including whitespace; do not re-serialize parsed JSON.
The helper's hash detects changed content; it is not authentication.

This function assumes those request checks have succeeded and the receiver has
a `certificates` table matching the example:

```ts
import { randomUUID } from "node:crypto";
import { Pool } from "pg";
import { withInbox, InboxConflictError } from "@writerelay/node";

const pool = new Pool({ connectionString: process.env.DATABASE_URL });

async function receiveCompletion(
  key: string,
  rawBody: Uint8Array, // Buffer is accepted.
  data: { completionId: string; learner: string; courseTitle: string },
): Promise<number> {
  try {
    await withInbox(pool, { key, body: rawBody }, async (tx) => {
      await tx.query(
        `INSERT INTO certificates
         (completion_id, certificate_id, learner, course_title)
         VALUES ($1, $2, $3, $4)`,
        [data.completionId, randomUUID(), data.learner, data.courseTitle],
      );
    });
    return 204; // Send HTTP success only after withInbox resolves.
  } catch (error) {
    if (error instanceof InboxConflictError) return 409;
    // The receiver failed to establish success. WriteRelay can retry a 503.
    return 503;
  }
}
```

Both a newly processed delivery and an identical duplicate can return `2xx`.
The full [certificate server](../../examples/nuxt-lms/certificate/server.ts)
includes authentication, request validation, and deliberate failure modes.

## Results and failures

`withInbox<T>(pool, delivery, callback): Promise<InboxResult<T>>` checks out a
dedicated client, starts a `READ COMMITTED` transaction, claims the key, and
commits before resolving. It releases the client on success and failure.

| Situation | Behavior |
| --- | --- |
| New key | Run the callback; commit the key and callback SQL; return `{ status: "processed", value }`. |
| Existing key and identical bytes | Skip the callback; return `{ status: "duplicate" }`. Previous callback values are not stored. |
| Same key, different bytes | Throw `InboxConflictError`; do not run the callback or overwrite the existing receipt. |
| Callback or SQL failure | Attempt rollback and propagate the original error. A later delivery can process after rollback. |
| Concurrent attempts | The unique insert waits for the first transaction. After its commit, skip the duplicate; after its rollback, the waiting attempt can process. |
| Connection lost during commit | Throw and discard the connection. The outcome may be unknown; retry the same key and bytes. |

The callback receives only `query(text, values?)`. Use that transaction for
**all** business SQL, await every query, and propagate failures. Do not use
`pool.query` or another connection inside the callback. Do not issue transaction
control commands, change isolation, or retain the transaction for background
work. The helper owns the transaction; it cannot be nested in an existing one.
If PostgreSQL reports `ROLLBACK` in response to `COMMIT` after a swallowed SQL
error, the helper throws `InboxTransactionError` rather than reporting success.

Keys must be 64 lowercase hexadecimal characters, bodies must be non-empty
`Uint8Array` instances, and the callback must be a function. Invalid arguments
throw `TypeError` before checking out a connection. The helper copies the body
before its first asynchronous operation. It does not validate the CloudEvent or
verify that the caller paired the correct event data with those bytes.

The helper does not retry automatically. The HTTP handler returns an error and
WriteRelay applies its configured retry policy; terminal failures require manual
redrive. If the success response is lost after commit, the next identical request
finds the saved receipt and skips the business SQL. Configure pool and statement
timeouts appropriate for your receiver and webhook deadline.

## Boundaries and retention

The transaction protects writes in the receiver's PostgreSQL database. Do not
send email, charge a card, or call another service inside the callback and assume
rollback can undo that action. Such work needs its own durable delivery intent
or downstream idempotency. The producer's PostgreSQL transaction and this
receiver transaction commit independently.

Retain inbox records for as long as duplicates or manual redrives can arrive.
There is no automatic cleanup. Deleting a key allows a later replay to process
again; a finite retention window must match your application's replay policy.
WriteRelay spool pruning does not prune receiver inboxes. Do not edit saved keys
or hashes. Business-level uniqueness still matters: different delivery keys can
refer to the same business action.

## Verify the behavior

`npm test` in `sdk/typescript` runs unit tests, type checks, and a standalone
package-install test. With the Nuxt stack running, from `examples/nuxt-lms`:

```bash
docker compose exec -T certificate node --test certificate/inbox.integration.test.ts
docker compose run --rm --no-deps verify
```

The first command creates and drops uniquely named test tables in the receiver
database. It checks commit, rollback, conflicts, swallowed SQL errors, and
concurrent attempts waiting for commit or rollback. The second exercises the
complete LMS flow, including a lost response followed by a duplicate delivery.
CI runs the full LMS verifier and a separate receiver matrix against PostgreSQL
14–18. To run the receiver matrix without starting the LMS, from the repo root:

```bash
make inbox-typescript-matrix
# Or one version:
POSTGRES_VERSION=14 make inbox-typescript
```

Each run builds the test image, verifies the server major, and removes its own
containers and volumes. It does not alter development or LMS data. See the
[implementation plan](../../docs/implementation-plan.md#go-and-typescript-receiver-compatibility)
for exact versions exercised. The [Go helper](../go/INBOX.md) uses compatible
inbox columns and hashes.
