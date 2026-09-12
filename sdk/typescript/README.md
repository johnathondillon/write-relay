# WriteRelay TypeScript SDK

`@writerelay/node` emits a WriteRelay event through your existing PostgreSQL
transaction client. Its receiver helper saves an inbox key and business writes
in one PostgreSQL transaction, so repeated webhook attempts can skip completed
work. It has no runtime dependencies and works with the
promise-based `pg` client API. This initial package supports Node 22+ and ESM.
It is kept private and has **not been published to npm**.

For receiver setup, usage, retries, and retention, see the
[receiver inbox guide](INBOX.md).

The SDK currently builds with TypeScript 5.9.
[Dependabot](../../.github/dependabot.yml) allows minor and patch compiler
updates; major upgrades are reviewed manually and verified with the SDK tests
and Nuxt example before adoption.

## Try it locally

The [Nuxt LMS example](../../examples/nuxt-lms/README.md) uses this SDK through
a local `file:` dependency. From the repository root:

```bash
cd examples/nuxt-lms
docker compose up --build --wait
```

Open <http://127.0.0.1:3000>. Docker builds the SDK, runs its tests, and builds
the example. No npm account or separate SDK service is needed. PostgreSQL and
the WriteRelay daemon still run as part of the example's Compose stack.

To build, test, and package the SDK yourself, from the repository root:

```bash
cd sdk/typescript
npm ci
npm test
npm pack
```

To use that tarball in another Node application:

```bash
npm install /absolute/path/to/write-relay/sdk/typescript/writerelay-node-0.1.0.tgz
```

The tarball includes compiled JavaScript, TypeScript declarations, and the
license and receiver inbox guide. Install your database driver separately, for example `npm install pg`
and `npm install --save-dev @types/pg` for TypeScript applications.

## Emit inside your transaction

Install [WriteRelay's SQL function](../../sql/postgres/001_install.sql), grant
the application role access, and configure a running relay as described in the
[root README](../../README.md). The SDK does not set up database objects.

This function uses the `completions` table from the LMS example:

```ts
import { Pool } from "pg";
import { emit } from "@writerelay/node";

const pool = new Pool({ connectionString: process.env.DATABASE_URL });

async function completeCourse(
  id: string,
  learner: string,
  courseId: string,
  courseTitle: string,
) {
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    await client.query(
      "INSERT INTO completions (id, learner, course_id, course_title) VALUES ($1, $2, $3, $4)",
      [id, learner, courseId, courseTitle],
    );
    await emit(client, {
      id,
      source: "urn:example:lms",
      type: "course.completed",
      data: { completionId: id, learner, courseId, courseTitle },
    });
    await client.query("COMMIT");
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}
```

Use the **same checked-out client** for `BEGIN`, business writes, `emit`, and
`COMMIT`/`ROLLBACK`. Do not pass `pool` or use `pool.query` for these statements.
This follows [node-postgres's transaction requirements](https://node-postgres.com/features/transactions).

`emit` cannot verify that your client has an open transaction. If you call it
outside one, PostgreSQL can commit the emission separately from your business
write. `emit` never begins, commits, rolls back, releases a connection, or
retries a transaction for you. Always await it before committing, and roll back
on validation or database errors. Catching an emission error and committing
anyway can save a business change without its event.

## API and validation

`emit<T>(client: TransactionClient, event: WriteRelayEvent<T>): Promise<void>`
issues one parameterized `SELECT writerelay.emit($1::jsonb)` statement. A
resolved promise means the statement succeeded; it does not mean the
transaction committed or the receiver accepted the event.

| Attribute | Behavior |
| --- | --- |
| `id`, `source`, `type` | Required non-empty strings. |
| `specversion` | Defaults to `"1.0"`; other versions are rejected. |
| `data` | Optional JSON value; use a domain type with `WriteRelayEvent<T>` or `emit<T>`. |
| `datacontenttype` | Defaults to `"application/json"` when data is present; explicit values must be non-empty strings. |
| `subject` | Optional string. |
| `time` | Optional RFC 3339 string with a timezone, such as `"2026-09-08T12:00:00Z"`. Convert `Date` explicitly with `.toISOString()`. |

This first SDK supports the attributes above, not arbitrary CloudEvents
extensions or binary data. It validates WriteRelay's supported envelope;
it does not claim complete CloudEvents conformance. Metadata defaults do not
generate an ID or timestamp and the caller's event object is not mutated.

Data must contain plain objects, arrays, strings, finite numbers, booleans, or
null. The SDK rejects cycles, nested `undefined`, sparse arrays, `BigInt`,
functions, class instances, and other values that could serialize with data
loss. An omitted or `undefined` top-level `data` means no data attribute.
PostgreSQL-incompatible NUL characters and unpaired Unicode surrogates are
also rejected. Validation throws `TypeError` before any SQL is sent; database
errors propagate unchanged. Error messages do not include event payloads.

The SQL function remains authoritative for its 262,144-byte limit, measured
after PostgreSQL converts the event to `jsonb` text. The daemon can impose a
smaller configured limit. Keep events within both limits; the SDK does not
approximate PostgreSQL's serialized size.

## Retries and delivery

Supply a stable ID for each logical event. Reuse its `source`, `id`, and full
content when retrying that event, including any timestamp. A new ID on every
attempt defeats deduplication. Reusing an identity with different content
stops WriteRelay capture.

`emit` does not make producer application requests idempotent. If a connection fails
during `COMMIT`, the commit outcome may be unknown. Your application needs a
unique request ID and a way to look up the existing result before retrying.
The [LMS producer](../../examples/nuxt-lms/server/api/completions.post.ts)
demonstrates this with a unique completion ID and conflict handling.

WriteRelay retries retryable delivery failures after capture. The receiver must
still handle duplicates using the stable webhook `Idempotency-Key`. Use
[`withInbox`](INBOX.md) to commit that key with receiver database changes. The SDK
does not atomically commit a remote operation with PostgreSQL. See the
[FAQ](../../docs/faq.md).

SQL-only integration remains supported: applications can issue the same
parameterized SQL directly without this package.

## Verification

`npm test` checks validation, SQL parameterization, database-error propagation,
stable identities, TypeScript declarations, and installation of the packed
package in a standalone consumer. Receiver tests also check duplicate/conflict
handling, rollback, commit errors, and connection cleanup. No database is required
for those tests.

The Nuxt example's Docker build runs these SDK tests and typechecks its real
`pg` client integration. With the example running, execute:

```bash
# From examples/nuxt-lms:
docker compose exec -T certificate node --test certificate/inbox.integration.test.ts
docker compose run --rm --no-deps verify
```

This exercises the SDK against PostgreSQL and the daemon: commit delivers,
rollback leaves neither the business record nor a delivered event, concurrent
producer requests stay idempotent, outages retry, and a lost response produces
a duplicate with the same delivery key. CI runs the LMS workflow and a separate
receiver integration matrix against PostgreSQL 14–18 on SDK changes.

The receiver tests use disposable tables to verify atomic inbox/business writes,
conflicts, and concurrent attempts waiting for commit or rollback. See the
[inbox guide](INBOX.md#verify-the-behavior) for details and version evidence.

Run `make inbox-typescript-matrix` from the repository root for the standalone
receiver compatibility matrix, or `POSTGRES_VERSION=14 make inbox-typescript`
for one version. These commands manage disposable databases automatically.
