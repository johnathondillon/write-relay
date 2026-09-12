# Receive webhooks with a Go PostgreSQL inbox

`WithInbox` commits a webhook receipt and your receiver's business writes in one
transaction. Identical attempts skip completed work. Callback failures roll back
both writes so a later delivery can try again. It supports pgxpool v5;
`WithInboxSQL` provides the same behavior for `database/sql` with a PostgreSQL
driver. The compatibility tests use pgx's stdlib driver for that API.

Install the package as described in the [SDK README](README.md). For a complete
local HTTP server and copyable requests, use the
[Go receiver example](../../examples/go-receiver/README.md).

## Create the inbox once

Run the returned DDL through your receiver application's migration process:

```go
ddl, err := writerelay.InboxTableSQL(writerelay.InboxTable{})
if err != nil {
    return err
}
_, err = migrationConnection.Exec(ctx, ddl)
return err
```

The default table is `public.writerelay_inbox`. It has a primary key
`idempotency_key`, a `payload_sha256` column, and a `received_at` timestamp that
defaults to `now()`. Keys and hashes are 64 lowercase hexadecimal characters.
The schema and original-byte SHA-256 format match the TypeScript receiver helper.
An existing compatible inbox can be reused without recreating it.

For custom names, set `InboxTable{Schema: "receivers", Table: "orders_inbox"}`
in both the migration and each `InboxDelivery`. Empty fields use the defaults.
Names are quoted separately and restricted to ASCII SQL identifiers up to 63
characters. The schema must exist; the DDL does not use `IF NOT EXISTS` and
should run once. Request handling never executes migrations.

The runtime role needs schema `USAGE`, plus `INSERT`, `SELECT`, and `UPDATE` on
the inbox table: PostgreSQL requires `UPDATE` permission for `SELECT ... FOR
SHARE`. Grant the permissions needed for your business SQL separately.

## Process a validated request

Authenticate the request, enforce a body-size limit, and validate the event
before calling the helper. Pass the unmodified WriteRelay `Idempotency-Key` and
original request bytes, not re-encoded JSON. A content hash is not authentication.

```go
result, err := writerelay.WithInbox(ctx, pool, writerelay.InboxDelivery{
    Key: key,
    Body: rawBody,
}, func(tx writerelay.InboxTx) error {
    _, err := tx.Exec(ctx,
        "INSERT INTO received_orders(source, event_id, order_id) VALUES($1, $2, $3)",
        event.Source, event.ID, event.Subject,
    )
    return err
})
```

Here `pool` is a `*pgxpool.Pool`. The callback exposes `Exec`, `Query`, and
`QueryRow` on one dedicated transaction. For `database/sql`, pass a `*sql.DB`:

```go
result, err := writerelay.WithInboxSQL(ctx, db, delivery,
    func(tx writerelay.InboxSQLTx) error {
        _, err := tx.ExecContext(ctx,
            "INSERT INTO received_orders(source, event_id, order_id) VALUES($1, $2, $3)",
            event.Source, event.ID, event.Subject,
        )
        return err
    },
)
```

The SQL callback exposes `ExecContext`, `QueryContext`, and `QueryRowContext`.
Finish all work, consume/close query rows, and return errors from the callback.
Do not use another connection, start goroutines with the transaction, retain it
for later use, or issue transaction control statements. The helper starts and
commits its own transaction; it cannot be nested inside an existing one.
It returns no stored business result for duplicates; query your own business
records if your HTTP response needs one.

## Results and retries

Only inspect `result` when `err == nil`:

| Outcome | Result |
| --- | --- |
| New delivery | `InboxProcessed`, after the receipt and callback writes commit. |
| Committed key with identical bytes | `InboxDuplicate`; callback is skipped. |
| Same key with different bytes | `ErrInboxConflict`; no business callback or receipt overwrite. |
| Invalid key, empty body, bad table name, nil pool/database/handler | An error matching `ErrInvalidDelivery`, before checkout. |
| Callback or SQL error | Rollback is attempted; original error is returned. |
| Missing receipt after a conflict or unexpected insert result | `ErrInboxTransaction`; processing is not reported as successful. |

Return HTTP `2xx` for both successful results, after the function returns. The
example maps content conflicts to `409`, invalid arguments to `400`, and other
processing failures to `503`. WriteRelay applies its configured retry policy;
the SDK does not retry automatically. Terminal failures require manual redrive.

Concurrent unique inserts wait for the competing transaction. If it commits,
the waiting attempt reads the saved hash and skips the callback. If it rolls
back, the waiting attempt can claim the key and process. The helper explicitly
uses `READ COMMITTED`, even when the session default uses another isolation level.

Cancellation and panics trigger deferred rollback; panics continue unwinding.
pgx cleanup uses a fresh context with a five-second deadline so a canceled request
cannot prevent rollback. `database/sql` handles transaction cancellation and
connection return through its driver; its rollback API has no context argument.
Configure driver connection/statement timeouts and request deadlines. Both APIs
check for an aborted transaction before commit, so swallowing an SQL error does
not turn PostgreSQL's implicit rollback into successful processing.

A commit error may have an unknown outcome. Return an error and retry the same
key and bytes: an existing receipt identifies completed work. Driver errors
remain available to `errors.Is`/`errors.As`; apply your normal redaction before
logging them. The helper retains no body reference after hashing it; do not
mutate the supplied slice concurrently with the call.

## Boundaries and retention

Only writes in the receiver's PostgreSQL transaction are protected. External
calls, emails, or payments cannot be undone by rollback; use another durable
intent or the downstream service's idempotency support for those actions.
The producer and receiver transactions commit independently.

Keep inbox keys and hashes unchanged for the entire permitted duplicate and
manual-redrive horizon. There is no automatic cleanup. Deleting receipts or
switching tables allows later replays to process again. Use separate tables for
independent consumers. A distinct delivery key can still refer to the same
business action, so retain appropriate business uniqueness constraints too.

## Verification

From the repository root:

```bash
go test ./sdk/go
go test -race ./sdk/go
make integration-matrix
make inbox-typescript-matrix
```

Go integration tests exercise both APIs and mixed-API concurrent attempts. They
verify commit visibility, duplicates, content conflicts, callback/SQL failures,
cancellation, panic cleanup, and retry after rollback. The Go HTTP example tests
verify rollback followed by success and a lost response followed by a duplicate
with one business record. Both Go and TypeScript receiver suites run against
PostgreSQL 14–18 in CI using disposable databases.
