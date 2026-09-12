# WriteRelay Go SDK

The `writerelay` package emits an event through an existing PostgreSQL
transaction. It supports pgx v5 (including transactions from `pgxpool`) and
`database/sql`. The application owns its business writes, event identities,
and commit/rollback decisions. The [receiver inbox helper](INBOX.md) also
provides `WithInbox` and `WithInboxSQL` for committing delivery keys with
receiver database writes. No SDK service needs to be deployed.

Import it as:

```go
import writerelay "github.com/johnathondillon/write-relay/sdk/go"
```

This package is part of the repository's Go module and currently requires
Go 1.26.8 or newer. It has no separate module version or standalone SDK release.
It imports pgx and the standard library, with no daemon or SQLite runtime imports.
To try this checkout from another Go application, use a local module replacement:

```bash
go mod edit -replace=github.com/johnathondillon/write-relay=/absolute/path/to/write-relay
go get github.com/johnathondillon/write-relay/sdk/go
```

Replace the path with your checkout's location. The local replacement is for
development; remove it when adopting a repository version containing the SDK.

## Emit inside a transaction

Install [the SQL function](../../sql/postgres/001_install.sql), grant the
application role `USAGE` on its schema and `EXECUTE` on the function, and set up
the relay using the [root quickstart](../../README.md#local-quick-start).
`Emit` does not create database objects or configure the daemon.

This example uses the development `orders` table:

```go
func recordOrder(ctx context.Context, conn *pgx.Conn, orderID string) error {
    tx, err := conn.Begin(ctx)
    if err != nil {
        return err
    }
    defer tx.Rollback(context.Background())

    if _, err := tx.Exec(ctx,
        "INSERT INTO orders(id, status) VALUES($1, 'paid')", orderID,
    ); err != nil {
        return err
    }
    data, err := json.Marshal(struct {
        OrderID string `json:"orderId"`
    }{OrderID: orderID})
    if err != nil {
        return err
    }
    if err := writerelay.Emit(ctx, tx, writerelay.Event{
        ID: "evt-" + orderID,
        Source: "urn:service:billing",
        Type: "order.paid",
        Subject: &orderID,
        Data: data,
    }); err != nil {
        return err
    }
    return tx.Commit(ctx)
}
```

Use imports `context`, `encoding/json`, `github.com/jackc/pgx/v5`, and the SDK
import above. A transaction from `pool.Begin(ctx)` can be used the same way.
`Emit` takes a `pgx.Tx`; a bare connection or pool cannot be passed instead.

For `database/sql`, use a PostgreSQL driver and pass the same `*sql.Tx` used
by your business writes to `EmitSQL`:

```go
tx, err := db.BeginTx(ctx, nil)
if err != nil {
    return err
}
defer tx.Rollback()
if _, err := tx.ExecContext(ctx,
    "INSERT INTO orders(id, status) VALUES($1, 'paid')", orderID,
); err != nil {
    return err
}
if err := writerelay.EmitSQL(ctx, tx, event); err != nil {
    return err
}
return tx.Commit()
```

These are synchronous calls: wait for the emission to return before committing.
Always roll back on validation or database errors. A validation error happens
before SQL, so it does **not** automatically abort your database transaction;
ignoring it and committing can save a business write without its event.

## API and validation

```go
func Emit(ctx context.Context, tx pgx.Tx, event Event) error
func EmitSQL(ctx context.Context, tx *sql.Tx, event Event) error
```

Both issue one parameterized `SELECT writerelay.emit($1::jsonb)` on the supplied
transaction. A nil error means the statement succeeded. It does not mean the
transaction committed or a receiver accepted the event. Neither call begins,
commits, rolls back, closes, releases, or retries a transaction.

| Event field | Behavior |
| --- | --- |
| `ID`, `Source`, `Type` | Required non-empty UTF-8 strings, without NUL. |
| `SpecVersion` | Empty defaults to `"1.0"`; other versions are rejected. |
| `Subject` | `*string`; nil omits it, while a pointer to an empty string includes it. |
| `Time` | Empty omits it; otherwise a strict RFC 3339 string with a timezone. Use `time.Time.Format(time.RFC3339Nano)` when appropriate. |
| `Data` | `json.RawMessage`; nil omits it. Use `json.RawMessage("null")` for explicit JSON null. A non-nil empty slice is invalid. |
| `DataContentType` | Empty defaults to `"application/json"` when data is present; otherwise omitted. A non-empty value is preserved. |

Serialize Go structs/maps explicitly with `json.Marshal` and handle its error,
or supply already encoded JSON. Objects, arrays, scalars, and null are supported;
large integers are not converted through floating point. Invalid JSON, invalid
UTF-8, NUL escapes, and unpaired Unicode surrogate escapes are rejected before
SQL, including in object keys. Standard `json.Marshal` rules apply when your
application prepares the data: for example, `[]byte` becomes base64 and invalid
UTF-8 in a Go string can be replaced during that earlier serialization. The SDK
validates the JSON it receives; it cannot detect changes that already happened.

This version supports the same envelope attributes as the TypeScript SDK, not
arbitrary CloudEvents extensions or binary mode. It makes no full CloudEvents
conformance claim. It generates no IDs or timestamps and does not mutate the
event or its data. PostgreSQL normalizes the envelope through `jsonb`; input
whitespace, object key order, and duplicate JSON keys are not preserved.

Use `errors.Is(err, writerelay.ErrInvalidEvent)` for SDK validation failures
and `errors.Is(err, writerelay.ErrNilTransaction)` for a missing transaction.
Validation messages contain field names, not event values. Database errors
propagate unchanged, preserving `errors.Is`/`errors.As` and PostgreSQL SQLSTATEs.
Apply your application's normal redaction before logging driver errors.

The SQL function remains authoritative for its 262,144-byte limit after
conversion to PostgreSQL `jsonb` text. The daemon can impose a smaller limit.
The SDK does not approximate PostgreSQL's serialized size.

## Retries and delivery

Keep `Source`, `ID`, and the entire event content stable when retrying one
logical event, including any timestamp. A new ID on each attempt defeats
deduplication; changed content for an existing identity stops capture.

`Emit` does not make producer application requests idempotent. A connection failure
during commit can leave its outcome unknown. Use a unique business/request ID
and check for an existing committed result before retrying. The
[Nuxt producer](../../examples/nuxt-lms/server/api/completions.post.ts) illustrates
that application responsibility.

After capture, the daemon owns delivery retries. Receivers still need to
deduplicate the stable webhook `Idempotency-Key`; PostgreSQL and a remote
operation do not commit atomically together. Use the [inbox helper](INBOX.md)
for receiver database transactions, and see the [FAQ](../../docs/faq.md).

## Example and verification

The [runnable Go producer](../../examples/go-producer/README.md) demonstrates
commit, rollback, and unchanged-event replay using the development stack.

From the repository root:

```bash
go test ./sdk/go
go test -race ./sdk/go
make integration-version
```

Unit tests need no database and cover parameterization, defaults, metadata,
JSON validation, transaction ownership, and unchanged error propagation.
The PostgreSQL integration suite exercises both pgxpool transactions and
`database/sql` with the pgx driver: commit reaches the webhook, rollback and
error paths leave no business/event records, and identical re-emission stays
deduplicated. A later committed marker bounds absence checks. Existing CI runs
these tests against PostgreSQL 14–18.

The [Go receiver example](../../examples/go-receiver/README.md) demonstrates
HTTP duplicate handling, rollback, and a lost response after commit. Its tests
and the receiver SDK tests run in `make integration-version` and the matrix.
