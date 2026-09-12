# ADR 0009: Receiver inbox keys commit with business writes

Status: accepted

HTTP delivery remains at least once. A receiver may commit its work and lose the
response before the daemon records success. The TypeScript SDK's `withInbox`
helper owns a receiver PostgreSQL transaction that stores a unique delivery key,
the SHA-256 hash of the original request bytes, and the callback's business writes
together. The HTTP handler reports success only after the helper commits.

`INSERT ... ON CONFLICT DO NOTHING` arbitrates concurrent attempts. A new key
runs the callback. An existing key requires identical bytes before returning a
duplicate result without running the callback. Different bytes fail with
`InboxConflictError`; existing receipts are never overwritten. Explicit
`READ COMMITTED` isolation lets the statement following a conflicting insert see
the other transaction's committed receipt. `SELECT ... FOR SHARE` holds that
receipt stable through the duplicate transaction's commit.

The helper takes a pool and uses one checked-out client throughout. The callback
receives a query interface and must await all business SQL, propagate errors,
and leave transaction control to the helper. PostgreSQL can answer `COMMIT` with
a `ROLLBACK` command tag when a callback swallowed an SQL error; this is treated
as failure. Rollback failures preserve the original error and discard the
connection. Commit errors also discard the connection because the outcome may be
unknown. A retry uses the same key and bytes to resolve that uncertainty.

Applications authenticate, bound and validate incoming requests first. Neither a
delivery key nor its content hash authenticates a sender. External side effects
inside callbacks are not protected by the PostgreSQL transaction. The producer
SDK's `emit` continues to use the caller's transaction as defined in ADR 0008.

Applications own inbox migrations, grants, and retention. `inboxTableSQL` returns
DDL for a stable, explicitly named table; request handling never runs DDL. Inbox
records must survive the permitted duplicate/redrive horizon. Separate consumers
use separate inbox tables. The certificate example retains its existing table,
column names, and hash format, preserving earlier deduplication records.

Unit and package tests check arguments, transaction lifetime, error propagation,
and exported types. PostgreSQL tests verify atomic commit/rollback, hash conflicts,
swallowed errors, and actual concurrent lock waits through commit and rollback.
The LMS verifier exercises the shared helper after a committed receiver write
loses its HTTP response. These checks do not change the daemon's durability,
acknowledgment, delivery ordering, or retry decisions.

## Go receiver API and compatibility matrix

The Go SDK provides `WithInbox` for `*pgxpool.Pool` and `WithInboxSQL` for
`*sql.DB`. Their callbacks expose query methods without transaction control or
connection access. The Go schema generator uses the same columns, identifier
restrictions, and raw-byte SHA-256 format as TypeScript. A compatible existing
inbox can be reused when changing receiver languages. Empty Go table fields use
the defaults; TypeScript uses omitted fields for defaults.

Both Go APIs explicitly select `READ COMMITTED`. Before commit, they execute a
statement that fails if the callback swallowed an SQL error and left PostgreSQL
in an aborted transaction. This protects the SQL API even though `database/sql`
does not expose PostgreSQL's commit command tag. pgx also rejects a ROLLBACK tag
from Commit. Errors yield an empty result, so callers cannot mistake an uncertain
commit for a processed or duplicate success.

Deferred rollback covers callback errors, request cancellation, and panic
unwinding. pgx cleanup gets a fresh five-second context; `database/sql` owns its
transaction cancellation and cleanup through its driver. Neither API retries
SQL or suppresses a callback panic. Callback code must finish queries and close
rows before returning, avoid concurrent transaction use, and leave transaction
control to the helper.

The Go compatibility suite exercises both drivers and mixed-driver concurrent
attempts. A runnable Go HTTP receiver has lost-response and rollback/retry tests
in the same PostgreSQL 14–18 matrix. TypeScript receiver tests now run in their
own disposable PostgreSQL 14–18 matrix, independently of the complete LMS check.
