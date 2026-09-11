# ADR 0008: Producer SDKs leave transaction and identity ownership to applications

Status: accepted

Producer SDKs validate the supported envelope and execute a single parameterized
`SELECT writerelay.emit($1::jsonb)` through the application's existing transaction.
They do not create database objects, generate identities or timestamps, retry
SQL, or manage commit/rollback. A successful emission call means only that its
SQL statement succeeded. The application must roll back on any SDK error,
including validation failures that occur before PostgreSQL receives a statement.

The Go package lives at `sdk/go` in the existing repository module. `Emit` takes
a pgx v5 `Tx`, including pool transactions, and `EmitSQL` takes a `*sql.Tx` using
a PostgreSQL driver. Bare connections and pools are not accepted. The TypeScript
SDK uses a checked-out client and documents that it cannot verify a transaction
exists; both SDKs require business writes and emission to share the transaction.

The first Go envelope supports the same attributes as the TypeScript SDK. Go's
`Data` is a `json.RawMessage`: callers serialize domain structs/maps explicitly,
and nil means absent while JSON `null` means present. Validation rejects malformed
JSON and PostgreSQL-incompatible Unicode before SQL. The SDK cannot detect data
loss that occurred before the caller supplied JSON. PostgreSQL `jsonb` remains
responsible for normalization and the SQL size limit. Neither SDK claims full
CloudEvents conformance or supports arbitrary extension attributes yet.

An event's source, ID, and entire content remain caller-controlled and stable
across retries. The SDK does not resolve ambiguous commit outcomes or implement
request idempotency. Existing capture replay checks and receiver deduplication
remain separate responsibilities; the daemon's durability, ACK, delivery, and
retry rules are unchanged.

Go unit tests verify one parameterized statement on the supplied transaction,
defaults, validation before SQL, and unchanged database errors. Integration
tests exercise both Go transaction APIs through PostgreSQL and the relay, proving
commit delivery, rollback/error absence after a later committed marker, and
deduplication of identical re-emission. Existing process-crash tests continue to
cover the daemon's durability boundaries.
