# WriteRelay

WriteRelay is an architectural proof for PostgreSQL-first transactional event
transport. An application emits a structured event inside its PostgreSQL
transaction. A Go daemon reads committed logical messages from WAL and commits
them to a durable local SQLite spool before acknowledging the transaction's WAL
position.

> WriteRelay provides atomic event creation with a PostgreSQL transaction, a
> durable relay handoff, and at-least-once external delivery.

Milestones 0 and 1 implement capture only. There are no external delivery sinks
yet. WriteRelay does not claim exactly-once processing, global ordering, or
atomicity with an external broker.

## Status

This repository is a Milestone 1 architectural proof, not a production-ready
delivery system. Its public project name is **WriteRelay**, its repository name
is `write-relay`, and its Go module path is
`github.com/johnathondillon/write-relay`.

The current implementation targets PostgreSQL 14–18 and is locally exercised
with PostgreSQL 18. Compatibility across every declared major version still
needs CI coverage.

## How capture works

1. `writerelay.emit(jsonb)` validates the envelope and calls
   `pg_logical_emit_message(true, 'writerelay.v1', payload)`.
2. PostgreSQL includes the message in logical decoding only with its transaction.
3. The daemon buffers matching messages from `Begin` through `Commit`.
4. It inserts the complete batch and transaction-end checkpoint in one SQLite
   transaction configured with WAL journaling and `synchronous=FULL`.
5. Only after SQLite commits does it send a standby status update at the durable
   transaction-end LSN.

The spool is replay-safe on `(source, id)`. Identical content is accepted as a
replay; different content for the same identity stops capture.

## Local quick start

Prerequisites are Go 1.26.5 or newer, Docker Compose, and optionally `psql`.
Use the latest available security patch for the selected Go release.

```bash
cp writerelay.example.yaml writerelay.yaml
export WRITERELAY_POSTGRES_DSN='postgres://writerelay_repl:dev-repl-password@localhost:5432/writerelay?sslmode=disable'

make postgres-up
make setup

go run ./cmd/writerelayd doctor --config ./writerelay.yaml
go run ./cmd/writerelayd run --config ./writerelay.yaml
```

The credentials above are for the disposable Compose environment only. In
another terminal, emit a committed event:

```bash
psql 'postgres://writerelay_app:dev-app-password@localhost:5432/writerelay?sslmode=disable' \
  -f examples/commit.sql
```

Inspect captured rows:

```bash
go run ./cmd/writerelayd spool list \
  --config ./writerelay.yaml \
  --limit 20
```

Then run the rollback example:

```bash
psql 'postgres://writerelay_app:dev-app-password@localhost:5432/writerelay?sslmode=disable' \
  -f examples/rollback.sql
```

`evt-example-rolled-back` must not appear in the spool.

The Compose initialization installs the SQL function, development roles, empty
publication, and example `orders` table. `make setup` validates those objects and
creates the missing `pgoutput` slot. It never drops or recreates an existing
object automatically.

## Configuration

Configuration is strict YAML: unknown fields, unsupported versions, invalid
identifiers, and unsafe bounds are rejected. Secrets should be provided through
the configured environment variable.

The defaults cap each event at 256 KiB, each PostgreSQL transaction at 10,000
accepted events and 8 MiB of accepted event bytes, and standby status intervals
between one second and five minutes.

## Development commands

```bash
make help
make fmt
make build
make test
make vet
make race
make vuln
make check
make integration
make postgres-down
```

Integration tests use Docker Compose and prove committed capture, rollback
absence, ordering within a transaction, durable checkpoint acknowledgment, and
graceful shutdown. Focused SQLite tests cover replay and identity conflicts.

## Documentation

- [Project specification](docs/specification.md)
- [Architecture](docs/architecture.md)
- [Correctness invariants](docs/correctness.md)
- [Security model](docs/security-model.md)
- [Implementation plan](docs/implementation-plan.md)
- [ADRs](docs/adr)

## License

Apache License 2.0. See [LICENSE](LICENSE).
