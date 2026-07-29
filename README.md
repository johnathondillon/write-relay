# WriteRelay

WriteRelay is an architectural proof for PostgreSQL-first transactional event
transport. An application emits a structured event inside its PostgreSQL
transaction. A Go daemon reads committed logical messages from WAL and commits
them to a durable local SQLite spool before acknowledging the transaction's WAL
position. It then creates durable per-sink delivery records and sends events to
configured stdout or HTTP webhook sinks with bounded retries.

> WriteRelay provides atomic event creation with a PostgreSQL transaction, a
> durable relay handoff, and at-least-once external delivery.

Milestone 2 adds ordered at-least-once webhook delivery. WriteRelay does not
claim exactly-once processing, global ordering, or atomicity with an external
broker.

## Status

This repository is a Milestone 2 architectural proof, not a production-ready
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

## How delivery works

1. On startup, the daemon registers each configured sink in SQLite. A new sink
   receives pending records for existing events; changing a sink's type or
   target requires a new sink name.
2. New event rows and their active-sink delivery rows commit in the same SQLite
   transaction.
3. One worker selects the oldest non-terminal event independently for each sink.
4. A `2xx` webhook response marks delivery complete. Network failures, `408`,
   `425`, `429`, and `5xx` responses retry with bounded exponential backoff and
   a bounded `Retry-After` value.
5. Other HTTP responses and exhausted retries enter retained `dead_letter`
   state. Operators can inspect and explicitly redrive them.

Webhook requests contain the original event bytes and a stable
`Idempotency-Key`. A crash after the destination accepts a request but before
SQLite records success can cause a duplicate request with the same key.

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

## Configure delivery

Capture-only mode uses `sinks: []`. For a local development stream, configure:

```yaml
delivery:
  poll_interval: 1s
  request_timeout: 10s
  retry:
    initial_delay: 1s
    max_delay: 5m
    max_attempts: 10
  sinks:
    - name: development
      type: stdout
```

The stdout sink prints full payloads and is intended only for development. A
webhook sink uses HTTPS by default:

```yaml
delivery:
  poll_interval: 1s
  request_timeout: 10s
  retry:
    initial_delay: 1s
    max_delay: 5m
    max_attempts: 10
  sinks:
    - name: orders_webhook
      type: webhook
      url: https://events.example.com/writerelay
      authorization_env: WRITERELAY_WEBHOOK_AUTHORIZATION
      signing_secret_env: WRITERELAY_WEBHOOK_SIGNING_SECRET
```

`authorization_env` should resolve to the complete `Authorization` header
value. When `signing_secret_env` is set, WriteRelay adds
`X-WriteRelay-Timestamp` and an HMAC-SHA256
`X-WriteRelay-Signature: v1=<hex>` over `<timestamp>.<raw-body>`. Redirects are
not followed.

Inspect delivery state:

```bash
go run ./cmd/writerelayd spool deliveries \
  --config ./writerelay.yaml \
  --state dead_letter \
  --limit 20
```

After correcting the destination or event handling, explicitly redrive one
dead-letter record:

```bash
go run ./cmd/writerelayd spool redrive \
  --config ./writerelay.yaml \
  --sink orders_webhook \
  --source urn:service:billing \
  --id evt-example-committed
```

The Compose initialization installs the SQL function, development roles, empty
publication, and example `orders` table. `make setup` validates those objects and
creates the missing `pgoutput` slot. It never drops or recreates an existing
object automatically.

## Configuration

Configuration is strict YAML: unknown fields, unsupported versions, invalid
identifiers, unsafe URLs, duplicate sinks, and unsafe bounds are rejected.
Database, authorization, and signing secrets should be provided through their
configured environment variables.

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
absence, ordering within a transaction, durable checkpoint acknowledgment,
webhook delivery/retry, and graceful shutdown. Focused tests cover replay,
identity conflicts, sink backfill, per-sink order, retry/dead-letter state,
redrive, redirects, signatures, timeouts, and crash-window duplicates.

## Documentation

- [Project specification](docs/specification.md)
- [Architecture](docs/architecture.md)
- [Correctness invariants](docs/correctness.md)
- [Security model](docs/security-model.md)
- [Implementation plan](docs/implementation-plan.md)
- [ADRs](docs/adr)

## License

Apache License 2.0. See [LICENSE](LICENSE).
