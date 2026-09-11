# WriteRelay

WriteRelay is an architectural proof for PostgreSQL-first transactional event
transport. An application emits a structured event inside its PostgreSQL
transaction. A Go daemon reads committed logical messages from WAL and commits
them to a durable local SQLite spool before acknowledging the transaction's WAL
position. It then creates durable per-sink delivery records and sends events to
configured stdout or HTTP webhook sinks with bounded retries.

> WriteRelay provides atomic event creation with a PostgreSQL transaction, a
> durable relay handoff, and at-least-once external delivery.

Milestones 2 and 3 add ordered at-least-once webhook delivery and deterministic
process-crash recovery proofs. WriteRelay does not claim exactly-once
processing, global ordering, or atomicity with an external broker.

## What WriteRelay does for your application

An application can save a business change and emit an event announcing it in
the same PostgreSQL transaction. Both commit together, or both roll back.
WriteRelay then durably captures the event and delivers it separately. If the
destination is unavailable, retryable failures are retried automatically within
configured limits; permanent failures and exhausted retries are retained for
inspection and manual redrive. A delivery failure does not undo the original
database change.

For example, after a payment provider reports a successful payment, your LMS
can record that payment and emit `payment.recorded` together. WriteRelay can
then notify a course-access service using the user and course IDs you include
in the event. The external payment is already complete; it is outside that
PostgreSQL transaction.

Current destinations are stdout and HTTP endpoints that accept WriteRelay's
event format. Provider-specific API calls and direct queue integrations require
additional integration code. See the [FAQ](docs/faq.md) for examples and the
boundaries of these guarantees.

## Status

This repository is a Milestone 3 architectural proof, not a production-ready
delivery system. Its public project name is **WriteRelay**, its repository name
is `write-relay`, and its Go module path is
`github.com/johnathondillon/write-relay`.

The current implementation targets PostgreSQL 14–18. The CI integration matrix
tests every declared major version using official PostgreSQL Docker images.
Managed-service compatibility remains unverified.

Milestone 4 has started with an unpublished
[TypeScript/Node producer SDK](sdk/typescript/README.md), used by the
[Nuxt LMS example](examples/nuxt-lms/README.md). It validates and emits events
through the application's existing PostgreSQL transaction client. SQL-only
integration remains supported.

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
   receives pending records for existing events with retained payloads; changing
   a sink's type or target requires a new sink name.
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

### When a receiver should return success

WriteRelay treats any `2xx` response, including `202 Accepted`, as successful
delivery. Return success only after the work is complete or the receiver has
durably saved it for later processing. In the latter case, the receiver owns
retries and recovery from that point onward. Once WriteRelay records delivery
as successful, it does not retry it or monitor downstream processing.

Starting background work in memory and immediately returning success can lose
that work if the receiver crashes. For a course-access service, commit the
access grant or durably enqueue the request before returning success.

### Handling duplicate webhooks

WriteRelay sends the **same `Idempotency-Key` on every attempt for a given
delivery**. The receiving service is responsible for using that key to avoid
processing the delivery more than once.

For example, an LMS saves a course completion and emits a `course.completed`
event in the same PostgreSQL transaction. WriteRelay later sends the event to a
certificate service. If that service creates the certificate but its success
response is lost, WriteRelay can send the event again with the same key.

The certificate service should:

1. Start a database transaction and insert the key into a table with a unique
   constraint on the key.
2. If the key is new, create the certificate in that same transaction, then
   commit both the certificate and the key before returning a `2xx` response.
3. If the key was already committed, return a `2xx` response without creating
   another certificate.

The unique constraint protects against concurrent duplicate requests. Saving
the key and certificate together ensures a failure rolls both back, allowing a
later attempt to try again. This transaction protects changes in the receiver's
database; any additional external calls need their own duplicate handling.

## Crash-recovery proof

`make failure` runs ordinary persistence and delivery code in child processes
and terminates those processes without deferred cleanup at every critical
boundary:

- before and midway through a SQLite capture transaction;
- after SQLite commit but before PostgreSQL acknowledgment;
- immediately after acknowledgment;
- before and during a webhook request;
- after destination success but before local success is recorded.

The parent tests reopen the same spool and prove atomic rollback, durable replay,
checkpoint/acknowledgment agreement, per-sink ordering, and retry of ambiguous
requests. The in-flight and post-success cases intentionally demonstrate that
the same idempotency key may be sent more than once.

Crash hooks are injected directly by tests. The production daemon has no
configuration, environment variable, or endpoint that can activate them.

## Preview packages

The [release workflow](.github/workflows/release.yml) builds Linux and macOS
binaries for amd64 and arm64, checksums, and versioned Linux Docker images.
See the [installation guide](docs/installation.md) for published previews and
the [release guide](docs/releases.md) to build and review packages locally.
The sample versions in those guides are illustrative; downloads require a
published preview on the [releases page](https://github.com/johnathondillon/write-relay/releases).
WriteRelay remains an architectural preview.

## Local quick start

For a browser-based walkthrough with no local Go or Node setup, run the
[Nuxt LMS learning lab](examples/nuxt-lms/README.md) with Docker Compose. It
demonstrates committed events, rollback, automatic retry, duplicate handling,
and manual dead-letter redrive using a separate certificate service.

Prerequisites are Go 1.26.8 or newer, Docker Compose, and optionally `psql`.
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

Get a summary of the local spool:

```bash
go run ./cmd/writerelayd spool stats --config ./writerelay.yaml
go run ./cmd/writerelayd spool stats --config ./writerelay.yaml --json
```

`stats` reports captured identity and pruned-payload counts, the durable
checkpoint, delivery counts by state and sink, oldest waiting age, and
database/WAL/SHM file sizes. It reads
an existing spool without creating or migrating it and requires no PostgreSQL
connection or resolved sink secrets. For the Nuxt example, use the
[Docker stats commands](examples/nuxt-lms/README.md#watch-the-relays-delivery-counts).

Waiting means `pending` or `retry_wait`; age starts at the event's original local
capture time, including after sink backfill or manual redrive. Dead letters are
reported separately. Counts include inactive sinks with retained history, and
an event sent to multiple sinks contributes multiple delivery records. File
sizes are approximate file lengths, including SQLite's WAL, not PostgreSQL's
retained WAL or filesystem allocated space. A successful snapshot does not
establish that the daemon is running or a destination is healthy.

Preview cleanup of old, successfully delivered payloads:

```bash
go run ./cmd/writerelayd spool prune --config ./writerelay.yaml \
  --before 2026-09-01T00:00:00Z --limit 100 --dry-run
```

Remove `--dry-run` to apply. Every associated delivery must have succeeded before
the cutoff; pending work, retries, dead letters, and capture-only events remain
intact. Identity/digest and delivery history are retained for replay protection.
Pruned payloads cannot be backfilled to a new sink. Freed space is reusable by
SQLite; the file does not automatically shrink. This requires schema 3: stop the
old daemon and restart the updated binary to migrate before pruning. See the
[retention guide](docs/retention.md) for batch limits, upgrade and disk behavior.

Inspect individual delivery records:

```bash
go run ./cmd/writerelayd spool deliveries \
  --config ./writerelay.yaml \
  --state dead_letter \
  --limit 20
```

After correcting the destination or event handling, explicitly redrive one
dead-letter record:

If you are running the **Nuxt LMS learning lab**, use its
[Docker redrive instructions](examples/nuxt-lms/README.md#inspect-a-dead-letter-and-retry-it-manually),
which use the example's `certificates` sink and the Completion ID from the page.
The command below is for this README's billing quick start.

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

## Live health and monitoring

Enable the optional HTTP listener in your configuration, then restart the daemon:

```yaml
monitoring:
  listen: 127.0.0.1:9090
  sample_interval: 15s
```

```bash
curl -i http://127.0.0.1:9090/healthz
curl -i http://127.0.0.1:9090/readyz
curl http://127.0.0.1:9090/metrics
```

`healthz` reports process liveness. `readyz` requires an observed replication
connection and a fresh successful spool sample. Metrics expose capture progress,
per-sink pending/retry/dead-letter counts, oldest waiting age, retained/pruned
payload counts, and SQLite file sizes. Receiver failures appear in delivery
metrics while capture can remain ready. Spool statistics are cached between
samples; failed or stale samples fail readiness and suppress those counts.

Monitoring is disabled by default and has no authentication. Use loopback or a
private monitoring network. See the [monitoring guide](docs/monitoring.md) for
signal limits and Prometheus configuration, or try the
[LMS walkthrough](examples/nuxt-lms/README.md#watch-live-health-and-metrics).

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
make failure
make vet
make race
make vuln
make check
make integration
make postgres-down
POSTGRES_VERSION=14 make integration-version
make integration-matrix
```

`make integration` uses the persistent PostgreSQL 18 development database.
`make integration-version` creates a disposable database for the selected major
(18 by default). `make integration-matrix` runs all five versions, 14–18.
The disposable runs use unique Compose projects and automatically assigned
loopback ports, print the exact server version, and remove their containers
and anonymous volumes on exit. They do not use the development or LMS volumes.
Go and Docker Compose are required; the first run downloads database images.

The [CI matrix](.github/workflows/ci.yml) runs the same disposable test command
on pushes and pull requests, with an independent result for each major version.
Failures print PostgreSQL logs and do not cancel the other matrix jobs.

Integration tests prove committed capture, rollback absence, ordering within a
transaction, durable checkpoint acknowledgment, webhook delivery/retry, and
graceful shutdown. They reopen the SQLite spool, capture events committed while
the relay is stopped, and verify that re-emitting identical event content does
not duplicate event or delivery records. Focused tests cover replay,
identity conflicts, sink backfill, per-sink order, retry/dead-letter state,
redrive, redirects, signatures, timeouts, and real child-process crash recovery.

## Documentation

- [FAQ: use cases, retries, and integration boundaries](docs/faq.md)
- [Project specification](docs/specification.md)
- [Architecture](docs/architecture.md)
- [Correctness invariants](docs/correctness.md)
- [Security model](docs/security-model.md)
- [Health checks and monitoring](docs/monitoring.md)
- [Implementation plan](docs/implementation-plan.md)
- [Preview installation](docs/installation.md)
- [Release packaging and publication](docs/releases.md)
- [ADRs](docs/adr)

## License

Apache License 2.0. See [LICENSE](LICENSE).
