# WriteRelay Project Specification

> **Public name:** `WriteRelay`; repository: `write-relay`; Go module: `github.com/johnathondillon/write-relay`.
>
> A preliminary public-use and trademark collision search found no exact competing software brand or registered mark. This is practical publication screening, not a legal guarantee of worldwide trademark availability.
>
> **Implemented scope:** Milestones 0 and 1 prove that a local PostgreSQL
> transaction can emit a structured event into WAL and the Go daemon can capture
> the committed event into a durable SQLite spool before acknowledging its WAL
> position. A rolled-back event never reaches the spool. Milestone 2 adds
> durable per-sink state plus ordered stdout and HTTP webhook delivery with
> retry, dead-letter, inspection, and redrive. Milestone 3 proves the documented
> crash windows with real child-process termination and spool reopening.

---

# 1. Product mission

Build an open-source, PostgreSQL-first transactional event transport.

An application should be able to update business data and intentionally emit a domain event inside the **same PostgreSQL transaction**. PostgreSQL records that event as a transactional logical-decoding message in its write-ahead log. A separate Go daemon reads committed messages through PostgreSQL's logical replication protocol, durably stores them locally, acknowledges the corresponding WAL position, and later delivers them to external sinks with at-least-once semantics.

The product exists to reduce the operational cost of the transactional outbox pattern for PostgreSQL applications while preserving an honest reliability model.

Conceptually:

```text
Application transaction
        │
        ├── UPDATE / INSERT business data
        │
        └── writerelay.emit(event)
                    │
                    ▼
             PostgreSQL WAL
                    │
                    ▼
       Logical replication reader
                    │
                    ▼
          Durable local SQLite spool
                    │
              acknowledge WAL
                    │
                    ▼
       Delivery workers and external sinks
```

The first release is not a general-purpose CDC platform. It transports explicit
application-defined domain events and delivers them with an at-least-once
contract.

---

# 2. The problem being solved

A common business operation needs to do two things:

```text
1. Commit a database change.
2. Publish an event to another system.
```

For example:

```text
1. Mark an order as paid in PostgreSQL.
2. Publish order.paid to Kafka, SQS, NATS, or a webhook.
```

A normal database transaction cannot atomically commit an operation in an unrelated message broker. A process can crash after either side succeeds, creating missed events or duplicates. Companies commonly solve this with an outbox table and a polling or CDC relay.

This project tests and productionizes a narrower PostgreSQL-specific alternative:

```sql
BEGIN;

UPDATE orders
SET status = 'paid'
WHERE id = 'ord_123';

SELECT writerelay.emit(
    '{
      "specversion": "1.0",
      "id": "0197f8e2-f6cf-7f6f-a3da-d736db668018",
      "source": "urn:service:billing",
      "type": "order.paid",
      "subject": "ord_123",
      "datacontenttype": "application/json",
      "data": {
        "amount": 12900,
        "currency": "USD"
      }
    }'::jsonb
);

COMMIT;
```

The SQL wrapper calls PostgreSQL's transactional logical-message primitive. If the transaction rolls back, the event must not be treated as committed. If the transaction commits, the event becomes available to logical decoding along with the transaction boundary.

---

# 3. Reliability contract

The README, code comments, logs, documentation, and future marketing must use the following terminology precisely.

## 3.1 Guarantees the project is designed to provide

Given a healthy PostgreSQL instance, available WAL, a recoverable local spool, and an eventually recoverable destination:

1. **Atomic event creation with the PostgreSQL transaction**  
   An event emitted through the transactional SQL API is committed or rolled back with the surrounding PostgreSQL transaction.

2. **No delivery of rolled-back transactional events**  
   A transactional event from an aborted transaction must never be persisted as a deliverable event.

3. **Durable handoff before PostgreSQL acknowledgment**  
   The daemon must durably commit a completed PostgreSQL transaction's relevant events to the local spool before advancing the replication slot's acknowledged flush position past that transaction.

4. **At-least-once delivery**  
   Once delivery sinks exist, a committed event may be attempted more than once after failures.

5. **Stable deduplication identity**  
   The combination of CloudEvents-style `source` and `id` identifies an event. Replaying the same event must not create a second spool record or a second pending delivery record.

6. **Ordering in the initial implementation**  
   Events from one PostgreSQL replication slot are persisted in commit order. Events within one transaction are persisted in their original message order. Initial delivery will use one worker and preserve spool order.

## 3.2 Guarantees the project must not claim

Do not claim any of the following:

- exactly-once end-to-end processing;
- zero duplicates at an external destination;
- atomicity between PostgreSQL and an external broker;
- global ordering across multiple PostgreSQL databases or slots;
- zero data loss if PostgreSQL discards required WAL;
- durability if the spool storage lies about `fsync`, is lost, or is corrupted beyond recovery;
- compatibility with every managed PostgreSQL provider before it is tested.

Use this concise public description:

> WriteRelay provides atomic event creation with a PostgreSQL transaction, a durable relay handoff, and at-least-once external delivery.

---

# 4. PostgreSQL-first scope

The underlying dual-write problem is database-independent, but the optimized implementation in this repository is initially PostgreSQL-specific.

## Supported target for the first release

- PostgreSQL 14 through 18.
- Go 1.26.5 or newer for the daemon and tooling.
- Linux and macOS development environments.
- Docker Compose for local integration testing.
- One PostgreSQL database and one logical replication slot per daemon instance.
- PostgreSQL's built-in `pgoutput` logical decoding output plugin.
- Protocol version 1 initially, with transaction streaming disabled.
- An empty publication, because `pgoutput` requires a publication name even when the daemon only wants logical messages.
- Logical messages enabled with the `messages` output-plugin option.
- A single exact message prefix: `writerelay.v1`.
- JSON events using a constrained CloudEvents 1.0-compatible structured envelope.
- SQLite as the local durable spool.

## Why protocol version 1 initially

The initial product transports small, explicit domain events. It does not need in-progress streaming of very large transactions. Protocol version 1 avoids handling `Stream Start`, `Stream Stop`, streamed subtransactions, stream aborts, and two-phase-commit messages in the first implementation.

Support for protocol versions 2 through 4 can be added only after the basic durability and crash-recovery path is proven.

---

# 5. Version 0.1 boundaries

## In scope

- SQL installation script and `writerelay.emit(jsonb)` function.
- An empty PostgreSQL publication.
- Logical replication slot setup and validation.
- Go replication client using `pglogrepl` and `pgx`.
- Parsing `Begin`, logical-decoding `Message`, `Commit`, and keepalive records.
- Filtering by the exact prefix `writerelay.v1`.
- Validation of the event envelope.
- Transaction-aware buffering until a PostgreSQL `Commit` record is received.
- Durable SQLite persistence of a complete committed batch.
- WAL acknowledgment only after successful spool commit.
- Replay-safe insertion based on `(source, event_id)`.
- Structured logging with `log/slog`.
- `run`, `doctor`, `setup`, and `version` CLI commands.
- Docker Compose local environment.
- Unit and integration tests.
- Architecture, correctness, security, and contribution documentation.
- Durable per-sink delivery records created atomically with captured events.
- One ordered delivery worker with independent per-sink progress.
- Stdout development and HTTP webhook sinks.
- Bounded retry, `Retry-After`, retained dead-letter state, inspection, and
  explicit redrive.

## Explicitly out of scope for the current execution target

- Kafka, SQS, SNS, RabbitMQ, NATS, Pub/Sub, Event Hubs, or Kinesis.
- A UI or admin dashboard.
- MySQL, SQL Server, Oracle, MongoDB, or SQLite source adapters.
- Kubernetes operators or Helm charts.
- A custom PostgreSQL extension or custom output plugin.
- General table-change CDC.
- Schema registry integration.
- Event transformations.
- Multi-tenant control planes.
- Horizontal delivery scaling.
- Automatic event deletion or retention.
- Global ordering.
- Two-phase commit decoding.
- Streaming of in-progress PostgreSQL transactions.
- Exactly-once claims.

---

# 6. Critical design decisions

Create ADRs for these decisions under `docs/adr/`.

## ADR 0001 — Use transactional logical messages instead of an outbox table

The PostgreSQL path uses:

```sql
pg_logical_emit_message(true, 'writerelay.v1', payload)
```

The boolean must be `true` in the supported application API. The daemon must reject a message using the protected `writerelay.v1` prefix if PostgreSQL marks it non-transactional.

Do not create an outbox table in the PostgreSQL implementation.

## ADR 0002 — Use built-in `pgoutput`

Use PostgreSQL's built-in `pgoutput` output plugin. Do not require `wal2json`, `test_decoding`, Debezium, or a custom server extension.

Start replication with options equivalent to:

```text
proto_version '1'
publication_names 'writerelay_publication'
messages 'true'
```

The publication and slot names must pass strict identifier validation before being placed in plugin arguments.

## ADR 0003 — Use an empty publication

Create the publication without adding tables:

```sql
CREATE PUBLICATION writerelay_publication;
```

This keeps ordinary table changes out of the stream while satisfying `pgoutput`'s publication requirement. The daemon should tolerate and ignore unrelated DML protocol messages defensively, but their presence should produce a warning because it indicates publication drift.

## ADR 0004 — SQLite is the first durable acknowledgment boundary

The PostgreSQL replication slot must not be advanced beyond a transaction until the transaction's accepted events and local checkpoint have committed to SQLite.

Configure SQLite with at least:

```sql
PRAGMA journal_mode = WAL;
PRAGMA synchronous = FULL;
PRAGMA foreign_keys = ON;
PRAGMA busy_timeout = 5000;
```

Use a bounded number of database connections. A single writer is acceptable for version 0.1.

## ADR 0005 — At-least-once and idempotent replay

A crash can occur after SQLite commits but before PostgreSQL receives or persists the standby status update. PostgreSQL may replay the transaction after restart. Therefore, spool insertion must be idempotent.

Uniqueness is `(event_source, event_id)`, not `event_id` alone.

Store a SHA-256 digest of the canonical payload:

- same `(source, id)` and same digest: treat as a harmless replay;
- same `(source, id)` and different digest: return a fatal protocol-integrity error and do not acknowledge past the offending transaction.

## ADR 0006 — Preserve raw event bytes

Validate the event by parsing JSON, but store and later deliver the exact content bytes received from PostgreSQL. Do not decode and re-encode the event in the relay path.

The SQL `jsonb` API may canonicalize JSON before writing it to WAL. Once the daemon receives those bytes, it must preserve them.

---

# 7. Event protocol

## 7.1 WAL message prefix

Use exactly:

```text
writerelay.v1
```

Messages with other prefixes are not WriteRelay events and should be ignored. A message with this prefix that is malformed, oversized, or non-transactional is a fatal protocol error by default. Silent data loss is worse than stopping the relay.

## 7.2 Event envelope

Version 0.1 accepts a JSON object with the following required attributes:

```json
{
  "specversion": "1.0",
  "id": "0197f8e2-f6cf-7f6f-a3da-d736db668018",
  "source": "urn:service:billing",
  "type": "order.paid"
}
```

Optional attributes include:

```json
{
  "subject": "ord_123",
  "time": "2026-07-27T20:24:00Z",
  "datacontenttype": "application/json",
  "dataschema": "https://schemas.example.test/order-paid/v1",
  "data": {
    "amount": 12900,
    "currency": "USD"
  }
}
```

Validation rules:

- root value must be a JSON object;
- `specversion` must equal `1.0`;
- `id`, `source`, and `type` must be non-empty strings;
- `time`, when present, must be RFC 3339;
- `datacontenttype`, when present, must be a non-empty string;
- unknown top-level attributes are allowed and preserved;
- default maximum content size is 256 KiB;
- the limit is measured in bytes received from PostgreSQL;
- large documents and binaries should be placed in object storage with a reference in `data`.

The relay must not generate a missing ID. Event identity belongs to the producer so retries of the producer transaction can reuse the same identity intentionally.

## 7.3 Internal captured-event metadata

The spool should retain metadata separate from the event JSON:

```go
type CapturedEvent struct {
    Source        string
    ID            string
    Type          string
    Subject       string
    Payload       []byte
    PayloadSHA256 [32]byte

    TransactionID uint32
    MessageLSN    pglogrepl.LSN
    CommitLSN     pglogrepl.LSN
    CommitEndLSN  pglogrepl.LSN
    CommitTime    time.Time
    MessageIndex  int
}
```

Names may change to fit the codebase, but preserve the information and semantics.

---

# 8. PostgreSQL SQL API

Create an idempotent installation script at:

```text
sql/postgres/001_install.sql
```

The script should:

1. create a `writerelay` schema if it does not exist;
2. create or replace `writerelay.emit(event jsonb)`;
3. validate required envelope attributes;
4. enforce the 256 KiB default limit;
5. call `pg_logical_emit_message(true, 'writerelay.v1', payload_text)`;
6. return the resulting `pg_lsn`;
7. use `SECURITY INVOKER`, not `SECURITY DEFINER`;
8. revoke default public execution on the wrapper function;
9. document the explicit `GRANT EXECUTE` required for an application role;
10. add comments to the schema and function.

A suitable shape is:

```sql
CREATE SCHEMA IF NOT EXISTS writerelay;

CREATE OR REPLACE FUNCTION writerelay.emit(event jsonb)
RETURNS pg_lsn
LANGUAGE plpgsql
VOLATILE
STRICT
SECURITY INVOKER
PARALLEL UNSAFE
AS $$
DECLARE
    payload_text text;
    payload_bytes integer;
BEGIN
    IF jsonb_typeof(event) <> 'object' THEN
        RAISE EXCEPTION 'WriteRelay event must be a JSON object';
    END IF;

    IF event->>'specversion' IS DISTINCT FROM '1.0' THEN
        RAISE EXCEPTION 'WriteRelay specversion must be 1.0';
    END IF;

    IF COALESCE(event->>'id', '') = '' THEN
        RAISE EXCEPTION 'WriteRelay event id is required';
    END IF;

    IF COALESCE(event->>'source', '') = '' THEN
        RAISE EXCEPTION 'WriteRelay event source is required';
    END IF;

    IF COALESCE(event->>'type', '') = '' THEN
        RAISE EXCEPTION 'WriteRelay event type is required';
    END IF;

    payload_text := event::text;
    payload_bytes := octet_length(convert_to(payload_text, 'UTF8'));

    IF payload_bytes > 262144 THEN
        RAISE EXCEPTION 'WriteRelay event exceeds 262144-byte limit';
    END IF;

    RETURN pg_logical_emit_message(
        true,
        'writerelay.v1',
        payload_text
    );
END;
$$;

REVOKE ALL ON FUNCTION writerelay.emit(jsonb) FROM PUBLIC;
```

Verify and adjust the exact SQL against the supported PostgreSQL versions. Do not blindly copy this skeleton without testing it.

Create a separate development script that creates:

- an application role;
- a replication role;
- the empty publication;
- appropriate grants;
- an example `orders` table.

Do not make the generic installation script create user accounts or hard-coded passwords.

---

# 9. Replication reader behavior

Use:

- `github.com/jackc/pglogrepl`;
- `github.com/jackc/pgx/v5/pgconn`;
- PostgreSQL's streaming replication protocol rather than polling `pg_logical_slot_get_changes`.

## 9.1 Connection handling

Parse the PostgreSQL configuration using `pgconn.ParseConfig`. Create replication mode by setting the appropriate replication runtime parameter rather than concatenating secrets into a new DSN string.

Never log the full DSN or password.

Use a separate normal SQL connection for setup and doctor checks when needed.

## 9.2 Slot handling

For development, `setup` may create a missing slot using `pgoutput` when explicitly requested.

For production behavior:

- do not drop or recreate an existing slot automatically;
- fail if a slot exists with a different output plugin;
- fail if the slot belongs to a different database;
- fail clearly if the slot is active elsewhere;
- report retained WAL and slot lag in `doctor` output;
- default automatic slot creation to false outside the development Compose configuration.

## 9.3 Transaction state machine

For protocol version 1, model the stream explicitly:

```text
Idle
  └── Begin(xid) → InTransaction

InTransaction
  ├── Message(prefix != writerelay.v1) → ignore
  ├── Message(prefix == writerelay.v1) → validate and append
  ├── DML/schema message → warn and ignore
  ├── Commit → persist accepted batch, then acknowledge
  └── connection loss → discard in-memory batch; PostgreSQL will replay
```

A nested `Begin`, `Commit` without `Begin`, or transactional WriteRelay message outside a transaction is a protocol error.

A transaction can contain zero accepted WriteRelay events. Before acknowledging it, persist the new durable transaction-end checkpoint in SQLite even though no event row is inserted. This keeps the local durable checkpoint aligned with every PostgreSQL acknowledgment.

## 9.4 Keepalive handling

When PostgreSQL requests a standby status reply, reply with the **last durably acknowledged spool LSN**, not PostgreSQL's current server WAL end and not the latest merely received WAL position.

Do not copy simplistic sample code that advances the client position to every `WALStart` or keepalive `ServerWALEnd`. That can acknowledge data that has not reached the durable spool.

## 9.5 Commit and acknowledgment sequence

For every completed transaction, including one containing zero accepted events:

```text
1. Receive Begin.
2. Buffer matching transactional messages in memory.
3. Receive Commit with commit and transaction-end LSNs.
4. Validate the complete batch.
5. Begin a SQLite transaction.
6. Insert or verify all event records in message order; this step is empty when the transaction has no accepted events.
7. Update local last-durable-commit metadata.
8. Commit SQLite with FULL synchronous durability.
9. Send PostgreSQL standby status with the durable transaction-end LSN.
10. Clear the in-memory transaction state.
```

If steps 5 through 8 fail, do not acknowledge the PostgreSQL transaction.

If step 9 fails after SQLite commits, a restart may replay the transaction. The unique identity and digest checks must make that safe.

---

# 10. SQLite spool

Create the spool behind an interface so its implementation can evolve without coupling the replication reader to SQL statements.

A reasonable initial interface is:

```go
type Spool interface {
    PersistCommittedBatch(
        ctx context.Context,
        batch CommittedBatch,
    ) (PersistResult, error)

    LastDurableLSN(ctx context.Context) (pglogrepl.LSN, error)
    Close() error
}
```

The exact API may improve during implementation. Keep it small and centered on transactional persistence.

## 10.1 Initial schema

Create embedded migrations rather than scattering `CREATE TABLE` statements through Go code.

A starting schema:

```sql
CREATE TABLE IF NOT EXISTS spool_metadata (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS events (
    sequence          INTEGER PRIMARY KEY AUTOINCREMENT,
    event_source      TEXT NOT NULL,
    event_id          TEXT NOT NULL,
    event_type        TEXT NOT NULL,
    subject           TEXT,
    payload            BLOB NOT NULL,
    payload_sha256     BLOB NOT NULL,
    transaction_id     INTEGER NOT NULL,
    message_lsn        TEXT NOT NULL,
    commit_lsn         TEXT NOT NULL,
    commit_end_lsn     TEXT NOT NULL,
    commit_time        TEXT NOT NULL,
    message_index      INTEGER NOT NULL,
    captured_at        TEXT NOT NULL,

    UNIQUE (event_source, event_id)
);

```

LSN text does not sort numerically in every representation, so do not create ordering logic around lexical LSN comparison. The monotonic SQLite `sequence` is the initial spool order. A later migration may store an integer representation of LSN as well.

## 10.2 Duplicate handling

When the unique key already exists:

1. load its stored digest;
2. compare it in constant time with the incoming digest;
3. if equal, count it as a replay and continue;
4. if different, return a typed integrity error;
5. do not overwrite the original payload.

The entire PostgreSQL transaction batch must be handled in one SQLite transaction.

## 10.3 Spool inspection

For Milestone 1, add either:

```text
writerelayd spool list --config writerelay.yaml
```

or a small internal test helper that allows integration tests to inspect captured rows. A user-facing inspect command is preferred if it does not distort the core work.

Do not implement event deletion yet. Retention begins after a delivery state machine exists.

## 10.4 Delivery schema and state

Schema migration version 2 adds durable sink identity and a composite delivery
record keyed by `(event_sequence, sink_id)`. New event and active-sink delivery
rows commit together. Registering a new sink backfills every existing event in
the same SQLite transaction.

Delivery states are:

```text
pending
  ├─ transient failure ─► retry_wait ─► pending attempt
  ├─ success ───────────► delivered
  └─ permanent/exhausted failure ─────► dead_letter

dead_letter ── explicit operator redrive ──► retry_wait
```

`delivered` and `dead_letter` are retained terminal states. There is no event
deletion in Milestone 2.

---

# 11. CLI and configuration

Prefer the Go standard library's `flag` package unless a compelling need for Cobra appears. Do not add a large CLI framework only for four commands.

Commands:

```text
writerelayd run --config ./writerelay.yaml
writerelayd doctor --config ./writerelay.yaml
writerelayd setup --config ./writerelay.yaml --create-slot
writerelayd spool list --config ./writerelay.yaml --limit 20
writerelayd spool deliveries --config ./writerelay.yaml --state dead_letter
writerelayd spool redrive --config ./writerelay.yaml --sink NAME --source SOURCE --id ID
writerelayd version
```

## 11.1 Example configuration

```yaml
version: 1

postgres:
  dsn_env: WRITERELAY_POSTGRES_DSN
  slot: writerelay_slot_v1
  publication: writerelay_publication
  message_prefix: writerelay.v1
  status_interval: 10s
  create_slot_if_missing: false

spool:
  path: ./data/writerelay.sqlite
  max_event_bytes: 262144

delivery:
  poll_interval: 1s
  request_timeout: 10s
  retry:
    initial_delay: 1s
    max_delay: 5m
    max_attempts: 10
  sinks: []

logging:
  level: info
  format: text
```

Configuration requirements:

- reject unknown YAML fields;
- validate `version`;
- require exactly one of `dsn` and `dsn_env` if both are supported;
- environment value overrides must never be printed;
- validate slot and publication names with a conservative PostgreSQL-identifier rule for version 0.1;
- require the prefix to be exactly `writerelay.v1` for now;
- parse and bound durations;
- create the spool parent directory with restrictive permissions when safe;
- provide actionable error messages.

`doctor` should check as much as possible without mutating state:

- database connectivity;
- PostgreSQL server version;
- `wal_level`;
- `max_replication_slots` and `max_wal_senders`;
- publication existence and whether it is empty;
- slot existence, plugin, database, active state, and `confirmed_flush_lsn`;
- replication privilege;
- SQL emit function existence;
- spool path writability and schema version;
- message-size configuration.

Do not print passwords or secret query parameters.

---

# 12. Repository layout

The implemented repository is organized around these stable package and
documentation boundaries:

```text
.
├── cmd/writerelayd/
├── internal/
│   ├── app/
│   ├── cli/
│   ├── config/
│   ├── delivery/
│   ├── event/
│   ├── logging/
│   ├── postgres/
│   └── spool/
├── sql/postgres/
├── examples/
├── tests/integration/
├── docs/
│   ├── adr/
│   ├── architecture.md
│   ├── correctness.md
│   ├── implementation-plan.md
│   ├── security-model.md
│   └── specification.md
├── .github/
│   ├── dependabot.yml
│   └── workflows/ci.yml
├── compose.yaml
├── writerelay.example.yaml
├── Dockerfile
├── go.mod
├── go.sum
├── LICENSE
├── Makefile
├── README.md
├── CONTRIBUTING.md
└── SECURITY.md
```

Use this single Go module:

```text
github.com/johnathondillon/write-relay
```

Keep the README, imports, examples, and release metadata synchronized with this
canonical module path.

Use Apache License 2.0 unless an existing repository license says otherwise.

---

# 13. Core types and package boundaries

Keep PostgreSQL protocol parsing, event validation, and durable storage independently testable.

## 13.1 Event validation

The event package should expose something similar to:

```go
type Metadata struct {
    SpecVersion string
    ID          string
    Source      string
    Type        string
    Subject     string
    Time        *time.Time
    DataContentType string
}

func Validate(payload []byte, maxBytes int) (Metadata, error)
```

Validation must not mutate or re-encode `payload`.

## 13.2 Committed batch

```go
type CommittedBatch struct {
    TransactionID uint32
    CommitLSN     pglogrepl.LSN
    CommitEndLSN  pglogrepl.LSN
    CommitTime    time.Time
    Events        []CapturedEvent
}
```

## 13.3 Replication handler

Prefer a callback that makes the acknowledgment boundary explicit:

```go
type BatchHandler interface {
    Persist(ctx context.Context, batch CommittedBatch) error
}
```

The replication reader may acknowledge `CommitEndLSN` only after `Persist` returns nil.

Avoid an API where the handler can asynchronously claim success before durability is established.

## 13.4 Typed errors

Define errors that callers and tests can distinguish:

- configuration error;
- unsupported PostgreSQL version;
- slot mismatch;
- protocol state error;
- malformed protected-prefix event;
- oversized event;
- event identity conflict;
- spool durability error.

Wrap errors with context while preserving `errors.Is` or `errors.As` behavior.

---

# 14. Correctness invariants

Put these in `docs/correctness.md` and make them visible in code reviews.

1. The protected SQL API always emits a transactional logical message.
2. The daemon accepts protected-prefix messages only when PostgreSQL marks them transactional.
3. A protected-prefix event is not persisted before its PostgreSQL `Commit` record.
4. An aborted or incomplete in-memory transaction is discarded on disconnect.
5. A replication acknowledgment never exceeds the last locally durable transaction-end LSN.
6. A keepalive response never substitutes the server WAL end for the durable local LSN.
7. A SQLite batch is atomic: either every accepted event in the PostgreSQL transaction is represented or none is.
8. Replay of identical `(source, id, digest)` is harmless.
9. Reuse of `(source, id)` with different content stops progress rather than silently overwriting data.
10. Logs never contain PostgreSQL passwords or full secret DSNs.
11. The project never presents at-least-once delivery as exactly once.
12. Shutdown is graceful: stop receiving new work, finish or roll back the current SQLite transaction, send no speculative acknowledgment, and close resources.
13. Every new event and all active-sink delivery rows commit atomically.
14. Pending or retrying earlier events block later events for that sink.
15. A destination success is not terminal until SQLite records `delivered`.
16. An unrecorded destination success remains eligible with the same idempotency
    key and may be delivered more than once.
17. Sink target changes cannot silently reuse a durable sink name.
18. Redirects never forward webhook credentials.

---

# 15. Milestone 0 — Repository scaffold

Complete all items in this milestone.

## Deliverables

- Initialize the Go module.
- Create the repository structure.
- Add Apache-2.0 license unless an existing license conflicts.
- Add `.editorconfig`, `.gitignore`, `Makefile`, and CI workflow.
- Add a minimal multi-stage `Dockerfile`.
- Add `compose.yaml` with PostgreSQL configured for logical replication.
- Add strict configuration loading and tests.
- Add CLI command routing and `version` command.
- Add `README.md`, `CONTRIBUTING.md`, and `SECURITY.md`.
- Add architecture, correctness, security, implementation-plan, and ADR documents.
- Add structured logging setup using `log/slog`.
- Ensure `go test ./...` succeeds before moving to Milestone 1.

## Compose requirements

Use a currently supported PostgreSQL image, preferably PostgreSQL 18 for local development. Configure at least:

```text
wal_level=logical
max_replication_slots=10
max_wal_senders=10
```

Add a health check. Use clearly development-only credentials. Do not expose those credentials as production examples.

The Makefile should offer discoverable commands such as:

```text
make fmt
make test
make vet
make lint
make build
make postgres-up
make postgres-down
make integration
make check
```

Commands should fail when their underlying task fails.

---

# 16. Milestone 1 — Transactional capture to durable spool

Milestone 1 is complete. This section remains the capture contract.

## 16.1 SQL and setup

- Implement and test `sql/postgres/001_install.sql`.
- Create the empty publication in development setup.
- Create development application and replication roles with least-privilege grants.
- Add committed and rolled-back SQL examples.
- Add `setup` behavior that validates before mutating and never drops objects automatically.

## 16.2 Event package

- Implement envelope validation.
- Preserve raw bytes.
- Enforce maximum size.
- Allow unknown extension attributes.
- Add table-driven unit tests for valid and invalid envelopes.

## 16.3 Decoder and state machine

- Parse protocol version 1 messages.
- Handle `Begin`, logical-decoding `Message`, `Commit`, primary keepalive, and XLogData wrappers.
- Buffer matching messages until commit.
- Ignore unrelated prefixes.
- Warn on unexpected DML/schema records.
- Reject protected-prefix non-transactional messages.
- Detect invalid state transitions.
- Add unit tests using encoded or constructed `pglogrepl` messages where practical.

## 16.4 SQLite spool

- Add embedded migrations and schema-version tracking.
- Configure durability pragmas.
- Persist a committed batch atomically.
- Store identity, digest, payload, transaction metadata, LSN metadata, commit time, and message order.
- Implement identical-replay handling.
- Implement conflicting-identity detection.
- Store the last durable transaction-end LSN in the same SQLite transaction as the batch.
- Add unit tests for inserts, replay, conflicts, rollback on error, and reopening the database.

## 16.5 Replication loop

- Connect using replication mode.
- Start `pgoutput` with protocol version 1, the empty publication, and `messages=true`.
- Maintain the last durable LSN separately from received/server positions.
- Persist the batch before sending standby status.
- Reply to keepalives using only the durable LSN.
- Use context cancellation and graceful shutdown.
- Add reconnect behavior with bounded exponential backoff and jitter.
- Do not acknowledge a failed batch.

Keep reconnect behavior simple and observable. Do not create a broad resilience framework.

## 16.6 Doctor and inspection

- Implement useful `doctor` checks.
- Provide a way to inspect the captured spool records.
- Redact secrets.

## 16.7 End-to-end integration tests

At minimum, prove:

### Test A — Committed event is captured

1. Start PostgreSQL with logical replication enabled.
2. Install the SQL function and empty publication.
3. Create a logical slot using `pgoutput`.
4. Start the daemon against an empty temporary spool.
5. Begin a SQL transaction.
6. Update or insert an example order.
7. Emit a valid event.
8. Commit.
9. Assert one matching event appears in the spool.
10. Assert the stored payload and metadata are correct.

### Test B — Rolled-back event is absent

1. Begin a SQL transaction.
2. Update or insert an example order.
3. Emit a valid event.
4. Roll back.
5. Wait for a bounded period.
6. Assert no event with that identity appears in the spool.

### Test C — Multiple events preserve order

1. Emit two events in one transaction.
2. Commit.
3. Assert their spool sequence follows message order.
4. Assert they share transaction and commit metadata.

### Test D — Identical replay is idempotent

This may be implemented as a focused spool test in Milestone 1 if deterministic PostgreSQL crash injection is not ready.

1. Persist a batch.
2. Persist the exact batch again.
3. Assert no duplicate event row appears.
4. Assert the operation succeeds as a replay.

### Test E — Conflicting identity stops progress

1. Persist an event.
2. Attempt to persist the same `(source, id)` with different bytes.
3. Assert a typed integrity error.
4. Assert the original payload remains unchanged.

Use bounded polling helpers in integration tests. Do not use arbitrary long sleeps as the primary synchronization mechanism.

---

# 17. Milestone 1 acceptance criteria

Milestone 1 is complete only when all of the following are true:

- [x] `go build ./...` passes.
- [x] `go test ./...` passes.
- [x] `go vet ./...` passes.
- [x] Formatting checks pass.
- [x] Docker Compose starts a healthy PostgreSQL instance with logical replication enabled.
- [x] `writerelayd doctor` gives a useful pass/fail report without exposing secrets.
- [x] The SQL installation script succeeds on the local target PostgreSQL version.
- [x] The empty publication exists and contains no tables.
- [x] The replication slot uses `pgoutput`.
- [x] A committed event reaches SQLite.
- [x] A rolled-back event does not reach SQLite.
- [x] Multiple events in one transaction retain message order.
- [x] SQLite commits before the daemon advances the durable acknowledged LSN.
- [x] Keepalive replies do not acknowledge the server WAL end speculatively.
- [x] Replaying identical event content does not duplicate it.
- [x] Conflicting content for the same identity raises a typed error.
- [x] Graceful shutdown leaves the spool valid and does not acknowledge unpersisted data.
- [x] README quick-start commands have been executed or explicitly marked unverified.
- [x] `docs/implementation-plan.md` accurately reflects completed and remaining work.

Do not mark the milestone complete merely because package skeletons exist.

---

# 18. Delivery, recovery, and later roadmap

## Milestone 2 — Delivery engine

- [x] Add `Sink` interface.
- [x] Add durable per-sink delivery records.
- [x] Add ordered single-worker dispatcher.
- [x] Add stdout development sink.
- [x] Add HTTP webhook sink.
- [x] Add exponential retry, bounded `Retry-After`, timeout, and dead-letter state.
- [x] Add delivery inspection and explicit dead-letter redrive.
- [x] Do not delete events; retain both terminal states for a later retention
  milestone.

## Milestone 3 — Failure injection and recovery

- [x] Terminate before the SQLite transaction.
- [x] Terminate midway through batch insertion.
- [x] Terminate after SQLite commit but before PostgreSQL acknowledgment.
- [x] Terminate immediately after PostgreSQL acknowledgment.
- [x] Terminate before a sink request.
- [x] Kill the process while a sink request is in flight.
- [x] Terminate after destination acceptance but before the local success mark.
- [x] Reopen the same spool and prove rollback, replay, retry, order, and stable
  idempotency behavior.
- [x] Keep all hooks inert and inaccessible to production configuration.

The tests run ordinary component code in child test processes and terminate
without deferred cleanup. A parent process reopens the SQLite file and asserts
durable state. In-flight and accepted-but-unrecorded requests retry with the same
idempotency key and may be duplicated, preserving the at-least-once contract.

## Milestone 4 — Producer SDKs and protocol conformance

- Go producer SDK.
- C# producer SDK.
- TypeScript/Node producer SDK.
- SQL-only integration remains first-class.
- CloudEvents conformance tests.
- Event ID helper with caller-controlled retry identity.
- Consumer inbox/idempotency helper as a separate package.

## Milestone 5 — Production sinks and observability

- SQS.
- NATS JetStream.
- Kafka.
- OpenTelemetry traces and metrics.
- Prometheus-compatible metrics endpoint.
- Health and readiness endpoints.
- Spool size limits and explicit backpressure policy.
- Retained-WAL alerts.

## Milestone 6 — Benchmark suite

Compare reproducibly:

1. outbox table plus polling;
2. outbox table plus Debezium;
3. transactional logical messages plus WriteRelay.

Measure:

- transactions per second;
- transaction p50 and p99 latency;
- PostgreSQL CPU;
- WAL bytes per event;
- database disk growth;
- event capture latency;
- retained WAL during downstream failure;
- recovery after relay crashes;
- duplicate rate under injected failures;
- event sizes such as 1 KiB, 10 KiB, 100 KiB, and 256 KiB.

Publish negative results as well as favorable ones.

## Milestone 7 — Other database durability engines

Only after the PostgreSQL implementation is stable, explore a database-neutral protocol with database-specific engines:

- MySQL: likely transactional outbox plus binlog capture unless a safe native primitive is identified;
- SQL Server: Service Broker-backed transactional handoff;
- other databases only with a clear, testable durability model.

Do not force every database into PostgreSQL's implementation model.

---

# 19. Security model

Create `docs/security-model.md` and cover at least:

## Database roles

- The application role receives only the privileges needed for business tables and explicit `EXECUTE` on `writerelay.emit(jsonb)`.
- The replication user has `LOGIN` and `REPLICATION`, database connectivity, and only the additional privileges required by PostgreSQL.
- Do not run the daemon as a PostgreSQL superuser in production examples.
- Development Compose credentials must be labeled development-only.

## Event content

- Event payloads can contain sensitive business data.
- Logs must include event IDs and types, not full payloads by default.
- The spool contains durable event payloads and must be protected like application data.
- Create the spool directory and file with restrictive permissions where supported.
- Do not expose a spool inspection HTTP endpoint in version 0.1.

## Configuration

- Read secrets from environment variables or a future secret-provider integration.
- Redact DSNs and headers from logs and errors.
- Validate config paths and avoid following surprising symlink behavior when creating files.

## Denial-of-service controls

- Enforce an event-size limit in both SQL and the daemon.
- Bound in-memory transaction buffering.
- If one transaction exceeds a configured event-count or byte limit, stop with a clear error and do not acknowledge it.
- Set reasonable database and network timeouts.
- A future spool-size limit must fail closed rather than discard accepted events silently.

## Supply chain

- Keep dependencies minimal.
- Pin CI actions to immutable commit SHAs.
- Run a pinned `govulncheck` version in CI.
- Generate an SBOM in a later release.

---

# 20. Engineering standards

## Go

- Use Go 1.26.5 or newer language and module settings.
- Use `context.Context` for blocking I/O and lifecycle cancellation.
- Use `log/slog` for structured logs.
- Use `%w` for error wrapping.
- Avoid package globals except immutable build metadata.
- Keep interfaces narrow and consumer-defined.
- Prefer standard library functionality over new dependencies.
- Run `gofmt` or `goimports` consistently.
- Ensure code works with the race detector where practical.

## Dependencies initially allowed

Keep the initial dependency set close to:

- `github.com/jackc/pglogrepl`;
- `github.com/jackc/pgx/v5`;
- `gopkg.in/yaml.v3`;
- a maintained SQLite driver that supports the project's no-CGO or deployment decision;
- a small assertion library only if it materially improves tests.

Document the SQLite driver choice in an ADR. Prefer a cross-platform build without requiring local C toolchains, but verify performance and maintenance status rather than choosing solely for convenience.

Do not add an ORM.

## SQL

- Keep migrations rerunnable where practical.
- Use explicit transactions for local migrations.
- Do not interpolate unvalidated identifiers.
- Comment security-sensitive grants and revokes.
- Test against every PostgreSQL major version in the declared support matrix before a stable release.

## Tests

- Unit tests must not depend on Docker.
- Integration tests must be separately identifiable and skippable with a clear reason.
- Use temporary directories and databases.
- Never share a fixed spool path between parallel tests.
- Use deterministic IDs in tests.
- Avoid flaky timing assumptions.

---

# 21. Expected README quick start

The README must keep the Milestone 1 capture proof executable and include
Milestone 2 sink configuration, delivery inspection, and redrive.

Example shape:

```bash
cp writerelay.example.yaml writerelay.yaml
export WRITERELAY_POSTGRES_DSN='postgres://writerelay_repl:dev-repl-password@localhost:5432/writerelay?sslmode=disable'

make postgres-up
make setup

go run ./cmd/writerelayd doctor --config ./writerelay.yaml
go run ./cmd/writerelayd run --config ./writerelay.yaml
```

In another terminal:

```bash
psql 'postgres://writerelay_app:dev-app-password@localhost:5432/writerelay?sslmode=disable' \
  -f examples/commit.sql
```

Then inspect the spool:

```bash
go run ./cmd/writerelayd spool list \
  --config ./writerelay.yaml \
  --limit 20
```

Run the rollback example and demonstrate that its ID does not appear:

```bash
psql 'postgres://writerelay_app:dev-app-password@localhost:5432/writerelay?sslmode=disable' \
  -f examples/rollback.sql
```

The actual commands may differ, but keep the happy path similarly small and executable.

---

# 22. Questions the implementation must answer with tests or documentation

Do not leave these as vague assumptions:

1. Does the selected `pglogrepl` version decode protected logical messages correctly with `pgoutput`, `messages=true`, and protocol version 1?
2. Does an empty publication work for logical messages on every supported PostgreSQL major version?
3. What exact LSN is used for the durable ACK: commit LSN or transaction-end LSN, and why?
4. How does the daemon obtain its initial start LSN for a new versus existing slot?
5. What happens when SQLite commits and the PostgreSQL status update is lost?
6. What happens when a malformed protected-prefix event blocks the slot?
7. How is spool corruption surfaced?
8. How are credentials redacted from nested errors?
9. What is the maximum number and total bytes of events allowed in one PostgreSQL transaction?
10. Which managed PostgreSQL services permit the required replication settings and privileges? This can remain future research, but do not imply universal support.

Record verified answers in the relevant ADR or correctness document.

---

# 23. Primary technical references

Use primary documentation and source code when resolving implementation details.

- PostgreSQL logical message function:  
  https://www.postgresql.org/docs/current/functions-admin.html

- PostgreSQL logical streaming replication protocol and `pgoutput` options:  
  https://www.postgresql.org/docs/current/protocol-logical-replication.html

- PostgreSQL logical replication message formats:  
  https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html

- PostgreSQL streaming replication protocol and standby status updates:  
  https://www.postgresql.org/docs/current/protocol-replication.html

- PostgreSQL logical decoding concepts:  
  https://www.postgresql.org/docs/current/logicaldecoding.html

- PostgreSQL `CREATE PUBLICATION`, including empty publications:  
  https://www.postgresql.org/docs/current/sql-createpublication.html

- PostgreSQL replication settings:  
  https://www.postgresql.org/docs/current/runtime-config-replication.html

- `pglogrepl` source and examples:  
  https://github.com/jackc/pglogrepl

- CloudEvents specification repository:  
  https://github.com/cloudevents/spec

- Go release history:  
  https://go.dev/doc/devel/release

When documentation and a sample differ, treat PostgreSQL's protocol documentation and verified integration tests as authoritative. Sample replication clients often advance LSNs optimistically for demonstration purposes; do not copy that behavior into this project's durability path.
