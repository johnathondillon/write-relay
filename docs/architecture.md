# Architecture

## Scope

Milestones 1 through 3 capture explicit application-defined events from one
PostgreSQL database and replication slot into one local SQLite spool, then
deliver them to configured stdout or HTTP webhook sinks, and prove the critical
recovery boundaries under real child-process termination. They do not capture
general table changes.

```text
application transaction
  ├─ business SQL
  └─ writerelay.emit(jsonb)
          │ transactional logical message
          ▼
PostgreSQL WAL / pgoutput protocol v1
          │ Begin → Message* → Commit
          ▼
Go transaction state machine
          │ complete committed batch
          ▼
SQLite transaction
  ├─ insert or verify every event
  ├─ create delivery row per active sink
  └─ update last_durable_lsn = CommitEndLSN
          │ FULL synchronous commit succeeds
          ▼
standby status at last_durable_lsn
          │
          ▼
oldest non-terminal delivery for each sink
          ▼
single delivery worker
          │
          ├─ 2xx / stdout write ────────────────► delivered
          ├─ transient / attempts remain ──────► retry_wait
          └─ permanent / attempts exhausted ───► dead_letter
```

## Package boundaries

- `internal/event` parses and validates an event without re-encoding its bytes.
- `internal/postgres` owns slot validation, protocol decoding, transaction state,
  keepalives, reconnects, setup, and doctor checks.
- `internal/delivery` owns the sink interface, ordered worker, retry decisions,
  stable webhook identity, HTTP behavior, signatures, and stdout development
  sink.
- `internal/failure` contains inert hook boundaries. Only tests inject
  functions; production composition uses the zero value.
- `internal/spool` defines capture metadata and the small persistence interface.
- `internal/spool/sqlite` owns embedded migrations, SQLite durability, identity
  replay checks, checkpoints, sink registration, delivery state, inspection,
  and redrive.
- `internal/app` composes the capture and delivery lifecycles over one spool.
- `internal/cli` composes commands without a large CLI framework.
- `sql/postgres` is both the administrator-facing SQL asset and the embedded
  source used by `setup`.

## Protocol and transaction state

The daemon starts `pgoutput` with `proto_version '1'`, an exact validated
publication name, and `messages 'true'`. The configured publication must be
empty. Relation, DML, and schema messages are ignored with warnings because
their presence indicates drift.

The state machine accepts one `Begin`, buffers transactional
`writerelay.v1` messages, and produces one batch on `Commit`. A nested begin,
commit without begin, protected non-transactional message, malformed protected
event, or configured transaction-limit violation is fatal and blocks the slot.
Disconnect discards the in-memory transaction; reconnect begins at the last
durable local checkpoint so PostgreSQL replays incomplete work.

Transactions with no accepted events still commit a new SQLite checkpoint
before acknowledgment.

## LSN selection and acknowledgment

The durable acknowledgment is `CommitMessage.TransactionEndLSN`, called
`CommitEndLSN` in the spool. `CommitLSN` identifies the commit record;
transaction-end LSN is the first position after the transaction and is the
correct flush boundary to report after all its data is durable.

For an empty local spool, capture starts at the slot's
`confirmed_flush_lsn`, falling back to `restart_lsn`. For an existing spool, its
`last_durable_lsn` is authoritative. A slot that is missing, active elsewhere,
owned by another database, or not using `pgoutput` fails validation. Development
may create a missing slot only when explicitly enabled.

`WALStart`, keepalive `ServerWALEnd`, and merely received messages never advance
the acknowledgment. Periodic and requested keepalive replies use the same
durable LSN for write, flush, and apply positions.

## Crash and replay behavior

If SQLite fails before commit, no status update is sent. If SQLite commits but
the status update is lost, reconnect resumes from the local checkpoint or
PostgreSQL may replay already-seen content. The unique key `(event_source,
event_id)` and SHA-256 digest make identical replay harmless. A different digest
for the same identity rolls back the complete SQLite batch and stops progress.

If a destination accepts a request but SQLite does not record `delivered`, the
same delivery remains non-terminal and is attempted again after restart. The
webhook idempotency key remains stable across attempts, but destinations are
still responsible for deduplication. This is the unavoidable at-least-once
crash window.

## Deterministic process-crash testing

Failure tests launch the ordinary spool, acknowledgment, webhook, and worker
code inside child test processes. Injected hooks call `os.Exit` at deterministic
boundaries, so deferred `Close` and rollback calls do not run. The parent opens
the same SQLite file and verifies recovered state.

```text
before SQLite transaction ──────────────► no batch / no checkpoint
after first insert, before commit ──────► no partial batch / no deliveries
after SQLite commit, before ACK ────────► batch durable / replay harmless
immediately after ACK ──────────────────► durable checkpoint equals ACK
before webhook request ─────────────────► no call / delivery pending
request received, response withheld ────► ambiguous call / delivery pending
after 2xx, before local success ─────────► accepted call / delivery pending
restart ambiguous delivery ─────────────► same idempotency key / possible duplicate
```

For the in-flight case, the parent destination confirms receipt and deliberately
withholds its response before killing the child. This proves the ambiguity
without relying on a returned network error. Recovery retries the oldest pending
event while later events for that sink remain blocked.

There is deliberately no runtime failpoint registry. Tests pass hook functions
through alternate internal constructors; the public configuration and
production CLI cannot select or activate them.

SQLite sequence, not textual LSN order, defines spool order. Events are inserted
in PostgreSQL commit order and their accepted-message order within a transaction.

## Durability implementation

SQLite uses the CGO-free `modernc.org/sqlite` driver, one connection/writer, WAL
journaling, `synchronous=FULL`, foreign keys, and a five-second busy timeout.
Schema migrations are embedded and applied sequentially. Version 2 adds durable
sink identities and per-event delivery records. An event insert and all delivery
records for active sinks commit in the capture transaction. A newly configured
sink is registered and backfilled for existing events in one SQLite transaction.
The spool directory and file are created with restrictive permissions, and an
existing symlink at the spool path is rejected.

## Delivery state and ordering

Delivery states are `pending`, `retry_wait`, `delivered`, and `dead_letter`.
The latter two are retained terminal states. There is deliberately no transient
durable `in_flight` state: an unrecorded attempt remains eligible, so a process
crash cannot strand it.

One worker issues one request at a time. For each active sink, a later event is
not eligible while an earlier event remains `pending` or `retry_wait`. A delayed
retry blocks later events for that sink but does not block another sink.
Dead-lettering is terminal for ordering, so later events can proceed while the
failed record remains available for inspection and explicit redrive.

Sink name is durable identity. Its type and non-secret target fingerprint cannot
change in place; operators use a new name for a new destination. Removing a
sink with non-terminal deliveries is rejected. Re-enabling the same durable
sink backfills events captured while it was inactive.

Webhook delivery sends the raw structured event with a stable idempotency key.
Redirects are disabled to prevent credential forwarding. Optional authorization
and signing material is resolved from environment variables and never stored in
SQLite. The worker retries network failures, `408`, `425`, `429`, and `5xx`,
honoring `Retry-After` only within the configured maximum delay. Other responses
dead-letter immediately.

## Operational behavior

Transient replication connection failures reconnect with bounded exponential
backoff and jitter. Protocol, identity, slot, and spool-durability failures are
fatal so the daemon cannot silently skip an event. Destination failures are
durable delivery outcomes rather than daemon failures. Context cancellation
discards uncommitted capture state, sends no speculative ACK, stops requesting
new deliveries, records a completed request outcome when possible, and closes
resources.
