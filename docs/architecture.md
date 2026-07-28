# Architecture

## Scope

Milestone 1 captures explicit application-defined events from one PostgreSQL
database and replication slot into one local SQLite spool. It does not capture
general table changes or deliver to external systems.

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
  └─ update last_durable_lsn = CommitEndLSN
          │ FULL synchronous commit succeeds
          ▼
standby status at last_durable_lsn
```

## Package boundaries

- `internal/event` parses and validates an event without re-encoding its bytes.
- `internal/postgres` owns slot validation, protocol decoding, transaction state,
  keepalives, reconnects, setup, and doctor checks.
- `internal/spool` defines capture metadata and the small persistence interface.
- `internal/spool/sqlite` owns embedded migrations, SQLite durability, identity
  replay checks, checkpoints, and inspection.
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

SQLite sequence, not textual LSN order, defines spool order. Events are inserted
in PostgreSQL commit order and their accepted-message order within a transaction.

## Durability implementation

SQLite uses the CGO-free `modernc.org/sqlite` driver, one connection/writer, WAL
journaling, `synchronous=FULL`, foreign keys, and a five-second busy timeout.
Schema migration version 1 is embedded. The spool directory and file are created
with restrictive permissions, and an existing symlink at the spool path is
rejected.

## Operational behavior

Transient replication connection failures reconnect with bounded exponential
backoff and jitter. Protocol, identity, slot, and spool-durability failures are
fatal so the daemon cannot silently skip an event. Context cancellation discards
uncommitted in-memory state, sends no speculative ACK, and closes resources.

