# Correctness model

## Invariants

1. The protected SQL API always passes `true` to
   `pg_logical_emit_message`.
2. The daemon accepts `writerelay.v1` only when PostgreSQL marks the message
   transactional.
3. A protected event is not persisted before its PostgreSQL `Commit`.
4. Disconnect or shutdown discards an incomplete in-memory transaction.
5. An acknowledgment never exceeds the last locally durable transaction-end LSN.
6. Keepalive server WAL positions never substitute for the durable local LSN.
7. Every accepted event and the checkpoint for a PostgreSQL transaction commit
   atomically in one SQLite transaction.
8. Identical `(source, id, digest)` replay is harmless.
9. Reusing `(source, id)` with different bytes stops progress and preserves the
   original payload.
10. Logs and user-facing connection errors contain no passwords or full DSNs.
11. At-least-once behavior is never presented as exactly once.
12. Graceful shutdown persists no incomplete PostgreSQL transaction and sends no
    speculative acknowledgment.

## Answers required by Milestone 1

1. The pinned `pglogrepl` decodes the protocol-v1 `M` logical message into
   transactional flag, message LSN, prefix, and raw content. A constructed-wire
   unit test and PostgreSQL 18 integration test exercise this with
   `pgoutput`/`messages=true`.
2. PostgreSQL documents that `CREATE PUBLICATION name` creates an empty
   publication. The integration test verifies this on PostgreSQL 18. PostgreSQL
   14–17 remain to be added to the CI matrix; support is not yet empirically
   claimed for every environment.
3. The ACK uses transaction-end LSN, not commit LSN. It is the boundary after
   the completed transaction and is stored atomically with the batch.
4. A new/empty spool starts from the slot's confirmed flush LSN, falling back to
   restart LSN. A reopened spool starts from its local durable checkpoint.
5. If SQLite commits and status transmission is lost, reconnect/replay is safe:
   identical identity and digest do not create another row.
6. A malformed, oversized, non-transactional, or over-limit protected event is
   fatal. The slot remains behind the offending transaction for operator action.
7. SQLite open, pragma, migration, query, parse, and commit failures are surfaced
   with context. Unsupported schema versions and invalid checkpoint LSNs fail
   startup; the daemon does not ACK through corruption.
8. Configuration never prints resolved environment values. Connection errors
   expose a PostgreSQL message and SQLSTATE when safe, otherwise a redacted
   generic message rather than nesting a DSN-bearing error.
9. Defaults are 10,000 accepted events and 8 MiB of accepted event bytes per
   PostgreSQL transaction, independently of the 256 KiB per-event cap. Both
   transaction bounds are validated and configurable.
10. Managed-service compatibility is future research. No universal managed
    PostgreSQL support is implied.

## Test evidence

Unit tests cover validation, state transitions, raw logical-message decoding,
batch durability before ACK, keepalive status positions, atomic spool rollback,
reopen, identical replay, conflicting identity, and zero-event checkpoints.

The tagged integration test uses bounded polling and a committed marker after a
rollback. Seeing the marker before checking absence proves the rolled-back
identity was not merely delayed.

