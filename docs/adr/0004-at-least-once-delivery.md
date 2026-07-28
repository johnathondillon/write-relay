# ADR 0004: At-least-once replay and stable identity

Status: accepted

A crash after SQLite commit but before PostgreSQL records the status update can
cause replay. Event identity is `(source, id)`, not `id` alone. The spool stores
SHA-256 over the exact bytes received from PostgreSQL.

An existing identity with the same digest is a harmless replay. A different
digest is a typed integrity failure that rolls back the batch, preserves the
original bytes, and prevents acknowledgment.

This supports at-least-once processing. It does not make an exactly-once
end-to-end claim, and future consumers still need idempotency.

