# ADR 0001: Transactional logical messages

Status: accepted

WriteRelay transports explicit domain events through
`pg_logical_emit_message(true, 'writerelay.v1', payload)`, wrapped by a
`SECURITY INVOKER` SQL function. It does not create or poll an outbox table.

The transactional flag makes event creation commit or roll back with business
SQL. The protected prefix is reserved: a non-transactional message using it is a
fatal protocol error. Other prefixes are ignored.

This is PostgreSQL-specific and intentionally narrower than general CDC.

