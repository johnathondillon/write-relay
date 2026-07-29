# ADR 0005: Durable per-sink state and ordered at-least-once delivery

Status: accepted

Each active sink receives a durable delivery row for every captured event. New
event and delivery rows commit in the same SQLite transaction. Registering a new
sink backfills existing events in one transaction. Sink name is durable identity;
changing its type or non-secret target requires a new name.

Delivery states are `pending`, `retry_wait`, `delivered`, and `dead_letter`.
`delivered` and `dead_letter` are retained terminal states. Removing a sink with
non-terminal deliveries is rejected, and event deletion remains out of scope
until an explicit retention policy exists.

One worker sends one event at a time. It preserves event sequence independently
for each sink: a pending or delayed earlier event blocks later events for that
sink without blocking other sinks. A terminal dead letter allows later events
to proceed and can be explicitly redriven.

There is no durable `in_flight` state. The worker sends while the row remains
non-terminal and marks the result afterward. If a process crashes after a
destination accepts the request but before SQLite records success, the same row
is attempted again. Webhooks receive a stable idempotency key derived from sink
name plus event `(source, id)`, but this mitigates rather than eliminates
duplicates: destination idempotency remains required.

Transient network errors, HTTP `408`, `425`, `429`, and `5xx` retry with bounded
exponential delay. A valid `Retry-After` can increase that delay only up to the
configured maximum. Other HTTP responses dead-letter immediately; retriable
failures dead-letter when the attempt limit is exhausted.

Webhook redirects are disabled. Optional authorization and HMAC signing secrets
come from environment variables and are not stored in the spool. The stdout
sink is for development because it exposes complete event payloads.
