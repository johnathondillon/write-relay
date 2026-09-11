# ADR 0005: Durable per-sink state and ordered at-least-once delivery

Status: accepted

Each active sink receives a durable delivery row for every captured event. New
event and delivery rows commit in the same SQLite transaction. Registering a new
sink backfills existing events in one transaction. Sink name is durable identity;
changing its type or non-secret target requires a new name.

ADR 0007 later narrows backfill to events with retained payloads and adds manual
payload-only retention. Event identity and all delivery rows remain retained.

Delivery states are `pending`, `retry_wait`, `delivered`, and `dead_letter`.
`delivered` and `dead_letter` are retained terminal states. Removing a sink with
non-terminal deliveries is rejected, and event deletion remains out of scope
until an explicit retention policy exists.

One worker sends one event at a time. It preserves event sequence independently
for each sink: a pending or delayed earlier event blocks later events for that
sink without blocking other sinks. A terminal dead letter allows later events
to proceed and can be explicitly redriven.

Delivery selection first finds the oldest non-terminal event for each active
sink using the existing sink/sequence index, then checks whether that event is
due. Eligible heads are ordered by event sequence and sink ID. This replaces a
predecessor check repeated for every due backlog row, which caused a 10,000-event
load/recovery run to exceed its five-minute deadline. Selecting only due rows
inside the per-sink lookup would incorrectly bypass delayed retries. No schema,
durability, or state-transition change is needed. Retained terminal history can
still require scanning within a sink; this is not a constant-time queue claim.

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
