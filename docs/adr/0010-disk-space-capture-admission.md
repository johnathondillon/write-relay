# ADR 0010: Pause capture before disk pressure becomes a durability failure

Status: accepted

An extended receiver outage can grow the SQLite spool. Add optional filesystem
available-byte thresholds under `spool.disk_space`, disabled by default. Use
unprivileged available blocks from the spool file's filesystem on Linux/macOS.
This is a capture admission policy, not a reservation, hard file-size cap, or
payload deletion policy.

Check before replication startup, periodically during receive, and forcibly
before every complete batch is passed to SQLite. The last check includes
zero-event checkpoint batches. Below the pause threshold, or when the probe
fails, close the stream, discard unpersisted memory, and wait without terminating
the independent delivery worker. Resume only at the higher recovery threshold.
Hysteresis is in memory; a new process makes a fresh initial threshold decision.

Closing the stream bounds memory and avoids having to consume an unbounded WAL
stream while withholding persistence. Reconnect uses the existing durable local
checkpoint, so outstanding transactions replay through the existing identity and
content checks. Keepalive positions never substitute for the durable checkpoint.
The guard does not alter persist-before-ACK ordering or SQLite failure handling.
A real durability failure remains fatal rather than becoming an automatic retry.

Status reads are independent of the filesystem syscall. Monitoring reports pause
reasons and threshold/available-byte gauges, omits failed/stale available readings,
and makes readiness fail during a pause or stale enabled disk measurement.
Liveness remains true unless the daemon is shutting down. A probe error is
reported using a fixed reason, without raw OS error/path content. Transition logs
remain available when HTTP monitoring is disabled.

Pausing moves backlog retention to PostgreSQL WAL. Operators must separately
monitor slot-retained WAL, database-host free space, and slot validity. If the
server discards required WAL, the local guard cannot reconstruct it. The runbook
provides the existing doctor command and a PostgreSQL monitoring query.

Measurements cannot predict all batch/WAL growth or reserve space against other
processes. Spool initialization, migrations, sink backfill, delivery updates,
and maintenance remain outside this admission check. Successful deliveries do
not automatically free payloads, and pruning makes SQLite space reusable without
guaranteeing more filesystem free space. Configure meaningful reserve headroom.

Tests cover hysteresis and probe errors; force a fresh pre-persist check and prove
no batch write or ACK occurs while blocked; terminate a paused child and verify
safe replay; and exercise pause, ongoing delivery, same-spool restart, checkpoint
and ACK stability, ordered recovery, and probe-error recovery on PostgreSQL 14–18.
The test-only injected probe has no production configuration or environment hook.
