# ADR 0007: Prune delivered payloads while retaining replay identity

Status: accepted

The spool needs a manual retention operation without losing replay protection.
Deleting entire event rows would allow a previously captured identity to appear
new on replay, and would discard the digest needed to detect conflicting content.

Schema 3 adds `events.payload_pruned_at`. Pruning replaces `payload` with an empty
blob and records that timestamp atomically, retaining the event sequence, source,
ID, original SHA-256, metadata, every delivery row, and the durable checkpoint.
Identical replay still succeeds without restoring bytes; conflicting replay
still stops capture. Payload pruning is irreversible through the application.

An event is eligible only when it has at least one delivery and **all** of its
associated deliveries, including inactive sinks, are `delivered` with a success
timestamp strictly before the operator's explicit cutoff. Pending work, retry
waits, dead letters, null success times, and capture-only events are excluded.
Selection is ordered by event sequence and limited to 1–1000 events per command.

`spool prune --dry-run` opens the existing current-schema database with `mode=ro`
and reads one snapshot. Apply opens it with `mode=rw`, verifies WAL mode, uses
`synchronous=FULL`, and acquires `BEGIN IMMEDIATE` before selection. Eligibility
and updates share that write transaction, serializing against capture, sink
registration, and delivery changes. A preview does not reserve a batch; apply
re-evaluates current durable state. Errors and cancellation roll the batch back.
Inert test hooks terminate a child process after its first payload update and
after commit to prove all-or-nothing recovery.

New sink registration and reactivation backfill only retained payloads. Replay
of a pruned identity never creates additional deliveries. Database triggers
reject pruning unfinished deliveries, restoring or changing a pruned payload or
its digest, and creating or rescheduling deliveries for pruned payloads. These
guards also make stale write paths fail closed instead of sending empty payloads.

This narrows the backfill behavior in ADR 0005: a new sink receives retained
history, not all historical events after pruning. Operators needing old history
must register that sink before pruning or keep an external archive.

Normal spool initialization migrates schemas 1/2 to 3 without pruning data.
The prune command never creates or migrates a spool. Stop the old daemon before
upgrading, then restart the new binary to migrate. Older binaries reject schema 3
on startup; rollback to an older binary requires a compatible pre-upgrade backup.

Freed SQLite pages can be reused; this command does not run `VACUUM`, shrink the
database file, impose a hard size cap, or securely erase old WAL/backups. Metadata
and history still grow. Payload-byte counts describe logical content removed,
not filesystem bytes reclaimed. Physical compaction and metadata retention are
separate future work.
