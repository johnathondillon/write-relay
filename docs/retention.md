# Manual payload retention

`spool prune` removes old, successfully delivered event payloads. It retains
event identities, original digests, metadata, delivery history, and the durable
PostgreSQL checkpoint, so replay remains protected.

## Preview and apply

From the repository root, choose an explicit UTC cutoff for successful delivery:

```bash
go run ./cmd/writerelayd spool prune \
  --config ./writerelay.yaml \
  --before 2026-09-01T00:00:00Z \
  --limit 100 \
  --dry-run
```

The command prints one JSON object with `dry_run`, `before`, `limit`, `events`,
`payload_bytes`, and an `entries` array of sequence/source/ID/byte counts. It never
prints payload content. `events` and `payload_bytes` describe this bounded batch;
they are not totals for every eligible event in the spool.

To apply that retention policy, run the same command without `--dry-run`:

```bash
go run ./cmd/writerelayd spool prune \
  --config ./writerelay.yaml \
  --before 2026-09-01T00:00:00Z \
  --limit 100
```

`--before` is required, must include a timezone, and cannot be in the future.
The default batch limit is 1000; allowed limits are 1–1000. Apply rechecks durable
state rather than reserving the earlier preview. Repeat preview/apply as needed;
an `events: 0` result means no payloads currently qualify. Already-pruned events
are skipped, including when rerunning after a lost command response.

## What qualifies

Every associated delivery must have succeeded **strictly before** the cutoff.
The retention age is measured from delivery success, not event creation or
capture. An old event that succeeded recently remains available.

| Saved state | Payload can be pruned? |
| --- | --- |
| All deliveries succeeded before the cutoff | Yes, including inactive sinks' successful history. |
| Any pending or retry-wait delivery | No. |
| Any dead-letter delivery | No; it stays available for inspection and redrive. |
| Any success at/after the cutoff or missing its timestamp | No. |
| No delivery records | No; capture-only events remain available for a future sink. |

Decisions use saved delivery records, not sink declarations in the command's
configuration file. The command does not register sinks, contact PostgreSQL,
send events, or resolve destination secrets.

## Replay and new sinks

Pruning retains `(source, id)` and the original payload digest indefinitely.
Identical replay is recognized and does not restore the payload or add delivery
records. Different content with the same identity still stops capture.

A newly added or reactivated sink backfills only events whose payloads remain.
Pruned history cannot be delivered to that sink. Register a sink before pruning
if it needs that history, or keep an external archive. There is no restore or
force-redrive option for a pruned payload. Existing dead letters are never pruned.

`spool list` continues to show pruned identities with `payload: null` and
`payload_pruned_at`. `spool deliveries` retains their successful history.
`spool stats` keeps counting all captured identities and additionally reports
`pruned_payloads`; the readable summary shows retained versus pruned payloads.

## Upgrade and operational behavior

This feature requires spool schema 3. Stop the old daemon, deploy the updated
binary, and start it to migrate the spool before pruning. Migration preserves
existing payloads and delivery state. Older binaries cannot open schema 3.
Keep a consistent pre-upgrade backup if you need to roll back the binary.

Both preview and apply require an existing regular spool file and the current
schema; neither creates a database, migrates it, or changes file permissions.
Preview uses SQLite read-only mode, which may still maintain WAL shared memory.

Apply uses one durable write transaction per batch. It can run alongside the
updated daemon; concurrent writers serialize, or report a lock error if the
five-second timeout expires. Use smaller batches to reduce lock contention.
Cancellation or a crash before commit leaves the whole batch unchanged. A crash
after commit leaves the whole batch pruned. The checkpoint never advances during
cleanup.

## Disk space

Pruning makes payload storage reusable by SQLite. It does **not** automatically
shrink the database file or guarantee a particular number of filesystem bytes
freed. The WAL may temporarily grow during cleanup. Physical compaction is a
separate operation; see [SQLite's VACUUM documentation](https://www.sqlite.org/lang_vacuum.html).

Identity, metadata (including subject), and delivery-history rows remain and
still consume space. This is not a hard spool-size cap or secure erasure: old
bytes may remain in free pages, WAL files, snapshots, or backups. Continue to
monitor disk and retained PostgreSQL WAL.

See [ADR 0007](adr/0007-delivered-payload-retention.md) for the transaction and
replay decisions and the [Nuxt walkthrough](../examples/nuxt-lms/README.md#try-payload-cleanup)
for a local demonstration.
