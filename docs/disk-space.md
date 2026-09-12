# Capture disk-space protection

WriteRelay can pause new capture when the filesystem containing its SQLite spool
runs low on available space. Existing delivery workers keep running so the
receiver can finish the local backlog. This is an optional admission check,
not a disk reservation, a hard spool-size limit, or automatic cleanup.

## Configure thresholds

Add this under the existing `spool` section and restart the daemon:

```yaml
spool:
  path: ./data/writerelay.sqlite
  max_event_bytes: 262144
  disk_space:
    pause_below_bytes: 536870912   # Pause below 512 MiB available.
    resume_at_bytes: 1073741824   # Resume at or above 1 GiB available.
    check_interval: 5s
```

The byte counts are examples; size the reserve for your maximum transaction,
SQLite WAL growth, delivery updates, migrations, other processes, and the time
needed for an operator to respond. The reserve is shared filesystem capacity,
not capacity assigned exclusively to WriteRelay.

| Setting | Behavior |
| --- | --- |
| `pause_below_bytes` | Non-negative integer bytes. Zero or omission disables protection. Pause when available bytes are strictly below this value. |
| `resume_at_bytes` | Required when enabled, and must exceed `pause_below_bytes`. An already-paused process resumes at or above this value. Must be zero/omitted when disabled. |
| `check_interval` | Defaults to `5s`; accepts `1s` through `1m`. Controls routine checks and paused recovery checks. |

The two thresholds prevent repeated pause/resume cycles near a single boundary.
The paused state is held in memory: a fresh process makes its initial decision
using the pause threshold, then applies the higher recovery threshold after a
pause. No spool migration is required. Existing configurations remain disabled.

## What happens during a pause

Capture checks space before connecting, periodically while receiving, and
**immediately before persisting each complete PostgreSQL transaction**, including
transactions that would only advance the checkpoint. The pre-persist check always
refreshes the measurement even if the periodic sample is recent.

When space is low, capture closes its replication stream and discards any
unpersisted in-memory transaction or completed batch. It does not write that
batch to SQLite or acknowledge it. After space recovers, capture reconnects from
the last durable checkpoint and PostgreSQL replays the outstanding work. Existing
identity/digest checks make identical replay safe.

A filesystem measurement error also pauses capture, with reason
`disk_check_failed`. A later successful measurement must reach the recovery
threshold before capture resumes. Raw filesystem errors and paths are not exposed
in the pause reason or metrics. A paused daemon can still shut down normally.

Delivery continues using records already in SQLite, provided its database writes
succeed. A successful delivery changes its durable state but does not delete the
payload or shrink the spool. Low disk is not an instruction to drop pending events
or dead letters. Manual [payload pruning](retention.md) can make eligible storage
reusable; it does not necessarily increase filesystem free space enough to resume.

## Observe the pause

Enable [monitoring](monitoring.md), then inspect:

```bash
curl -i http://127.0.0.1:9090/healthz
curl -i http://127.0.0.1:9090/readyz
curl http://127.0.0.1:9090/metrics
```

A paused daemon remains live (`/healthz` returns `200`) but is not ready
(`/readyz` returns `503`). Readiness includes:

```json
{
  "ready": false,
  "capture_connected": false,
  "capture_paused": true,
  "capture_pause_reason": "low_disk",
  "disk_protection_enabled": true,
  "disk_sample_fresh": true,
  "spool_sample_fresh": true
}
```

`capture_pause_reason` is `low_disk`, `disk_check_failed`, or an empty string.
During stream shutdown `capture_connected` can briefly still be true; the pause
itself is enough to make readiness fail. Enabled protection also fails readiness
before its first successful sample or when the sample is stale. A disk sample
expires after twice `check_interval`; HTTP handlers never probe the filesystem.

| Metric | Meaning |
| --- | --- |
| `writerelay_disk_protection_enabled` | Whether this process has protection enabled. |
| `writerelay_capture_paused` | Whether capture is paused by the disk guard. |
| `writerelay_capture_pause{reason="low_disk"}` | Waiting for sufficient available space. |
| `writerelay_capture_pause{reason="disk_check_failed"}` | The last filesystem probe failed. |
| `writerelay_disk_available_bytes` | Bytes available to the daemon's user on the spool filesystem. Omitted on failed/stale samples. |
| `writerelay_disk_sample_fresh` | Latest probe succeeded and is fresh. |
| `writerelay_disk_sample_timestamp_seconds` | Time of the latest probe attempt, including failures; zero before the first. |
| `writerelay_disk_pause_below_bytes` | Configured pause threshold. |
| `writerelay_disk_resume_at_bytes` | Configured recovery threshold. |

The disk sample/threshold/available-byte metrics appear only when protection is
enabled. Delivery and spool metrics remain independently available if their own
sample is fresh. Pause and recovery transitions are logged even when the HTTP
monitor is disabled.

Alert on `writerelay_capture_paused == 1`, and on stale measurements when
protection is enabled. A low-space pause calls for restoring capacity; repeatedly
restarting the daemon does not free capacity.

## Monitor PostgreSQL WAL as well

Create the replication slot with `setup --create-slot` before relying on capture
or replay. A slot cannot retroactively capture events emitted before its creation.
If the development `create_slot_if_missing` option is used, low-space startup
also delays that creation; it does not protect events produced before setup.

Pausing capture keeps the replication slot behind. PostgreSQL may retain growing
WAL for that slot while applications keep committing. Local free-space protection
cannot protect the PostgreSQL server's disk. If PostgreSQL's WAL retention limit
invalidates the slot, freeing local space alone cannot restore the missing WAL.

The existing doctor command reports slot-retained WAL when its query succeeds:

```bash
go run ./cmd/writerelayd doctor --config ./writerelay.yaml
```

A paused stream is normally inactive. With an appropriately privileged PostgreSQL
monitoring connection, this read-only query reports the configured slot's retained
WAL in bytes (replace the slot name when needed):

```sql
SELECT slot_name,
       active,
       restart_lsn,
       confirmed_flush_lsn,
       pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)::bigint
         AS retained_wal_bytes
FROM pg_replication_slots
WHERE slot_name = 'writerelay_slot_v1';
```

A null `restart_lsn` yields null retained bytes, not a healthy zero. Collect this
query through your PostgreSQL monitoring system and alert on both retained WAL
and database-host free space. The daemon's filesystem metric does **not** measure
PostgreSQL WAL. Size alerts and your PostgreSQL retention policy for the expected
outage duration and WAL generation rate.

## Recovery and limits

1. Inspect the pause reason, local available space, and PostgreSQL retained WAL.
2. Restore capacity on the spool filesystem, for example by expanding its volume
   or removing unrelated files that are safe to delete. Keep the spool and its
   SQLite sidecars intact.
3. Once the recovery threshold is met, capture resumes automatically at the next
   check. Confirm readiness, renewed capture progress, and falling backlog/WAL.
4. If the slot was invalidated or required WAL is unavailable, investigate and
   recover from authoritative data. Do not recreate the slot and assume the
   omitted interval was delivered.

The probe uses `statfs` available blocks for the daemon's user on Linux and macOS,
with the existing spool file selecting the filesystem. In Docker this reflects
the container's spool filesystem or mounted volume, which can differ from host
free space. Unsupported platforms fail the probe when protection is enabled.

Checks cannot reserve space against concurrent writers, predict SQLite write
amplification, detect every quota/inode limit, or guarantee the next transaction
fits. Startup initialization, migrations, sink registration/backfill, delivery
updates, and manual maintenance are outside the capture admission check and can
still write while space is low. A real SQLite durability error remains fatal;
the guard never converts a failed write into a retry or advances an acknowledgment.

Use a healthy local filesystem. A stalled OS filesystem syscall cannot be canceled
by a Go context; cached monitoring stays responsive and expires the old sample,
but a stuck filesystem can delay capture shutdown. Automatic payload cleanup and
a hard spool-size policy remain separate work.

## Verification

Unit tests inject available-byte readings and errors to verify thresholds,
caching, forced pre-persist checks, stale metrics, and cancellation. Process-crash
tests terminate a paused child before any write or ACK, then verify clean replay.

The PostgreSQL 14–18 matrix verifies unchanged checkpoint/ACK while paused,
continued delivery, low-space restart, ordered replay, rollback absence, and
recovery from measurement errors. Tests inject observations rather than filling
the developer machine's disk. The Linux container smoke check also exercises the
real non-root filesystem probe with a deliberately unreachable high threshold.
See [ADR 0010](adr/0010-disk-space-capture-admission.md) for the design decision.
