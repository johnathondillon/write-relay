# Load and backlog recovery

`make load` runs a local, disposable scenario against the actual `writerelayd`
process. It checks correctness under a backlog and records throughput, delivery
latency, recovery time, and SQLite spool growth. Performance numbers are
observations for this workload and machine; they are not production capacity
guarantees or CI speed requirements.

## Run it

From the repository root, with Go and Docker Compose installed and Docker running:

```bash
make load
```

The default run produces 10,000 events on PostgreSQL 18. It builds the daemon,
starts PostgreSQL in a unique Compose project with a dynamically assigned
loopback port, and starts a local HTTP receiver with its own durable SQLite
database. No application deployment or manual failure injection is needed.
The PostgreSQL container, its volume, and temporary daemon/receiver files are
removed on exit. Development and LMS containers and data are not used.

The console prints phase summaries and the JSON report. The report also remains
at `artifacts/load/report.json`, which is ignored by Git and Docker builds.
Each invocation overwrites that report; set `LOAD_REPORT` to keep separate runs.

```bash
# A quick run, including a partial final transaction batch.
LOAD_EVENTS=250 LOAD_BATCH_SIZE=30 make load

# Larger events and a longer deadline; save a separate report.
LOAD_EVENTS=20000 LOAD_BATCH_SIZE=100 LOAD_PAYLOAD_BYTES=4096 \
  LOAD_TIMEOUT_SECONDS=600 LOAD_REPORT=artifacts/load/large.json make load

# Run the same scenario on another supported PostgreSQL major.
POSTGRES_VERSION=14 make load
```

| Variable | Default | Meaning and bounds |
| --- | --- | --- |
| `LOAD_EVENTS` | `10000` | Total committed events, 10–100,000. |
| `LOAD_BATCH_SIZE` | `100` | Events per producer transaction, 1–1,000. |
| `LOAD_PAYLOAD_BYTES` | `256` | ASCII padding bytes inside each event's `data`, 0–65,536; envelope overhead is additional. |
| `LOAD_TIMEOUT_SECONDS` | `300` | Deadline for the scenario after build/database startup, 30–1,800 seconds. |
| `POSTGRES_VERSION` | `18` | Official PostgreSQL image major, 14–18. |
| `LOAD_REPORT` | `artifacts/load/report.json` | JSON output path, relative to the repository root or absolute. |

The test rejects batch/payload combinations whose conservative size estimate
exceeds the daemon's default 8 MiB transaction limit. Production limits are not
raised. The test uses a 50 ms delivery poll, 100 ms–1 s retry backoff, 2 s request
timeout, and a 1,000-attempt retry budget so the deliberate outage can recover
automatically. These are harness settings, not recommended application defaults.

## What happens

1. Commit the first 10% of events with a healthy receiver. Each transaction
   inserts business rows and calls `writerelay.emit` for those same identities.
   Wait until every baseline event has a durable delivered state in the spool.
2. Make the receiver return HTTP 503, then commit the remaining events. Wait
   until all are captured durably and the backlog contains a retrying delivery.
3. Send `SIGKILL` to the daemon. Reopen its spool for inspection and check that
   all waiting events and the durable checkpoint survived.
4. Restart the daemon with the same spool. Wait for a fresh failed delivery
   attempt from that process, then restore the receiver.
5. Commit the first backlog event at the receiver, but close its HTTP connection
   before returning success. Verify that the resulting retry has the same
   idempotency key and payload and creates no second receiver record.
6. Drain the backlog, shut down the daemon, and compare every business identity,
   spool event, and receiver receipt. Assert preserved first-acceptance order,
   matching payload digests, exactly one receiver record per event, and no
   pending, retrying, or dead-letter deliveries.

The receiver persists its simulated business effect and inbox key in one SQLite
row using `synchronous=FULL`. Its deduplication is implemented by the receiver;
WriteRelay still provides at-least-once delivery. This scenario kills the daemon,
not the receiver, database server, host, or disk. The separate
`make failure` suite covers deterministic crashes at specific durability
boundaries; this test adds a larger workload and real daemon restart.

## Read the report

Check `status` first. A passing run reports `passed`; an assertion failure writes
`failed` with the measurements collected so far. The runner initially writes
`not_completed`, preventing a build/startup failure or interrupted run from
leaving an old successful report in place. A failure also prints the daemon log
tail when available and PostgreSQL container logs before cleanup.

| Field | Interpretation |
| --- | --- |
| `baseline.events_per_second` | Baseline events divided by time from starting production until all are durably marked delivered. |
| `outage_capture.events_per_second` | Backlog events divided by time from starting outage production until all are durably captured and waiting. No receiver delivery latency is recorded for this phase. |
| `recovery.settle_seconds` | Time from starting the replacement daemon until the entire backlog is durably marked delivered. Includes startup, the fresh 503 probe, retry delays, and the deliberately lost response. |
| `recovery.events_per_second` | Backlog size divided by that recovery time. |
| `produce_seconds` | Time spent producing and committing the phase's SQL batches. |
| `receiver_latency` | Nearest-rank p50/p95/p99 and maximum milliseconds from just before each producer transaction begins to the receiver's first durable commit. Recovery latencies include time spent waiting during the outage. |
| `http_503_attempts` / `duplicate_attempts` | Observed unavailable requests and successful receiver deduplications. At least one of each must occur. |
| `unique_receiver_records` | Must equal `options.events` on success. |
| `snapshots` | Event/delivery counts, checkpoint, and database/WAL/SHM file lengths at each phase boundary. Compare storage totals to observe spool growth. |
| `sampled_peak_spool_bytes` | Largest combined database/WAL/SHM file length observed while polling the spool; not a guaranteed peak or allocated-disk measurement. |

The report includes Go/PostgreSQL versions, OS/architecture, CPU count, and input
settings. Compare like-for-like runs on the same hardware and storage. The
producer is serial and batched; the receiver is local and commits each unique
event. Spool polling every 200 ms adds work and timing granularity. These numbers
include those choices and are not isolated component benchmarks. The harness
does not measure PostgreSQL WAL retention, receiver disk growth, CPU usage, or
memory usage. No payload pruning occurs during the run.

## CI

The `Load and backlog recovery` job runs 2,000 events against PostgreSQL 18 on
pushes and pull requests, with a 180-second scenario deadline and a 10-minute
job limit. It uploads `load-recovery-report` for 14 days, including partial
reports on failure when available. Open the workflow run's artifacts to download
the JSON. Throughput and latency do not have pass/fail thresholds: correctness
assertions, process failures, and timeouts determine the result.

Ordinary `make test` and `make race` do not start this Docker-backed harness.
For a smaller run that also checks the harness and daemon with Go's race detector:

```bash
GOFLAGS=-race LOAD_EVENTS=250 LOAD_BATCH_SIZE=30 \
  LOAD_REPORT=artifacts/load/race.json make load
```
