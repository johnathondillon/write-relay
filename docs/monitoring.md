# Health checks and metrics

Monitoring is optional and disabled unless `monitoring.listen` is set. Add this
to your YAML configuration and restart `writerelayd run`:

```yaml
monitoring:
  listen: 127.0.0.1:9090
  sample_interval: 15s
```

The address must be a literal IP and port from 1–65535 (IPv6 example:
`'[::1]:9090'`). The sampling interval defaults to 15 seconds and accepts 1 second
through 5 minutes. Other CLI commands do not start the HTTP listener. A port
binding or HTTP serving failure stops the daemon and returns an error.

```bash
curl -i http://127.0.0.1:9090/healthz
curl -i http://127.0.0.1:9090/readyz
curl http://127.0.0.1:9090/metrics
```

These are read-only GET endpoints; they expose no events or administrative
operations. There is no authentication or TLS on this listener. Keep it on
loopback or a private monitoring network. In containers, bind `0.0.0.0:9090`
inside the container and restrict any published host port to loopback, as the
[LMS example](../examples/nuxt-lms/README.md#watch-live-health-and-metrics) does.

## What the health checks mean

| Endpoint | HTTP 200 | HTTP 503 |
| --- | --- | --- |
| `/healthz` | The daemon's HTTP server is responding and shutdown has not started. | Shutdown is in progress. |
| `/readyz` | Replication streaming has started, the spool sample is fresh, and enabled disk protection has a fresh, unpaused sample. | Capture is disconnected/paused, a required sample is unavailable/stale, or shutdown has started. |

Readiness returns a small JSON object:

```json
{"capture_connected":true,"ready":true,"spool_sample_fresh":true,"capture_paused":false,"capture_pause_reason":"","disk_protection_enabled":false,"disk_sample_fresh":false}
```

Disk protection is disabled in the JSON example above. When enabled, low space
or a failed probe pauses capture and fails readiness while liveness stays healthy.
`capture_pause_reason` is `low_disk`, `disk_check_failed`, or empty. Disk samples
expire after twice their own `spool.disk_space.check_interval`; a stale sample
also fails readiness. See the [disk-space guide](disk-space.md) for thresholds,
available-byte gauges, recovery, and PostgreSQL WAL monitoring.

An idle database can be ready without producing events. A receiver outage,
retry backlog, or dead letter does **not** make capture unready. Use delivery
metrics to detect those conditions. Restarting a relay because a receiver is
offline does not repair the receiver.

`capture_connected` is observed connection state, not an active database probe.
It becomes true after replication startup and local checkpoint loading, and
false when the capture session exits, including during reconnect backoff.
A network partition can remain undetected until PostgreSQL/the transport reports
an error. Readiness does not prove that the stream is caught up, that a future
spool write will succeed, or that a receiver has completed downstream work.
Liveness alone does not detect a stalled capture worker.

The last-success timestamp and transaction counter indicate observed capture
progress. They update only after a batch is persisted and its standby status
update is sent. They include empty batches and identical replays, reset on
process restart, and are not event counts. A sent status update does not prove
that PostgreSQL has already recorded it. No-events periods are normal; do not
alert on an old capture timestamp alone.

## Metrics

`/metrics` serves the [Prometheus text exposition format
0.0.4](https://prometheus.io/docs/instrumenting/exposition_formats/). It can be
read directly with curl; running Prometheus or Grafana is optional.

| Metric | Meaning |
| --- | --- |
| `writerelay_capture_connected` | Observed streaming connection: 1 or 0. |
| `writerelay_capture_transactions_total` | Batches persisted and status updates sent by this process. Counter. |
| `writerelay_capture_last_success_timestamp_seconds` | Unix time of that last success, or 0 before any success in this process. |
| `writerelay_spool_sample_fresh` | Latest spool sample succeeded and is fresh: 1 or 0. |
| `writerelay_spool_sample_timestamp_seconds` | Unix time of the last successful snapshot, or 0 before one succeeds. |
| `writerelay_spool_events` | Saved event identities, including those with pruned payloads. |
| `writerelay_spool_payloads{state}` | Payload counts: `retained` or `pruned`. |
| `writerelay_spool_file_bytes{file}` | File lengths: `database`, `wal`, or `shm`. |
| `writerelay_sink_active{sink}` | Whether the durable sink is currently configured: 1 or 0. |
| `writerelay_deliveries{sink,state}` | Delivery counts: `pending`, `retry_wait`, `delivered`, or `dead_letter`. |
| `writerelay_oldest_waiting_age_seconds{sink}` | Age since original capture of the oldest pending/retry-wait event, or 0 if none. |

All metrics except the transaction counter are gauges. Delivery counts include
inactive sinks with retained history. Empty sinks export zero counts, and
capture-only configurations have no sink series. An event sent to two sinks
contributes two deliveries. `retry_wait` is the number of records awaiting retry,
not a count of failed HTTP attempts. Redrive can reduce the dead-letter gauge.

Waiting age includes time before backfill or redrive and excludes terminal
records. Between successful snapshots, age continues advancing from the sampled
oldest event's capture time. A delivery that just completed can therefore remain
visible until the next sample. Negative ages from wall-clock changes clamp to
zero. File sizes are approximate SQLite file lengths sampled separately; they
are neither allocated disk space, free disk capacity, nor PostgreSQL retained
WAL. Pruning makes space reusable but does not automatically shrink the file.

Labels contain only sink names and fixed categories. Payloads, event IDs,
source URIs, LSNs, destination URLs, credentials, and error details are omitted.

## Sampling and failure behavior

A single background sampler reads an independent SQLite connection in read-only
mode, immediately at startup and then waits `sample_interval` after each sample.
Each read has a five-second context deadline. HTTP requests read the cached
snapshot and never query the spool, PostgreSQL, or destinations.

A snapshot is fresh only if the latest read succeeded and its age is at most
`sample_interval + 10s`, allowing the next read and scheduling slack. On failure
or expiry, `/readyz` returns 503 and `/metrics` still returns 200 with
`writerelay_spool_sample_fresh 0`. It omits all spool/sink counts rather than
reporting zero or stale backlog. Capture metrics and the last successful sample
timestamp remain available. A later successful sample restores readiness if
capture is connected. Configure monitoring to check freshness as well as scrape
success; a 200 metrics response alone does not establish a healthy spool.

Sampling scans retained delivery history. Increase the interval for a large
spool and measure the cost; HTTP caching does not make aggregation free. The
read transaction can briefly delay WAL checkpoint reclamation. Monitoring never
advances checkpoints, writes delivery state, prunes data, or changes ACK order.

## Using Prometheus

For Prometheus running on the same host as a relay listening on loopback:

```yaml
scrape_configs:
  - job_name: writerelay
    scrape_interval: 15s
    static_configs:
      - targets: ['127.0.0.1:9090']
```

If Prometheus is in a container, configure an address it can reach; its own
`127.0.0.1` refers to that container.

Useful queries for a single relay include:

```promql
sum(writerelay_deliveries{state=~"pending|retry_wait"})
writerelay_deliveries{state="dead_letter"} > 0
writerelay_oldest_waiting_age_seconds > 300
writerelay_capture_connected == 0
writerelay_spool_sample_fresh == 0
up{job="writerelay"} == 0
```

The five-minute waiting threshold is an example; choose a delay appropriate to
your application. Inspect and redrive dead letters using the CLI. Monitor host
free space and PostgreSQL slot WAL retention separately. Optional disk protection
adds local available-space metrics and pause signals; the
[disk-space guide](disk-space.md#monitor-postgresql-wal-as-well) provides a
PostgreSQL retained-WAL query. A capture pause can increase retained WAL.
