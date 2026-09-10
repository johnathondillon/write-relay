# Nuxt LMS learning lab

A local, runnable example of WriteRelay with a Nuxt/TypeScript producer and a
separate TypeScript certificate service. Complete a course, interrupt delivery,
and watch recovery. All records are disposable example data.

## Run it

You need Docker with Compose. No local Go, Node, PostgreSQL installation, cloud
account, or deployment is required. The first build needs internet access to
download images and dependencies.

From the repository root:

```bash
cd examples/nuxt-lms
docker compose up --build
```

Open **http://127.0.0.1:3000** once the LMS is listening. The first build can
take several minutes. For a busy port, use `LMS_PORT=3005 docker compose up --build`
and open http://127.0.0.1:3005 instead. Using the explicit IPv4 loopback address
also avoids reaching an unrelated app listening on IPv6 `localhost`.

Compose initializes both databases, installs the SQL function, creates the
replication slot, and starts the services. No separate setup commands are needed.
The PostgreSQL health check waits for TCP readiness, so relay setup starts only
after the image has finished running its initialization scripts.
The LMS port (3000) and relay monitoring port (9090) are published, both bound
to `127.0.0.1`. If 9090 is busy, set `RELAY_MONITORING_PORT=9095` when starting
Compose and use that port in the monitoring commands below.

## What runs locally

```text
Browser → Nuxt LMS → PostgreSQL database: writerelay
                     completion + course.completed event
                                      ↓ committed WAL
                                 WriteRelay
                                 SQLite spool
                                      ↓ HTTP webhook
                              Certificate service
                                      ↓
                     PostgreSQL database: certificates
                     certificate + deduplication key
```

The databases share one local PostgreSQL container for convenience but have
separate credentials. The LMS cannot write certificates. The receiver cannot
read LMS records. The LMS displays receiver results through its HTTP status API.

The page shows the latest 50 completions, 100 certificates, and 100 receiver
requests. These are bounded views, not lifetime counters or authoritative relay
delivery state. Use the CLI below for pending, retry, delivered, and dead-letter
records. A certificate may exist before WriteRelay records successful delivery.

## Try the happy path

1. Keep the receiver on **Normal**.
2. Choose a course and click **Complete course**.
3. The completion appears as **Saved**, then the certificate becomes **Issued**.

Each click creates a new demo completion. The Nuxt endpoint saves the completion
and emits the event on the same PostgreSQL connection, in one transaction. The
HTTP receiver returns success only after its certificate and key commit.

## Try automatic recovery

1. Click **Simulate outage**. The certificate service now returns HTTP `503`.
2. Complete a course. Its completion stays saved while its certificate is absent.
3. Open **Receiver request history** to see retryable failures.
4. Switch to **Normal** within about 40 seconds. A retry issues the certificate.

For an actual process outage, use another terminal in this directory:

```bash
docker compose stop certificate
# Complete a course in the browser, then:
docker compose start certificate
```

While stopped, the page shows the receiver as unreachable; it does not infer that
previous certificates were deleted. Requests that never arrive do not appear in
receiver history. If the outage lasts long enough to exhaust retries, follow
the redrive steps below.

The example retries up to 10 attempts, starting at two seconds and capping delays
at five seconds. This deliberately short policy makes the demonstration quick.
Restarting a receiver resets its in-memory demo mode to Normal but retains its
database records. A pending event may consume a one-shot control before a newly
created event, so finish one experiment before starting another.

## Watch the relay's delivery counts

Keep the example running. In another terminal, from the repository root:

```bash
cd examples/nuxt-lms
docker compose exec relay writerelayd spool stats \
  --config /etc/writerelay/example.yaml
```

If you are already in `examples/nuxt-lms`, skip `cd`. This uses the binary inside
the relay container, so you do not need Go installed. If you started an older
version of the example, first rebuild with `docker compose up --build -d`.

The summary shows all stored events and delivery states, the `certificates`
sink's counts, its oldest waiting event's age, and the SQLite spool file sizes.
Unlike the page's bounded receiver history, these counts come from the relay's
complete persisted delivery state.

Try **Simulate outage**, complete a course, then rerun the command. The
`retry_wait` count should increase once a request fails. Restore **Normal** and
rerun after recovery: that delivery moves to `delivered`. A `dead_letter` stays
separate from the waiting count until you explicitly redrive it. Waiting age is
time since original local capture, not time since the last attempt.

For machine-readable output, add `--json`:

```bash
docker compose exec -T relay writerelayd spool stats \
  --config /etc/writerelay/example.yaml --json
```

The JSON response is one object with `sampled_at`, `event_count`,
`last_durable_lsn`, `deliveries`, `oldest_waiting`, `storage`, and `sinks`.
Each sink has its own `deliveries` and `oldest_waiting`; the latter is `null`
when nothing is pending or waiting to retry. File sizes are byte counts and
waiting ages are whole seconds.

This is a snapshot of saved state, not a daemon health check. To inspect the
same spool with the relay stopped, use a temporary CLI container:

```bash
docker compose run --rm --no-deps relay spool stats \
  --config /etc/writerelay/example.yaml
```

## Try a lost success response

1. With no older work pending, click **Lose next response**.
2. Complete a course.
3. The receiver commits the certificate, then closes the HTTP connection without
   sending success. The control automatically returns to Normal.
4. In request history, observe **Issued · response lost**, followed by
   **Duplicate handled**, with the same idempotency key. Only one certificate
   exists for that completion.

The receiver stores the key and certificate in the same database transaction.
A unique constraint handles concurrent duplicate requests. Identical retries
return success; a reused key with different event bytes returns a conflict.
Failure controls live only in this example receiver, not in the Go daemon.

## Try a rollback

Click **Try a rollback instead**. The endpoint executes the completion insert
and event emission, then rolls the transaction back. Neither a completion nor
a certificate appears. A later committed completion can still be delivered.

## Inspect a dead letter and retry it manually

1. Click **Reject events**, then complete a course.
2. Wait for **Rejected · 422** in receiver history. This response is permanent,
   so WriteRelay retains a dead letter instead of retrying automatically.
3. Switch back to **Normal**.
4. In the page's records table, copy the **Completion ID** from the rejected
   completion's row. Copy the full UUID, not the certificate ID or idempotency key.
5. Open another terminal. From the repository root, enter the example directory:

```bash
cd examples/nuxt-lms
```

If your terminal is already in `examples/nuxt-lms`, skip the `cd` command. Keep
the example running while you use the commands below.

6. Run this command, replacing **only `YOUR_COMPLETION_ID`** with the UUID you
   copied. The config path, sink, and source already match this example:

```bash
docker compose exec relay writerelayd spool redrive \
  --config /etc/writerelay/example.yaml \
  --sink certificates \
  --source urn:writerelay:example:lms \
  --id YOUR_COMPLETION_ID
```

This runs the CLI inside the existing relay container; you do not need Go
installed or a local `writerelay.yaml` file.

7. The terminal should print `redrive scheduled`. Return to the page and watch
   that completion's certificate become **Issued**, usually within a few seconds.

Returning the receiver to Normal does not by itself reactivate dead letters.
Redrive retains the event and stable delivery key. If the command cannot find a
dead letter, verify the ID and inspect the relay's records from the same directory:

```bash
docker compose exec relay writerelayd spool deliveries \
  --config /etc/writerelay/example.yaml --state dead_letter
```

The `id` field in this output is the page's Completion ID. You can also inspect
the original captured event and service logs:

```bash
docker compose exec relay writerelayd spool list --config /etc/writerelay/example.yaml
docker compose logs -f relay certificate
```

## Try payload cleanup

Rebuild the updated stack first with `docker compose up --build --wait` so the
daemon migrates its spool to schema 3. Complete a course normally and wait for
its certificate. For this disposable example, choose the current UTC cutoff:

```bash
PRUNE_BEFORE=$(date -u +%Y-%m-%dT%H:%M:%SZ)
docker compose exec relay writerelayd spool prune \
  --config /etc/writerelay/example.yaml \
  --before "$PRUNE_BEFORE" --limit 100 --dry-run
```

The JSON preview lists event IDs and payload-byte counts eligible for cleanup.
Only deliveries that succeeded before the cutoff qualify. To apply, use the
same cutoff without `--dry-run`:

```bash
docker compose exec relay writerelayd spool prune \
  --config /etc/writerelay/example.yaml \
  --before "$PRUNE_BEFORE" --limit 100
docker compose exec relay writerelayd spool stats \
  --config /etc/writerelay/example.yaml
```

Stats now reports pruned payloads, while saved completions and certificates stay
visible in the LMS. Pending work and dead letters remain available. The spool
keeps identities and delivery history; it does not automatically shrink the
SQLite file. New sinks cannot backfill pruned payloads. See the
[retention guide](../../docs/retention.md) before choosing a cutoff for real data.

## Watch live health and metrics

After updating this checkout, run `docker compose up --build -d` from this
example directory to rebuild the relay and expose its monitoring port. Existing
example records are preserved.

```bash
curl -i http://127.0.0.1:9090/healthz
curl -i http://127.0.0.1:9090/readyz
curl http://127.0.0.1:9090/metrics
```

Both health endpoints should return 200 once capture starts and the first spool
sample completes. No Prometheus deployment is needed to read these metrics.

1. In the LMS, click **Simulate outage**, then complete a course.
2. Fetch `/metrics` again after a couple of seconds. Look for
   `writerelay_deliveries{sink="certificates",state="retry_wait"} 1` and an
   increasing `writerelay_oldest_waiting_age_seconds{sink="certificates"}`.
   Counts can be higher if you already have waiting completions.
3. `/readyz` should still return 200: PostgreSQL capture can continue while the
   certificate service is failing.
4. Click **Normal** before the ten retry attempts are exhausted.
   After the certificate is issued and the next sample completes, `retry_wait`
   returns to zero and `delivered` increases.
5. To observe a dead letter, use **Reject events**, complete another course,
   and fetch `/metrics` again. The `dead_letter` count increases. Follow the
   [redrive instructions above](#inspect-a-dead-letter-and-retry-it-manually)
   to recover it. A receiver mode change alone does not redrive dead letters.

The example samples every second. Retained/pruned payload counts and
`writerelay_spool_file_bytes` are also available. See the
[monitoring guide](../../docs/monitoring.md) for every metric and its limits.

To observe a database disconnect in this disposable lab:

```bash
docker compose stop postgres
curl -i http://127.0.0.1:9090/readyz
curl -i http://127.0.0.1:9090/healthz
docker compose start postgres
```

Once the relay observes the disconnect, readiness returns 503 while liveness
remains 200. After PostgreSQL starts, allow time for reconnect backoff; readiness
returns to 200. The LMS and certificate service also use this PostgreSQL
container, so their database operations are unavailable while it is stopped.

## Read the integration code

- [Producer transaction](server/api/completions.post.ts): save the completion,
  call the [TypeScript SDK](../../sdk/typescript/README.md)'s `emit(client, event)`
  on the same client, and commit. The SDK executes
  `SELECT writerelay.emit($1::jsonb)`. A client-supplied UUID
  makes retries of an ambiguous producer HTTP request safe as well.
- [Receiver transaction](certificate/server.ts): authenticate, validate, insert
  the inbox key and certificate together, and only then return success.
- [Database setup](sql/002_example.sql): separate roles, databases, and tables.
- [Relay settings](relay.yaml): one HTTP sink and a short demo retry policy.
- [Nuxt page](app/app.vue): displays saved completions and receiver observations.

The producer transaction follows [node-postgres's same-client transaction
pattern](https://node-postgres.com/features/transactions). The Nuxt image runs
its [built Node server](https://nuxt.com/docs/4.x/getting-started/deployment).

This is a teaching example, not an LMS starter for deployment. It has no user
authentication, uses fixed local credentials and HTTP inside the Compose
network, and exposes deliberate receiver failure controls through the LMS.
WriteRelay does not atomically commit an external payment or remote service
operation with the LMS transaction. See the [project FAQ](../../docs/faq.md).

## Verify and develop

With the stack running, run the automated behavior checks using Docker:

```bash
docker compose run --rm --no-deps verify
```

This adds labeled test completions and checks concurrent producer requests,
rollback absence after a later delivered event, automatic outage recovery,
retry metrics while capture stays ready, and duplicate handling after a lost
response. Avoid using the UI's failure
controls while verification is running. For Node development (Node 22.18+),
build the local SDK first. Starting in `examples/nuxt-lms`:

```bash
cd ../../sdk/typescript
npm ci
npm test
cd ../../examples/nuxt-lms
npm ci
npm run typecheck
```

The Docker LMS build also type-checks both services. After edits, rebuild with
`docker compose up --build`. Dependencies are locked in `package-lock.json`.
The SDK is a local `file:../../sdk/typescript` dependency, so keep the example
inside the repository. Docker uses the repository root as its build context
and builds the SDK automatically. After SDK edits during Node development,
run `npm run build` in `sdk/typescript` again.

The example uses TypeScript 5.9 with `vue-tsc` 3.3.11. Upgrading TypeScript alone
to 7.0.2 fails during `nuxt typecheck`: `vue-tsc` loads `typescript/lib/tsc`,
which TypeScript 7 no longer exports (`ERR_PACKAGE_PATH_NOT_EXPORTED`).
[Dependabot configuration](../../.github/dependabot.yml) skips TypeScript major
version updates so the compiler and Nuxt/Vue tooling can be upgraded together.
Minor and patch updates remain enabled. Revisit that rule when upgrading the
tooling, and verify both the Docker build and the behavior checks above before
accepting a new compiler major version.

## Stop or start fresh

```bash
# Stop and remove example containers; preserve example data for the next run.
docker compose down

# Delete ONLY this example's database and relay volumes for a fresh start.
docker compose down --volumes
```

The Compose project is named `writerelay-nuxt-lms`, separate from the root
repository's development stack. Reset both volumes together: the replication
slot and SQLite checkpoint belong to the same capture history. Database init
scripts run only for a fresh PostgreSQL volume.
