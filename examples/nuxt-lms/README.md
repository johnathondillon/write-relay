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
Only the LMS port is published, bound to `127.0.0.1`.

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

## Read the integration code

- [Producer transaction](server/api/completions.post.ts): save the completion,
  call `SELECT writerelay.emit($1::jsonb)`, and commit. A client-supplied UUID
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
and duplicate handling after a lost response. Avoid using the UI's failure
controls while verification is running. For Node development (Node 22.18+):

```bash
npm ci
npm run typecheck
```

The Docker LMS build also type-checks both services. After edits, rebuild with
`docker compose up --build`. Dependencies are locked in `package-lock.json`.

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
