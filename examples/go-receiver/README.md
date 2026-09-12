# Local Go receiver example

This server uses the [Go inbox helper](../../sdk/go/INBOX.md) to save webhook
receipts and received orders in one PostgreSQL transaction. You can demonstrate
duplicates, rollback, and a lost success response entirely on your machine.

Requires Docker Compose and the Go version in the root `go.mod`. The commands
below run from the repository root. They send HTTP requests directly to simulate
WriteRelay deliveries; a separate daemon is not required for this walkthrough.

## Start it

```bash
docker compose -f examples/go-receiver/compose.yaml up -d --wait
export WRITERELAY_RECEIVER_DSN='postgres://postgres:receiver-example-password@127.0.0.1:55433/receiver?sslmode=disable'
go run ./examples/go-receiver --init
go run ./examples/go-receiver
```

Run `--init` once per fresh database. It creates the inbox and `received_orders`
tables atomically; it deliberately fails if they already exist. On later starts,
run the server without `--init` to preserve saved receipts. The server listens
only on `127.0.0.1:8081`. Use `--port 8082` if that port is occupied and
update the curl URL accordingly; `--port 0` prints an available local address.

If database port 55433 is occupied, set `RECEIVER_POSTGRES_PORT` for Compose and
use that same port in the DSN. These are separate example database volumes.

## Process once, skip a duplicate

In a second terminal, from the repository root, define the request helper:

```bash
key=$(printf '%064d' 1)
body='{"specversion":"1.0","id":"evt-go-receiver-1","source":"urn:example:billing","type":"order.paid","subject":"order-1"}'
deliver() {
  curl -sS -i http://127.0.0.1:8081/webhook \
    -H 'Authorization: Bearer local-example-token' \
    -H 'Content-Type: application/json' \
    -H "Idempotency-Key: $key" \
    --data-binary "$body" "$@"
}
deliver
deliver
```

The first response is `200` with `{"status":"processed"}`. The second is `200`
with `{"status":"duplicate"}`. There is one saved order and one inbox receipt:

```bash
docker compose -f examples/go-receiver/compose.yaml exec -T postgres \
  psql -U postgres -d receiver -c 'TABLE received_orders' -c 'TABLE writerelay_inbox'
```

The keys here are fixed tutorial values in the expected 64-character format.
A real receiver uses the header supplied by WriteRelay unchanged on each retry.
For the same key, even adding whitespace to the original body causes `409`:

```bash
body="$body "
deliver
```

No receipt or order is overwritten by the conflict.

## Roll back, then retry

Use another event and key; keep both unchanged for its retry:

```bash
key=$(printf '%064d' 2)
body='{"specversion":"1.0","id":"evt-go-receiver-2","source":"urn:example:billing","type":"order.paid","subject":"order-2"}'
deliver -H 'X-Example-Failure: rollback'
```

This returns `503` after deliberately failing the callback. Repeat the database
inspection command above: neither the new order nor its receipt is saved.
Then retry without the failure header:

```bash
deliver
```

Now it returns `processed` and both rows are saved. With an actual WriteRelay
webhook sink, its retry policy would send the later attempt automatically.

## Lose the response after commit

```bash
key=$(printf '%064d' 3)
body='{"specversion":"1.0","id":"evt-go-receiver-3","source":"urn:example:billing","type":"order.paid","subject":"order-3"}'
deliver -H 'X-Example-Failure: drop-response'
```

Expect curl to report an empty reply: the server closed the socket **after**
committing. Inspect the database to see that order 3 and its receipt exist.
Retry the same request without the failure header:

```bash
deliver
```

The response is `duplicate` and there is still one order 3 record. A receiver
restart also preserves the receipts because they are stored in PostgreSQL.
Use fresh IDs and keys if repeating a scenario on an existing database.

## Connect it to WriteRelay

The [Go producer example](../go-producer/README.md) emits compatible
`order.paid` events. For a daemon running on your host, configure a webhook sink
with URL `http://127.0.0.1:8081/webhook`, `allow_insecure_http: true`, and the
`Authorization: Bearer local-example-token` header using the documented
[webhook configuration](../../README.md). Start the receiver before emitting.
A container's loopback address refers to that container; the URL above is for
running the daemon on the host.

This is a teaching server with fixed local credentials and deliberate failure
headers, not a deployment template. It validates its small `order.paid` envelope;
other event types need application-specific handlers. External service calls
inside an inbox callback are not protected by its PostgreSQL transaction.

## Verify and stop

The automated HTTP tests run in the repository's disposable integration stack:

```bash
make integration-version
# Or test all supported PostgreSQL majors:
make integration-matrix
```

They exercise this handler's authentication, validation, rollback, conflicts,
and lost-response retry. For TypeScript receiver compatibility checks, run
`make inbox-typescript-matrix`.

Stop the server with Ctrl-C and stop the example database with:

```bash
docker compose -f examples/go-receiver/compose.yaml down
```

To deliberately reset this example's orders and inbox receipts, use
`docker compose -f examples/go-receiver/compose.yaml down --volumes` instead,
then start it and run `--init` again.
