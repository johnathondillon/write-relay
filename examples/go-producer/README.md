# Go producer example

This command writes a development order and emits `order.paid` in the same
PostgreSQL transaction using the [Go SDK](../../sdk/go/README.md). It runs locally
against the existing development stack. Go and Docker Compose are required.

## Start the relay

From the repository root, follow the [root quickstart](../../README.md#local-quick-start)
to create `writerelay.yaml`, start PostgreSQL, install/setup the replication slot,
and run the daemon. If that stack is already running, reuse it.

The example uses the existing `orders` table and application role from
`sql/postgres/dev_setup.sql`; it does not create tables, roles, or slots itself.
The Nuxt LMS stack is a separate example with a different schema.

## Commit an order

In a second terminal, from the repository root:

```bash
export WRITERELAY_APP_DSN='postgres://writerelay_app:dev-app-password@localhost:5432/writerelay?sslmode=disable'
go run ./examples/go-producer --id ord-go-committed
```

These credentials belong to the local development stack. Expected output:

```text
Committed the order and event; delivery is handled by the relay.
```

Inspect both records:

```bash
docker compose exec -T postgres psql -U postgres -d writerelay \
  -c "SELECT id, status FROM orders WHERE id = 'ord-go-committed';"

go run ./cmd/writerelayd spool list --config ./writerelay.yaml --limit 100
```

The order should have status `paid`. After capture catches up, the spool contains
`evt-go-ord-go-committed` with source `urn:service:billing`. An SDK success means
the SQL statement succeeded; this command additionally commits its transaction.
Actual delivery depends on the sinks configured in `writerelay.yaml`.

## Roll back an order

Use a different ID that has not previously been committed:

```bash
go run ./examples/go-producer --id ord-go-rolled-back --rollback
go run ./examples/go-producer --id ord-go-after-rollback
```

The first command reports `Rolled back the order and event.` The second commits
a later marker. After `evt-go-ord-go-after-rollback` appears in the spool,
`evt-go-ord-go-rolled-back` must still be absent. Verify the business table too:

```bash
docker compose exec -T postgres psql -U postgres -d writerelay \
  -c "SELECT id, status FROM orders WHERE id IN ('ord-go-rolled-back', 'ord-go-after-rollback');"

go run ./cmd/writerelayd spool list --config ./writerelay.yaml --limit 100
```

Only `ord-go-after-rollback` should be present in that query. With an existing
spool containing many events, increase the inspection limit as needed.

## Replay unchanged content

```bash
go run ./examples/go-producer --id ord-go-committed
```

The example deliberately uses a fixed payload, stable derived event ID, and an
order upsert. Repeating this exact command keeps one business row and one spool
identity. This is a narrow demonstration; a real producer must reject reuse of
a request ID with different business content and resolve unknown commit outcomes.

Read [main.go](main.go) for the complete transaction flow. Stop the relay with
Ctrl-C and use `make postgres-down` when finished; this retains development data.
