// Command go-producer records a development order and its event in one transaction.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	writerelay "github.com/johnathondillon/write-relay/sdk/go"
)

func main() {
	id := flag.String("id", "", "stable order ID (required)")
	rollback := flag.Bool("rollback", false, "roll back the business write and event")
	flag.Parse()
	dsn := os.Getenv("WRITERELAY_APP_DSN")
	if dsn == "" || *id == "" || flag.NArg() != 0 {
		fmt.Fprintln(os.Stderr, "set WRITERELAY_APP_DSN and pass --id <order-id> [--rollback]")
		os.Exit(2)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := recordOrder(ctx, dsn, *id, *rollback); err != nil {
		// Driver errors may include DSNs or business values. Show a safe category.
		var pgError *pgconn.PgError
		switch {
		case errors.Is(err, writerelay.ErrInvalidEvent):
			fmt.Fprintln(os.Stderr, err)
		case errors.As(err, &pgError):
			fmt.Fprintf(os.Stderr, "producer failed with PostgreSQL SQLSTATE %s\n", pgError.Code)
		default:
			fmt.Fprintln(os.Stderr, "producer failed; check database connectivity and setup")
		}
		fmt.Fprintln(os.Stderr, "commit was not confirmed; check the order before retrying")
		os.Exit(1)
	}
	if *rollback {
		fmt.Println("Rolled back the order and event.")
	} else {
		fmt.Println("Committed the order and event; delivery is handled by the relay.")
	}
}

func recordOrder(ctx context.Context, dsn, id string, rollback bool) error {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())
	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = tx.Rollback(cleanupCtx) // Harmless after a completed commit or rollback.
	}()
	if _, err := tx.Exec(ctx, `INSERT INTO orders(id, status) VALUES($1, 'paid')
ON CONFLICT(id) DO UPDATE SET status = EXCLUDED.status`, id); err != nil {
		return err
	}
	data, err := json.Marshal(struct {
		Amount   int    `json:"amount"`
		Currency string `json:"currency"`
	}{Amount: 12900, Currency: "USD"})
	if err != nil {
		return err
	}
	if err := writerelay.Emit(ctx, tx, writerelay.Event{
		ID: "evt-go-" + id, Source: "urn:service:billing", Type: "order.paid",
		Subject: &id, Data: data,
	}); err != nil {
		return err
	}
	if rollback {
		return tx.Rollback(ctx)
	}
	return tx.Commit(ctx)
}
