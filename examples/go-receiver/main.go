// Command go-receiver demonstrates durable webhook processing on localhost.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	writerelay "github.com/johnathondillon/write-relay/sdk/go"
)

var errDemoRollback = errors.New("example rollback")

type receiver struct {
	pool   *pgxpool.Pool
	table  writerelay.InboxTable
	orders string // Trusted SQL identifier, fixed by this example or test setup.
}

type orderEvent struct {
	SpecVersion string `json:"specversion"`
	ID          string `json:"id"`
	Source      string `json:"source"`
	Type        string `json:"type"`
	Subject     string `json:"subject"`
}

func (receiver receiver) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" || r.URL.Path != "/webhook" {
		http.NotFound(w, r)
		return
	}
	// Fixed local-only credentials for the tutorial, not deployment configuration.
	if r.Header.Get("Authorization") != "Bearer local-example-token" {
		http.Error(w, "unauthorized", 401)
		return
	}
	defer r.Body.Close()
	raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 262144))
	if err != nil {
		http.Error(w, "invalid or oversized body", 400)
		return
	}
	var event orderEvent
	if err = json.Unmarshal(raw, &event); err != nil || event.SpecVersion != "1.0" || event.ID == "" || event.Source == "" || event.Type != "order.paid" || event.Subject == "" {
		http.Error(w, "invalid order.paid event", 400)
		return
	}
	keys := r.Header.Values("Idempotency-Key")
	if len(keys) != 1 {
		http.Error(w, "one Idempotency-Key required", 400)
		return
	}
	mode := r.Header.Get("X-Example-Failure")
	if mode != "" && mode != "rollback" && mode != "drop-response" {
		http.Error(w, "unknown example failure", 400)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	result, err := writerelay.WithInbox(ctx, receiver.pool, writerelay.InboxDelivery{InboxTable: receiver.table, Key: keys[0], Body: raw}, func(tx writerelay.InboxTx) error {
		if _, err := tx.Exec(ctx, "INSERT INTO "+receiver.orders+" (source,event_id,order_id) VALUES ($1,$2,$3)", event.Source, event.ID, event.Subject); err != nil {
			return err
		}
		if mode == "rollback" {
			return errDemoRollback
		}
		return nil
	})
	if err != nil {
		switch {
		case errors.Is(err, writerelay.ErrInvalidDelivery):
			http.Error(w, "invalid delivery key", 400)
		case errors.Is(err, writerelay.ErrInboxConflict):
			http.Error(w, "same key with different content", 409)
		default:
			http.Error(w, "processing failed; retry the same key and body", 503)
		}
		return
	}
	if mode == "drop-response" {
		// The database commit has finished. Close the socket before sending success.
		conn, _, err := http.NewResponseController(w).Hijack()
		if err == nil {
			_ = conn.Close()
			return
		}
		panic(http.ErrAbortHandler)
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]writerelay.InboxResult{"status": result})
}

func initialize(ctx context.Context, pool *pgxpool.Pool) error {
	ddl, err := writerelay.InboxTableSQL(writerelay.InboxTable{})
	if err != nil {
		return err
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = tx.Rollback(cleanup)
	}()
	if _, err = tx.Exec(ctx, ddl); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `CREATE TABLE received_orders (source text NOT NULL,event_id text NOT NULL,order_id text NOT NULL,PRIMARY KEY(source,event_id))`); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func main() {
	initDB := flag.Bool("init", false, "create the example tables once, then exit")
	port := flag.Int("port", 8081, "localhost HTTP port (0 chooses an available port)")
	flag.Parse()
	dsn := os.Getenv("WRITERELAY_RECEIVER_DSN")
	if dsn == "" || flag.NArg() != 0 || *port < 0 || *port > 65535 {
		fmt.Fprintln(os.Stderr, "set WRITERELAY_RECEIVER_DSN; optionally pass --init or --port <port>")
		os.Exit(2)
	}
	if err := run(dsn, *initDB, *port); err != nil {
		fmt.Fprintln(os.Stderr, "receiver failed; check database connectivity and setup (details redacted)")
		os.Exit(1)
	}
}

func run(dsn string, initDB bool, port int) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return err
	}
	cfg.ConnConfig.ConnectTimeout = 5 * time.Second
	cfg.ConnConfig.RuntimeParams["statement_timeout"] = "5000"
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return err
	}
	defer pool.Close()
	startup, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err = pool.Ping(startup); err != nil {
		return err
	}
	if initDB {
		if err = initialize(startup, pool); err == nil {
			fmt.Println("Created receiver inbox and orders tables.")
		}
		return err
	}
	server := &http.Server{Handler: receiver{pool: pool, orders: "received_orders"}, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 30 * time.Second}
	listener, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		return err
	}
	defer listener.Close()
	finished := make(chan error, 1)
	go func() { finished <- server.Serve(listener) }()
	fmt.Printf("Receiver listening at http://%s/webhook\n", listener.Addr())
	select {
	case err = <-finished:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
		cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err = server.Shutdown(cleanup); err != nil {
			_ = server.Close()
		}
		return err
	}
}
