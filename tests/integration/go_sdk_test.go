//go:build integration

package integration

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/johnathondillon/write-relay/internal/config"
	"github.com/johnathondillon/write-relay/internal/delivery"
	commitpostgres "github.com/johnathondillon/write-relay/internal/postgres"
	sqlitespool "github.com/johnathondillon/write-relay/internal/spool/sqlite"
	writerelay "github.com/johnathondillon/write-relay/sdk/go"
)

type sdkTransaction struct {
	write    func(string) error
	emit     func(writerelay.Event) error
	commit   func() error
	rollback func() error
}

func TestGoSDKTransactions(t *testing.T) {
	dsn := os.Getenv("WRITERELAY_INTEGRATION_DSN")
	if dsn == "" {
		dsn = "postgres://postgres:postgres-dev-password@localhost:5432/writerelay?sslmode=disable"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close(context.Background())
	suffix := strconv.FormatInt(time.Now().UnixNano(), 36)
	slot, publication, table := "wr_sdk_"+suffix, "wr_sdk_pub_"+suffix, "wr_sdk_rows_"+suffix
	defer func() {
		cleanupCtx, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		for _, query := range []string{"SELECT pg_drop_replication_slot('" + slot + "')", "DROP PUBLICATION IF EXISTS " + publication, "DROP TABLE IF EXISTS " + table} {
			if _, err := admin.Exec(cleanupCtx, query); err != nil {
				t.Errorf("SDK fixture cleanup: %v", err)
			}
		}
	}()
	var mu sync.Mutex
	attempts := make(map[string]int)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		var envelope struct {
			ID string `json:"id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&envelope); err != nil {
			http.Error(w, "invalid JSON", 400)
			return
		}
		mu.Lock()
		attempts[envelope.ID]++
		mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	cfg := config.Config{
		Version:  config.CurrentVersion,
		Postgres: config.PostgresConfig{DSN: dsn, Slot: slot, Publication: publication, MessagePrefix: config.RequiredPrefix, StatusInterval: time.Second, MaxTransactionEvents: 100, MaxTransactionBytes: 1024 * 1024},
		Spool:    config.SpoolConfig{Path: filepath.Join(t.TempDir(), "sdk.sqlite"), MaxEventBytes: config.DefaultMaxEventBytes},
		Delivery: config.DeliveryConfig{PollInterval: 20 * time.Millisecond, RequestTimeout: time.Second, Retry: config.RetryConfig{InitialDelay: 50 * time.Millisecond, MaxDelay: time.Second, MaxAttempts: 3}, Sinks: []config.SinkConfig{{Name: "integration_webhook", Type: "webhook", URL: server.URL, AllowInsecureHTTP: true}}},
	}
	if _, err := commitpostgres.Setup(ctx, cfg, true); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(ctx, "CREATE TABLE "+table+" (id text PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}
	store, err := sqlitespool.Open(ctx, cfg.Spool.Path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	sender, registration, err := delivery.NewWebhookSender(cfg.Delivery.Sinks[0], time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.ConfigureSinks(ctx, []delivery.SinkRegistration{registration}); err != nil {
		t.Fatal(err)
	}
	stop, _ := startRuntime(t, cfg, store, sender, slog.New(slog.NewTextHandler(io.Discard, nil)))
	defer func() {
		if err := stop(); err != nil {
			t.Error(err)
		}
	}()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	dbConfig, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	db := stdlib.OpenDB(*dbConfig)
	defer db.Close()

	for _, driverName := range []string{"pgxpool", "database_sql"} {
		t.Run(driverName, func(t *testing.T) {
			begin := func() sdkTransaction {
				t.Helper()
				writeSQL := "INSERT INTO " + table + "(id) VALUES($1)"
				if driverName == "pgxpool" {
					tx, err := pool.Begin(ctx)
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { _ = tx.Rollback(context.Background()) })
					return sdkTransaction{
						write:  func(id string) error { _, err := tx.Exec(ctx, writeSQL, id); return err },
						emit:   func(e writerelay.Event) error { return writerelay.Emit(ctx, tx, e) },
						commit: func() error { return tx.Commit(ctx) }, rollback: func() error { return tx.Rollback(ctx) },
					}
				}
				tx, err := db.BeginTx(ctx, &sql.TxOptions{})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = tx.Rollback() })
				return sdkTransaction{
					write:  func(id string) error { _, err := tx.ExecContext(ctx, writeSQL, id); return err },
					emit:   func(e writerelay.Event) error { return writerelay.EmitSQL(ctx, tx, e) },
					commit: tx.Commit, rollback: tx.Rollback,
				}
			}
			checkBusiness := func(id string, want int) {
				t.Helper()
				var count int
				if err := admin.QueryRow(ctx, "SELECT count(*) FROM "+table+" WHERE id=$1", id).Scan(&count); err != nil || count != want {
					t.Fatalf("business row %s: count=%d want=%d err=%v", id, count, want, err)
				}
			}
			makeEvent := func(id string) writerelay.Event {
				return writerelay.Event{ID: id, Source: "urn:integration:go-sdk:" + suffix, Type: "order.paid", Time: "2026-09-10T12:00:00Z", Data: json.RawMessage(`{"amount":9007199254740993,"message":"雪"}`)}
			}
			id := driverName + "-committed-" + suffix
			committed := makeEvent(id)
			tx := begin()
			if err := tx.write(id); err != nil {
				t.Fatal(err)
			}
			if err := tx.emit(committed); err != nil {
				t.Fatal(err)
			}
			// Emit returned, but another connection still cannot see the business row.
			checkBusiness(id, 0)
			if err := tx.commit(); err != nil {
				t.Fatal(err)
			}
			checkBusiness(id, 1)
			waitForDelivered(t, ctx, store, id, 1)

			var absent []string
			for _, outcome := range []string{"rollback", "validation_error", "database_error"} {
				rejectedID := driverName + "-" + outcome + "-" + suffix
				absent = append(absent, rejectedID)
				tx := begin()
				if err := tx.write(rejectedID); err != nil {
					t.Fatal(err)
				}
				event := makeEvent(rejectedID)
				if outcome == "validation_error" {
					event.Type = ""
				}
				if outcome == "database_error" {
					event.Data = json.RawMessage(`"` + strings.Repeat("x", config.DefaultMaxEventBytes) + `"`)
				}
				err := tx.emit(event)
				switch outcome {
				case "rollback":
					if err != nil {
						t.Fatal(err)
					}
				case "validation_error":
					if !errors.Is(err, writerelay.ErrInvalidEvent) {
						t.Fatalf("expected SDK validation error, got %v", err)
					}
				case "database_error":
					var pgError *pgconn.PgError
					if !errors.As(err, &pgError) || pgError.Code != "54000" {
						t.Fatalf("expected PostgreSQL size error unchanged, got %v", err)
					}
				}
				if err := tx.rollback(); err != nil {
					t.Fatal(err)
				}
				checkBusiness(rejectedID, 0)
			}
			// Replay the original identity and content, then emit a later marker so
			// absence checks cannot pass merely because capture has not caught up.
			tx = begin()
			if err := tx.emit(committed); err != nil {
				t.Fatal(err)
			}
			markerID := driverName + "-marker-" + suffix
			if err := tx.emit(makeEvent(markerID)); err != nil {
				t.Fatal(err)
			}
			if err := tx.commit(); err != nil {
				t.Fatal(err)
			}
			waitForDelivered(t, ctx, store, markerID, 1)
			rows, err := store.ListEvents(ctx, 100)
			if err != nil {
				t.Fatal(err)
			}
			for _, id := range absent {
				if findEvent(rows, id) != nil {
					t.Fatalf("uncommitted SDK event captured: %s", id)
				}
				mu.Lock()
				calls := attempts[id]
				mu.Unlock()
				if calls != 0 {
					t.Fatalf("uncommitted SDK event delivered: %s", id)
				}
			}
			row := findEvent(rows, id)
			if row == nil {
				t.Fatal("committed SDK event missing")
			}
			var envelope map[string]json.RawMessage
			if err := json.Unmarshal(row.Payload, &envelope); err != nil {
				t.Fatal(err)
			}
			// Decode with UseNumber to verify large integer content survived jsonb.
			decoder := json.NewDecoder(strings.NewReader(string(envelope["data"])))
			decoder.UseNumber()
			var data map[string]any
			if err := decoder.Decode(&data); err != nil {
				t.Fatal(err)
			}
			if data["amount"] != json.Number("9007199254740993") || data["message"] != "雪" || string(envelope["specversion"]) != `"1.0"` || string(envelope["datacontenttype"]) != `"application/json"` {
				t.Fatal("SDK payload changed during capture")
			}
			mu.Lock()
			calls := attempts[id]
			mu.Unlock()
			if calls != 1 {
				t.Fatalf("identical SDK emission redelivered: %d attempts", calls)
			}
		})
	}
	if count, err := store.EventCount(ctx); err != nil || count != 4 {
		t.Fatalf("expected two committed identities per driver: count=%d err=%v", count, err)
	}
}
