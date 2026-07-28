//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/johnathondillon/write-relay/internal/config"
	commitpostgres "github.com/johnathondillon/write-relay/internal/postgres"
	sqlitespool "github.com/johnathondillon/write-relay/internal/spool/sqlite"
	install "github.com/johnathondillon/write-relay/sql/postgres"
)

func TestTransactionalCapture(t *testing.T) {
	dsn := os.Getenv("WRITERELAY_INTEGRATION_DSN")
	if dsn == "" {
		dsn = "postgres://postgres:postgres-dev-password@localhost:5432/writerelay?sslmode=disable"
	}
	suffix := strconv.FormatInt(time.Now().UnixNano(), 36)
	slot := "wr_test_" + suffix
	publication := "wr_test_pub_" + suffix
	if len(slot) > 63 || len(publication) > 63 {
		t.Fatal("generated identifier too long")
	}
	cfg := config.Config{
		Version: config.CurrentVersion,
		Postgres: config.PostgresConfig{
			DSN:                  dsn,
			Slot:                 slot,
			Publication:          publication,
			MessagePrefix:        config.RequiredPrefix,
			StatusInterval:       time.Second,
			MaxTransactionEvents: 100,
			MaxTransactionBytes:  1024 * 1024,
		},
		Spool: config.SpoolConfig{
			Path:          filepath.Join(t.TempDir(), "capture.sqlite"),
			MaxEventBytes: config.DefaultMaxEventBytes,
		},
		Logging: config.LoggingConfig{Level: "info", Format: "text"},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect to integration PostgreSQL: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		cleanupConn, connectErr := pgx.Connect(cleanupCtx, dsn)
		if connectErr == nil {
			defer cleanupConn.Close(context.Background())
			_, _ = cleanupConn.Exec(cleanupCtx, "SELECT pg_drop_replication_slot($1)", slot)
			_, _ = cleanupConn.Exec(cleanupCtx, "DROP PUBLICATION IF EXISTS "+publication)
		}
	})
	defer admin.Close(context.Background())

	if _, err := admin.Exec(ctx, install.InstallSQL); err != nil {
		t.Fatalf("apply idempotent SQL installation: %v", err)
	}
	assertSQLRejects(t, ctx, admin, `[]`)
	assertSQLRejects(t, ctx, admin,
		`{"specversion":"1.0","id":123,"source":"urn:test","type":"invalid"}`)
	assertSQLRejects(t, ctx, admin,
		`{"specversion":"1.0","id":"oversized","source":"urn:test","type":"invalid","data":"`+
			strings.Repeat("x", config.DefaultMaxEventBytes)+`"}`)
	if _, err := admin.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS writerelay_integration_orders (
			id text PRIMARY KEY,
			status text NOT NULL
		)
	`); err != nil {
		t.Fatalf("create integration business table: %v", err)
	}

	if _, err := commitpostgres.Setup(ctx, cfg, true); err != nil {
		t.Fatalf("setup: %v", err)
	}
	var tableCount int
	if err := admin.QueryRow(ctx,
		`SELECT count(*) FROM pg_publication_tables WHERE pubname=$1`, publication,
	).Scan(&tableCount); err != nil || tableCount != 0 {
		t.Fatalf("publication is not empty: count=%d err=%v", tableCount, err)
	}

	store, err := sqlitespool.Open(ctx, cfg.Spool.Path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	runCtx, stopRun := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() {
		runDone <- commitpostgres.NewReplicator(cfg, store, logger).Run(runCtx)
	}()
	var stopOnce sync.Once
	var runErr error
	stopAndWait := func() {
		stopOnce.Do(func() {
			stopRun()
			select {
			case runErr = <-runDone:
			case <-time.After(5 * time.Second):
				runErr = fmt.Errorf("replicator did not shut down gracefully")
			}
		})
	}
	t.Cleanup(stopAndWait)

	committedID := "evt-committed-" + suffix
	inTransaction(t, ctx, admin, "ord-committed-"+suffix, true,
		[]string{eventJSON(committedID, "order.paid")})
	waitForID(t, ctx, store, committedID)
	rows, err := store.ListEvents(ctx, 100)
	if err != nil {
		t.Fatal(err)
	}
	committed := findEvent(rows, committedID)
	if committed == nil {
		t.Fatalf("committed event %q was not captured", committedID)
	}
	var payload map[string]any
	if err := json.Unmarshal(committed.Payload, &payload); err != nil {
		t.Fatalf("stored payload is invalid: %v", err)
	}
	if payload["id"] != committedID || committed.CommitEndLSN == "" || committed.TransactionID == 0 {
		t.Fatalf("stored event metadata is incomplete: %#v", committed)
	}

	rolledBackID := "evt-rolled-back-" + suffix
	rolledBackOrderID := "ord-rolled-back-" + suffix
	inTransaction(t, ctx, admin, rolledBackOrderID, false,
		[]string{eventJSON(rolledBackID, "order.paid")})
	markerID := "evt-after-rollback-" + suffix
	inTransaction(t, ctx, admin, "ord-marker-"+suffix, true,
		[]string{eventJSON(markerID, "marker")})
	waitForID(t, ctx, store, markerID)
	rows, _ = store.ListEvents(ctx, 100)
	if findEvent(rows, rolledBackID) != nil {
		t.Fatalf("rolled-back event %q reached the spool", rolledBackID)
	}
	var rolledBackBusinessRows int
	if err := admin.QueryRow(ctx,
		`SELECT count(*) FROM writerelay_integration_orders WHERE id=$1`, rolledBackOrderID,
	).Scan(&rolledBackBusinessRows); err != nil || rolledBackBusinessRows != 0 {
		t.Fatalf("rolled-back business row persisted: count=%d err=%v", rolledBackBusinessRows, err)
	}

	firstID := "evt-first-" + suffix
	secondID := "evt-second-" + suffix
	inTransaction(t, ctx, admin, "ord-batch-"+suffix, true, []string{
		eventJSON(firstID, "batch.item"),
		eventJSON(secondID, "batch.item"),
	})
	waitForID(t, ctx, store, secondID)
	rows, _ = store.ListEvents(ctx, 100)
	first := findEvent(rows, firstID)
	second := findEvent(rows, secondID)
	if first == nil || second == nil ||
		first.Sequence >= second.Sequence ||
		first.MessageIndex != 0 || second.MessageIndex != 1 ||
		first.TransactionID != second.TransactionID ||
		first.CommitEndLSN != second.CommitEndLSN {
		t.Fatalf("transaction order/metadata not preserved: first=%#v second=%#v", first, second)
	}

	durable, err := store.LastDurableLSN(ctx)
	if err != nil || durable == 0 {
		t.Fatalf("durable checkpoint: %s, %v", durable, err)
	}
	waitForConfirmedLSN(t, ctx, admin, slot, durable)

	stopAndWait()
	if runErr != nil {
		t.Fatalf("replicator shutdown: %v", runErr)
	}
}

func assertSQLRejects(t *testing.T, ctx context.Context, conn *pgx.Conn, payload string) {
	t.Helper()
	if _, err := conn.Exec(ctx, `SELECT writerelay.emit($1::jsonb)`, payload); err == nil {
		t.Fatalf("SQL API accepted invalid payload")
	}
}

func inTransaction(
	t *testing.T,
	ctx context.Context,
	conn *pgx.Conn,
	orderID string,
	commit bool,
	events []string,
) {
	t.Helper()
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO writerelay_integration_orders(id, status)
		VALUES ($1, 'paid')
		ON CONFLICT (id) DO UPDATE SET status=EXCLUDED.status
	`, orderID); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatalf("write business row: %v", err)
	}
	for _, payload := range events {
		if _, err := tx.Exec(ctx, `SELECT writerelay.emit($1::jsonb)`, payload); err != nil {
			_ = tx.Rollback(ctx)
			t.Fatalf("emit event: %v", err)
		}
	}
	if commit {
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
	} else if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
}

func eventJSON(id, eventType string) string {
	return fmt.Sprintf(
		`{"specversion":"1.0","id":%q,"source":"urn:integration","type":%q,"data":{"ok":true}}`,
		id, eventType,
	)
}

func waitForID(t *testing.T, ctx context.Context, store *sqlitespool.Store, id string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		rows, err := store.ListEvents(ctx, 100)
		if err == nil && findEvent(rows, id) != nil {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for event %q", id)
}

func waitForConfirmedLSN(
	t *testing.T,
	ctx context.Context,
	conn *pgx.Conn,
	slot string,
	durable pglogrepl.LSN,
) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var value string
		err := conn.QueryRow(ctx,
			`SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name=$1`, slot,
		).Scan(&value)
		if err == nil {
			confirmed, parseErr := pglogrepl.ParseLSN(value)
			if parseErr == nil && confirmed >= durable {
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("slot did not confirm durable checkpoint %s", durable)
}

func findEvent(rows []sqlitespool.EventRow, id string) *sqlitespool.EventRow {
	for index := range rows {
		if rows[index].ID == id {
			return &rows[index]
		}
	}
	return nil
}
