//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/johnathondillon/write-relay/internal/config"
	"github.com/johnathondillon/write-relay/internal/delivery"
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
	var webhookMu sync.Mutex
	webhookAttempts := make(map[string]int)
	webhookServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		defer request.Body.Close()
		var envelope struct {
			ID string `json:"id"`
		}
		if err := json.NewDecoder(request.Body).Decode(&envelope); err != nil {
			http.Error(w, "invalid event", http.StatusBadRequest)
			return
		}
		webhookMu.Lock()
		webhookAttempts[envelope.ID]++
		attempt := webhookAttempts[envelope.ID]
		webhookMu.Unlock()
		if strings.Contains(envelope.ID, "retry") && attempt == 1 {
			http.Error(w, "try again", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer webhookServer.Close()

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
		Delivery: config.DeliveryConfig{
			PollInterval:   20 * time.Millisecond,
			RequestTimeout: time.Second,
			Retry: config.RetryConfig{
				InitialDelay: 50 * time.Millisecond,
				MaxDelay:     time.Second,
				MaxAttempts:  3,
			},
			Sinks: []config.SinkConfig{{
				Name: "integration_webhook", Type: "webhook", URL: webhookServer.URL,
				AllowInsecureHTTP: true,
			}},
		},
		Logging: config.LoggingConfig{Level: "info", Format: "text"},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
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
	var serverVersion string
	var serverVersionNum int
	if err := admin.QueryRow(ctx,
		`SELECT current_setting('server_version'), current_setting('server_version_num')::integer`,
	).Scan(&serverVersion, &serverVersionNum); err != nil {
		t.Fatalf("read PostgreSQL version: %v", err)
	}
	t.Logf("PostgreSQL %s (server_version_num=%d)", serverVersion, serverVersionNum)
	if expected := os.Getenv("WRITERELAY_INTEGRATION_POSTGRES_MAJOR"); expected != "" {
		major, err := strconv.Atoi(expected)
		if err != nil || serverVersionNum/10000 != major {
			t.Fatalf("expected PostgreSQL major %q, connected to %s", expected, serverVersion)
		}
	}

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
	webhookSender, registration, err := delivery.NewWebhookSender(
		cfg.Delivery.Sinks[0], cfg.Delivery.RequestTimeout,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.ConfigureSinks(ctx, []delivery.SinkRegistration{registration}); err != nil {
		t.Fatal(err)
	}
	stopAndWait, replicator := startRuntime(t, cfg, store, webhookSender, logger)

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
	waitForWebhookAttempts(t, committedID, 1, &webhookMu, webhookAttempts)

	rolledBackID := "evt-rolled-back-" + suffix
	rolledBackOrderID := "ord-rolled-back-" + suffix
	inTransaction(t, ctx, admin, rolledBackOrderID, false,
		[]string{eventJSON(rolledBackID, "order.paid")})
	markerID := "evt-after-rollback-" + suffix
	inTransaction(t, ctx, admin, "ord-marker-"+suffix, true,
		[]string{eventJSON(markerID, "marker")})
	waitForID(t, ctx, store, markerID)
	rows, err = store.ListEvents(ctx, 100)
	if err != nil {
		t.Fatal(err)
	}
	if findEvent(rows, rolledBackID) != nil {
		t.Fatalf("rolled-back event %q reached the spool", rolledBackID)
	}
	webhookMu.Lock()
	rolledBackAttempts := webhookAttempts[rolledBackID]
	webhookMu.Unlock()
	if rolledBackAttempts != 0 {
		t.Fatalf("rolled-back event %q reached webhook", rolledBackID)
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
	rows, err = store.ListEvents(ctx, 100)
	if err != nil {
		t.Fatal(err)
	}
	first := findEvent(rows, firstID)
	second := findEvent(rows, secondID)
	if first == nil || second == nil ||
		first.Sequence >= second.Sequence ||
		first.MessageIndex != 0 || second.MessageIndex != 1 ||
		first.TransactionID != second.TransactionID ||
		first.CommitEndLSN != second.CommitEndLSN {
		t.Fatalf("transaction order/metadata not preserved: first=%#v second=%#v", first, second)
	}
	waitForWebhookAttempts(t, secondID, 1, &webhookMu, webhookAttempts)

	retryID := "evt-retry-" + suffix
	inTransaction(t, ctx, admin, "ord-retry-"+suffix, true,
		[]string{eventJSON(retryID, "order.retry")})
	waitForWebhookAttempts(t, retryID, 2, &webhookMu, webhookAttempts)
	waitForDelivered(t, ctx, store, retryID, 2)

	durable, err := store.LastDurableLSN(ctx)
	if err != nil || durable == 0 {
		t.Fatalf("durable checkpoint: %s, %v", durable, err)
	}
	waitForConfirmedLSN(t, ctx, admin, slot, durable)

	// Terminate only this test's replication connection. Observation must report
	// disconnected during backoff and become connected again after streaming starts.
	waitForCaptureConnection(t, replicator, true)
	beforeStatus := replicator.Status()
	if beforeStatus.Transactions == 0 || beforeStatus.LastCaptureUnix == 0 {
		t.Fatalf("missing capture progress: %+v", beforeStatus)
	}
	var terminated bool
	if err := admin.QueryRow(ctx, `SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots WHERE slot_name=$1`, slot).Scan(&terminated); err != nil || !terminated {
		t.Fatalf("terminate test replication connection: %v, %v", terminated, err)
	}
	waitForCaptureConnection(t, replicator, false)
	waitForCaptureConnection(t, replicator, true)
	t.Log("capture status followed stream disconnect and reconnect")

	if err := stopAndWait(); err != nil {
		t.Fatalf("runtime shutdown: %v", err)
	}

	if replicator.Status().Connected {
		t.Fatal("capture still connected after shutdown")
	}

	// Reopen the real spool, then catch up with transactions committed offline.
	// Re-emitting identical content exercises identity replay through pgoutput;
	// it does not force the server to resend already acknowledged WAL.
	beforeEvents, err := store.ListEvents(ctx, 100)
	if err != nil {
		t.Fatal(err)
	}
	beforeDeliveries, err := store.ListDeliveries(ctx, "", 100)
	if err != nil {
		t.Fatal(err)
	}
	checkpoint, err := store.LastDurableLSN(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := sqlitespool.Open(ctx, cfg.Spool.Path)
	if err != nil {
		t.Fatal(err)
	}
	store = reopened
	if got, err := store.LastDurableLSN(ctx); err != nil || got != checkpoint {
		t.Fatalf("checkpoint changed on reopen: got=%s want=%s err=%v", got, checkpoint, err)
	}
	if err := store.ConfigureSinks(ctx, []delivery.SinkRegistration{registration}); err != nil {
		t.Fatal(err)
	}
	offlineID := "evt-offline-" + suffix
	inTransaction(t, ctx, admin, "ord-offline-"+suffix, true, []string{
		eventJSON(committedID, "order.paid"),
		eventJSON(offlineID, "order.offline"),
	})
	stopRestart, _ := startRuntime(t, cfg, store, webhookSender, logger)
	waitForID(t, ctx, store, offlineID)
	waitForDelivered(t, ctx, store, offlineID, 1)
	afterCheckpoint, err := store.LastDurableLSN(ctx)
	if err != nil || afterCheckpoint <= checkpoint {
		t.Fatalf("restart did not advance checkpoint: before=%s after=%s err=%v", checkpoint, afterCheckpoint, err)
	}
	waitForConfirmedLSN(t, ctx, admin, slot, afterCheckpoint)
	if err := stopRestart(); err != nil {
		t.Fatalf("restarted runtime shutdown: %v", err)
	}
	afterEvents, err := store.ListEvents(ctx, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(afterEvents) != len(beforeEvents)+1 {
		t.Fatalf("restart/replay changed event count: before=%d after=%d", len(beforeEvents), len(afterEvents))
	}
	for _, before := range beforeEvents {
		after := findEvent(afterEvents, before.ID)
		if after == nil || !reflect.DeepEqual(before, *after) {
			t.Fatalf("restart/replay changed stored event %q", before.ID)
		}
	}
	afterDeliveries, err := store.ListDeliveries(ctx, "", 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(afterDeliveries) != len(beforeDeliveries)+1 {
		t.Fatalf("restart/replay changed delivery count: before=%d after=%d", len(beforeDeliveries), len(afterDeliveries))
	}
	for _, before := range beforeDeliveries {
		found := false
		for _, after := range afterDeliveries {
			if before.ID == after.ID && before.SinkName == after.SinkName {
				found = reflect.DeepEqual(before, after)
				break
			}
		}
		if !found {
			t.Fatalf("restart/replay changed delivery %q", before.ID)
		}
	}
	webhookMu.Lock()
	replayedAttempts := webhookAttempts[committedID]
	webhookMu.Unlock()
	if replayedAttempts != 1 {
		t.Fatalf("already-delivered identity was sent again: attempts=%d", replayedAttempts)
	}
	// Pruning must retain identities even when replay comes through PostgreSQL.
	pruned, err := sqlitespool.Prune(ctx, cfg.Spool.Path, sqlitespool.PruneOptions{Before: time.Now().UTC(), Limit: 1000})
	if err != nil || pruned.Events != len(afterEvents) {
		t.Fatalf("prune delivered payloads: events=%d err=%v", pruned.Events, err)
	}
	afterPruneID := "evt-after-prune-" + suffix
	inTransaction(t, ctx, admin, "ord-after-prune-"+suffix, true, []string{
		eventJSON(committedID, "order.paid"),
		eventJSON(afterPruneID, "order.after-prune"),
	})
	stopAfterPrune, _ := startRuntime(t, cfg, store, webhookSender, logger)
	waitForDelivered(t, ctx, store, afterPruneID, 1)
	if err := stopAfterPrune(); err != nil {
		t.Fatal(err)
	}
	stats, err := sqlitespool.ReadStats(ctx, cfg.Spool.Path)
	if err != nil || stats.PrunedPayloads != int64(len(afterEvents)) || stats.EventCount != int64(len(afterEvents)+1) {
		t.Fatalf("pruned identity replay changed payload/identity counts: %+v err=%v", stats, err)
	}
	webhookMu.Lock()
	prunedReplayAttempts := webhookAttempts[committedID]
	webhookMu.Unlock()
	if prunedReplayAttempts != 1 {
		t.Fatalf("pruned identity redelivered: attempts=%d", prunedReplayAttempts)
	}
	t.Log("verified commit, rollback, ordering, retry, ACK, restart, offline capture, identity replay, and payload pruning")
}

func startRuntime(
	t *testing.T,
	cfg config.Config,
	store *sqlitespool.Store,
	sender delivery.Sink,
	logger *slog.Logger,
) (func() error, *commitpostgres.Replicator) {
	t.Helper()
	return startRuntimeWithReplicator(t, cfg, store, sender, logger, commitpostgres.NewReplicator(cfg, store, logger))
}

func startRuntimeWithReplicator(t *testing.T, cfg config.Config, store *sqlitespool.Store, sender delivery.Sink, logger *slog.Logger, replicator *commitpostgres.Replicator) (func() error, *commitpostgres.Replicator) {
	t.Helper()
	runCtx, stopRun := context.WithCancel(context.Background())
	runDone := make(chan error, 2)
	if replicator.Status().Connected {
		t.Fatal("replicator ready before startup")
	}
	go func() {
		runDone <- replicator.Run(runCtx)
	}()
	go func() {
		worker := delivery.NewWorker(
			store, map[string]delivery.Sink{"integration_webhook": sender},
			cfg.Delivery.PollInterval, cfg.Delivery.Retry.InitialDelay,
			cfg.Delivery.Retry.MaxDelay, cfg.Delivery.Retry.MaxAttempts, logger,
		)
		runDone <- worker.Run(runCtx)
	}()
	var stopOnce sync.Once
	var runErr error
	stopAndWait := func() error {
		stopOnce.Do(func() {
			stopRun()
			for component := 0; component < 2; component++ {
				select {
				case err := <-runDone:
					if err != nil && runErr == nil {
						runErr = err
					}
				case <-time.After(5 * time.Second):
					runErr = fmt.Errorf("runtime component did not shut down gracefully")
				}
			}
		})
		return runErr
	}
	t.Cleanup(func() {
		if err := stopAndWait(); err != nil {
			t.Errorf("runtime cleanup: %v", err)
		}
	})
	return stopAndWait, replicator
}

func waitForWebhookAttempts(
	t *testing.T,
	id string,
	want int,
	mu *sync.Mutex,
	attempts map[string]int,
) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		got := attempts[id]
		mu.Unlock()
		if got >= want {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d webhook attempts for %q", want, id)
}

func findDelivered(rows []sqlitespool.DeliveryRow, id string, attempts int) bool {
	for _, row := range rows {
		if row.ID == id && row.State == "delivered" && row.Attempts == attempts {
			return true
		}
	}
	return false
}

func waitForDelivered(
	t *testing.T,
	ctx context.Context,
	store *sqlitespool.Store,
	id string,
	attempts int,
) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		rows, err := store.ListDeliveries(ctx, "delivered", 100)
		if err == nil && findDelivered(rows, id, attempts) {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for durable delivery state for %q", id)
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

func waitForCaptureConnection(t *testing.T, replicator *commitpostgres.Replicator, connected bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if replicator.Status().Connected == connected {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("capture connection did not become %v: %+v", connected, replicator.Status())
}
