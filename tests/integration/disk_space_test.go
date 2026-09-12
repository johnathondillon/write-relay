//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/johnathondillon/write-relay/internal/config"
	"github.com/johnathondillon/write-relay/internal/delivery"
	commitpostgres "github.com/johnathondillon/write-relay/internal/postgres"
	sqlitespool "github.com/johnathondillon/write-relay/internal/spool/sqlite"
)

func TestDiskPauseKeepsDeliveryRunningAndReplaysAfterRestart(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	dsn := os.Getenv("WRITERELAY_INTEGRATION_DSN")
	if dsn == "" {
		dsn = "postgres://postgres:postgres-dev-password@localhost:5432/writerelay?sslmode=disable"
	}
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close(context.Background())
	suffix := strconv.FormatInt(time.Now().UnixNano(), 36)
	slot, publication := "wr_disk_"+suffix, "wr_disk_pub_"+suffix
	var receiverReady atomic.Bool
	var mu sync.Mutex
	var received []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		if !receiverReady.Load() {
			http.Error(w, "outage", 503)
			return
		}
		var event struct {
			ID string `json:"id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
			http.Error(w, "bad event", 400)
			return
		}
		mu.Lock()
		received = append(received, event.ID)
		mu.Unlock()
		w.WriteHeader(204)
	}))
	defer server.Close()
	cfg := config.Config{
		Version:  config.CurrentVersion,
		Postgres: config.PostgresConfig{DSN: dsn, Slot: slot, Publication: publication, MessagePrefix: config.RequiredPrefix, StatusInterval: 50 * time.Millisecond, MaxTransactionEvents: 100, MaxTransactionBytes: 1024 * 1024},
		Spool:    config.SpoolConfig{Path: filepath.Join(t.TempDir(), "spool.sqlite"), MaxEventBytes: config.DefaultMaxEventBytes, DiskSpace: config.DiskSpaceConfig{PauseBelowBytes: 100, ResumeAtBytes: 200, CheckInterval: 20 * time.Millisecond}},
		Delivery: config.DeliveryConfig{PollInterval: 10 * time.Millisecond, RequestTimeout: time.Second, Retry: config.RetryConfig{InitialDelay: 30 * time.Millisecond, MaxDelay: 100 * time.Millisecond, MaxAttempts: 100}, Sinks: []config.SinkConfig{{Name: "integration_webhook", Type: "webhook", URL: server.URL, AllowInsecureHTTP: true}}},
	}
	if _, err := commitpostgres.Setup(ctx, cfg, true); err != nil {
		t.Fatal(err)
	}
	defer func() {
		cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		for _, q := range []string{"SELECT pg_drop_replication_slot('" + slot + "')", "DROP PUBLICATION " + publication} {
			if _, err := admin.Exec(cleanup, q); err != nil {
				t.Error(err)
			}
		}
	}()
	store, err := sqlitespool.Open(ctx, cfg.Spool.Path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	sender, registration, err := delivery.NewWebhookSender(cfg.Delivery.Sinks[0], time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.ConfigureSinks(ctx, []delivery.SinkRegistration{registration}); err != nil {
		t.Fatal(err)
	}
	var free atomic.Uint64
	free.Store(200)
	var broken atomic.Bool
	probe := func(path string) (uint64, error) {
		if path != cfg.Spool.Path {
			return 0, errors.New("wrong filesystem path")
		}
		if broken.Load() {
			return 0, errors.New("injected statfs error")
		}
		return free.Load(), nil
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	start := func() (func() error, *commitpostgres.Replicator) {
		r := commitpostgres.NewReplicatorWithDiskProbe(cfg, store, logger, probe)
		return startRuntimeWithReplicator(t, cfg, store, sender, logger, r)
	}
	stop, r := start()
	defer func() {
		if err := stop(); err != nil {
			t.Error(err)
		}
	}()
	wait := func(label string, condition func() bool) {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for !condition() {
			if time.Now().After(deadline) || ctx.Err() != nil {
				t.Fatal("timed out: " + label)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	emit := func(rollback bool, ids ...string) {
		t.Helper()
		tx, err := admin.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(context.Background())
		for _, id := range ids {
			payload := fmt.Sprintf(`{"specversion":"1.0","id":%q,"source":"urn:disk-test","type":"disk.test"}`, id)
			if _, err := tx.Exec(ctx, "SELECT writerelay.emit($1::jsonb)", payload); err != nil {
				t.Fatal(err)
			}
		}
		if rollback {
			err = tx.Rollback(ctx)
		} else {
			err = tx.Commit(ctx)
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	emit(false, "before-pause")
	waitForID(t, ctx, store, "before-pause")
	wait("first delivery retry persisted", func() bool {
		rows, err := store.ListDeliveries(ctx, "", 10)
		return err == nil && len(rows) == 1 && rows[0].State == "retry_wait"
	})
	checkpoint, err := store.LastDurableLSN(ctx)
	if err != nil {
		t.Fatal(err)
	}
	waitForConfirmedLSN(t, ctx, admin, slot, checkpoint)
	free.Store(99)
	wait("capture disconnects on low disk", func() bool { s := r.Status(); return s.Disk.Paused && !s.Connected })
	var confirmedBefore string
	if err := admin.QueryRow(ctx, "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name=$1", slot).Scan(&confirmedBefore); err != nil {
		t.Fatal(err)
	}
	emit(false, "paused-a", "paused-b") // One PostgreSQL transaction, not partial capture.
	emit(true, "rolled-back")
	receiverReady.Store(true)
	// The outage may produce several attempts; the successful retry must still
	// finish while capture is disconnected.
	wait("delivery drains while capture is paused", func() bool {
		rows, err := store.ListDeliveries(ctx, "delivered", 10)
		return err == nil && len(rows) == 1 && rows[0].ID == "before-pause" && rows[0].Attempts >= 2
	})
	assertPaused := func() {
		t.Helper()
		current, err := store.LastDurableLSN(ctx)
		if err != nil || current != checkpoint {
			t.Fatal("paused checkpoint moved", current, checkpoint, err)
		}
		count, err := store.EventCount(ctx)
		if err != nil || count != 1 {
			t.Fatal("paused capture wrote events", count, err)
		}
		var confirmed string
		if err := admin.QueryRow(ctx, "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name=$1", slot).Scan(&confirmed); err != nil || confirmed != confirmedBefore {
			t.Fatal("paused ACK advanced", confirmed, confirmedBefore, err)
		}
	}
	assertPaused()
	if err := stop(); err != nil {
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
	emit(false, "offline-marker")
	stop, r = start()
	wait("restart stays paused", func() bool { return r.Status().Disk.Paused })
	free.Store(150)
	wait("hysteresis sample", func() bool { return r.Status().Disk.AvailableBytes == 150 })
	assertPaused()
	free.Store(200)
	waitForDelivered(t, ctx, store, "offline-marker", 1)
	wait("stream recovers", func() bool { s := r.Status(); return s.Connected && !s.Disk.Paused })
	mu.Lock()
	got := append([]string(nil), received...)
	mu.Unlock()
	if !reflect.DeepEqual(got, []string{"before-pause", "paused-a", "paused-b", "offline-marker"}) {
		t.Fatal("delivery order or replay changed", got)
	}
	if count, err := store.EventCount(ctx); err != nil || count != 4 {
		t.Fatal(count, err)
	}
	// Measurement failures pause conservatively and recover without process exit.
	broken.Store(true)
	wait("probe error pause", func() bool { s := r.Status(); return s.Disk.Reason == "disk_check_failed" && !s.Connected })
	emit(false, "after-probe-error")
	broken.Store(false)
	waitForDelivered(t, ctx, store, "after-probe-error", 1)
	durable, err := store.LastDurableLSN(ctx)
	if err != nil || durable <= checkpoint {
		t.Fatal(durable, err)
	}
	waitForConfirmedLSN(t, ctx, admin, slot, durable)
	t.Log("verified pause without ACK advancement, ongoing delivery, low-disk restart, hysteresis, ordered replay, rollback absence, and probe-error recovery")
}
