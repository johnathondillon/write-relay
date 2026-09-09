package sqlite

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/johnathondillon/write-relay/internal/delivery"
)

func TestStatsCountsWaitingAgeAndInactiveSinks(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "spool.sqlite")
	store, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	active, retired, empty := testSink("active", "a"), testSink("retired", "b"), testSink("empty", "c")
	if err := store.ConfigureSinks(ctx, []delivery.SinkRegistration{empty}); err != nil {
		t.Fatal(err)
	}
	if err := store.ConfigureSinks(ctx, []delivery.SinkRegistration{active, retired}); err != nil {
		t.Fatal(err)
	}
	base := time.Now().UTC().Truncate(time.Second)
	states := []string{"delivered", "dead_letter", "retry_wait", "pending"}
	for i, state := range states {
		batch := testBatchWithIdentity("urn:test", state, uint64(0x100+i*0x100))
		if _, err := store.PersistCommittedBatch(ctx, batch); err != nil {
			t.Fatal(err)
		}
		// Controlled capture times distinguish waiting age from commit/attempt time.
		captured := base.Add(-time.Duration(4-i) * time.Hour)
		if _, err := store.db.ExecContext(ctx, "UPDATE events SET captured_at = ? WHERE event_id = ?", formatTimestamp(captured), state); err != nil {
			t.Fatal(err)
		}
		if _, err := store.db.ExecContext(ctx, `UPDATE deliveries SET state = ? WHERE event_sequence = ? AND sink_id = (SELECT sink_id FROM delivery_sinks WHERE sink_name = 'active')`, state, i+1); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := store.db.ExecContext(ctx, `UPDATE deliveries SET state = 'delivered' WHERE sink_id = (SELECT sink_id FROM delivery_sinks WHERE sink_name = 'retired')`); err != nil {
		t.Fatal(err)
	}
	if err := store.ConfigureSinks(ctx, []delivery.SinkRegistration{active}); err != nil {
		t.Fatal(err)
	}

	stats, err := ReadStats(ctx, path) // writer remains open; committed data is in WAL
	if err != nil {
		t.Fatal(err)
	}
	if stats.EventCount != 4 || stats.Deliveries != (DeliveryCounts{Total: 8, Pending: 1, RetryWait: 1, Delivered: 5, DeadLetter: 1}) {
		t.Fatalf("unexpected totals: %+v", stats)
	}
	if len(stats.Sinks) != 3 || stats.Sinks[0].Name != "active" || !stats.Sinks[0].Active || stats.Sinks[1].Name != "empty" || stats.Sinks[1].Active || stats.Sinks[2].Name != "retired" || stats.Sinks[2].Active {
		t.Fatalf("unexpected sinks: %+v", stats.Sinks)
	}
	if stats.Sinks[1].Deliveries.Total != 0 || stats.Sinks[1].OldestWaiting != nil || stats.Sinks[2].Deliveries.Delivered != 4 || stats.Sinks[2].OldestWaiting != nil {
		t.Fatalf("empty/terminal sinks: %+v", stats.Sinks)
	}
	wantCaptured := base.Add(-2 * time.Hour)
	if stats.OldestWaiting == nil || !stats.OldestWaiting.CapturedAt.Equal(wantCaptured) || stats.OldestWaiting.AgeSeconds != int64(stats.SampledAt.Sub(wantCaptured)/time.Second) {
		t.Fatalf("waiting age includes a terminal event or wrong time: %+v", stats.OldestWaiting)
	}
	lsn, err := store.LastDurableLSN(ctx)
	if err != nil || stats.LastDurableLSN != lsn.String() {
		t.Fatalf("checkpoint: %s %v", stats.LastDurableLSN, err)
	}
	if stats.Storage.WALBytes <= 0 || stats.Storage.SHMBytes <= 0 || stats.Storage.TotalBytes != stats.Storage.DatabaseBytes+stats.Storage.WALBytes+stats.Storage.SHMBytes {
		t.Fatalf("live WAL not measured: %+v", stats.Storage)
	}

	// An in-progress writer must not leak half-committed state into the report.
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, "UPDATE deliveries SET state = 'pending'"); err != nil {
		t.Fatal(err)
	}
	uncommitted, err := ReadStats(ctx, path)
	if err != nil || uncommitted.Deliveries != stats.Deliveries {
		t.Fatalf("uncommitted data visible: %+v, %v", uncommitted, err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}

	if err := store.RedriveDelivery(ctx, "active", "urn:test", "dead_letter", base); err != nil {
		t.Fatal(err)
	}
	redriven, err := ReadStats(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	if redriven.Deliveries.DeadLetter != 0 || redriven.Deliveries.RetryWait != 2 || !redriven.OldestWaiting.CapturedAt.Equal(base.Add(-3*time.Hour)) {
		t.Fatalf("redrive must retain original capture age: %+v", redriven)
	}
}

func TestStatsEmptyCaptureOnlyAndBackfill(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "spool.sqlite")
	store, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	stats, err := ReadStats(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	if stats.EventCount != 0 || stats.Sinks == nil || len(stats.Sinks) != 0 || stats.OldestWaiting != nil || stats.LastDurableLSN != "0/0" {
		t.Fatalf("empty spool: %+v", stats)
	}
	batch := testBatchWithIdentity("urn:test", "one", 0x100)
	if _, err := store.PersistCommittedBatch(ctx, batch); err != nil {
		t.Fatal(err)
	}
	stats, err = ReadStats(ctx, path)
	if err != nil || stats.EventCount != 1 || stats.Deliveries.Total != 0 || stats.OldestWaiting != nil {
		t.Fatalf("capture only: %+v %v", stats, err)
	}
	// A future wall-clock capture value must not produce a negative age.
	if _, err := store.db.ExecContext(ctx, "UPDATE events SET captured_at = ?", formatTimestamp(time.Now().Add(time.Hour))); err != nil {
		t.Fatal(err)
	}
	if err := store.ConfigureSinks(ctx, []delivery.SinkRegistration{testSink("new", "target")}); err != nil {
		t.Fatal(err)
	}
	stats, err = ReadStats(ctx, path)
	if err != nil || stats.Deliveries.Pending != 1 || stats.OldestWaiting == nil || stats.OldestWaiting.AgeSeconds != 0 {
		t.Fatalf("backfill/future clock: %+v %v", stats, err)
	}
}

func TestStatsDoesNotCreateMigrateOrChmod(t *testing.T) {
	ctx := context.Background()
	directory := t.TempDir()
	missing := filepath.Join(directory, "missing", "spool.sqlite")
	if _, err := ReadStats(ctx, missing); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("missing spool error = %v", err)
	}
	if _, err := os.Stat(filepath.Dir(missing)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stats created a directory: %v", err)
	}
	for _, version := range []int{1, currentSchemaVersion + 1} {
		path := filepath.Join(t.TempDir(), "legacy.sqlite")
		db, err := sql.Open("sqlite", path)
		if err != nil {
			t.Fatal(err)
		}
		initial, err := migrations.ReadFile("migrations/001_initial.sql")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.ExecContext(ctx, string(initial)); err != nil {
			t.Fatal(err)
		}
		if _, err := db.ExecContext(ctx, "UPDATE schema_migrations SET version = ?", version); err != nil {
			t.Fatal(err)
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		before, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := ReadStats(ctx, path); err == nil || !strings.Contains(err.Error(), "does not migrate") {
			t.Fatalf("schema %d: %v", version, err)
		}
		after, err := os.ReadFile(path)
		if err != nil || string(before) != string(after) {
			t.Fatalf("stats changed schema %d: %v", version, err)
		}
	}
	path := filepath.Join(directory, "original.sqlite")
	store, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	// URI metacharacters must be treated as a filename, not SQLite options.
	escaped := filepath.Join(directory, "spool ?#%.sqlite")
	if err := os.Rename(path, escaped); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(escaped, 0o400); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(escaped)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ReadStats(ctx, escaped); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(escaped)
	if err != nil || info.Mode().Perm() != 0o400 {
		t.Fatalf("stats changed permissions: %v", err)
	}
	after, err := os.ReadFile(escaped)
	if err != nil || string(before) != string(after) {
		t.Fatalf("stats changed database: %v", err)
	}
	link := filepath.Join(directory, "link.sqlite")
	if err := os.Symlink(escaped, link); err != nil {
		t.Fatal(err)
	}
	for _, invalid := range []string{link, directory} {
		if _, err := ReadStats(ctx, invalid); err == nil {
			t.Fatalf("accepted non-file %s", invalid)
		}
	}
}

func TestStatsCountsBeyondInspectionRowLimit(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "spool.sqlite")
	store, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.ConfigureSinks(ctx, []delivery.SinkRegistration{testSink("all", "target")}); err != nil {
		t.Fatal(err)
	}
	batch := testBatchWithIdentity("urn:test", "base", 0x100)
	template := batch.Events[0]
	batch.Events = nil
	for i := 0; i < 1001; i++ {
		event := template
		event.ID = fmt.Sprintf("event-%d", i)
		event.Payload = []byte(fmt.Sprintf(`{"specversion":"1.0","source":"urn:test","id":%q,"type":"created"}`, event.ID))
		event.PayloadSHA256 = sha256.Sum256(event.Payload)
		event.MessageIndex = i
		batch.Events = append(batch.Events, event)
	}
	if _, err := store.PersistCommittedBatch(ctx, batch); err != nil {
		t.Fatal(err)
	}
	stats, err := ReadStats(ctx, path)
	if err != nil || stats.EventCount != 1001 || stats.Deliveries.Pending != 1001 || stats.Sinks[0].Deliveries.Total != 1001 {
		t.Fatalf("stats truncated to an inspection page: %+v %v", stats, err)
	}
}

func TestStatsFileSizesAndCancellation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "spool.sqlite")
	if err := os.WriteFile(path, make([]byte, 123), 0o600); err != nil {
		t.Fatal(err)
	}
	sizes, err := spoolFileSizes(path)
	if err != nil || sizes != (FileSizes{DatabaseBytes: 123, TotalBytes: 123}) {
		t.Fatalf("missing sidecars: %+v %v", sizes, err)
	}
	if err := os.WriteFile(path+"-wal", make([]byte, 45), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path+"-shm", make([]byte, 6), 0o600); err != nil {
		t.Fatal(err)
	}
	sizes, err = spoolFileSizes(path)
	if err != nil || sizes != (FileSizes{DatabaseBytes: 123, WALBytes: 45, SHMBytes: 6, TotalBytes: 174}) {
		t.Fatalf("sidecar total: %+v %v", sizes, err)
	}
	if _, err := ReadStats(context.Background(), path); err == nil {
		t.Fatal("accepted corrupt database")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := ReadStats(ctx, path); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled context: %v", err)
	}
}
