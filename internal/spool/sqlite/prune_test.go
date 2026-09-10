package sqlite

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/johnathondillon/write-relay/internal/delivery"
	"github.com/johnathondillon/write-relay/internal/failure"
	"github.com/johnathondillon/write-relay/internal/spool"
)

func pruneFixture(t *testing.T, ids ...string) (*Store, string, time.Time) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "spool.sqlite")
	store, err := Open(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if err := store.ConfigureSinks(t.Context(), []delivery.SinkRegistration{testSink("a", "a"), testSink("b", "b")}); err != nil {
		t.Fatal(err)
	}
	for index, id := range ids {
		if _, err := store.PersistCommittedBatch(t.Context(), testBatchWithIdentity("urn:test", id, uint64(index+1)*0x100)); err != nil {
			t.Fatal(err)
		}
	}
	cutoff := time.Now().UTC().Add(-24 * time.Hour)
	// A known, old successful delivery history for both sinks.
	if _, err := store.db.ExecContext(t.Context(), `UPDATE deliveries SET state='delivered', attempts=1, delivered_at=?`, formatTimestamp(cutoff.Add(-time.Hour))); err != nil {
		t.Fatal(err)
	}
	return store, path, cutoff
}

func TestPrunePolicyPreviewAndBatches(t *testing.T) {
	store, path, cutoff := pruneFixture(t, "old-first", "old-second", "pending", "retry", "dead", "boundary", "recent", "unknown", "no-deliveries")
	ctx := t.Context()
	for id, state := range map[string]string{"pending": "pending", "retry": "retry_wait", "dead": "dead_letter"} {
		if _, err := store.db.ExecContext(ctx, `UPDATE deliveries SET state=?, delivered_at=NULL WHERE sink_id=2 AND event_sequence=(SELECT sequence FROM events WHERE event_id=?)`, state, id); err != nil {
			t.Fatal(err)
		}
	}
	for id, at := range map[string]any{"boundary": formatTimestamp(cutoff), "recent": formatTimestamp(cutoff.Add(time.Nanosecond)), "unknown": nil} {
		if _, err := store.db.ExecContext(ctx, `UPDATE deliveries SET delivered_at=? WHERE sink_id=2 AND event_sequence=(SELECT sequence FROM events WHERE event_id=?)`, at, id); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := store.db.ExecContext(ctx, `DELETE FROM deliveries WHERE event_sequence=(SELECT sequence FROM events WHERE event_id='no-deliveries')`); err != nil {
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
	preview, err := Prune(ctx, path, PruneOptions{Before: cutoff, Limit: 1, DryRun: true})
	if err != nil {
		t.Fatal(err)
	}
	if preview.Events != 1 || preview.Entries[0].ID != "old-first" || preview.PayloadBytes == 0 || !preview.DryRun {
		t.Fatalf("preview: %+v", preview)
	}
	rows, err := store.ListEvents(ctx, 100)
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		if row.PayloadPrunedAt != nil || len(row.Payload) == 0 {
			t.Fatal("preview changed a payload")
		}
	}
	applied, err := Prune(ctx, path, PruneOptions{Before: cutoff, Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	if applied.DryRun || !reflect.DeepEqual(preview.Entries, applied.Entries) || applied.PayloadBytes != preview.PayloadBytes {
		t.Fatalf("apply differs from preview: %+v", applied)
	}
	remaining, err := Prune(ctx, path, PruneOptions{Before: cutoff, Limit: 1000})
	if err != nil {
		t.Fatal(err)
	}
	if remaining.Events != 1 || remaining.Entries[0].ID != "old-second" {
		t.Fatalf("remaining: %+v", remaining)
	}
	again, err := Prune(ctx, path, PruneOptions{Before: cutoff, Limit: 1000})
	if err != nil || again.Events != 0 {
		t.Fatalf("repeat: %+v %v", again, err)
	}
	rows, err = store.ListEvents(ctx, 100)
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		want := row.ID == "old-first" || row.ID == "old-second"
		if (row.PayloadPrunedAt != nil) != want || (len(row.Payload) == 0) != want {
			t.Fatalf("wrong payload retained/pruned: %s", row.ID)
		}
	}
	afterDeliveries, err := store.ListDeliveries(ctx, "", 100)
	if err != nil || !reflect.DeepEqual(beforeDeliveries, afterDeliveries) {
		t.Fatalf("delivery history changed: %v", err)
	}
	if _, err := store.db.ExecContext(ctx, `UPDATE events SET payload=X'', payload_pruned_at=? WHERE event_id='pending'`, formatTimestamp(time.Now())); err == nil {
		t.Fatal("database allowed pruning an unfinished delivery")
	}
	if got, err := store.LastDurableLSN(ctx); err != nil || got != checkpoint {
		t.Fatalf("checkpoint changed: %v", err)
	}
	stats, err := ReadStats(ctx, path)
	if err != nil || stats.EventCount != 9 || stats.PrunedPayloads != 2 {
		t.Fatalf("stats: %+v %v", stats, err)
	}
}

func TestPrunedIdentityReplayConflictAndBackfill(t *testing.T) {
	store, path, cutoff := pruneFixture(t, "old", "retained")
	ctx := t.Context()
	if _, err := store.db.ExecContext(ctx, `UPDATE deliveries SET delivered_at=? WHERE event_sequence=2`, formatTimestamp(time.Now())); err != nil {
		t.Fatal(err)
	}
	// Inactive sink history still participates in eligibility and is retained.
	if err := store.ConfigureSinks(ctx, []delivery.SinkRegistration{testSink("a", "a")}); err != nil {
		t.Fatal(err)
	}
	if result, err := Prune(ctx, path, PruneOptions{Before: cutoff, Limit: 1000}); err != nil || result.Events != 1 {
		t.Fatalf("prune: %+v %v", result, err)
	}
	if err := store.ConfigureSinks(ctx, []delivery.SinkRegistration{testSink("a", "a"), testSink("c", "c")}); err != nil {
		t.Fatal(err)
	}
	replay := testBatchWithIdentity("urn:test", "old", 0x500)
	if result, err := store.PersistCommittedBatch(ctx, replay); err != nil || result.Replayed != 1 || result.Inserted != 0 {
		t.Fatalf("replay: %+v %v", result, err)
	}
	rows, err := store.ListDeliveries(ctx, "", 100)
	if err != nil || len(rows) != 5 {
		t.Fatalf("backfill/replay delivery count: %d %v", len(rows), err)
	}
	for _, row := range rows {
		if row.SinkName == "c" && row.ID == "old" {
			t.Fatal("pruned event backfilled")
		}
	}
	conflict := testBatchWithIdentity("urn:test", "old", 0x600)
	conflict.Events[0].Payload = []byte(`{"changed":true}`)
	conflict.Events[0].PayloadSHA256 = sha256.Sum256(conflict.Events[0].Payload)
	if _, err := store.PersistCommittedBatch(ctx, conflict); !errors.Is(err, spool.ErrIdentityConflict) {
		t.Fatalf("conflict: %v", err)
	}
	if checkpoint, err := store.LastDurableLSN(ctx); err != nil || checkpoint != replay.CommitEndLSN {
		t.Fatalf("conflict advanced checkpoint: %v", err)
	}
	events, err := store.ListEvents(ctx, 100)
	if err != nil || len(events) != 2 || events[0].PayloadPrunedAt == nil || len(events[0].Payload) != 0 {
		t.Fatalf("replay restored payload: %v", err)
	}
	// Database guards prevent stale code paths from resurrecting payloads/deliveries.
	for _, query := range []string{
		`UPDATE events SET payload=X'01', payload_pruned_at=NULL WHERE sequence=1`,
		`UPDATE events SET payload_sha256=zeroblob(32) WHERE sequence=1`,
		`INSERT INTO deliveries(event_sequence,sink_id,state,next_attempt_at) VALUES(1,3,'pending','2000')`,
		`UPDATE deliveries SET state='retry_wait' WHERE event_sequence=1`,
	} {
		if _, err := store.db.ExecContext(ctx, query); err == nil {
			t.Fatalf("guard allowed: %s", query)
		}
	}
}

func TestPruneSerializesWithSinkRegistration(t *testing.T) {
	store, path, cutoff := pruneFixture(t, "old")
	ctx := t.Context()
	locked, release := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	go func() {
		_, err := pruneWithHooks(ctx, path, PruneOptions{Before: cutoff, Limit: 1000}, failure.Hooks{AfterPrunePayload: func(int) { close(locked); <-release }})
		done <- err
	}()
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	select {
	case <-locked:
	case err := <-done:
		t.Fatalf("prune ended before lock: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("prune did not acquire lock")
	}
	if _, err := store.db.ExecContext(ctx, "PRAGMA busy_timeout=1"); err != nil {
		t.Fatal(err)
	}
	sinks := []delivery.SinkRegistration{testSink("a", "a"), testSink("b", "b"), testSink("c", "c")}
	if err := store.ConfigureSinks(ctx, sinks); err == nil {
		t.Fatal("sink registration wrote through prune lock")
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if err := store.ConfigureSinks(ctx, sinks); err != nil {
		t.Fatal(err)
	}
	rows, err := store.ListDeliveries(ctx, "", 100)
	if err != nil || len(rows) != 2 {
		t.Fatalf("new sink backfilled pruned payload: %v", err)
	}

	// When registration wins first, its pending delivery prevents pruning.
	if _, err := store.PersistCommittedBatch(ctx, testBatchWithIdentity("urn:test", "new", 0x500)); err != nil {
		t.Fatal(err)
	}
	if result, err := Prune(ctx, path, PruneOptions{Before: cutoff, Limit: 1000}); err != nil || result.Events != 0 {
		t.Fatalf("pending new sink pruned: %+v %v", result, err)
	}
}

func TestPruneValidationAndNonMigratingPreview(t *testing.T) {
	ctx := t.Context()
	cutoff := time.Now().Add(-time.Hour)
	missing := filepath.Join(t.TempDir(), "missing.sqlite")
	for _, dry := range []bool{true, false} {
		if _, err := Prune(ctx, missing, PruneOptions{Before: cutoff, Limit: 1000, DryRun: dry}); err == nil {
			t.Fatal("missing spool accepted")
		}
	}
	if _, err := os.Stat(missing); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("created a missing spool")
	}
	for _, options := range []PruneOptions{{Before: cutoff, Limit: 0}, {Before: cutoff, Limit: 1001}, {Limit: 1}, {Before: time.Now().Add(time.Hour), Limit: 1}} {
		if _, err := Prune(ctx, missing, options); err == nil {
			t.Fatal("invalid options accepted")
		}
	}
	for _, schema := range []int{1, 2} {
		path := filepath.Join(t.TempDir(), "legacy.sqlite")
		db, err := sql.Open("sqlite", path)
		if err != nil {
			t.Fatal(err)
		}
		for _, file := range []string{"001_initial.sql", "002_delivery.sql"}[:schema] {
			migration, err := migrations.ReadFile("migrations/" + file)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := db.ExecContext(ctx, string(migration)); err != nil {
				t.Fatal(err)
			}
		}
		for _, dry := range []bool{true, false} {
			if _, err := Prune(ctx, path, PruneOptions{Before: cutoff, Limit: 1, DryRun: dry}); err == nil {
				t.Fatal("prune accepted legacy schema")
			}
		}
		var got int
		if err := db.QueryRowContext(ctx, "SELECT MAX(version) FROM schema_migrations").Scan(&got); err != nil || got != schema {
			t.Fatal("prune migrated legacy schema")
		}
		db.Close()
		store, err := Open(ctx, path)
		if err != nil {
			t.Fatal(err)
		}
		if got, err := store.SchemaVersion(ctx); err != nil || got != 3 {
			t.Fatalf("migration: %d %v", got, err)
		}
		store.Close()
	}
	store, path, _ := pruneFixture(t, "one")
	store.Close()
	if err := os.Chmod(path, 0400); err != nil {
		t.Fatal(err)
	}
	if _, err := Prune(ctx, path, PruneOptions{Before: cutoff, Limit: 1, DryRun: true}); err != nil {
		t.Fatal(err)
	}
	if info, err := os.Stat(path); err != nil || info.Mode().Perm() != 0400 {
		t.Fatal("preview changed permissions")
	}
	link := filepath.Join(t.TempDir(), "symlink.sqlite")
	if err := os.Symlink(path, link); err != nil {
		t.Fatal(err)
	}
	if _, err := Prune(ctx, link, PruneOptions{Before: cutoff, Limit: 1, DryRun: true}); err == nil {
		t.Fatal("symlink accepted")
	}
}

func TestProcessCrashDuringPrune(t *testing.T) {
	for _, mode := range []string{"before_commit", "after_commit"} {
		t.Run(mode, func(t *testing.T) {
			store, path, cutoff := pruneFixture(t, "first", "second")
			checkpoint, err := store.LastDurableLSN(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			store.Close()
			command := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestPruneCrashHelper$")
			command.Env = append(os.Environ(), "WRITERELAY_TEST_PRUNE_PATH="+path, "WRITERELAY_TEST_PRUNE_MODE="+mode, "WRITERELAY_TEST_PRUNE_CUTOFF="+cutoff.Format(time.RFC3339Nano))
			err = command.Run()
			var exited *exec.ExitError
			if !errors.As(err, &exited) || exited.ExitCode() != crashExitCode {
				t.Fatalf("child: %v", err)
			}
			reopened, err := Open(t.Context(), path)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			rows, err := reopened.ListEvents(t.Context(), 100)
			if err != nil {
				t.Fatal(err)
			}
			if len(rows) != 2 {
				t.Fatalf("crash removed event identities: %d", len(rows))
			}
			history, err := reopened.ListDeliveries(t.Context(), "delivered", 100)
			if err != nil || len(history) != 4 {
				t.Fatalf("crash changed delivered history: %v", err)
			}
			for _, row := range rows {
				if (row.PayloadPrunedAt != nil) != (mode == "after_commit") {
					t.Fatalf("partial prune after crash: %s", row.ID)
				}
			}
			if got, err := reopened.LastDurableLSN(t.Context()); err != nil || got != checkpoint {
				t.Fatal("prune crash changed checkpoint")
			}
			if result, err := reopened.PersistCommittedBatch(t.Context(), testBatchWithIdentity("urn:test", "first", 0x500)); err != nil || result.Replayed != 1 {
				t.Fatalf("replay after crash: %+v %v", result, err)
			}
		})
	}
}

func TestPruneCrashHelper(t *testing.T) {
	path := os.Getenv("WRITERELAY_TEST_PRUNE_PATH")
	if path == "" {
		return
	}
	cutoff, err := time.Parse(time.RFC3339Nano, os.Getenv("WRITERELAY_TEST_PRUNE_CUTOFF"))
	if err != nil {
		t.Fatal(err)
	}
	hooks := failure.Hooks{}
	if os.Getenv("WRITERELAY_TEST_PRUNE_MODE") == "before_commit" {
		hooks.AfterPrunePayload = func(int) { os.Exit(crashExitCode) }
	} else {
		hooks.AfterPruneCommit = func() { os.Exit(crashExitCode) }
	}
	if _, err := pruneWithHooks(context.Background(), path, PruneOptions{Before: cutoff, Limit: 1000}, hooks); err != nil {
		t.Fatal(err)
	}
	t.Fatal("crash hook did not run")
}

func TestPruneCancellationRollsBackBatch(t *testing.T) {
	store, path, cutoff := pruneFixture(t, "first", "second")
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	_, err := pruneWithHooks(ctx, path, PruneOptions{Before: cutoff, Limit: 1000}, failure.Hooks{AfterPrunePayload: func(int) { cancel() }})
	if err == nil {
		t.Fatal("canceled prune succeeded")
	}
	rows, err := store.ListEvents(t.Context(), 100)
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		if row.PayloadPrunedAt != nil {
			t.Fatal("cancellation left partial cleanup")
		}
	}
	if result, err := Prune(t.Context(), path, PruneOptions{Before: cutoff, Limit: 1000}); err != nil || result.Events != 2 {
		t.Fatalf("retry after canceled prune: %+v %v", result, err)
	}
}

func TestRetentionMigrationPreservesExistingPayloadAndHistory(t *testing.T) {
	ctx := t.Context()
	path := filepath.Join(t.TempDir(), "v2.sqlite")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range []string{"001_initial.sql", "002_delivery.sql"} {
		migration, err := migrations.ReadFile("migrations/" + file)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.ExecContext(ctx, string(migration)); err != nil {
			t.Fatal(err)
		}
	}
	batch := testBatchWithIdentity("urn:test", "legacy", 0x100)
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := insertOrVerify(ctx, tx, batch.Events[0]); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO spool_metadata(key,value) VALUES('last_durable_lsn','0/120');
INSERT INTO delivery_sinks(sink_name,sink_type,config_sha256,active,created_at) VALUES('legacy','stdout',zeroblob(32),1,'2020-01-01T00:00:00Z');
INSERT INTO deliveries(event_sequence,sink_id,state,attempts,next_attempt_at,delivered_at) VALUES(1,1,'delivered',1,'2020-01-01T00:00:00Z','2020-01-01T00:00:00.000000000Z');`); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	db.Close()
	store, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	rows, err := store.ListEvents(ctx, 100)
	if err != nil || len(rows) != 1 || !reflect.DeepEqual(rows[0].Payload, batch.Events[0].Payload) || rows[0].PayloadPrunedAt != nil {
		t.Fatalf("migration changed payload: %v", err)
	}
	history, err := store.ListDeliveries(ctx, "delivered", 100)
	if err != nil || len(history) != 1 || history[0].Attempts != 1 {
		t.Fatalf("migration changed history: %v", err)
	}
	if got, err := store.LastDurableLSN(ctx); err != nil || got != batch.CommitEndLSN {
		t.Fatalf("migration changed checkpoint: %v", err)
	}
	if result, err := Prune(ctx, path, PruneOptions{Before: time.Now().Add(-time.Hour), Limit: 1000}); err != nil || result.Events != 1 {
		t.Fatalf("prune migrated data: %+v %v", result, err)
	}
}
