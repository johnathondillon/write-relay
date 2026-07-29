package sqlite

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/johnathondillon/write-relay/internal/delivery"
	"github.com/johnathondillon/write-relay/internal/spool"
)

func TestPersistReplayConflictAndReopen(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "spool.sqlite")
	store, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	batch := testBatch(`{"specversion":"1.0","id":"one","source":"urn:test","type":"created"}`)

	result, err := store.PersistCommittedBatch(ctx, batch)
	if err != nil {
		t.Fatal(err)
	}
	if result.Inserted != 1 || result.DurableLSN != batch.CommitEndLSN {
		t.Fatalf("unexpected result: %#v", result)
	}
	result, err = store.PersistCommittedBatch(ctx, batch)
	if err != nil {
		t.Fatal(err)
	}
	if result.Replayed != 1 {
		t.Fatalf("expected replay, got %#v", result)
	}

	conflict := testBatch(`{"specversion":"1.0","id":"one","source":"urn:test","type":"changed"}`)
	conflict.CommitLSN = batch.CommitLSN + 10
	conflict.CommitEndLSN = batch.CommitEndLSN + 10
	conflict.Events[0].CommitLSN = conflict.CommitLSN
	conflict.Events[0].CommitEndLSN = conflict.CommitEndLSN
	_, err = store.PersistCommittedBatch(ctx, conflict)
	if !errors.Is(err, spool.ErrIdentityConflict) {
		t.Fatalf("expected identity conflict, got %v", err)
	}
	count, _ := store.EventCount(ctx)
	if count != 1 {
		t.Fatalf("conflict partially committed: count=%d", count)
	}
	lsn, _ := store.LastDurableLSN(ctx)
	if lsn != batch.CommitEndLSN {
		t.Fatalf("checkpoint advanced on conflict: %s", lsn)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	rows, err := store.ListEvents(ctx, 20)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || string(rows[0].Payload) != string(batch.Events[0].Payload) {
		t.Fatalf("unexpected reopened rows: %#v", rows)
	}
}

func TestBatchRollsBackWhenLaterEventConflicts(t *testing.T) {
	ctx := context.Background()
	store, err := Open(ctx, filepath.Join(t.TempDir(), "spool.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	original := testBatch(`{"specversion":"1.0","id":"existing","source":"urn:test","type":"created"}`)
	if _, err := store.PersistCommittedBatch(ctx, original); err != nil {
		t.Fatal(err)
	}

	batch := testBatch(`{"specversion":"1.0","id":"new","source":"urn:test","type":"created"}`)
	second := batch.Events[0]
	second.ID = "existing"
	second.Payload = []byte(`different`)
	second.PayloadSHA256 = sha256.Sum256(second.Payload)
	second.MessageIndex = 1
	batch.Events = append(batch.Events, second)
	_, err = store.PersistCommittedBatch(ctx, batch)
	if !errors.Is(err, spool.ErrIdentityConflict) {
		t.Fatalf("expected conflict, got %v", err)
	}
	count, _ := store.EventCount(ctx)
	if count != 1 {
		t.Fatalf("first insert was not rolled back: count=%d", count)
	}
}

func TestZeroEventBatchAdvancesCheckpoint(t *testing.T) {
	ctx := context.Background()
	store, err := Open(ctx, filepath.Join(t.TempDir(), "spool.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	batch := spool.CommittedBatch{
		TransactionID: 9,
		CommitLSN:     pglogrepl.LSN(0x200),
		CommitEndLSN:  pglogrepl.LSN(0x220),
		CommitTime:    time.Now().UTC(),
	}
	if _, err := store.PersistCommittedBatch(ctx, batch); err != nil {
		t.Fatal(err)
	}
	lsn, _ := store.LastDurableLSN(ctx)
	if lsn != batch.CommitEndLSN {
		t.Fatalf("got durable LSN %s", lsn)
	}
}

func TestMigratesVersionOneSpool(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "spool.sqlite")
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
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	store, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	version, err := store.SchemaVersion(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if version != 2 {
		t.Fatalf("schema version = %d, want 2", version)
	}
}

func TestSinkBackfillAtomicCreationAndOrdering(t *testing.T) {
	ctx := context.Background()
	store, err := Open(ctx, filepath.Join(t.TempDir(), "spool.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	first := testBatchWithIdentity("urn:test", "first", 0x100)
	if _, err := store.PersistCommittedBatch(ctx, first); err != nil {
		t.Fatal(err)
	}
	sinks := []delivery.SinkRegistration{
		testSink("sink_a", "a"),
		testSink("sink_b", "b"),
	}
	if err := store.ConfigureSinks(ctx, sinks); err != nil {
		t.Fatal(err)
	}
	second := testBatchWithIdentity("urn:test", "second", 0x200)
	if _, err := store.PersistCommittedBatch(ctx, second); err != nil {
		t.Fatal(err)
	}

	rows, err := store.ListDeliveries(ctx, "", 20)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 4 {
		t.Fatalf("delivery count = %d, want 4", len(rows))
	}
	item, found, err := store.NextDueDelivery(ctx, time.Now().Add(time.Second))
	if err != nil || !found {
		t.Fatalf("next delivery: found=%v err=%v", found, err)
	}
	if item.ID != "first" || item.SinkName != "sink_a" {
		t.Fatalf("unexpected first delivery: %#v", item)
	}

	at := time.Now().UTC()
	if err := store.MarkFailed(
		ctx, item, 1, false, at.Add(time.Hour), "temporary", 503, at,
	); err != nil {
		t.Fatal(err)
	}
	item, found, err = store.NextDueDelivery(ctx, at.Add(time.Second))
	if err != nil || !found {
		t.Fatalf("next unblocked sink: found=%v err=%v", found, err)
	}
	if item.ID != "first" || item.SinkName != "sink_b" {
		t.Fatalf("retry wait did not preserve per-sink order: %#v", item)
	}
	if err := store.MarkDelivered(ctx, item, 1, 204, at); err != nil {
		t.Fatal(err)
	}
	item, found, err = store.NextDueDelivery(ctx, at.Add(time.Second))
	if err != nil || !found {
		t.Fatalf("next sink_b delivery: found=%v err=%v", found, err)
	}
	if item.ID != "second" || item.SinkName != "sink_b" {
		t.Fatalf("unexpected independently ordered delivery: %#v", item)
	}
}

func TestSinkConflictRemovalAndRedrive(t *testing.T) {
	ctx := context.Background()
	store, err := Open(ctx, filepath.Join(t.TempDir(), "spool.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	sink := testSink("orders", "target-a")
	if err := store.ConfigureSinks(ctx, []delivery.SinkRegistration{sink}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PersistCommittedBatch(
		ctx, testBatchWithIdentity("urn:test", "one", 0x100),
	); err != nil {
		t.Fatal(err)
	}
	conflict := testSink("orders", "target-b")
	if err := store.ConfigureSinks(ctx, []delivery.SinkRegistration{conflict}); !errors.Is(err, delivery.ErrSinkConfigurationConflict) {
		t.Fatalf("expected configuration conflict, got %v", err)
	}
	if err := store.ConfigureSinks(ctx, nil); err == nil {
		t.Fatal("expected removal with pending delivery to fail")
	}
	item, found, err := store.NextDueDelivery(ctx, time.Now().Add(time.Second))
	if err != nil || !found {
		t.Fatalf("next delivery: found=%v err=%v", found, err)
	}
	now := time.Now().UTC()
	if err := store.MarkFailed(ctx, item, 1, true, now, "bad request", 400, now); err != nil {
		t.Fatal(err)
	}
	if err := store.RedriveDelivery(ctx, "orders", "urn:test", "one", now); err != nil {
		t.Fatal(err)
	}
	rows, err := store.ListDeliveries(ctx, "retry_wait", 20)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].Attempts != 0 || rows[0].LastError != "" {
		t.Fatalf("unexpected redriven delivery: %#v", rows)
	}
}

func testBatch(payload string) spool.CommittedBatch {
	commitTime := time.Date(2026, 7, 27, 20, 24, 0, 0, time.UTC)
	digest := sha256.Sum256([]byte(payload))
	batch := spool.CommittedBatch{
		TransactionID: 42,
		CommitLSN:     pglogrepl.LSN(0x100),
		CommitEndLSN:  pglogrepl.LSN(0x120),
		CommitTime:    commitTime,
	}
	batch.Events = []spool.CapturedEvent{{
		Source:        "urn:test",
		ID:            "one",
		Type:          "created",
		Payload:       []byte(payload),
		PayloadSHA256: digest,
		TransactionID: batch.TransactionID,
		MessageLSN:    pglogrepl.LSN(0x80),
		CommitLSN:     batch.CommitLSN,
		CommitEndLSN:  batch.CommitEndLSN,
		CommitTime:    batch.CommitTime,
		MessageIndex:  0,
	}}
	return batch
}

func testBatchWithIdentity(source, id string, lsn uint64) spool.CommittedBatch {
	payload := `{"specversion":"1.0","id":"` + id + `","source":"` + source + `","type":"created"}`
	batch := testBatch(payload)
	batch.TransactionID = uint32(lsn)
	batch.CommitLSN = pglogrepl.LSN(lsn)
	batch.CommitEndLSN = pglogrepl.LSN(lsn + 0x20)
	batch.Events[0].Source = source
	batch.Events[0].ID = id
	batch.Events[0].TransactionID = batch.TransactionID
	batch.Events[0].CommitLSN = batch.CommitLSN
	batch.Events[0].CommitEndLSN = batch.CommitEndLSN
	return batch
}

func testSink(name, target string) delivery.SinkRegistration {
	return delivery.SinkRegistration{
		Name: name, Type: "webhook", ConfigSHA256: sha256.Sum256([]byte(target)),
	}
}
