package sqlite

import (
	"context"
	"crypto/sha256"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
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
