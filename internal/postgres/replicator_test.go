package postgres

import (
	"context"
	"errors"
	"github.com/johnathondillon/write-relay/internal/config"
	"github.com/johnathondillon/write-relay/internal/diskspace"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/johnathondillon/write-relay/internal/spool"
)

func TestFatalCaptureErrors(t *testing.T) {
	for _, target := range []error{
		ErrProtocolState, ErrProtectedEvent, ErrTransactionLimit,
		ErrSlotMismatch, ErrUnsupportedVersion,
		spool.ErrIdentityConflict, spool.ErrDurability,
	} {
		if !isFatalCaptureError(errors.Join(errors.New("context"), target)) {
			t.Fatalf("%v should be fatal", target)
		}
	}
	if isFatalCaptureError(errors.New("network reset")) {
		t.Fatal("network error should reconnect")
	}
}

func TestPersistThenAcknowledgeOrdersDurabilityBeforeACK(t *testing.T) {
	events := []string{}
	fake := &recordingSpool{events: &events, result: spool.PersistResult{DurableLSN: 120}}
	batch := spool.CommittedBatch{CommitLSN: 100, CommitEndLSN: 120}
	_, err := persistThenAcknowledge(context.Background(), fake, batch, func(lsn pglogrepl.LSN) error {
		events = append(events, "ack")
		if lsn != 120 {
			t.Fatalf("acknowledged %s", lsn)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 || events[0] != "persist" || events[1] != "ack" {
		t.Fatalf("unexpected operation order: %v", events)
	}
}

func TestPersistThenAcknowledgeDoesNotACKFailedBatch(t *testing.T) {
	events := []string{}
	fake := &recordingSpool{events: &events, err: spool.ErrDurability}
	_, err := persistThenAcknowledge(context.Background(), fake, spool.CommittedBatch{CommitEndLSN: 120}, func(pglogrepl.LSN) error {
		events = append(events, "ack")
		return nil
	})
	if !errors.Is(err, spool.ErrDurability) {
		t.Fatalf("got %v", err)
	}
	if len(events) != 1 || events[0] != "persist" {
		t.Fatalf("unexpected operations: %v", events)
	}
}

func TestStandbyStatusUsesOnlyDurableLSN(t *testing.T) {
	const durable = pglogrepl.LSN(0x1234)
	status := standbyStatus(durable)
	if status.WALWritePosition != durable ||
		status.WALFlushPosition != durable ||
		status.WALApplyPosition != durable {
		t.Fatalf("status does not consistently use durable LSN: %#v", status)
	}
}

type recordingSpool struct {
	events *[]string
	result spool.PersistResult
	err    error
}

func (s *recordingSpool) PersistCommittedBatch(context.Context, spool.CommittedBatch) (spool.PersistResult, error) {
	*s.events = append(*s.events, "persist")
	return s.result, s.err
}

func (*recordingSpool) LastDurableLSN(context.Context) (pglogrepl.LSN, error) {
	return 0, nil
}

func (*recordingSpool) Close() error { return nil }

var _ spool.Spool = (*recordingSpool)(nil)

func TestDiskCheckBeforeBatchCannotWriteOrAcknowledge(t *testing.T) {
	operations := []string{}
	store := &recordingSpool{events: &operations, result: spool.PersistResult{DurableLSN: 120}}
	cfg := config.Config{Spool: config.SpoolConfig{DiskSpace: config.DiskSpaceConfig{PauseBelowBytes: 100, ResumeAtBytes: 200, CheckInterval: time.Hour}}}
	free := uint64(200)
	var probeErr error
	r := NewReplicatorWithDiskProbe(cfg, store, slog.New(slog.NewTextHandler(io.Discard, nil)), func(string) (uint64, error) { return free, probeErr })
	if err := r.checkDisk(false); err != nil {
		t.Fatal(err)
	}
	free = 99 // Even a fresh healthy cache must not bypass the pre-persist probe.
	ack := func(pglogrepl.LSN) error { operations = append(operations, "ack"); return nil }
	for _, empty := range []bool{false, true} {
		batch := spool.CommittedBatch{CommitEndLSN: 120}
		if !empty {
			batch.Events = []spool.CapturedEvent{{ID: "protected"}}
		}
		if _, err := r.persistBatch(t.Context(), batch, ack); !errors.Is(err, diskspace.ErrPaused) {
			t.Fatal(err)
		}
		if len(operations) != 0 {
			t.Fatal(operations)
		}
	}
	free = 200
	probeErr = errors.New("secret filesystem path")
	if _, err := r.persistBatch(t.Context(), spool.CommittedBatch{CommitEndLSN: 120}, ack); !errors.Is(err, diskspace.ErrPaused) {
		t.Fatal(err)
	}
	if len(operations) != 0 {
		t.Fatal(operations)
	}
	probeErr = nil
	if _, err := r.persistBatch(t.Context(), spool.CommittedBatch{CommitEndLSN: 120}, ack); err != nil {
		t.Fatal(err)
	}
	if len(operations) != 2 || operations[0] != "persist" || operations[1] != "ack" {
		t.Fatal(operations)
	}
}

func TestDiskPauseCancelsWithoutPostgresConnection(t *testing.T) {
	cfg := config.Config{Spool: config.SpoolConfig{DiskSpace: config.DiskSpaceConfig{PauseBelowBytes: 100, ResumeAtBytes: 200, CheckInterval: time.Hour}}}
	r := NewReplicatorWithDiskProbe(cfg, nil, slog.New(slog.NewTextHandler(io.Discard, nil)), func(string) (uint64, error) { return 0, nil })
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	deadline := time.After(time.Second)
	for !r.Status().Disk.Paused {
		select {
		case <-deadline:
			t.Fatal("capture did not pause")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("pause ignored cancellation")
	}
	if r.Status().Connected {
		t.Fatal("low disk startup connected to PostgreSQL")
	}
}
