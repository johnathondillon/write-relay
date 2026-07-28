package postgres

import (
	"context"
	"errors"
	"testing"

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
