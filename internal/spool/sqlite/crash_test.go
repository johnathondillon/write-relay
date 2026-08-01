package sqlite

import (
	"context"
	"crypto/sha256"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/johnathondillon/write-relay/internal/delivery"
	"github.com/johnathondillon/write-relay/internal/failure"
	"github.com/johnathondillon/write-relay/internal/spool"
)

const crashExitCode = 86

func TestProcessCrashRollsBackOrRetainsSQLiteBatch(t *testing.T) {
	tests := []struct {
		name           string
		mode           string
		wantEvents     int
		wantDeliveries int
		wantCheckpoint pglogrepl.LSN
	}{
		{
			name: "before transaction", mode: "before_transaction",
		},
		{
			name: "midway through inserts", mode: "midway_insert",
		},
		{
			name: "after commit", mode: "after_commit",
			wantEvents: 2, wantDeliveries: 2, wantCheckpoint: pglogrepl.LSN(0x220),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "spool.sqlite")
			runSQLiteCrashChild(t, test.mode, path)

			store, err := Open(t.Context(), path)
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			events, err := store.EventCount(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			deliveries, err := store.ListDeliveries(t.Context(), "", 20)
			if err != nil {
				t.Fatal(err)
			}
			checkpoint, err := store.LastDurableLSN(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			if events != test.wantEvents ||
				len(deliveries) != test.wantDeliveries ||
				checkpoint != test.wantCheckpoint {
				t.Fatalf(
					"recovered state events=%d deliveries=%d checkpoint=%s",
					events, len(deliveries), checkpoint,
				)
			}
		})
	}
}

func runSQLiteCrashChild(t *testing.T, mode, path string) {
	t.Helper()
	command := exec.CommandContext(
		t.Context(), os.Args[0], "-test.run=^TestSQLiteCrashHelper$",
	)
	command.Env = append(os.Environ(),
		"WRITERELAY_TEST_CRASH_HELPER=sqlite",
		"WRITERELAY_TEST_CRASH_MODE="+mode,
		"WRITERELAY_TEST_CRASH_PATH="+path,
	)
	err := command.Run()
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) || exitErr.ExitCode() != crashExitCode {
		t.Fatalf("child exit = %v, want code %d", err, crashExitCode)
	}
}

func TestSQLiteCrashHelper(t *testing.T) {
	if os.Getenv("WRITERELAY_TEST_CRASH_HELPER") != "sqlite" {
		return
	}
	mode := os.Getenv("WRITERELAY_TEST_CRASH_MODE")
	hooks := failure.Hooks{}
	switch mode {
	case "before_transaction":
		hooks.BeforeSpoolTransaction = crashProcess
	case "midway_insert":
		hooks.AfterSpoolEvent = func(index int) {
			if index == 0 {
				crashProcess()
			}
		}
	case "after_commit":
		hooks.AfterSpoolCommit = crashProcess
	default:
		t.Fatalf("unknown crash mode %q", mode)
	}
	store, err := OpenWithHooks(
		context.Background(), os.Getenv("WRITERELAY_TEST_CRASH_PATH"), hooks,
	)
	if err != nil {
		t.Fatal(err)
	}
	registration := delivery.SinkRegistration{
		Name: "orders", Type: "webhook",
		ConfigSHA256: sha256.Sum256([]byte("target")),
	}
	if err := store.ConfigureSinks(
		context.Background(), []delivery.SinkRegistration{registration},
	); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PersistCommittedBatch(context.Background(), crashBatch()); err != nil {
		t.Fatal(err)
	}
	t.Fatal("crash hook did not terminate child")
}

func crashProcess() {
	os.Exit(crashExitCode)
}

func crashBatch() spool.CommittedBatch {
	commitTime := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	batch := spool.CommittedBatch{
		TransactionID: 99,
		CommitLSN:     pglogrepl.LSN(0x200),
		CommitEndLSN:  pglogrepl.LSN(0x220),
		CommitTime:    commitTime,
	}
	for index, id := range []string{"first", "second"} {
		payload := []byte(`{"specversion":"1.0","id":"` + id +
			`","source":"urn:crash","type":"created"}`)
		batch.Events = append(batch.Events, spool.CapturedEvent{
			Source: "urn:crash", ID: id, Type: "created", Payload: payload,
			PayloadSHA256: sha256.Sum256(payload),
			TransactionID: batch.TransactionID,
			MessageLSN:    pglogrepl.LSN(0x180 + index),
			CommitLSN:     batch.CommitLSN,
			CommitEndLSN:  batch.CommitEndLSN,
			CommitTime:    commitTime,
			MessageIndex:  index,
		})
	}
	return batch
}
