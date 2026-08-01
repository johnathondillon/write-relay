package postgres

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
	sqlitespool "github.com/johnathondillon/write-relay/internal/spool/sqlite"
)

const postgresCrashExitCode = 87

func TestProcessCrashAroundAcknowledgmentReplaysSafely(t *testing.T) {
	tests := []struct {
		name           string
		mode           string
		wantInitialACK bool
	}{
		{
			name: "after spool commit before acknowledgment",
			mode: "after_commit_before_ack",
		},
		{
			name:           "immediately after acknowledgment",
			mode:           "after_ack",
			wantInitialACK: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			directory := t.TempDir()
			path := filepath.Join(directory, "spool.sqlite")
			ackPath := filepath.Join(directory, "ack")
			runPostgresCrashChild(t, test.mode, path, ackPath)

			_, ackErr := os.Stat(ackPath)
			if test.wantInitialACK && ackErr != nil {
				t.Fatalf("ack marker missing: %v", ackErr)
			}
			if !test.wantInitialACK && !errors.Is(ackErr, os.ErrNotExist) {
				t.Fatalf("unexpected ack marker state: %v", ackErr)
			}
			store, err := sqlitespool.Open(t.Context(), path)
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			assertCapturedCrashState(t, store)

			result, err := persistThenAcknowledge(
				t.Context(), store, acknowledgmentCrashBatch(),
				func(lsn pglogrepl.LSN) error {
					if lsn != pglogrepl.LSN(0x320) {
						t.Fatalf("acknowledged %s", lsn)
					}
					return os.WriteFile(ackPath, []byte(lsn.String()), 0o600)
				},
			)
			if err != nil {
				t.Fatal(err)
			}
			if result.Inserted != 0 || result.Replayed != 1 {
				t.Fatalf("replay result = %#v", result)
			}
			assertCapturedCrashState(t, store)
			if _, err := os.Stat(ackPath); err != nil {
				t.Fatalf("ack marker after recovery: %v", err)
			}
		})
	}
}

func runPostgresCrashChild(t *testing.T, mode, path, ackPath string) {
	t.Helper()
	command := exec.CommandContext(
		t.Context(), os.Args[0], "-test.run=^TestPostgresCrashHelper$",
	)
	command.Env = append(os.Environ(),
		"WRITERELAY_TEST_CRASH_HELPER=postgres",
		"WRITERELAY_TEST_CRASH_MODE="+mode,
		"WRITERELAY_TEST_CRASH_PATH="+path,
		"WRITERELAY_TEST_ACK_PATH="+ackPath,
	)
	output, err := command.CombinedOutput()
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) || exitErr.ExitCode() != postgresCrashExitCode {
		t.Fatalf(
			"child exit = %v, want code %d; output=%s",
			err, postgresCrashExitCode, output,
		)
	}
}

func TestPostgresCrashHelper(t *testing.T) {
	if os.Getenv("WRITERELAY_TEST_CRASH_HELPER") != "postgres" {
		return
	}
	mode := os.Getenv("WRITERELAY_TEST_CRASH_MODE")
	storeHooks := failure.Hooks{}
	ackHooks := failure.Hooks{}
	switch mode {
	case "after_commit_before_ack":
		storeHooks.AfterSpoolCommit = crashPostgresProcess
	case "after_ack":
		ackHooks.AfterAcknowledgment = crashPostgresProcess
	default:
		t.Fatalf("unknown crash mode %q", mode)
	}
	store, err := sqlitespool.OpenWithHooks(
		context.Background(), os.Getenv("WRITERELAY_TEST_CRASH_PATH"), storeHooks,
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
	_, err = persistThenAcknowledgeWithHooks(
		context.Background(), store, acknowledgmentCrashBatch(),
		func(lsn pglogrepl.LSN) error {
			return os.WriteFile(
				os.Getenv("WRITERELAY_TEST_ACK_PATH"),
				[]byte(lsn.String()), 0o600,
			)
		},
		ackHooks,
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Fatal("crash hook did not terminate child")
}

func assertCapturedCrashState(t *testing.T, store *sqlitespool.Store) {
	t.Helper()
	count, err := store.EventCount(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	deliveries, err := store.ListDeliveries(t.Context(), "", 10)
	if err != nil {
		t.Fatal(err)
	}
	checkpoint, err := store.LastDurableLSN(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 || len(deliveries) != 1 ||
		deliveries[0].State != "pending" ||
		checkpoint != pglogrepl.LSN(0x320) {
		t.Fatalf(
			"captured state count=%d deliveries=%#v checkpoint=%s",
			count, deliveries, checkpoint,
		)
	}
}

func acknowledgmentCrashBatch() spool.CommittedBatch {
	payload := []byte(
		`{"specversion":"1.0","id":"ack","source":"urn:crash","type":"created"}`,
	)
	commitTime := time.Date(2026, 7, 28, 12, 30, 0, 0, time.UTC)
	batch := spool.CommittedBatch{
		TransactionID: 100,
		CommitLSN:     pglogrepl.LSN(0x300),
		CommitEndLSN:  pglogrepl.LSN(0x320),
		CommitTime:    commitTime,
	}
	batch.Events = []spool.CapturedEvent{{
		Source:        "urn:crash",
		ID:            "ack",
		Type:          "created",
		Payload:       payload,
		PayloadSHA256: sha256.Sum256(payload),
		TransactionID: batch.TransactionID,
		MessageLSN:    pglogrepl.LSN(0x280),
		CommitLSN:     batch.CommitLSN,
		CommitEndLSN:  batch.CommitEndLSN,
		CommitTime:    commitTime,
		MessageIndex:  0,
	}}
	return batch
}

func crashPostgresProcess() {
	os.Exit(postgresCrashExitCode)
}
