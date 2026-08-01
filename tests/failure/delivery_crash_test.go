package failure_test

import (
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/johnathondillon/write-relay/internal/config"
	"github.com/johnathondillon/write-relay/internal/delivery"
	"github.com/johnathondillon/write-relay/internal/failure"
	"github.com/johnathondillon/write-relay/internal/spool"
	sqlitespool "github.com/johnathondillon/write-relay/internal/spool/sqlite"
)

const deliveryCrashExitCode = 88

func TestProcessCrashAroundDeliveryRetriesPendingEvent(t *testing.T) {
	for _, mode := range []string{"before_request", "in_flight", "after_success"} {
		t.Run(mode, func(t *testing.T) {
			var mu sync.Mutex
			var keys []string
			var attempts []string
			firstInFlight := make(chan struct{})
			releaseFirst := make(chan struct{})
			firstDone := make(chan struct{})
			server := httptest.NewServer(http.HandlerFunc(func(
				writer http.ResponseWriter,
				request *http.Request,
			) {
				mu.Lock()
				keys = append(keys, request.Header.Get("Idempotency-Key"))
				attempts = append(attempts, request.Header.Get("X-WriteRelay-Attempt"))
				call := len(keys)
				mu.Unlock()
				if mode == "in_flight" && call == 1 {
					close(firstInFlight)
					<-releaseFirst
					close(firstDone)
					return
				}
				writer.WriteHeader(http.StatusNoContent)
			}))
			defer server.Close()

			path := filepath.Join(t.TempDir(), "spool.sqlite")
			seedPendingDelivery(t, path, server.URL)
			if mode == "in_flight" {
				runAndKillInFlightChild(
					t, mode, path, server.URL, firstInFlight, releaseFirst,
				)
				select {
				case <-firstDone:
				case <-time.After(5 * time.Second):
					t.Fatal("in-flight handler did not observe child termination")
				}
			} else {
				runDeliveryCrashChild(t, mode, path, server.URL)
			}

			assertPendingDelivery(t, path)
			recoverDelivery(t, path, server.URL)

			mu.Lock()
			gotKeys := append([]string(nil), keys...)
			gotAttempts := append([]string(nil), attempts...)
			mu.Unlock()
			wantCalls := 2
			if mode == "before_request" {
				wantCalls = 1
			}
			if len(gotKeys) != wantCalls {
				t.Fatalf("destination calls=%d want=%d", len(gotKeys), wantCalls)
			}
			for index, key := range gotKeys {
				if key == "" || key != gotKeys[0] {
					t.Fatalf("idempotency keys = %#v", gotKeys)
				}
				if gotAttempts[index] != "1" {
					t.Fatalf("attempt headers = %#v", gotAttempts)
				}
			}
		})
	}
}

func seedPendingDelivery(t *testing.T, path, endpoint string) {
	t.Helper()
	store, err := sqlitespool.Open(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	sink, registration, err := delivery.NewWebhookSender(
		webhookConfig(endpoint), time.Second,
	)
	if err != nil || sink == nil {
		t.Fatalf("webhook sink: %v", err)
	}
	if err := store.ConfigureSinks(
		t.Context(), []delivery.SinkRegistration{registration},
	); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PersistCommittedBatch(
		t.Context(), deliveryCrashBatch(),
	); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
}

func runDeliveryCrashChild(t *testing.T, mode, path, endpoint string) {
	t.Helper()
	command := deliveryChildCommand(t, mode, path, endpoint)
	output, err := command.CombinedOutput()
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) || exitErr.ExitCode() != deliveryCrashExitCode {
		t.Fatalf(
			"child exit=%v want=%d output=%s",
			err, deliveryCrashExitCode, output,
		)
	}
}

func runAndKillInFlightChild(
	t *testing.T,
	mode string,
	path string,
	endpoint string,
	requestReceived <-chan struct{},
	releaseRequest chan<- struct{},
) {
	t.Helper()
	command := deliveryChildCommand(t, mode, path, endpoint)
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-requestReceived:
	case <-time.After(5 * time.Second):
		_ = command.Process.Kill()
		_ = command.Wait()
		t.Fatal("destination did not receive in-flight request")
	}
	if err := command.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err := command.Wait(); err == nil {
		t.Fatal("killed child exited successfully")
	}
	close(releaseRequest)
}

func deliveryChildCommand(
	t *testing.T,
	mode string,
	path string,
	endpoint string,
) *exec.Cmd {
	t.Helper()
	command := exec.CommandContext(
		t.Context(), os.Args[0], "-test.run=^TestDeliveryCrashHelper$",
	)
	command.Env = append(os.Environ(),
		"WRITERELAY_TEST_CRASH_HELPER=delivery",
		"WRITERELAY_TEST_CRASH_MODE="+mode,
		"WRITERELAY_TEST_CRASH_PATH="+path,
		"WRITERELAY_TEST_WEBHOOK_URL="+endpoint,
	)
	return command
}

func TestDeliveryCrashHelper(t *testing.T) {
	if os.Getenv("WRITERELAY_TEST_CRASH_HELPER") != "delivery" {
		return
	}
	mode := os.Getenv("WRITERELAY_TEST_CRASH_MODE")
	hooks := failure.Hooks{}
	switch mode {
	case "before_request":
		hooks.BeforeSinkRequest = crashDeliveryProcess
	case "in_flight":
	case "after_success":
		hooks.AfterSinkSuccess = crashDeliveryProcess
	default:
		t.Fatalf("unknown crash mode %q", mode)
	}
	path := os.Getenv("WRITERELAY_TEST_CRASH_PATH")
	endpoint := os.Getenv("WRITERELAY_TEST_WEBHOOK_URL")
	store, err := sqlitespool.Open(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	sink, _, err := delivery.NewWebhookSenderWithHooks(
		webhookConfig(endpoint), 30*time.Second, hooks,
	)
	if err != nil {
		t.Fatal(err)
	}
	worker := delivery.NewWorkerWithHooks(
		store, map[string]delivery.Sink{"orders": sink},
		time.Second, time.Second, time.Minute, 3, testLogger(), hooks,
	)
	if _, err := worker.ProcessOne(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Fatal("crash hook did not terminate child")
}

func assertPendingDelivery(t *testing.T, path string) {
	t.Helper()
	store, err := sqlitespool.Open(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	rows, err := store.ListDeliveries(t.Context(), "", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 ||
		rows[0].ID != "delivery" || rows[0].State != "pending" || rows[0].Attempts != 0 ||
		rows[1].ID != "later" || rows[1].State != "pending" || rows[1].Attempts != 0 {
		t.Fatalf("delivery after crash = %#v", rows)
	}
}

func recoverDelivery(t *testing.T, path, endpoint string) {
	t.Helper()
	store, err := sqlitespool.Open(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	sink, _, err := delivery.NewWebhookSender(
		webhookConfig(endpoint), time.Second,
	)
	if err != nil {
		t.Fatal(err)
	}
	worker := delivery.NewWorker(
		store, map[string]delivery.Sink{"orders": sink},
		time.Second, time.Second, time.Minute, 3, testLogger(),
	)
	processed, err := worker.ProcessOne(t.Context())
	if err != nil || !processed {
		t.Fatalf("recovery processed=%v err=%v", processed, err)
	}
	rows, err := store.ListDeliveries(t.Context(), "", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 ||
		rows[0].ID != "delivery" || rows[0].State != "delivered" ||
		rows[0].Attempts != 1 || rows[0].ResponseStatus != http.StatusNoContent ||
		rows[1].ID != "later" || rows[1].State != "pending" || rows[1].Attempts != 0 {
		t.Fatalf("delivery after recovery = %#v", rows)
	}
}

func webhookConfig(endpoint string) config.SinkConfig {
	return config.SinkConfig{
		Name: "orders", Type: "webhook", URL: endpoint,
		AllowInsecureHTTP: true,
	}
}

func deliveryCrashBatch() spool.CommittedBatch {
	payload := []byte(
		`{"specversion":"1.0","id":"delivery","source":"urn:crash","type":"created"}`,
	)
	commitTime := time.Date(2026, 7, 28, 13, 0, 0, 0, time.UTC)
	batch := spool.CommittedBatch{
		TransactionID: 101,
		CommitLSN:     pglogrepl.LSN(0x400),
		CommitEndLSN:  pglogrepl.LSN(0x420),
		CommitTime:    commitTime,
	}
	batch.Events = []spool.CapturedEvent{{
		Source:        "urn:crash",
		ID:            "delivery",
		Type:          "created",
		Payload:       payload,
		PayloadSHA256: sha256.Sum256(payload),
		TransactionID: batch.TransactionID,
		MessageLSN:    pglogrepl.LSN(0x380),
		CommitLSN:     batch.CommitLSN,
		CommitEndLSN:  batch.CommitEndLSN,
		CommitTime:    commitTime,
		MessageIndex:  0,
	}}
	laterPayload := []byte(
		`{"specversion":"1.0","id":"later","source":"urn:crash","type":"created"}`,
	)
	batch.Events = append(batch.Events, spool.CapturedEvent{
		Source:        "urn:crash",
		ID:            "later",
		Type:          "created",
		Payload:       laterPayload,
		PayloadSHA256: sha256.Sum256(laterPayload),
		TransactionID: batch.TransactionID,
		MessageLSN:    pglogrepl.LSN(0x381),
		CommitLSN:     batch.CommitLSN,
		CommitEndLSN:  batch.CommitEndLSN,
		CommitTime:    commitTime,
		MessageIndex:  1,
	})
	return batch
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func crashDeliveryProcess() {
	os.Exit(deliveryCrashExitCode)
}
