package cli

import (
	"bytes"
	"context"
	"crypto/sha256"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/johnathondillon/write-relay/internal/delivery"
	"github.com/johnathondillon/write-relay/internal/spool"
	sqlitespool "github.com/johnathondillon/write-relay/internal/spool/sqlite"
)

func TestDeliveryInspectionAndRedriveCommands(t *testing.T) {
	ctx := context.Background()
	directory := t.TempDir()
	spoolPath := filepath.Join(directory, "spool.sqlite")
	configPath := filepath.Join(directory, "writerelay.yaml")
	configYAML := `
version: 1
postgres:
  dsn_env: TEST_POSTGRES_DSN
  slot: writerelay_slot_v1
  publication: writerelay_publication
  message_prefix: writerelay.v1
spool:
  path: ` + spoolPath + `
`
	if err := os.WriteFile(configPath, []byte(configYAML), 0o600); err != nil {
		t.Fatal(err)
	}
	store, err := sqlitespool.Open(ctx, spoolPath)
	if err != nil {
		t.Fatal(err)
	}
	registration := delivery.SinkRegistration{
		Name: "orders", Type: "webhook",
		ConfigSHA256: sha256.Sum256([]byte("target")),
	}
	if err := store.ConfigureSinks(ctx, []delivery.SinkRegistration{registration}); err != nil {
		t.Fatal(err)
	}
	payload := []byte(`{"specversion":"1.0","id":"one","source":"urn:test","type":"created"}`)
	digest := sha256.Sum256(payload)
	commitTime := time.Now().UTC()
	batch := spool.CommittedBatch{
		TransactionID: 1, CommitLSN: pglogrepl.LSN(0x100),
		CommitEndLSN: pglogrepl.LSN(0x120), CommitTime: commitTime,
	}
	batch.Events = []spool.CapturedEvent{{
		Source: "urn:test", ID: "one", Type: "created", Payload: payload,
		PayloadSHA256: digest, TransactionID: batch.TransactionID,
		MessageLSN: pglogrepl.LSN(0x80), CommitLSN: batch.CommitLSN,
		CommitEndLSN: batch.CommitEndLSN, CommitTime: commitTime,
	}}
	if _, err := store.PersistCommittedBatch(ctx, batch); err != nil {
		t.Fatal(err)
	}
	item, found, err := store.NextDueDelivery(ctx, time.Now().Add(time.Second))
	if err != nil || !found {
		t.Fatalf("next delivery found=%v err=%v", found, err)
	}
	if err := store.MarkFailed(ctx, item, 1, true, time.Now(), "bad request", 400, time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	var output bytes.Buffer
	var stderr bytes.Buffer
	if err := spoolCommand(ctx, []string{
		"deliveries", "--config", configPath, "--state", "dead_letter",
	}, &output, &stderr); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), `"state":"dead_letter"`) ||
		!strings.Contains(output.String(), `"id":"one"`) {
		t.Fatalf("delivery output = %q", output.String())
	}
	output.Reset()
	if err := spoolCommand(ctx, []string{
		"redrive", "--config", configPath, "--sink", "orders",
		"--source", "urn:test", "--id", "one",
	}, &output, &stderr); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), "redrive scheduled") {
		t.Fatalf("redrive output = %q", output.String())
	}

	store, err = sqlitespool.Open(ctx, spoolPath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	rows, err := store.ListDeliveries(ctx, "retry_wait", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].Attempts != 0 {
		t.Fatalf("redriven rows = %#v", rows)
	}
}
