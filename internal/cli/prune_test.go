package cli

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/johnathondillon/write-relay/internal/delivery"
	"github.com/johnathondillon/write-relay/internal/spool"
	sqlitespool "github.com/johnathondillon/write-relay/internal/spool/sqlite"
)

func TestPruneCommandPreviewApplyAndInspection(t *testing.T) {
	ctx := t.Context()
	directory := t.TempDir()
	path := filepath.Join(directory, "spool.sqlite")
	configPath := filepath.Join(directory, "writerelay.yaml")
	t.Setenv("PRUNE_TEST_DSN", "")
	if err := os.WriteFile(configPath, []byte("version: 1\npostgres:\n  dsn_env: PRUNE_TEST_DSN\n  slot: test_slot\n  publication: test_pub\n  message_prefix: writerelay.v1\nspool:\n  path: "+path+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	store, err := sqlitespool.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.ConfigureSinks(ctx, []delivery.SinkRegistration{{Name: "orders", Type: "stdout"}}); err != nil {
		t.Fatal(err)
	}
	payload := []byte(`{"specversion":"1.0","id":"one","source":"urn:test","type":"created","data":{"private":"payload"}}`)
	at := time.Now().UTC().Add(-48 * time.Hour)
	batch := spool.CommittedBatch{TransactionID: 1, CommitLSN: pglogrepl.LSN(0x100), CommitEndLSN: pglogrepl.LSN(0x120), CommitTime: at}
	batch.Events = []spool.CapturedEvent{{Source: "urn:test", ID: "one", Type: "created", Payload: payload, PayloadSHA256: sha256.Sum256(payload), TransactionID: 1, CommitLSN: batch.CommitLSN, CommitEndLSN: batch.CommitEndLSN, CommitTime: at}}
	if _, err := store.PersistCommittedBatch(ctx, batch); err != nil {
		t.Fatal(err)
	}
	item, found, err := store.NextDueDelivery(ctx, time.Now().Add(time.Second))
	if err != nil || !found {
		t.Fatalf("delivery: %v", err)
	}
	if err := store.MarkDelivered(ctx, item, 1, 0, at); err != nil {
		t.Fatal(err)
	}
	cutoff := time.Now().Add(-24 * time.Hour).Format(time.RFC3339Nano)
	run := func(args ...string) ([]byte, error) {
		var out, stderr bytes.Buffer
		err := spoolCommand(ctx, append(args, "--config", configPath), &out, &stderr)
		return out.Bytes(), err
	}
	for _, dry := range []bool{true, false} {
		args := []string{"prune", "--before", cutoff, "--limit", "1"}
		if dry {
			args = append(args, "--dry-run")
		}
		output, err := run(args...)
		if err != nil {
			t.Fatal(err)
		}
		var result sqlitespool.PruneResult
		if err := json.Unmarshal(output, &result); err != nil {
			t.Fatal(err)
		}
		if result.Events != 1 || result.DryRun != dry || result.PayloadBytes != int64(len(payload)) {
			t.Fatalf("result: %s", output)
		}
		if bytes.Contains(output, []byte("private")) {
			t.Fatal("prune output leaked payload")
		}
		listing, err := run("list")
		if err != nil {
			t.Fatal(err)
		}
		var row map[string]any
		if err := json.Unmarshal(listing, &row); err != nil {
			t.Fatal(err)
		}
		if (row["payload"] == nil) != (!dry) || (row["payload_pruned_at"] != nil) != (!dry) {
			t.Fatalf("list: %s", listing)
		}
	}
	for _, args := range [][]string{{"prune"}, {"prune", "--before", "invalid"}, {"prune", "--before", cutoff, "--limit", "0"}, {"prune", "--before", cutoff, "--limit", "1001"}, {"prune", "--before", cutoff, "extra"}, {"prune", "--before", "2999-01-01T00:00:00Z"}} {
		output, err := run(args...)
		if err == nil || len(output) != 0 {
			t.Fatalf("invalid flags produced success: %v %s", args, output)
		}
	}
	stats, err := run("stats", "--json")
	if err != nil {
		t.Fatal(err)
	}
	var result sqlitespool.Stats
	if err := json.Unmarshal(stats, &result); err != nil || result.PrunedPayloads != 1 || result.Deliveries.Delivered != 1 {
		t.Fatalf("stats: %s %v", stats, err)
	}
}
