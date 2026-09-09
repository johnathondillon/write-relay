package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/johnathondillon/write-relay/internal/delivery"
	sqlitespool "github.com/johnathondillon/write-relay/internal/spool/sqlite"
)

func TestSpoolStatsFormatsAndValidation(t *testing.T) {
	ctx := context.Background()
	directory := t.TempDir()
	path := filepath.Join(directory, "spool.sqlite")
	configPath := filepath.Join(directory, "writerelay.yaml")
	// Neither secret is set. Stats must not connect to PostgreSQL or construct sinks.
	t.Setenv("STATS_TEST_DSN", "")
	t.Setenv("STATS_TEST_AUTH", "")
	configuration := `version: 1
postgres:
  dsn_env: STATS_TEST_DSN
  slot: example_slot
  publication: example_publication
  message_prefix: writerelay.v1
spool:
  path: ` + path + `
delivery:
  sinks:
    - name: configured_but_not_registered
      type: webhook
      url: https://example.invalid/private-target
      authorization_env: STATS_TEST_AUTH
`
	if err := os.WriteFile(configPath, []byte(configuration), 0o600); err != nil {
		t.Fatal(err)
	}
	store, err := sqlitespool.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.ConfigureSinks(ctx, []delivery.SinkRegistration{{Name: "certificates", Type: "webhook"}}); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	var text, stderr bytes.Buffer
	if err := Execute(ctx, []string{"spool", "stats", "--config", configPath}, &text, &stderr, BuildInfo{}); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"Captured events: 0", "0 pending", "0 retry_wait", "0 delivered", "0 dead_letter", "Oldest waiting: none", "SQLite WAL", "certificates", "OLDEST WAITING"} {
		if !strings.Contains(text.String(), want) {
			t.Fatalf("missing %q: %s", want, text.String())
		}
	}
	if strings.Contains(text.String(), "configured_but_not_registered") || strings.Contains(text.String(), "private-target") {
		t.Fatalf("report leaked config or used non-durable sink state: %s", text.String())
	}
	var output bytes.Buffer
	if err := Execute(ctx, []string{"spool", "stats", "--json", "--config", configPath}, &output, &stderr, BuildInfo{}); err != nil {
		t.Fatal(err)
	}
	decoder := json.NewDecoder(&output)
	var stats sqlitespool.Stats
	if err := decoder.Decode(&stats); err != nil {
		t.Fatal(err)
	}
	if stats.EventCount != 0 || len(stats.Sinks) != 1 || stats.Sinks[0].Name != "certificates" || stats.OldestWaiting != nil {
		t.Fatalf("JSON: %+v", stats)
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		t.Fatalf("trailing output: %v", err)
	}

	for _, flags := range [][]string{{"extra"}, {"--limit", "1"}, {"--json=invalid"}} {
		output.Reset()
		args := append([]string{"stats", "--config", configPath}, flags...)
		if err := spoolCommand(ctx, args, &output, &stderr); err == nil || output.Len() != 0 {
			t.Fatalf("accepted flags %v or printed partial stats: %v", flags, err)
		}
	}
	for _, flags := range [][]string{nil, {"--json"}} {
		args := append([]string{"stats", "--config", configPath}, flags...)
		if err := spoolCommand(ctx, args, failingStatsWriter{}, &stderr); !errors.Is(err, errStatsOutput) {
			t.Fatalf("writer error lost: %v", err)
		}
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	output.Reset()
	if err := spoolCommand(ctx, []string{"stats", "--config", configPath}, &output, &stderr); !errors.Is(err, os.ErrNotExist) || output.Len() != 0 {
		t.Fatalf("missing spool: %v, %s", err, output.String())
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("stats created missing spool")
	}
}

var errStatsOutput = errors.New("output unavailable")

type failingStatsWriter struct{}

func (failingStatsWriter) Write([]byte) (int, error) { return 0, errStatsOutput }
