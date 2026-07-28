package config

import (
	"strings"
	"testing"
)

const validConfig = `
version: 1
postgres:
  dsn_env: WRITERELAY_POSTGRES_DSN
  slot: writerelay_slot_v1
  publication: writerelay_publication
  message_prefix: writerelay.v1
  status_interval: 10s
  create_slot_if_missing: false
  max_transaction_events: 100
  max_transaction_bytes: 1048576
spool:
  path: ./data/spool.sqlite
  max_event_bytes: 262144
logging:
  level: info
  format: text
`

func TestDecodeValid(t *testing.T) {
	cfg, err := Decode(strings.NewReader(validConfig))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Postgres.StatusInterval.String() != "10s" {
		t.Fatalf("unexpected interval: %s", cfg.Postgres.StatusInterval)
	}
}

func TestDecodeRejectsUnknownField(t *testing.T) {
	_, err := Decode(strings.NewReader(validConfig + "\nunknown: true\n"))
	if err == nil || !strings.Contains(err.Error(), "field unknown not found") {
		t.Fatalf("expected unknown-field error, got %v", err)
	}
}

func TestDecodeRejectsInvalidIdentifier(t *testing.T) {
	input := strings.Replace(validConfig, "writerelay_slot_v1", "bad-slot", 1)
	_, err := Decode(strings.NewReader(input))
	if err == nil || !strings.Contains(err.Error(), "postgres.slot") {
		t.Fatalf("expected slot error, got %v", err)
	}
}

func TestDecodeRequiresOneDSNSource(t *testing.T) {
	input := strings.Replace(validConfig, "  dsn_env: WRITERELAY_POSTGRES_DSN\n", "", 1)
	_, err := Decode(strings.NewReader(input))
	if err == nil || !strings.Contains(err.Error(), "exactly one") {
		t.Fatalf("expected DSN source error, got %v", err)
	}
}
