package config

import (
	"os"
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
delivery:
  poll_interval: 1s
  request_timeout: 10s
  retry:
    initial_delay: 1s
    max_delay: 5m
    max_attempts: 10
  sinks:
    - name: orders_webhook
      type: webhook
      url: https://events.example.test/orders
      authorization_env: WRITERELAY_WEBHOOK_AUTHORIZATION
      signing_secret_env: WRITERELAY_WEBHOOK_SIGNING_SECRET
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
	if cfg.Delivery.RequestTimeout.String() != "10s" || len(cfg.Delivery.Sinks) != 1 {
		t.Fatalf("unexpected delivery config: %#v", cfg.Delivery)
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

func TestDecodeAllowsCaptureOnlyConfiguration(t *testing.T) {
	input := `
version: 1
postgres:
  dsn_env: WRITERELAY_POSTGRES_DSN
  slot: writerelay_slot_v1
  publication: writerelay_publication
  message_prefix: writerelay.v1
spool:
  path: ./data/spool.sqlite
`
	cfg, err := Decode(strings.NewReader(input))
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Delivery.Sinks) != 0 || cfg.Delivery.Retry.MaxAttempts != 10 {
		t.Fatalf("unexpected defaults: %#v", cfg.Delivery)
	}
}

func TestDecodeRejectsUnsafeWebhookConfiguration(t *testing.T) {
	input := strings.Replace(
		validConfig,
		"https://events.example.test/orders",
		"http://events.example.test/orders",
		1,
	)
	_, err := Decode(strings.NewReader(input))
	if err == nil || !strings.Contains(err.Error(), "requires HTTPS") {
		t.Fatalf("expected HTTPS error, got %v", err)
	}
}

func TestDecodeRejectsDuplicateSinkName(t *testing.T) {
	duplicate := `
    - name: orders_webhook
      type: webhook
      url: https://other.example.test/orders
`
	input := strings.Replace(validConfig, "logging:\n", duplicate+"logging:\n", 1)
	_, err := Decode(strings.NewReader(input))
	if err == nil || !strings.Contains(err.Error(), "duplicated") {
		t.Fatalf("expected duplicate-name error, got %v", err)
	}
}

func TestDecodeAllowsStdoutDevelopmentSink(t *testing.T) {
	input := strings.Replace(validConfig, `
    - name: orders_webhook
      type: webhook
      url: https://events.example.test/orders
      authorization_env: WRITERELAY_WEBHOOK_AUTHORIZATION
      signing_secret_env: WRITERELAY_WEBHOOK_SIGNING_SECRET
`, `
    - name: development
      type: stdout
`, 1)
	cfg, err := Decode(strings.NewReader(input))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Delivery.Sinks[0].Type != "stdout" {
		t.Fatalf("sink = %#v", cfg.Delivery.Sinks[0])
	}
}

func TestExampleConfigurationStaysValid(t *testing.T) {
	file, err := os.Open("../../writerelay.example.yaml")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if _, err := Decode(file); err != nil {
		t.Fatal(err)
	}
}
