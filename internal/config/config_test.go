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

func TestMonitoringConfiguration(t *testing.T) {
	cfg, err := Decode(strings.NewReader(validConfig))
	if err != nil || cfg.Monitoring.Listen != "" || cfg.Monitoring.SampleInterval.String() != "15s" {
		t.Fatalf("monitoring defaults: %+v %v", cfg.Monitoring, err)
	}
	for _, address := range []string{"127.0.0.1:9090", "[::1]:9090", "0.0.0.0:9090"} {
		cfg, err := Decode(strings.NewReader(validConfig + "\nmonitoring:\n  listen: '" + address + "'\n  sample_interval: 1s\n"))
		if err != nil || cfg.Monitoring.Listen != address || cfg.Monitoring.SampleInterval.String() != "1s" {
			t.Fatalf("valid monitoring: %+v %v", cfg.Monitoring, err)
		}
	}
	for _, settings := range []string{"listen: ':9090'", "listen: 'localhost:9090'", "listen: '127.0.0.1:0'", "listen: '127.0.0.1:65536'", "sample_interval: 1ms", "sample_interval: 10m", "sample_interval: nope", "unknown: true"} {
		if _, err := Decode(strings.NewReader(validConfig + "\nmonitoring:\n  " + settings + "\n")); err == nil {
			t.Fatalf("accepted invalid monitoring: %s", settings)
		}
	}
}

func TestDiskSpaceConfiguration(t *testing.T) {
	defaults, err := Decode(strings.NewReader(validConfig))
	if err != nil {
		t.Fatal(err)
	}
	if defaults.Spool.DiskSpace.PauseBelowBytes != 0 || defaults.Spool.DiskSpace.CheckInterval.String() != "5s" {
		t.Fatal(defaults.Spool.DiskSpace)
	}
	for _, test := range []struct {
		yaml  string
		valid bool
	}{
		{"pause_below_bytes: 100\n    resume_at_bytes: 200", true},
		{"pause_below_bytes: 0\n    resume_at_bytes: 0", true},
		{"pause_below_bytes: -1", false},
		{"pause_below_bytes: 100\n    resume_at_bytes: -1", false},
		{"pause_below_bytes: 100", false},
		{"resume_at_bytes: 100", false},
		{"pause_below_bytes: 100\n    resume_at_bytes: 100", false},
		{"pause_below_bytes: 100\n    resume_at_bytes: 99", false},
		{"check_interval: 0s", false},
		{"check_interval: 61s", false},
		{"check_interval: nonsense", false},
		{"pause_below_bytes: 9223372036854775808", false},
		{"unknown_threshold: 100", false},
	} {
		t.Run(test.yaml, func(t *testing.T) {
			yaml := strings.Replace(validConfig, "  max_event_bytes: 262144", "  max_event_bytes: 262144\n  disk_space:\n    "+test.yaml, 1)
			_, err := Decode(strings.NewReader(yaml))
			if (err == nil) != test.valid {
				t.Fatal(err)
			}
		})
	}
}
