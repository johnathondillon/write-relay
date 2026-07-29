package config

import (
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"regexp"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

const (
	CurrentVersion       = 1
	RequiredPrefix       = "writerelay.v1"
	DefaultMaxEventBytes = 256 * 1024
)

var identifierPattern = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,62}$`)

type Config struct {
	Version  int            `yaml:"version"`
	Postgres PostgresConfig `yaml:"postgres"`
	Spool    SpoolConfig    `yaml:"spool"`
	Delivery DeliveryConfig `yaml:"delivery"`
	Logging  LoggingConfig  `yaml:"logging"`
}

type PostgresConfig struct {
	DSN                  string        `yaml:"dsn"`
	DSNEnv               string        `yaml:"dsn_env"`
	Slot                 string        `yaml:"slot"`
	Publication          string        `yaml:"publication"`
	MessagePrefix        string        `yaml:"message_prefix"`
	StatusInterval       time.Duration `yaml:"-"`
	StatusIntervalValue  string        `yaml:"status_interval"`
	CreateSlotIfMissing  bool          `yaml:"create_slot_if_missing"`
	MaxTransactionEvents int           `yaml:"max_transaction_events"`
	MaxTransactionBytes  int           `yaml:"max_transaction_bytes"`
}

type SpoolConfig struct {
	Path          string `yaml:"path"`
	MaxEventBytes int    `yaml:"max_event_bytes"`
}

type DeliveryConfig struct {
	PollInterval        time.Duration `yaml:"-"`
	PollIntervalValue   string        `yaml:"poll_interval"`
	RequestTimeout      time.Duration `yaml:"-"`
	RequestTimeoutValue string        `yaml:"request_timeout"`
	Retry               RetryConfig   `yaml:"retry"`
	Sinks               []SinkConfig  `yaml:"sinks"`
}

type RetryConfig struct {
	InitialDelay      time.Duration `yaml:"-"`
	InitialDelayValue string        `yaml:"initial_delay"`
	MaxDelay          time.Duration `yaml:"-"`
	MaxDelayValue     string        `yaml:"max_delay"`
	MaxAttempts       int           `yaml:"max_attempts"`
}

type SinkConfig struct {
	Name              string `yaml:"name"`
	Type              string `yaml:"type"`
	URL               string `yaml:"url"`
	AuthorizationEnv  string `yaml:"authorization_env"`
	SigningSecretEnv  string `yaml:"signing_secret_env"`
	AllowInsecureHTTP bool   `yaml:"allow_insecure_http"`
}

type LoggingConfig struct {
	Level  string `yaml:"level"`
	Format string `yaml:"format"`
}

func Load(path string) (Config, error) {
	f, err := os.Open(path)
	if err != nil {
		return Config{}, fmt.Errorf("open config: %w", err)
	}
	defer f.Close()

	cfg, err := Decode(f)
	if err != nil {
		return Config{}, fmt.Errorf("load config %q: %w", path, err)
	}
	return cfg, nil
}

func Decode(r io.Reader) (Config, error) {
	var cfg Config
	decoder := yaml.NewDecoder(r)
	decoder.KnownFields(true)
	if err := decoder.Decode(&cfg); err != nil {
		return Config{}, fmt.Errorf("decode YAML: %w", err)
	}
	if err := cfg.setDefaultsAndValidate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func (c *Config) setDefaultsAndValidate() error {
	if c.Version != CurrentVersion {
		return fmt.Errorf("configuration version must be %d", CurrentVersion)
	}
	if (c.Postgres.DSN == "") == (c.Postgres.DSNEnv == "") {
		return errors.New("postgres requires exactly one of dsn and dsn_env")
	}
	if c.Postgres.DSNEnv != "" {
		envPattern := regexp.MustCompile(`^[A-Z_][A-Z0-9_]*$`)
		if !envPattern.MatchString(c.Postgres.DSNEnv) {
			return errors.New("postgres.dsn_env must be an uppercase environment variable name")
		}
	}
	if !identifierPattern.MatchString(c.Postgres.Slot) {
		return errors.New("postgres.slot must match ^[a-z_][a-z0-9_]{0,62}$")
	}
	if !identifierPattern.MatchString(c.Postgres.Publication) {
		return errors.New("postgres.publication must match ^[a-z_][a-z0-9_]{0,62}$")
	}
	if c.Postgres.MessagePrefix != RequiredPrefix {
		return fmt.Errorf("postgres.message_prefix must be exactly %q", RequiredPrefix)
	}
	if c.Postgres.StatusIntervalValue == "" {
		c.Postgres.StatusIntervalValue = "10s"
	}
	duration, err := time.ParseDuration(c.Postgres.StatusIntervalValue)
	if err != nil {
		return fmt.Errorf("postgres.status_interval: %w", err)
	}
	if duration < time.Second || duration > 5*time.Minute {
		return errors.New("postgres.status_interval must be between 1s and 5m")
	}
	c.Postgres.StatusInterval = duration
	if c.Postgres.MaxTransactionEvents == 0 {
		c.Postgres.MaxTransactionEvents = 10_000
	}
	if c.Postgres.MaxTransactionEvents < 1 || c.Postgres.MaxTransactionEvents > 1_000_000 {
		return errors.New("postgres.max_transaction_events must be between 1 and 1000000")
	}
	if c.Postgres.MaxTransactionBytes == 0 {
		c.Postgres.MaxTransactionBytes = 8 * 1024 * 1024
	}
	if c.Postgres.MaxTransactionBytes < DefaultMaxEventBytes {
		return fmt.Errorf("postgres.max_transaction_bytes must be at least %d", DefaultMaxEventBytes)
	}
	if c.Spool.Path == "" {
		return errors.New("spool.path is required")
	}
	if c.Spool.MaxEventBytes == 0 {
		c.Spool.MaxEventBytes = DefaultMaxEventBytes
	}
	if c.Spool.MaxEventBytes < 1 || c.Spool.MaxEventBytes > DefaultMaxEventBytes {
		return fmt.Errorf("spool.max_event_bytes must be between 1 and %d", DefaultMaxEventBytes)
	}
	if err := c.Delivery.setDefaultsAndValidate(); err != nil {
		return err
	}
	if c.Logging.Level == "" {
		c.Logging.Level = "info"
	}
	switch c.Logging.Level {
	case "debug", "info", "warn", "error":
	default:
		return errors.New("logging.level must be debug, info, warn, or error")
	}
	if c.Logging.Format == "" {
		c.Logging.Format = "text"
	}
	if c.Logging.Format != "text" && c.Logging.Format != "json" {
		return errors.New("logging.format must be text or json")
	}
	return nil
}

func (c *DeliveryConfig) setDefaultsAndValidate() error {
	if c.PollIntervalValue == "" {
		c.PollIntervalValue = "1s"
	}
	pollInterval, err := boundedDuration(
		"delivery.poll_interval", c.PollIntervalValue, 50*time.Millisecond, time.Minute,
	)
	if err != nil {
		return err
	}
	c.PollInterval = pollInterval

	if c.RequestTimeoutValue == "" {
		c.RequestTimeoutValue = "10s"
	}
	requestTimeout, err := boundedDuration(
		"delivery.request_timeout", c.RequestTimeoutValue, 100*time.Millisecond, 2*time.Minute,
	)
	if err != nil {
		return err
	}
	c.RequestTimeout = requestTimeout

	if c.Retry.InitialDelayValue == "" {
		c.Retry.InitialDelayValue = "1s"
	}
	initialDelay, err := boundedDuration(
		"delivery.retry.initial_delay", c.Retry.InitialDelayValue, 100*time.Millisecond, time.Hour,
	)
	if err != nil {
		return err
	}
	c.Retry.InitialDelay = initialDelay

	if c.Retry.MaxDelayValue == "" {
		c.Retry.MaxDelayValue = "5m"
	}
	maxDelay, err := boundedDuration(
		"delivery.retry.max_delay", c.Retry.MaxDelayValue, 100*time.Millisecond, 24*time.Hour,
	)
	if err != nil {
		return err
	}
	if maxDelay < initialDelay {
		return errors.New("delivery.retry.max_delay must not be less than initial_delay")
	}
	c.Retry.MaxDelay = maxDelay
	if c.Retry.MaxAttempts == 0 {
		c.Retry.MaxAttempts = 10
	}
	if c.Retry.MaxAttempts < 1 || c.Retry.MaxAttempts > 1000 {
		return errors.New("delivery.retry.max_attempts must be between 1 and 1000")
	}

	names := make(map[string]struct{}, len(c.Sinks))
	for index := range c.Sinks {
		sink := &c.Sinks[index]
		path := fmt.Sprintf("delivery.sinks[%d]", index)
		if !identifierPattern.MatchString(sink.Name) {
			return fmt.Errorf("%s.name must match ^[a-z_][a-z0-9_]{0,62}$", path)
		}
		if _, exists := names[sink.Name]; exists {
			return fmt.Errorf("%s.name %q is duplicated", path, sink.Name)
		}
		names[sink.Name] = struct{}{}
		switch sink.Type {
		case "webhook":
			endpoint, err := url.Parse(sink.URL)
			if err != nil || endpoint.Host == "" {
				return fmt.Errorf("%s.url must be an absolute HTTP(S) URL", path)
			}
			if endpoint.User != nil || endpoint.Fragment != "" {
				return fmt.Errorf("%s.url must not contain user information or a fragment", path)
			}
			switch strings.ToLower(endpoint.Scheme) {
			case "https":
			case "http":
				if !sink.AllowInsecureHTTP {
					return fmt.Errorf("%s.url requires HTTPS unless allow_insecure_http is true", path)
				}
			default:
				return fmt.Errorf("%s.url must use HTTP or HTTPS", path)
			}
			if err := validateOptionalEnvironmentName(path+".authorization_env", sink.AuthorizationEnv); err != nil {
				return err
			}
			if err := validateOptionalEnvironmentName(path+".signing_secret_env", sink.SigningSecretEnv); err != nil {
				return err
			}
		case "stdout":
			if sink.URL != "" || sink.AuthorizationEnv != "" ||
				sink.SigningSecretEnv != "" || sink.AllowInsecureHTTP {
				return fmt.Errorf("%s stdout sink does not accept webhook fields", path)
			}
		default:
			return fmt.Errorf("%s.type must be webhook or stdout", path)
		}
	}
	return nil
}

func boundedDuration(name, value string, minimum, maximum time.Duration) (time.Duration, error) {
	duration, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", name, err)
	}
	if duration < minimum || duration > maximum {
		return 0, fmt.Errorf("%s must be between %s and %s", name, minimum, maximum)
	}
	return duration, nil
}

func validateOptionalEnvironmentName(name, value string) error {
	if value == "" {
		return nil
	}
	envPattern := regexp.MustCompile(`^[A-Z_][A-Z0-9_]*$`)
	if !envPattern.MatchString(value) {
		return fmt.Errorf("%s must be an uppercase environment variable name", name)
	}
	return nil
}

func (c Config) PostgreSQLDSN() (string, error) {
	if c.Postgres.DSN != "" {
		return c.Postgres.DSN, nil
	}
	value, ok := os.LookupEnv(c.Postgres.DSNEnv)
	if !ok || value == "" {
		return "", fmt.Errorf("environment variable %s is not set", c.Postgres.DSNEnv)
	}
	return value, nil
}
