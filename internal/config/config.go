package config

import (
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
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
