package logging

import (
	"fmt"
	"io"
	"log/slog"

	"github.com/johnathondillon/write-relay/internal/config"
)

func New(cfg config.LoggingConfig, output io.Writer) (*slog.Logger, error) {
	var level slog.Level
	switch cfg.Level {
	case "debug":
		level = slog.LevelDebug
	case "info":
		level = slog.LevelInfo
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	default:
		return nil, fmt.Errorf("unsupported log level %q", cfg.Level)
	}

	options := &slog.HandlerOptions{Level: level}
	if cfg.Format == "json" {
		return slog.New(slog.NewJSONHandler(output, options)), nil
	}
	return slog.New(slog.NewTextHandler(output, options)), nil
}
