package app

import (
	"context"
	"fmt"
	"io"
	"log/slog"

	"github.com/johnathondillon/write-relay/internal/config"
	"github.com/johnathondillon/write-relay/internal/delivery"
	"github.com/johnathondillon/write-relay/internal/postgres"
	sqlitespool "github.com/johnathondillon/write-relay/internal/spool/sqlite"
)

func Run(ctx context.Context, cfg config.Config, logger *slog.Logger, stdout io.Writer) error {
	store, err := sqlitespool.Open(ctx, cfg.Spool.Path)
	if err != nil {
		return err
	}
	defer store.Close()

	sinks := make(map[string]delivery.Sink, len(cfg.Delivery.Sinks))
	registrations := make([]delivery.SinkRegistration, 0, len(cfg.Delivery.Sinks))
	for _, sinkConfig := range cfg.Delivery.Sinks {
		switch sinkConfig.Type {
		case "webhook":
			sink, registration, err := delivery.NewWebhookSender(
				sinkConfig, cfg.Delivery.RequestTimeout,
			)
			if err != nil {
				return fmt.Errorf("configure delivery sink %q: %w", sinkConfig.Name, err)
			}
			sinks[sinkConfig.Name] = sink
			registrations = append(registrations, registration)
		case "stdout":
			sink, registration := delivery.NewStdoutSink(sinkConfig.Name, stdout)
			sinks[sinkConfig.Name] = sink
			registrations = append(registrations, registration)
		default:
			return fmt.Errorf("unsupported delivery sink type %q", sinkConfig.Type)
		}
	}
	if err := store.ConfigureSinks(ctx, registrations); err != nil {
		return err
	}

	replicator := postgres.NewReplicator(cfg, store, logger)
	if len(sinks) == 0 {
		return replicator.Run(ctx)
	}
	worker := delivery.NewWorker(
		store, sinks, cfg.Delivery.PollInterval,
		cfg.Delivery.Retry.InitialDelay, cfg.Delivery.Retry.MaxDelay,
		cfg.Delivery.Retry.MaxAttempts, logger,
	)

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	results := make(chan error, 2)
	go func() {
		results <- replicator.Run(runCtx)
	}()
	go func() {
		results <- worker.Run(runCtx)
	}()

	first := <-results
	cancel()
	second := <-results
	if first != nil {
		return first
	}
	return second
}
