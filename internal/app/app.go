package app

import (
	"context"
	"fmt"
	"io"
	"log/slog"

	"github.com/johnathondillon/write-relay/internal/config"
	"github.com/johnathondillon/write-relay/internal/delivery"
	"github.com/johnathondillon/write-relay/internal/monitoring"
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
	tasks := []func(context.Context) error{replicator.Run}
	if len(sinks) > 0 {
		worker := delivery.NewWorker(
			store, sinks, cfg.Delivery.PollInterval,
			cfg.Delivery.Retry.InitialDelay, cfg.Delivery.Retry.MaxDelay,
			cfg.Delivery.Retry.MaxAttempts, logger,
		)
		tasks = append(tasks, worker.Run)
	}
	if cfg.Monitoring.Listen != "" {
		monitor := monitoring.New(cfg.Spool.Path, cfg.Monitoring.SampleInterval, replicator.Status)
		tasks = append(tasks, func(ctx context.Context) error {
			return monitor.Run(ctx, cfg.Monitoring.Listen, logger)
		})
	}
	return runTogether(ctx, tasks...)
}

// Wait for every component before closing their shared spool. The first exit
// cancels its peers; preserve errors even if graceful shutdown arrives first.
func runTogether(ctx context.Context, tasks ...func(context.Context) error) error {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	results := make(chan error, len(tasks))
	for _, task := range tasks {
		go func() { results <- task(runCtx) }()
	}
	var firstError error
	for range tasks {
		err := <-results
		cancel()
		if firstError == nil && err != nil {
			firstError = err
		}
	}
	return firstError
}
