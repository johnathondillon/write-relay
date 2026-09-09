package delivery

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/johnathondillon/write-relay/internal/failure"
)

const maxStoredErrorBytes = 2048

var ErrSinkConfigurationConflict = errors.New("delivery sink configuration conflicts with durable spool")

type SinkRegistration struct {
	Name         string
	Type         string
	ConfigSHA256 [32]byte
}

type Delivery struct {
	EventSequence int64
	SinkID        int64
	SinkName      string
	SinkType      string
	Source        string
	ID            string
	Type          string
	Subject       string
	Payload       []byte
	Attempts      int
}

type AttemptResult struct {
	Success    bool
	Retryable  bool
	RetryAfter time.Duration
	StatusCode int
	Failure    string
}

type Store interface {
	ConfigureSinks(context.Context, []SinkRegistration) error
	NextDueDelivery(context.Context, time.Time) (Delivery, bool, error)
	MarkDelivered(context.Context, Delivery, int, int, time.Time) error
	MarkFailed(context.Context, Delivery, int, bool, time.Time, string, int, time.Time) error
}

type Sink interface {
	Send(context.Context, Delivery) AttemptResult
}

type Worker struct {
	store        Store
	sinks        map[string]Sink
	pollInterval time.Duration
	initialDelay time.Duration
	maxDelay     time.Duration
	maxAttempts  int
	logger       *slog.Logger
	hooks        failure.Hooks
}

func NewWorker(
	store Store,
	sinks map[string]Sink,
	pollInterval time.Duration,
	initialDelay time.Duration,
	maxDelay time.Duration,
	maxAttempts int,
	logger *slog.Logger,
) *Worker {
	return NewWorkerWithHooks(
		store, sinks, pollInterval, initialDelay, maxDelay, maxAttempts,
		logger, failure.Hooks{},
	)
}

// NewWorkerWithHooks exists for deterministic process-crash tests. Production
// composition calls NewWorker with inert hooks.
func NewWorkerWithHooks(
	store Store,
	sinks map[string]Sink,
	pollInterval time.Duration,
	initialDelay time.Duration,
	maxDelay time.Duration,
	maxAttempts int,
	logger *slog.Logger,
	hooks failure.Hooks,
) *Worker {
	return &Worker{
		store: store, sinks: sinks, pollInterval: pollInterval,
		initialDelay: initialDelay, maxDelay: maxDelay, maxAttempts: maxAttempts,
		logger: logger, hooks: hooks,
	}
}

func (w *Worker) Run(ctx context.Context) error {
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-timer.C:
		}
		processed, err := w.ProcessOne(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		delay := w.pollInterval
		if processed {
			delay = 0
		}
		timer.Reset(delay)
	}
}

func (w *Worker) ProcessOne(ctx context.Context) (bool, error) {
	item, found, err := w.store.NextDueDelivery(ctx, time.Now().UTC())
	if err != nil {
		return false, fmt.Errorf("load next delivery: %w", err)
	}
	if !found {
		return false, nil
	}
	sink, ok := w.sinks[item.SinkName]
	if !ok {
		return false, fmt.Errorf("no delivery sink configured for active sink %q", item.SinkName)
	}

	result := sink.Send(ctx, item)
	if ctx.Err() != nil && !result.Success {
		return false, nil
	}
	attempt := item.Attempts + 1
	recordCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	now := time.Now().UTC()
	if result.Success {
		w.hooks.CallAfterSinkSuccess()
		if err := w.store.MarkDelivered(recordCtx, item, attempt, result.StatusCode, now); err != nil {
			return false, fmt.Errorf("record successful delivery: %w", err)
		}
		w.logger.Info("event delivered",
			"sink", item.SinkName, "source", item.Source, "id", item.ID,
			"attempt", attempt, "status", result.StatusCode)
		return true, nil
	}

	permanent := !result.Retryable || attempt >= w.maxAttempts
	retryAt := now
	if !permanent {
		delay := w.retryDelay(item.Attempts)
		if result.RetryAfter > delay {
			delay = result.RetryAfter
			if delay > w.maxDelay {
				delay = w.maxDelay
			}
		}
		retryAt = now.Add(delay)
	}
	failure := truncate(result.Failure, maxStoredErrorBytes)
	if err := w.store.MarkFailed(
		recordCtx, item, attempt, permanent, retryAt, failure, result.StatusCode, now,
	); err != nil {
		return false, fmt.Errorf("record failed delivery: %w", err)
	}
	if permanent {
		w.logger.Error("event moved to dead letter",
			"sink", item.SinkName, "source", item.Source, "id", item.ID,
			"attempt", attempt, "status", result.StatusCode, "reason", failure)
	} else {
		w.logger.Warn("event delivery will retry",
			"sink", item.SinkName, "source", item.Source, "id", item.ID,
			"attempt", attempt, "status", result.StatusCode, "retry_at", retryAt,
			"reason", failure)
	}
	return true, nil
}

func (w *Worker) retryDelay(previousAttempts int) time.Duration {
	delay := w.initialDelay
	for i := 0; i < previousAttempts && delay < w.maxDelay; i++ {
		if delay > w.maxDelay/2 {
			return w.maxDelay
		}
		delay *= 2
	}
	if delay > w.maxDelay {
		return w.maxDelay
	}
	return delay
}

func truncate(value string, limit int) string {
	if len(value) <= limit {
		return value
	}
	return value[:limit]
}
