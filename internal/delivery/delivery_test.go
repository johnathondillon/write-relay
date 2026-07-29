package delivery

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"
)

type fakeDeliveryStore struct {
	item              Delivery
	available         bool
	failDeliveredOnce bool
	delivered         bool
	failed            bool
	permanent         bool
	attempt           int
	retryAt           time.Time
}

func (*fakeDeliveryStore) ConfigureSinks(context.Context, []SinkRegistration) error {
	return nil
}

func (s *fakeDeliveryStore) NextDueDelivery(context.Context, time.Time) (Delivery, bool, error) {
	return s.item, s.available, nil
}

func (s *fakeDeliveryStore) MarkDelivered(
	_ context.Context,
	_ Delivery,
	attempt int,
	_ int,
	_ time.Time,
) error {
	if s.failDeliveredOnce {
		s.failDeliveredOnce = false
		return errors.New("simulated crash before recording success")
	}
	s.delivered = true
	s.available = false
	s.attempt = attempt
	return nil
}

func (s *fakeDeliveryStore) MarkFailed(
	_ context.Context,
	_ Delivery,
	attempt int,
	permanent bool,
	retryAt time.Time,
	_ string,
	_ int,
	_ time.Time,
) error {
	s.failed = true
	s.permanent = permanent
	s.attempt = attempt
	s.retryAt = retryAt
	s.item.Attempts = attempt
	if permanent {
		s.available = false
	}
	return nil
}

type senderFunc func(context.Context, Delivery) AttemptResult

func (fn senderFunc) Send(ctx context.Context, item Delivery) AttemptResult {
	return fn(ctx, item)
}

func TestSuccessIsRetriedIfRecordingItFails(t *testing.T) {
	store := &fakeDeliveryStore{
		item: Delivery{
			EventSequence: 1, SinkID: 1, SinkName: "orders",
			Source: "urn:test", ID: "one", Payload: []byte(`{}`),
		},
		available: true, failDeliveredOnce: true,
	}
	calls := 0
	worker := testWorker(store, senderFunc(func(context.Context, Delivery) AttemptResult {
		calls++
		return AttemptResult{Success: true, StatusCode: 204}
	}), 10)

	if _, err := worker.ProcessOne(t.Context()); err == nil {
		t.Fatal("expected simulated durable recording failure")
	}
	if !store.available {
		t.Fatal("delivery disappeared after unrecorded success")
	}
	if _, err := worker.ProcessOne(t.Context()); err != nil {
		t.Fatal(err)
	}
	if calls != 2 || !store.delivered {
		t.Fatalf("calls=%d delivered=%v", calls, store.delivered)
	}
}

func TestRetryAndDeadLetterDecisions(t *testing.T) {
	store := &fakeDeliveryStore{
		item: Delivery{
			EventSequence: 1, SinkID: 1, SinkName: "orders",
			Source: "urn:test", ID: "one", Payload: []byte(`{}`),
		},
		available: true,
	}
	worker := testWorker(store, senderFunc(func(context.Context, Delivery) AttemptResult {
		return AttemptResult{
			Retryable: true, RetryAfter: 10 * time.Second,
			StatusCode: 503, Failure: "temporary",
		}
	}), 2)
	before := time.Now()
	if _, err := worker.ProcessOne(t.Context()); err != nil {
		t.Fatal(err)
	}
	if !store.failed || store.permanent || store.attempt != 1 ||
		store.retryAt.Before(before.Add(9*time.Second)) {
		t.Fatalf("unexpected retry state: %#v", store)
	}
	if _, err := worker.ProcessOne(t.Context()); err != nil {
		t.Fatal(err)
	}
	if !store.permanent || store.attempt != 2 {
		t.Fatalf("expected exhausted delivery to dead-letter: %#v", store)
	}
}

func TestPermanentFailureDeadLettersImmediately(t *testing.T) {
	store := &fakeDeliveryStore{
		item: Delivery{SinkName: "orders"}, available: true,
	}
	worker := testWorker(store, senderFunc(func(context.Context, Delivery) AttemptResult {
		return AttemptResult{StatusCode: 400, Failure: "bad request"}
	}), 10)
	if _, err := worker.ProcessOne(t.Context()); err != nil {
		t.Fatal(err)
	}
	if !store.permanent || store.attempt != 1 {
		t.Fatalf("unexpected permanent failure: %#v", store)
	}
}

func testWorker(store Store, sink Sink, maxAttempts int) *Worker {
	return NewWorker(
		store, map[string]Sink{"orders": sink}, time.Second,
		time.Second, time.Minute, maxAttempts,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
}
