package app

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestRunTogetherCancelsAndWaitsForPeers(t *testing.T) {
	failure := errors.New("monitoring bind failed")
	stopped := make(chan struct{})
	result := make(chan error, 1)
	go func() {
		result <- runTogether(context.Background(),
			func(context.Context) error { return failure },
			func(ctx context.Context) error { <-ctx.Done(); close(stopped); return nil },
		)
	}()
	select {
	case err := <-result:
		if !errors.Is(err, failure) {
			t.Fatalf("lost component error: %v", err)
		}
		select {
		case <-stopped:
		default:
			t.Fatal("returned before peer stopped")
		}
	case <-time.After(time.Second):
		t.Fatal("components failed to stop")
	}
}

func TestRunTogetherPreservesErrorAfterCleanExit(t *testing.T) {
	failure := errors.New("delivery recording failed")
	err := runTogether(context.Background(),
		func(context.Context) error { return nil },
		func(ctx context.Context) error { <-ctx.Done(); return failure },
	)
	if !errors.Is(err, failure) {
		t.Fatalf("lost shutdown error: %v", err)
	}
}
