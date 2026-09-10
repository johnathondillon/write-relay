// Package monitoring exposes bounded, read-only observations of the daemon.
package monitoring

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/johnathondillon/write-relay/internal/postgres"
	sqlitespool "github.com/johnathondillon/write-relay/internal/spool/sqlite"
)

const sampleTimeout = 5 * time.Second

type Server struct {
	path     string
	interval time.Duration
	capture  func() postgres.CaptureStatus
	read     func(context.Context, string) (sqlitespool.Stats, error)
	mu       sync.RWMutex
	stats    sqlitespool.Stats
	sampleOK bool
}

func New(path string, interval time.Duration, capture func() postgres.CaptureStatus) *Server {
	return &Server{path: path, interval: interval, capture: capture, read: sqlitespool.ReadStats}
}

// Run owns the listener and sampler. A bind/serve failure stops the daemon;
// a failed statistics sample makes readiness false but does not stop capture.
func (s *Server) Run(ctx context.Context, address string, logger *slog.Logger) error {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return fmt.Errorf("listen for monitoring: %w", err)
	}
	defer listener.Close()
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	sampled := make(chan struct{})
	go func() {
		defer close(sampled)
		s.sampleLoop(runCtx, logger)
	}()
	server := &http.Server{
		Handler: s.handler(runCtx), ReadHeaderTimeout: 3 * time.Second,
		ReadTimeout: 5 * time.Second, WriteTimeout: 5 * time.Second,
		IdleTimeout: 30 * time.Second, MaxHeaderBytes: 8 * 1024,
	}
	served := make(chan error, 1)
	go func() { served <- server.Serve(listener) }()
	logger.Info("monitoring listening", "address", listener.Addr().String())
	select {
	case err = <-served:
	case <-ctx.Done():
		shutdownCtx, stop := context.WithTimeout(context.Background(), 5*time.Second)
		if server.Shutdown(shutdownCtx) != nil {
			_ = server.Close()
		}
		stop()
		err = <-served
	}
	cancel()
	_ = server.Close()
	<-sampled
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func (s *Server) sampleLoop(ctx context.Context, logger *slog.Logger) {
	timer := time.NewTimer(0)
	defer timer.Stop()
	wasOK := true
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			ok := s.sample(ctx)
			if !ok && wasOK && ctx.Err() == nil {
				logger.Warn("monitoring spool sample failed; readiness unavailable")
			}
			wasOK = ok
			timer.Reset(s.interval)
		}
	}
}

func (s *Server) sample(ctx context.Context) bool {
	readCtx, cancel := context.WithTimeout(ctx, sampleTimeout)
	defer cancel()
	stats, err := s.read(readCtx, s.path)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sampleOK = err == nil && readCtx.Err() == nil
	if s.sampleOK {
		s.stats = stats
	}
	return s.sampleOK
}

func (s *Server) snapshot(now time.Time) (sqlitespool.Stats, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	// Allow a complete next sample plus scheduling slack. A stuck sampler cannot
	// keep exporting an old snapshot indefinitely as healthy.
	fresh := s.sampleOK && !s.stats.SampledAt.IsZero() &&
		now.Sub(s.stats.SampledAt) <= s.interval+2*sampleTimeout
	return s.stats, fresh
}

func (s *Server) handler(ctx context.Context) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		writeHealth(w, ctx.Err() == nil, map[string]bool{"alive": ctx.Err() == nil})
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		_, fresh := s.snapshot(time.Now())
		connected := s.capture().Connected
		ready := ctx.Err() == nil && connected && fresh
		writeHealth(w, ready, map[string]bool{
			"ready": ready, "capture_connected": connected, "spool_sample_fresh": fresh,
		})
	})
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store")
		now := time.Now()
		stats, fresh := s.snapshot(now)
		writeMetrics(w, s.capture(), stats, fresh, now)
	})
	return mux
}

func writeHealth(w http.ResponseWriter, ok bool, body map[string]bool) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	if !ok {
		w.WriteHeader(http.StatusServiceUnavailable)
	}
	_ = json.NewEncoder(w).Encode(body)
}
