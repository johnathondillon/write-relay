package monitoring

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/johnathondillon/write-relay/internal/diskspace"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/johnathondillon/write-relay/internal/postgres"
	sqlitespool "github.com/johnathondillon/write-relay/internal/spool/sqlite"
)

func get(handler http.Handler, path string) *httptest.ResponseRecorder {
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
	return response
}

func TestHealthReadinessAndCachedSpoolFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	path := filepath.Join(t.TempDir(), "spool.sqlite")
	store, err := sqlitespool.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	connected := false
	server := New(path, time.Second, func() postgres.CaptureStatus { return postgres.CaptureStatus{Connected: connected} })
	handler := server.handler(ctx)
	if get(handler, "/healthz").Code != 200 || get(handler, "/readyz").Code != 503 {
		t.Fatal("startup must be alive but not ready")
	}
	reads := 0
	server.read = func(ctx context.Context, path string) (sqlitespool.Stats, error) {
		reads++
		return sqlitespool.ReadStats(ctx, path)
	}
	if !server.sample(ctx) {
		t.Fatal("sample failed")
	}
	if get(handler, "/readyz").Code != 503 {
		t.Fatal("spool alone must not make capture ready")
	}
	connected = true
	// Idle capture needs no events or destinations to be ready. Repeated HTTP
	// requests must only read the cache, including metrics and readiness probes.
	for range 50 {
		if get(handler, "/readyz").Code != 200 {
			t.Fatal("idle capture should be ready")
		}
		response := get(handler, "/metrics")
		if response.Header().Get("Content-Type") != "text/plain; version=0.0.4; charset=utf-8" || !strings.Contains(response.Body.String(), "writerelay_spool_events 0\n") {
			t.Fatal(response)
		}
	}
	if reads != 1 {
		t.Fatalf("HTTP requests caused %d reads", reads)
	}
	connected = false
	if get(handler, "/readyz").Code != 503 || get(handler, "/healthz").Code != 200 {
		t.Fatal("disconnect should fail readiness only")
	}
	connected = true
	server.read = func(context.Context, string) (sqlitespool.Stats, error) {
		return sqlitespool.Stats{}, errors.New("secret-dsn")
	}
	if server.sample(ctx) {
		t.Fatal("expected sample failure")
	}
	response := get(handler, "/metrics").Body.String()
	if get(handler, "/readyz").Code != 503 || !strings.Contains(response, "writerelay_spool_sample_fresh 0\n") || strings.Contains(response, "writerelay_spool_events") || strings.Contains(response, "secret-dsn") {
		t.Fatalf("failed read must omit spool counts and secrets: %s", response)
	}
	server.read = sqlitespool.ReadStats
	if !server.sample(ctx) || get(handler, "/readyz").Code != 200 {
		t.Fatal("readiness did not recover")
	}
	server.mu.Lock()
	server.stats.SampledAt = time.Now().Add(-time.Minute)
	server.mu.Unlock()
	if get(handler, "/readyz").Code != 503 || strings.Contains(get(handler, "/metrics").Body.String(), "writerelay_spool_events") {
		t.Fatal("stale sample reported as fresh")
	}
	if !server.sample(ctx) {
		t.Fatal("sample failed")
	}
	cancel()
	if get(handler, "/healthz").Code != 503 || get(handler, "/readyz").Code != 503 {
		t.Fatal("shutdown must fail probes")
	}
}

func TestSamplerCancellationAndBindFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	server := New("unused", time.Second, func() postgres.CaptureStatus { return postgres.CaptureStatus{} })
	started := make(chan struct{})
	server.read = func(ctx context.Context, _ string) (sqlitespool.Stats, error) {
		if _, ok := ctx.Deadline(); !ok {
			t.Error("sample has no deadline")
		}
		close(started)
		<-ctx.Done()
		return sqlitespool.Stats{}, ctx.Err()
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	done := make(chan struct{})
	go func() { server.sampleLoop(ctx, logger); close(done) }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("sampler never started")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("sampler did not stop")
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	if err := server.Run(context.Background(), listener.Addr().String(), logger); err == nil {
		t.Fatal("occupied monitoring port should fail startup")
	}
}

func TestMetricsStatesRetentionAndPrivacy(t *testing.T) {
	now := time.Now()
	stats := sqlitespool.Stats{
		SampledAt: now, EventCount: 10, PrunedPayloads: 3, LastDurableLSN: "SECRET-LSN",
		Storage: sqlitespool.FileSizes{DatabaseBytes: 1024, WALBytes: 2048, SHMBytes: 512},
		Sinks: []sqlitespool.SinkStats{
			{Name: "active", Active: true, Deliveries: sqlitespool.DeliveryCounts{Pending: 2, RetryWait: 3, Delivered: 4, DeadLetter: 1}, OldestWaiting: &sqlitespool.WaitingStats{CapturedAt: now.Add(-30 * time.Second)}},
			{Name: "retired", Deliveries: sqlitespool.DeliveryCounts{Delivered: 10}},
			{Name: "empty", Active: true},
		},
	}
	var out strings.Builder
	writeMetrics(&out, postgres.CaptureStatus{Connected: true, Transactions: 5, LastCaptureUnix: now.Unix()}, stats, true, now)
	for _, want := range []string{
		"writerelay_capture_transactions_total 5\n",
		"writerelay_spool_events 10\n",
		"writerelay_spool_payloads{state=\"retained\"} 7\n",
		"writerelay_spool_payloads{state=\"pruned\"} 3\n",
		"writerelay_spool_file_bytes{file=\"wal\"} 2048\n",
		"writerelay_deliveries{sink=\"active\",state=\"pending\"} 2\n",
		"writerelay_deliveries{sink=\"active\",state=\"retry_wait\"} 3\n",
		"writerelay_deliveries{sink=\"active\",state=\"dead_letter\"} 1\n",
		"writerelay_deliveries{sink=\"retired\",state=\"delivered\"} 10\n",
		"writerelay_sink_active{sink=\"retired\"} 0\n",
		"writerelay_oldest_waiting_age_seconds{sink=\"active\"} 30\n",
		"writerelay_oldest_waiting_age_seconds{sink=\"empty\"} 0\n",
	} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("missing %s", want)
		}
	}
	if strings.Contains(out.String(), "SECRET") {
		t.Fatal("checkpoint was leaked")
	}
	if got := label("a\\b\"c\nd"); got != `"a\\b\"c\nd"` {
		t.Fatalf("invalid Prometheus escaping: %s", got)
	}
}

func TestDiskPauseReadinessAndMetrics(t *testing.T) {
	capture := postgres.CaptureStatus{Connected: true, Disk: diskspace.Status{Enabled: true, PauseBelowBytes: 100, ResumeAtBytes: 200}}
	server := New("unused", time.Second, func() postgres.CaptureStatus { return capture })
	server.stats = sqlitespool.Stats{SampledAt: time.Now()}
	server.sampleOK = true
	handler := server.handler(t.Context())
	if get(handler, "/readyz").Code != 503 {
		t.Fatal("unmeasured disk reported ready")
	}
	capture.Disk.SampleFresh = true
	capture.Disk.AvailableBytes = 99
	capture.Disk.Paused = true
	capture.Disk.Reason = "low_disk"
	for _, reason := range []string{"low_disk", "disk_check_failed"} {
		capture.Disk.Reason = reason
		capture.Disk.SampleFresh = reason == "low_disk"
		response := get(handler, "/readyz")
		var body map[string]any
		if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
			t.Fatal(err)
		}
		if response.Code != 503 || body["capture_pause_reason"] != reason || body["capture_paused"] != true || get(handler, "/healthz").Code != 200 {
			t.Fatal(body)
		}
		metrics := get(handler, "/metrics").Body.String()
		if !strings.Contains(metrics, `writerelay_capture_pause{reason="`+reason+`"} 1`) || !strings.Contains(metrics, "writerelay_spool_events 0") {
			t.Fatal(metrics)
		}
		if strings.Contains(metrics, "# TYPE writerelay_disk_available_bytes gauge") != capture.Disk.SampleFresh {
			t.Fatal("stale free space was exported")
		}
	}
	capture.Disk.Paused = false
	capture.Disk.Reason = ""
	capture.Disk.SampleFresh = true
	capture.Disk.AvailableBytes = 200
	if get(handler, "/readyz").Code != 200 {
		t.Fatal("recovery not ready")
	}
	capture.Disk.SampleFresh = false
	if get(handler, "/readyz").Code != 503 {
		t.Fatal("stale disk sample ready")
	}
	capture.Disk.Enabled = false
	if get(handler, "/readyz").Code != 200 {
		t.Fatal("disabled protection changed readiness")
	}
}
