//go:build load

package load

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/johnathondillon/write-relay/internal/config"
	"github.com/johnathondillon/write-relay/internal/postgres"
	sqlitespool "github.com/johnathondillon/write-relay/internal/spool/sqlite"
)

type options struct {
	Events         int `json:"events"`
	BatchSize      int `json:"batch_size"`
	PaddingBytes   int `json:"data_padding_bytes"`
	TimeoutSeconds int `json:"timeout_seconds"`
}

type latency struct {
	P50 float64 `json:"p50_ms"`
	P95 float64 `json:"p95_ms"`
	P99 float64 `json:"p99_ms"`
	Max float64 `json:"max_ms"`
}

type phase struct {
	Events          int      `json:"events"`
	ProduceSeconds  float64  `json:"produce_seconds,omitempty"`
	SettleSeconds   float64  `json:"settle_seconds"`
	EventsPerSecond float64  `json:"events_per_second"`
	ReceiverLatency *latency `json:"receiver_latency,omitempty"`
}

type snapshot struct {
	Phase string            `json:"phase"`
	Stats sqlitespool.Stats `json:"spool"`
}

type report struct {
	SchemaVersion         int        `json:"schema_version"`
	Status                string     `json:"status"`
	Options               options    `json:"options"`
	GoVersion             string     `json:"go_version"`
	Platform              string     `json:"platform"`
	CPUs                  int        `json:"logical_cpus"`
	PostgresVersion       string     `json:"postgres_version"`
	Baseline              phase      `json:"baseline"`
	OutageCapture         phase      `json:"outage_capture"`
	Recovery              phase      `json:"recovery"`
	UnavailableAttempts   int64      `json:"http_503_attempts"`
	DuplicateAttempts     int        `json:"duplicate_attempts"`
	UniqueReceiverRecords int        `json:"unique_receiver_records"`
	SampledPeakSpoolBytes int64      `json:"sampled_peak_spool_bytes"`
	Snapshots             []snapshot `json:"snapshots"`
}

func TestLoadRecovery(t *testing.T) {
	opts := options{
		Events:         integerOption(t, "LOAD_EVENTS", 10_000, 10, 100_000),
		BatchSize:      integerOption(t, "LOAD_BATCH_SIZE", 100, 1, 1000),
		PaddingBytes:   integerOption(t, "LOAD_PAYLOAD_BYTES", 256, 0, 65536),
		TimeoutSeconds: integerOption(t, "LOAD_TIMEOUT_SECONDS", 300, 30, 1800),
	}
	if opts.BatchSize*(opts.PaddingBytes+1024) > 8*1024*1024 {
		t.Fatal("batch and payload settings exceed the test's 8 MiB transaction budget")
	}
	dsn := os.Getenv("WRITERELAY_LOAD_DSN")
	binary := os.Getenv("WRITERELAY_LOAD_BINARY")
	if dsn == "" || !filepath.IsAbs(binary) {
		t.Fatal("run make load: an isolated database DSN and absolute daemon path are required")
	}
	r := report{SchemaVersion: 1, Status: "failed", Options: opts, GoVersion: runtime.Version(), Platform: runtime.GOOS + "/" + runtime.GOARCH, CPUs: runtime.NumCPU()}
	// Write even a failed run's partial report, so an old success cannot stand in
	// for this run. Performance fields are observations, never pass thresholds.
	defer func() {
		if !t.Failed() {
			r.Status = "passed"
		}
		encoded, err := json.MarshalIndent(r, "", "  ")
		if err != nil {
			t.Error(err)
			return
		}
		if path := os.Getenv("LOAD_REPORT"); path != "" {
			if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
				t.Error(err)
				return
			}
			if err := os.WriteFile(path, append(encoded, '\n'), 0644); err != nil {
				t.Error(err)
			}
		}
		t.Logf("LOAD_REPORT\n%s", encoded)
	}()
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(opts.TimeoutSeconds)*time.Second)
	defer cancel()
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close(context.Background())
	var serverMajor int
	if err := admin.QueryRow(ctx, `SELECT version(), current_setting('server_version_num')::integer / 10000`).Scan(&r.PostgresVersion, &serverMajor); err != nil {
		t.Fatal(err)
	}
	if expected := os.Getenv("POSTGRES_VERSION"); expected != "" && expected != strconv.Itoa(serverMajor) {
		t.Fatalf("expected PostgreSQL %s, got %d", expected, serverMajor)
	}
	suffix := strconv.FormatInt(time.Now().UnixNano(), 36)
	source := "urn:writerelay:load:" + suffix
	table := "writerelay_load_" + suffix
	slot := "wr_load_" + suffix
	publication := "wr_load_pub_" + suffix
	// Only the table, publication, and slot created by this invocation are removed.
	defer func() {
		cleanupCtx, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		for _, query := range []string{"SELECT pg_drop_replication_slot('" + slot + "')", "DROP PUBLICATION IF EXISTS " + publication, "DROP TABLE IF EXISTS " + table} {
			if _, err := admin.Exec(cleanupCtx, query); err != nil {
				t.Errorf("load object cleanup: %v", err)
			}
		}
	}()
	directory := t.TempDir()
	receiver := newReceiver(t, directory, source, opts.Events, opts.PaddingBytes)
	defer receiver.db.Close()
	server := httptest.NewServer(http.HandlerFunc(receiver.serve))
	defer server.Close()
	cfgText := fmt.Sprintf(`version: 1
postgres:
  dsn_env: WRITERELAY_LOAD_DSN
  slot: %s
  publication: %s
  message_prefix: writerelay.v1
  status_interval: 1s
spool:
  path: %s
delivery:
  poll_interval: 50ms
  request_timeout: 2s
  retry:
    initial_delay: 100ms
    max_delay: 1s
    max_attempts: 1000
  sinks:
    - name: load_receiver
      type: webhook
      url: %s
      allow_insecure_http: true
logging:
  level: warn
  format: text
`, slot, publication, quoted(filepath.Join(directory, "spool.sqlite")), server.URL)
	// Validate through the ordinary production configuration parser.
	cfg, err := config.Decode(strings.NewReader(cfgText))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := postgres.Setup(ctx, cfg, true); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(ctx, "CREATE TABLE "+table+" (id integer PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(directory, "writerelay.yaml")
	if err := os.WriteFile(configPath, []byte(cfgText), 0600); err != nil {
		t.Fatal(err)
	}
	logPath := filepath.Join(directory, "relay.log")
	var daemon *process
	defer func() {
		if daemon != nil {
			daemon.stop(t, false)
		}
		if t.Failed() {
			data, _ := os.ReadFile(logPath)
			if len(data) > 8192 {
				data = data[len(data)-8192:]
			}
			t.Logf("daemon log tail:\n%s", data)
		}
	}()
	daemon = startProcess(t, binary, configPath, logPath)
	stats := waitStats(t, ctx, cfg.Spool.Path, daemon, receiver, &r, func(s sqlitespool.Stats) bool { return s.EventCount == 0 })
	r.Snapshots = append(r.Snapshots, snapshot{"empty", stats})
	baseline := max(1, opts.Events/10)
	started := time.Now()
	produce(t, ctx, admin, table, receiver, opts, 1, baseline)
	produced := time.Since(started)
	stats = waitStats(t, ctx, cfg.Spool.Path, daemon, receiver, &r, func(s sqlitespool.Stats) bool { return s.Deliveries.Delivered == int64(baseline) })
	r.Baseline = phase{Events: baseline, ProduceSeconds: produced.Seconds(), SettleSeconds: time.Since(started).Seconds(), ReceiverLatency: receiver.latencies(1, baseline)}
	r.Baseline.EventsPerSecond = float64(baseline) / r.Baseline.SettleSeconds
	r.Snapshots = append(r.Snapshots, snapshot{"baseline_complete", stats})
	t.Logf("baseline: %d events delivered in %.2fs", baseline, r.Baseline.SettleSeconds)

	receiver.unavailable.Store(true)
	started = time.Now()
	produce(t, ctx, admin, table, receiver, opts, baseline+1, opts.Events)
	produced = time.Since(started)
	backlog := opts.Events - baseline
	stats = waitStats(t, ctx, cfg.Spool.Path, daemon, receiver, &r, func(s sqlitespool.Stats) bool {
		return s.EventCount == int64(opts.Events) && s.Deliveries.Pending+s.Deliveries.RetryWait == int64(backlog) && s.Deliveries.RetryWait > 0
	})
	r.OutageCapture = phase{Events: backlog, ProduceSeconds: produced.Seconds(), SettleSeconds: time.Since(started).Seconds()}
	r.OutageCapture.EventsPerSecond = float64(backlog) / r.OutageCapture.SettleSeconds
	r.Snapshots = append(r.Snapshots, snapshot{"outage_captured", stats})
	t.Logf("outage: %d events durably waiting; killing daemon", backlog)
	// SIGKILL exercises the real daemon without Close/deferred cleanup or any
	// production failpoint. The receiver is still returning 503 at this boundary.
	daemon.stop(t, true)
	daemon = nil
	afterKill, err := sqlitespool.ReadStats(ctx, cfg.Spool.Path)
	if err != nil {
		t.Fatal(err)
	}
	if afterKill.EventCount != int64(opts.Events) || afterKill.Deliveries.Delivered != int64(baseline) || afterKill.Deliveries.Pending+afterKill.Deliveries.RetryWait != int64(backlog) {
		t.Fatalf("backlog lost on kill: %+v", afterKill)
	}
	assertCheckpoint(t, stats.LastDurableLSN, afterKill.LastDurableLSN)
	r.Snapshots = append(r.Snapshots, snapshot{"after_kill", afterKill})

	started = time.Now()
	daemon = startProcess(t, binary, configPath, logPath)
	// A spool snapshot alone doesn't prove the process restarted. Wait for a
	// fresh 503 request from the new daemon before restoring the receiver.
	beforeAttempts := receiver.failures.Load()
	waitUntil(t, ctx, daemon, receiver, func() bool { return receiver.failures.Load() > beforeAttempts })
	afterRestart, err := sqlitespool.ReadStats(ctx, cfg.Spool.Path)
	if err != nil {
		t.Fatal(err)
	}
	assertCheckpoint(t, afterKill.LastDurableLSN, afterRestart.LastDurableLSN)
	r.Snapshots = append(r.Snapshots, snapshot{"after_restart", afterRestart})
	receiver.dropID.Store(int64(baseline + 1))
	receiver.unavailable.Store(false)
	stats = waitStats(t, ctx, cfg.Spool.Path, daemon, receiver, &r, func(s sqlitespool.Stats) bool { return s.Deliveries.Delivered == int64(opts.Events) })
	r.Recovery = phase{Events: backlog, SettleSeconds: time.Since(started).Seconds(), ReceiverLatency: receiver.latencies(baseline+1, opts.Events)}
	r.Recovery.EventsPerSecond = float64(backlog) / r.Recovery.SettleSeconds
	r.Snapshots = append(r.Snapshots, snapshot{"recovered", stats})
	if stats.EventCount != int64(opts.Events) || stats.Deliveries.Total != int64(opts.Events) || stats.Deliveries.Pending+stats.Deliveries.RetryWait+stats.Deliveries.DeadLetter != 0 {
		t.Fatalf("unexpected final spool state: %+v", stats)
	}
	daemon.stop(t, false)
	daemon = nil
	verifyRecords(t, ctx, admin, table, cfg.Spool.Path, receiver, opts.Events)
	receiver.mu.Lock()
	r.DuplicateAttempts = receiver.duplicates
	r.UniqueReceiverRecords = receiver.accepted
	receiver.mu.Unlock()
	r.UnavailableAttempts = receiver.failures.Load()
	if r.DuplicateAttempts < 1 {
		t.Fatal("lost success response did not trigger a deduplicated retry")
	}
	t.Logf("recovery: %d events delivered in %.2fs; %d duplicate attempt(s) deduplicated", backlog, r.Recovery.SettleSeconds, r.DuplicateAttempts)
}

func integerOption(t *testing.T, name string, fallback, minimum, maximum int) int {
	t.Helper()
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed < minimum || parsed > maximum {
		t.Fatalf("%s must be an integer between %d and %d", name, minimum, maximum)
	}
	return parsed
}

func quoted(value string) string { data, _ := json.Marshal(value); return string(data) }

func produce(t *testing.T, ctx context.Context, conn *pgx.Conn, table string, r *receiver, opts options, first, last int) {
	t.Helper()
	for start := first; start <= last; start += opts.BatchSize {
		end := min(last, start+opts.BatchSize-1)
		r.mu.Lock()
		now := time.Now()
		for id := start; id <= end; id++ {
			r.started[id] = now
		}
		r.mu.Unlock()
		tx, err := conn.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		_, err = tx.Exec(ctx, "INSERT INTO "+table+" SELECT generate_series($1::integer,$2::integer)", start, end)
		if err == nil {
			_, err = tx.Exec(ctx, `SELECT writerelay.emit(jsonb_build_object(
 'specversion','1.0','id',g.id::text,'source',$3::text,'type','load.recorded',
 'data',jsonb_build_object('sequence',g.id,'padding',$4::text)))
 FROM generate_series($1::integer,$2::integer) AS g(id) ORDER BY g.id`, start, end, r.source, r.padding)
		}
		if err != nil {
			_ = tx.Rollback(ctx)
			t.Fatal(err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
	}
}

func waitStats(t *testing.T, ctx context.Context, path string, p *process, receiver *receiver, r *report, ready func(sqlitespool.Stats) bool) sqlitespool.Stats {
	t.Helper()
	var stats sqlitespool.Stats
	var lastError error
	lastLog := time.Now()
	defer func() {
		if t.Failed() {
			if !stats.SampledAt.IsZero() {
				r.Snapshots = append(r.Snapshots, snapshot{"last_observed_before_failure", stats})
			}
			if lastError != nil {
				t.Logf("last spool sample error: %v", lastError)
			}
		}
	}()
	waitUntil(t, ctx, p, receiver, func() bool {
		sampleCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()
		sampled, err := sqlitespool.ReadStats(sampleCtx, path)
		lastError = err
		if err != nil {
			return false
		}
		stats = sampled
		r.SampledPeakSpoolBytes = max(r.SampledPeakSpoolBytes, stats.Storage.TotalBytes)
		if time.Since(lastLog) >= 10*time.Second {
			t.Logf("progress: captured=%d delivered=%d waiting=%d spool_bytes=%d", stats.EventCount, stats.Deliveries.Delivered, stats.Deliveries.Pending+stats.Deliveries.RetryWait, stats.Storage.TotalBytes)
			lastLog = time.Now()
		}
		if stats.Deliveries.DeadLetter != 0 {
			t.Fatalf("unexpected dead letters: %+v", stats.Deliveries)
		}
		return ready(stats)
	})
	return stats
}

func waitUntil(t *testing.T, ctx context.Context, p *process, r *receiver, ready func() bool) {
	t.Helper()
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		r.mu.Lock()
		failure := r.failure
		r.mu.Unlock()
		if failure != "" {
			t.Fatal(failure)
		}
		select {
		case <-p.done:
			t.Fatalf("daemon exited before completion: %v", p.err)
		default:
		}
		if ready() {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("load/recovery deadline exceeded: %v", ctx.Err())
		case <-ticker.C:
		}
	}
}

func assertCheckpoint(t *testing.T, before, after string) {
	t.Helper()
	old, err := pglogrepl.ParseLSN(before)
	if err != nil {
		t.Fatal(err)
	}
	current, err := pglogrepl.ParseLSN(after)
	if err != nil {
		t.Fatal(err)
	}
	if current < old {
		t.Fatalf("checkpoint regressed: %s -> %s", before, after)
	}
}

// Each process is waited exactly once; channel close synchronizes access to err.
type process struct {
	cmd  *exec.Cmd
	done chan struct{}
	err  error
}

func startProcess(t *testing.T, binary, configPath, logPath string) *process {
	t.Helper()
	log, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(binary, "run", "--config", configPath)
	cmd.Stdout = log
	cmd.Stderr = log
	if err := cmd.Start(); err != nil {
		log.Close()
		t.Fatal(err)
	}
	p := &process{cmd: cmd, done: make(chan struct{})}
	go func() { p.err = cmd.Wait(); _ = log.Close(); close(p.done) }()
	return p
}

func (p *process) stop(t *testing.T, kill bool) {
	t.Helper()
	select {
	case <-p.done:
		if kill {
			t.Errorf("daemon exited before the planned kill: %v", p.err)
		} else if p.err != nil {
			t.Errorf("daemon failed: %v", p.err)
		}
		return
	default:
	}
	if kill {
		if err := p.cmd.Process.Kill(); err != nil {
			t.Errorf("kill daemon: %v", err)
		}
	} else {
		_ = p.cmd.Process.Signal(os.Interrupt)
	}
	select {
	case <-p.done:
	case <-time.After(5 * time.Second):
		_ = p.cmd.Process.Kill()
		<-p.done
		t.Error("daemon did not stop within 5 seconds")
	}
	if !kill && p.err != nil {
		t.Errorf("daemon shutdown failed: %v", p.err)
	}
}

// Keep the receiver's transaction and idempotency state on independent storage.
// No receiver state is stored inside WriteRelay's spool.
type receiver struct {
	db                   *sql.DB
	source, padding      string
	mu                   sync.Mutex
	started              []time.Time
	latency              []time.Duration
	keys                 map[int]string
	accepted, duplicates int
	failure              string
	unavailable          atomic.Bool
	failures             atomic.Int64
	dropID               atomic.Int64
}

func newReceiver(t *testing.T, directory, source string, events, padding int) *receiver {
	t.Helper()
	dsn := url.URL{Scheme: "file", Path: filepath.ToSlash(filepath.Join(directory, "receiver.sqlite"))}
	query := url.Values{"_pragma": {"journal_mode(WAL)", "synchronous(FULL)", "busy_timeout(5000)"}}
	dsn.RawQuery = query.Encode()
	db, err := sql.Open("sqlite", dsn.String())
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	_, err = db.Exec(`CREATE TABLE receipts (event_id INTEGER PRIMARY KEY, idempotency_key TEXT NOT NULL UNIQUE, payload_sha256 BLOB NOT NULL, accepted_order INTEGER NOT NULL UNIQUE)`)
	if err != nil {
		db.Close()
		t.Fatal(err)
	}
	return &receiver{db: db, source: source, padding: strings.Repeat("x", padding), started: make([]time.Time, events+1), latency: make([]time.Duration, events+1), keys: make(map[int]string)}
}

func (r *receiver) serve(w http.ResponseWriter, request *http.Request) {
	payload, err := io.ReadAll(io.LimitReader(request.Body, 256*1024+1))
	defer request.Body.Close()
	var envelope struct {
		SpecVersion string `json:"specversion"`
		ID          string `json:"id"`
		Source      string `json:"source"`
		Type        string `json:"type"`
		Data        struct {
			Sequence int    `json:"sequence"`
			Padding  string `json:"padding"`
		} `json:"data"`
	}
	if err == nil {
		err = json.Unmarshal(payload, &envelope)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	fail := func(message string) { r.failure = message; http.Error(w, "invalid load event", 400) }
	id, parseErr := strconv.Atoi(envelope.ID)
	if err != nil || parseErr != nil || id < 1 || id >= len(r.started) || envelope.SpecVersion != "1.0" || envelope.Source != r.source || envelope.Type != "load.recorded" || envelope.Data.Sequence != id || envelope.Data.Padding != r.padding || r.started[id].IsZero() {
		fail("receiver got an unexpected identity or payload")
		return
	}
	key := request.Header.Get("Idempotency-Key")
	if key == "" || (r.keys[id] != "" && r.keys[id] != key) {
		fail("idempotency key changed between attempts")
		return
	}
	r.keys[id] = key
	if r.unavailable.Load() {
		r.failures.Add(1)
		http.Error(w, "load-test outage", 503)
		return
	}
	hash := sha256.Sum256(payload)
	var storedKey string
	var storedHash []byte
	err = r.db.QueryRowContext(request.Context(), "SELECT idempotency_key,payload_sha256 FROM receipts WHERE event_id=?", id).Scan(&storedKey, &storedHash)
	if err == nil {
		if storedKey != key || string(storedHash) != string(hash[:]) {
			fail("duplicate identity changed key or payload")
			return
		}
		r.duplicates++
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if err != sql.ErrNoRows {
		fail("receiver inbox read failed: " + err.Error())
		return
	}
	if id != r.accepted+1 {
		fail(fmt.Sprintf("first-acceptance order changed: got %d, want %d", id, r.accepted+1))
		return
	}
	// One committed row represents both the test business effect and its inbox key.
	if _, err = r.db.ExecContext(request.Context(), "INSERT INTO receipts VALUES (?,?,?,?)", id, key, hash[:], r.accepted+1); err != nil {
		fail("receiver commit failed: " + err.Error())
		return
	}
	r.accepted++
	r.latency[id] = time.Since(r.started[id])
	if r.dropID.CompareAndSwap(int64(id), 0) {
		connection, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			fail("failed to simulate a lost response")
			return
		}
		_ = connection.Close()
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (r *receiver) latencies(first, last int) *latency {
	r.mu.Lock()
	defer r.mu.Unlock()
	values := append([]time.Duration(nil), r.latency[first:last+1]...)
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	percentile := func(p int) float64 {
		index := (len(values)*p+99)/100 - 1
		return float64(values[max(0, index)]) / float64(time.Millisecond)
	}
	return &latency{P50: percentile(50), P95: percentile(95), P99: percentile(99), Max: float64(values[len(values)-1]) / float64(time.Millisecond)}
}

func verifyRecords(t *testing.T, ctx context.Context, admin *pgx.Conn, table, path string, r *receiver, count int) {
	t.Helper()
	rows, err := admin.Query(ctx, "SELECT id FROM "+table+" ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	seen := 0
	for rows.Next() {
		var id int
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		seen++
		if id != seen {
			t.Fatalf("producer identity gap: %d != %d", id, seen)
		}
	}
	rows.Close()
	if rows.Err() != nil || seen != count {
		t.Fatalf("producer rows: %d, %v", seen, rows.Err())
	}
	dsn := url.URL{Scheme: "file", Path: filepath.ToSlash(path), RawQuery: "mode=ro"}
	db, err := sql.Open("sqlite", dsn.String())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	spoolRows, err := db.QueryContext(ctx, `SELECT e.event_id,e.event_source,e.payload,e.payload_sha256,d.state FROM events e JOIN deliveries d ON e.sequence=d.event_sequence ORDER BY e.sequence`)
	if err != nil {
		t.Fatal(err)
	}
	defer spoolRows.Close()
	seen = 0
	for spoolRows.Next() {
		var id, source, state string
		var payload, hash []byte
		if err := spoolRows.Scan(&id, &source, &payload, &hash, &state); err != nil {
			t.Fatal(err)
		}
		seen++
		digest := sha256.Sum256(payload)
		if id != strconv.Itoa(seen) || source != r.source || state != "delivered" || string(hash) != string(digest[:]) {
			t.Fatalf("spool identity/order/payload/state mismatch at %d", seen)
		}
		var receivedID int
		var receivedHash []byte
		if err := r.db.QueryRowContext(ctx, "SELECT event_id,payload_sha256 FROM receipts WHERE accepted_order=?", seen).Scan(&receivedID, &receivedHash); err != nil {
			t.Fatal(err)
		}
		if receivedID != seen || string(receivedHash) != string(hash) {
			t.Fatalf("receiver identity/order/payload mismatch at %d", seen)
		}
	}
	if spoolRows.Err() != nil || seen != count {
		t.Fatalf("spool rows: %d, %v", seen, spoolRows.Err())
	}
	var receiverCount int
	if err := r.db.QueryRowContext(ctx, "SELECT count(*) FROM receipts").Scan(&receiverCount); err != nil || receiverCount != count {
		t.Fatalf("receiver rows: %d, %v", receiverCount, err)
	}
}
