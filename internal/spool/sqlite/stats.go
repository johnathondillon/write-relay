package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"time"
)

// DeliveryCounts counts delivery records, not distinct events. One event can
// have a delivery for each sink. RetryWait includes retries that are already due.
type DeliveryCounts struct {
	Total      int64 `json:"total"`
	Pending    int64 `json:"pending"`
	RetryWait  int64 `json:"retry_wait"`
	Delivered  int64 `json:"delivered"`
	DeadLetter int64 `json:"dead_letter"`
}

type WaitingStats struct {
	CapturedAt time.Time `json:"captured_at"`
	AgeSeconds int64     `json:"age_seconds"`
}

type SinkStats struct {
	Name          string         `json:"sink"`
	Type          string         `json:"type"`
	Active        bool           `json:"active"`
	Deliveries    DeliveryCounts `json:"deliveries"`
	OldestWaiting *WaitingStats  `json:"oldest_waiting"`
}

// FileSizes reports file lengths, not allocated disk blocks or PostgreSQL WAL.
// Files are sampled separately and can change while a daemon is running.
type FileSizes struct {
	DatabaseBytes int64 `json:"database_bytes"`
	WALBytes      int64 `json:"wal_bytes"`
	SHMBytes      int64 `json:"shm_bytes"`
	TotalBytes    int64 `json:"total_bytes"`
}

type Stats struct {
	SampledAt      time.Time      `json:"sampled_at"`
	EventCount     int64          `json:"event_count"`
	PrunedPayloads int64          `json:"pruned_payloads"`
	LastDurableLSN string         `json:"last_durable_lsn"`
	Deliveries     DeliveryCounts `json:"deliveries"`
	OldestWaiting  *WaitingStats  `json:"oldest_waiting"`
	Storage        FileSizes      `json:"storage"`
	Sinks          []SinkStats    `json:"sinks"`
}

// ReadStats opens an existing spool in SQLite read-only mode. Unlike Open, it
// does not create a database, apply migrations, change permissions, or configure
// sinks. SQLite may still maintain the WAL shared-memory sidecar for its reader.
func ReadStats(ctx context.Context, path string) (Stats, error) {
	var result Stats
	if path == "" {
		return result, errors.New("spool path is required")
	}
	info, err := os.Lstat(path)
	if err != nil {
		return result, fmt.Errorf("inspect existing spool (stats requires an initialized spool): %w", err)
	}
	if !info.Mode().IsRegular() {
		return result, errors.New("spool path must be a regular file, not a symbolic link or directory")
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return result, fmt.Errorf("resolve spool path: %w", err)
	}
	dsn := url.URL{Scheme: "file", Path: filepath.ToSlash(abs)}
	query := url.Values{"mode": {"ro"}, "_pragma": {"busy_timeout(5000)"}}
	dsn.RawQuery = query.Encode()
	db, err := sql.Open("sqlite", dsn.String())
	if err != nil {
		return result, fmt.Errorf("open spool for stats: %w", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	result, err = readStatsSnapshot(ctx, db)
	if err != nil {
		return result, err
	}
	result.Storage, err = spoolFileSizes(abs)
	return result, err
}

func readStatsSnapshot(ctx context.Context, db *sql.DB) (Stats, error) {
	result := Stats{Sinks: make([]SinkStats, 0)}
	tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return result, fmt.Errorf("begin spool stats snapshot: %w", err)
	}
	defer tx.Rollback()
	var version int
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(version), 0) FROM schema_migrations`).Scan(&version); err != nil {
		return result, fmt.Errorf("read spool stats schema: %w", err)
	}
	// The first SELECT establishes the snapshot. Older schemas are intentionally
	// not upgraded by an inspection command.
	result.SampledAt = time.Now().UTC()
	if version != currentSchemaVersion {
		return result, fmt.Errorf("spool stats requires schema version %d, found %d; stats does not migrate the spool", currentSchemaVersion, version)
	}
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*), COALESCE(SUM(payload_pruned_at IS NOT NULL), 0) FROM events`).Scan(&result.EventCount, &result.PrunedPayloads); err != nil {
		return result, fmt.Errorf("count captured events: %w", err)
	}
	lsn, err := lastDurableLSNTx(ctx, tx)
	if err != nil {
		return result, fmt.Errorf("read spool stats checkpoint: %w", err)
	}
	result.LastDurableLSN = lsn.String()

	rows, err := tx.QueryContext(ctx, `
		SELECT s.sink_name, s.sink_type, s.active,
		       COUNT(d.event_sequence),
		       COALESCE(SUM(d.state = 'pending'), 0),
		       COALESCE(SUM(d.state = 'retry_wait'), 0),
		       COALESCE(SUM(d.state = 'delivered'), 0),
		       COALESCE(SUM(d.state = 'dead_letter'), 0),
		       MIN(CASE WHEN d.state IN ('pending', 'retry_wait') THEN e.captured_at END)
		FROM delivery_sinks s
		LEFT JOIN deliveries d ON d.sink_id = s.sink_id
		LEFT JOIN events e ON e.sequence = d.event_sequence
		GROUP BY s.sink_id
		ORDER BY s.sink_name
	`)
	if err != nil {
		return result, fmt.Errorf("read per-sink stats: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var sink SinkStats
		var oldest sql.NullString
		if err := rows.Scan(&sink.Name, &sink.Type, &sink.Active,
			&sink.Deliveries.Total, &sink.Deliveries.Pending, &sink.Deliveries.RetryWait,
			&sink.Deliveries.Delivered, &sink.Deliveries.DeadLetter, &oldest); err != nil {
			return result, fmt.Errorf("scan per-sink stats: %w", err)
		}
		capturedAt, err := nullableTime(oldest)
		if err != nil {
			return result, fmt.Errorf("parse oldest waiting capture time: %w", err)
		}
		if capturedAt != nil {
			sink.OldestWaiting = &WaitingStats{
				CapturedAt: *capturedAt,
				AgeSeconds: max(0, int64(result.SampledAt.Sub(*capturedAt)/time.Second)),
			}
			if result.OldestWaiting == nil || capturedAt.Before(result.OldestWaiting.CapturedAt) {
				result.OldestWaiting = sink.OldestWaiting
			}
		}
		result.Deliveries.Total += sink.Deliveries.Total
		result.Deliveries.Pending += sink.Deliveries.Pending
		result.Deliveries.RetryWait += sink.Deliveries.RetryWait
		result.Deliveries.Delivered += sink.Deliveries.Delivered
		result.Deliveries.DeadLetter += sink.Deliveries.DeadLetter
		result.Sinks = append(result.Sinks, sink)
	}
	if err := rows.Err(); err != nil {
		return result, fmt.Errorf("iterate per-sink stats: %w", err)
	}
	if err := rows.Close(); err != nil {
		return result, fmt.Errorf("close per-sink stats: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return result, fmt.Errorf("finish spool stats snapshot: %w", err)
	}
	return result, nil
}

func spoolFileSizes(path string) (FileSizes, error) {
	var result FileSizes
	for _, file := range []struct {
		suffix string
		size   *int64
	}{{"", &result.DatabaseBytes}, {"-wal", &result.WALBytes}, {"-shm", &result.SHMBytes}} {
		info, err := os.Stat(path + file.suffix)
		if file.suffix != "" && errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return result, fmt.Errorf("measure spool file: %w", err)
		}
		if !info.Mode().IsRegular() {
			return result, fmt.Errorf("spool file %q is not a regular file", path+file.suffix)
		}
		*file.size = info.Size()
		result.TotalBytes += info.Size()
	}
	return result, nil
}
