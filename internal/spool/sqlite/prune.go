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

	"github.com/johnathondillon/write-relay/internal/failure"
)

type PruneOptions struct {
	Before time.Time
	Limit  int
	DryRun bool
}

type PruneEntry struct {
	Sequence     int64  `json:"sequence"`
	Source       string `json:"source"`
	ID           string `json:"id"`
	PayloadBytes int64  `json:"payload_bytes"`
}

// PayloadBytes is logical payload size, not a promise of filesystem space freed.
type PruneResult struct {
	DryRun       bool         `json:"dry_run"`
	Before       time.Time    `json:"before"`
	Limit        int          `json:"limit"`
	Events       int          `json:"events"`
	PayloadBytes int64        `json:"payload_bytes"`
	Entries      []PruneEntry `json:"entries"`
}

// Prune removes payload bytes only, retaining identity, digest, and delivery
// history. It requires an existing current-schema spool and never migrates it.
// DryRun uses mode=ro. Application acquires the SQLite write lock before selecting
// candidates so sink registration or delivery state changes cannot race selection.
func Prune(ctx context.Context, path string, options PruneOptions) (PruneResult, error) {
	return pruneWithHooks(ctx, path, options, failure.Hooks{})
}

func pruneWithHooks(ctx context.Context, path string, options PruneOptions, hooks failure.Hooks) (PruneResult, error) {
	result := PruneResult{DryRun: options.DryRun, Before: options.Before.UTC(), Limit: options.Limit, Entries: []PruneEntry{}}
	now := time.Now().UTC()
	if options.Before.IsZero() || options.Before.After(now) {
		return result, errors.New("prune cutoff must be a non-zero timestamp no later than now")
	}
	if options.Limit < 1 || options.Limit > 1000 {
		return result, errors.New("prune limit must be between 1 and 1000")
	}
	if path == "" {
		return result, errors.New("spool path is required")
	}
	info, err := os.Lstat(path)
	if err != nil {
		return result, fmt.Errorf("inspect existing spool for prune: %w", err)
	}
	if !info.Mode().IsRegular() {
		return result, errors.New("spool path must be a regular file, not a symbolic link or directory")
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return result, err
	}
	mode, begin := "rw", "BEGIN IMMEDIATE"
	if options.DryRun {
		mode, begin = "ro", "BEGIN"
	}
	dsn := url.URL{Scheme: "file", Path: filepath.ToSlash(abs)}
	dsn.RawQuery = url.Values{"mode": {mode}, "_pragma": {"busy_timeout(5000)", "foreign_keys(ON)", "synchronous(FULL)"}}.Encode()
	db, err := sql.Open("sqlite", dsn.String())
	if err != nil {
		return result, fmt.Errorf("open spool for prune: %w", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	conn, err := db.Conn(ctx)
	if err != nil {
		return result, err
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, begin); err != nil {
		return result, fmt.Errorf("begin prune transaction: %w", err)
	}
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = conn.ExecContext(cleanupCtx, "ROLLBACK")
	}()
	var version int
	if err := conn.QueryRowContext(ctx, `SELECT COALESCE(MAX(version), 0) FROM schema_migrations`).Scan(&version); err != nil {
		return result, fmt.Errorf("read prune schema: %w", err)
	}
	if version != currentSchemaVersion {
		return result, fmt.Errorf("prune requires schema version %d, found %d; restart with the current daemon to migrate first", currentSchemaVersion, version)
	}
	var journal string
	if err := conn.QueryRowContext(ctx, "PRAGMA journal_mode").Scan(&journal); err != nil || journal != "wal" {
		return result, fmt.Errorf("prune requires a WAL spool (mode=%q, error=%v)", journal, err)
	}
	rows, err := conn.QueryContext(ctx, `
		SELECT e.sequence, e.event_source, e.event_id, length(e.payload)
		FROM events e
		WHERE e.payload_pruned_at IS NULL
		  AND EXISTS (SELECT 1 FROM deliveries d WHERE d.event_sequence = e.sequence)
		  AND NOT EXISTS (
		      SELECT 1 FROM deliveries d WHERE d.event_sequence = e.sequence
		      AND (d.state <> 'delivered' OR d.delivered_at IS NULL OR d.delivered_at >= ?)
		  )
		ORDER BY e.sequence LIMIT ?
	`, formatTimestamp(options.Before), options.Limit)
	if err != nil {
		return result, fmt.Errorf("select prune candidates: %w", err)
	}
	for rows.Next() {
		var entry PruneEntry
		if err := rows.Scan(&entry.Sequence, &entry.Source, &entry.ID, &entry.PayloadBytes); err != nil {
			rows.Close()
			return result, err
		}
		result.Entries = append(result.Entries, entry)
		result.PayloadBytes += entry.PayloadBytes
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return result, err
	}
	if err := rows.Close(); err != nil {
		return result, err
	}
	result.Events = len(result.Entries)
	if !options.DryRun {
		for index, entry := range result.Entries {
			changed, err := conn.ExecContext(ctx,
				`UPDATE events SET payload = X'', payload_pruned_at = ? WHERE sequence = ? AND payload_pruned_at IS NULL`,
				formatTimestamp(now), entry.Sequence)
			if err != nil {
				return result, fmt.Errorf("prune payload: %w", err)
			}
			count, err := changed.RowsAffected()
			if err != nil || count != 1 {
				return result, fmt.Errorf("prune changed %d rows, expected one (error=%v)", count, err)
			}
			hooks.CallAfterPrunePayload(index)
		}
	}
	if _, err := conn.ExecContext(ctx, "COMMIT"); err != nil {
		return result, fmt.Errorf("commit prune transaction: %w", err)
	}
	if !options.DryRun {
		hooks.CallAfterPruneCommit()
	}
	return result, nil
}
