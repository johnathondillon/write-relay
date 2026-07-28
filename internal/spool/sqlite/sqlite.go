package sqlite

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/johnathondillon/write-relay/internal/spool"
	_ "modernc.org/sqlite"
)

//go:embed migrations/*.sql
var migrations embed.FS

const (
	currentSchemaVersion = 1
	durableLSNKey        = "last_durable_lsn"
)

type Store struct {
	db   *sql.DB
	path string
}

type EventRow struct {
	Sequence      int64
	Source        string
	ID            string
	Type          string
	Subject       string
	Payload       []byte
	TransactionID uint32
	MessageLSN    string
	CommitLSN     string
	CommitEndLSN  string
	CommitTime    time.Time
	MessageIndex  int
}

func Open(ctx context.Context, path string) (*Store, error) {
	if path == "" {
		return nil, errors.New("spool path is required")
	}
	parent := filepath.Dir(path)
	if err := os.MkdirAll(parent, 0o700); err != nil {
		return nil, fmt.Errorf("create spool directory: %w", err)
	}
	if info, err := os.Lstat(path); err == nil && info.Mode()&os.ModeSymlink != 0 {
		return nil, errors.New("spool path must not be a symbolic link")
	} else if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return nil, fmt.Errorf("inspect spool path: %w", err)
	}

	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open SQLite spool: %w", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	store := &Store{db: db, path: path}
	if err := store.initialize(ctx); err != nil {
		db.Close()
		return nil, err
	}
	if err := os.Chmod(path, 0o600); err != nil {
		db.Close()
		return nil, fmt.Errorf("restrict spool permissions: %w", err)
	}
	return store, nil
}

func (s *Store) initialize(ctx context.Context) error {
	pragmas := []string{
		"PRAGMA journal_mode = WAL",
		"PRAGMA synchronous = FULL",
		"PRAGMA foreign_keys = ON",
		"PRAGMA busy_timeout = 5000",
	}
	for _, statement := range pragmas {
		if _, err := s.db.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("%w: apply %q: %v", spool.ErrDurability, statement, err)
		}
	}

	var version int
	err := s.db.QueryRowContext(ctx, `SELECT COALESCE(MAX(version), 0) FROM schema_migrations`).Scan(&version)
	if err != nil {
		if !stringsContainsNoTable(err) {
			return fmt.Errorf("read spool schema version: %w", err)
		}
		migration, readErr := migrations.ReadFile("migrations/001_initial.sql")
		if readErr != nil {
			return fmt.Errorf("read embedded migration: %w", readErr)
		}
		tx, beginErr := s.db.BeginTx(ctx, nil)
		if beginErr != nil {
			return fmt.Errorf("%w: begin initial migration: %v", spool.ErrDurability, beginErr)
		}
		if _, execErr := tx.ExecContext(ctx, string(migration)); execErr != nil {
			_ = tx.Rollback()
			return fmt.Errorf("%w: apply initial migration: %v", spool.ErrDurability, execErr)
		}
		if commitErr := tx.Commit(); commitErr != nil {
			return fmt.Errorf("%w: commit initial migration: %v", spool.ErrDurability, commitErr)
		}
		version = 1
	}
	if version != currentSchemaVersion {
		return fmt.Errorf("unsupported spool schema version %d (expected %d)", version, currentSchemaVersion)
	}
	return nil
}

func stringsContainsNoTable(err error) bool {
	return err != nil && (contains(err.Error(), "no such table") || contains(err.Error(), "does not exist"))
}

func contains(value, fragment string) bool {
	for i := 0; i+len(fragment) <= len(value); i++ {
		if value[i:i+len(fragment)] == fragment {
			return true
		}
	}
	return false
}

func (s *Store) PersistCommittedBatch(ctx context.Context, batch spool.CommittedBatch) (result spool.PersistResult, err error) {
	if batch.CommitEndLSN < batch.CommitLSN {
		return result, fmt.Errorf("%w: commit end LSN %s precedes commit LSN %s", spool.ErrDurability, batch.CommitEndLSN, batch.CommitLSN)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return result, fmt.Errorf("%w: begin transaction: %v", spool.ErrDurability, err)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	for index, captured := range batch.Events {
		if err = validateCapturedEvent(batch, captured, index); err != nil {
			return result, err
		}
		inserted, insertErr := insertOrVerify(ctx, tx, captured)
		if insertErr != nil {
			err = insertErr
			return result, err
		}
		if inserted {
			result.Inserted++
		} else {
			result.Replayed++
		}
	}

	current, loadErr := lastDurableLSNTx(ctx, tx)
	if loadErr != nil {
		err = fmt.Errorf("%w: read durable checkpoint: %v", spool.ErrDurability, loadErr)
		return result, err
	}
	result.DurableLSN = current
	if batch.CommitEndLSN > current {
		_, execErr := tx.ExecContext(ctx, `
			INSERT INTO spool_metadata(key, value) VALUES (?, ?)
			ON CONFLICT(key) DO UPDATE SET value = excluded.value
		`, durableLSNKey, batch.CommitEndLSN.String())
		if execErr != nil {
			err = fmt.Errorf("%w: update durable checkpoint: %v", spool.ErrDurability, execErr)
			return result, err
		}
		result.DurableLSN = batch.CommitEndLSN
	}
	if commitErr := tx.Commit(); commitErr != nil {
		err = fmt.Errorf("%w: commit batch: %v", spool.ErrDurability, commitErr)
		return result, err
	}
	return result, nil
}

func validateCapturedEvent(batch spool.CommittedBatch, captured spool.CapturedEvent, index int) error {
	if captured.Source == "" || captured.ID == "" || captured.Type == "" {
		return fmt.Errorf("%w: event %d has empty identity or type", spool.ErrDurability, index)
	}
	if captured.TransactionID != batch.TransactionID ||
		captured.CommitLSN != batch.CommitLSN ||
		captured.CommitEndLSN != batch.CommitEndLSN ||
		!captured.CommitTime.Equal(batch.CommitTime) ||
		captured.MessageIndex != index {
		return fmt.Errorf("%w: event %d metadata does not match committed batch", spool.ErrDurability, index)
	}
	computed := sha256.Sum256(captured.Payload)
	if subtle.ConstantTimeCompare(computed[:], captured.PayloadSHA256[:]) != 1 {
		return fmt.Errorf("%w: event %d payload digest is invalid", spool.ErrDurability, index)
	}
	return nil
}

func insertOrVerify(ctx context.Context, tx *sql.Tx, captured spool.CapturedEvent) (bool, error) {
	result, err := tx.ExecContext(ctx, `
		INSERT INTO events(
			event_source, event_id, event_type, subject, payload, payload_sha256,
			transaction_id, message_lsn, commit_lsn, commit_end_lsn, commit_time,
			message_index, captured_at
		) VALUES (?, ?, ?, NULLIF(?, ''), ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(event_source, event_id) DO NOTHING
	`,
		captured.Source, captured.ID, captured.Type, captured.Subject, captured.Payload,
		captured.PayloadSHA256[:], captured.TransactionID, captured.MessageLSN.String(),
		captured.CommitLSN.String(), captured.CommitEndLSN.String(),
		captured.CommitTime.UTC().Format(time.RFC3339Nano), captured.MessageIndex,
		time.Now().UTC().Format(time.RFC3339Nano),
	)
	if err != nil {
		return false, fmt.Errorf("%w: insert event %s/%s: %v", spool.ErrDurability, captured.Source, captured.ID, err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("%w: inspect insert result: %v", spool.ErrDurability, err)
	}
	if affected == 1 {
		return true, nil
	}

	var digest []byte
	if err := tx.QueryRowContext(ctx, `
		SELECT payload_sha256 FROM events WHERE event_source = ? AND event_id = ?
	`, captured.Source, captured.ID).Scan(&digest); err != nil {
		return false, fmt.Errorf("%w: load replay digest: %v", spool.ErrDurability, err)
	}
	if len(digest) != len(captured.PayloadSHA256) ||
		subtle.ConstantTimeCompare(digest, captured.PayloadSHA256[:]) != 1 {
		return false, fmt.Errorf("%w: source=%q id=%q", spool.ErrIdentityConflict, captured.Source, captured.ID)
	}
	return false, nil
}

func (s *Store) LastDurableLSN(ctx context.Context) (pglogrepl.LSN, error) {
	var value string
	err := s.db.QueryRowContext(ctx, `SELECT value FROM spool_metadata WHERE key = ?`, durableLSNKey).Scan(&value)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("read durable LSN: %w", err)
	}
	lsn, err := pglogrepl.ParseLSN(value)
	if err != nil {
		return 0, fmt.Errorf("parse durable LSN %q: %w", value, err)
	}
	return lsn, nil
}

func lastDurableLSNTx(ctx context.Context, tx *sql.Tx) (pglogrepl.LSN, error) {
	var value string
	err := tx.QueryRowContext(ctx, `SELECT value FROM spool_metadata WHERE key = ?`, durableLSNKey).Scan(&value)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return pglogrepl.ParseLSN(value)
}

func (s *Store) ListEvents(ctx context.Context, limit int) ([]EventRow, error) {
	if limit < 1 || limit > 1000 {
		return nil, errors.New("limit must be between 1 and 1000")
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT sequence, event_source, event_id, event_type, COALESCE(subject, ''), payload,
		       transaction_id, message_lsn, commit_lsn, commit_end_lsn, commit_time, message_index
		FROM events ORDER BY sequence LIMIT ?
	`, limit)
	if err != nil {
		return nil, fmt.Errorf("list spool events: %w", err)
	}
	defer rows.Close()

	var result []EventRow
	for rows.Next() {
		var row EventRow
		var transactionID int64
		var commitTime string
		if err := rows.Scan(
			&row.Sequence, &row.Source, &row.ID, &row.Type, &row.Subject, &row.Payload,
			&transactionID, &row.MessageLSN, &row.CommitLSN, &row.CommitEndLSN,
			&commitTime, &row.MessageIndex,
		); err != nil {
			return nil, fmt.Errorf("scan spool event: %w", err)
		}
		row.TransactionID = uint32(transactionID)
		row.CommitTime, err = time.Parse(time.RFC3339Nano, commitTime)
		if err != nil {
			return nil, fmt.Errorf("parse event commit time: %w", err)
		}
		result = append(result, row)
	}
	return result, rows.Err()
}

func (s *Store) SchemaVersion(ctx context.Context) (int, error) {
	var version int
	err := s.db.QueryRowContext(ctx, `SELECT MAX(version) FROM schema_migrations`).Scan(&version)
	return version, err
}

func (s *Store) EventCount(ctx context.Context) (int, error) {
	var count int
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM events`).Scan(&count)
	return count, err
}

func (s *Store) Close() error {
	return s.db.Close()
}

func ParseSequence(value string) (int64, error) {
	return strconv.ParseInt(value, 10, 64)
}
