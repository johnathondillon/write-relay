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
	"strings"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/johnathondillon/write-relay/internal/delivery"
	"github.com/johnathondillon/write-relay/internal/failure"
	"github.com/johnathondillon/write-relay/internal/spool"
	_ "modernc.org/sqlite"
)

//go:embed migrations/*.sql
var migrations embed.FS

const (
	currentSchemaVersion = 2
	durableLSNKey        = "last_durable_lsn"
	timestampFormat      = "2006-01-02T15:04:05.000000000Z07:00"
)

type Store struct {
	db    *sql.DB
	path  string
	hooks failure.Hooks
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

type DeliveryRow struct {
	EventSequence  int64      `json:"event_sequence"`
	SinkName       string     `json:"sink"`
	State          string     `json:"state"`
	Attempts       int        `json:"attempts"`
	NextAttemptAt  time.Time  `json:"next_attempt_at"`
	LastAttemptAt  *time.Time `json:"last_attempt_at,omitempty"`
	DeliveredAt    *time.Time `json:"delivered_at,omitempty"`
	DeadLetteredAt *time.Time `json:"dead_lettered_at,omitempty"`
	LastError      string     `json:"last_error,omitempty"`
	ResponseStatus int        `json:"response_status,omitempty"`
	Source         string     `json:"source"`
	ID             string     `json:"id"`
	Type           string     `json:"type"`
}

func Open(ctx context.Context, path string) (*Store, error) {
	return OpenWithHooks(ctx, path, failure.Hooks{})
}

// OpenWithHooks exists for deterministic process-crash tests. Production
// composition calls Open, which always uses inert hooks.
func OpenWithHooks(ctx context.Context, path string, hooks failure.Hooks) (*Store, error) {
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
	store := &Store{db: db, path: path, hooks: hooks}
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
		version = 0
	}
	if version > currentSchemaVersion {
		return fmt.Errorf("unsupported spool schema version %d (maximum %d)", version, currentSchemaVersion)
	}
	for next := version + 1; next <= currentSchemaVersion; next++ {
		if err := s.applyMigration(ctx, next); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) applyMigration(ctx context.Context, version int) error {
	prefix := fmt.Sprintf("%03d_", version)
	entries, err := fs.ReadDir(migrations, "migrations")
	if err != nil {
		return fmt.Errorf("read embedded migrations: %w", err)
	}
	var path string
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), prefix) {
			path = "migrations/" + entry.Name()
			break
		}
	}
	if path == "" {
		return fmt.Errorf("embedded migration %d is missing", version)
	}
	migration, err := migrations.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read embedded migration %d: %w", version, err)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("%w: begin migration %d: %v", spool.ErrDurability, version, err)
	}
	if _, err := tx.ExecContext(ctx, string(migration)); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("%w: apply migration %d: %v", spool.ErrDurability, version, err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("%w: commit migration %d: %v", spool.ErrDurability, version, err)
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
	s.hooks.CallBeforeSpoolTransaction()
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
		inserted, sequence, insertErr := insertOrVerify(ctx, tx, captured)
		if insertErr != nil {
			err = insertErr
			return result, err
		}
		if deliveryErr := ensureDeliveriesForEvent(ctx, tx, sequence); deliveryErr != nil {
			err = deliveryErr
			return result, err
		}
		s.hooks.CallAfterSpoolEvent(index)
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
	s.hooks.CallAfterSpoolCommit()
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

func insertOrVerify(ctx context.Context, tx *sql.Tx, captured spool.CapturedEvent) (bool, int64, error) {
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
		return false, 0, fmt.Errorf("%w: insert event %s/%s: %v", spool.ErrDurability, captured.Source, captured.ID, err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return false, 0, fmt.Errorf("%w: inspect insert result: %v", spool.ErrDurability, err)
	}
	if affected == 1 {
		sequence, err := result.LastInsertId()
		if err != nil {
			return false, 0, fmt.Errorf("%w: inspect inserted event sequence: %v", spool.ErrDurability, err)
		}
		return true, sequence, nil
	}

	var digest []byte
	var sequence int64
	if err := tx.QueryRowContext(ctx, `
		SELECT sequence, payload_sha256 FROM events WHERE event_source = ? AND event_id = ?
	`, captured.Source, captured.ID).Scan(&sequence, &digest); err != nil {
		return false, 0, fmt.Errorf("%w: load replay digest: %v", spool.ErrDurability, err)
	}
	if len(digest) != len(captured.PayloadSHA256) ||
		subtle.ConstantTimeCompare(digest, captured.PayloadSHA256[:]) != 1 {
		return false, 0, fmt.Errorf("%w: source=%q id=%q", spool.ErrIdentityConflict, captured.Source, captured.ID)
	}
	return false, sequence, nil
}

func ensureDeliveriesForEvent(ctx context.Context, tx *sql.Tx, sequence int64) error {
	_, err := tx.ExecContext(ctx, `
		INSERT INTO deliveries(event_sequence, sink_id, state, attempts, next_attempt_at)
		SELECT ?, sink_id, 'pending', 0, ?
		FROM delivery_sinks
		WHERE active = 1
		ON CONFLICT(event_sequence, sink_id) DO NOTHING
	`, sequence, formatTimestamp(time.Now()))
	if err != nil {
		return fmt.Errorf("%w: create delivery records for event %d: %v", spool.ErrDurability, sequence, err)
	}
	return nil
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

func (s *Store) ConfigureSinks(ctx context.Context, registrations []delivery.SinkRegistration) (err error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("%w: begin sink configuration: %v", spool.ErrDurability, err)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	configured := make(map[string]struct{}, len(registrations))
	now := formatTimestamp(time.Now())
	for _, registration := range registrations {
		if registration.Name == "" || registration.Type == "" {
			return fmt.Errorf("%w: sink name and type are required", spool.ErrDurability)
		}
		if _, exists := configured[registration.Name]; exists {
			return fmt.Errorf("%w: duplicate sink %q", spool.ErrDurability, registration.Name)
		}
		configured[registration.Name] = struct{}{}

		var sinkID int64
		var sinkType string
		var digest []byte
		queryErr := tx.QueryRowContext(ctx, `
			SELECT sink_id, sink_type, config_sha256
			FROM delivery_sinks WHERE sink_name = ?
		`, registration.Name).Scan(&sinkID, &sinkType, &digest)
		switch {
		case errors.Is(queryErr, sql.ErrNoRows):
			result, insertErr := tx.ExecContext(ctx, `
				INSERT INTO delivery_sinks(
					sink_name, sink_type, config_sha256, active, created_at
				) VALUES (?, ?, ?, 1, ?)
			`, registration.Name, registration.Type, registration.ConfigSHA256[:], now)
			if insertErr != nil {
				return fmt.Errorf("%w: register sink %q: %v", spool.ErrDurability, registration.Name, insertErr)
			}
			sinkID, insertErr = result.LastInsertId()
			if insertErr != nil {
				return fmt.Errorf("%w: inspect registered sink %q: %v", spool.ErrDurability, registration.Name, insertErr)
			}
		case queryErr != nil:
			return fmt.Errorf("%w: inspect sink %q: %v", spool.ErrDurability, registration.Name, queryErr)
		case sinkType != registration.Type ||
			len(digest) != len(registration.ConfigSHA256) ||
			subtle.ConstantTimeCompare(digest, registration.ConfigSHA256[:]) != 1:
			return fmt.Errorf("%w: sink=%q; use a new sink name when changing its type or target",
				delivery.ErrSinkConfigurationConflict, registration.Name)
		default:
			if _, err := tx.ExecContext(ctx, `
				UPDATE delivery_sinks SET active = 1 WHERE sink_id = ?
			`, sinkID); err != nil {
				return fmt.Errorf("%w: activate sink %q: %v", spool.ErrDurability, registration.Name, err)
			}
		}

		if _, err := tx.ExecContext(ctx, `
			INSERT INTO deliveries(event_sequence, sink_id, state, attempts, next_attempt_at)
			SELECT sequence, ?, 'pending', 0, ? FROM events WHERE 1
			ON CONFLICT(event_sequence, sink_id) DO NOTHING
		`, sinkID, now); err != nil {
			return fmt.Errorf("%w: backfill sink %q: %v", spool.ErrDurability, registration.Name, err)
		}
	}

	rows, err := tx.QueryContext(ctx, `
		SELECT sink_id, sink_name FROM delivery_sinks WHERE active = 1
	`)
	if err != nil {
		return fmt.Errorf("%w: list active sinks: %v", spool.ErrDurability, err)
	}
	var deactivate []struct {
		id   int64
		name string
	}
	for rows.Next() {
		var item struct {
			id   int64
			name string
		}
		if err := rows.Scan(&item.id, &item.name); err != nil {
			rows.Close()
			return fmt.Errorf("%w: scan active sink: %v", spool.ErrDurability, err)
		}
		if _, exists := configured[item.name]; !exists {
			deactivate = append(deactivate, item)
		}
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("%w: close active sink rows: %v", spool.ErrDurability, err)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("%w: list active sinks: %v", spool.ErrDurability, err)
	}
	for _, item := range deactivate {
		var pending int
		if err := tx.QueryRowContext(ctx, `
			SELECT COUNT(*) FROM deliveries
			WHERE sink_id = ? AND state IN ('pending', 'retry_wait')
		`, item.id).Scan(&pending); err != nil {
			return fmt.Errorf("%w: inspect sink %q deliveries: %v", spool.ErrDurability, item.name, err)
		}
		if pending != 0 {
			return fmt.Errorf(
				"cannot remove active sink %q with %d non-terminal deliveries",
				item.name, pending,
			)
		}
		if _, err := tx.ExecContext(ctx, `
			UPDATE delivery_sinks SET active = 0 WHERE sink_id = ?
		`, item.id); err != nil {
			return fmt.Errorf("%w: deactivate sink %q: %v", spool.ErrDurability, item.name, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("%w: commit sink configuration: %v", spool.ErrDurability, err)
	}
	return nil
}

func (s *Store) NextDueDelivery(ctx context.Context, now time.Time) (delivery.Delivery, bool, error) {
	var item delivery.Delivery
	err := s.db.QueryRowContext(ctx, `
		SELECT e.sequence, s.sink_id, s.sink_name, s.sink_type,
		       e.event_source, e.event_id, e.event_type, COALESCE(e.subject, ''),
		       e.payload, d.attempts
		FROM deliveries d
		JOIN delivery_sinks s ON s.sink_id = d.sink_id
		JOIN events e ON e.sequence = d.event_sequence
		WHERE s.active = 1
		  AND d.state IN ('pending', 'retry_wait')
		  AND d.next_attempt_at <= ?
		  AND NOT EXISTS (
		      SELECT 1 FROM deliveries earlier
		      WHERE earlier.sink_id = d.sink_id
		        AND earlier.event_sequence < d.event_sequence
		        AND earlier.state IN ('pending', 'retry_wait')
		  )
		ORDER BY e.sequence, s.sink_id
		LIMIT 1
	`, formatTimestamp(now)).Scan(
		&item.EventSequence, &item.SinkID, &item.SinkName, &item.SinkType,
		&item.Source, &item.ID, &item.Type, &item.Subject, &item.Payload, &item.Attempts,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return delivery.Delivery{}, false, nil
	}
	if err != nil {
		return delivery.Delivery{}, false, fmt.Errorf("query next due delivery: %w", err)
	}
	return item, true, nil
}

func (s *Store) MarkDelivered(
	ctx context.Context,
	item delivery.Delivery,
	attempt int,
	status int,
	at time.Time,
) error {
	var responseStatus any
	if status != 0 {
		responseStatus = status
	}
	result, err := s.db.ExecContext(ctx, `
		UPDATE deliveries
		SET state = 'delivered', attempts = ?, last_attempt_at = ?,
		    delivered_at = ?, dead_lettered_at = NULL, last_error = NULL,
		    response_status = ?
		WHERE event_sequence = ? AND sink_id = ? AND attempts = ?
		  AND state IN ('pending', 'retry_wait')
	`, attempt, formatTimestamp(at), formatTimestamp(at),
		responseStatus, item.EventSequence, item.SinkID, item.Attempts)
	return expectOneDeliveryUpdate(result, err, "mark delivered")
}

func (s *Store) MarkFailed(
	ctx context.Context,
	item delivery.Delivery,
	attempt int,
	permanent bool,
	retryAt time.Time,
	failure string,
	status int,
	at time.Time,
) error {
	state := "retry_wait"
	var deadLetteredAt any
	if permanent {
		state = "dead_letter"
		deadLetteredAt = formatTimestamp(at)
	}
	var responseStatus any
	if status != 0 {
		responseStatus = status
	}
	result, err := s.db.ExecContext(ctx, `
		UPDATE deliveries
		SET state = ?, attempts = ?, next_attempt_at = ?, last_attempt_at = ?,
		    delivered_at = NULL, dead_lettered_at = ?, last_error = ?,
		    response_status = ?
		WHERE event_sequence = ? AND sink_id = ? AND attempts = ?
		  AND state IN ('pending', 'retry_wait')
	`, state, attempt, formatTimestamp(retryAt),
		formatTimestamp(at), deadLetteredAt, failure, responseStatus,
		item.EventSequence, item.SinkID, item.Attempts)
	return expectOneDeliveryUpdate(result, err, "mark failed")
}

func expectOneDeliveryUpdate(result sql.Result, err error, operation string) error {
	if err != nil {
		return fmt.Errorf("%w: %s: %v", spool.ErrDurability, operation, err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("%w: inspect %s: %v", spool.ErrDurability, operation, err)
	}
	if affected != 1 {
		return fmt.Errorf("%w: %s changed %d rows; delivery state was modified concurrently",
			spool.ErrDurability, operation, affected)
	}
	return nil
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

func (s *Store) ListDeliveries(ctx context.Context, state string, limit int) ([]DeliveryRow, error) {
	if limit < 1 || limit > 1000 {
		return nil, errors.New("limit must be between 1 and 1000")
	}
	switch state {
	case "", "pending", "retry_wait", "delivered", "dead_letter":
	default:
		return nil, errors.New("state must be pending, retry_wait, delivered, or dead_letter")
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT d.event_sequence, s.sink_name, d.state, d.attempts,
		       d.next_attempt_at, d.last_attempt_at, d.delivered_at,
		       d.dead_lettered_at, COALESCE(d.last_error, ''),
		       d.response_status, e.event_source, e.event_id, e.event_type
		FROM deliveries d
		JOIN delivery_sinks s ON s.sink_id = d.sink_id
		JOIN events e ON e.sequence = d.event_sequence
		WHERE (? = '' OR d.state = ?)
		ORDER BY d.event_sequence, s.sink_id
		LIMIT ?
	`, state, state, limit)
	if err != nil {
		return nil, fmt.Errorf("list deliveries: %w", err)
	}
	defer rows.Close()

	var result []DeliveryRow
	for rows.Next() {
		var row DeliveryRow
		var nextAttempt string
		var lastAttempt, deliveredAt, deadLetteredAt sql.NullString
		var responseStatus sql.NullInt64
		if err := rows.Scan(
			&row.EventSequence, &row.SinkName, &row.State, &row.Attempts,
			&nextAttempt, &lastAttempt, &deliveredAt, &deadLetteredAt,
			&row.LastError, &responseStatus, &row.Source, &row.ID, &row.Type,
		); err != nil {
			return nil, fmt.Errorf("scan delivery: %w", err)
		}
		row.NextAttemptAt, err = time.Parse(time.RFC3339Nano, nextAttempt)
		if err != nil {
			return nil, fmt.Errorf("parse next delivery attempt: %w", err)
		}
		if row.LastAttemptAt, err = nullableTime(lastAttempt); err != nil {
			return nil, fmt.Errorf("parse last delivery attempt: %w", err)
		}
		if row.DeliveredAt, err = nullableTime(deliveredAt); err != nil {
			return nil, fmt.Errorf("parse delivered time: %w", err)
		}
		if row.DeadLetteredAt, err = nullableTime(deadLetteredAt); err != nil {
			return nil, fmt.Errorf("parse dead-letter time: %w", err)
		}
		if responseStatus.Valid {
			row.ResponseStatus = int(responseStatus.Int64)
		}
		result = append(result, row)
	}
	return result, rows.Err()
}

func nullableTime(value sql.NullString) (*time.Time, error) {
	if !value.Valid {
		return nil, nil
	}
	parsed, err := time.Parse(time.RFC3339Nano, value.String)
	if err != nil {
		return nil, err
	}
	return &parsed, nil
}

func (s *Store) RedriveDelivery(
	ctx context.Context,
	sinkName string,
	source string,
	id string,
	at time.Time,
) error {
	result, err := s.db.ExecContext(ctx, `
		UPDATE deliveries
		SET state = 'retry_wait', attempts = 0, next_attempt_at = ?,
		    last_attempt_at = NULL, delivered_at = NULL, dead_lettered_at = NULL,
		    last_error = NULL, response_status = NULL
		WHERE state = 'dead_letter'
		  AND sink_id = (
		      SELECT sink_id FROM delivery_sinks
		      WHERE sink_name = ? AND active = 1
		  )
		  AND event_sequence = (
		      SELECT sequence FROM events WHERE event_source = ? AND event_id = ?
		  )
	`, formatTimestamp(at), sinkName, source, id)
	if err != nil {
		return fmt.Errorf("%w: redrive delivery: %v", spool.ErrDurability, err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("%w: inspect redrive: %v", spool.ErrDurability, err)
	}
	if affected != 1 {
		return errors.New("no matching dead-letter delivery was found")
	}
	return nil
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

func formatTimestamp(value time.Time) string {
	return value.UTC().Format(timestampFormat)
}

func ParseSequence(value string) (int64, error) {
	return strconv.ParseInt(value, 10, 64)
}
