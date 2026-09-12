package writerelay

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	// ErrInvalidDelivery identifies invalid inbox arguments before checkout.
	ErrInvalidDelivery = errors.New("writerelay: invalid inbox delivery")
	// ErrInboxConflict means a committed key has different original request bytes.
	ErrInboxConflict = errors.New("writerelay: inbox key reused with different content")
	// ErrInboxTransaction means the inbox transaction could not establish success.
	ErrInboxTransaction = errors.New("writerelay: inconsistent inbox transaction")
)

// InboxTable selects an application-owned inbox. Empty fields default to
// public.writerelay_inbox. Use a separate table per independent consumer.
type InboxTable struct{ Schema, Table string }

// InboxDelivery contains the original webhook identity and bytes. Authenticate,
// bound, and validate the request first. Do not re-serialize the parsed body.
type InboxDelivery struct {
	InboxTable
	Key  string
	Body []byte
}

// InboxResult is meaningful only when the returned error is nil.
type InboxResult string

const (
	InboxProcessed InboxResult = "processed"
	InboxDuplicate InboxResult = "duplicate"
)

// InboxTx exposes business SQL on the helper's pgx transaction. Await/finish all
// work and close rows before returning. Do not manage transactions, retain this
// handle, use concurrent goroutines, or perform external side effects.
type InboxTx interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
	Query(context.Context, string, ...any) (pgx.Rows, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}

// InboxSQLTx is the database/sql counterpart of InboxTx.
type InboxSQLTx interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

// Hide transaction control and underlying connection methods from callbacks.
type inboxPGXView struct{ InboxTx }
type inboxSQLView struct{ InboxSQLTx }

var inboxIdentifier = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,62}$`)
var inboxKey = regexp.MustCompile(`^[a-f0-9]{64}$`)

func (table InboxTable) name() (string, error) {
	if table.Schema == "" {
		table.Schema = "public"
	}
	if table.Table == "" {
		table.Table = "writerelay_inbox"
	}
	if !inboxIdentifier.MatchString(table.Schema) || !inboxIdentifier.MatchString(table.Table) {
		return "", fmt.Errorf("%w: table identifiers must be ASCII SQL identifiers of at most 63 characters", ErrInvalidDelivery)
	}
	return `"` + table.Schema + `"."` + table.Table + `"`, nil
}

// InboxTableSQL returns CREATE TABLE DDL for a one-time application migration.
// It never connects or executes SQL, and does not create the containing schema.
// The columns and hashes are compatible with the TypeScript inbox helper.
func InboxTableSQL(table InboxTable) (string, error) {
	name, err := table.name()
	if err != nil {
		return "", err
	}
	return `CREATE TABLE ` + name + ` (
 idempotency_key text PRIMARY KEY CHECK (idempotency_key ~ '^[a-f0-9]{64}$'),
 payload_sha256 text NOT NULL CHECK (payload_sha256 ~ '^[a-f0-9]{64}$'),
 received_at timestamptz NOT NULL DEFAULT now()
)`, nil
}

func prepareInbox(delivery InboxDelivery, hasHandler bool) (table, digest string, err error) {
	if !inboxKey.MatchString(delivery.Key) || len(delivery.Body) == 0 || !hasHandler {
		return "", "", fmt.Errorf("%w: require a 64-character lowercase hex key, original body bytes, and handler", ErrInvalidDelivery)
	}
	table, err = delivery.InboxTable.name()
	if err != nil {
		return "", "", err
	}
	// Hash synchronously before checkout; no body reference is retained. Callers
	// must not mutate the slice concurrently with this function.
	digest = fmt.Sprintf("%x", sha256.Sum256(delivery.Body))
	return table, digest, nil
}

// WithInbox commits the delivery key and callback writes in one pgxpool
// transaction. Identical duplicates skip the callback; changed bytes conflict.
// Return HTTP success only on nil error. No automatic retry occurs. A commit
// error can have an unknown outcome: retry the same key/body. Retain receipts
// for the entire possible replay horizon. Callback panics roll back then unwind.
func WithInbox(ctx context.Context, pool *pgxpool.Pool, delivery InboxDelivery, handle func(InboxTx) error) (InboxResult, error) {
	table, digest, err := prepareInbox(delivery, handle != nil)
	if err != nil {
		return "", err
	}
	if pool == nil {
		return "", fmt.Errorf("%w: pool is required", ErrInvalidDelivery)
	}
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return "", err
	}
	return runInbox(ctx, table, delivery.Key, digest, inboxSession{
		exec: func(ctx context.Context, query string, args ...any) (int64, error) {
			tag, err := tx.Exec(ctx, query, args...)
			return tag.RowsAffected(), err
		},
		read: func(ctx context.Context, query, key string) (string, error) {
			var hash string
			err := tx.QueryRow(ctx, query, key).Scan(&hash)
			return hash, err
		},
		commit:   tx.Commit,
		rollback: tx.Rollback,
	}, func() error { return handle(inboxPGXView{tx}) })
}

// WithInboxSQL is WithInbox for database/sql using a PostgreSQL driver. It owns
// the transaction; do not pass an existing transaction or commit in the callback.
// The pgx stdlib driver is exercised by the compatibility tests.
func WithInboxSQL(ctx context.Context, db *sql.DB, delivery InboxDelivery, handle func(InboxSQLTx) error) (InboxResult, error) {
	table, digest, err := prepareInbox(delivery, handle != nil)
	if err != nil {
		return "", err
	}
	if db == nil {
		return "", fmt.Errorf("%w: database is required", ErrInvalidDelivery)
	}
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return "", err
	}
	return runInbox(ctx, table, delivery.Key, digest, inboxSession{
		exec: func(ctx context.Context, query string, args ...any) (int64, error) {
			result, err := tx.ExecContext(ctx, query, args...)
			if err != nil {
				return 0, err
			}
			return result.RowsAffected()
		},
		read: func(ctx context.Context, query, key string) (string, error) {
			var hash string
			err := tx.QueryRowContext(ctx, query, key).Scan(&hash)
			return hash, err
		},
		commit:   func(context.Context) error { return tx.Commit() },
		rollback: func(context.Context) error { return tx.Rollback() },
	}, func() error { return handle(inboxSQLView{tx}) })
}

type inboxSession struct {
	exec     func(context.Context, string, ...any) (int64, error)
	read     func(context.Context, string, string) (string, error)
	commit   func(context.Context) error
	rollback func(context.Context) error
}

func runInbox(ctx context.Context, table, key, digest string, tx inboxSession, handle func() error) (InboxResult, error) {
	committed := false
	defer func() {
		if !committed {
			// Cancellation and panics must not strand a pgxpool transaction. Preserve
			// the original error; pgx discards a connection whose rollback fails.
			cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = tx.rollback(cleanup)
		}
	}()
	n, err := tx.exec(ctx, `INSERT INTO `+table+` (idempotency_key, payload_sha256) VALUES ($1, $2) ON CONFLICT (idempotency_key) DO NOTHING`, key, digest)
	if err != nil {
		return "", err
	}
	result := InboxProcessed
	switch n {
	case 1:
		if err := handle(); err != nil {
			return "", err
		}
	case 0:
		previous, err := tx.read(ctx, `SELECT payload_sha256 FROM `+table+` WHERE idempotency_key = $1 FOR SHARE`, key)
		if errors.Is(err, pgx.ErrNoRows) || errors.Is(err, sql.ErrNoRows) {
			return "", ErrInboxTransaction
		}
		if err != nil {
			return "", err
		}
		if previous != digest {
			return "", ErrInboxConflict
		}
		result = InboxDuplicate
	default:
		return "", ErrInboxTransaction
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	// In an aborted PostgreSQL transaction COMMIT may report ROLLBACK. pgx
	// rejects that tag; database/sql does not expose tags. This statement fails
	// if the callback swallowed an SQL error, before either API attempts commit.
	if _, err := tx.exec(ctx, "SELECT 1"); err != nil {
		return "", err
	}
	if err := tx.commit(ctx); err != nil {
		return "", err
	}
	committed = true
	return result, nil
}
