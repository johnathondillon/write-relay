//go:build integration

package integration

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	writerelay "github.com/johnathondillon/write-relay/sdk/go"
)

type inboxExec func(context.Context, string, ...any) error
type inboxRunner func(context.Context, writerelay.InboxDelivery, func(inboxExec) error) (writerelay.InboxResult, error)
type inboxOutcome struct {
	result writerelay.InboxResult
	err    error
}

func TestGoReceiverInbox(t *testing.T) {
	dsn := os.Getenv("WRITERELAY_INTEGRATION_DSN")
	if dsn == "" {
		dsn = "postgres://postgres:postgres-dev-password@localhost:5432/writerelay?sslmode=disable"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.MaxConns = 5
	cfg.ConnConfig.RuntimeParams["statement_timeout"] = "8000"
	// Prove the helper overrides a session default that cannot safely refresh the
	// statement snapshot after a conflicting INSERT.
	cfg.ConnConfig.RuntimeParams["default_transaction_isolation"] = "serializable"
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	db := stdlib.OpenDB(*cfg.ConnConfig)
	defer db.Close()
	db.SetMaxOpenConns(5)
	table := "wr_inbox_" + strconv.FormatInt(time.Now().UnixNano(), 36)
	effects := table + "_effects"
	ddl, err := writerelay.InboxTableSQL(writerelay.InboxTable{Table: table})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, ddl); err != nil {
		t.Fatal(err)
	}
	defer func() {
		cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		if _, err := pool.Exec(cleanup, "DROP TABLE IF EXISTS "+effects+", "+table); err != nil {
			t.Error(err)
		}
	}()
	if _, err = pool.Exec(ctx, "CREATE TABLE "+effects+" (id text PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}
	var version string
	if err = pool.QueryRow(ctx, "SHOW server_version").Scan(&version); err != nil {
		t.Fatal(err)
	}
	t.Logf("Go receiver inbox: PostgreSQL %s", version)
	runners := map[string]inboxRunner{
		"pgxpool": func(ctx context.Context, d writerelay.InboxDelivery, handle func(inboxExec) error) (writerelay.InboxResult, error) {
			return writerelay.WithInbox(ctx, pool, d, func(tx writerelay.InboxTx) error {
				return handle(func(ctx context.Context, q string, a ...any) error { _, err := tx.Exec(ctx, q, a...); return err })
			})
		},
		"database_sql": func(ctx context.Context, d writerelay.InboxDelivery, handle func(inboxExec) error) (writerelay.InboxResult, error) {
			return writerelay.WithInboxSQL(ctx, db, d, func(tx writerelay.InboxSQLTx) error {
				return handle(func(ctx context.Context, q string, a ...any) error {
					_, err := tx.ExecContext(ctx, q, a...)
					return err
				})
			})
		},
	}
	receipt := func(label string) writerelay.InboxDelivery {
		return writerelay.InboxDelivery{InboxTable: writerelay.InboxTable{Table: table}, Key: fmt.Sprintf("%x", sha256.Sum256([]byte(label))), Body: []byte(`{"ok":true}`)}
	}
	check := func(t *testing.T, key string, want int) {
		t.Helper()
		for _, query := range []string{"SELECT count(*) FROM " + table + " WHERE idempotency_key=$1", "SELECT count(*) FROM " + effects + " WHERE id=$1"} {
			var n int
			if err := pool.QueryRow(ctx, query, key).Scan(&n); err != nil || n != want {
				t.Fatalf("count=%d want=%d error=%v", n, want, err)
			}
		}
	}
	write := func(ctx context.Context, exec inboxExec, key string) error {
		return exec(ctx, "INSERT INTO "+effects+" VALUES ($1)", key)
	}
	for name, run := range runners {
		t.Run(name, func(t *testing.T) {
			d := receipt(name + "-committed")
			result, err := run(ctx, d, func(exec inboxExec) error {
				// This expression errors if the helper did not choose READ COMMITTED.
				if err := exec(ctx, "SELECT 1 / CASE WHEN current_setting('transaction_isolation')='read committed' THEN 1 ELSE 0 END"); err != nil {
					return err
				}
				if err := write(ctx, exec, d.Key); err != nil {
					return err
				}
				check(t, d.Key, 0) // A separate connection cannot see either uncommitted row.
				return nil
			})
			if err != nil || result != writerelay.InboxProcessed {
				t.Fatal(result, err)
			}
			check(t, d.Key, 1)
			result, err = run(ctx, d, func(inboxExec) error { return errors.New("duplicate callback ran") })
			if err != nil || result != writerelay.InboxDuplicate {
				t.Fatal(result, err)
			}
			d.Body = []byte(`{"ok":false}`)
			if _, err = run(ctx, d, func(inboxExec) error { return errors.New("conflict callback ran") }); !errors.Is(err, writerelay.ErrInboxConflict) {
				t.Fatal(err)
			}
			check(t, d.Key, 1)

			failure := errors.New("business failed")
			for _, mode := range []string{"error", "swallowed_sql", "cancel", "panic"} {
				t.Run(mode, func(t *testing.T) {
					d := receipt(name + "-" + mode)
					request, stop := context.WithCancel(ctx)
					defer stop()
					var gotPanic any
					func() {
						defer func() { gotPanic = recover() }()
						result, err = run(request, d, func(exec inboxExec) error {
							if err := write(request, exec, d.Key); err != nil {
								return err
							}
							switch mode {
							case "error":
								return failure
							case "swallowed_sql":
								_ = exec(request, "SELECT 1/0")
							case "cancel":
								stop()
							case "panic":
								panic(failure)
							}
							return nil
						})
					}()
					if mode == "panic" {
						if gotPanic != failure {
							t.Fatal(gotPanic)
						}
					} else if err == nil || result != "" {
						t.Fatal(result, err)
					}
					if mode == "error" && !errors.Is(err, failure) {
						t.Fatal(err)
					}
					check(t, d.Key, 0)
					if _, err = run(ctx, d, func(exec inboxExec) error { return write(ctx, exec, d.Key) }); err != nil {
						t.Fatal(err)
					}
					check(t, d.Key, 1)
				})
			}
		})
	}
	// Mixed APIs share the same receipt table. Observe the actual lock wait;
	// do not assume concurrency based on a fixed sleep.
	for _, firstName := range []string{"pgxpool", "database_sql"} {
		for _, rollback := range []bool{false, true} {
			t.Run(fmt.Sprintf("concurrent_%s_rollback_%t", firstName, rollback), func(t *testing.T) {
				secondName := "database_sql"
				if firstName == secondName {
					secondName = "pgxpool"
				}
				d := receipt(t.Name())
				entered, gate := make(chan struct{}), make(chan struct{})
				finish := sync.OnceFunc(func() { close(gate) })
				first, second := make(chan inboxOutcome, 1), make(chan inboxOutcome, 1)
				failure := errors.New("first rolled back")
				go func() {
					r, e := runners[firstName](ctx, d, func(exec inboxExec) error {
						if err := write(ctx, exec, d.Key); err != nil {
							return err
						}
						close(entered)
						select {
						case <-gate:
						case <-ctx.Done():
							return ctx.Err()
						}
						if rollback {
							return failure
						}
						return nil
					})
					first <- inboxOutcome{r, e}
				}()
				secondStarted := false
				defer func() {
					finish()
					if secondStarted {
						<-second
					}
					<-first
				}()
				select {
				case <-entered:
				case got := <-first:
					first <- got
					t.Fatal(got.err)
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				go func() {
					r, e := runners[secondName](ctx, d, func(exec inboxExec) error { return write(ctx, exec, d.Key) })
					second <- inboxOutcome{r, e}
				}()
				secondStarted = true
				deadline := time.Now().Add(5 * time.Second)
				for {
					var blocked int
					err := pool.QueryRow(ctx, "SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE $1", `INSERT INTO "public"."`+table+`"%`).Scan(&blocked)
					if err != nil {
						t.Fatal(err)
					}
					if blocked > 0 {
						break
					}
					if time.Now().After(deadline) {
						t.Fatal("no competing INSERT lock wait")
					}
					time.Sleep(20 * time.Millisecond)
				}
				// Release the first callback; cleanup can safely call finish again.
				finish()
				a, b := <-first, <-second
				first <- a
				second <- b // Leave completed outcomes for deferred cleanup.
				if rollback {
					if !errors.Is(a.err, failure) || b.err != nil || b.result != writerelay.InboxProcessed {
						t.Fatal(a, b)
					}
				} else if a.err != nil || b.err != nil || b.result != writerelay.InboxDuplicate {
					t.Fatal(a, b)
				}
				check(t, d.Key, 1)
			})
		}
	}
}
