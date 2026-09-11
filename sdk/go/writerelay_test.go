package writerelay_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	writerelay "github.com/johnathondillon/write-relay/sdk/go"
)

type recordingTx struct {
	pgx.Tx // Unexpected transaction operations panic rather than silently passing.
	calls  int
	query  string
	args   []any
	ctx    context.Context
	err    error
}

func (tx *recordingTx) Exec(ctx context.Context, query string, args ...any) (pgconn.CommandTag, error) {
	tx.calls++
	tx.query, tx.args, tx.ctx = query, args, ctx
	return pgconn.CommandTag{}, tx.err
}

func minimal() writerelay.Event {
	return writerelay.Event{ID: "event-1", Source: "urn:test", Type: "order.paid"}
}

func emitted(t *testing.T, tx *recordingTx) map[string]json.RawMessage {
	t.Helper()
	if tx.query != "SELECT writerelay.emit($1::jsonb)" || len(tx.args) != 1 {
		t.Fatalf("unexpected SQL: %q, %#v", tx.query, tx.args)
	}
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal([]byte(tx.args[0].(string)), &envelope); err != nil {
		t.Fatal(err)
	}
	return envelope
}

func TestEmitParameterizationDefaultsAndStableContent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	event := minimal()
	event.ID = "quote'); SELECT pg_sleep(10); --"
	event.Data = json.RawMessage(`{"amount":9007199254740993,"nested":[true,null,"雪"]}`)
	original := string(event.Data)
	tx := &recordingTx{}
	if err := writerelay.Emit(ctx, tx, event); err != nil {
		t.Fatal(err)
	}
	envelope := emitted(t, tx)
	if string(envelope["specversion"]) != `"1.0"` || string(envelope["datacontenttype"]) != `"application/json"` || string(envelope["data"]) != original {
		t.Fatalf("unexpected defaults or changed data: %s", tx.args[0])
	}
	var id string
	_ = json.Unmarshal(envelope["id"], &id)
	if id != event.ID || tx.ctx != ctx || tx.calls != 1 {
		t.Fatal("identity, context, or execution count changed")
	}
	first := tx.args[0]
	if err := writerelay.Emit(ctx, tx, event); err != nil || tx.args[0] != first || tx.calls != 2 {
		t.Fatalf("repeated emission changed content: %v", err)
	}
	if event.SpecVersion != "" || event.DataContentType != "" || string(event.Data) != original {
		t.Fatal("caller event was mutated")
	}
}

func TestOptionalMetadataAndJSONValues(t *testing.T) {
	for _, raw := range []string{"", "null", "false", "0", `""`, "[]", "{}", `"\\u0000"`, `"\uD83D\uDE00"`, `{"\uD83D\uDE00":"ok"}`} {
		t.Run(raw, func(t *testing.T) {
			event := minimal()
			if raw != "" {
				event.Data = json.RawMessage(raw)
			}
			tx := &recordingTx{}
			if err := writerelay.Emit(context.Background(), tx, event); err != nil {
				t.Fatal(err)
			}
			envelope := emitted(t, tx)
			_, dataPresent := envelope["data"]
			_, contentTypePresent := envelope["datacontenttype"]
			if dataPresent != (raw != "") || contentTypePresent != dataPresent {
				t.Fatal("absent data and JSON values were conflated")
			}
			if len(envelope) != 4 && raw == "" {
				t.Fatalf("optional metadata unexpectedly added: %#v", envelope)
			}
		})
	}
	empty := ""
	event := minimal()
	event.Subject, event.Time, event.DataContentType = &empty, "2024-02-29T23:59:59.123456789+05:30", "application/cloudevents+json"
	tx := &recordingTx{}
	if err := writerelay.Emit(context.Background(), tx, event); err != nil {
		t.Fatal(err)
	}
	envelope := emitted(t, tx)
	if string(envelope["subject"]) != `""` || string(envelope["time"]) != `"`+event.Time+`"` || string(envelope["datacontenttype"]) != `"application/cloudevents+json"` {
		t.Fatal("explicit metadata changed")
	}
}

func TestInvalidEventsNeverExecuteSQL(t *testing.T) {
	tests := map[string]func(*writerelay.Event){
		"missing id":        func(e *writerelay.Event) { e.ID = "" },
		"missing source":    func(e *writerelay.Event) { e.Source = "" },
		"missing type":      func(e *writerelay.Event) { e.Type = "" },
		"version":           func(e *writerelay.Event) { e.SpecVersion = "2.0" },
		"invalid UTF8":      func(e *writerelay.Event) { e.ID = string([]byte{0xff}) },
		"NUL source":        func(e *writerelay.Event) { e.Source = "secret\x00" },
		"NUL subject":       func(e *writerelay.Event) { s := "secret\x00"; e.Subject = &s },
		"NUL content type":  func(e *writerelay.Event) { e.DataContentType = "secret\x00" },
		"empty nonnil data": func(e *writerelay.Event) { e.Data = json.RawMessage{} },
	}
	for name, change := range tests {
		t.Run(name, func(t *testing.T) { e := minimal(); change(&e); assertInvalid(t, e) })
	}
	for _, raw := range []string{`{"secret":`, `null true`, `NaN`, `"\u0000"`, `{"\u0000":1}`, `"\uD800"`, `"\uDC00"`, `"\uD800\u0041"`, `"\uD800\\uDC00"`, `"` + string([]byte{0xff}) + `"`} {
		t.Run("data/"+raw, func(t *testing.T) { e := minimal(); e.Data = json.RawMessage(raw); assertInvalid(t, e) })
	}
	for _, timestamp := range []string{"secret", "2025-02-29T00:00:00Z", "2026-01-01", "2026-01-01T1:00:00Z", "2026-01-01T00:00:00,1Z", "2026-01-01T00:00:00+24:00", "2026-01-01T00:00:00+00:60", "2026-01-01T24:00:00Z", "2026-01-01T00:00:60Z"} {
		t.Run(timestamp, func(t *testing.T) { e := minimal(); e.Time = timestamp; assertInvalid(t, e) })
	}
}

func assertInvalid(t *testing.T, event writerelay.Event) {
	t.Helper()
	tx := &recordingTx{}
	err := writerelay.Emit(context.Background(), tx, event)
	if !errors.Is(err, writerelay.ErrInvalidEvent) || tx.calls != 0 {
		t.Fatalf("validation failed to prevent SQL: calls=%d err=%v", tx.calls, err)
	}
	if strings.Contains(err.Error(), "secret") {
		t.Fatal("validation error exposed payload")
	}
}

func TestDatabaseErrorAndNilTransactions(t *testing.T) {
	want := &pgconn.PgError{Code: "42501", Message: "permission denied"}
	tx := &recordingTx{err: want}
	if err := writerelay.Emit(context.Background(), tx, minimal()); err != want || tx.calls != 1 {
		t.Fatalf("error replaced or retried: calls=%d err=%v", tx.calls, err)
	}
	var typedNil *recordingTx
	for _, tx := range []pgx.Tx{nil, typedNil} {
		if err := writerelay.Emit(context.Background(), tx, minimal()); !errors.Is(err, writerelay.ErrNilTransaction) {
			t.Fatalf("nil transaction: %v", err)
		}
	}
	if err := writerelay.EmitSQL(context.Background(), nil, minimal()); !errors.Is(err, writerelay.ErrNilTransaction) {
		t.Fatalf("nil sql transaction: %v", err)
	}
}

// A database/sql driver verifies the concrete *sql.Tx path without PostgreSQL.
type sqlRecorder struct {
	recordingTx
	begins, commits, rollbacks int
}
type connector struct{ conn *sqlRecorder }

func (c connector) Connect(context.Context) (driver.Conn, error) { return c.conn, nil }
func (c connector) Driver() driver.Driver                        { return stubDriver{} }

type stubDriver struct{}

func (stubDriver) Open(string) (driver.Conn, error) { return nil, errors.New("use connector") }
func (c *sqlRecorder) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("unexpected prepare")
}
func (c *sqlRecorder) Close() error              { return nil }
func (c *sqlRecorder) Begin() (driver.Tx, error) { c.begins++; return c, nil }
func (c *sqlRecorder) Commit() error             { c.commits++; return nil }
func (c *sqlRecorder) Rollback() error           { c.rollbacks++; return nil }
func (c *sqlRecorder) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	values := make([]any, len(args))
	for i, arg := range args {
		values[i] = arg.Value
	}
	_, err := c.recordingTx.Exec(ctx, query, values...)
	return driver.RowsAffected(1), err
}

func TestEmitSQLUsesCallerTransactionAndPropagatesErrors(t *testing.T) {
	ctx := context.Background()
	c := &sqlRecorder{}
	db := sql.OpenDB(connector{c})
	defer db.Close()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	event := minimal()
	event.Data = json.RawMessage(`{"ok":true}`)
	if err := writerelay.EmitSQL(ctx, tx, event); err != nil {
		t.Fatal(err)
	}
	pgxTx := &recordingTx{}
	if err := writerelay.Emit(ctx, pgxTx, event); err != nil {
		t.Fatal(err)
	}
	if c.query != pgxTx.query || !reflect.DeepEqual(c.args, pgxTx.args) {
		t.Fatal("SQL and pgx paths differ")
	}
	want := errors.New("database error")
	c.err = want
	if err := writerelay.EmitSQL(ctx, tx, event); err != want || c.calls != 2 {
		t.Fatalf("database error replaced or retried: %v", err)
	}
	event.ID = ""
	if err := writerelay.EmitSQL(ctx, tx, event); !errors.Is(err, writerelay.ErrInvalidEvent) || c.calls != 2 {
		t.Fatal("invalid event reached SQL")
	}
	if c.begins != 1 || c.commits != 0 || c.rollbacks != 0 {
		t.Fatal("SDK took transaction ownership")
	}
	if err := tx.Rollback(); err != nil || c.rollbacks != 1 {
		t.Fatalf("caller cannot roll back: %v", err)
	}
	if err := writerelay.EmitSQL(ctx, tx, minimal()); !errors.Is(err, sql.ErrTxDone) {
		t.Fatalf("closed transaction: %v", err)
	}
}

func FuzzRawJSON(f *testing.F) {
	for _, seed := range []string{`null`, `"\uD83D\uDE00"`, `"\uD800"`, `{"key":"\\u0000"}`, `{"x":[1,true]}`} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, data string) {
		e := minimal()
		e.Data = json.RawMessage(data)
		tx := &recordingTx{}
		err := writerelay.Emit(context.Background(), tx, e)
		if err == nil {
			emitted(t, tx)
		} else if !errors.Is(err, writerelay.ErrInvalidEvent) || tx.calls != 0 {
			t.Fatalf("unexpected failure: %v", err)
		}
	})
}
