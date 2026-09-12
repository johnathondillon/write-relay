// Package writerelay emits domain events inside application-owned PostgreSQL
// transactions and consumes deliveries with receiver-owned inbox transactions.
// Emit leaves transaction control to the caller; WithInbox manages its own.
package writerelay

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
)

// ErrInvalidEvent identifies validation failures before any SQL is sent.
var ErrInvalidEvent = errors.New("writerelay: invalid event")

// ErrNilTransaction means no transaction was supplied.
var ErrNilTransaction = errors.New("writerelay: transaction is required")

// Event is the envelope supported by WriteRelay's producer SDKs. ID, Source,
// and Type are required. The caller owns the identity and content across retries.
// This is a supported subset of CloudEvents, not full conformance support.
type Event struct {
	ID              string          `json:"id"`
	Source          string          `json:"source"`
	Type            string          `json:"type"`
	SpecVersion     string          `json:"specversion"`               // Empty defaults to "1.0".
	Subject         *string         `json:"subject,omitempty"`         // Nil omits; pointer to "" includes an empty subject.
	Time            string          `json:"time,omitempty"`            // Empty omits; otherwise strict RFC 3339.
	DataContentType string          `json:"datacontenttype,omitempty"` // Empty defaults to application/json when Data is present.
	Data            json.RawMessage `json:"data,omitempty"`            // Nil omits; []byte("null") includes JSON null.
}

const emitStatement = "SELECT writerelay.emit($1::jsonb)"

// Emit validates and emits an event on an existing pgx v5 transaction. Use the
// same transaction for business writes and roll back on any error. A nil error
// means the statement succeeded, not that the transaction committed or delivery
// occurred. Database errors are returned unchanged. Pools/connections are not
// accepted in place of a transaction; transactions from pgxpool are supported.
func Emit(ctx context.Context, tx pgx.Tx, event Event) error {
	if tx == nil || (reflect.ValueOf(tx).Kind() == reflect.Ptr && reflect.ValueOf(tx).IsNil()) {
		return ErrNilTransaction
	}
	payload, err := encode(event)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, emitStatement, payload)
	return err
}

// EmitSQL is Emit for database/sql. The transaction must use a PostgreSQL
// driver. The SDK never begins, commits, rolls back, or retries it.
func EmitSQL(ctx context.Context, tx *sql.Tx, event Event) error {
	if tx == nil {
		return ErrNilTransaction
	}
	payload, err := encode(event)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, emitStatement, payload)
	return err
}

var timestamp = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$`)

func encode(event Event) (string, error) {
	for _, field := range []struct{ name, value string }{
		{"id", event.ID}, {"source", event.Source}, {"type", event.Type},
	} {
		if field.value == "" || !validString(field.value) {
			return "", fmt.Errorf("%w: %s must be a non-empty UTF-8 string without NUL", ErrInvalidEvent, field.name)
		}
	}
	if event.SpecVersion == "" {
		event.SpecVersion = "1.0"
	}
	if event.SpecVersion != "1.0" {
		return "", fmt.Errorf("%w: specversion must be 1.0", ErrInvalidEvent)
	}
	if event.Subject != nil && !validString(*event.Subject) {
		return "", fmt.Errorf("%w: subject must be UTF-8 without NUL", ErrInvalidEvent)
	}
	if !validString(event.DataContentType) {
		return "", fmt.Errorf("%w: datacontenttype must be UTF-8 without NUL", ErrInvalidEvent)
	}
	if event.Time != "" {
		_, err := time.Parse(time.RFC3339Nano, event.Time)
		// Go's parser accepts some offsets outside RFC 3339 and comma fractions.
		if err != nil || !timestamp.MatchString(event.Time) {
			return "", fmt.Errorf("%w: time must be RFC 3339", ErrInvalidEvent)
		}
		if !strings.HasSuffix(event.Time, "Z") {
			offset := event.Time[len(event.Time)-5:]
			if offset[:2] > "23" || offset[3:] > "59" {
				return "", fmt.Errorf("%w: time has an invalid timezone offset", ErrInvalidEvent)
			}
		}
	}
	if event.Data != nil {
		if !validJSON(event.Data) {
			return "", fmt.Errorf("%w: data must be valid UTF-8 JSON without NUL or unpaired surrogates", ErrInvalidEvent)
		}
		if event.DataContentType == "" {
			event.DataContentType = "application/json"
		}
	}
	payload, err := json.Marshal(event)
	if err != nil {
		return "", fmt.Errorf("%w: cannot encode envelope", ErrInvalidEvent)
	}
	return string(payload), nil
}

func validString(value string) bool {
	return utf8.ValidString(value) && !strings.ContainsRune(value, 0)
}

// encoding/json accepts unpaired surrogate escapes and replaces invalid UTF-8;
// PostgreSQL jsonb rejects them. Inspect escapes after validating JSON syntax,
// including object keys. Escaped backslashes must not be mistaken for \u escapes.
func validJSON(data []byte) bool {
	if !utf8.Valid(data) || !json.Valid(data) {
		return false
	}
	for i := 0; i < len(data); i++ {
		if data[i] != '\\' {
			continue
		}
		i++
		if data[i] != 'u' {
			continue
		}
		code, _ := strconv.ParseUint(string(data[i+1:i+5]), 16, 16)
		i += 4
		if code == 0 || (code >= 0xdc00 && code <= 0xdfff) {
			return false
		}
		if code >= 0xd800 && code <= 0xdbff {
			if i+6 >= len(data) || data[i+1] != '\\' || data[i+2] != 'u' {
				return false
			}
			low, err := strconv.ParseUint(string(data[i+3:i+7]), 16, 16)
			if err != nil || low < 0xdc00 || low > 0xdfff {
				return false
			}
			i += 6
		}
	}
	return true
}
