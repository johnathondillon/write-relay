package writerelay

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestInboxValidationAndDDL(t *testing.T) {
	key := strings.Repeat("a", 64)
	for _, d := range []InboxDelivery{
		{}, {Key: key}, {Key: "short", Body: []byte("{}")},
		{Key: strings.Repeat("A", 64), Body: []byte("{}")},
		{Key: key, Body: []byte("{}"), InboxTable: InboxTable{Table: `x"; DROP TABLE x; --`}},
		{Key: key, Body: []byte("{}"), InboxTable: InboxTable{Schema: "public.other"}},
	} {
		if _, err := WithInbox(context.Background(), nil, d, func(InboxTx) error { return nil }); !errors.Is(err, ErrInvalidDelivery) {
			t.Fatal(err)
		}
		if _, err := WithInboxSQL(context.Background(), nil, d, func(InboxSQLTx) error { return nil }); !errors.Is(err, ErrInvalidDelivery) {
			t.Fatal(err)
		}
	}
	d := InboxDelivery{Key: key, Body: []byte("{}")}
	if _, _, err := prepareInbox(d, false); !errors.Is(err, ErrInvalidDelivery) {
		t.Fatal(err)
	}
	table, hash, err := prepareInbox(d, true)
	if err != nil || table != `"public"."writerelay_inbox"` || hash != "44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a" {
		t.Fatalf("%s %s %v", table, hash, err)
	}
	ddl, err := InboxTableSQL(InboxTable{Schema: "Receiver", Table: "Inbox"})
	if err != nil || !strings.HasPrefix(ddl, `CREATE TABLE "Receiver"."Inbox"`) {
		t.Fatal(ddl, err)
	}
	if _, err := InboxTableSQL(InboxTable{Table: strings.Repeat("x", 64)}); !errors.Is(err, ErrInvalidDelivery) {
		t.Fatal(err)
	}
}

func TestInboxTransactionOutcomes(t *testing.T) {
	original := errors.New("original")
	for _, stage := range []string{"processed", "duplicate", "conflict", "insert", "read", "callback", "guard", "commit", "panic", "cancel", "missing", "unexpected"} {
		t.Run(stage, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var calls []string
			session := inboxSession{
				exec: func(_ context.Context, q string, args ...any) (int64, error) {
					if q == "SELECT 1" {
						calls = append(calls, "guard")
						if stage == "guard" {
							return 0, original
						}
						return 0, nil
					}
					calls = append(calls, "insert")
					if !reflect.DeepEqual(args, []any{"key", "hash"}) || !strings.HasPrefix(q, `INSERT INTO "public"."inbox"`) {
						t.Fatal(q, args)
					}
					if stage == "insert" {
						return 0, original
					}
					if stage == "unexpected" {
						return 2, nil
					}
					if stage == "duplicate" || stage == "conflict" || stage == "read" || stage == "missing" {
						return 0, nil
					}
					return 1, nil
				},
				read: func(_ context.Context, q, key string) (string, error) {
					calls = append(calls, "read")
					if key != "key" || !strings.HasSuffix(q, "FOR SHARE") {
						t.Fatal(q, key)
					}
					if stage == "read" {
						return "", original
					}
					if stage == "missing" {
						return "", ErrInboxTransaction
					}
					if stage == "conflict" {
						return "different", nil
					}
					return "hash", nil
				},
				commit: func(context.Context) error {
					calls = append(calls, "commit")
					if stage == "commit" {
						return original
					}
					return nil
				},
				rollback: func(cleanup context.Context) error {
					calls = append(calls, "rollback")
					if cleanup.Err() != nil {
						t.Fatal("rollback inherited canceled context")
					}
					if deadline, ok := cleanup.Deadline(); !ok || time.Until(deadline) > 6*time.Second {
						t.Fatal("unbounded cleanup")
					}
					return errors.New("rollback error must not mask original")
				},
			}
			var result InboxResult
			var err error
			var recovered any
			func() {
				defer func() { recovered = recover() }()
				result, err = runInbox(ctx, `"public"."inbox"`, "key", "hash", session, func() error {
					calls = append(calls, "callback")
					if stage == "callback" {
						return original
					}
					if stage == "panic" {
						panic(original)
					}
					if stage == "cancel" {
						cancel()
					}
					return nil
				})
			}()
			if stage == "processed" || stage == "duplicate" {
				want := InboxProcessed
				if stage == "duplicate" {
					want = InboxDuplicate
				}
				if result != want || err != nil || calls[len(calls)-1] != "commit" {
					t.Fatal(result, err, calls)
				}
			} else {
				if result != "" || calls[len(calls)-1] != "rollback" {
					t.Fatal(result, calls)
				}
				switch stage {
				case "panic":
					if recovered != original {
						t.Fatal(recovered)
					}
				case "cancel":
					if !errors.Is(err, context.Canceled) {
						t.Fatal(err)
					}
				case "conflict":
					if !errors.Is(err, ErrInboxConflict) {
						t.Fatal(err)
					}
				case "unexpected", "missing":
					if !errors.Is(err, ErrInboxTransaction) {
						t.Fatal(err)
					}
				default:
					if err != original {
						t.Fatal(err)
					}
				}
			}
			if stage == "duplicate" || stage == "conflict" || stage == "read" || stage == "missing" {
				for _, call := range calls {
					if call == "callback" {
						t.Fatal("callback on existing receipt")
					}
				}
			}
		})
	}
}
