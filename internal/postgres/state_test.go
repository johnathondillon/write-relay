package postgres

import (
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
)

func TestStateMachineBuffersUntilCommitAndPreservesOrder(t *testing.T) {
	state := testState()
	begin := &pglogrepl.BeginMessage{Xid: 42}
	begin.SetType(pglogrepl.MessageTypeBegin)
	if batch, err := state.Consume(begin); err != nil || batch != nil {
		t.Fatalf("begin: batch=%v err=%v", batch, err)
	}
	for index, id := range []string{"one", "two"} {
		message := &pglogrepl.LogicalDecodingMessage{
			LSN:           pglogrepl.LSN(100 + index),
			Transactional: true,
			Prefix:        "writerelay.v1",
			Content:       []byte(`{"specversion":"1.0","id":"` + id + `","source":"urn:test","type":"created"}`),
		}
		message.SetType(pglogrepl.MessageTypeMessage)
		if batch, err := state.Consume(message); err != nil || batch != nil {
			t.Fatalf("message: batch=%v err=%v", batch, err)
		}
	}
	commitTime := time.Date(2026, 7, 27, 20, 24, 0, 0, time.UTC)
	commit := &pglogrepl.CommitMessage{
		CommitLSN:         200,
		TransactionEndLSN: 220,
		CommitTime:        commitTime,
	}
	commit.SetType(pglogrepl.MessageTypeCommit)
	batch, err := state.Consume(commit)
	if err != nil {
		t.Fatal(err)
	}
	if len(batch.Events) != 2 || batch.Events[0].ID != "one" || batch.Events[1].MessageIndex != 1 {
		t.Fatalf("unexpected batch: %#v", batch)
	}
	if batch.CommitEndLSN != 220 || batch.Events[0].TransactionID != 42 {
		t.Fatalf("missing commit metadata: %#v", batch)
	}
}

func TestStateMachineProtocolErrors(t *testing.T) {
	tests := []struct {
		name string
		run  func(*StateMachine) error
		want error
	}{
		{
			name: "commit without begin",
			run: func(s *StateMachine) error {
				message := &pglogrepl.CommitMessage{}
				message.SetType(pglogrepl.MessageTypeCommit)
				_, err := s.Consume(message)
				return err
			},
			want: ErrProtocolState,
		},
		{
			name: "nested begin",
			run: func(s *StateMachine) error {
				message := &pglogrepl.BeginMessage{}
				message.SetType(pglogrepl.MessageTypeBegin)
				_, _ = s.Consume(message)
				_, err := s.Consume(message)
				return err
			},
			want: ErrProtocolState,
		},
		{
			name: "nontransactional protected message",
			run: func(s *StateMachine) error {
				message := &pglogrepl.LogicalDecodingMessage{Prefix: "writerelay.v1"}
				message.SetType(pglogrepl.MessageTypeMessage)
				_, err := s.Consume(message)
				return err
			},
			want: ErrProtectedEvent,
		},
		{
			name: "malformed protected message",
			run: func(s *StateMachine) error {
				begin := &pglogrepl.BeginMessage{}
				begin.SetType(pglogrepl.MessageTypeBegin)
				_, _ = s.Consume(begin)
				message := &pglogrepl.LogicalDecodingMessage{
					Transactional: true, Prefix: "writerelay.v1", Content: []byte(`{}`),
				}
				message.SetType(pglogrepl.MessageTypeMessage)
				_, err := s.Consume(message)
				return err
			},
			want: ErrProtectedEvent,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.run(testState()); !errors.Is(err, tt.want) {
				t.Fatalf("got %v, want %v", err, tt.want)
			}
		})
	}
}

func TestStateMachineIgnoresOtherPrefix(t *testing.T) {
	state := testState()
	begin := &pglogrepl.BeginMessage{Xid: 5}
	begin.SetType(pglogrepl.MessageTypeBegin)
	_, _ = state.Consume(begin)
	message := &pglogrepl.LogicalDecodingMessage{
		Transactional: true, Prefix: "other", Content: []byte(`not-json`),
	}
	message.SetType(pglogrepl.MessageTypeMessage)
	if _, err := state.Consume(message); err != nil {
		t.Fatal(err)
	}
	commit := &pglogrepl.CommitMessage{CommitLSN: 10, TransactionEndLSN: 11, CommitTime: time.Now()}
	commit.SetType(pglogrepl.MessageTypeCommit)
	batch, err := state.Consume(commit)
	if err != nil || len(batch.Events) != 0 {
		t.Fatalf("batch=%#v err=%v", batch, err)
	}
}

func testState() *StateMachine {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	return NewStateMachine("writerelay.v1", 262144, 100, 1048576, logger)
}
