package postgres

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"log/slog"

	"github.com/jackc/pglogrepl"
	"github.com/johnathondillon/write-relay/internal/event"
	"github.com/johnathondillon/write-relay/internal/spool"
)

var (
	ErrProtocolState      = errors.New("logical replication protocol state error")
	ErrProtectedEvent     = errors.New("invalid protected-prefix event")
	ErrTransactionLimit   = errors.New("transaction event buffer limit exceeded")
	ErrSlotMismatch       = errors.New("replication slot does not match configuration")
	ErrUnsupportedVersion = errors.New("unsupported PostgreSQL version")
)

type StateMachine struct {
	prefix        string
	maxEventBytes int
	maxEvents     int
	maxBytes      int
	logger        *slog.Logger

	inTransaction bool
	transactionID uint32
	events        []spool.CapturedEvent
	bufferedBytes int
}

func NewStateMachine(prefix string, maxEventBytes, maxEvents, maxBytes int, logger *slog.Logger) *StateMachine {
	if logger == nil {
		logger = slog.Default()
	}
	return &StateMachine{
		prefix: prefix, maxEventBytes: maxEventBytes, maxEvents: maxEvents,
		maxBytes: maxBytes, logger: logger,
	}
}

// Consume applies one pgoutput protocol-v1 message. A returned batch represents
// a complete PostgreSQL transaction and must be durably persisted before its
// CommitEndLSN may be acknowledged.
func (s *StateMachine) Consume(message pglogrepl.Message) (*spool.CommittedBatch, error) {
	switch typed := message.(type) {
	case *pglogrepl.BeginMessage:
		if s.inTransaction {
			return nil, fmt.Errorf("%w: nested Begin", ErrProtocolState)
		}
		s.inTransaction = true
		s.transactionID = typed.Xid
		s.events = s.events[:0]
		s.bufferedBytes = 0
		return nil, nil

	case *pglogrepl.LogicalDecodingMessage:
		return nil, s.consumeLogicalMessage(typed)

	case *pglogrepl.CommitMessage:
		if !s.inTransaction {
			return nil, fmt.Errorf("%w: Commit without Begin", ErrProtocolState)
		}
		batch := spool.CommittedBatch{
			TransactionID: s.transactionID,
			CommitLSN:     typed.CommitLSN,
			CommitEndLSN:  typed.TransactionEndLSN,
			CommitTime:    typed.CommitTime,
			Events:        append([]spool.CapturedEvent(nil), s.events...),
		}
		for index := range batch.Events {
			batch.Events[index].TransactionID = batch.TransactionID
			batch.Events[index].CommitLSN = batch.CommitLSN
			batch.Events[index].CommitEndLSN = batch.CommitEndLSN
			batch.Events[index].CommitTime = batch.CommitTime
			batch.Events[index].MessageIndex = index
		}
		s.reset()
		return &batch, nil

	case *pglogrepl.RelationMessage, *pglogrepl.TypeMessage,
		*pglogrepl.InsertMessage, *pglogrepl.UpdateMessage,
		*pglogrepl.DeleteMessage, *pglogrepl.TruncateMessage:
		s.logger.Warn("publication drift: unexpected table-change protocol message ignored",
			"message_type", message.Type().String())
		return nil, nil

	default:
		s.logger.Warn("unexpected pgoutput protocol message ignored",
			"message_type", message.Type().String())
		return nil, nil
	}
}

func (s *StateMachine) consumeLogicalMessage(message *pglogrepl.LogicalDecodingMessage) error {
	if message.Prefix != s.prefix {
		return nil
	}
	if !message.Transactional {
		return fmt.Errorf("%w: prefix %q was used by a non-transactional message", ErrProtectedEvent, s.prefix)
	}
	if !s.inTransaction {
		return fmt.Errorf("%w: protected transactional message outside Begin/Commit", ErrProtocolState)
	}
	if len(s.events) >= s.maxEvents {
		return fmt.Errorf("%w: more than %d accepted events", ErrTransactionLimit, s.maxEvents)
	}
	if len(message.Content) > s.maxBytes-s.bufferedBytes {
		return fmt.Errorf("%w: accepted event bytes exceed %d", ErrTransactionLimit, s.maxBytes)
	}
	metadata, err := event.Validate(message.Content, s.maxEventBytes)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrProtectedEvent, err)
	}
	payload := append([]byte(nil), message.Content...)
	s.events = append(s.events, spool.CapturedEvent{
		Source:        metadata.Source,
		ID:            metadata.ID,
		Type:          metadata.Type,
		Subject:       metadata.Subject,
		Payload:       payload,
		PayloadSHA256: sha256.Sum256(payload),
		MessageLSN:    message.LSN,
	})
	s.bufferedBytes += len(payload)
	return nil
}

func (s *StateMachine) Reset() {
	s.reset()
}

func (s *StateMachine) reset() {
	s.inTransaction = false
	s.transactionID = 0
	s.events = nil
	s.bufferedBytes = 0
}
