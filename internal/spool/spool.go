package spool

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pglogrepl"
)

var (
	ErrIdentityConflict = errors.New("event identity conflicts with existing payload")
	ErrDurability       = errors.New("spool durability failure")
)

type CapturedEvent struct {
	Source        string
	ID            string
	Type          string
	Subject       string
	Payload       []byte
	PayloadSHA256 [32]byte

	TransactionID uint32
	MessageLSN    pglogrepl.LSN
	CommitLSN     pglogrepl.LSN
	CommitEndLSN  pglogrepl.LSN
	CommitTime    time.Time
	MessageIndex  int
}

type CommittedBatch struct {
	TransactionID uint32
	CommitLSN     pglogrepl.LSN
	CommitEndLSN  pglogrepl.LSN
	CommitTime    time.Time
	Events        []CapturedEvent
}

type PersistResult struct {
	Inserted   int
	Replayed   int
	DurableLSN pglogrepl.LSN
}

type Spool interface {
	PersistCommittedBatch(context.Context, CommittedBatch) (PersistResult, error)
	LastDurableLSN(context.Context) (pglogrepl.LSN, error)
	Close() error
}
