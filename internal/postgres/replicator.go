package postgres

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/johnathondillon/write-relay/internal/config"
	"github.com/johnathondillon/write-relay/internal/spool"
)

type Replicator struct {
	cfg    config.Config
	spool  spool.Spool
	logger *slog.Logger
}

func NewReplicator(cfg config.Config, durableSpool spool.Spool, logger *slog.Logger) *Replicator {
	return &Replicator{cfg: cfg, spool: durableSpool, logger: logger}
}

func (r *Replicator) Run(ctx context.Context) error {
	delay := time.Second
	for {
		err := r.runOnce(ctx)
		if ctx.Err() != nil {
			return nil
		}
		if isFatalCaptureError(err) {
			return err
		}
		jitter := time.Duration(rand.Int64N(int64(delay))) - delay/2
		wait := delay + jitter
		r.logger.Warn("replication connection interrupted; reconnecting",
			"error", safePGError(err), "retry_in", wait)
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
		}
		if delay < 30*time.Second {
			delay *= 2
			if delay > 30*time.Second {
				delay = 30 * time.Second
			}
		}
	}
}

func (r *Replicator) runOnce(ctx context.Context) error {
	dsn, err := r.cfg.PostgreSQLDSN()
	if err != nil {
		return err
	}
	startLSN, err := r.startLSN(ctx, dsn)
	if err != nil {
		return err
	}
	connection, err := connectReplication(ctx, dsn)
	if err != nil {
		return err
	}
	defer connection.Close(context.Background())

	options := pglogrepl.StartReplicationOptions{
		Mode: pglogrepl.LogicalReplication,
		PluginArgs: []string{
			"proto_version '1'",
			fmt.Sprintf("publication_names '%s'", r.cfg.Postgres.Publication),
			"messages 'true'",
		},
	}
	if err := pglogrepl.StartReplication(ctx, connection, r.cfg.Postgres.Slot, startLSN, options); err != nil {
		return fmt.Errorf("start logical replication: %w", err)
	}
	r.logger.Info("logical replication started",
		"slot", r.cfg.Postgres.Slot, "publication", r.cfg.Postgres.Publication,
		"start_lsn", startLSN.String())

	durableLSN, err := r.spool.LastDurableLSN(ctx)
	if err != nil {
		return err
	}
	if durableLSN == 0 {
		durableLSN = startLSN
	}
	state := NewStateMachine(
		r.cfg.Postgres.MessagePrefix, r.cfg.Spool.MaxEventBytes,
		r.cfg.Postgres.MaxTransactionEvents, r.cfg.Postgres.MaxTransactionBytes,
		r.logger,
	)

	for {
		receiveCtx, cancel := context.WithTimeout(ctx, r.cfg.Postgres.StatusInterval)
		message, receiveErr := connection.ReceiveMessage(receiveCtx)
		cancel()
		if receiveErr != nil {
			if ctx.Err() != nil {
				state.Reset()
				return nil
			}
			if pgconn.Timeout(receiveErr) {
				if err := sendStandbyStatus(ctx, connection, durableLSN); err != nil {
					state.Reset()
					return err
				}
				continue
			}
			state.Reset()
			return fmt.Errorf("receive replication message: %w", receiveErr)
		}
		copyData, ok := message.(*pgproto3.CopyData)
		if !ok || len(copyData.Data) == 0 {
			continue
		}

		switch copyData.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			keepalive, err := pglogrepl.ParsePrimaryKeepaliveMessage(copyData.Data[1:])
			if err != nil {
				return fmt.Errorf("parse primary keepalive: %w", err)
			}
			if keepalive.ReplyRequested {
				// ServerWALEnd is deliberately ignored for acknowledgment.
				if err := sendStandbyStatus(ctx, connection, durableLSN); err != nil {
					return err
				}
			}

		case pglogrepl.XLogDataByteID:
			xlogData, err := pglogrepl.ParseXLogData(copyData.Data[1:])
			if err != nil {
				return fmt.Errorf("parse XLogData: %w", err)
			}
			decoded, err := DecodeWALData(xlogData.WALData)
			if err != nil {
				return err
			}
			batch, err := state.Consume(decoded)
			if err != nil {
				return err
			}
			if batch == nil {
				continue
			}
			result, err := persistThenAcknowledge(ctx, r.spool, *batch, func(ackLSN pglogrepl.LSN) error {
				return sendStandbyStatus(ctx, connection, ackLSN)
			})
			if err != nil {
				return err
			}
			durableLSN = result.DurableLSN
			r.logger.Info("committed transaction captured",
				"transaction_id", batch.TransactionID,
				"events", len(batch.Events),
				"inserted", result.Inserted,
				"replayed", result.Replayed,
				"durable_lsn", durableLSN.String())
		}
	}
}

func (r *Replicator) startLSN(ctx context.Context, dsn string) (pglogrepl.LSN, error) {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return 0, redactConnectionError(err)
	}
	defer conn.Close(ctx)
	slot, err := readSlot(ctx, conn, r.cfg.Postgres.Slot)
	if err != nil {
		return 0, err
	}
	if !slot.Exists {
		if !r.cfg.Postgres.CreateSlotIfMissing {
			return 0, fmt.Errorf("replication slot %q does not exist; run setup --create-slot", r.cfg.Postgres.Slot)
		}
		if err := conn.Close(ctx); err != nil {
			return 0, err
		}
		if err := createReplicationSlot(ctx, dsn, r.cfg.Postgres.Slot); err != nil {
			return 0, err
		}
		conn, err = pgx.Connect(ctx, dsn)
		if err != nil {
			return 0, redactConnectionError(err)
		}
		defer conn.Close(ctx)
		slot, err = readSlot(ctx, conn, r.cfg.Postgres.Slot)
		if err != nil {
			return 0, err
		}
	}
	var database string
	var version int
	if err := conn.QueryRow(ctx, `
		SELECT current_database(), current_setting('server_version_num')::integer
	`).Scan(&database, &version); err != nil {
		return 0, fmt.Errorf("read current database/version: %w", err)
	}
	if err := validateServerVersion(version); err != nil {
		return 0, err
	}
	if err := validateSlot(slot, database); err != nil {
		return 0, err
	}
	local, err := r.spool.LastDurableLSN(ctx)
	if err != nil {
		return 0, err
	}
	if local != 0 {
		return local, nil
	}
	if slot.ConfirmedFlushLSN != 0 {
		return slot.ConfirmedFlushLSN, nil
	}
	if slot.RestartLSN != 0 {
		return slot.RestartLSN, nil
	}
	return 0, errors.New("replication slot has no usable start LSN")
}

func sendStandbyStatus(ctx context.Context, conn *pgconn.PgConn, durableLSN pglogrepl.LSN) error {
	status := standbyStatus(durableLSN)
	if err := pglogrepl.SendStandbyStatusUpdate(ctx, conn, status); err != nil {
		return fmt.Errorf("send standby status at durable LSN %s: %w", durableLSN, err)
	}
	return nil
}

func standbyStatus(durableLSN pglogrepl.LSN) pglogrepl.StandbyStatusUpdate {
	return pglogrepl.StandbyStatusUpdate{
		WALWritePosition: durableLSN,
		WALFlushPosition: durableLSN,
		WALApplyPosition: durableLSN,
		ClientTime:       time.Now(),
	}
}

func persistThenAcknowledge(
	ctx context.Context,
	durableSpool spool.Spool,
	batch spool.CommittedBatch,
	acknowledge func(pglogrepl.LSN) error,
) (spool.PersistResult, error) {
	result, err := durableSpool.PersistCommittedBatch(ctx, batch)
	if err != nil {
		return result, err
	}
	if result.DurableLSN < batch.CommitEndLSN {
		return result, fmt.Errorf("%w: spool returned checkpoint %s before transaction end %s",
			spool.ErrDurability, result.DurableLSN, batch.CommitEndLSN)
	}
	if err := acknowledge(result.DurableLSN); err != nil {
		// SQLite is already committed. Reconnect/replay is safe because identity
		// plus payload digest makes persistence idempotent.
		return result, err
	}
	return result, nil
}

func isFatalCaptureError(err error) bool {
	return errors.Is(err, ErrProtocolState) ||
		errors.Is(err, ErrProtectedEvent) ||
		errors.Is(err, ErrTransactionLimit) ||
		errors.Is(err, ErrSlotMismatch) ||
		errors.Is(err, ErrUnsupportedVersion) ||
		errors.Is(err, spool.ErrIdentityConflict) ||
		errors.Is(err, spool.ErrDurability)
}
