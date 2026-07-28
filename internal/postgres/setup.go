package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/johnathondillon/write-relay/internal/config"
	install "github.com/johnathondillon/write-relay/sql/postgres"
)

type SlotInfo struct {
	Name              string
	Plugin            string
	Database          string
	Active            bool
	ConfirmedFlushLSN pglogrepl.LSN
	RestartLSN        pglogrepl.LSN
	Exists            bool
}

type SetupResult struct {
	InstalledFunction  bool
	CreatedPublication bool
	CreatedSlot        bool
}

func Setup(ctx context.Context, cfg config.Config, createSlot bool) (SetupResult, error) {
	var result SetupResult
	dsn, err := cfg.PostgreSQLDSN()
	if err != nil {
		return result, err
	}
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return result, redactConnectionError(err)
	}
	defer conn.Close(ctx)

	var database string
	var version int
	if err := conn.QueryRow(ctx, `
		SELECT current_database(), current_setting('server_version_num')::integer
	`).Scan(&database, &version); err != nil {
		return result, fmt.Errorf("read current database/version: %w", err)
	}
	if err := validateServerVersion(version); err != nil {
		return result, err
	}

	var functionExists bool
	if err := conn.QueryRow(ctx, `
		SELECT to_regprocedure('writerelay.emit(jsonb)') IS NOT NULL
	`).Scan(&functionExists); err != nil {
		return result, fmt.Errorf("inspect emit function: %w", err)
	}
	if !functionExists {
		// Installation is deferred until all existing publication and slot
		// objects have passed the read-only preflight below.
	}

	var publicationExists bool
	if err := conn.QueryRow(ctx, `
		SELECT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = $1)
	`, cfg.Postgres.Publication).Scan(&publicationExists); err != nil {
		return result, fmt.Errorf("inspect publication: %w", err)
	}
	if !publicationExists {
		// Creation is deferred until the preflight is complete.
	} else {
		var publicationTables int
		if err := conn.QueryRow(ctx, `
			SELECT count(*) FROM pg_publication_tables WHERE pubname = $1
		`, cfg.Postgres.Publication).Scan(&publicationTables); err != nil {
			return result, fmt.Errorf("inspect publication tables: %w", err)
		}
		if publicationTables != 0 {
			return result, fmt.Errorf("%w: publication %q contains %d table(s), expected empty",
				ErrSlotMismatch, cfg.Postgres.Publication, publicationTables)
		}
	}

	slot, err := readSlot(ctx, conn, cfg.Postgres.Slot)
	if err != nil {
		return result, err
	}
	if slot.Exists {
		if err := validateSlot(slot, database); err != nil {
			return result, err
		}
	} else if !createSlot {
		return result, fmt.Errorf("replication slot %q is missing; rerun setup with --create-slot", cfg.Postgres.Slot)
	}

	// All validation is complete. Mutations begin here.
	if !functionExists {
		if _, err := conn.Exec(ctx, install.InstallSQL); err != nil {
			return result, fmt.Errorf("install SQL API (administrator privileges may be required): %w", err)
		}
		result.InstalledFunction = true
	}
	if !publicationExists {
		if _, err := conn.Exec(ctx, "CREATE PUBLICATION "+cfg.Postgres.Publication); err != nil {
			return result, fmt.Errorf("create empty publication: %w", err)
		}
		result.CreatedPublication = true
	}
	if slot.Exists {
		return result, nil
	}
	if err := conn.Close(ctx); err != nil {
		return result, fmt.Errorf("close setup SQL connection: %w", err)
	}
	if err := createReplicationSlot(ctx, dsn, cfg.Postgres.Slot); err != nil {
		return result, err
	}
	result.CreatedSlot = true
	return result, nil
}

func readSlot(ctx context.Context, conn *pgx.Conn, name string) (SlotInfo, error) {
	var result SlotInfo
	var confirmed, restart *string
	err := conn.QueryRow(ctx, `
		SELECT slot_name, plugin, database, active, confirmed_flush_lsn::text, restart_lsn::text
		FROM pg_replication_slots
		WHERE slot_name = $1
	`, name).Scan(&result.Name, &result.Plugin, &result.Database, &result.Active, &confirmed, &restart)
	if errors.Is(err, pgx.ErrNoRows) {
		return result, nil
	}
	if err != nil {
		return result, fmt.Errorf("inspect replication slot: %w", err)
	}
	result.Exists = true
	if confirmed != nil {
		result.ConfirmedFlushLSN, err = pglogrepl.ParseLSN(*confirmed)
		if err != nil {
			return result, fmt.Errorf("parse slot confirmed_flush_lsn: %w", err)
		}
	}
	if restart != nil {
		result.RestartLSN, err = pglogrepl.ParseLSN(*restart)
		if err != nil {
			return result, fmt.Errorf("parse slot restart_lsn: %w", err)
		}
	}
	return result, nil
}

func validateSlot(slot SlotInfo, database string) error {
	if slot.Plugin != "pgoutput" {
		return fmt.Errorf("%w: slot %q uses plugin %q, expected pgoutput", ErrSlotMismatch, slot.Name, slot.Plugin)
	}
	if slot.Database != database {
		return fmt.Errorf("%w: slot %q belongs to database %q, connected to %q",
			ErrSlotMismatch, slot.Name, slot.Database, database)
	}
	if slot.Active {
		return fmt.Errorf("%w: slot %q is active in another session", ErrSlotMismatch, slot.Name)
	}
	return nil
}

func validateServerVersion(version int) error {
	if version < 140000 || version >= 190000 {
		return fmt.Errorf("%w: server_version_num=%d, supported range is PostgreSQL 14 through 18",
			ErrUnsupportedVersion, version)
	}
	return nil
}

func createReplicationSlot(ctx context.Context, dsn, slot string) error {
	conn, err := connectReplication(ctx, dsn)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)
	_, err = pglogrepl.CreateReplicationSlot(ctx, conn, slot, "pgoutput", pglogrepl.CreateReplicationSlotOptions{
		Mode:           pglogrepl.LogicalReplication,
		SnapshotAction: "NOEXPORT_SNAPSHOT",
	})
	if err != nil {
		return fmt.Errorf("create pgoutput replication slot %q: %w", slot, err)
	}
	return nil
}

func connectReplication(ctx context.Context, dsn string) (*pgconn.PgConn, error) {
	pgConfig, err := pgconn.ParseConfig(dsn)
	if err != nil {
		return nil, errors.New("parse PostgreSQL connection configuration")
	}
	if pgConfig.RuntimeParams == nil {
		pgConfig.RuntimeParams = make(map[string]string)
	}
	pgConfig.RuntimeParams["replication"] = "database"
	conn, err := pgconn.ConnectConfig(ctx, pgConfig)
	if err != nil {
		return nil, redactConnectionError(err)
	}
	return conn, nil
}

func redactConnectionError(err error) error {
	if err == nil {
		return nil
	}
	// pgx can include host and user for diagnostics, but callers should never
	// receive a nested error that might reproduce a password-bearing DSN.
	return fmt.Errorf("PostgreSQL connection failed: %s", safePGError(err))
}

func safePGError(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return fmt.Sprintf("%s (SQLSTATE %s)", pgErr.Message, pgErr.Code)
	}
	return "connection unavailable (details redacted)"
}
