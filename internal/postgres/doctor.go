package postgres

import (
	"context"
	"fmt"
	"strconv"

	"github.com/jackc/pgx/v5"
	"github.com/johnathondillon/write-relay/internal/config"
)

type Check struct {
	Name   string
	Status string
	Detail string
}

func Doctor(ctx context.Context, cfg config.Config) ([]Check, error) {
	dsn, err := cfg.PostgreSQLDSN()
	if err != nil {
		return nil, err
	}
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return []Check{{Name: "database connectivity", Status: "fail", Detail: safePGError(err)}}, nil
	}
	defer conn.Close(ctx)

	checks := []Check{{Name: "database connectivity", Status: "pass", Detail: "connected"}}
	var versionText, walLevel, maxSlots, maxSenders string
	if err := conn.QueryRow(ctx, `
		SELECT current_setting('server_version_num'),
		       current_setting('wal_level'),
		       current_setting('max_replication_slots'),
		       current_setting('max_wal_senders')
	`).Scan(&versionText, &walLevel, &maxSlots, &maxSenders); err != nil {
		return checks, fmt.Errorf("read PostgreSQL settings: %w", err)
	}
	version, _ := strconv.Atoi(versionText)
	versionStatus := "pass"
	if version < 140000 || version >= 190000 {
		versionStatus = "fail"
	}
	checks = append(checks,
		Check{Name: "PostgreSQL version", Status: versionStatus, Detail: versionText},
		checkValue("wal_level", walLevel == "logical", walLevel),
		checkPositive("max_replication_slots", maxSlots),
		checkPositive("max_wal_senders", maxSenders),
	)

	var replication, superuser bool
	if err := conn.QueryRow(ctx, `
		SELECT rolreplication, rolsuper FROM pg_roles WHERE rolname = current_user
	`).Scan(&replication, &superuser); err != nil {
		return checks, fmt.Errorf("inspect current role: %w", err)
	}
	checks = append(checks, checkValue(
		"replication privilege", replication || superuser,
		fmt.Sprintf("replication=%t superuser=%t", replication, superuser),
	))

	var functionExists bool
	if err := conn.QueryRow(ctx, `
		SELECT to_regprocedure('writerelay.emit(jsonb)') IS NOT NULL
	`).Scan(&functionExists); err != nil {
		return checks, fmt.Errorf("inspect emit function: %w", err)
	}
	checks = append(checks, checkValue("SQL emit function", functionExists, "writerelay.emit(jsonb)"))

	var publicationExists bool
	var publicationTables int
	if err := conn.QueryRow(ctx, `
		SELECT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = $1),
		       (SELECT count(*) FROM pg_publication_tables WHERE pubname = $1)
	`, cfg.Postgres.Publication).Scan(&publicationExists, &publicationTables); err != nil {
		return checks, fmt.Errorf("inspect publication: %w", err)
	}
	checks = append(checks,
		checkValue("publication exists", publicationExists, cfg.Postgres.Publication),
		checkValue("publication is empty", publicationExists && publicationTables == 0,
			fmt.Sprintf("%d table(s)", publicationTables)),
	)

	slot, err := readSlot(ctx, conn, cfg.Postgres.Slot)
	if err != nil {
		return checks, err
	}
	checks = append(checks, checkValue("replication slot exists", slot.Exists, cfg.Postgres.Slot))
	if slot.Exists {
		checks = append(checks,
			checkValue("slot plugin", slot.Plugin == "pgoutput", slot.Plugin),
			checkValue("slot database", slot.Database == conn.Config().Database, slot.Database),
			checkValue("slot inactive", !slot.Active, fmt.Sprintf("active=%t", slot.Active)),
			Check{Name: "slot confirmed flush LSN", Status: "pass", Detail: slot.ConfirmedFlushLSN.String()},
		)
		var retained string
		if err := conn.QueryRow(ctx, `
			SELECT pg_size_pretty(
				COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn), 0)
			) FROM pg_replication_slots WHERE slot_name = $1
		`, cfg.Postgres.Slot).Scan(&retained); err == nil {
			checks = append(checks, Check{Name: "slot retained WAL", Status: "pass", Detail: retained})
		}
	}
	checks = append(checks, Check{
		Name: "message-size configuration", Status: "pass",
		Detail: fmt.Sprintf("%d bytes", cfg.Spool.MaxEventBytes),
	})
	return checks, nil
}

func checkValue(name string, ok bool, detail string) Check {
	status := "fail"
	if ok {
		status = "pass"
	}
	return Check{Name: name, Status: status, Detail: detail}
}

func checkPositive(name, value string) Check {
	parsed, _ := strconv.Atoi(value)
	return checkValue(name, parsed > 0, value)
}
