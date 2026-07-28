# Security model

## Database roles

The generic install creates no roles or passwords. An application role needs
business-table privileges plus `USAGE` on schema `writerelay` and explicit
`EXECUTE` on `writerelay.emit(jsonb)`. Public execution is revoked. The
function is `SECURITY INVOKER` with a fixed `pg_catalog` search path.

The daemon role needs `LOGIN`, `REPLICATION`, database connectivity, and access
to inspect the publication, slot, and function (including schema `USAGE` to
resolve the function name). It should not receive function `EXECUTE` or business
table access and should not be a superuser.
Compose credentials and its broader setup privileges are development-only. The
published Compose port binds to loopback so the fixed development passwords are
not exposed on other host interfaces.

## Event content and spool

Payloads may contain regulated or sensitive business data. Normal logs include
transaction IDs, event counts, types/IDs when needed, and LSNs—not payloads.
`spool list` is an explicit local administrative action and does print payloads.

The SQLite spool must be protected like application data. The implementation
creates parent directories with mode `0700`, applies mode `0600` to the database,
and rejects an existing symlink at the database path. Backup, volume encryption,
host access controls, and secure deletion remain operator responsibilities.

## Configuration and secrets

Use `dsn_env` so configuration files do not contain passwords. Resolved
environment values are never logged or printed. Connection errors are reduced
to a safe PostgreSQL message/SQLSTATE or a generic redacted failure.

The setup command interpolates only slot/publication identifiers that passed the
strict lowercase PostgreSQL identifier rule. It never interpolates a secret and
never drops or recreates an existing slot.

## Denial-of-service controls

Both SQL and Go enforce the 256 KiB event limit. The daemon additionally bounds
accepted events and bytes buffered per transaction. A violation stops capture
without acknowledgment instead of silently discarding content. SQLite has a
bounded busy timeout and a single writer connection.

Milestone 1 has no spool-size limit because there is no delivery/retention state
machine. Operators must monitor disk and PostgreSQL retained WAL. A future limit
must fail closed.

## Supply chain

Dependencies are intentionally limited to pgx/pglogrepl, strict YAML decoding,
and the CGO-free SQLite driver. Go module checksums are committed. CI actions
are pinned to immutable commit SHAs, and CI runs the pinned Go vulnerability
scanner against reachable code. The minimum Go patch release is updated when
standard-library security fixes require it. SBOM generation remains roadmap
work.
