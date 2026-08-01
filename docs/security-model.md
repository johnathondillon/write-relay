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
The stdout sink also prints complete payloads and is development-only.

The SQLite spool must be protected like application data. The implementation
creates parent directories with mode `0700`, applies mode `0600` to the database,
and rejects an existing symlink at the database path. Backup, volume encryption,
host access controls, and secure deletion remain operator responsibilities.

Delivery history retains destination status, bounded safe error categories, and
event identity, but never response bodies, authorization values, signing
secrets, or webhook URLs. Dead-letter records remain until a future explicit
retention policy is implemented.

## Configuration and secrets

Use `dsn_env` so configuration files do not contain passwords. Resolved
environment values are never logged or printed. Connection errors are reduced
to a safe PostgreSQL message/SQLSTATE or a generic redacted failure.

Webhook authorization and HMAC signing secrets are read from named environment
variables at startup. The configured URL must not contain user information.
HTTPS is required unless `allow_insecure_http` is explicitly enabled for local
development. Redirects are disabled so credentials cannot be forwarded to a
different origin. Network errors are reduced to bounded categories and response
bodies are discarded rather than persisted or logged.

Webhook targets are operator-controlled network destinations, so deploying
WriteRelay with untrusted configuration would create an SSRF capability.
Configuration files and environment variables must be restricted to trusted
operators. Egress policy should constrain destinations in sensitive networks.

The setup command interpolates only slot/publication identifiers that passed the
strict lowercase PostgreSQL identifier rule. It never interpolates a secret and
never drops or recreates an existing slot.

## Denial-of-service controls

Both SQL and Go enforce the 256 KiB event limit. The daemon additionally bounds
accepted events and bytes buffered per transaction. A violation stops capture
without acknowledgment instead of silently discarding content. SQLite has a
bounded busy timeout and a single writer connection.

Webhook requests have a bounded timeout, redirects are disabled, response bodies
are read only to a small bound, retries have bounded delay and attempt count,
and stored error text is capped. The current implementation still has no
spool-size or retention limit. Operators must monitor disk and PostgreSQL
retained WAL. A future limit must fail closed.

## Failure-test isolation

Crash boundaries are ordinary function hooks whose zero value is inert.
Production composition does not expose them through YAML, environment variables,
signals, HTTP endpoints, or CLI flags. Environment variables used by the
subprocess harness are referenced only from `_test.go` files and therefore are
not compiled into `writerelayd`.

This prevents a diagnostic crash feature from becoming a production
denial-of-service control surface. Adding any runtime-accessible failpoint would
require a separate security review and is not part of Milestone 3.

## Supply chain

Dependencies are intentionally limited to pgx/pglogrepl, strict YAML decoding,
and the CGO-free SQLite driver. Go module checksums are committed. CI actions
are pinned to immutable commit SHAs, and CI runs the pinned Go vulnerability
scanner against reachable code. The minimum Go patch release is updated when
standard-library security fixes require it. SBOM generation remains roadmap
work.
