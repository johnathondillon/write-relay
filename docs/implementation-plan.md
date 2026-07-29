# Implementation plan

Last synchronized: 2026-07-27.

## Current execution target

Prove that every captured event receives a durable per-sink delivery record,
that one ordered worker can deliver the event to an HTTP webhook, and that
retry, dead-letter, replay, and shutdown behavior preserve an honest
at-least-once contract.

## Milestone 0 — repository scaffold

- [x] Go 1.26.5 module and repository layout.
- [x] Strict YAML configuration, identifier/bounds validation, and tests.
- [x] Standard-library command routing and structured `log/slog` configuration.
- [x] Apache-2.0 licensing, contribution/security policies, README, architecture,
  correctness, security, plan, and ADRs.
- [x] Makefile, editor settings, ignore rules, CI, multi-stage Dockerfile, and
  PostgreSQL 18 Compose service with logical replication settings.
- [x] Immutable CI action pins, reachable-code vulnerability scanning, and a
  loopback-only development database port.
- [x] Unit suite established before Milestone 1 implementation.
- [x] Public project name selected as `WriteRelay`, repository name selected as
  `write-relay`, and Go module path set to
  `github.com/johnathondillon/write-relay`.

## Milestone 1 — transactional capture

- [x] Idempotent SQL installation, development roles/publication/table, and
  committed/rollback examples.
- [x] Raw-byte-preserving CloudEvents-style envelope validation.
- [x] Protocol-v1 decoder and explicit Begin/Message/Commit state machine.
- [x] Protected-prefix, transaction-count, and transaction-byte failure bounds.
- [x] Embedded SQLite migration, FULL durability pragmas, atomic batch/checkpoint,
  replay verification, conflict error, reopen tests, and inspection command.
- [x] Replication-mode connection, validated `pgoutput` arguments, durable start
  LSN selection, durable-only status updates, keepalives, reconnect backoff, and
  graceful context cancellation.
- [x] Non-mutating doctor checks where possible and explicit setup mutation.
- [x] Docker-tagged end-to-end harness for commit, rollback, order, ACK checkpoint,
  and shutdown.

## Milestone 2 — ordered delivery

- [x] Versioned SQLite migration for durable sinks and per-event delivery state.
- [x] Atomic delivery-record creation for new events and safe backfill when a
  sink is first configured.
- [x] One worker that preserves event order independently for every sink.
- [x] Development stdout sink and HTTP webhook sink with stable idempotency keys,
  bounded requests,
  optional authorization/signing secrets, and redirects disabled.
- [x] Bounded exponential retry and `Retry-After` handling for transient failures
  plus retained dead-letter state for permanent or exhausted failures.
- [x] Explicit inspection and redrive commands for operators.
- [x] Crash/replay, ordering, retry, dead-letter, configuration, and graceful
  shutdown tests.
- [x] Runtime, doctor, examples, architecture, correctness, security, and public
  documentation synchronized with delivery behavior.

## Verification record

This section records only commands actually executed in this workspace.

- [x] The complete pre-release rename to `WriteRelay`, repository
  `write-relay`, module `github.com/johnathondillon/write-relay`, binary
  `writerelayd`, SQL API `writerelay.emit(jsonb)`, and protocol prefix
  `writerelay.v1` was applied across code, configuration, examples, tests,
  Docker assets, and documentation.
- [x] `make check`: formatting check, `go build ./...`, `go test ./...`,
  `go mod verify`, and `go vet ./...` passed with Go 1.26.5.
- [x] `make race` passed with Go 1.26.5.
- [x] `make vuln` passed with `govulncheck` v1.6.0 after upgrading
  `golang.org/x/text` to v0.39.0; no reachable vulnerabilities were found.
- [x] CI actions were resolved to immutable commit SHAs and the workflow now
  runs formatting, module integrity, build, unit, vet, race, and vulnerability
  checks.
- [x] `docker compose up -d --wait postgres` started healthy PostgreSQL 18.4
  with logical replication enabled.
- [x] `go test -tags=integration -count=1 -v ./tests/integration/...`
  passed against PostgreSQL 18.4, including committed webhook delivery, rollback
  absence at the destination, transient `503` retry, durable success state, and
  two-component graceful shutdown.
- [x] The current idempotent SQL installation ran, rejected a non-object,
  non-string identity, and oversized payload, and left an empty publication.
- [x] A transaction containing business SQL plus an event committed and reached
  SQLite with payload and transaction metadata.
- [x] Rolled-back business SQL and event were absent after a later committed
  marker was captured.
- [x] Two events in one transaction retained accepted-message order and shared
  transaction/commit metadata.
- [x] The SQLite durable transaction-end checkpoint was observed at the
  replication slot, and focused tests proved persist-before-ACK/no-ACK-on-error.
- [x] Identical replay, conflicting identity, atomic rollback, zero-event
  checkpoint, reopen, and graceful shutdown tests passed.
- [x] Schema migration from version 1 to version 2, sink backfill, atomic
  delivery creation, durable target conflict, per-sink ordering, independent
  sink progress, retry/dead-letter, redrive, stdout, webhook identity,
  authorization, signature, redirect, timeout, and crash-window tests passed.
- [x] `spool deliveries` inspection and explicit `spool redrive` were exercised
  in the CLI unit suite.
- [x] README `setup`, `doctor`, committed SQL, rollback SQL, and `spool list`
  commands were executed successfully with the documented development roles.
- [x] The multi-stage Docker image built and its non-root runtime executed the
  default version command after Milestone 2.

## Deliberately deferred

- PostgreSQL 14–17 CI matrix and managed-service compatibility research.
- Deterministic process-crash failpoints beyond focused replay tests.
- Event deletion/retention, UI, non-webhook sinks, other databases, protocol
  versions 2–4, two-phase commit, and transaction streaming.
- SBOM generation, metrics, and spool size policy.

## Next milestone

Milestone 3 is deterministic process-level failure injection around capture,
request, destination acceptance, and local success recording. Explicit
retention and spool-size policy remain later work after those recovery paths are
proven.
