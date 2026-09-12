# Implementation plan

Last synchronized: 2026-09-10.

## Current execution target

Exercise a larger committed workload through an HTTP receiver outage, forced
daemon restart, and backlog recovery. Verify identities, payloads, order, and
receiver deduplication while recording performance and spool-size observations.

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

## Milestone 3 — deterministic failure injection

- [x] Inert, code-injected test hooks with no environment-triggered production
  failpoint controls.
- [x] Child-process termination before a SQLite batch transaction and midway
  through event insertion proves no partial batch or checkpoint survives.
- [x] Termination after SQLite commit but before PostgreSQL acknowledgment proves
  durable replay without duplicate event or delivery rows.
- [x] Termination immediately after acknowledgment proves the local checkpoint
  and acknowledged boundary agree.
- [x] Termination before a sink request proves no destination call and a pending
  delivery on restart.
- [x] Termination while a request is in flight proves retry after an ambiguous
  destination outcome.
- [x] Termination after destination acceptance but before local success proves a
  duplicate request with the same idempotency key.
- [x] Recovery tests verify ordering, attempts, terminal state, and spool
  integrity after reopening.
- [x] Runtime, architecture, correctness, security, specification, and public
  documentation synchronized with the tested crash model.

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
- [x] `make failure` passed deterministic child-process termination before and
  during SQLite commit, around PostgreSQL acknowledgment, before and during a
  webhook request, and after destination success.
- [x] Crash recovery reopened the same SQLite files and proved atomic rollback,
  durable replay, checkpoint/ACK agreement, stable idempotency keys, retained
  pending attempts, and per-sink ordering.
- [x] `make check`, `make race`, and `make vuln` passed after Milestone 3; the
  vulnerability scanner reported no reachable vulnerabilities.
- [x] The PostgreSQL 18 integration suite passed after hook injection, proving
  the zero-value production path did not alter capture or delivery behavior.
- [x] The Milestone 3 multi-stage Docker image built and its non-root runtime
  executed the default version command.

## Deliberately deferred

- Managed-service compatibility research.
- Event/identity deletion, automatic retention, UI, non-webhook sinks, other databases, protocol
  versions 2–4, two-phase commit, and transaction streaming.
- SBOM generation and spool size policy.

## Milestone 4 progress

- [x] Initial TypeScript/Node producer SDK with envelope validation,
  caller-controlled event identity, and emission on an existing transaction client.
- [x] Nuxt example uses the local SDK; CI exercises commit, rollback, producer
  retries, receiver outages, and duplicate delivery through it.
- [x] SDK unit tests, declaration checks, and standalone tarball installation.
- [x] Go producer SDK for pgx v5 and `database/sql` transactions, explicit JSON
  data, envelope validation, and a runnable local producer example.
- [x] Go SDK unit tests and PostgreSQL integration coverage for commit,
  rollback, validation/database errors, and unchanged-identity replay.
- [x] TypeScript receiver inbox helper with atomic business writes, duplicate
  detection, content conflicts, and concurrent commit/rollback tests. The Nuxt
  certificate service uses it with its existing inbox records.
- [x] Go receiver inbox helpers for pgxpool and `database/sql`, with a runnable
  HTTP example covering rollback and lost-response retry.
- [x] Go and TypeScript receiver compatibility tests against PostgreSQL 14–18.
- [ ] C# SDK, full CloudEvents conformance coverage, and receiver helpers for
  other languages.

The TypeScript package remains unpublished. Optional capture disk-space
protection is available. Automatic retention and a hard spool-size policy remain
later work.

### TypeScript receiver verification

- On 2026-09-11, all 15 SDK tests passed, including declaration checks and a
  standalone tarball consumer using both producer and receiver exports.
- The Docker Nuxt build passed SDK tests, service type checks, and the production
  build. Five receiver integration tests passed against PostgreSQL 18.4, including
  concurrent commit/rollback, conflicting content, and a swallowed SQL error.
- The complete LMS verifier passed concurrent producer requests, rollback,
  outage recovery and monitoring, and a lost response followed by a duplicate
  delivery with one certificate. These checks ran in a disposable stack.
- `make check`, `make failure`, and `make vuln` passed. The new receiver helper
  initially covered PostgreSQL 18.4; the expanded matrix is recorded below.
- Initial CI coverage ran receiver tests before the LMS verifier; the receiver
  tests now have a separate compatibility matrix.
  See the [receiver guide](../sdk/typescript/INBOX.md) and ADR 0009 for setup and
  transaction boundaries.

### Go and TypeScript receiver compatibility

- On 2026-09-12, `make integration-matrix` and `make inbox-typescript-matrix`
  passed against PostgreSQL 14.24, 15.19, 16.15, 17.11, and 18.4.
- Go coverage includes both receiver APIs, mixed-API concurrent attempts,
  commit/rollback visibility, conflicts, swallowed SQL errors, cancellation,
  and panic cleanup. The HTTP example verifies rollback followed by success and
  a lost response followed by a duplicate with one business record.
- The TypeScript runner builds the SDK and runs its unit/package tests, then
  runs the five receiver database tests for each major. It verifies the actual
  server version and cleans up its own test containers and volumes.
- `make check`, `make race`, `make failure`, and `make vuln` passed with Go 1.26.8.
- The finalized Go integration and HTTP tests also passed with the race detector
  against PostgreSQL 18.4.
- The native Go receiver command was built and run against a separate disposable
  PostgreSQL 18.4 database. Initialization, normal delivery, duplicate handling,
  rollback/retry, and lost-response/retry passed; three orders and three receipts
  remained after the scenarios, before the test stack was removed.
- See the [Go receiver guide](../sdk/go/INBOX.md),
  [runnable example](../examples/go-receiver/README.md), and ADR 0009.

### Go SDK verification

- On 2026-09-10, `make check`, `make race`, `make failure`, and `make vuln`
  passed with Go 1.26.8. A 10-second fuzz run of raw JSON validation passed.
- `make integration-matrix` passed against PostgreSQL 14.24, 15.19, 16.15,
  17.11, and 18.4, including both Go SDK transaction APIs.
- A standalone consumer imported the SDK through the documented local module
  replacement and compiled successfully.
- The runnable Go producer's commit, rollback, marker, and unchanged-event
  replay commands passed against a disposable PostgreSQL 18.4 instance and
  real daemon. The example used the restricted development application role;
  only the two committed identities reached the spool. Test resources were
  removed afterward.
- The Go package shares the repository module and has no separate SDK release.
  See [ADR 0008](adr/0008-producer-sdk-transactions.md) and the
  [Go SDK guide](../sdk/go/README.md).

## Manual payload retention

- Schema 3 retains event identity/digest and delivery history when pruning old,
  fully delivered payloads. Pending, retrying, dead-letter, and capture-only
  payloads remain intact.
- `spool prune --before ... --dry-run` previews a bounded batch; apply serializes
  selection and updates with SQLite's write lock. Prune never advances the ACK
  checkpoint or backfills pruned payloads to new sinks.
- Focused migration, replay, cancellation, concurrency, CLI, and process-crash
  coverage plus pruned-identity replay in the PostgreSQL matrix.
- `make check`, `make race`, `make failure`, `make vuln`, and
  `make integration-matrix` passed locally on 2026-09-09, including PostgreSQL
  14.24, 15.19, 16.15, 17.11, and 18.4. No user spool was pruned during testing.

## PostgreSQL compatibility matrix

- [x] CI runs independent PostgreSQL 14, 15, 16, 17, and 18 integration jobs with
  fail-fast disabled, using official Docker images and the Go version in `go.mod`.
- [x] `make integration-version` and `make integration-matrix` use disposable
  databases, unique project names, dynamic loopback ports, and automatic cleanup.
- [x] Integration tests log and verify the server major, then exercise commit,
  rollback, transaction order, webhook retry, and durable ACK.
- [x] Graceful runtime restart reopens the same spool, preserves its checkpoint,
  catches up with offline commits, and deduplicates identical identity/content
  without changing existing event metadata or delivery state.
- [x] Local matrix on 2026-09-09 passed against PostgreSQL 14.24, 15.19, 16.15,
  17.11, and 18.4. Each disposable database was removed after its run.
- [x] `make check`, `make failure`, and `make vuln` passed after the matrix and
  restart coverage were added. Hosted CI results will be available after push.

## Live monitoring

- Optional HTTP liveness/readiness endpoints and Prometheus text metrics.
- Observed capture connection/progress, cached delivery counts and waiting age,
  payload retention counts, and SQLite file lengths.
- Background read-only sampling with deadlines and explicit failure/staleness
  behavior; no changes to durability, acknowledgment, or retry decisions.
- LMS outage walkthrough and automated checks for retry metrics with capture
  readiness, plus replication disconnect/reconnect coverage in the PG matrix.
- `make check`, `make race`, `make failure`, `make vuln`, and the PostgreSQL
  14.24/15.19/16.15/17.11/18.4 integration matrix passed on 2026-09-10.
- The Docker LMS build and behavior checks passed in a separate disposable
  stack. Stopping its PostgreSQL produced `/readyz` 503 with `/healthz` 200;
  restarting PostgreSQL restored readiness. The user's example data was untouched.

## Load and backlog recovery

- Configurable `make load` runner with disposable PostgreSQL, the real daemon,
  and an independent durable HTTP receiver; defaults to 10,000 committed events.
- Healthy baseline, 503 backlog, forced process termination, same-spool restart,
  and a lost success response that requires receiver deduplication.
- Exact producer/spool/receiver identity and payload checks, preserved order,
  checkpoint non-regression, and fully delivered terminal state.
- JSON throughput, latency, recovery time, and spool-size observations with
  documented measurement boundaries; no fixed performance assertions.
- The initial 10,000-event run exposed repeated predecessor scans in delivery
  selection and timed out after five minutes. Selection now finds each sink's
  oldest non-terminal row before checking due time, preserving ordering without
  a schema change; ADR 0005 records the decision.
- CI runs a bounded 2,000-event scenario and retains the report as an artifact.
- Local verification on 2026-09-10 passed with Go 1.26.8 and PostgreSQL 18.4:
  the default 10,000-event run recovered its 9,000-event backlog in 11.64 seconds
  and verified 10,000 unique receiver records plus a deduplicated retry. These
  are observations on macOS/arm64, not performance requirements.
- `make check`, `make race`, `make failure`, `make vuln`, and
  `make integration-version` passed. A separate race-enabled load run passed
  with 250 events, batch size 30, and zero padding, exercising partial batches.
- See [load testing](load-testing.md) for commands, options, and limitations.


## Capture disk-space protection

- Optional available-byte thresholds pause capture below a reserve and resume
  at a higher threshold. The probe targets the existing spool filesystem on
  Linux/macOS and runs before connection, periodically, and before every batch.
- Low space or probe failure disconnects capture without writing or acknowledging
  unpersisted work. The independent delivery worker can continue; recovery replays
  from the existing SQLite checkpoint. Default configurations remain disabled.
- Readiness and metrics report pause reasons, sample freshness, available space,
  and thresholds. The runbook documents reserve sizing, PostgreSQL WAL monitoring,
  slot-loss risk, and the distinction from payload pruning or a hard spool cap.
- Unit tests cover thresholds, probe failures, forced admission checks, unchanged
  ACK ordering, stale monitoring, and cancellation. A process-crash test verifies
  no paused batch/checkpoint/ACK survives and later replay commits safely.
- On 2026-09-12, `make check`, `make race`, `make failure`, and `make vuln`
  passed with Go 1.26.8. The PostgreSQL 14.24/15.19/16.15/17.11/18.4 matrix passed
  pause/ACK stability, ongoing delivery, low-space restart, hysteresis, ordered
  replay, rollback absence, and measurement-error recovery.
- The race-enabled PostgreSQL 18.4 suite passed. A disposable non-root Linux
  container passed real filesystem probing, low-space startup, health/readiness
  and metric checks, and clean shutdown without filling the host disk.
- The existing 2,000-event load/outage/restart scenario passed with protection
  disabled, confirming the default capture path still handles backlog recovery.
- See [ADR 0010](adr/0010-disk-space-capture-admission.md) and the
  [disk-space guide](disk-space.md).
