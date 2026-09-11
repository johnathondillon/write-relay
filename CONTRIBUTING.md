# Contributing

WriteRelay's reliability claims depend on reviewable ordering and durability
invariants. Read `docs/architecture.md` and `docs/correctness.md` before changing
the capture or delivery paths.

For local checks:

```bash
make check
make failure
make vuln
make integration
```

CI additionally runs the integration suite against PostgreSQL 14–18. Run that
same matrix locally with `make integration-matrix`, or select one version with
`POSTGRES_VERSION=14 make integration-version` (default: 18). These commands
start disposable Docker Compose databases on automatically assigned loopback
ports, verify the server major, and clean up their own containers and anonymous
volumes on exit. They leave the persistent development and LMS databases alone.
Each test logs the exact server version for compatibility evidence.

CI also runs a bounded load/outage/restart scenario. Use `make load` for the
default 10,000-event workload, or `LOAD_EVENTS=2000 make load` for the CI-sized
run. See [load testing](docs/load-testing.md) for configuration and report
definitions. The harness uses the `load` build tag and disposable Docker
resources; performance measurements are informational, while correctness and
timeouts determine whether the test passes.

Unit tests must not require Docker. Integration tests use the `integration`
build tag, temporary spool paths, unique slots/publications, bounded polling,
and deterministic event identities where practical.

Changes that affect WAL acknowledgment, transaction buffering, identity,
payload bytes, SQLite durability, delivery ordering, retry classification, or
terminal state require an ADR update and focused failure tests. Do not introduce
additional production sinks, table CDC, or protocol versions 2–4 as part of an
unrelated change.

Failure hooks must remain code-injected and inert by default. Do not add a
production YAML field, environment switch, signal, endpoint, or CLI option that
can activate a process crash.

Before opening a contribution:

1. Run formatting, module verification, build, tests, vet, and `govulncheck`.
2. Keep README examples and architecture documents synchronized.
3. Avoid logging event payloads, passwords, or full DSNs.
4. State which PostgreSQL versions were actually exercised.
5. Never describe at-least-once behavior as exactly once.
