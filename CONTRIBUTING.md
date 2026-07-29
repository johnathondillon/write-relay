# Contributing

WriteRelay's reliability claims depend on reviewable ordering and durability
invariants. Read `docs/architecture.md` and `docs/correctness.md` before changing
the capture or delivery paths.

For local checks:

```bash
make check
make vuln
make integration
```

Unit tests must not require Docker. Integration tests use the `integration`
build tag, temporary spool paths, unique slots/publications, bounded polling,
and deterministic event identities where practical.

Changes that affect WAL acknowledgment, transaction buffering, identity,
payload bytes, SQLite durability, delivery ordering, retry classification, or
terminal state require an ADR update and focused failure tests. Do not introduce
additional production sinks, table CDC, or protocol versions 2–4 as part of an
unrelated change.

Before opening a contribution:

1. Run formatting, module verification, build, tests, vet, and `govulncheck`.
2. Keep README examples and architecture documents synchronized.
3. Avoid logging event payloads, passwords, or full DSNs.
4. State which PostgreSQL versions were actually exercised.
5. Never describe at-least-once behavior as exactly once.
