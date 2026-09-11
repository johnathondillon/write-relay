# Contributing

WriteRelay's reliability claims depend on reviewable ordering and durability
invariants. Read `docs/architecture.md` and `docs/correctness.md` before changing
the capture or delivery paths.

## Branches and pull requests

Create a branch from an up-to-date `main` for each focused change. Use
`type/short-description`, with lowercase words separated by hyphens:

```text
feat/health-monitoring
fix/postgres-startup-race
docs/contribution-conventions
```

Use `type(scope): description` for PR titles, following
[Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/).
The scope is optional; when useful, name the affected area, such as `spool`,
`monitoring`, `sdk`, or `example`. Describe the resulting change with a short
action phrase:

```text
feat(monitoring): add health checks and metrics
fix(example): wait for PostgreSQL TCP readiness
docs: document contribution conventions
```

| Type | Use for |
| --- | --- |
| `feat` | New functionality |
| `fix` | Bug fixes |
| `docs` | Documentation changes |
| `test` | Test additions or corrections |
| `refactor` | Code restructuring that preserves behavior |
| `ci` | CI workflow changes |
| `chore` | Dependency updates and other maintenance |

Mark breaking changes with `!`, for example
`feat(config)!: require an explicit spool path`, and explain the compatibility
impact and migration steps in the PR description.

Use the [PR template](.github/pull_request_template.md) to explain what changed,
why it was needed, and how it was tested. Include relevant limitations and
report checks that were not run. Keep each PR focused on one coherent change.

Squash merge PRs into `main`, using the PR title as the squash commit's subject.
Individual development commits can stay informal; review the final title and
commit message before merging. Dependabot may keep its generated branch names;
normalize its PR title before merging, for example
`chore(deps): update modernc.org/sqlite`.

## Validation and correctness

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
