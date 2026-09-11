#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."
export POSTGRES_VERSION="${POSTGRES_VERSION:-18}"
case "$POSTGRES_VERSION" in
  14|15|16|17|18) ;;
  *) echo "POSTGRES_VERSION must be one of 14, 15, 16, 17, or 18" >&2; exit 2 ;;
esac
export LOAD_REPORT="${LOAD_REPORT:-$(pwd)/artifacts/load/report.json}"
# Prevent an old successful report from being mistaken for this invocation if
# compilation or database startup fails before the Go test can write its report.
mkdir -p "$(dirname "$LOAD_REPORT")"
# Go runs the test binary from its package directory, so resolve relative
# output paths here while still in the repository root.
export LOAD_REPORT="$(cd "$(dirname "$LOAD_REPORT")" && pwd)/$(basename "$LOAD_REPORT")"
printf '%s\n' '{"schema_version":1,"status":"not_completed"}' > "$LOAD_REPORT"
project="writerelay-load-$$"
scratch=$(mktemp -d)
compose=(docker compose -f compose.integration.yaml -p "$project")
cleanup() {
  local result=$?
  trap - EXIT
  if (( result != 0 )); then
    "${compose[@]}" logs --no-color postgres || true
  fi
  if ! "${compose[@]}" down --volumes --remove-orphans; then
    echo "Could not clean up load-test project: $project" >&2
    result=1
  fi
  rm -rf "$scratch"
  exit "$result"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

export WRITERELAY_LOAD_BINARY="$scratch/writerelayd"
go build -o "$WRITERELAY_LOAD_BINARY" ./cmd/writerelayd
"${compose[@]}" up -d --wait --wait-timeout 120 postgres
address=$("${compose[@]}" port postgres 5432)
export WRITERELAY_LOAD_DSN="postgres://postgres:integration-test-password@${address}/writerelay?sslmode=disable"
# The scenario has its own configurable deadline (at most 30 minutes). This
# outer timeout allows cleanup and catches bugs that bypass that context.
go test -tags=load -count=1 -timeout=32m -v ./tests/load/...
echo "Load report: $LOAD_REPORT"
