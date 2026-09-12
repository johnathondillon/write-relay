#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."
export POSTGRES_VERSION="${POSTGRES_VERSION:-18}"
case "$POSTGRES_VERSION" in
  14|15|16|17|18) ;;
  *) echo "POSTGRES_VERSION must be one of 14, 15, 16, 17, or 18" >&2; exit 2 ;;
esac

# Unique project and ephemeral port allow concurrent runs alongside development.
project="writerelay-integration-${POSTGRES_VERSION}-$$"
compose=(docker compose -f compose.integration.yaml -p "$project")
cleanup() {
  local result=$?
  trap - EXIT
  if (( result != 0 )); then
    "${compose[@]}" logs --no-color postgres || true
  fi
  # Remove only this invocation's containers and anonymous image volumes.
  if ! "${compose[@]}" down --volumes --remove-orphans; then
    echo "Could not clean up integration project: $project" >&2
    result=1
  fi
  exit "$result"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

"${compose[@]}" up -d --wait --wait-timeout 120 postgres
address=$("${compose[@]}" port postgres 5432)
export WRITERELAY_INTEGRATION_DSN="postgres://postgres:integration-test-password@${address}/writerelay?sslmode=disable"
# The test checks the actual server major, so a wrong image cannot pass silently.
export WRITERELAY_INTEGRATION_POSTGRES_MAJOR="$POSTGRES_VERSION"
go test -tags=integration -count=1 -timeout=3m -v ./tests/integration/... ./examples/go-receiver
