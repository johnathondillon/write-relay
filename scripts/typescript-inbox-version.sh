#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
export POSTGRES_VERSION="${POSTGRES_VERSION:-18}"
case "$POSTGRES_VERSION" in
  14|15|16|17|18) ;;
  *) echo "POSTGRES_VERSION must be 14, 15, 16, 17, or 18" >&2; exit 2 ;;
esac
project="writerelay-inbox-${POSTGRES_VERSION}-$$"
compose=(docker compose -f compose.inbox.yaml -p "$project")
cleanup() {
  local result=$?
  trap - EXIT
  if (( result != 0 )); then "${compose[@]}" logs --no-color postgres || true; fi
  if ! "${compose[@]}" down --volumes --remove-orphans --rmi local; then result=1; fi
  exit "$result"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
"${compose[@]}" up -d --wait --wait-timeout 120 postgres
"${compose[@]}" run --rm --no-deps --build inbox-tests
