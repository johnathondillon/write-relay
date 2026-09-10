#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."
version="${VERSION:?Set VERSION to a preview version, for example v0.1.0-preview.1}"
if [[ ! "$version" =~ ^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)-(preview|alpha|beta|rc)\.(0|[1-9][0-9]*)$ ]]; then
  echo "VERSION must be a preview tag such as v0.1.0-preview.1" >&2
  exit 2
fi
release_os="${GOOS:-$(go env GOOS)}"
release_arch="${GOARCH:-$(go env GOARCH)}"
case "$release_os/$release_arch" in
  linux/amd64|linux/arm64|darwin/amd64|darwin/arm64) ;;
  *) echo "Supported targets: linux/darwin on amd64/arm64" >&2; exit 2 ;;
esac
commit=$(git rev-parse HEAD)
build_date=$(git show -s --format=%cI HEAD)
output="${RELEASE_DIR:-dist}"
mkdir -p "$output"
output=$(cd "$output" && pwd)
name="writerelay_${version}_${release_os}_${release_arch}"
staging=$(mktemp -d)
trap 'rm -rf "$staging"' EXIT
mkdir -p "$staging/$name/sql/postgres"
CGO_ENABLED=0 GOOS="$release_os" GOARCH="$release_arch" go build \
  -trimpath -buildvcs=false \
  -ldflags="-s -w -X main.version=$version -X main.commit=$commit -X main.date=$build_date" \
  -o "$staging/$name/writerelayd" ./cmd/writerelayd
chmod 755 "$staging/$name/writerelayd"
cp LICENSE README.md writerelay.example.yaml "$staging/$name/"
cp -R docs "$staging/$name/docs"
cp sql/postgres/001_install.sql "$staging/$name/sql/postgres/"
tar -czf "$output/$name.tar.gz" -C "$staging" "$name"
(cd "$output" && shasum -a 256 "$name.tar.gz" > "$name.tar.gz.sha256")
echo "Packaged $output/$name.tar.gz"
