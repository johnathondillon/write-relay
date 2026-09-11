# Building and publishing previews

The [preview-release workflow](../.github/workflows/release.yml) packages the Go
daemon. It does not publish the TypeScript SDK or declare production readiness.
GitHub [releases](https://docs.github.com/en/repositories/releasing-projects-on-github/about-releases)
attach binaries and notes to a Git tag.

## What runs when

| Trigger | Behavior |
| --- | --- |
| Relevant pull request or push to `main` | Build packages, smoke-test them on all four native platforms, and save Actions artifacts. No registry or release writes. |
| Manual workflow dispatch | The same build-only checks. No publication, even when dispatched against a tag. |
| Push a tag such as `v0.1.0-preview.1` | Validate that the tagged commit is on `main`, run checks, then publish a GitHub prerelease and versioned GHCR image. |

Accepted tags follow `vMAJOR.MINOR.PATCH-preview.N`, with `alpha`, `beta`, and `rc`
also accepted. Numeric components cannot have leading zeroes. Stable tags such
as `v1.0.0` are rejected by this preview workflow. Ordinary builds use
`v0.0.0-preview.0` as their artifact version, with the actual commit embedded.

Each platform builds with the Go version from `go.mod` and `CGO_ENABLED=0`:
Linux amd64/arm64 and macOS Intel/Apple Silicon. Native runners execute the
**extracted packaged binary**, verify version/commit metadata and help, start
capture against an unavailable local database, read the newly initialized
SQLite spool, and stop the daemon gracefully. No PostgreSQL service is needed
for this packaging smoke test. Linux jobs also build and exercise their native
Docker image, including its non-root user and writable volume.

Before publication, separate jobs run Go formatting, module verification,
build/unit/vet/race/crash-recovery/vulnerability checks and PostgreSQL 14–18
integration tests. Publication depends on every packaging and release-check job
succeeding. Existing CI continues checking ordinary PRs.

## Local build

Requires Git, Go from `go.mod`, Bash, tar, and `shasum`. Run from a checkout:

```bash
VERSION=v0.1.0-preview.1 make package
```

`dist/` contains the native archive and its `.sha256` file. Archives include the
binary, configuration example, license, SQL installer, and documentation.
To cross-compile:

```bash
VERSION=v0.1.0-preview.1 GOOS=linux GOARCH=amd64 make package
VERSION=v0.1.0-preview.1 GOOS=linux GOARCH=arm64 make package
VERSION=v0.1.0-preview.1 GOOS=darwin GOARCH=amd64 make package
VERSION=v0.1.0-preview.1 GOOS=darwin GOARCH=arm64 make package
```

An existing archive with the same version/platform name is replaced locally.
`RELEASE_DIR` optionally selects an output directory instead of `dist/`.
Build metadata includes the supplied version, `HEAD` commit, and that commit's
date. Local builds can include uncommitted changes; only a clean tagged checkout
identifies the source precisely. Archive timestamps are not normalized, so this
is not a reproducible-build guarantee.

Verify and smoke-test an archive on its matching host (Python 3.12+):

```bash
(cd dist && shasum -a 256 -c *.sha256)
python3 scripts/smoke-release.py \
  --archive dist/writerelay_v0.1.0-preview.1_darwin_arm64.tar.gz \
  --version v0.1.0-preview.1 --commit "$(git rev-parse HEAD)"
```

For a local Docker build:

```bash
docker build \
  --build-arg VERSION=v0.1.0-preview.1 \
  --build-arg COMMIT="$(git rev-parse HEAD)" \
  --build-arg BUILD_DATE="$(git show -s --format=%cI HEAD)" \
  -t writerelay-preview:local .
python3 scripts/smoke-release.py --image writerelay-preview:local \
  --version v0.1.0-preview.1 --commit "$(git rev-parse HEAD)"
```

Smoke tests use temporary files and, in Docker mode, a unique container and
anonymous spool volume that they remove afterward. They do not use the
persistent development or LMS stacks.

## Publish a preview

The workflow uses `GITHUB_TOKEN`; only the publishing job requests
`contents: write` and `packages: write`. Repository/organization policy must
permit those writes. The image has an OCI source label linking it to this
repository. Follow GitHub's [container registry guidance](https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-container-registry)
if an existing package needs Actions access or its visibility needs to be made
public. A public source repository does not by itself guarantee public package
visibility; verify an unauthenticated pull before announcing the preview.

After the workflow has been merged, use the GitHub Actions UI to run it manually
on `main` and inspect all four package jobs. Download the `package-*` artifacts
from the run to review them. Each artifact is retained for seven days; Linux
artifacts also contain a saved, tested Docker image for the publishing job.

When ready to publish, choose an unused preview version and tag a clean,
reviewed `main` commit:

```bash
git switch main
git pull --ff-only
git tag -a v0.1.0-preview.1 -m 'WriteRelay v0.1.0-preview.1'
git push origin v0.1.0-preview.1
```

**Pushing this tag publishes externally after validation passes.** Build-only
workflow runs and merging the workflow itself do not publish a release.

The publishing job downloads artifacts from its own run, verifies all four
archive checksums, and combines them into `SHA256SUMS`. It loads and pushes the
exact Docker images that passed native smoke tests; it does not rebuild them.
The multi-platform tag is `ghcr.io/johnathondillon/write-relay:<version>`;
`<version>-amd64` and `<version>-arm64` are also pushed. No `latest` tag is moved.
The GitHub release contains four `.tar.gz` archives, `SHA256SUMS`, and preview
notes. Saved Docker image archives remain Actions artifacts rather than release
assets. macOS notarization and cryptographic artifact signing are not included.

The job refuses to replace an existing GitHub release. Publication across GHCR
and GitHub Releases is not atomic: a failure can leave image tags without a
GitHub release. Inspect the run before retrying; retrying such a partial run
can replace those image tags. Once a preview is public, use a new version for
changes rather than moving its Git tag or overwriting its artifacts.

After publication, download an archive and test its checksum/version, pull the
image without registry credentials, and follow the [installation guide](installation.md).
Hosted CI and external publication can only be verified after the workflow is
pushed; a local packaging test does not establish either result.
