# Installing a WriteRelay preview

WriteRelay is an architectural preview, not a production-ready delivery system.
Preview packaging provides binaries for Linux and macOS on amd64 (Intel/AMD)
and arm64 (Apple Silicon/ARM), plus a Linux Docker image for both architectures.
No Go or Node installation is needed to run a packaged release.

The commands below apply **after a preview has been published** on the
[releases page](https://github.com/johnathondillon/write-relay/releases).
`v0.1.0-preview.1` is an example version; select an available preview there.
The Node SDK is still unpublished and is not included as an npm release.

## Download a binary

Choose the archive matching your operating system and processor:

| System | Archive suffix |
| --- | --- |
| Linux, Intel/AMD 64-bit | `linux_amd64.tar.gz` |
| Linux, ARM 64-bit | `linux_arm64.tar.gz` |
| macOS, Intel | `darwin_amd64.tar.gz` |
| macOS, Apple Silicon | `darwin_arm64.tar.gz` |

For example, on Apple Silicon:

```bash
VERSION=v0.1.0-preview.1
PLATFORM=darwin_arm64
ARCHIVE="writerelay_${VERSION}_${PLATFORM}.tar.gz"
RELEASE_URL="https://github.com/johnathondillon/write-relay/releases/download/$VERSION"
curl --fail --location --output "$ARCHIVE" "$RELEASE_URL/$ARCHIVE"
curl --fail --location --output SHA256SUMS "$RELEASE_URL/SHA256SUMS"
```

Check the archive against the matching line in `SHA256SUMS` before extracting.
On macOS:

```bash
awk -v name="$ARCHIVE" '$2 == name { print; found=1 } END { if (!found) exit 1 }' \
  SHA256SUMS > selected.sha256
shasum -a 256 -c selected.sha256
```

On Linux, use `sha256sum -c selected.sha256` for the last command. It must report
`OK`. Checksums detect corrupted downloads; they are not a publisher signature.

```bash
tar -xzf "$ARCHIVE"
cd "writerelay_${VERSION}_${PLATFORM}"
./writerelayd version
./writerelayd --help
```

The archive includes `writerelayd`, `writerelay.example.yaml`, `LICENSE`, the
PostgreSQL installation SQL, and project documentation. Source-code examples and
SDKs referenced in the documentation remain in the repository. Optionally place
`writerelayd` in a directory on your `PATH`; the commands here run it directly.

macOS binaries are not signed with a Developer ID or notarized. macOS may ask
for approval to open a downloaded executable. Follow Apple's
[guidance for opening software from an unidentified developer](https://support.apple.com/en-us/102445)
after verifying the download; do not disable system-wide security protections.

## Configure PostgreSQL and the spool

Provide a PostgreSQL 14–18 database configured for logical replication, with
sufficient replication slots and WAL senders. Use the database roles and grants
described in the [security model](security-model.md). Hosted providers may have
additional requirements; managed-service compatibility remains unverified.

```bash
cp writerelay.example.yaml writerelay.yaml
export WRITERELAY_POSTGRES_DSN='postgres://writerelay_repl:YOUR_PASSWORD@DB_HOST:5432/APP_DB?sslmode=verify-full'
```

Replace the placeholder connection details and configure certificates as needed.
Edit `writerelay.yaml` to choose a persistent `spool.path` writable by the daemon
user, the slot/publication names, and delivery sinks. The sample defaults to
capture-only mode (`sinks: []`). Relative spool paths resolve from the process's
working directory; prefer an absolute path for a service installation.

An administrator must install the SQL API and create the empty publication and
replication slot. The bundled `sql/postgres/001_install.sql` installs the SQL API.
Alternatively, `setup` can install missing objects when run with suitable
administrator credentials:

```bash
# Run with credentials authorized to install the missing database objects.
./writerelayd setup --config ./writerelay.yaml --create-slot
# Switch WRITERELAY_POSTGRES_DSN to the restricted relay role before running.
./writerelayd doctor --config ./writerelay.yaml
./writerelayd run --config ./writerelay.yaml
```

`setup` does not create application tables or database roles and does not grant
application access to `writerelay.emit`. It does not repair incompatible existing
slots/publications by dropping them. See the [FAQ](faq.md) and
[architecture](architecture.md) for transaction and delivery behavior.

To try the complete LMS demonstration with an initialized local database, use the
[repository example](https://github.com/johnathondillon/write-relay/tree/main/examples/nuxt-lms).

## Docker

After publication, the versioned image is available at
`ghcr.io/johnathondillon/write-relay:<version>`. Docker selects the Linux amd64 or
arm64 image for the host. There is deliberately no `latest` tag for previews.

```bash
VERSION=v0.1.0-preview.1
IMAGE="ghcr.io/johnathondillon/write-relay:$VERSION"
docker pull "$IMAGE"
docker run --rm "$IMAGE" version
```

Use a configuration file with `spool.path: /var/lib/writerelay/spool.sqlite`.
Supply a PostgreSQL address reachable **from the container**; its `localhost`
refers to the container itself. Provision the PostgreSQL objects first as above.
From the directory containing `writerelay.yaml`:

```bash
docker volume create writerelay-data
docker run --rm --name writerelay \
  --env WRITERELAY_POSTGRES_DSN \
  --mount type=bind,src="$(pwd)/writerelay.yaml",dst=/etc/writerelay.yaml,readonly \
  --mount type=volume,src=writerelay-data,dst=/var/lib/writerelay \
  "$IMAGE" run --config /etc/writerelay.yaml
```

The image runs as a non-root user. A new named volume inherits the spool
directory's ownership; existing bind-mounted directories need suitable write
permissions. Pass configured webhook authorization/signing environment variables
as well when enabling those features. The monitoring listener is optional; see
[monitoring](monitoring.md) for container binding and host-port examples.

Stopping or replacing the container must preserve `writerelay-data`. The local
spool and PostgreSQL slot belong to the same capture history. Do not run two
relay processes against one spool or slot. Review [retention](retention.md) and
[correctness](correctness.md) before operating or upgrading an installation.

## Upgrades

Stop the old daemon, keep its spool and configuration, and start the selected
new version. Startup may migrate the spool schema; downgrading to a binary that
does not support the resulting schema is not supported. Read that preview's
release notes before changing versions. Preserve a recoverable copy of the
stopped spool according to your storage procedures; PostgreSQL may already have
discarded WAL acknowledged by the relay, so losing the spool can lose pending
work. Release packaging does not add a backup/restore facility.
