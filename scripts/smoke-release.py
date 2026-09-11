#!/usr/bin/env python3
"""Exercise the shipped executable and its SQLite driver without PostgreSQL.

Requires Python 3.12+; Docker mode also requires a local Docker daemon.
"""
import argparse
import json
from pathlib import Path
import subprocess
import tarfile
import tempfile
import time
import uuid


def run(command, **kwargs):
    return subprocess.run(command, check=True, text=True, capture_output=True,
                          timeout=20, **kwargs)


def wait_for_startup(read_logs, exited, timeout=15):
    """Observe startup without opening SQLite while it switches to WAL mode."""
    deadline = time.monotonic() + timeout
    while True:
        if exited():
            raise RuntimeError("Packaged daemon exited before completing startup")
        for line in read_logs().splitlines():
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue  # A concurrently written final log line may be partial.
            # This smoke test deliberately points at an unavailable PostgreSQL.
            # app.Run opens/migrates the spool and configures sinks before it
            # starts the replication loop that emits this message.
            if isinstance(record, dict) and record.get("msg") == "replication connection interrupted; reconnecting":
                return
        if time.monotonic() >= deadline:
            raise RuntimeError("Timed out waiting for packaged daemon startup")
        time.sleep(0.1)


def smoke(args, directory):
    container = None
    process = None
    logfile = directory / "daemon.log"
    if args.archive:
        with tarfile.open(args.archive) as archive:
            archive.extractall(directory, filter="data")
        binaries = list(directory.glob("writerelay_*/writerelayd"))
        if len(binaries) != 1:
            raise RuntimeError("Expected exactly one packaged writerelayd")
        command = [str(binaries[0])]
        spool_path = str(directory / "spool.sqlite")
    else:
        command = ["docker", "run", "--rm", "--network", "none", args.image]
        spool_path = "/var/lib/writerelay/spool.sqlite"
    version = run(command + ["version"]).stdout.strip()
    if not version.startswith(f"writerelayd {args.version} commit={args.commit} built="):
        raise RuntimeError(f"Wrong build metadata: {version}")
    if version.endswith("built=unknown"):
        raise RuntimeError("Missing build date")
    run(command + ["--help"])
    config = directory / "writerelay.yaml"
    config.write_text(f"""version: 1
postgres:
  dsn: postgres://smoke:unused@127.0.0.1:1/smoke?sslmode=disable&connect_timeout=1
  slot: smoke_slot
  publication: smoke_publication
  message_prefix: writerelay.v1
spool:
  path: {json.dumps(spool_path)}
delivery:
  sinks: []
logging:
  level: warn
  format: json
""")
    config.chmod(0o644)
    try:
        if args.image:
            container = "writerelay-smoke-" + uuid.uuid4().hex
            run(["docker", "run", "-d", "--name", container, "--network", "none",
                 "-v", "/var/lib/writerelay",
                 "--mount", f"type=bind,src={config},dst=/tmp/writerelay.yaml,readonly",
                 args.image, "run", "--config", "/tmp/writerelay.yaml"])
            command = ["docker", "exec", container, "writerelayd"]
            config_arg = "/tmp/writerelay.yaml"
            user = run(["docker", "exec", container, "id", "-u"]).stdout.strip()
            if user == "0":
                raise RuntimeError("Release image runs as root")
        else:
            config_arg = str(config)
            with logfile.open("w") as log:
                process = subprocess.Popen(command + ["run", "--config", config_arg],
                                           stdout=log, stderr=log)
        if container:
            def read_logs():
                logs = run(["docker", "logs", container])
                return logs.stdout + logs.stderr

            def exited():
                state = run(["docker", "inspect", "--format", "{{.State.Running}}", container])
                return state.stdout.strip() != "true"
        else:
            read_logs = lambda: logfile.read_text()
            exited = lambda: process.poll() is not None
        wait_for_startup(read_logs, exited)
        result = run(command + ["spool", "stats", "--config", config_arg, "--json"])
        stats = json.loads(result.stdout)
        assert stats["event_count"] == 0, stats
        assert stats["deliveries"]["total"] == 0, stats
        assert stats["last_durable_lsn"] == "0/0", stats
        if container:
            run(["docker", "stop", "--time", "5", container])
            result = run(["docker", "inspect", "--format", "{{.State.ExitCode}}", container])
            assert result.stdout.strip() == "0", result.stdout
        else:
            process.terminate()
            assert process.wait(timeout=5) == 0
            run(command + ["spool", "stats", "--config", config_arg, "--json"])
        print(f"PASS: {version}; help, SQLite initialization/read, and graceful shutdown")
    except Exception:
        if container:
            print(subprocess.run(["docker", "logs", container], capture_output=True,
                                 text=True, timeout=10).stderr)
        elif logfile.exists():
            print(logfile.read_text())
        raise
    finally:
        if process and process.poll() is None:
            process.kill()
            process.wait(timeout=5)
        if container:
            run(["docker", "rm", "-f", "-v", container])


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    target = parser.add_mutually_exclusive_group(required=True)
    target.add_argument("--archive", type=Path)
    target.add_argument("--image")
    parser.add_argument("--version", required=True)
    parser.add_argument("--commit", required=True)
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="writerelay-release-smoke-") as temporary:
        smoke(args, Path(temporary))
