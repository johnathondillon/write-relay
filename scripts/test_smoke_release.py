"""Regression tests for the release smoke test's startup sequencing."""
import importlib.util
import json
from pathlib import Path
import unittest
from unittest.mock import Mock, patch


spec = importlib.util.spec_from_file_location(
    "smoke_release", Path(__file__).with_name("smoke-release.py"))
smoke_release = importlib.util.module_from_spec(spec)
spec.loader.exec_module(smoke_release)
STARTED = json.dumps({"msg": "replication connection interrupted; reconnecting"})


class StartupTests(unittest.TestCase):
    def test_waits_for_complete_startup_record(self):
        logs = Mock(side_effect=["", '{"msg": "replication',
                                 '{"msg": "unrelated"}\n[]\n', STARTED])
        with patch.object(smoke_release.time, "sleep") as sleep:
            smoke_release.wait_for_startup(logs, lambda: False)
        self.assertEqual(logs.call_count, 4)
        self.assertEqual(sleep.call_count, 3)

    def test_early_exit_fails_even_with_an_old_startup_record(self):
        with self.assertRaisesRegex(RuntimeError, "exited"):
            smoke_release.wait_for_startup(lambda: STARTED, lambda: True)

    def test_timeout_does_not_fall_through_to_spool_inspection(self):
        with patch.object(smoke_release.time, "monotonic", side_effect=[0, 16]):
            with self.assertRaisesRegex(RuntimeError, "Timed out"):
                smoke_release.wait_for_startup(lambda: "", lambda: False)

    def test_container_stats_wait_for_startup(self):
        # Drive the complete Docker-mode orchestration without Docker. The fake
        # daemon has not finished startup when the first log read occurs. Any
        # premature stats invocation fails, reproducing the old ordering bug.
        logs_read = 0
        stats_read = 0

        def run(command, **kwargs):
            nonlocal logs_read, stats_read
            output = ""
            if command[-1] == "version":
                output = "writerelayd v0.0.0-preview.0 commit=test built=2026-09-10"
            elif command[-2:] == ["id", "-u"]:
                output = "1000"
            elif command[:2] == ["docker", "logs"]:
                logs_read += 1
                output = STARTED if logs_read >= 2 else ""
                return Mock(stdout="", stderr=output)
            elif "{{.State.Running}}" in command:
                output = "true"
            elif "{{.State.ExitCode}}" in command:
                output = "0"
            elif "stats" in command:
                self.assertGreaterEqual(logs_read, 2, "stats raced spool initialization")
                stats_read += 1
                output = json.dumps({"event_count": 0, "deliveries": {"total": 0}, "last_durable_lsn": "0/0"})
            return Mock(stdout=output, stderr="", returncode=0)

        args = Mock(archive=None, image="test-image", version="v0.0.0-preview.0", commit="test")
        with smoke_release.tempfile.TemporaryDirectory() as directory:
            with patch.object(smoke_release, "run", side_effect=run), \
                 patch.object(smoke_release.subprocess, "run", side_effect=run), \
                 patch.object(smoke_release.time, "sleep"), \
                 patch("builtins.print"):
                smoke_release.smoke(args, Path(directory))
        self.assertEqual(stats_read, 1)


if __name__ == "__main__":
    unittest.main()
