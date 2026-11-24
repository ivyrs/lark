#!/usr/bin/env python3
"""Headless integration check. Build all workspace binaries before running."""
import io
import json
import os
from pathlib import Path
import socket
import subprocess
import tempfile
import time
import unittest
from unittest.mock import Mock, patch
import wave

BIN = Path(os.environ.get("LARK_BIN_DIR", "target/debug")).resolve()


def wait_until(check, description, timeout=10):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = check()
        if result:
            return result
        time.sleep(0.05)
    raise AssertionError(f"timed out waiting for {description}")


def read_until(conn, reader, matches, description, timeout=10):
    """Subscriptions multiplex responses and events; unrelated frames are valid."""
    deadline = time.monotonic() + timeout
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise AssertionError(f"timed out waiting for {description}")
        conn.settimeout(remaining)
        try:
            line = reader.readline()
        except TimeoutError as error:
            raise AssertionError(f"timed out waiting for {description}") from error
        if not line:
            raise AssertionError(f"daemon closed connection waiting for {description}")
        message = json.loads(line)
        if matches(message):
            return message


class EventWaitTests(unittest.TestCase):
    def test_interleaved_notifications_and_subscription_reply(self):
        messages = [
            {"method": "player.state"},
            {"id": 1, "result": None},
            {"method": "queue.cleared"},
            {"method": "player.state"},
            {"method": "library.updated", "params": {"source": "other"}},
            {"method": "library.updated", "params": {"source": "local", "deletes": 1}},
        ]
        reader = io.StringIO("".join(json.dumps(message) + "\n" for message in messages))
        conn = Mock()
        reply = read_until(conn, reader, lambda m: m.get("id") == 1, "subscribe reply")
        self.assertNotIn("error", reply)
        event = read_until(conn, reader, is_local_sync, "local library sync")
        self.assertEqual(event["params"]["deletes"], 1)

    def test_unrelated_events_do_not_extend_deadline(self):
        reader = io.StringIO('{"method":"player.state"}\n' * 3)
        conn = Mock()
        with patch.object(time, "monotonic", side_effect=[0, 1, 11]):
            with self.assertRaisesRegex(AssertionError, "timed out waiting for local sync"):
                read_until(conn, reader, is_local_sync, "local sync")
        conn.settimeout.assert_called_once_with(9)

    def test_eof_reports_closed_connection(self):
        with self.assertRaisesRegex(AssertionError, "daemon closed connection"):
            read_until(Mock(), io.StringIO(""), is_local_sync, "local sync")

    def test_socket_timeout_reports_waited_event(self):
        reader = Mock()
        reader.readline.side_effect = TimeoutError()
        with self.assertRaisesRegex(AssertionError, "timed out waiting for local sync"):
            read_until(Mock(), reader, is_local_sync, "local sync")


def is_local_sync(message):
    return (message.get("method") == "library.updated"
            and message.get("params", {}).get("source") == "local")


class SmokeTest(unittest.TestCase):
    def setUp(self):
        for name in ("larkd", "lark", "lark-source-local"):
            if not (BIN / name).is_file():
                self.fail(f"missing {BIN / name}; run cargo build --workspace first")
        self.tmp = tempfile.TemporaryDirectory(prefix="lark-smoke-")
        self.addCleanup(self.tmp.cleanup)
        root = Path(self.tmp.name)
        self.music = root / "music"
        self.music.mkdir()
        self.track = self.music / "fixture.wav"
        with wave.open(str(self.track), "wb") as audio:
            audio.setnchannels(1)
            audio.setsampwidth(2)
            audio.setframerate(8000)
            audio.writeframes(b"\0\0" * 16000)
        self.env = dict(os.environ, **{
            "XDG_CONFIG_HOME": str(root / "config"),
            "XDG_DATA_HOME": str(root / "data"),
            "XDG_STATE_HOME": str(root / "state"),
            "XDG_RUNTIME_DIR": str(root),
            "LARK_SOCKET": str(root / "lark.sock"),
        })
        self.config = root / "config.toml"
        # JSON strings are valid TOML basic strings for these temporary paths.
        self.config.write_text(
            'output = "null"\n[[plugin]]\nname = "local"\n'
            f'exec = [{json.dumps(str(BIN / "lark-source-local"))}]\n'
            f'[plugin.config]\nmusic_dir = {json.dumps(str(self.music))}\n'
        )
        self.log = (root / "daemon.log").open("w+")
        self.addCleanup(self.log.close)
        self.daemon = subprocess.Popen(
            [str(BIN / "larkd"), "--config", str(self.config)],
            env=self.env, stdout=self.log, stderr=self.log,
        )
        self.addCleanup(self.stop_daemon)
        wait_until(self.ready, "daemon socket")
        wait_until(lambda: self.rpc("library.search", {"query": "fixture"}), "initial library sync")

    def stop_daemon(self):
        if self.daemon.poll() is None:
            self.daemon.terminate()
            try:
                self.daemon.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.daemon.kill()
                self.daemon.wait(timeout=5)
                self.fail("daemon failed to shut down gracefully")

    def ready(self):
        if self.daemon.poll() is not None:
            self.log.seek(0)
            self.fail(f"daemon exited during startup:\n{self.log.read()}")
        return Path(self.env["LARK_SOCKET"]).exists()

    def rpc(self, method, params=None):
        with socket.socket(socket.AF_UNIX) as conn:
            conn.settimeout(5)
            conn.connect(self.env["LARK_SOCKET"])
            conn.sendall((json.dumps({"id": 1, "method": method, "params": params}) + "\n").encode())
            with conn.makefile("r") as reader:
                response = json.loads(reader.readline())
            self.assertNotIn("error", response, response)
            return response.get("result")

    def cli(self, *args):
        result = subprocess.run(
            [str(BIN / "lark"), *args], env=self.env,
            capture_output=True, text=True, timeout=5,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        return result.stdout

    def test_library_playback_and_resync(self):
        self.assertEqual(self.rpc("hello")["api"], 1)
        source = self.rpc("sources")[0]
        self.assertTrue(source["running"])
        self.assertEqual(source["tracks"], 1)
        self.assertIn("fixture", self.cli("search", "fixture"))
        self.assertIn("added 1", self.cli("add", "fixture"))
        self.assertEqual(len(self.rpc("status")["queue"]), 1)
        self.cli("play")
        wait_until(lambda: self.rpc("history"), "playback start")
        self.cli("pause")
        self.assertEqual(self.rpc("status")["player"]["state"], "paused")
        self.cli("seek", "-1")
        self.cli("play")
        wait_until(lambda: self.rpc("status")["player"]["state"] == "stopped", "null playback completion")
        history = self.rpc("history")
        self.assertTrue(history[0]["completed"])
        self.assertEqual(history[0]["track"]["title"], "fixture")
        self.cli("clear")
        self.assertEqual(self.rpc("status")["queue"], [])
        self.assertIsNone(self.rpc("status")["player"]["current"])
        self.cli("add", "fixture")
        self.cli("play")
        self.cli("stop")
        self.cli("clear")
        self.track.unlink()
        # Wait on the sync event, not an arbitrary delay or library query race.
        with socket.socket(socket.AF_UNIX) as conn:
            conn.settimeout(10)
            conn.connect(self.env["LARK_SOCKET"])
            with conn.makefile("r") as reader:
                conn.sendall(b'{"id":1,"method":"subscribe"}\n')
                reply = read_until(conn, reader, lambda m: m.get("id") == 1, "subscribe reply")
                self.assertNotIn("error", reply, reply)
                # Deliberately enqueue a valid unrelated event before the sync.
                self.cli("stop")
                self.cli("sync")
                event = read_until(conn, reader, is_local_sync, "local library sync")
                self.assertEqual(event["params"]["deletes"], 1)
        self.assertEqual(self.rpc("library.search", {"query": "fixture"}), [])
        # Stale library rows must not erase historical snapshots.
        self.assertEqual(self.rpc("history")[0]["track"]["title"], "fixture")
        self.stop_daemon()
        self.assertEqual(self.daemon.returncode, 0)
        self.assertFalse(Path(self.env["LARK_SOCKET"]).exists())


if __name__ == "__main__":
    unittest.main(verbosity=2)
