#!/usr/bin/env python3
"""Run the stdio MPD frontend through a real larkd, optionally with LARK_RMPC_BIN."""
import fcntl
import json
import os
from pathlib import Path
import pty
import re
import select
import socket
import struct
import subprocess
import sys
import tempfile
import termios
import time
import unittest
import wave

from smoke import BIN, wait_until

PLUGIN = Path(__file__).resolve().parents[1] / "plugins/mpd/lark_frontend_mpd.py"


def argument(value):
    return '"' + str(value).replace("\\", "\\\\").replace('"', '\\"') + '"'


class MPDConnection:
    def __init__(self, port):
        self.socket = socket.create_connection(("127.0.0.1", port), timeout=10)
        self.reader = self.socket.makefile("r", encoding="utf-8")
        greeting = self.reader.readline()
        if not greeting.startswith("OK MPD "):
            raise AssertionError(f"invalid MPD greeting: {greeting!r}")

    def send(self, text):
        self.socket.sendall((text + "\n").encode())

    def read(self, allow_error=False):
        lines = []
        while True:
            line = self.reader.readline()
            if not line:
                raise AssertionError("MPD frontend closed the connection")
            line = line.rstrip("\n")
            if line == "OK":
                return lines
            if line.startswith("ACK "):
                if allow_error:
                    return lines + [line]
                raise AssertionError(line)
            lines.append(line)

    def command(self, text, allow_error=False):
        self.send(text)
        return self.read(allow_error)

    def close(self):
        self.reader.close()
        self.socket.close()


class MPDSmokeTest(unittest.TestCase):
    def setUp(self):
        self.assertTrue(PLUGIN.is_file(), f"missing {PLUGIN}")
        for binary in ("larkd", "lark", "lark-source-local"):
            self.assertTrue((BIN / binary).is_file(), "run cargo build --workspace first")
        self.tmp = tempfile.TemporaryDirectory(prefix="lark-mpd-smoke-")
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.music = self.root / "music"
        self.music.mkdir()
        self.write_audio('First "quoted" track.wav')
        self.write_audio("Second track.wav")
        self.env = dict(os.environ, **{
            "XDG_CONFIG_HOME": str(self.root / "config"),
            "XDG_DATA_HOME": str(self.root / "data"),
            "XDG_STATE_HOME": str(self.root / "state"),
            "XDG_RUNTIME_DIR": str(self.root),
            "LARK_SOCKET": str(self.root / "lark.sock"),
            "LARK_LOG": "info",
            "TERM": "xterm-256color",
        })
        config = self.root / "lark.toml"
        self.config = config
        config.write_text(
            'output = "null"\n[[plugin]]\nname = "local"\n'
            f'exec = [{json.dumps(str(BIN / "lark-source-local"))}]\n'
            f'[plugin.config]\nmusic_dir = {json.dumps(str(self.music))}\n'
            '[[plugin]]\nname = "mpd"\n'
            f'exec = [{json.dumps(sys.executable)}, {json.dumps(str(PLUGIN))}]\n'
            '[plugin.config]\nhost = "127.0.0.1"\nport = 0\n'
        )
        self.log_path = self.root / "daemon.log"
        self.log = self.log_path.open("w+")
        self.addCleanup(self.log.close)
        self.daemon = subprocess.Popen([str(BIN / "larkd"), "--config", str(config)],
                                       env=self.env, stdout=self.log, stderr=self.log)
        self.addCleanup(self.stop_daemon)
        def ready():
            if self.daemon.poll() is not None:
                self.fail(self.log_path.read_text())
            return re.search(r"MPD listening on 127\.0\.0\.1:(\d+)", self.log_path.read_text())
        match = wait_until(ready, "MPD listener")
        self.port = int(match.group(1))
        wait_until(lambda: self.rpc("sources") and self.rpc("sources")[0]["tracks"] == 2,
                   "local catalogue sync")
        self.tracks = self.rpc("library.tracks", {"limit": 10})
        self.assertEqual(len(self.tracks), 2)
        self.client = MPDConnection(self.port)
        self.addCleanup(self.client.close)

    def write_audio(self, name):
        with wave.open(str(self.music / name), "wb") as audio:
            audio.setparams((1, 2, 8000, 0, "NONE", "not compressed"))
            audio.writeframes(b"\0\0" * 32000)

    def stop_daemon(self):
        if self.daemon.poll() is None:
            self.daemon.terminate()
            try:
                self.daemon.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.daemon.kill()
                self.daemon.wait(timeout=5)
                self.fail("daemon failed to terminate frontend")

    def rpc(self, method, params=None):
        with socket.socket(socket.AF_UNIX) as connection:
            connection.settimeout(10)
            connection.connect(self.env["LARK_SOCKET"])
            connection.sendall((json.dumps({"id": 1, "method": method, "params": params}) + "\n").encode())
            with connection.makefile("r") as reader:
                reply = json.loads(reader.readline())
            self.assertNotIn("error", reply, reply)
            return reply.get("result")

    def test_queue_controls_command_lists_and_notifications(self):
        self.assertEqual([source["name"] for source in self.rpc("sources")], ["local"])
        songs = self.client.command("listallinfo")
        for track in self.tracks:
            self.assertIn("file: " + track["uri"], songs)
        commands = self.client.command("commands")
        self.assertIn("command: idle", commands)
        request = "command_list_ok_begin\n" + "\n".join("addid " + argument(t["uri"]) for t in self.tracks)
        result = self.client.command(request + "\nstatus\ncommand_list_end")
        self.assertEqual(result.count("list_OK"), 3)
        self.assertEqual(len(self.rpc("status")["queue"]), 2)
        self.client.command("play 0")
        wait_until(lambda: self.rpc("status")["player"]["state"] == "playing", "MPD play")
        self.client.command("pause 1")
        self.assertEqual(self.rpc("status")["player"]["state"], "paused")
        self.client.command("seekcur 1")
        self.client.command("setvol 25")
        self.assertAlmostEqual(self.rpc("status")["volume"], 0.25, places=4)
        self.assertIn("volume: 25", self.client.command("status"))
        observer = MPDConnection(self.port)
        self.addCleanup(observer.close)
        observer.send("idle playlist")
        self.client.command("clear")
        self.assertIn("changed: playlist", observer.read())
        observer.send("idle player")
        observer.send("noidle")
        observer.read()  # Must not hang even if a pending player event wins.
        self.assertEqual(self.rpc("status")["queue"], [])
        error = self.client.command("definitely_unsupported", allow_error=True)
        self.assertTrue(error[-1].startswith("ACK [5@0]"), error)

    def test_removed_or_disabled_source_is_hidden_and_reenable_restores_metadata(self):
        original_config = self.config.read_text()
        track = self.tracks[0]
        self.client.command("addid " + argument(track["uri"]))
        self.client.command("play 0")
        wait_until(lambda: self.rpc("history"), "history before removing source")
        self.client.command("stop")

        def restart_private_daemon(config_text):
            self.stop_daemon()
            self.config.write_text(config_text)
            self.log.seek(0)
            self.log.truncate(0)
            self.log.flush()
            self.daemon = subprocess.Popen([str(BIN / "larkd"), "--config", str(self.config)],
                                           env=self.env, stdout=self.log, stderr=self.log)
            def ready():
                if self.daemon.poll() is not None:
                    self.fail(self.log_path.read_text())
                return re.search(r"MPD listening on 127\.0\.0\.1:(\d+)", self.log_path.read_text())
            match = wait_until(ready, "restarted MPD frontend")
            client = MPDConnection(int(match.group(1)))
            self.addCleanup(client.close)
            return client

        only_frontend = ('output = "null"\n[[plugin]]\nname = "mpd"\n'
                         f'exec = [{json.dumps(sys.executable)}, {json.dumps(str(PLUGIN))}]\n'
                         '[plugin.config]\nhost = "127.0.0.1"\nport = 0\n')
        disabled_source = original_config.replace('name = "local"\n',
                                                  'name = "local"\nenabled = false\n', 1)
        for config_text in (only_frontend, disabled_source):
            client = restart_private_daemon(config_text)
            self.assertEqual(self.rpc("sources"), [])
            self.assertEqual(self.rpc("library.tracks", {"limit": 10}), [])
            self.assertEqual(self.rpc("library.search", {"query": "First"}), [])
            self.assertEqual(client.command("listallinfo"), [])
            self.assertEqual(self.rpc("history")[0]["track"]["uri"], track["uri"])

        client = restart_private_daemon(original_config)
        wait_until(lambda: self.rpc("sources") and self.rpc("sources")[0]["running"], "re-enabled local source")
        self.assertEqual(len(self.rpc("library.tracks", {"limit": 10})), 2)
        self.assertIn("file: " + track["uri"], client.command("listallinfo"))
        self.assertEqual(self.rpc("history")[0]["track"]["uri"], track["uri"])

    def test_library_updated_invalidates_frontend_catalogue(self):
        self.client.command("listallinfo")
        observer = MPDConnection(self.port)
        self.addCleanup(observer.close)
        observer.send("idle database")
        self.write_audio("Third track.wav")
        self.rpc("library.sync")
        self.assertIn("changed: database", observer.read())
        songs = self.client.command("listallinfo")
        self.assertEqual(sum(line.startswith("file: ") for line in songs), 3)

    def rmpc_config(self):
        config = self.root / "rmpc.ron"
        if not config.exists():
            generated = subprocess.run([os.environ["LARK_RMPC_BIN"], "config"], env=self.env,
                                       capture_output=True, text=True, timeout=10)
            self.assertEqual(generated.returncode, 0, generated.stderr)
            content = re.sub(r'address:\s*"[^"]*"', f'address: "127.0.0.1:{self.port}"', generated.stdout, count=1)
            config.write_text(content)
        return config

    def rmpc(self, *args):
        command = os.environ["LARK_RMPC_BIN"]
        config = self.rmpc_config()
        result = subprocess.run([command, "--config", str(config), "--address", f"127.0.0.1:{self.port}", *args],
                                env=self.env, capture_output=True, text=True, timeout=15)
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        return result.stdout

    @unittest.skipUnless(os.environ.get("LARK_RMPC_BIN"), "set LARK_RMPC_BIN for real-client validation")
    def test_real_rmpc_command_mode_and_tui(self):
        self.rmpc("status")
        self.rmpc("listall")
        self.rmpc("add", self.tracks[0]["uri"])
        self.rmpc("play", "0")
        self.rmpc("pause")
        self.rmpc("volume", "30")
        self.assertAlmostEqual(self.rpc("status")["volume"], 0.3, places=4)
        self.assertIn(self.tracks[0]["uri"], [song["file"] for song in json.loads(self.rmpc("queue"))])
        self.rmpc("song")
        self.rmpc("outputs")
        self.rmpc("decoders")
        master, slave = pty.openpty()
        self.addCleanup(os.close, master)
        self.addCleanup(os.close, slave)
        fcntl.ioctl(slave, termios.TIOCSWINSZ, struct.pack("HHHH", 30, 120, 0, 0))
        process = subprocess.Popen([os.environ["LARK_RMPC_BIN"], "--config", str(self.rmpc_config()), "--address", f"127.0.0.1:{self.port}"],
                                   env=self.env, stdin=slave, stdout=slave, stderr=slave, start_new_session=True)
        def cleanup_client():
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)
        self.addCleanup(cleanup_client)
        captured = bytearray()
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline and process.poll() is None:
            ready, _, _ = select.select([master], [], [], 0.1)
            if ready:
                captured.extend(os.read(master, 65536))
                text = re.sub(rb"\x1b\[[0-?]*[ -/]*[@-~]", b"", captured)
                if b"Queue" in text and (b"Artists" in text or b"Albums" in text):
                    break
        self.assertIsNone(process.poll(), captured.decode(errors="replace"))
        self.assertIn(b"Queue", captured)
        os.write(master, b"q")
        self.assertEqual(process.wait(timeout=5), 0, captured.decode(errors="replace"))
        self.rmpc("stop")
        self.rmpc("clear")


if __name__ == "__main__":
    unittest.main(verbosity=2)
