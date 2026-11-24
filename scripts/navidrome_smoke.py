#!/usr/bin/env python3
"""Exercise the Navidrome source through larkd with a local authenticated API."""
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import io
import json
import os
import re
from pathlib import Path
import secrets
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from urllib.parse import parse_qs, urlsplit
import wave

from smoke import BIN, read_until, wait_until

PLUGIN = Path(__file__).resolve().parents[1] / "plugins/navidrome/lark_source_navidrome.py"


class NavidromeFixture:
    def __init__(self):
        self.password = secrets.token_urlsafe(24)
        self.username = "fixture-user"
        self.requests = []
        self.lock = threading.Lock()
        self.fail_song_offset = None
        self.stream_gate = threading.Event()
        self.stream_gate.set()
        self.stream_first_chunk = threading.Event()
        self.stream_sent_all = threading.Event()
        self.stream_error = None
        self.ranges = []
        self.songs = {
            "album/one?": {"id": "song/one ?&", "title": "Navfixture First", "album": "First Album"},
            "album/two?": {"id": "song/two ?&", "title": "Navfixture Second", "album": "Second Album"},
        }
        audio = io.BytesIO()
        with wave.open(audio, "wb") as output:
            output.setnchannels(1)
            output.setsampwidth(2)
            output.setframerate(8000)
            output.writeframes(b"\0\0" * 8000)
        self.audio = audio.getvalue()
        fixture = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass  # Requests contain auth tokens: never log their URLs.

            def reply(self, data, content_type="application/json"):
                if not isinstance(data, bytes):
                    data = json.dumps({"subsonic-response": {
                        "status": "ok", "version": "1.16.1", **data,
                    }}).encode()
                self.send_response(200)
                self.send_header("Content-Type", content_type)
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def do_GET(self):
                parsed = urlsplit(self.path)
                params = {key: values[0] for key, values in parse_qs(parsed.query, keep_blank_values=True).items()}
                salt = params.get("s", "")
                token = hashlib.md5((fixture.password + salt).encode()).hexdigest()
                if (params.get("u") != fixture.username or not salt
                        or not secrets.compare_digest(params.get("t", ""), token)
                        or "p" in params):
                    self.reply({"status": "failed", "error": {"code": 40, "message": "Wrong credentials"}})
                    return
                if not parsed.path.startswith("/subsonic/rest/") or not parsed.path.endswith(".view"):
                    self.send_error(404)
                    return
                endpoint = parsed.path.rsplit("/", 1)[-1].removesuffix(".view")
                with fixture.lock:
                    fixture.requests.append((endpoint, params.get("id"), params.get("songOffset", params.get("offset"))))
                    songs = dict(fixture.songs)
                    fail_song_offset = fixture.fail_song_offset
                if endpoint == "ping":
                    self.reply({})
                elif endpoint == "search3":
                    offset, size = int(params.get("songOffset", 0)), int(params.get("songCount", 100))
                    if (params.get("query") != "" or params.get("artistCount") != "0"
                            or params.get("albumCount") != "0"):
                        self.send_error(400)
                        return
                    if offset == fail_song_offset:
                        self.reply({"status": "failed", "error": {"code": 0, "message": "Temporary fixture outage"}})
                        return
                    metadata = [dict(song, artist="Fixture Artist", albumId=album_id,
                                     artists=[{"id": "artist/one", "name": "Fixture Artist"}],
                                     albumArtists=[{"id": "artist/one", "name": "Fixture Artist"}],
                                     track=1, discNumber=1, duration=1, year=2026,
                                     contentType="audio/wav", suffix="wav", size=len(fixture.audio))
                                for album_id, song in sorted(songs.items())]
                    self.reply({"searchResult3": {"song": metadata[offset:offset + size]}})
                elif endpoint == "getAlbumList2":
                    if params.get("type") != "alphabeticalByName":
                        self.send_error(400)
                        return
                    albums = [{"id": album_id, "name": song["album"]} for album_id, song in sorted(songs.items())]
                    offset, size = int(params.get("offset", 0)), int(params.get("size", 100))
                    self.reply({"albumList2": {"album": albums[offset:offset + size]}})
                elif endpoint == "getAlbum":
                    if fail_song_offset is not None:
                        self.reply({"status": "failed", "error": {"code": 0, "message": "Temporary fixture outage"}})
                        return
                    song = songs.get(params.get("id"))
                    if song is None:
                        self.send_error(404)
                        return
                    metadata = dict(song, artist="Fixture Artist", albumId=params["id"],
                                    artists=[{"id": "artist/one", "name": "Fixture Artist"}],
                                    track=1, discNumber=1, duration=1, year=2026,
                                    contentType="audio/wav", suffix="wav", size=len(fixture.audio))
                    self.reply({"album": {"id": params["id"], "name": song["album"],
                                          "artist": "Fixture Artist", "song": [metadata]}})
                elif endpoint == "stream":
                    if params.get("format") != "raw" or params.get("id") not in {s["id"] for s in songs.values()}:
                        self.send_error(400)
                        return
                    if fixture.stream_error is not None:
                        self.send_response(fixture.stream_error)
                        self.send_header("Content-Length", "0")
                        self.end_headers()
                        return
                    start = 0
                    requested_range = self.headers.get("Range")
                    if requested_range is not None:
                        try:
                            start = int(requested_range.removeprefix("bytes=").split("-", 1)[0])
                        except ValueError:
                            self.send_error(400)
                            return
                        with fixture.lock:
                            fixture.ranges.append(start)
                    if start >= len(fixture.audio):
                        self.send_response(416)
                        self.send_header("Content-Range", f"bytes */{len(fixture.audio)}")
                        self.send_header("Content-Length", "0")
                        self.end_headers()
                        return
                    payload = fixture.audio[start:]
                    self.send_response(206 if requested_range else 200)
                    self.send_header("Content-Type", "audio/wav")
                    self.send_header("Accept-Ranges", "bytes")
                    if requested_range:
                        self.send_header("Content-Range", f"bytes {start}-{len(fixture.audio)-1}/{len(fixture.audio)}")
                    self.send_header("Content-Length", str(len(payload)))
                    self.end_headers()
                    try:
                        self.wfile.write(payload[:65536])
                        self.wfile.flush()
                        fixture.stream_first_chunk.set()
                        if not fixture.stream_gate.wait(timeout=8):
                            return
                        self.wfile.write(payload[65536:])
                        self.wfile.flush()
                        fixture.stream_sent_all.set()
                    except (BrokenPipeError, ConnectionResetError):
                        pass
                else:
                    self.send_error(404)

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.url = f"http://127.0.0.1:{self.server.server_port}/subsonic"

    def close(self):
        self.stream_gate.set()
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)

    def count(self, endpoint):
        with self.lock:
            return sum(request[0] == endpoint for request in self.requests)


class NavidromeSmokeTest(unittest.TestCase):
    def make_fixture(self):
        return NavidromeFixture()

    def setUp(self):
        for binary in ("larkd", "lark"):
            self.assertTrue((BIN / binary).is_file(), "run cargo build --workspace first")
        self.assertTrue(PLUGIN.is_file(), f"plugin is missing: {PLUGIN}")
        self.tmp = tempfile.TemporaryDirectory(prefix="lark-nav-smoke-")
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.fixture = self.make_fixture()
        self.addCleanup(self.fixture.close)
        self.env = dict(os.environ, **{
            "XDG_CONFIG_HOME": str(self.root / "config"),
            "XDG_DATA_HOME": str(self.root / "data"),
            "XDG_STATE_HOME": str(self.root / "state"),
            "XDG_RUNTIME_DIR": str(self.root),
            "LARK_SOCKET": str(self.root / "lark.sock"),
            "NAVIDROME_PASSWORD": self.fixture.password,
            "LARK_LOG": "info,hyper=trace,hyper_util=trace,reqwest=trace,rustls=trace",
        })
        self.config = self.root / "config.toml"
        self.config.write_text(
            'output = "null"\n[[plugin]]\nname = "navidrome"\n'
            f'exec = [{json.dumps(sys.executable)}, {json.dumps(str(PLUGIN))}]\n'
            '[plugin.config]\n'
            f'url = {json.dumps(self.fixture.url)}\n'
            f'username = {json.dumps(self.fixture.username)}\n'
            f'allow_http = {str(getattr(self.fixture, "allow_http", True)).lower()}\n'
            f'page_size = {getattr(self.fixture, "page_size", 1)}\n'
        )
        self.log_path = self.root / "daemon.log"
        self.log = self.log_path.open("w+")
        self.addCleanup(self.log.close)
        self.daemon = None
        self.addCleanup(self.stop_daemon)
        self.start_daemon()

    def start_daemon(self):
        previous_syncs = self.log_path.read_text().count("sync complete")
        self.daemon = subprocess.Popen(
            [str(BIN / "larkd"), "--config", str(self.config)], env=self.env,
            stdout=self.log, stderr=self.log,
        )
        def ready():
            if self.daemon.poll() is not None:
                self.fail(f"daemon exited:\n{self.log_path.read_text()}")
            return (Path(self.env["LARK_SOCKET"]).exists()
                    and self.log_path.read_text().count("sync complete") > previous_syncs)
        wait_until(ready, "Navidrome initial sync", timeout=getattr(self.fixture, "sync_timeout", 10))

    def stop_daemon(self):
        if self.daemon is not None and self.daemon.poll() is None:
            self.daemon.terminate()
            try:
                self.daemon.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.daemon.kill()
                self.daemon.wait(timeout=5)
                self.fail("daemon did not shut down")

    def rpc(self, method, params=None):
        with socket.socket(socket.AF_UNIX) as conn:
            conn.settimeout(35)
            conn.connect(self.env["LARK_SOCKET"])
            conn.sendall((json.dumps({"id": 1, "method": method, "params": params}) + "\n").encode())
            with conn.makefile("r") as reader:
                response = json.loads(reader.readline())
            self.assertNotIn("error", response, response)
            return response.get("result")

    def cli(self, *args):
        result = subprocess.run([str(BIN / "lark"), *args], env=self.env,
                                capture_output=True, text=True, timeout=35)
        self.assertEqual(result.returncode, 0, result.stderr)
        return result.stdout

    def sync(self):
        with socket.socket(socket.AF_UNIX) as conn:
            conn.settimeout(10)
            conn.connect(self.env["LARK_SOCKET"])
            with conn.makefile("r") as reader:
                conn.sendall(b'{"id":1,"method":"subscribe"}\n')
                reply = read_until(conn, reader, lambda m: m.get("id") == 1, "subscribe reply")
                self.assertNotIn("error", reply, reply)
                self.cli("sync")
                return read_until(conn, reader, lambda m: m.get("method") == "library.updated"
                                  and m.get("params", {}).get("source") == "navidrome", "Navidrome sync")

    def test_mpd_frontend_can_browse_and_play_remote_http_tracks(self):
        from mpd_smoke import MPDConnection, argument
        mpd_script = PLUGIN.parent.parent / "mpd/lark_frontend_mpd.py"
        with self.config.open("a") as config:
            config.write('\n[[plugin]]\nname = "mpd"\n'
                         f'exec = [{json.dumps(sys.executable)}, {json.dumps(str(mpd_script))}]\n'
                         '[plugin.config]\nhost = "127.0.0.1"\nport = 0\n')
        self.stop_daemon()
        self.start_daemon()
        match = wait_until(lambda: re.search(r"MPD listening on 127\.0\.0\.1:(\d+)",
                                            self.log_path.read_text()), "MPD frontend startup")
        client = MPDConnection(int(match.group(1)))
        self.addCleanup(client.close)
        track = self.rpc("library.search", {"query": "Navfixture First"})[0]
        self.assertIn("file: " + track["uri"], client.command("listallinfo"))
        client.command("addid " + argument(track["uri"]))
        client.command("play 0")
        wait_until(lambda: self.rpc("history"), "MPD-driven native HTTP playback")
        self.assertGreaterEqual(self.fixture.count("stream"), 1)
        self.assertIn("Fixture Artist", "\n".join(client.command("currentsong")))
        client.command("stop")
        self.assertEqual([source["name"] for source in self.rpc("sources")], ["navidrome"])

    def test_http_playback_starts_before_response_finishes_and_stop_is_responsive(self):
        audio = io.BytesIO()
        with wave.open(audio, "wb") as output:
            output.setparams((1, 2, 8000, 0, "NONE", "not compressed"))
            output.writeframes(b"\0\0" * 80000)
        self.fixture.audio = audio.getvalue()
        self.fixture.stream_gate.clear()
        self.cli("add", "Navfixture First")
        self.cli("play")
        self.assertTrue(self.fixture.stream_first_chunk.wait(timeout=5))
        wait_until(lambda: self.rpc("history"), "playback from partial HTTP response", timeout=5)
        self.assertFalse(self.fixture.stream_sent_all.is_set(), "playback must not wait for a full download")
        started = time.monotonic()
        self.cli("stop")
        self.assertLess(time.monotonic() - started, 1.0, "stop must not wait for the stalled body")
        self.cli("clear")
        self.fixture.stream_gate.set()
        self.assertEqual(self.rpc("status")["player"]["state"], "stopped")
        self.assertIsNone(self.rpc("status")["player"]["current"])
        self.assertEqual(list((self.root / "state").rglob("*.audio")), [])

    def test_http_seek_uses_range_requests(self):
        audio = io.BytesIO()
        with wave.open(audio, "wb") as output:
            output.setparams((1, 2, 8000, 0, "NONE", "not compressed"))
            output.writeframes(b"\0\0" * 80000)
        self.fixture.audio = audio.getvalue()
        self.cli("add", "Navfixture First")
        self.cli("play")
        wait_until(lambda: self.rpc("history"), "HTTP playback start")
        self.cli("pause")
        self.cli("seek", "5")
        wait_until(lambda: any(offset > 0 for offset in self.fixture.ranges), "HTTP Range seek")
        self.assertEqual(self.rpc("status")["player"]["state"], "paused")
        self.cli("stop")

    def test_http_errors_do_not_log_signed_urls_even_with_transport_trace_enabled(self):
        self.fixture.stream_error = 401
        self.cli("add", "Navfixture First")
        self.cli("play")
        wait_until(lambda: "playback error" in self.log_path.read_text(), "HTTP authentication error")
        wait_until(lambda: self.rpc("status")["player"]["state"] == "stopped", "failed HTTP playback stop")
        logs = self.log_path.read_text()
        self.assertNotIn(self.fixture.password, logs)
        self.assertNotIn("/rest/stream.view?", logs)
        self.assertNotRegex(logs, r"[?&](u|s|t)=")

    def test_failed_scan_preserves_catalogue_and_recovers(self):
        previous_failures = self.log_path.read_text().count("sync failed")
        with self.fixture.lock:
            self.fixture.songs["album/one?"]["title"] = "Navfixture Changed"
            self.fixture.fail_song_offset = 1
        self.cli("sync")
        wait_until(lambda: self.log_path.read_text().count("sync failed") > previous_failures,
                   "failed second song page")
        self.assertEqual(self.rpc("library.search", {"query": "Navfixture Changed"}), [])
        self.assertEqual(len(self.rpc("library.search", {"query": "Navfixture First"})), 1)
        self.assertEqual(self.rpc("sources")[0]["tracks"], 2)
        with self.fixture.lock:
            self.fixture.fail_song_offset = None
        self.sync()
        self.assertEqual(len(self.rpc("library.search", {"query": "Navfixture Changed"})), 1)
        matching = self.rpc("library.search", {"query": "Navfixture First"})
        self.assertTrue(all(track["title"] != "Navfixture First" for track in matching))

    def test_catalogue_streaming_deletion_and_restart(self):
        tracks = self.rpc("library.search", {"query": "navfixture"})
        self.assertEqual(len(tracks), 2)
        self.assertTrue(all(track["uri"].startswith("navidrome:track:") for track in tracks))
        self.assertEqual(tracks[0]["artists"], ["Fixture Artist"])
        self.assertEqual(tracks[0]["duration_ms"], 1000)
        self.assertEqual(self.fixture.count("search3"), 3)
        self.assertEqual(self.fixture.count("getAlbumList2"), 0)
        self.assertEqual(self.fixture.count("getAlbum"), 0)
        self.assertIn("Navfixture First", self.cli("search", "Navfixture First"))
        self.cli("add", "Navfixture First")
        for played in (1, 2):
            self.cli("play", "0")
            wait_until(lambda: len(self.rpc("history")) >= played, "Navidrome playback start")
            wait_until(lambda: self.rpc("status")["player"]["state"] == "stopped", "streamed WAV completion")
        self.assertGreaterEqual(self.fixture.count("stream"), 2, "each play resolves a core-owned HTTP source")
        self.assertEqual(list((self.root / "state").rglob("*.audio")), [])
        self.assertTrue(self.rpc("history")[0]["completed"])
        with self.fixture.lock:
            del self.fixture.songs["album/one?"]
        event = self.sync()
        self.assertEqual(event["params"]["deletes"], 1)
        self.assertEqual(self.rpc("library.search", {"query": "Navfixture First"}), [])
        self.assertEqual(len(self.rpc("library.search", {"query": "navfixture"})), 1)
        self.assertEqual(self.rpc("history")[0]["track"]["title"], "Navfixture First")
        self.stop_daemon()
        self.start_daemon()
        self.assertEqual(len(self.rpc("library.search", {"query": "navfixture"})), 1)
        self.assertEqual(self.rpc("sources")[0]["tracks"], 1)
        self.assertNotIn(self.fixture.password, self.log_path.read_text())
        self.assertNotIn("?u=", self.log_path.read_text())
        self.stop_daemon()
        self.assertEqual(self.daemon.returncode, 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
