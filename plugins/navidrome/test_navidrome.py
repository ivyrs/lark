"""Protocol, state, and HTTP regressions using only disposable loopback data."""

import hashlib
import http.server
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import threading
import unittest
from unittest.mock import patch
from urllib.parse import parse_qs, urlsplit

import lark_source_navidrome as plugin


PASSWORD = "fixture-password-never-in-output"
USERNAME = "fixture & listener"


def song(ident, **fields):
    return {"id": ident, "title": f"Song {ident}", **fields}


class Server:
    def __init__(self):
        self.calls = []
        self.songs = []
        self.page_cap = None
        self.override_pages = {}
        self.redirect = False
        self.wait_on_search = None
        fixture = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def log_message(self, *_):
                pass

            def do_GET(self):
                path = urlsplit(self.path)
                query = {k: v[0] for k, v in parse_qs(path.query, keep_blank_values=True).items()}
                fixture.calls.append((path.path, query))
                if fixture.redirect:
                    self.send_response(302)
                    self.send_header("Location", "/redirected?" + path.query)
                    self.end_headers()
                    return
                expected = hashlib.md5((PASSWORD + query.get("s", "")).encode(), usedforsecurity=False).hexdigest()
                if query.get("t") != expected:
                    self.reply({"status": "failed", "error": {"code": 40, "message": PASSWORD + self.path}})
                    return
                if not path.path.startswith("/library/rest/"):
                    self.send_error(404)
                    return
                endpoint = path.path.removeprefix("/library/rest/")
                if endpoint == "ping.view":
                    self.reply({"status": "ok"})
                elif endpoint == "search3.view":
                    offset, size = int(query["songOffset"]), int(query["songCount"])
                    size = min(size, fixture.page_cap) if fixture.page_cap else size
                    page = fixture.songs[offset:offset + size]
                    response = {"status": "ok", "searchResult3": {"song": page} if page else {}}
                    self.reply(fixture.override_pages.get(offset, response), fixture.wait_on_search)
                else:
                    self.send_error(404)

            def reply(self, response, wait=None):
                data = json.dumps({"subsonic-response": response}).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                if wait is not None:
                    wait.wait(2)
                try:
                    self.wfile.write(data)
                except OSError:
                    pass  # the client can time out before reading

        self.httpd = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self.httpd.server_port}/library/"
        self.thread = threading.Thread(target=self.httpd.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True)
        self.thread.start()

    def close(self):
        self.httpd.shutdown()
        self.httpd.server_close()
        self.thread.join()


class NavidromeTests(unittest.TestCase):
    def setUp(self):
        self.server = Server()
        self.addCleanup(self.server.close)
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        environment = patch.dict(os.environ, {"NAVIDROME_PASSWORD": PASSWORD, "LARK_TEST_NAVIDROME": PASSWORD})
        environment.start()
        self.addCleanup(environment.stop)
        self.source = plugin.Source()
        self.addCleanup(lambda: self.source.close())

    def config(self, **options):
        return {"api": 1, "state_dir": str(self.root),
                "config": {"url": self.server.url, "username": USERNAME, "allow_http": True, **options}}

    def initialize(self, **options):
        return self.source.call("initialize", self.config(**options))

    def restart(self, **options):
        self.source.close()
        self.source = plugin.Source()
        self.initialize(**options)

    def pull(self, cursor):
        return self.source.call("sync.pull", {"cursor": cursor})

    def sync(self, cursor=None):
        rows = []
        for _ in range(30):
            result = self.pull(cursor)
            rows.extend(result["upserts"])
            if result["done"]:
                return rows, result["deletes"], result["next_cursor"]
            self.assertEqual(result["deletes"], [])
            self.assertIsNone(result["next_cursor"])
        self.fail("scan did not finish")

    def resolve(self, ident):
        return self.source.call("resolve", {"uri": plugin.track_uri(ident)})

    def test_stdio_authentication_and_errors_are_sanitized(self):
        requests = [{"id": 1, "method": "initialize", "params": self.config(password_env="LARK_TEST_NAVIDROME")},
                    {"id": 2, "method": "sync.pull", "params": {}},
                    {"id": 3, "method": "missing"}, {"id": 4, "method": "shutdown"}]
        command = [sys.executable, "-B", plugin.__file__]
        result = subprocess.run(command, input="\n".join(map(json.dumps, requests)) + "\n",
                                text=True, capture_output=True, check=True, timeout=10)
        replies = list(map(json.loads, result.stdout.splitlines()))
        self.assertEqual([reply["id"] for reply in replies], [1, 2, 3, 4])
        self.assertEqual(replies[0]["result"], {"name": "navidrome", "scheme": "navidrome", "cache": "full", "version": "0.1.0"})
        self.assertTrue(replies[1]["result"]["done"])
        self.assertEqual(replies[2]["error"]["code"], -32601)
        self.assertIsNone(replies[3]["result"])
        self.assertIn("[navidrome] sync started:", result.stderr)
        self.assertIn("[navidrome] scan complete: 0 song pages, 0 tracks", result.stderr)
        self.assertNotIn(PASSWORD, result.stderr)
        salts = set()
        for path, query in self.server.calls:
            self.assertIn(path, ("/library/rest/ping.view", "/library/rest/search3.view"))
            self.assertEqual({k: query[k] for k in ("u", "v", "c", "f")},
                             {"u": USERNAME, "v": "1.16.1", "c": "lark", "f": "json"})
            self.assertEqual(query["t"], hashlib.md5((PASSWORD + query["s"]).encode(), usedforsecurity=False).hexdigest())
            self.assertNotIn("p", query)
            self.assertNotIn(query["s"], salts)
            salts.add(query["s"])
        with patch.dict(os.environ, {"NAVIDROME_PASSWORD": "bad password"}):
            result = subprocess.run(command, input=json.dumps({"id": 5, "method": "initialize", "params": self.config()}) + "\n",
                                    text=True, capture_output=True, check=True, timeout=10)
        self.assertIn("authentication failed", result.stdout)
        self.assertNotIn(PASSWORD, result.stdout + result.stderr)
        self.assertNotIn("&t=", result.stdout + result.stderr)
        self.assertNotIn("bad password", result.stdout + result.stderr)

    def test_config_tls_and_redirect_rejection(self):
        bad_configs = [{"allow_http": False}, {"allow_http": 1}, {"url": "https://user:pw@example.test"},
                       {"url": "https://localhost/a?secret=hi"}, {"url": "ftp://example.test/music"},
                       {"password": "plaintext"}, {"password_env": "missing_NAVIDROME_FIXTURE_PASSWORD"},
                       {"username": " "}, {"max_cache_bytes": 0}, {"page_size": 501}, {"page_size": True}]
        for options in bad_configs:
            with self.subTest(options=options), self.assertRaises(plugin.PluginError):
                self.initialize(**options)
        self.assertEqual(self.server.calls, [])
        self.assertEqual(list(self.root.iterdir()), [])
        self.server.redirect = True
        with self.assertRaisesRegex(plugin.PluginError, "redirect"):
            self.initialize()
        self.assertEqual([path for path, _ in self.server.calls], ["/library/rest/ping.view"])

    def search_calls(self):
        return [q for path, q in self.server.calls if path == "/library/rest/search3.view"]

    def test_pagination_metadata_and_uri_roundtrip(self):
        uid = "song:/? café%"
        self.server.songs = [
            song(uid, album="Album", artists=[{"name": "One"}, {"name": "Two"}],
                 albumArtists=[{"name": "Album artist"}], displayAlbumArtist="unused fallback",
                 releaseDate={"year": 2024, "month": 1, "day": 2}, year=2023,
                 track=4, discNumber=2, duration=123, musicBrainzId="recording-id",
                 albumMusicBrainzId="release-id", isrc=["ISRC"], genres=[{"name": "Genre"}],
                 recordLabels=[{"name": "Label"}]),
            song("another", album="Other", year=2010, artist="Band", displayAlbumArtist="Other artist", genre="Alt"),
        ]
        self.server.page_cap = 1  # a server can return short, nonempty pages
        self.initialize(page_size=5)
        for _ in range(2):
            self.assertEqual(self.pull(None), plugin.batch([]))
        rows, deletions, cursor = self.sync()
        self.assertEqual(deletions, [])
        self.assertTrue(cursor)
        self.assertEqual(rows[0], {"uri": "navidrome:track:song%3A%2F%3F%20caf%C3%A9%25", "title": f"Song {uid}",
                                   "artists": ["One", "Two"], "album_artists": ["Album artist"], "album": "Album",
                                   "track_no": 4, "disc_no": 2, "duration_ms": 123000, "date": "2024-01-02",
                                   "mbid": "recording-id", "release_mbid": "release-id", "isrc": "ISRC",
                                   "tags": [["genre", "Genre"], ["label", "Label"]]})
        self.assertEqual(rows[1]["date"], "2010")
        self.assertEqual(rows[1]["artists"], ["Band"])
        self.assertEqual(rows[1]["album_artists"], ["Other artist"])
        self.assertEqual(rows[1]["tags"], [["genre", "Alt"]])
        self.assertEqual([(q["query"], q["artistCount"], q["albumCount"], q["songCount"], q["songOffset"])
                          for q in self.search_calls()], [("", "0", "0", "5", str(i)) for i in range(3)])
        self.assertEqual(len(self.server.calls), 4)  # ping plus three search3 pages

    def test_resolve_signs_fresh_http_urls_without_network_or_writes(self):
        self.initialize(max_cache_bytes=1)  # deprecated and ignored
        calls = list(self.server.calls)
        before = set(self.root.iterdir())
        uid = "../song:/? café%&id=not-another-field"
        salts = set()
        for _ in range(2):
            resolved = self.resolve(uid)
            self.assertEqual(set(resolved), {"kind", "url", "headers"})
            self.assertEqual(resolved["kind"], "http")
            self.assertEqual(resolved["headers"], [])
            url = urlsplit(resolved["url"])
            server = urlsplit(self.server.url)
            self.assertEqual((url.scheme, url.netloc, url.path, url.fragment),
                             (server.scheme, server.netloc, "/library/rest/stream.view", ""))
            query = parse_qs(url.query)
            salt, = query.pop("s")
            token, = query.pop("t")
            self.assertRegex(salt, r"^[a-f0-9]{32}$")
            self.assertNotIn(salt, salts)
            salts.add(salt)
            self.assertEqual(token, hashlib.md5((PASSWORD + salt).encode(), usedforsecurity=False).hexdigest())
            self.assertEqual(query, {"u": [USERNAME], "v": ["1.16.1"], "c": ["lark"], "f": ["json"],
                                     "id": [uid], "format": ["raw"]})
            self.assertNotIn(PASSWORD, resolved["url"])
        with self.assertRaisesRegex(plugin.PluginError, "URI"):
            self.source.call("resolve", {"uri": "navidrome:track:%FF"})
        self.assertEqual(self.server.calls, calls)
        self.assertEqual(set(self.root.iterdir()), before)
        self.assertFalse((self.root / "audio").exists())

    def test_dropped_final_batch_replays_upserts_and_deletes_across_restart(self):
        self.server.songs = [song("remove")]
        old = self.sync_after_init()
        # More than one replay page, plus a deletion from the previous scan.
        self.server.songs = [song(str(i)) for i in range(501)]
        before = len(self.search_calls())
        first_result = self.sync(old)
        self.assertEqual([q["songOffset"] for q in self.search_calls()[before:]],
                         ["0", "100", "200", "300", "400", "500", "501"])
        self.assertEqual(len(first_result[0]), 501)
        self.assertEqual(first_result[1], [plugin.track_uri("remove")])
        self.server.songs.clear()
        self.restart()
        before = len(self.server.calls)
        self.assertEqual(self.sync(old), first_result)
        self.assertEqual(len(self.server.calls), before)  # exact replay, not a rescan
        candidate = first_result[2]
        rows, deletions, latest = self.sync(candidate)
        self.assertEqual(rows, [])
        self.assertEqual(set(deletions), {plugin.track_uri(str(i)) for i in range(501)})
        self.assertNotEqual(latest, candidate)
        with self.assertRaisesRegex(plugin.PluginError, "unknown.*cursor"):
            self.pull(old)

    def sync_after_init(self, **options):
        self.initialize(**options)
        return self.sync()[2]

    def test_scan_progress_is_counted_without_publishing_partial_metadata(self):
        messages = []
        self.source.progress = messages.append
        self.server.songs = [song(str(i)) for i in range(26)]
        self.initialize(page_size=1)
        for _ in range(26):
            self.assertEqual(self.pull(None), plugin.batch([]))
        self.assertEqual(len(messages), 2)
        self.assertIn("0 on first sync", messages[0])
        self.assertEqual(messages[1], "sync scanning: 25 song pages, 25 tracks staged")
        completed = self.pull(None)
        self.assertTrue(completed["done"])
        self.assertEqual(len(completed["upserts"]), 26)
        self.assertEqual(messages[-1], "scan complete: 26 song pages, 26 tracks; publishing library")

    def test_slow_scan_reports_progress_before_twenty_five_pages(self):
        messages = []
        self.source.progress = messages.append
        self.server.songs = [song("one")]
        self.initialize()
        with patch.object(plugin.time, "monotonic", side_effect=[0, 6]):
            self.assertEqual(self.pull(None), plugin.batch([]))
        self.assertEqual(messages[-1], "sync scanning: 1 song pages, 1 tracks staged")

    def test_failed_scan_does_not_publish_upserts_or_change_the_baseline(self):
        self.server.songs = [song("old")]
        cursor = self.sync_after_init(page_size=1)
        self.server.songs = [song("would become a ghost")]
        failures = [
            ({"status": "failed", "error": {"code": 70, "message": PASSWORD}}, "search3 failed.*code 70"),
            ({"status": "ok"}, "malformed catalogue"),
            ({"status": "ok", "searchResult3": {"song": [song("one"), song("two")]}}, "more songs than requested"),
            ({"status": "ok", "searchResult3": {"song": [self.server.songs[0]]}}, "repeated song id"),
        ]
        for page, message in failures:
            with self.subTest(message=message):
                self.server.override_pages[1] = page
                self.assertEqual(self.pull(cursor), plugin.batch([]))
                with self.assertRaisesRegex(plugin.PluginError, message) as error:
                    self.pull(cursor)
                self.assertNotIn(PASSWORD, str(error.exception))
        self.restart(page_size=1)
        self.server.override_pages.clear()
        self.server.songs = [song("replacement")]
        rows, deletes, _ = self.sync(cursor)
        self.assertEqual([row["uri"] for row in rows], [plugin.track_uri("replacement")])
        self.assertEqual(deletes, [plugin.track_uri("old")])

    def test_version_one_catalogue_and_legacy_cache_survive_upgrade(self):
        audio = self.root / "audio"
        audio.mkdir()
        legacy = audio / ("a" * 64 + ".audio")
        partial = audio / ".download-incomplete"
        legacy.write_bytes(b"old audio bytes")
        partial.write_bytes(b"interrupted download")
        identity = hashlib.sha256(json.dumps([self.server.url.rstrip("/"), USERNAME]).encode()).hexdigest()
        previous = plugin.track_uri("removed")
        candidate = {"uri": plugin.track_uri("existing"), "title": "Track from old schema"}
        with sqlite3.connect(self.root / "catalogue.sqlite3") as db:
            db.executescript("""
                CREATE TABLE state (version INTEGER NOT NULL, identity TEXT NOT NULL, baseline TEXT, candidate TEXT);
                CREATE TABLE baseline (uri TEXT PRIMARY KEY NOT NULL);
                CREATE TABLE candidate (seq INTEGER PRIMARY KEY, uri TEXT NOT NULL UNIQUE, track TEXT NOT NULL);
            """)
            db.execute("INSERT INTO state VALUES (1, ?, 'old-baseline', 'old-candidate')", (identity,))
            db.execute("INSERT INTO baseline VALUES (?)", (previous,))
            db.execute("INSERT INTO candidate VALUES (1, ?, ?)", (candidate["uri"], json.dumps(candidate)))
        self.initialize(max_cache_bytes=1)
        before = len(self.server.calls)
        self.assertEqual(self.sync("old-baseline"), ([candidate], [previous], "old-candidate"))
        self.assertEqual(len(self.server.calls), before)
        _, deletes, _ = self.sync("old-candidate")
        self.assertEqual(deletes, [candidate["uri"]])
        self.source.close()
        with self.assertRaisesRegex(plugin.PluginError, "another server/user"):
            self.initialize(username="different")
        self.assertEqual(legacy.read_bytes(), b"old audio bytes")
        self.assertEqual(partial.read_bytes(), b"interrupted download")

    def test_catalogue_request_obeys_total_deadline(self):
        self.initialize()
        wait = threading.Event()
        self.server.wait_on_search = wait
        self.addCleanup(wait.set)
        with patch.object(plugin, "RPC_TIMEOUT", 0.15), self.assertRaisesRegex(plugin.PluginError, "time limit"):
            self.pull(None)
        self.server.wait_on_search = None
        wait.set()
        self.assertEqual(self.sync()[:2], ([], []))


if __name__ == "__main__":
    unittest.main()
