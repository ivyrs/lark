#!/usr/bin/env python3
"""Opt-in real-server check: temporary lark state, no writes to the server library.

Set LARK_NAVIDROME_URL, LARK_NAVIDROME_USERNAME, LARK_NAVIDROME_QUERY,
NAVIDROME_PASSWORD, and optionally LARK_NAVIDROME_ALLOW_HTTP=1.
"""
import os
import unittest

import navidrome_smoke as integration
from smoke import wait_until


class Account:
    def __init__(self):
        self.url = os.environ["LARK_NAVIDROME_URL"]
        self.username = os.environ["LARK_NAVIDROME_USERNAME"]
        self.password = os.environ["NAVIDROME_PASSWORD"]
        self.allow_http = os.environ.get("LARK_NAVIDROME_ALLOW_HTTP") == "1"
        self.page_size = 100
        self.sync_timeout = 300

    def close(self):
        pass  # The caller owns the real server; never stop or modify it.


class NavidromeLiveTest(integration.NavidromeSmokeTest):
    def make_fixture(self):
        required = ("LARK_NAVIDROME_URL", "LARK_NAVIDROME_USERNAME",
                    "LARK_NAVIDROME_QUERY", "NAVIDROME_PASSWORD")
        if not all(os.environ.get(key) for key in required):
            self.skipTest("set " + ", ".join(required) + " to opt in")
        return Account()

    def test_mpd_frontend_can_browse_and_play_remote_http_tracks(self):
        self.skipTest("combined MPD/source fixture only")

    def test_http_playback_starts_before_response_finishes_and_stop_is_responsive(self):
        self.skipTest("partial-response gating is only safe on the local API fixture")

    def test_http_seek_uses_range_requests(self):
        self.skipTest("controlled Range fixture only")

    def test_http_errors_do_not_log_signed_urls_even_with_transport_trace_enabled(self):
        self.skipTest("HTTP fault injection is only safe on the local API fixture")

    def test_failed_scan_preserves_catalogue_and_recovers(self):
        self.skipTest("fault injection is only safe on the local API fixture")

    # Override the fixture-specific test: a live check must never delete music.
    def test_catalogue_streaming_deletion_and_restart(self):
        tracks = self.rpc("library.search", {"query": os.environ["LARK_NAVIDROME_QUERY"], "limit": 1})
        self.assertTrue(tracks, "query must match at least one song in this account's catalogue")
        track = tracks[0]
        self.assertTrue(track["uri"].startswith("navidrome:track:"))
        for iteration in (1, 2):
            if iteration == 2:
                self.stop_daemon()
                self.start_daemon()
            self.rpc("queue.add", {"uris": [track["uri"]], "play": True})
            wait_until(lambda: len(self.rpc("history")) >= iteration, "real Navidrome audio decode")
            self.cli("stop")
            self.assertEqual(self.rpc("history")[0]["track"]["uri"], track["uri"])
            files = list((self.root / "state").rglob("*.audio"))
            self.assertEqual(files, [], "the source must not download or cache audio")
        self.assertNotIn(self.fixture.password, self.log_path.read_text())
        self.stop_daemon()
        self.assertEqual(self.daemon.returncode, 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
