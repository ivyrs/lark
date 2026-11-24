import asyncio
import json
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

import lark_frontend_mpd as mpd


TRACK = {
    "uri": 'fixture:A "quoted"\\ song', "title": "A title\nOK", "artists": ["The Artist", "Guest"],
    "album_artists": ["The Artist"], "album": "Album", "duration_ms": 2200,
    "tags": [["GENRE", "Jazz"]],
}


def quote(value):
    return '"' + value.replace("\\", "\\\\").replace('"', '\\"') + '"'


class ParsingTests(unittest.TestCase):
    def test_framing_and_filter_escapes(self):
        self.assertEqual(mpd.split_command(f"addid {quote(TRACK['uri'])}"), ("addid", [TRACK["uri"]]))
        self.assertEqual(mpd.split_command('lsinfo ""'), ("lsinfo", [""]))
        for line in ['add "unterminated', 'add "no"separator', 'add incomplete\\']:
            with self.subTest(line=line), self.assertRaises(mpd.MPDError):
                mpd.split_command(line)
        cmd = quote(r"((Artist == 'The Artist') AND (Title contains 'title'))")
        _, args = mpd.split_command(f"find {cmd} sort Title window 0:1")
        self.assertEqual(list(mpd.matching_tracks({"t": TRACK}, args, allow_options=True)), [TRACK])
        cmd = quote(r"(Title == 'A \'quoted\' path \\here')")
        _, args = mpd.split_command(f"find {cmd}")
        self.assertEqual(mpd.expression(args[0]), [("Title", "==", "A 'quoted' path \\here")])
        with self.assertRaises(mpd.MPDError):
            mpd.expression("(Artist =~ '.*')")
        self.assertEqual(mpd.seek_offset("-1.250", relative_ok=True), (-1250, True))
        with self.assertRaises(mpd.MPDError):
            mpd.seek_offset("nan")

    def test_loopback_only(self):
        self.assertEqual(mpd.bind_address({}), ("127.0.0.1", 6600))
        self.assertEqual(mpd.bind_address({"port": 0, "host": "::1"}), ("::1", 0))
        for config in ({"port": False}, {"port": 65536}, {"host": "0.0.0.0"},
                       {"host": "example.org"}, {"host": "::"}):
            with self.subTest(config=config), self.assertRaises(ValueError):
                mpd.bind_address(config)

    def test_metadata_output_and_grouping(self):
        lines = list(mpd.track_lines(TRACK, 0, 12))
        self.assertIn("Title: A title OK\n", lines)
        self.assertIn("Artist: Guest\n", lines)
        self.assertIn("Genre: Jazz\n", lines)
        self.assertIn("duration: 2.200\n", lines)
        self.assertIn("Id: 12\n", lines)
        self.assertEqual(list(mpd.list_tags({"t": TRACK}, ["album", "group", "albumartist"])),
                         ["AlbumArtist: The Artist\n", "Album: Album\n"])


class CatalogueTests(unittest.IsolatedAsyncioTestCase):
    async def test_keyset_cache_and_refresh_race(self):
        class Core:
            tracks = [{"uri": uri} for uri in ["t:a", "t:b", "t:c"]]
            calls = []
            update = False

            async def call(self, method, params):
                self.calls.append((method, dict(params)))
                if self.update:
                    self.update = False
                    catalogue.invalidate()
                return [t for t in self.tracks if t["uri"] > params.get("after_uri", "")][:2]

        core = Core()
        catalogue = mpd.Catalogue(core)
        with patch.object(mpd, "PAGE_SIZE", 2):
            first = await catalogue.get()
            self.assertEqual(list(first[0]), ["t:a", "t:b", "t:c"])
            self.assertEqual(core.calls, [
                ("library.tracks", {"limit": 2}),
                ("library.tracks", {"limit": 2, "after_uri": "t:b"}),
            ])
            self.assertIs(await catalogue.get(), first)
            catalogue.invalidate()
            core.update = True
            with self.assertRaisesRegex(mpd.CoreError, "changed during refresh"):
                await catalogue.get()
            self.assertIsNone(catalogue.snapshot)
            core.tracks.append({"uri": "t:d"})
            self.assertEqual(list((await catalogue.get())[0]), ["t:a", "t:b", "t:c", "t:d"])

    async def test_cache_limits_and_nonadvancing_pages(self):
        class Core:
            async def call(self, method, params):
                return [TRACK]

        catalogue = mpd.Catalogue(Core())
        with patch.object(mpd, "MAX_CATALOGUE_BYTES", 1), self.assertRaisesRegex(mpd.CoreError, "cache limit"):
            await catalogue.get()
        with patch.object(mpd, "PAGE_SIZE", 1), self.assertRaisesRegex(mpd.CoreError, "unordered"):
            await catalogue.get()
        self.assertIsNone(catalogue.snapshot)


class FrontendTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.plugin = await asyncio.create_subprocess_exec(
            sys.executable, str(Path(mpd.__file__)), stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        )
        self.addAsyncCleanup(self.stop_plugin)
        await self.send({"id": 1, "method": "initialize", "params": {
            "api": 1, "config": {"host": "127.0.0.1", "port": 0}, "state_dir": "/unused",
        }})
        result = await self.receive()
        self.assertEqual(result, {"id": 1, "result": {
            "name": "mpd", "role": "frontend", "scheme": "", "cache": "none", "version": "0.1.0",
        }})
        # IDs are namespaced by direction: subscribe can share initialize's ID.
        self.assertEqual(await self.receive(), {"id": 1, "method": "subscribe"})
        await self.send({"id": 1, "result": None})
        ready = (await asyncio.wait_for(self.plugin.stderr.readline(), 3)).decode()
        self.assertRegex(ready, r"^MPD listening on 127\.0\.0\.1:[0-9]+\n$")
        self.port = int(ready.split(":")[-1])

    async def stop_plugin(self):
        if self.plugin.returncode is None:
            self.plugin.stdin.close()
            try:
                await asyncio.wait_for(self.plugin.wait(), 3)
            except asyncio.TimeoutError:
                self.plugin.kill()
                await self.plugin.wait()
                self.fail("MPD plugin did not shut down on stdin EOF")
        self.assertEqual(self.plugin.returncode, 0, (await self.plugin.stderr.read()).decode())

    async def send(self, msg):
        self.plugin.stdin.write((json.dumps(msg) + "\n").encode())
        await self.plugin.stdin.drain()

    async def receive(self):
        return json.loads(await asyncio.wait_for(self.plugin.stdout.readline(), 3))

    async def connect(self):
        reader, writer = await asyncio.open_connection("127.0.0.1", self.port)

        async def cleanup():
            writer.close()
            await writer.wait_closed()

        self.addAsyncCleanup(cleanup)
        self.assertEqual(await asyncio.wait_for(reader.readline(), 3), mpd.MPD_GREETING.encode())
        return reader, writer

    async def reply(self, reader):
        async def read():
            lines = []
            while True:
                line = (await reader.readline()).decode().rstrip("\n")
                self.assertNotEqual(line, "", "MPD closed before a response")
                lines.append(line)
                if line == "OK" or line.startswith("ACK "):
                    return lines
        return await asyncio.wait_for(read(), 3)

    async def test_idle_is_per_client_and_late_noidle_does_not_desync(self):
        a, wa = await self.connect()
        b, wb = await self.connect()
        wa.write(b"idle player\n")
        wb.write(b"idle playlist\n")
        await self.send({"method": "queue.inserted", "params": {}})
        self.assertEqual(await self.reply(b), ["changed: playlist", "OK"])
        # The playlist event is irrelevant to A's current wait but is not lost.
        wa.write(b"noidle\nidle playlist\n")
        self.assertEqual(await self.reply(a), ["OK"])
        self.assertEqual(await self.reply(a), ["changed: playlist", "OK"])
        wa.write(b"idle mixer\n")
        await self.send({"method": "player.volume", "params": {"volume": 0.2}})
        self.assertEqual(await self.reply(a), ["changed: mixer", "OK"])
        wa.write(b"noidle\nunsupported\n")
        self.assertEqual(await self.reply(a), ['ACK [5@0] {unsupported} unknown command "unsupported"'])

    async def test_batch_order_and_bidirectional_routing(self):
        reader, writer = await self.connect()
        writer.write(b"command_list_ok_begin\nping\nsetvol 24\nblah\nclear\ncommand_list_end\n")
        request = await self.receive()
        self.assertEqual(request["method"], "player.volume")
        self.assertEqual(request["params"], {"volume": 0.24})
        # A daemon request with the same ID can arrive while the RPC is in flight.
        await self.send({"id": request["id"], "method": "invalid"})
        self.assertEqual((await self.receive())["error"]["code"], -32601)
        await self.send({"id": request["id"], "result": None})
        self.assertEqual(await self.reply(reader), [
            "list_OK", "list_OK", 'ACK [5@2] {blah} unknown command "blah"',
        ])
        writer.write(b"status\n")
        request = await self.receive()
        self.assertEqual(request["method"], "status")  # clear was skipped
        await self.send({"id": request["id"], "result": {"volume": 0.24, "queue": [],
                         "player": {"current": None, "state": "stopped", "position_ms": 0}}})
        self.assertIn("volume: 24", await self.reply(reader))

    async def test_invalid_frame_and_shutdown_with_idler(self):
        reader, writer = await self.connect()
        writer.write(b"x" * (mpd.MAX_LINE + 1) + b"\n")
        self.assertEqual(await asyncio.wait_for(reader.read(), 3), b"")
        reader, writer = await self.connect()
        writer.write(b"idle\n")
        await self.send({"id": 91, "method": "shutdown"})
        self.assertEqual(await self.receive(), {"id": 91, "result": None})
        self.assertEqual(await asyncio.wait_for(reader.read(), 3), b"")
        self.assertEqual(await asyncio.wait_for(self.plugin.wait(), 3), 0)


if __name__ == "__main__":
    unittest.main()
