#!/usr/bin/env python3
"""Loopback MPD frontend for Lark; no network requests to music sources."""

import asyncio
import ipaddress
import itertools
import json
import re
import sys
import time


VERSION = "0.1.0"
MPD_GREETING = "OK MPD 0.24.0\n"
MAX_CLIENTS = 32
MAX_LINE = 16 * 1024
MAX_BATCH_BYTES = 512 * 1024
MAX_BATCH_COMMANDS = 1024
MAX_RPC_LINE = 8 * 1024 * 1024
RPC_TIMEOUT = 30
WRITE_TIMEOUT = 5
PAGE_SIZE = 500
MAX_TRACKS = 100_000
MAX_CATALOGUE_BYTES = 32 * 1024 * 1024

SUBSYSTEMS = frozenset({"database", "update", "playlist", "player", "mixer"})
EVENTS = {
    "player.state": {"player"},
    "player.volume": {"mixer"},
    "queue.inserted": {"playlist"},
    "queue.removed": {"playlist"},
    "queue.cleared": {"playlist"},
    "library.updated": {"database", "update"},
    "resync": SUBSYSTEMS,
}

TAGS = {
    "Artist": "artists",
    "AlbumArtist": "album_artists",
    "Album": "album",
    "Title": "title",
    "Track": "track_no",
    "Disc": "disc_no",
    "Date": "date",
    "MUSICBRAINZ_TRACKID": "mbid",
    "MUSICBRAINZ_ALBUMID": "release_mbid",
    "Genre": None,
    "Composer": None,
    "Performer": None,
    "Comment": None,
    "Label": None,
    "OriginalDate": None,
}
TAG_NAMES = {name.lower(): name for name in TAGS}


class MPDError(Exception):
    def __init__(self, message, code=2):
        super().__init__(message)
        self.code = code


class CoreError(Exception):
    pass


def clean(value):
    # Never let a source's metadata or an RPC error forge protocol lines.
    return str(value).replace("\r", " ").replace("\n", " ").replace("\0", " ")


def field(key, value):
    return f"{key}: {clean(value)}\n"


def bind_address(config):
    if not isinstance(config, dict):
        raise ValueError("MPD config must be an object")
    host = config.get("host", "127.0.0.1")
    port = config.get("port", 6600)
    if not isinstance(host, str) or "%" in host:
        raise ValueError("MPD host must be a loopback IP address or localhost")
    if host == "localhost":
        host = "127.0.0.1"
    try:
        addr = ipaddress.ip_address(host)
    except ValueError:
        raise ValueError("MPD host must be a loopback IP address or localhost") from None
    if not addr.is_loopback:
        raise ValueError("MPD refuses non-loopback binds (no authentication)")
    if type(port) is not int or not 0 <= port <= 65535:
        raise ValueError("MPD port must be an integer in 0..65535")
    return str(addr), port


def split_command(line):
    """MPD uses double quotes and backslashes, not shell expansions."""
    args = []
    i = 0
    while i < len(line):
        if line[i] in " \t":
            i += 1
            continue
        quoted = line[i] == '"'
        i += quoted
        chars = []
        while i < len(line):
            char = line[i]
            if quoted and char == '"':
                i += 1
                if i < len(line) and line[i] not in " \t":
                    raise MPDError("expected whitespace after quoted argument")
                break
            if not quoted and char in " \t":
                break
            if char == "\\":
                i += 1
                if i == len(line):
                    raise MPDError("incomplete escape")
                char = line[i]
            chars.append(char)
            i += 1
        else:
            if quoted:
                raise MPDError("unterminated quoted argument")
        args.append("".join(chars))
    if not args or not re.fullmatch(r"[a-z][a-z0-9_]*", args[0]):
        raise MPDError("invalid command")
    return args[0], args[1:]


def integer(value, name="integer", low=0, high=(1 << 64) - 1):
    if not re.fullmatch(r"-?[0-9]{1,20}", value) or not low <= int(value) <= high:
        raise MPDError(f"invalid {name}")
    return int(value)


def bounds(arg, *, single=True):
    if arg is None:
        return 0, None
    start, colon, end = arg.partition(":")
    first = integer(start, "range start")
    if not colon:
        if not single:
            raise MPDError("expected start:end range")
        return first, first + 1
    last = integer(end, "range end") if end else None
    if last is not None and last < first:
        raise MPDError("range end is before start")
    return first, last


def seek_offset(value, relative_ok=False):
    if not re.fullmatch(r"[+-]?(?:[0-9]{1,12}(?:\.[0-9]*)?|\.[0-9]+)", value):
        raise MPDError("invalid seek time")
    relative = relative_ok and value[0] in "+-"
    seconds = float(value)
    if seconds < 0 and not relative:
        raise MPDError("negative seek time")
    return int(seconds * 1000), relative


def tag_name(name, *, special=()):
    lower = name.lower()
    if lower in special:
        return lower
    if lower not in TAG_NAMES:
        raise MPDError(f"unsupported tag {name!r}")
    return TAG_NAMES[lower]


def tag_values(track, tag):
    if tag == "file":
        return (track["uri"],)
    key = TAGS[tag]
    if key is None:
        return tuple(v for k, v in track.get("tags", []) if k.lower() == tag.lower())
    value = track.get(key)
    if isinstance(value, list):
        return value
    return () if value is None else (str(value),)


def track_lines(track, pos=None, item_id=None):
    yield field("file", track["uri"])
    if track.get("duration_ms") is not None:
        duration = track["duration_ms"]
        yield field("Time", duration // 1000)
        yield field("duration", f"{duration / 1000:.3f}")
    for tag in TAGS:
        for value in tag_values(track, tag):
            yield field(tag, value)
    if pos is not None:
        yield field("Pos", pos)
        yield field("Id", item_id)


# An intentionally small MPD expression grammar. No Python eval or client regexes.
EXPR_TOKEN = re.compile(r"""\s*(\(|\)|==|!=|[a-zA-Z][a-zA-Z0-9_]*|'(?:[^'\\]|\\.)*'|"(?:[^"\\]|\\.)*")""")


def expression(text):
    tokens = []
    text = text.strip()
    pos = 0
    while pos < len(text):
        match = EXPR_TOKEN.match(text, pos)
        if match is None or len(tokens) >= 256:
            raise MPDError("unsupported filter expression")
        tokens.append(match[1])
        pos = match.end()
    tokens = iter(tokens)
    token = next(tokens, None)
    clauses = []

    def take():
        nonlocal token
        value, token = token, next(tokens, None)
        return value

    def term(depth):
        if depth > 16:
            raise MPDError("filter too deeply nested")
        if token == "(":
            take()
            term(depth + 1)
            while token == "AND":
                take()
                term(depth + 1)
            if take() != ")":
                raise MPDError("expected ')' or AND in filter")
            return
        name, op, value = take(), take(), take()
        if (not name or op not in ("==", "!=", "contains", "starts_with") or not value
                or value[0] not in "'\""):
            raise MPDError("unsupported filter expression")
        clauses.append((tag_name(name, special=("file", "any", "base")), op,
                        re.sub(r"\\(.)", r"\1", value[1:-1])))

    term(0)
    if token is not None:
        raise MPDError("unsupported filter expression")
    return clauses


def filters(args, *, search=False, allow_options=False):
    remaining = iter(args)
    clauses, sort, window = [], None, (0, None)
    has_window = False
    if args and args[0].lstrip().startswith("("):
        clauses = expression(next(remaining))
    for key in remaining:
        value = next(remaining, None)
        if value is None:
            raise MPDError(f"missing value for {key!r}")
        if allow_options and key == "sort" and sort is None:
            sort = (tag_name(value.lstrip("-"), special=("file",)), value.startswith("-"))
        elif allow_options and key == "window" and not has_window:
            window = bounds(value, single=False)
            has_window = True
        elif args[0].lstrip().startswith("(") or len(clauses) >= 32:
            raise MPDError("invalid filter options")
        else:
            clauses.append((tag_name(key, special=("file", "any", "base")),
                            "contains" if search else "==", value))
    return clauses, sort, window


def track_filter(clauses, *, search=False):
    # MPD's file/base comparisons are case sensitive, including in search.
    prepared = [(tag, op, needle if not search or tag in ("file", "base") else needle.casefold())
                for tag, op, needle in clauses]

    def matches(track):
        for tag, op, needle in prepared:
            if tag == "base":
                path = track["uri"]
                matched = not needle or path == needle or path.startswith(needle.rstrip("/") + "/")
            else:
                values = (itertools.chain.from_iterable(tag_values(track, t) for t in TAGS)
                          if tag == "any" else tag_values(track, tag))
                # MPD can select tagged values or tracks without that tag (empty equality).
                values = iter(values)
                first = next(values, "")
                values = itertools.chain((first,), values)
                if search and tag != "file":
                    values = (v.casefold() for v in values)
                if op == "contains":
                    matched = any(needle in v for v in values)
                elif op == "starts_with":
                    matched = any(v.startswith(needle) for v in values)
                else:
                    matched = any(v == needle for v in values)
            if matched == (op == "!="):
                return False
        return True
    return matches


def matching_tracks(catalogue, args, *, search=False, allow_options=False):
    clauses, sort, (first, last) = filters(args, search=search, allow_options=allow_options)
    matches = filter(track_filter(clauses, search=search), catalogue.values())
    if sort is not None:
        name, descending = sort

        def key(track):
            value = next(iter(tag_values(track, name)), "")
            if name in ("Track", "Disc"):
                return int(value) if value.isdecimal() else -1
            return value.casefold()
        matches = sorted(matches, key=key, reverse=descending)
    # Slice with Python-size integers; islice rejects indexes greater than sys.maxsize.
    return itertools.islice(matches, min(first, MAX_TRACKS),
                            None if last is None else min(last, MAX_TRACKS))


class StdioRPC:
    """One independent reader routes daemon requests, replies and notifications."""

    def __init__(self, reader, writer):
        self.reader = reader
        self.writer = writer
        self.pending = {}
        self.ids = itertools.count(1)
        self.write_lock = asyncio.Lock()

    async def send(self, msg):
        data = (json.dumps(msg, ensure_ascii=False, separators=(",", ":")) + "\n").encode()
        if len(data) > MAX_RPC_LINE:
            raise CoreError("request exceeds stdio frame limit")
        async with self.write_lock:
            self.writer.write(data)
            await asyncio.wait_for(self.writer.drain(), WRITE_TIMEOUT)

    async def call(self, method, params=None):
        request_id = next(self.ids)
        future = asyncio.get_running_loop().create_future()
        self.pending[request_id] = future
        try:
            async def exchange():
                request = {"id": request_id, "method": method}
                if params is not None:
                    request["params"] = params
                await self.send(request)
                return await future

            msg = await asyncio.wait_for(exchange(), RPC_TIMEOUT)
            if msg.get("error") is not None:
                error = msg["error"]
                raise CoreError(error.get("message", "core request failed"))
            return msg.get("result")
        except asyncio.TimeoutError:
            raise CoreError(f"core request {method} timed out") from None
        except OSError:
            raise CoreError("core connection closed") from None
        finally:
            self.pending.pop(request_id, None)

    def response(self, msg):
        future = self.pending.get(msg["id"])
        if future is not None and not future.done():
            future.set_result(msg)

    async def receive(self):
        data = await self.reader.readline()
        if not data:
            return None
        if not data.endswith(b"\n"):
            raise ValueError("truncated stdio frame")
        msg = json.loads(data)
        if (not isinstance(msg, dict) or not ("id" in msg or "method" in msg)
                or ("id" in msg and type(msg["id"]) is not int)
                or ("method" in msg and not isinstance(msg["method"], str))):
            raise ValueError("invalid stdio message")
        return msg


class Catalogue:
    """A disposable metadata view; the authoritative catalogue remains in larkd."""

    def __init__(self, rpc):
        self.rpc = rpc
        self.lock = asyncio.Lock()
        self.generation = 0
        self.snapshot = None
        self.updated_at = 0

    def invalidate(self):
        self.generation += 1
        self.snapshot = None
        self.updated_at = int(time.time())

    async def get(self):
        if self.snapshot is not None:
            return self.snapshot
        # A whole refresh has a deadline, in addition to each individual RPC.
        try:
            return await asyncio.wait_for(self.load(), RPC_TIMEOUT)
        except asyncio.TimeoutError:
            raise CoreError("catalogue refresh timed out") from None

    async def load(self):
        async with self.lock:
            if self.snapshot is not None:
                return self.snapshot
            generation = self.generation
            tracks = {}
            artists, albums = set(), set()
            duration = size = 0
            params = {"limit": PAGE_SIZE}
            cursor = None
            while True:
                page = await self.rpc.call("library.tracks", params)
                if self.generation != generation:
                    raise CoreError("catalogue changed during refresh; retry the command")
                if not isinstance(page, list) or len(page) > PAGE_SIZE:
                    raise CoreError("core returned invalid catalogue page")
                size += len(json.dumps(page, ensure_ascii=False).encode())
                if len(tracks) + len(page) > MAX_TRACKS or size > MAX_CATALOGUE_BYTES:
                    raise CoreError("catalogue exceeds MPD frontend cache limit")
                for track in page:
                    uri = track["uri"]
                    if cursor is not None and cursor >= uri:
                        raise CoreError("core returned unordered catalogue page")
                    cursor = uri
                    tracks[uri] = track
                    artists.update(track.get("artists", []))
                    if track.get("album"):
                        albums.add((tuple(track.get("album_artists") or track.get("artists", [])),
                                    track["album"]))
                    duration += track.get("duration_ms") or 0
                if len(page) < PAGE_SIZE:
                    break
                params["after_uri"] = cursor
            if not self.updated_at:
                self.updated_at = int(time.time())
            self.snapshot = tracks, len(artists), len(albums), duration // 1000
            return self.snapshot


class Frontend:
    def __init__(self, rpc):
        self.rpc = rpc
        self.catalogue = Catalogue(rpc)
        self.initialized = asyncio.Event()
        self.server = None
        self.clients = set()
        self.tasks = set()
        self.status_lock = asyncio.Lock()
        self.queue_ids = ()
        self.queue_version = 0
        self.started = time.monotonic()

    def connect(self, reader, writer):
        if len(self.clients) >= MAX_CLIENTS:
            writer.close()
            return
        client = MPDClient(self, reader, writer)
        self.clients.add(client)
        task = asyncio.create_task(client.run())
        self.tasks.add(task)

        def done(task):
            self.tasks.discard(task)
            self.clients.discard(client)
            if not task.cancelled() and task.exception():
                print(f"MPD client: {clean(task.exception())}", file=sys.stderr, flush=True)

        task.add_done_callback(done)

    def event(self, method):
        if method in ("library.updated", "resync"):
            self.catalogue.invalidate()
        for subsystem in EVENTS.get(method, ()):
            for client in self.clients:
                client.changed.add(subsystem)
                client.wake.set()

    async def status(self):
        # Compute revisions from the same snapshot used to generate the reply.
        # Core events and RPC results can arrive independently over stdio.
        async with self.status_lock:
            status = await self.rpc.call("status")
            ids = tuple(item["id"] for item in status["queue"])
            if ids != self.queue_ids:
                self.queue_ids = ids
                self.queue_version = (self.queue_version + 1) & 0xFFFFFFFF
            return status, self.queue_version

    async def start(self):
        await self.initialized.wait()
        await self.rpc.call("subscribe")
        await self.server.start_serving()
        host, port, *_ = self.server.sockets[0].getsockname()
        if ":" in host:
            host = f"[{host}]"
        print(f"MPD listening on {host}:{port}", file=sys.stderr, flush=True)

    async def pump(self):
        while True:
            msg = await self.rpc.receive()
            if msg is None:
                return
            method = msg.get("method")
            if method is None:
                self.rpc.response(msg)
            elif "id" not in msg:
                self.event(method)
            elif method == "initialize" and self.server is None:
                try:
                    params = msg.get("params")
                    if not isinstance(params, dict) or params.get("api") != 1:
                        raise ValueError("unsupported plugin API (expected version 1)")
                    host, port = bind_address(params.get("config", {}))
                    self.server = await asyncio.start_server(
                        self.connect, host, port, limit=MAX_LINE, start_serving=False,
                    )
                except (OSError, ValueError) as error:
                    await self.rpc.send({"id": msg["id"], "error": {
                        "code": -32602, "message": str(error),
                    }})
                    continue
                await self.rpc.send({"id": msg["id"], "result": {
                    "name": "mpd", "role": "frontend", "scheme": "", "cache": "none",
                    "version": VERSION,
                }})
                self.initialized.set()
            elif method == "shutdown":
                await self.rpc.send({"id": msg["id"], "result": None})
                return
            else:
                await self.rpc.send({"id": msg["id"], "error": {
                    "code": -32601, "message": f"unsupported frontend method {method}",
                }})

    async def close(self):
        if self.server is not None:
            self.server.close()
        tasks = tuple(self.tasks)
        for client in self.clients:
            client.writer.transport.abort()
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        if self.server is not None:
            await self.server.wait_closed()


def current_position(status):
    item_id = status["player"]["current"]
    return next((i for i, item in enumerate(status["queue"]) if item["id"] == item_id), None)


def item_position(queue, value):
    item_id = integer(value, "song ID")
    index = next((i for i, item in enumerate(queue) if item["id"] == item_id), None)
    if index is None:
        raise MPDError("no such song", 50)
    return index


def queue_range(queue, arg, *, strict=False):
    first, last = bounds(arg)
    if arg and ":" not in arg and first >= len(queue):
        raise MPDError("no such song", 50)
    if strict and first >= len(queue):
        raise MPDError("no such song", 50)
    return enumerate(queue[first:last], start=first)


def queue_lines(queue, arg, *, pos_only=False):
    for index, item in queue_range(queue, arg):
        if pos_only:
            yield field("cpos", index)
            yield field("Id", item["id"])
        else:
            yield from track_lines(item["track"], index, item["id"])


def volume(status):
    return max(0, min(100, round(status["volume"] * 100)))


def status_lines(status, version):
    player = status["player"]
    queue = status["queue"]
    state = {"playing": "play", "paused": "pause", "stopped": "stop"}[player["state"]]
    values = {
        "volume": volume(status), "repeat": 0, "random": 0, "single": 0, "consume": 0,
        "playlist": version, "playlistlength": len(queue), "state": state,
    }
    index = current_position(status)
    if index is not None:
        values.update(song=index, songid=queue[index]["id"])
        if state != "stop":
            next_item = queue[index + 1:index + 2]
            if next_item:
                values.update(nextsong=index + 1, nextsongid=next_item[0]["id"])
            elapsed = player["position_ms"]
            values["elapsed"] = f"{elapsed / 1000:.3f}"
            duration = player.get("duration_ms")
            if duration is not None:
                values["time"] = f"{elapsed // 1000}:{duration // 1000}"
                values["duration"] = f"{duration / 1000:.3f}"
    return (field(key, value) for key, value in values.items())


STATUS_ARGS = {
    "status": (0, 0), "getvol": (0, 0), "currentsong": (0, 0),
    "playlistinfo": (0, 1), "playlistid": (0, 1), "plchanges": (1, 2),
    "plchangesposid": (1, 2),
}
LIBRARY_ARGS = {
    "stats": (0, 0), "lsinfo": (0, 1), "listall": (0, 1), "listallinfo": (0, 1),
    "list": (1, 80), "find": (1, 70), "search": (1, 70), "count": (1, 70),
}
CONTROL_ARGS = {
    "add": (1, 1), "addid": (1, 2), "delete": (1, 1), "deleteid": (1, 1),
    "clear": (0, 0), "play": (0, 1), "playid": (0, 1), "pause": (0, 1),
    "stop": (0, 0), "next": (0, 0), "previous": (0, 0), "seek": (2, 2),
    "seekid": (2, 2), "seekcur": (1, 1), "setvol": (1, 1),
}
INFO_ARGS = {
    "commands": (0, 0), "tagtypes": (0, 0), "urlhandlers": (0, 0), "outputs": (0, 0),
    "binarylimit": (1, 1), "ping": (0, 0), "decoders": (0, 0),
}
COMMANDS = {**STATUS_ARGS, **LIBRARY_ARGS, **CONTROL_ARGS, **INFO_ARGS,
            "idle": (0, 32), "noidle": (0, 0), "close": (0, 0)}


def validate_command(cmd, args):
    limits = COMMANDS.get(cmd)
    if limits is None:
        raise MPDError(f'unknown command "{cmd}"', 5)
    if not limits[0] <= len(args) <= limits[1]:
        raise MPDError("wrong number of arguments")


class Commands:
    def __init__(self, frontend):
        self.frontend = frontend
        self.call = frontend.rpc.call

    async def execute(self, cmd, args):
        if cmd in STATUS_ARGS:
            return await self.read_status(cmd, args)
        if cmd in LIBRARY_ARGS:
            return await self.read_library(cmd, args)
        if cmd in CONTROL_ARGS:
            return await self.control(cmd, args)
        if cmd == "commands":
            return (field("command", name) for name in sorted(COMMANDS))
        if cmd == "tagtypes":
            return (field("tagtype", tag) for tag in TAGS)
        if cmd == "outputs":
            # Lark has one configured logical output, with no MPD output toggles.
            return ("outputid: 0\n", "outputname: Lark\n", "outputenabled: 1\n", "plugin: lark\n")
        if cmd == "decoders":
            # Common formats supported by the core's Symphonia build, not an
            # exhaustive probe of all codec/container combinations.
            return ("plugin: symphonia\n", "suffix: wav\n", "suffix: flac\n", "suffix: mp3\n",
                    "suffix: ogg\n", "suffix: m4a\n")
        if cmd == "binarylimit":
            # rmpc negotiates this on startup even when no binary commands exist.
            integer(args[0], "binary limit", low=64, high=1 << 30)
        return ()  # ping, urlhandlers (no external URL ingestion), binarylimit

    async def read_status(self, cmd, args):
        status, version = await self.frontend.status()
        queue = status["queue"]
        if cmd == "status":
            return status_lines(status, version)
        if cmd == "getvol":
            return (field("volume", volume(status)),)
        if cmd == "currentsong":
            index = current_position(status)
            return () if index is None else track_lines(queue[index]["track"], index, queue[index]["id"])
        if cmd == "playlistid" and args:
            index = item_position(queue, args[0])
            return track_lines(queue[index]["track"], index, queue[index]["id"])
        if cmd.startswith("plchanges"):
            old = integer(args[0], "playlist version", high=0xFFFFFFFF)
            if len(args) == 2:
                bounds(args[1], single=False)
            # Versions other than the current one force a complete refresh within
            # the requested range. No unbounded per-version journal is kept.
            return () if old == version else queue_lines(
                queue, args[1] if len(args) == 2 else None, pos_only=cmd == "plchangesposid",
            )
        return queue_lines(queue, args[0] if args else None)

    async def read_library(self, cmd, args):
        catalogue, artists, albums, playtime = await self.frontend.catalogue.get()
        if cmd == "stats":
            return (
                field("artists", artists), field("albums", albums), field("songs", len(catalogue)),
                field("uptime", int(time.monotonic() - self.frontend.started)),
                field("db_playtime", playtime), field("db_update", self.frontend.catalogue.updated_at),
            )
        if cmd in {"lsinfo", "listall", "listallinfo"}:
            path = args[0] if args else ""
            if path in ("", "/"):
                tracks = catalogue.values()
            elif path in catalogue:
                tracks = (catalogue[path],)
            else:
                raise MPDError("no such file or directory", 50)
            if cmd == "listall":
                return (field("file", track["uri"]) for track in tracks)
            return itertools.chain.from_iterable(map(track_lines, tracks))
        if cmd == "list":
            return list_tags(catalogue, args)
        tracks = matching_tracks(catalogue, args, search=cmd == "search", allow_options=cmd != "count")
        if cmd == "count":
            count = duration = 0
            for track in tracks:
                count += 1
                duration += track.get("duration_ms") or 0
            return (field("songs", count), field("playtime", duration // 1000))
        return itertools.chain.from_iterable(map(track_lines, tracks))

    async def control(self, cmd, args):
        simple = {"clear": "queue.clear", "stop": "player.stop", "next": "player.next",
                  "previous": "player.prev"}
        if cmd in simple:
            await self.call(simple[cmd])
        elif cmd == "pause":
            flag = integer(args[0], "pause", high=1) if args else None
            if flag == 1:
                await self.call("player.pause")
            else:
                status, _ = await self.frontend.status()
                if status["player"]["state"] != "stopped":
                    await self.call("player.toggle" if flag is None else "player.resume")
        elif cmd == "setvol":
            await self.call("player.volume", {"volume": integer(args[0], "volume", high=100) / 100})
        elif cmd in {"add", "addid"}:
            catalogue, *_ = await self.frontend.catalogue.get()
            if args[0] not in catalogue:
                raise MPDError("no such song in catalogue", 50)
            params = {"uris": [args[0]]}
            if len(args) == 2:
                params["position"] = integer(args[1], "queue position")
            items = await self.call("queue.add", params)
            if cmd == "addid":
                return (field("Id", items[0]["id"]),)
        elif cmd == "deleteid":
            status, _ = await self.frontend.status()
            queue = status["queue"]
            index = item_position(queue, args[0])
            await self.call("queue.remove", {"ids": [queue[index]["id"]]})
        elif cmd == "delete":
            status, _ = await self.frontend.status()
            items = queue_range(status["queue"], args[0], strict=True)
            await self.call("queue.remove", {"ids": [item["id"] for _, item in items]})
        elif cmd in ("play", "playid"):
            if not args or args[0] == "-1":
                # Ensures that `play` during playback does not restart the track.
                await self.call("player.resume")
            else:
                status, _ = await self.frontend.status()
                queue = status["queue"]
                index = (integer(args[0], "queue position") if cmd == "play"
                         else item_position(queue, args[0]))
                if index >= len(queue):
                    raise MPDError("no such song", 50)
                await self.call("player.play", {"index": index})
        else:  # seek, seekid, seekcur
            await self.seek(cmd, args)
        return ()

    async def seek(self, cmd, args):
        position, relative = seek_offset(args[-1], relative_ok=cmd == "seekcur")
        status, _ = await self.frontend.status()
        queue, player = status["queue"], status["player"]
        index = current_position(status)
        if cmd != "seekcur":
            target = item_position(queue, args[0]) if cmd == "seekid" else integer(args[0], "queue position")
            if target >= len(queue):
                raise MPDError("no such song", 50)
            if index != target or player["state"] == "stopped":
                await self.call("player.play", {"index": target})
        elif index is None or player["state"] == "stopped":
            raise MPDError("not playing", 55)
        if relative:
            position += player["position_ms"]
        await self.call("player.seek", {"position_ms": max(0, position)})


def list_tags(catalogue, args):
    target = tag_name(args[0], special=("file",))
    filters_args, remaining = args[1:], []
    if "group" in filters_args:
        # A quoted legacy search value can itself be "group". Group keywords
        # start after a complete expression or a whole number of tag/value pairs.
        first = 1 if filters_args[0].startswith("(") else 0
        for i in range(first, len(filters_args) - 1, 2):
            if filters_args[i] == "group":
                filters_args, remaining = filters_args[:i], filters_args[i:]
                break
    if len(remaining) > 6 or len(remaining) % 2 or any(key != "group" for key in remaining[::2]):
        raise MPDError("invalid list grouping")
    groups = [tag_name(arg) for arg in remaining[1::2]]
    if target == "Album" and len(filters_args) == 1 and not filters_args[0].startswith("("):
        filters_args = ["Artist", filters_args[0]]  # legacy `list album ARTIST`
    rows = set()
    for track in matching_tracks(catalogue, filters_args):
        columns = [set(tag_values(track, group)) or {""} for group in groups]
        columns.append(set(tag_values(track, target)))
        combinations = 1
        for column in columns:
            combinations *= len(column)
        if combinations > MAX_TRACKS:
            raise MPDError("too many list values", 52)
        for row in itertools.product(*columns):
            rows.add(row)
            if len(rows) > MAX_TRACKS:
                raise MPDError("too many list values", 52)
    return grouped_lines(groups + [target], sorted(rows))


def grouped_lines(columns, rows):
    previous = ()
    for row in rows:
        changed = False
        for i, (column, value) in enumerate(zip(columns, row)):
            changed |= i >= len(previous) or previous[i] != value
            if changed:
                yield field(column, value)
        previous = row


class MPDClient:
    def __init__(self, frontend, reader, writer):
        self.commands = Commands(frontend)
        self.reader = reader
        self.writer = writer
        self.read_task = None
        self.changed = set()
        self.wake = asyncio.Event()

    async def lines(self, lines):
        chunk = bytearray()
        for line in lines:
            chunk.extend(line.encode())
            if len(chunk) >= 32 * 1024:
                await self.write(chunk)
                chunk.clear()
        if chunk:
            await self.write(chunk)

    async def write(self, data):
        self.writer.write(data)
        try:
            await asyncio.wait_for(self.writer.drain(), WRITE_TIMEOUT)
        except asyncio.TimeoutError:
            self.writer.transport.abort()
            raise

    async def ack(self, command, error, index=0):
        if isinstance(error, CoreError):
            error = MPDError(str(error), 52)
        await self.lines((f"ACK [{error.code}@{index}] {{{command}}} {clean(error)}\n",))

    async def read_line(self):
        line = await self.reader.readline()
        if not line or not line.endswith(b"\n"):
            return None
        return line.decode().removesuffix("\n").removesuffix("\r")

    async def read(self):
        if self.read_task is None:
            return await self.read_line()
        task, self.read_task = self.read_task, None
        return await task

    async def idle(self, args):
        requested = frozenset(args) if args else SUBSYSTEMS
        while True:
            events = self.changed & requested
            if events:
                self.changed.difference_update(events)
                await self.lines(itertools.chain((field("changed", s) for s in sorted(events)), ("OK\n",)))
                return True
            self.wake.clear()
            if self.read_task is None:
                self.read_task = asyncio.create_task(self.read_line())
            wake_task = asyncio.create_task(self.wake.wait())
            try:
                await asyncio.wait((self.read_task, wake_task), return_when=asyncio.FIRST_COMPLETED)
            finally:
                wake_task.cancel()
                await asyncio.gather(wake_task, return_exceptions=True)
            # If both are ready, deliver the change first and leave the input
            # for the outer loop. It ignores a late noidle without a second OK.
            if self.changed & requested:
                continue
            if self.read_task.done():
                line = await self.read()
                if line == "noidle":
                    await self.lines(("OK\n",))
                    return True
                return False  # EOF or a command other than noidle during idle

    async def execute(self, line, *, in_batch=False):
        cmd = ""
        try:
            cmd, args = split_command(line)
            validate_command(cmd, args)
            if in_batch and cmd in ("idle", "noidle", "close"):
                raise MPDError("command is not allowed in command lists")
            if cmd == "close":
                return False
            if cmd == "idle":
                return await self.idle(args)
            if cmd == "noidle":
                return True  # idle already finished; do not add an OK
            lines = await self.commands.execute(cmd, args)
            await self.lines(lines)
        except (MPDError, CoreError) as error:
            if in_batch:
                raise BatchError(cmd, error) from error
            await self.ack(cmd, error)
            return True
        if not in_batch:
            await self.lines(("OK\n",))
        return True

    async def batch(self, with_ok):
        lines = []
        size = 0
        while True:
            line = await self.read()
            if line is None:
                return False
            if line == "command_list_end":
                break
            size += len(line.encode()) + 1
            if len(lines) >= MAX_BATCH_COMMANDS or size > MAX_BATCH_BYTES:
                await self.ack("command_list_end", MPDError("command list too large"), len(lines))
                return False
            lines.append(line)
        for index, line in enumerate(lines):
            try:
                await self.execute(line, in_batch=True)
                if with_ok:
                    await self.lines(("list_OK\n",))
            except BatchError as error:
                await self.ack(error.command, error.cause, index)
                return True
        await self.lines(("OK\n",))
        return True

    async def run(self):
        try:
            await self.lines((MPD_GREETING,))
            while (line := await self.read()) is not None:
                if line in ("command_list_begin", "command_list_ok_begin"):
                    if not await self.batch(line == "command_list_ok_begin"):
                        return
                elif not await self.execute(line):
                    return
        except (OSError, ValueError, asyncio.TimeoutError):
            # Client disconnected, timed out writing, or sent an invalid frame.
            pass
        finally:
            if self.read_task is not None:
                self.read_task.cancel()
                await asyncio.gather(self.read_task, return_exceptions=True)
            self.writer.close()
            try:
                await asyncio.wait_for(self.writer.wait_closed(), WRITE_TIMEOUT)
            except OSError:
                pass
            except asyncio.TimeoutError:
                self.writer.transport.abort()


class BatchError(Exception):
    def __init__(self, command, cause):
        super().__init__(str(cause))
        self.command = command
        self.cause = cause


async def main():
    loop = asyncio.get_running_loop()
    reader = asyncio.StreamReader(limit=MAX_RPC_LINE)
    read_pipe, _ = await loop.connect_read_pipe(lambda: asyncio.StreamReaderProtocol(reader), sys.stdin.buffer)
    write_pipe, write_protocol = await loop.connect_write_pipe(asyncio.streams.FlowControlMixin, sys.stdout.buffer)
    writer = asyncio.StreamWriter(write_pipe, write_protocol, None, loop)
    frontend = Frontend(StdioRPC(reader, writer))
    tasks = (asyncio.create_task(frontend.pump()), asyncio.create_task(frontend.start()))
    try:
        finished, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        for task in finished:
            task.result()
        if tasks[0] not in finished:
            await tasks[0]
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await frontend.close()
        read_pipe.close()
        write_pipe.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (CoreError, ValueError, OSError, asyncio.TimeoutError) as error:
        print(f"MPD frontend: {clean(error)}", file=sys.stderr, flush=True)
        sys.exit(1)
