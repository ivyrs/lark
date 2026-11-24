#!/usr/bin/env python3
"""Navidrome source for the Lark v1 stdio plugin protocol (Unix, stdlib only)."""

import contextlib
import fcntl
import hashlib
import http.client
import json
import os
from pathlib import Path
import re
import secrets
import signal
import sqlite3
import stat
import sys
import time
from urllib.parse import quote, unquote, urlencode, urlsplit


URI_PREFIX = "navidrome:track:"
RPC_TIMEOUT = 24  # larkd waits 30 seconds, including writing the reply
IO_TIMEOUT = 8
JSON_LIMIT = 8 * 1024 * 1024


class PluginError(Exception):
    def __init__(self, message, code=-32603):
        super().__init__(message)
        self.code = code


def invalid(message):
    return PluginError(message, -32602)


def deadline_expired(_signum, _frame):
    raise PluginError("Navidrome operation exceeded its time limit")


@contextlib.contextmanager
def deadline():
    # A socket timeout alone does not bound DNS, a trickling response, or a
    # slow SQLite operation. The Unix stdio process is serial.
    previous = signal.signal(signal.SIGALRM, deadline_expired)
    try:
        signal.setitimer(signal.ITIMER_REAL, RPC_TIMEOUT)
        yield
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous)


def configured_url(value, allow_http):
    if not isinstance(value, str) or any(ord(c) <= 32 or ord(c) == 127 for c in value):
        raise invalid("Navidrome url must be an absolute HTTPS URL")
    try:
        parts = urlsplit(value)
        if (parts.scheme not in {"https", "http"} or not parts.hostname
                or "@" in parts.netloc or "?" in value or "#" in value or "\\" in value):
            raise ValueError
        host = parts.hostname.encode("idna").decode("ascii")
        port = parts.port
        if port == 0:
            raise ValueError
        path = quote(parts.path.rstrip("/"), safe="/%:@!$&'()*+,;=")
    except ValueError:
        raise invalid("Navidrome url must be an absolute URL without credentials, query or fragment") from None
    if parts.scheme == "http" and not allow_http:
        raise invalid("Navidrome requires HTTPS unless allow_http is explicitly true")
    host = f"[{host}]" if ":" in host else host
    authority = host if port in {None, 443 if parts.scheme == "https" else 80} else f"{host}:{port}"
    return urlsplit(f"{parts.scheme}://{authority}{path}")


class Subsonic:
    def __init__(self, url, username, password):
        self.url = url
        self.username = username
        self.password = password

    def target(self, endpoint, **params):
        salt = secrets.token_hex(16)
        token = hashlib.md5((self.password + salt).encode(), usedforsecurity=False).hexdigest()
        query = urlencode({"u": self.username, "s": salt, "t": token, "v": "1.16.1",
                           "c": "lark", "f": "json", **params})
        return f"{self.url.path}/rest/{endpoint}.view?{query}"

    def stream(self, song_id):
        target = self.target("stream", id=song_id, format="raw")
        return {"kind": "http", "url": f"{self.url.scheme}://{self.url.netloc}{target}", "headers": []}

    @contextlib.contextmanager
    def get(self, endpoint, **params):
        target = self.target(endpoint, **params)
        # http.client never follows redirects or sends the token through an
        # automatic environment proxy. HTTPSConnection verifies the peer.
        connect = (http.client.HTTPSConnection if self.url.scheme == "https"
                   else http.client.HTTPConnection)
        with contextlib.closing(connect(self.url.netloc, timeout=IO_TIMEOUT)) as connection:
            try:
                connection.request("GET", target)
                response = connection.getresponse()
            except (http.client.HTTPException, OSError):
                # Raw exceptions, response headers and server errors can carry
                # credentials or the full authentication URL.
                raise PluginError(f"Navidrome {endpoint} request failed or timed out") from None
            with response:
                if response.status != 200:
                    if 300 <= response.status < 400:
                        raise PluginError(f"Navidrome {endpoint} refused a redirect; configure the final server URL")
                    if response.status in (401, 403):
                        raise PluginError(f"Navidrome {endpoint} rejected access (HTTP {response.status}); check credentials and server access")
                    raise PluginError(f"Navidrome {endpoint} failed (HTTP {response.status})")
                if response.headers.get("Content-Encoding", "identity").lower() != "identity":
                    raise PluginError(f"Navidrome {endpoint} used an unsupported content encoding")
                yield response

    def request_json(self, endpoint, **params):
        with self.get(endpoint, **params) as response:
            length = content_length(response)
            if length is not None and length > JSON_LIMIT:
                raise PluginError(f"Navidrome {endpoint} response is too large")
            data = read_body(response, JSON_LIMIT + 1)
        if len(data) > JSON_LIMIT:
            raise PluginError(f"Navidrome {endpoint} response is too large")
        if length is not None and len(data) != length:
            raise PluginError(f"Navidrome {endpoint} response was truncated")
        try:
            document = json.loads(data)
        except (ValueError, RecursionError):
            raise PluginError(f"Navidrome {endpoint} did not return a Subsonic JSON response") from None
        envelope = document.get("subsonic-response") if isinstance(document, dict) else None
        if not isinstance(envelope, dict) or envelope.get("status") not in ("ok", "failed"):
            raise PluginError(f"Navidrome {endpoint} returned an invalid Subsonic response")
        if envelope["status"] == "failed":
            error = envelope.get("error")
            code = error.get("code") if isinstance(error, dict) else None
            if type(code) is int and code in (40, 41):
                raise PluginError(f"Navidrome {endpoint} authentication failed; check username and password_env")
            if type(code) is int and 0 <= code <= 999:
                raise PluginError(f"Navidrome {endpoint} failed (Subsonic code {code})")
            raise PluginError(f"Navidrome {endpoint} failed")
        return envelope


def read_body(response, size):
    try:
        return response.read(size)
    except (http.client.HTTPException, OSError):
        raise PluginError("Navidrome response body was interrupted or timed out") from None


def content_length(response):
    lengths = response.headers.get_all("Content-Length", [])
    encoding = response.headers.get_all("Transfer-Encoding", [])
    if encoding and (lengths or len(encoding) != 1 or not response.chunked):
        raise PluginError("Navidrome returned unsupported transfer framing")
    if not lengths:
        return None
    if len(lengths) != 1 or not re.fullmatch(r"[0-9]{1,20}", lengths[0]):
        raise PluginError("Navidrome returned an invalid Content-Length")
    return int(lengths[0])


def string(value):
    if isinstance(value, str) and value and not re.search(r"[\ud800-\udfff]", value):
        return value
    return None


def names(value):
    if not isinstance(value, list):
        return []
    values = (string(item.get("name")) for item in value if isinstance(item, dict))
    return list(dict.fromkeys(name for name in values if name))


def first(*values):
    return next((s for s in map(string, values) if s), None)


def artist_names(people, *fallbacks):
    name = first(*fallbacks)
    return names(people) or ([name] if name else [])


def integer(value, maximum):
    return value if type(value) is int and 0 < value <= maximum else None


def date(value):
    if not isinstance(value, dict):
        return None
    year = integer(value.get("year"), 9999)
    month = integer(value.get("month"), 12)
    day = integer(value.get("day"), 31)
    if year:
        return f"{year:04}" + (f"-{month:02}" + (f"-{day:02}" if day else "") if month else "")
    return None


def track_uri(song_id):
    return URI_PREFIX + quote(song_id, safe="")


def song_id_from_uri(uri):
    if isinstance(uri, str) and uri.startswith(URI_PREFIX):
        try:
            song_id = unquote(uri[len(URI_PREFIX):], errors="strict")
            if string(song_id) and track_uri(song_id) == uri:
                return song_id
        except UnicodeError:
            pass
    raise invalid("expected a navidrome:track: URI")


def track(song):
    song_id = string(song.get("id"))
    title = string(song.get("title"))
    if song_id is None or title is None:
        raise PluginError("Navidrome returned a song without an id or title")
    year = integer(song.get("year"), 9999)
    duration = song.get("duration")
    isrc = song.get("isrc")
    if isinstance(isrc, list):
        isrc = first(*isrc)
    genre = string(song.get("genre"))
    genres = names(song.get("genres")) or ([genre] if genre else [])
    result = {
        "uri": track_uri(song_id),
        "title": title,
        "artists": artist_names(song.get("artists"), song.get("displayArtist"), song.get("artist")),
        "album_artists": artist_names(song.get("albumArtists"), song.get("displayAlbumArtist"), song.get("albumArtist")),
        "album": string(song.get("album")),
        "track_no": integer(song.get("track"), 2**32 - 1),
        "disc_no": integer(song.get("discNumber"), 2**32 - 1),
        "duration_ms": duration * 1000 if type(duration) is int and 0 <= duration <= (2**63 - 1) // 1000 else None,
        # Standard song results have a year and recording MBID, not album-only
        # fields. Preserve additional fields if a server includes them on songs.
        "date": date(song.get("releaseDate")) or date(song.get("originalReleaseDate")) or (f"{year:04}" if year else None),
        "mbid": string(song.get("musicBrainzId")),
        "release_mbid": string(song.get("albumMusicBrainzId")),
        "isrc": string(isrc),
        "tags": [["genre", name] for name in genres] + [["label", name] for name in names(song.get("recordLabels"))],
    }
    return {key: value for key, value in result.items() if value is not None}


def batch(upserts, deletes=(), cursor=None):
    return {"upserts": upserts, "deletes": list(deletes), "next_cursor": cursor, "done": cursor is not None}


class Scan:
    def __init__(self, cursor):
        self.cursor = cursor
        self.pages_processed = 0
        self.tracks_processed = 0

    def pull(self, api, size):
        """Consume one song page; only an empty page ends enumeration."""
        page = api.request_json("search3", query="", artistCount=0, albumCount=0,
                                songCount=size, songOffset=self.tracks_processed)
        result = page.get("searchResult3")
        songs = result.get("song", []) if isinstance(result, dict) else None
        if not isinstance(songs, list) or any(not isinstance(song, dict) for song in songs):
            raise PluginError("Navidrome search3 returned a malformed catalogue response")
        if len(songs) > size:
            raise PluginError("Navidrome search3 returned more songs than requested; retry sync")
        if not songs:
            return None
        tracks = [track(song) for song in songs]
        self.pages_processed += 1
        self.tracks_processed += len(tracks)
        return tracks


class Catalogue:
    """One acknowledged URI baseline and one replayable candidate in SQLite."""

    def __init__(self, db, identity, progress=None):
        self.db = db
        self.progress = progress or (lambda message: None)
        self.last_progress = 0
        self.scan = None
        self.replay_after = 0
        db.execute("BEGIN IMMEDIATE")
        with db:
            db.execute("""CREATE TABLE IF NOT EXISTS state (
                       version INTEGER NOT NULL, identity TEXT NOT NULL,
                       baseline TEXT, candidate TEXT)""")
            # Keep v1's layout, acknowledged baseline, and replayable candidate.
            row = db.execute("SELECT version, identity FROM state").fetchone()
            if row is None:
                db.execute("INSERT INTO state VALUES (1, ?, NULL, NULL)", (identity,))
            elif row != (1, identity):
                raise PluginError("Navidrome state belongs to another server/user or version; use a new source state directory")
            db.execute("CREATE TABLE IF NOT EXISTS baseline (uri TEXT PRIMARY KEY NOT NULL)")
            db.execute("""CREATE TABLE IF NOT EXISTS candidate (
                       seq INTEGER PRIMARY KEY, uri TEXT NOT NULL UNIQUE, track TEXT NOT NULL)""")

    def pull(self, cursor, api, size):
        try:
            return self._pull(cursor, api, size)
        except BaseException:
            self.db.rollback()
            self.replay_after = 0
            self.scan = None
            raise

    def _pull(self, cursor, api, size):
        db = self.db
        if self.scan is None:
            baseline, candidate = db.execute("SELECT baseline, candidate FROM state").fetchone()
            if candidate is not None and cursor == baseline:
                return self.replay(candidate)
            self.replay_after = 0
            if candidate is not None and cursor == candidate:
                # The core stored the final batch and cursor in one transaction.
                with db:
                    db.execute("DELETE FROM baseline")
                    db.execute("INSERT INTO baseline SELECT uri FROM candidate")
                    db.execute("DELETE FROM candidate")
                    db.execute("UPDATE state SET baseline = candidate, candidate = NULL")
            elif cursor is not None and cursor != baseline:
                raise PluginError("unknown Navidrome sync cursor; catalogue state and lark database must match")
            db.execute("BEGIN IMMEDIATE")
            # The scan's transaction stays open between pulls; if any call
            # fails or the process dies, the old complete snapshots survive.
            db.execute("DELETE FROM candidate")
            if cursor is None:
                db.execute("DELETE FROM baseline")
                db.execute("UPDATE state SET baseline = NULL")
            self.scan = Scan(cursor)
            self.last_progress = time.monotonic()
            self.progress("sync started: scanning catalogue; published track count stays unchanged until complete (0 on first sync)")
        elif cursor != self.scan.cursor:
            raise PluginError("Navidrome sync cursor changed during a scan; retry sync")

        upserts = self.scan.pull(api, size)
        if upserts is not None:
            try:
                db.executemany("INSERT INTO candidate(uri, track) VALUES (?, ?)",
                               ((t["uri"], json.dumps(t, separators=(",", ":"))) for t in upserts))
            except sqlite3.IntegrityError:
                raise PluginError("Navidrome returned a repeated song id; retry sync") from None
            # The core commits each batch independently; publishing before
            # the scan succeeds could strand tracks that vanish before retry.
            now = time.monotonic()
            if self.scan.pages_processed % 25 == 0 or now - self.last_progress >= 5:
                self.progress(f"sync scanning: {self.scan.pages_processed} song pages, "
                              f"{self.scan.tracks_processed} tracks staged")
                self.last_progress = now
            return batch([])

        self.progress(f"scan complete: {self.scan.pages_processed} song pages, "
                      f"{self.scan.tracks_processed} tracks; publishing library")
        next_cursor = secrets.token_hex(16)
        db.execute("UPDATE state SET candidate = ?", (next_cursor,))
        db.commit()
        self.scan = None
        return self.replay(next_cursor)

    def deletes(self):
        return [row[0] for row in self.db.execute("""SELECT uri FROM baseline
                WHERE NOT EXISTS (SELECT 1 FROM candidate WHERE candidate.uri = baseline.uri)""")]

    def replay(self, cursor):
        upserts = []
        size = 0
        with contextlib.closing(self.db.execute(
            "SELECT seq, track FROM candidate WHERE seq > ? ORDER BY seq LIMIT 501",
            (self.replay_after,),
        )) as rows:
            for seq, encoded in rows:
                if upserts and (len(upserts) == 500 or size + len(encoded) > JSON_LIMIT):
                    return batch(upserts)
                upserts.append(json.loads(encoded))
                self.replay_after = seq
                size += len(encoded)
        deletes = self.deletes()
        self.replay_after = 0
        return batch(upserts, deletes, cursor)


def lstat(path):
    try:
        return path.lstat()
    except FileNotFoundError:
        return None


class Source:
    def __init__(self, progress=None):
        self.progress = progress
        self.api = None
        self.lock = None
        self.db = None

    def close(self):
        try:
            if self.db is not None:
                self.db.close()
        finally:
            if self.lock is not None:
                os.close(self.lock)
            self.db = self.lock = self.api = None

    def initialize(self, params):
        if self.api is not None:
            raise invalid("Navidrome has already been initialized")
        if type(params.get("api")) is not int or params["api"] != 1:
            raise invalid("Navidrome requires Lark plugin API 1")
        cfg = params.get("config")
        fields = {"url", "username", "password_env", "allow_http", "page_size", "max_cache_bytes"}
        if not isinstance(cfg, dict) or cfg.keys() - fields:
            raise invalid("invalid Navidrome config; set url, username, and password_env (not a password)")
        allow_http = cfg.get("allow_http", False)
        if type(allow_http) is not bool:
            raise invalid("Navidrome allow_http must be a boolean")
        url = configured_url(cfg.get("url"), allow_http)
        username = string(cfg.get("username"))
        if not username or not username.strip():
            raise invalid("Navidrome username is required")
        password_env = cfg.get("password_env", "NAVIDROME_PASSWORD")
        if not isinstance(password_env, str) or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", password_env):
            raise invalid("Navidrome password_env must name a valid environment variable")
        password = string(os.environ.get(password_env))
        if not password:
            raise invalid("Navidrome password_env is not set to a non-empty password")
        self.page_size = integer(cfg.get("page_size", 100), 500)
        if self.page_size is None:
            raise invalid("Navidrome page_size must be an integer from 1 to 500")
        # Deprecated: accept the old setting, but leave all audio files alone.
        max_cache = cfg.get("max_cache_bytes", 1)
        if type(max_cache) is not int or max_cache <= 0:
            raise invalid("Navidrome max_cache_bytes must be a positive integer")
        state_dir = string(params.get("state_dir"))
        if not state_dir or "\x00" in state_dir:
            raise invalid("Navidrome requires a valid state_dir")
        root = Path(state_dir).absolute()
        api = Subsonic(url, username, password)
        api.request_json("ping")
        identity = hashlib.sha256(json.dumps([url.geturl(), username]).encode()).hexdigest()

        try:
            root.mkdir(parents=True, mode=0o700, exist_ok=True)
            self.lock = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
            try:
                fcntl.flock(self.lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise PluginError("Navidrome state directory is already in use") from None
            manifest = root / "catalogue.sqlite3"
            info = lstat(manifest)
            if info is not None and not stat.S_ISREG(info.st_mode):
                raise PluginError("Navidrome catalogue must be a regular file")
            self.db = sqlite3.connect(manifest, timeout=1)
            self.catalogue = Catalogue(self.db, identity, self.progress)
            self.api = api
        except BaseException:
            self.close()
            raise
        return {"name": "navidrome", "scheme": "navidrome", "cache": "full", "version": "0.1.0"}

    def call(self, method, params):
        with deadline():
            return self.dispatch(method, params)

    def dispatch(self, method, params):
        if method == "shutdown":
            return None
        if method not in ("initialize", "sync.pull", "resolve"):
            raise PluginError("unknown Navidrome method", -32601)
        if not isinstance(params, dict):
            raise invalid("Navidrome request params must be an object")
        if method == "initialize":
            return self.initialize(params)
        if self.api is None:
            raise PluginError("Navidrome has not been initialized")
        if method == "sync.pull":
            cursor = params.get("cursor")
            if cursor is not None and not string(cursor):
                raise invalid("Navidrome sync cursor must be a non-empty string or null")
            return self.catalogue.pull(cursor, self.api, self.page_size)
        return self.api.stream(song_id_from_uri(params.get("uri")))


def main():
    source = Source(progress=lambda message: print(f"[navidrome] {message}", file=sys.stderr, flush=True))
    try:
        for line in sys.stdin.buffer:
            try:
                request = json.loads(line)
            except (ValueError, RecursionError):
                continue  # no usable request id
            if not isinstance(request, dict):
                continue
            ident = request.get("id")
            if type(ident) is not int or not 0 <= ident < 2**64:
                continue
            method = request.get("method")
            try:
                result = source.call(method, request.get("params", {}))
            except PluginError as error:
                response = {"id": ident, "error": {"code": error.code, "message": str(error)}}
            except (OSError, sqlite3.Error):
                response = {"id": ident, "error": {"code": -32603,
                            "message": "Navidrome could not read or write its catalogue state"}}
            except Exception:
                # Avoid printing arbitrary network input, config or tracebacks.
                response = {"id": ident, "error": {"code": -32603, "message": "Navidrome internal error"}}
            else:
                response = {"id": ident, "result": result}
            print(json.dumps(response, separators=(",", ":")), flush=True)
            if method == "shutdown":
                break
    finally:
        source.close()


if __name__ == "__main__":
    main()
