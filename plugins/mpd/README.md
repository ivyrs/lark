# Lark MPD frontend

This first-party Python 3.9+ frontend lets MPD clients such as [rmpc](https://rmpc.mierak.dev/) browse and control Lark. It needs no Python packages. Lark owns playback, audio decoding, and the music library; the plugin translates MPD commands into Lark RPC and Lark notifications into MPD `idle` events.

With the Nix package, use `exec = ["lark-frontend-mpd"]` instead of the
checkout command below. Its Python interpreter and script are packaged. See
[the Nix guide](../../nix/README.md) for the user service and secrets.

## Setup

Add this to `~/.config/lark/config.toml` alongside your source plugins, using the absolute path to the script:

```toml
[[plugin]]
name = "mpd"
exec = ["python3", "/absolute/path/to/lark/plugins/mpd/lark_frontend_mpd.py"]
[plugin.config]
host = "127.0.0.1"
port = 6600
```

Start `larkd`, then connect your MPD client to `127.0.0.1:6600`. This is an **unauthenticated, loopback-only** listener; other users and programs on the same machine can access it. Do not expose it through a public proxy. `localhost` is treated as `127.0.0.1`; use `::1` explicitly for IPv6. If MPD already owns port 6600, choose another port. For tests, `port = 0` selects a free port. The plugin logs `MPD listening on HOST:PORT` to stderr when ready; stdout is reserved for the daemon protocol.

For rmpc, set `address: "127.0.0.1:6600"` inside the parentheses in `~/.config/rmpc/config.ron`. Use `rmpc status`, `rmpc listall`, or `rmpc` to check the connection. Some rmpc versions fall back to their default address when the config file is missing, even with `--address`.

## Source availability

`no running source for local:track:…` means the queued entry belongs to the local
source, not Navidrome. Enable the `local` plugin to play it, or use a Navidrome
entry instead. Normal library browsing now hides removed/disabled sources while
preserving their metadata and history. Existing explicit queue items and history
are not rewritten into another source's URI.

## Supported commands

| Area | Commands |
| --- | --- |
| Discovery | `commands`, `tagtypes`, `urlhandlers`, `outputs`, `decoders`, `binarylimit`, `ping`, `close` |
| Playback | `status`, `currentsong`, `getvol`, `play`, `playid`, `pause`, `stop`, `next`, `previous`, `seek`, `seekid`, `seekcur`, `setvol` |
| Queue | `playlistinfo`, `playlistid`, `plchanges`, `plchangesposid`, `add`, `addid`, `delete`, `deleteid`, `clear` |
| Library | `list`, `lsinfo`, `listall`, `listallinfo`, `find`, `search`, `count`, `stats` |
| Framing | `command_list_begin`, `command_list_ok_begin`, `command_list_end`, `idle`, `noidle` |

- The library is flat. The MPD `file` field is the opaque Lark track URI; pass it back unchanged to `add` or `addid`. Arbitrary URLs, filesystem paths, and directory adds are not supported. `addid` accepts an optional absolute queue position.
- Library commands use the core's indexed metadata, not files or a live source search. The plugin builds a disposable, paginated snapshot on first use and invalidates it on `library.updated` or `resync`. `status` does not scan the catalogue. `find` and `search` accept legacy tag/value pairs or parenthesized `AND` filters with `==`, `!=`, `contains`, and `starts_with`; regex, other Boolean operators, and the full MPD filter syntax are not implemented. `list` supports up to three grouping tags.
- `idle` events are coalesced per connection: `player`, `mixer`, `playlist`, `database`, and `update` (on library update completion). No polling. Playlist changes since an older revision may return a full refresh within the requested range. ID-targeted play/seek translate through the core's positional playback API, so those multi-step operations are not atomic against concurrent queue edits from other clients.
- The frontend is not a replacement for a full MPD server. There are no saved playlists, art downloads, playback modes, stickers, partitions, or output toggles. `outputs` lists Lark's single logical output; `decoders` lists common formats supported by Lark's Symphonia build. Unsupported commands return MPD `ACK` errors rather than pretending to succeed.
- Limits include 32 clients, 16 KiB MPD lines, 1,024 commands or 512 KiB per command list, and a catalogue of at most 100,000 tracks or 32 MiB of serialized metadata. Oversized catalogues fail cleanly rather than silently returning partial data; playback and queue reads don't require the catalogue. A core request or a catalogue refresh has a 30-second deadline. A timed-out mutation may still finish in the core; check its state before retrying.

## Tests

From the repository root:

```sh
python3 -m unittest discover -s plugins/mpd -v
```

Parent-verified against **rmpc 0.11.0**: CLI status/library/queue/playback/
volume/output/decoder commands and TUI rendering plus clean quit in a private
pseudo-terminal. For this optional real-client check after building the workspace:

```sh
LARK_RMPC_BIN=/path/to/rmpc python3 scripts/mpd_smoke.py
```

Tests use generated metadata, ephemeral ports, and a fake Lark peer over real stdio. See [`scripts/mpd_smoke.py`](../../scripts/mpd_smoke.py) for the disposable-daemon test and optional real-rmpc CLI/TUI checks. See the [MPD protocol reference](https://mpd.readthedocs.io/en/stable/protocol.html) for wire syntax and semantics.
