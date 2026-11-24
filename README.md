# lark

A TUI music client on top of a small, modern mpd-style daemon. Working name.

```
┌──────────┐   unix socket, JSON lines    ┌───────────────────────────┐   stdio JSON-RPC   ┌──────────────────┐
│ lark     │ ◀── typed events ──────────▶ │ larkd                     │ ◀────────────────▶ │ lark-source-local│
│ (TUI/ctl)│     requests                 │  core · library · engine  │   (any language)   │ other sources…   │
└──────────┘                              └───────────────────────────┘                    └──────────────────┘
```

- **larkd**: the daemon. It owns the queue, the player and a SQLite/FTS5 library. It decodes with symphonia and outputs through cpal (ALSA, PipeWire via ALSA, or CoreAudio).
- **lark-source-local**: the local-files source. It runs as an out-of-process plugin, exactly like a third-party one would.
- **[Navidrome source](plugins/navidrome/README.md)**: a standalone Python plugin; bulk-syncs metadata and returns signed HTTP URLs. The daemon streams and decodes audio.
- **[MPD frontend](plugins/mpd/README.md)**: a supervised Python plugin exposing a loopback MPD-compatible endpoint for clients such as rmpc.
- **lark**: the TUI, plus `lark <cmd>` for scripting and keybinds.
- **lark-protocol**: the shared wire types. It's the one place to read to write a client or plugin.

## Run

```sh
nix develop            # or: rustup + pkg-config + alsa-lib headers
cargo build --workspace --release
./target/release/larkd --music-dir ~/Music &
./target/release/lark
```

`larkd --output null` plays into nothing at real-time speed, which is handy for testing without a device. Logs go to stderr and are filtered with `LARK_LOG` (e.g. `LARK_LOG=larkd=debug`). Config lives at `~/.config/lark/config.toml` (see `config.example.toml`). Without it, larkd runs the local source on `~/Music`.

## Develop

```sh
nix develop
just check            # formatting, Clippy (including tests), workspace tests
just smoke            # local files, native Navidrome HTTP, and MPD frontend integration
just dev /path/to/music null
# in another shell: nix develop -c just tui
```

The smoke test uses temporary config, library, plugin state, socket, and audio;
no existing daemon or music collection is needed. It requires Python 3 (included
in the Nix shell). To include the actual rmpc CLI and TUI startup/quit check,
set `LARK_RMPC_BIN=/path/to/rmpc` when running `just smoke` (otherwise that one
check is explicitly skipped). Outside Nix, install Rust with rustfmt/Clippy, just, Python 3,
`pkg-config`, and the ALSA development package on Linux. The project currently
requires Unix sockets/signals; Windows is not supported.

See [DEVELOPMENT.md](DEVELOPMENT.md) for the code map, workflow, and review
follow-ups. Build the **whole workspace**, not only `larkd`: the local-source
executable must exist next to the daemon or on `PATH`.

## Install with Nix / Home Manager

See [nix/README.md](nix/README.md) for the complete package, Linux user-service
module, runtime password handling, and migration without deleting existing data.
Installed Python plugin launchers are `lark-source-navidrome` and
`lark-frontend-mpd`; they do not need checkout paths or Python on PATH.

## TUI keys

| | |
|---|---|
| `1` `2` `3` / `Tab` | queue / library / search |
| `space` `n` `p` `s` | toggle, next, prev, stop |
| `[` `]` | seek −/+10s |
| `+` `-` | volume |
| `j` `k` `g` `G` | move |
| `/` | search (live) |
| `u` | resync library |
| **queue** `enter` `d` `C` `o` | play, remove, clear, jump to current |
| **library** `h` `l` | change column (artists → releases → tracks) |
| **library/search** `enter` `a` `R` `A` | play next, append, replace queue, append all results |

## CLI

```sh
lark status | queue | sources | history
lark play [index] | pause | toggle | stop | next | prev
lark seek 90 | lark seek +10 | lark volume 80
lark search portishead
lark add -p mezzanine          # search + append, start playing
lark watch                     # event stream as JSON lines (great for bark modules)
lark raw library.releases '{"artist_id": 1}'
```

## Design notes

**Events, not polling.** Clients call `subscribe` and get typed deltas: `queue.inserted {position, items}`, `queue.removed {ids}`, `queue.cleared`, `player.state`, `library.updated`. `player.state` is only sent on real changes. Clients extrapolate position from `position_ms + (now - at_ms)`.

**Gapless.** The engine decodes the next track straight into the same ring buffer, so same-spec transitions have no gap. Boundaries are announced when they are *heard*, using a consumed-sample counter, not when they're decoded. A change of sample rate or channel count drains the buffer and reopens the device, so there is a short gap only then. If the device can't run at the track's rate, the engine falls back to a linear resampler.

**Plugins never touch audio.** A source answers `resolve(uri)` with `{kind: "file", path}` or `{kind: "http", url, headers}` (`pipe` is not implemented). HTTP fetching, decoding, buffering and output stay in the core. URIs are routed by scheme prefix (`local:track:…`), so one queue can mix sources.

**Native HTTP.** Audio is pulled progressively with bounded transport buffers,
not downloaded to a plugin cache. HTTPS validates certificates; redirects and
environment proxies are disabled for credential safety. Stop/replacement play
cancel stalled network I/O. Range-capable, known-length streams can seek;
unsupported format/server seeks restart at zero. Other engine commands can wait
for an in-flight read (up to the 10-second stall deadline). Network/open latency
can introduce gaps between remote tracks; local same-spec gapless behavior is
preserved.

**Core-owned library.** A source declares `cache: full | partial | none` in `initialize`:
- `full` sources sync with cursor-paged `sync.pull`. Deletes mark rows `stale` rather than removing them, so history and ratings survive.
- History stores a JSON snapshot of the track, so it still renders after a source disappears.
- Normal browse/search only exposes enabled, configured sources. Removing or
  disabling a plugin hides its catalogue without deleting metadata, cursors,
  ratings, or history; re-enabling restores cached entries without a forced scan.
  Configured sources retain their cached catalogue during temporary outages.

Defining `[[plugin]]` entries replaces the default local-only plugin list. Keep
an explicit `name = "local"` / `exec = ["lark-source-local"]` entry with
`[plugin.config] music_dir = "~/Music"` if you want local playback alongside
Navidrome. Source URIs are not interchangeable; a local URI needs a local source.

**Supervision.** Each plugin runs as a child process with restart and exponential backoff. If a plugin crashes, playback keeps going.

## Plugin protocol (stdio, one JSON object per line)

```
core → plugin   initialize {api, config, state_dir}   → {name, role?, scheme?, cache?, version}
                sync.pull  {cursor?}                  → {upserts:[Track], deletes:[uri], next_cursor, done}
                resolve    {uri}                      → {kind:"file", path} | {kind:"http", url, headers}
                search     {query, limit}             → [Track]     (partial/none sources)
                shutdown   {}
plugin → core   sync.dirty {}                         (source: please pull again)
                subscribe {}                         (frontend: then typed events)
                status / player.* / queue.* / library.* / … (frontend: same client API)
```

`role` defaults to `source` for existing plugins. A frontend replies with
`role: "frontend"` and uses the same stdio connection to issue client requests
and subscribe; it does not appear in `sources` or provide a URI scheme. Both
sides correlate replies by ID, not arrival order. Outbound frames and concurrent
requests are count-bounded; a slow peer is disconnected rather than accumulating
unbounded output. Input frame byte limits remain a follow-up.

`status` includes a shared `volume` scalar; `player.volume {volume}` announces
changes. `library.tracks {limit, after_uri?}` without artist/release filters
returns URI-ordered keyset pages for frontend mirrors (limit 1..1000).

A minimal Python source is about 40 lines: read lines from stdin, answer requests, write JSON to stdout, log to stderr.

## Roadmap (next, roughly in order)

- [ ] `notify` watcher in `local` → `sync.dirty`
- [x] basic `mpd` frontend plugin, validated against rmpc 0.11.0 CLI and TUI startup (not full MPD/ncmpcpp compatibility)
- [ ] `mpris` + `listenbrainz` frontend plugins (bidirectional API and subscriptions now available)
- [ ] streamed search fan-out to `partial`/`none` sources
- [x] native progressive `http` audio and bulk-paged Python Navidrome source
- [ ] album art (kitty graphics / sixel) in the TUI
- [ ] recording groups / source preference once a second real source exists
- [ ] proper resampler (rubato) and downmix matrices; ReplayGain
- [ ] queue persistence across daemon restarts
