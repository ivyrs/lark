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

**Plugins never touch audio.** A source answers `resolve(uri)` with `{kind: "file", path}` (and later `http` or `pipe`). Decoding, gapless and output stay in the core. URIs are routed by scheme prefix (`local:track:…`), so one queue can mix sources.

**Core-owned library.** A source declares `cache: full | partial | none` in `initialize`:
- `full` sources sync with cursor-paged `sync.pull`. Deletes mark rows `stale` rather than removing them, so history and ratings survive.
- History stores a JSON snapshot of the track, so it still renders after a source disappears.

**Supervision.** Each plugin runs as a child process with restart and exponential backoff. If a plugin crashes, playback keeps going.

## Plugin protocol (stdio, one JSON object per line)

```
core → plugin   initialize {api, config, state_dir}   → {name, scheme, cache, version}
                sync.pull  {cursor?}                  → {upserts:[Track], deletes:[uri], next_cursor, done}
                resolve    {uri}                      → {kind:"file", path} | {kind:"http", url, headers}
                search     {query, limit}             → [Track]     (partial/none sources)
                shutdown   {}
plugin → core   sync.dirty {}                         (please pull again)
```

A minimal Python source is about 40 lines: read lines from stdin, answer requests, write JSON to stdout, log to stderr.

## Roadmap (next, roughly in order)

- [ ] `notify` watcher in `local` → `sync.dirty`
- [ ] `mpd` frontend plugin (validate against rmpc / ncmpcpp)
- [ ] `mpris` + `listenbrainz` frontend plugins (needs plugin → core subscribe)
- [ ] streamed search fan-out to `partial`/`none` sources
- [ ] `http` streams (Subsonic, radio), then a Python Subsonic source to prove the SDK story
- [ ] album art (kitty graphics / sixel) in the TUI
- [ ] recording groups / source preference once a second real source exists
- [ ] proper resampler (rubato) and downmix matrices; ReplayGain
- [ ] queue persistence across daemon restarts
