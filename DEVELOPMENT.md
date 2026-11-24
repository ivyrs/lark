# Working on lark

## First session

1. Enter `nix develop` (the lockfile pins the development dependencies).
2. Run `just check` and `just smoke` before changing behavior.
3. Use `just dev /absolute/path/to/music null` for headless work; change `null`
   to `default` to exercise your actual audio device. Stop it with Ctrl-C.
4. In another shell, use `target/debug/lark status`, `search <text>`, or the
   TUI. `LARK_SOCKET=/path/to/socket` selects a different daemon for clients.

The normal daemon uses your XDG config/data/state directories. To experiment
without affecting a personal library, prefer `just smoke`, or set separate
`XDG_CONFIG_HOME`, `XDG_DATA_HOME`, `XDG_STATE_HOME`, and `XDG_RUNTIME_DIR` before
starting both daemon and clients. `--db` alone does **not** isolate plugin state.
`--socket` is a daemon override; clients must use the matching `LARK_SOCKET`.

Without Nix, equivalent checks are:

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
cargo build --workspace --locked
python3 -m unittest discover -s plugins/navidrome -p 'test_*.py' -v
python3 scripts/smoke.py
python3 scripts/navidrome_smoke.py
python3 -m unittest discover -s plugins/mpd -p 'test_*.py' -v
python3 scripts/mpd_smoke.py
```

`LARK_BIN_DIR=/absolute/path/to/binaries python3 scripts/smoke.py` tests an
alternate build (for example `target/release`). Python uses only its standard
library. Smoke checks have bounded waits, generate a silent WAV, and clean up
owned processes and temporary state on success or failure.

## Where changes belong

| Area | Entry points / ownership |
| --- | --- |
| Shared wire contract | `crates/lark-protocol/src/lib.rs`: request/response/event and track types; client and plugin API versions |
| Daemon startup/config | `crates/larkd/src/main.rs`, `config.rs`: XDG paths, channels, plugin sync loop |
| Queue/player state | `crates/larkd/src/core.rs`: single actor owns transport, queue, and play-history lifecycle |
| Audio | `engine.rs`: decode, transport generations, transitions, position; `http.rs`: native progressive HTTP/Range source; `output.rs`: cpal/null ring-buffer consumer |
| Library | `db.rs`: SQLite schema, transactions, FTS, metadata and history snapshots |
| Client socket | `server.rs`: Unix JSON-lines RPC, subscriptions and routing to core/library |
| Plugin supervision | `plugin.rs`: child lifecycle, stdio requests, timeout/restart policy |
| Navidrome | `plugins/navidrome/lark_source_navidrome.py`: independent Python source, bulk song paging, replayable metadata snapshots, signed HTTP descriptors |
| MPD frontend | `plugins/mpd/lark_frontend_mpd.py`: MPD TCP translation, bidirectional stdio RPC, event-driven idle, disposable catalogue mirror |
| Local files | `crates/lark-source-local/src/main.rs`: scan, metadata extraction, persistent index, sync batches |
| CLI / TUI | `crates/lark/src/main.rs`, `client.rs`, `tui.rs`: commands, blocking/live clients, UI/event handling |

Preserve core ownership of audio/queue/library; sources resolve URIs and report
metadata, not audio. Deletion marks library rows stale; do not erase history or
ratings when files disappear. History deliberately stores metadata snapshots.
Changes to the serialized protocol must consider daemon, both clients, and
third-party sources together.

## Review follow-ups (2026-11-23)

This is an early prototype, not a production-readiness certification. The
readiness pass adds regression coverage for incomplete scans, negative relative
seek, queue clearing/batch removal, and socket-file preservation. The following
issues remain for deliberate work rather than an incidental setup rewrite:

1. **Sync acknowledgement/replay (high priority).** The local source persists
   its new index before the final response is applied by the daemon. Lost final
   responses or failed database transactions can permanently skip updates when
   an older non-null cursor is retried. Design cursor-associated replay or an
   explicit commit acknowledgement, including restart and multi-page failure
   tests. Do not assume a successful plugin write means a successful core commit.
   The newer Navidrome source stages and replays snapshots; the local source
   still needs this treatment.
2. **Prefetch invalidation (high priority).** The engine can already have decoded
   the next track into its audio ring when that queue item is removed/replaced.
   `SetNext` cannot retract that buffered item, and a late `Started` event can
   reinstate an obsolete current id. Stop/replacement-play generations now drop
   stale engine events; removal of already-prefetched next audio still needs
   explicit invalidation before promising all queue-edit races are solved.
3. **Input frame byte limits.** The shared client/frontend RPC session now bounds
   outbound queues (128 frames), in-flight requests (64), and disconnects peers
   that cannot keep up. Unix/plugin input JSON lines and individual outgoing
   message sizes remain unbounded. Add byte/frame limits without breaking large
   metadata pages; bounded message counts are not bounded total bytes.
4. **Subsecond local edits.** The local index stores whole-second mtimes. Multiple
   tag changes within a second can go unnoticed. Change the versioned index
   representation and migration/reset policy with metadata-update coverage.
The shared-volume finding is fixed: core status and events now carry volume,
and the native TUI and MPD frontend consume it.

See README's roadmap for planned features (watcher, HTTP sources, persistent
queue, MPRIS, artwork, improved resampling). These are not setup failures.

## Scale considerations (source review, not benchmark results)

A local sync walks the entire music tree and builds maps of all supported files:
roughly O(F log F) time and O(F) memory for F paths, even if few tags changed.
Metadata upserts are paged at 500, but the scan/index itself is not bounded by
that page size. This is acceptable for an explicit resync of a modest library;
measure representative large libraries before implementing automatic watching.
Library result hydration also performs per-track queries (`tracks_for` in
`db.rs`), so latency grows with the number of returned tracks. Prefer targeted
queries or batch hydration if that becomes observable. Do not trim durable play
history just to bound a UI or transport buffer; bound those transient owners.

## Verified preparation baseline

On 2026-11-23, the final working tree passed
`nix develop path:. -c just check smoke` on aarch64 Linux: formatting, all-target
Clippy with warnings denied, **20 Rust tests**, workspace binary build, and the
real-process smoke test. A subsequent user run exposed an event-order assumption:
`player.state` can legitimately arrive before `library.updated`. The smoke reader
now filters for the expected response/source event under a single total deadline.
Four deterministic Python regressions cover interleaving, deadline exhaustion,
EOF, and socket timeout; the real-process test deliberately emits an unrelated
player event before sync. The full gate and eight additional concurrent smoke
runs passed after this correction. Physical audio and the interactive TUI were
not manually exercised.

## Navidrome extension validation

The Python source in `plugins/navidrome/` now bulk-enumerates songs via `search3`
and returns signed HTTP descriptors, with no per-album queries or audio cache.
Core `http.rs` uses reqwest/rustls to fetch progressively on the daemon runtime
while the decoder thread reads. Stop/replacement play cancel pending reads;
stamped engine events prevent those operations from reviving obsolete playback.
Network/format limitations and non-range seek fallback are documented in README.

`just check` includes Python source/frontend unit tests. `just smoke` covers
local playback; authenticated song paging; partial HTTP response playback;
responsive stop; byte-range seek; error redaction with dependency trace enabled;
failed-page preservation; history/deletion/restart; MPD queue, idle/noidle and
library invalidation; and an MPD-driven Navidrome playback path. Set
`LARK_RMPC_BIN=/path/to/rmpc` to additionally exercise actual rmpc command mode
and TUI startup/quit in a private pseudo-terminal. This passed with rmpc 0.11.0.
The raw MPD endpoint is a compatibility subset, not a full MPD implementation.

`scripts/navidrome_live_smoke.py` is opt-in, uses temporary lark state, and never
writes to the remote library. Native HTTP playback/restart passed against a
disposable Navidrome 0.64.2 with generated tagged FLAC; controlled fault tests
remain in the local fixture. See the [Navidrome guide](plugins/navidrome/README.md)
and [MPD guide](plugins/mpd/README.md) for setup and explicit limits.

The local source's old cursor replay limitation remains. Frontend metadata
mirrors are disposable, not a second owner of the library. `partial`/`none`
source search fan-out, MPD's unsupported features, pipe transport, and recording
groups are still future work.

## Verified native-streaming/frontend baseline

On 2026-11-23, `LARK_RMPC_BIN=/path/to/rmpc nix develop path:. -c just check smoke`
passed formatting, all-target Clippy with warnings denied, **35 Rust tests** and
**32 Python unit/integration tests**, including the real rmpc 0.11.0 CLI/TUI
check. Two additional concurrent runs each of the six Navidrome fixture tests
and three MPD/rmpc tests passed. The final binary also passed the opt-in native
HTTP playback/restart test against disposable Navidrome 0.64.2; five deliberately
fixture-only fault checks were skipped in that live run, not in the main gate.
All test services/state were cleaned up; the user's existing daemon and account
were not restarted or used. Physical audio and macOS remain unverified.

## Configured-source visibility follow-up

Normal search, artists/releases, filtered track reads and keyset pages now use a
connection-local `browse_track` view restricted to enabled configured names.
Removed/disabled sources are hidden, not marked stale or deleted. Explicit track
lookups, history snapshots, ratings, saved state and source cursors are preserved;
re-enabling exposes the cached catalogue again. Temporary outages of configured
sources do not clear it. Frontend roles cannot expose old source rows under their
name. Neither the whitelist nor the view is stored in the durable database.

The final full gate after this follow-up passed **36 Rust + 33 Python tests**,
including all real-rmpc checks. Added regressions cover every browse path, durable
data preservation, disabled/removed configurations, and re-enabling after actual
daemon restarts using the same private database.

## Validation limits

`just check` covers pure protocol/database/audio helpers and focused regression
cases. `just smoke` exercises real built daemon/source/client processes, sync,
search, queue reuse, pause/seek, null-output completion, historical metadata,
subscription events, and shutdown. It does not certify physical audio, gapless
sound quality, interactive terminal restoration, arbitrary third-party plugins, macOS, or
all crash/restart races. Validate those on the relevant platform before release.
