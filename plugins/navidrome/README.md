# Navidrome source for lark

A standalone **Python 3.9+ / standard-library-only** source plugin speaking
lark's stdio protocol. The plugin mirrors song metadata and signs playback
URLs; the lark daemon fetches and decodes the original audio as it streams.
The plugin does not download or cache audio.

With the Nix package, use `exec = ["lark-source-navidrome"]` instead of the
checkout command below. Its Python interpreter and script are packaged. See
[the Nix guide](../../nix/README.md) for the user service and secrets.

## Configure

Build the workspace (`nix develop -c cargo build --workspace --locked`), then add
this to `~/.config/lark/config.toml`. Use an absolute script path; TOML/exec
arguments do not expand `~` or shell variables.

```toml
[[plugin]]
name = "navidrome"
exec = ["python3", "/absolute/path/to/lark/plugins/navidrome/lark_source_navidrome.py"]
[plugin.config]
url = "https://music.example.com" # include a base path, if any; do not append /rest
username = "your-user"
password_env = "NAVIDROME_PASSWORD"
# page_size = 100                 # songs requested per page, 1..500
# allow_http = false             # explicit opt-in for trusted local HTTP only
```

Supply the password to **the daemon's** environment, not to the client. For
example, in Bash, prompt without storing it in shell history:

```bash
read -rsp 'Navidrome password: ' NAVIDROME_PASSWORD; echo
export NAVIDROME_PASSWORD
./target/debug/larkd
```

If you also want local files, keep an explicit `local` plugin entry: supplying
any `[[plugin]]` list replaces the daemon's default local-only list. Cached
metadata from removed/disabled sources remains stored but is hidden from normal
library browsing and search; history is preserved.

With the daemon running, `lark sources`, `lark search <words>`, and `lark add -p
<words>` work as for local music. Startup mirrors the catalogue; `lark sync`
refreshes it. **On first sync, `lark sources` reports 0 tracks until enumeration
finishes and metadata is published.** The plugin logs `sync started`, periodic
song-page and track counts (every 25 pages or 5 seconds, at page boundaries),
and `scan complete`; larkd then logs `sync complete`.

HTTPS uses normal certificate validation. Configure the final server URL;
the plugin does not follow catalogue redirects. Token/salt authentication
requires the actual account password, not a web UI JWT. The plugin does not
write authentication URLs to logs or state. Treat the signed stream URL as a
credential, and remember that environment variables have normal same-user
process visibility.

## Sync and playback

- A full sync uses OpenSubsonic `search3` with a blank query and zero artist
  and album counts. It requests at most one song page per pull, advances by
  the number of returned songs, then stops on an empty page. There are no
  per-album requests. The server must support empty-query enumeration; API
  failures and malformed pages abort the scan, but a successful empty result
  is indistinguishable from a genuinely empty library. There is no slower
  album-by-album fallback.
- Metadata comes from each song: title, album, track and album artists,
  track/disc numbers, duration, date or year, genres, and MusicBrainz IDs
  when provided. Album-only data (such as an exact release date, label, or
  release MBID) may be absent. URIs remain
  `navidrome:track:<percent-encoded-song-id>`; opaque IDs are not file paths.
- A scan stages metadata in SQLite before publishing. Failed scans emit no
  partial metadata or deletions. Complete snapshots replay after a lost reply
  or restart; the daemon's next cursor acknowledges the snapshot. The daemon
  applies emitted batches individually, not in one whole-library transaction.
- The remote API is not a snapshot transaction and the plugin does not watch
  for changes. Avoid editing the remote catalogue during a sync. Repeated song
  IDs abort the scan, but not every concurrent mutation can be detected.
- `resolve` signs a fresh `stream.view?format=raw` URL and returns immediately,
  without contacting Navidrome. The daemon streams the original audio and
  decodes it with Symphonia. The plugin has a 24-second RPC deadline and an
  8-second socket timeout for metadata; these do not govern audio playback.
- One Navidrome source/account per daemon for now: URI schemes are not
  namespaced by instance. Playlists, favourites, ratings, artwork, scrobbling,
  transcoding, live search, and multiple accounts are not implemented.

## Existing installations

The plugin keeps its existing server/account binding, URIs, and SQLite cursor
state. The state defaults to `$XDG_STATE_HOME/lark/plugins/navidrome/` (normally
`~/.local/state/lark/plugins/navidrome/`). Keep `catalogue.sqlite3` alongside
the corresponding lark library database; changed accounts and unknown
cursors fail explicitly rather than guessing at deletions. Use a private state
directory; it is locked against concurrent plugin use.

`max_cache_bytes` is deprecated. It is still accepted but ignored, and can
be removed from the config. The plugin leaves any old `audio/` directory and
its contents untouched. You can remove that legacy directory manually while
larkd is stopped; do not remove `catalogue.sqlite3`.

## Tests

```sh
python3 -B -m unittest discover -s plugins/navidrome
nix develop
just check             # Rust checks plus Python plugin unit tests
just smoke             # local-source smoke plus Navidrome API fixture
```

An **opt-in** smoke check against an actual server is also available. Set
`LARK_NAVIDROME_URL`, `LARK_NAVIDROME_USERNAME`, `LARK_NAVIDROME_QUERY` (a search
matching a song), and `NAVIDROME_PASSWORD`, then run:

```sh
python3 scripts/navidrome_live_smoke.py
```

For local HTTP only, explicitly set `LARK_NAVIDROME_ALLOW_HTTP=1`. This check
mirrors metadata and plays a matching song through a null output. It never
edits or deletes the remote library or uses your normal lark database.

API references: [authentication](https://opensubsonic.github.io/docs/api-reference/),
[search3](https://opensubsonic.github.io/docs/endpoints/search3/),
[song metadata](https://opensubsonic.github.io/docs/responses/child/),
[stream](https://opensubsonic.github.io/docs/endpoints/stream/).
