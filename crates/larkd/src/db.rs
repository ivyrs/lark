//! Core-owned library: SQLite + FTS5.
//!
//! Sources push metadata in via `apply_batch`; deletes mark rows stale so
//! ratings/history survive a source being briefly unavailable.

use std::path::Path;

use anyhow::Result;
use lark_protocol::{now_ms, ArtistRef, CacheMode, Play, ReleaseRef, SyncBatch, Track};
use rusqlite::{params, Connection, OptionalExtension};

const SCHEMA: &str = r#"
PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS source (
    name       TEXT PRIMARY KEY,
    scheme     TEXT NOT NULL,
    cache_mode TEXT NOT NULL,
    cursor     TEXT,
    last_sync  INTEGER
);
CREATE TABLE IF NOT EXISTS artist (
    id        INTEGER PRIMARY KEY,
    name      TEXT NOT NULL UNIQUE,
    sort_name TEXT,
    mbid      TEXT
);
CREATE TABLE IF NOT EXISTS release (
    id         INTEGER PRIMARY KEY,
    title      TEXT NOT NULL,
    artist_key TEXT NOT NULL,
    date       TEXT,
    mbid       TEXT UNIQUE,
    UNIQUE (title, artist_key)
);
CREATE TABLE IF NOT EXISTS release_artist (
    release_id INTEGER NOT NULL REFERENCES release(id) ON DELETE CASCADE,
    artist_id  INTEGER NOT NULL REFERENCES artist(id) ON DELETE CASCADE,
    pos        INTEGER NOT NULL,
    PRIMARY KEY (release_id, pos)
);
CREATE TABLE IF NOT EXISTS track (
    uri         TEXT PRIMARY KEY,
    source      TEXT NOT NULL,
    title       TEXT NOT NULL,
    release_id  INTEGER REFERENCES release(id),
    track_no    INTEGER,
    disc_no     INTEGER,
    duration_ms INTEGER,
    date        TEXT,
    mbid        TEXT,
    isrc        TEXT,
    cached_at   INTEGER NOT NULL,
    stale       INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS track_release ON track(release_id);
CREATE INDEX IF NOT EXISTS track_mbid ON track(mbid);
CREATE TABLE IF NOT EXISTS track_artist (
    uri       TEXT NOT NULL REFERENCES track(uri) ON DELETE CASCADE,
    artist_id INTEGER NOT NULL REFERENCES artist(id),
    role      TEXT NOT NULL DEFAULT 'main',
    pos       INTEGER NOT NULL,
    PRIMARY KEY (uri, role, pos)
);
CREATE TABLE IF NOT EXISTS tag (
    uri   TEXT NOT NULL REFERENCES track(uri) ON DELETE CASCADE,
    key   TEXT NOT NULL,
    value TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS tag_uri ON tag(uri);
-- History never joins only through `track`: snapshot keeps it renderable
-- after a source disappears or for uncached (cache = none) tracks.
CREATE TABLE IF NOT EXISTS play (
    id         INTEGER PRIMARY KEY,
    uri        TEXT NOT NULL,
    started_at INTEGER NOT NULL,
    played_ms  INTEGER NOT NULL DEFAULT 0,
    completed  INTEGER NOT NULL DEFAULT 0,
    snapshot   TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS play_uri ON play(uri);
CREATE TABLE IF NOT EXISTS rating (uri TEXT PRIMARY KEY, value INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS saved  (uri TEXT PRIMARY KEY, added_at INTEGER NOT NULL);
CREATE VIRTUAL TABLE IF NOT EXISTS track_fts USING fts5(
    uri UNINDEXED, title, artists, release, tokenize = 'unicode61 remove_diacritics 2'
);
"#;

pub struct Library {
    conn: Connection,
}

impl Library {
    pub fn open(path: &Path) -> Result<Self> {
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir)?;
        }
        let conn = Connection::open(path)?;
        conn.execute_batch(SCHEMA)?;
        Ok(Self { conn })
    }

    #[cfg(test)]
    pub fn memory() -> Result<Self> {
        let conn = Connection::open_in_memory()?;
        conn.execute_batch(SCHEMA)?;
        Ok(Self { conn })
    }

    // ── sources ──

    pub fn register_source(&self, name: &str, scheme: &str, cache: CacheMode) -> Result<()> {
        let mode = serde_json::to_value(cache)?
            .as_str()
            .unwrap_or("full")
            .to_string();
        self.conn.execute(
            "INSERT INTO source(name, scheme, cache_mode) VALUES (?1, ?2, ?3)
             ON CONFLICT(name) DO UPDATE SET scheme = ?2, cache_mode = ?3",
            params![name, scheme, mode],
        )?;
        Ok(())
    }

    pub fn source_cursor(&self, name: &str) -> Result<Option<String>> {
        Ok(self
            .conn
            .query_row("SELECT cursor FROM source WHERE name = ?1", [name], |r| {
                r.get(0)
            })
            .optional()?
            .flatten())
    }

    pub fn source_track_count(&self, name: &str) -> Result<u64> {
        let n: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM track WHERE source = ?1 AND stale = 0",
            [name],
            |r| r.get(0),
        )?;
        Ok(n as u64)
    }

    pub fn apply_batch(&mut self, source: &str, batch: &SyncBatch) -> Result<()> {
        let tx = self.conn.transaction()?;
        let now = now_ms() as i64;
        for t in &batch.upserts {
            upsert_track(&tx, source, t, now)?;
        }
        for uri in &batch.deletes {
            tx.execute("UPDATE track SET stale = 1 WHERE uri = ?1", [uri])?;
            tx.execute("DELETE FROM track_fts WHERE uri = ?1", [uri])?;
        }
        if batch.done {
            tx.execute(
                "UPDATE source SET cursor = ?2, last_sync = ?3 WHERE name = ?1",
                params![source, batch.next_cursor, now],
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    // ── reads ──

    pub fn track(&self, uri: &str) -> Result<Option<Track>> {
        load_track(&self.conn, uri)
    }

    fn tracks_for(&self, sql: &str, p: impl rusqlite::Params) -> Result<Vec<Track>> {
        let mut st = self.conn.prepare(sql)?;
        let uris: Vec<String> = st.query_map(p, |r| r.get(0))?.collect::<Result<_, _>>()?;
        let mut out = Vec::with_capacity(uris.len());
        for u in uris {
            if let Some(t) = load_track(&self.conn, &u)? {
                out.push(t);
            }
        }
        Ok(out)
    }

    pub fn search(&self, query: &str, limit: u32) -> Result<Vec<Track>> {
        let expr = fts_expr(query);
        if expr.is_empty() {
            return Ok(vec![]);
        }
        self.tracks_for(
            "SELECT f.uri FROM track_fts f JOIN track t ON t.uri = f.uri
             WHERE track_fts MATCH ?1 AND t.stale = 0
             ORDER BY bm25(track_fts, 0.0, 10.0, 6.0, 4.0) LIMIT ?2",
            params![expr, limit],
        )
    }

    pub fn artists(&self) -> Result<Vec<ArtistRef>> {
        let mut st = self.conn.prepare(
            "SELECT a.id, a.name, COUNT(DISTINCT r.id)
             FROM artist a
             JOIN release_artist ra ON ra.artist_id = a.id
             JOIN release r ON r.id = ra.release_id
             WHERE EXISTS (SELECT 1 FROM track t WHERE t.release_id = r.id AND t.stale = 0)
             GROUP BY a.id
             ORDER BY COALESCE(a.sort_name, a.name) COLLATE NOCASE",
        )?;
        let rows = st.query_map([], |r| {
            Ok(ArtistRef {
                id: r.get(0)?,
                name: r.get(1)?,
                releases: r.get(2)?,
            })
        })?;
        Ok(rows.collect::<Result<_, _>>()?)
    }

    pub fn releases(&self, artist_id: Option<i64>) -> Result<Vec<ReleaseRef>> {
        let mut st = self.conn.prepare(
            "SELECT * FROM (
               SELECT r.id, r.title, r.artist_key, r.date,
                      (SELECT COUNT(*) FROM track t WHERE t.release_id = r.id AND t.stale = 0) AS n
               FROM release r
               WHERE ?1 IS NULL OR EXISTS (
                 SELECT 1 FROM release_artist ra WHERE ra.release_id = r.id AND ra.artist_id = ?1)
             ) WHERE n > 0
             ORDER BY date, title COLLATE NOCASE",
        )?;
        let rows = st.query_map([artist_id], |r| {
            Ok(ReleaseRef {
                id: r.get(0)?,
                title: r.get(1)?,
                artist: r.get(2)?,
                date: r.get(3)?,
                tracks: r.get(4)?,
            })
        })?;
        Ok(rows.collect::<Result<_, _>>()?)
    }

    pub fn tracks_by_release(&self, release_id: i64) -> Result<Vec<Track>> {
        self.tracks_for(
            "SELECT uri FROM track WHERE release_id = ?1 AND stale = 0
             ORDER BY COALESCE(disc_no, 1), track_no, title",
            [release_id],
        )
    }

    pub fn tracks_by_artist(&self, artist_id: i64) -> Result<Vec<Track>> {
        self.tracks_for(
            "SELECT t.uri FROM track t
             JOIN release r ON r.id = t.release_id
             JOIN release_artist ra ON ra.release_id = r.id AND ra.artist_id = ?1
             WHERE t.stale = 0
             ORDER BY r.date, r.title COLLATE NOCASE, COALESCE(t.disc_no, 1), t.track_no",
            [artist_id],
        )
    }

    // ── history ──

    pub fn play_start(&self, track: &Track) -> Result<i64> {
        self.conn.execute(
            "INSERT INTO play(uri, started_at, snapshot) VALUES (?1, ?2, ?3)",
            params![track.uri, now_ms() as i64, serde_json::to_string(track)?],
        )?;
        Ok(self.conn.last_insert_rowid())
    }

    pub fn play_finish(&self, id: i64, played_ms: u64, completed: bool) -> Result<()> {
        self.conn.execute(
            "UPDATE play SET played_ms = ?2, completed = ?3 WHERE id = ?1",
            params![id, played_ms as i64, completed],
        )?;
        Ok(())
    }

    pub fn history(&self, limit: u32) -> Result<Vec<Play>> {
        let mut st = self.conn.prepare(
            "SELECT uri, started_at, played_ms, completed, snapshot FROM play ORDER BY id DESC LIMIT ?1",
        )?;
        let rows = st.query_map([limit], |r| {
            let snap: String = r.get(4)?;
            Ok(Play {
                uri: r.get(0)?,
                started_at: r.get::<_, i64>(1)? as u64,
                played_ms: r.get::<_, i64>(2)? as u64,
                completed: r.get(3)?,
                track: serde_json::from_str(&snap).unwrap_or_default(),
            })
        })?;
        Ok(rows.collect::<Result<_, _>>()?)
    }
}

fn artist_id(conn: &Connection, name: &str) -> rusqlite::Result<i64> {
    conn.execute("INSERT OR IGNORE INTO artist(name) VALUES (?1)", [name])?;
    conn.query_row("SELECT id FROM artist WHERE name = ?1", [name], |r| {
        r.get(0)
    })
}

fn upsert_track(conn: &Connection, source: &str, t: &Track, now: i64) -> Result<()> {
    let release_id = match &t.album {
        Some(album) if !album.trim().is_empty() => {
            let artists: &[String] = if !t.album_artists.is_empty() {
                &t.album_artists
            } else if !t.artists.is_empty() {
                &t.artists[..1]
            } else {
                &[]
            };
            let key = if artists.is_empty() {
                "Unknown Artist".to_string()
            } else {
                artists.join("; ")
            };
            let existing: Option<i64> = match &t.release_mbid {
                Some(m) => conn
                    .query_row("SELECT id FROM release WHERE mbid = ?1", [m], |r| r.get(0))
                    .optional()?,
                None => None,
            };
            let existing = match existing {
                Some(id) => Some(id),
                None => conn
                    .query_row(
                        "SELECT id FROM release WHERE title = ?1 AND artist_key = ?2",
                        params![album, key],
                        |r| r.get(0),
                    )
                    .optional()?,
            };
            let id = match existing {
                Some(id) => {
                    conn.execute(
                        "UPDATE release SET date = COALESCE(?2, date), mbid = COALESCE(?3, mbid) WHERE id = ?1",
                        params![id, t.date, t.release_mbid],
                    )?;
                    id
                }
                None => {
                    conn.execute(
                        "INSERT INTO release(title, artist_key, date, mbid) VALUES (?1, ?2, ?3, ?4)",
                        params![album, key, t.date, t.release_mbid],
                    )?;
                    let id = conn.last_insert_rowid();
                    let names: Vec<&str> = if artists.is_empty() {
                        vec!["Unknown Artist"]
                    } else {
                        artists.iter().map(|s| s.as_str()).collect()
                    };
                    for (pos, name) in names.iter().enumerate() {
                        let aid = artist_id(conn, name)?;
                        conn.execute(
                            "INSERT OR REPLACE INTO release_artist(release_id, artist_id, pos) VALUES (?1, ?2, ?3)",
                            params![id, aid, pos as i64],
                        )?;
                    }
                    id
                }
            };
            Some(id)
        }
        _ => None,
    };

    conn.execute(
        "INSERT INTO track(uri, source, title, release_id, track_no, disc_no, duration_ms, date, mbid, isrc, cached_at, stale)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, 0)
         ON CONFLICT(uri) DO UPDATE SET source = ?2, title = ?3, release_id = ?4, track_no = ?5, disc_no = ?6,
           duration_ms = ?7, date = ?8, mbid = ?9, isrc = ?10, cached_at = ?11, stale = 0",
        params![
            t.uri, source, t.title, release_id, t.track_no, t.disc_no,
            t.duration_ms.map(|d| d as i64), t.date, t.mbid, t.isrc, now
        ],
    )?;

    conn.execute("DELETE FROM track_artist WHERE uri = ?1", [&t.uri])?;
    for (pos, name) in t.artists.iter().enumerate() {
        let aid = artist_id(conn, name)?;
        conn.execute(
            "INSERT INTO track_artist(uri, artist_id, role, pos) VALUES (?1, ?2, 'main', ?3)",
            params![t.uri, aid, pos as i64],
        )?;
    }
    conn.execute("DELETE FROM tag WHERE uri = ?1", [&t.uri])?;
    for (k, v) in &t.tags {
        conn.execute(
            "INSERT INTO tag(uri, key, value) VALUES (?1, ?2, ?3)",
            params![t.uri, k, v],
        )?;
    }

    conn.execute("DELETE FROM track_fts WHERE uri = ?1", [&t.uri])?;
    let artists_txt = t
        .artists
        .iter()
        .chain(t.album_artists.iter())
        .cloned()
        .collect::<Vec<_>>()
        .join(" ");
    conn.execute(
        "INSERT INTO track_fts(uri, title, artists, release) VALUES (?1, ?2, ?3, ?4)",
        params![
            t.uri,
            t.title,
            artists_txt,
            t.album.clone().unwrap_or_default()
        ],
    )?;
    Ok(())
}

fn load_track(conn: &Connection, uri: &str) -> Result<Option<Track>> {
    let row = conn
        .query_row(
            "SELECT t.title, t.track_no, t.disc_no, t.duration_ms, t.date, t.mbid, t.isrc,
                    r.id, r.title, r.mbid
             FROM track t LEFT JOIN release r ON r.id = t.release_id WHERE t.uri = ?1",
            [uri],
            |r| {
                Ok((
                    Track {
                        uri: uri.to_string(),
                        title: r.get(0)?,
                        track_no: r.get(1)?,
                        disc_no: r.get(2)?,
                        duration_ms: r.get::<_, Option<i64>>(3)?.map(|d| d as u64),
                        date: r.get(4)?,
                        mbid: r.get(5)?,
                        isrc: r.get(6)?,
                        album: r.get(8)?,
                        release_mbid: r.get(9)?,
                        ..Default::default()
                    },
                    r.get::<_, Option<i64>>(7)?,
                ))
            },
        )
        .optional()?;
    let Some((mut t, release_id)) = row else {
        return Ok(None);
    };

    let mut st = conn.prepare_cached(
        "SELECT a.name FROM track_artist ta JOIN artist a ON a.id = ta.artist_id
         WHERE ta.uri = ?1 AND ta.role = 'main' ORDER BY ta.pos",
    )?;
    t.artists = st
        .query_map([uri], |r| r.get(0))?
        .collect::<Result<_, _>>()?;

    if let Some(rid) = release_id {
        let mut st = conn.prepare_cached(
            "SELECT a.name FROM release_artist ra JOIN artist a ON a.id = ra.artist_id
             WHERE ra.release_id = ?1 ORDER BY ra.pos",
        )?;
        t.album_artists = st
            .query_map([rid], |r| r.get(0))?
            .collect::<Result<_, _>>()?;
    }
    let mut st = conn.prepare_cached("SELECT key, value FROM tag WHERE uri = ?1")?;
    t.tags = st
        .query_map([uri], |r| Ok((r.get(0)?, r.get(1)?)))?
        .collect::<Result<_, _>>()?;
    Ok(Some(t))
}

/// Turn free text into an FTS5 prefix query: `foo bar` → `"foo"* "bar"*`.
fn fts_expr(q: &str) -> String {
    q.split_whitespace()
        .map(|w| w.replace('"', ""))
        .filter(|w| !w.is_empty())
        .map(|w| format!("\"{w}\"*"))
        .collect::<Vec<_>>()
        .join(" ")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t(uri: &str, title: &str, artist: &str, album: &str, n: u32) -> Track {
        Track {
            uri: uri.into(),
            title: title.into(),
            artists: vec![artist.into()],
            album: Some(album.into()),
            track_no: Some(n),
            ..Default::default()
        }
    }

    #[test]
    fn sync_search_browse_delete() {
        let mut lib = Library::memory().unwrap();
        lib.register_source("local", "local", CacheMode::Full)
            .unwrap();
        let batch = SyncBatch {
            upserts: vec![
                t(
                    "local:track:a",
                    "Teardrop",
                    "Massive Attack",
                    "Mezzanine",
                    3,
                ),
                t("local:track:b", "Angel", "Massive Attack", "Mezzanine", 1),
                t("local:track:c", "Roads", "Portishead", "Dummy", 5),
            ],
            deletes: vec![],
            next_cursor: Some("1".into()),
            done: true,
        };
        lib.apply_batch("local", &batch).unwrap();
        assert_eq!(lib.source_cursor("local").unwrap().as_deref(), Some("1"));

        let hits = lib.search("mass tear", 10).unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].title, "Teardrop");

        let artists = lib.artists().unwrap();
        assert_eq!(artists.len(), 2);
        let ma = artists.iter().find(|a| a.name == "Massive Attack").unwrap();
        let rels = lib.releases(Some(ma.id)).unwrap();
        assert_eq!(rels.len(), 1);
        let tracks = lib.tracks_by_release(rels[0].id).unwrap();
        assert_eq!(
            tracks.iter().map(|t| t.title.as_str()).collect::<Vec<_>>(),
            ["Angel", "Teardrop"]
        );

        lib.apply_batch(
            "local",
            &SyncBatch {
                deletes: vec!["local:track:c".into()],
                done: true,
                next_cursor: Some("2".into()),
                ..Default::default()
            },
        )
        .unwrap();
        assert!(lib.search("roads", 10).unwrap().is_empty());
        assert_eq!(lib.artists().unwrap().len(), 1);
        assert_eq!(lib.source_track_count("local").unwrap(), 2);

        let pid = lib.play_start(&tracks[0]).unwrap();
        lib.play_finish(pid, 1000, false).unwrap();
        assert_eq!(lib.history(10).unwrap()[0].track.title, "Angel");
    }
}
