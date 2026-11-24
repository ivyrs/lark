//! `local` source plugin: mirrors a music directory into the core library.
//!
//! Speaks the plugin protocol over stdio. Keeps its own index (path → mtime)
//! in its state dir so incremental syncs can report deletes.

use std::collections::{BTreeMap, VecDeque};
use std::io::{BufRead, Write};
use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;

use anyhow::{Context, Result};
use lark_protocol::{
    encode, CacheMode, InitializeParams, InitializeResult, Message, Request, ResolveParams,
    Response, RpcError, Stream, SyncBatch, SyncPull, Track, PLUGIN_API_VERSION,
};
use lofty::prelude::*;
use serde::Deserialize;
use serde_json::Value;

const SCHEME: &str = "local";
const BATCH: usize = 500;
const EXTENSIONS: &[&str] = &[
    "flac", "mp3", "ogg", "oga", "opus", "m4a", "mp4", "aac", "wav", "aif", "aiff", "caf", "mka",
    "alac",
];

#[derive(Deserialize)]
struct Config {
    #[serde(default = "default_music_dir")]
    music_dir: String,
}

fn default_music_dir() -> String {
    "~/Music".into()
}

fn expand(p: &str) -> PathBuf {
    if let Some(rest) = p.strip_prefix("~/") {
        if let Some(home) = std::env::var_os("HOME") {
            return PathBuf::from(home).join(rest);
        }
    }
    PathBuf::from(p)
}

struct Plugin {
    root: PathBuf,
    state_file: PathBuf,
    /// relative path → mtime (secs). Committed when a sync session completes.
    index: BTreeMap<String, u64>,
    session: Option<Session>,
}

struct Session {
    pending_upserts: VecDeque<String>,
    pending_deletes: Vec<String>,
    new_index: BTreeMap<String, u64>,
}

impl Plugin {
    fn init(p: InitializeParams) -> Result<(Self, InitializeResult)> {
        let cfg: Config = serde_json::from_value(p.config).unwrap_or(Config {
            music_dir: default_music_dir(),
        });
        let root = expand(&cfg.music_dir);
        std::fs::create_dir_all(&p.state_dir).ok();
        let state_file = PathBuf::from(&p.state_dir).join("index.json");
        let index = std::fs::read(&state_file)
            .ok()
            .and_then(|b| serde_json::from_slice(&b).ok())
            .unwrap_or_default();
        log(&format!("music_dir = {}", root.display()));
        Ok((
            Self {
                root,
                state_file,
                index,
                session: None,
            },
            InitializeResult {
                name: "local".into(),
                role: lark_protocol::PluginRole::Source,
                scheme: SCHEME.into(),
                cache: CacheMode::Full,
                version: env!("CARGO_PKG_VERSION").into(),
            },
        ))
    }

    fn uri(rel: &str) -> String {
        format!("{SCHEME}:track:{rel}")
    }

    fn rel_from_uri(uri: &str) -> Option<&str> {
        uri.strip_prefix(&format!("{SCHEME}:track:"))
    }

    fn pull(&mut self, p: SyncPull) -> Result<SyncBatch> {
        if self.session.is_none() {
            // No cursor means the core has nothing from us: report everything
            // again, but keep the committed index intact if scanning fails.
            self.session = Some(self.diff(p.cursor.is_none())?);
        }
        let s = self.session.as_mut().unwrap();

        let mut upserts = Vec::new();
        while upserts.len() < BATCH {
            let Some(rel) = s.pending_upserts.pop_front() else {
                break;
            };
            match read_track(&self.root, &rel) {
                Ok(t) => upserts.push(t),
                Err(e) => {
                    log(&format!("skip {rel}: {e:#}"));
                    s.new_index.remove(&rel);
                }
            }
        }
        let deletes = std::mem::take(&mut s.pending_deletes);
        let done = s.pending_upserts.is_empty();

        let mut next_cursor = None;
        if done {
            let s = self.session.take().unwrap();
            self.index = s.new_index;
            let tmp = self.state_file.with_extension("json.tmp");
            std::fs::write(&tmp, serde_json::to_vec(&self.index)?)?;
            std::fs::rename(&tmp, &self.state_file)?;
            next_cursor = Some(crate::now_secs().to_string());
        }
        Ok(SyncBatch {
            upserts,
            deletes,
            next_cursor,
            done,
        })
    }

    fn diff(&self, full_sync: bool) -> Result<Session> {
        let mut new_index = BTreeMap::new();
        let mut pending_upserts = VecDeque::new();
        let root_metadata = std::fs::metadata(&self.root)
            .with_context(|| format!("reading music_dir {}", self.root.display()))?;
        anyhow::ensure!(
            root_metadata.is_dir(),
            "music_dir {} is not a directory",
            self.root.display()
        );
        for entry in walkdir::WalkDir::new(&self.root).follow_links(true) {
            let entry =
                entry.with_context(|| format!("scanning music_dir {}", self.root.display()))?;
            if !entry.file_type().is_file() {
                continue;
            }
            let path = entry.path();
            let ext = path
                .extension()
                .and_then(|e| e.to_str())
                .map(|e| e.to_ascii_lowercase());
            if !ext.is_some_and(|e| EXTENSIONS.contains(&e.as_str())) {
                continue;
            }
            let Ok(rel) = path.strip_prefix(&self.root) else {
                continue;
            };
            let Some(rel) = rel.to_str() else { continue };
            let mtime = entry
                .metadata()
                .with_context(|| format!("reading metadata for {}", path.display()))?
                .modified()
                .ok()
                .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
                .map(|d| d.as_secs())
                .unwrap_or(0);
            if full_sync || self.index.get(rel) != Some(&mtime) {
                pending_upserts.push_back(rel.to_string());
            }
            new_index.insert(rel.to_string(), mtime);
        }
        let pending_deletes = self
            .index
            .keys()
            .filter(|k| !full_sync && !new_index.contains_key(*k))
            .map(|k| Self::uri(k))
            .collect::<Vec<_>>();
        log(&format!(
            "scan: {} files, {} changed, {} removed",
            new_index.len(),
            pending_upserts.len(),
            pending_deletes.len()
        ));
        Ok(Session {
            pending_upserts,
            pending_deletes,
            new_index,
        })
    }

    fn resolve(&self, p: ResolveParams) -> Result<Stream> {
        let rel = Self::rel_from_uri(&p.uri).context("not a local uri")?;
        let path = self.root.join(rel);
        anyhow::ensure!(path.is_file(), "file missing: {}", path.display());
        Ok(Stream::File {
            path: path.to_string_lossy().into_owned(),
        })
    }
}

fn split_multi<'a>(vals: impl Iterator<Item = &'a str>) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for v in vals {
        for part in v.split(['\0', ';']) {
            let part = part.trim();
            if !part.is_empty() && !out.iter().any(|o| o == part) {
                out.push(part.to_string());
            }
        }
    }
    out
}

fn read_track(root: &Path, rel: &str) -> Result<Track> {
    let path = root.join(rel);
    let tagged = lofty::read_from_path(&path)?;
    let duration_ms = Some(tagged.properties().duration().as_millis() as u64).filter(|d| *d > 0);
    let stem = Path::new(rel)
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or(rel)
        .to_string();

    let mut t = Track {
        uri: Plugin::uri(rel),
        title: stem,
        duration_ms,
        ..Default::default()
    };
    let Some(tag) = tagged.primary_tag().or_else(|| tagged.first_tag()) else {
        return Ok(t);
    };
    if let Some(title) = tag.title() {
        t.title = title.into_owned();
    }
    let mut artists = split_multi(tag.get_strings(ItemKey::TrackArtists));
    if artists.is_empty() {
        artists = split_multi(tag.get_strings(ItemKey::TrackArtist));
    }
    t.artists = artists;
    t.album = tag.album().map(|a| a.into_owned());
    t.album_artists = split_multi(tag.get_strings(ItemKey::AlbumArtist));
    t.track_no = tag.track();
    t.disc_no = tag.disk();
    t.date = tag
        .get_string(ItemKey::RecordingDate)
        .or_else(|| tag.get_string(ItemKey::ReleaseDate))
        .or_else(|| tag.get_string(ItemKey::Year))
        .map(str::to_string);
    t.mbid = tag
        .get_string(ItemKey::MusicBrainzRecordingId)
        .map(str::to_string);
    t.release_mbid = tag
        .get_string(ItemKey::MusicBrainzReleaseId)
        .map(str::to_string);
    t.isrc = tag.get_string(ItemKey::Isrc).map(str::to_string);
    for g in tag.get_strings(ItemKey::Genre) {
        t.tags.push(("genre".into(), g.to_string()));
    }
    Ok(t)
}

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Logs go to stderr (inherited by the daemon) and to the core as a notification.
fn log(msg: &str) {
    eprintln!("[local] {msg}");
}

fn send(out: &mut impl Write, msg: &impl serde::Serialize) {
    let _ = out.write_all(encode(msg).as_bytes());
    let _ = out.flush();
}

fn handle(plugin: &mut Option<Plugin>, req: Request) -> Result<Value, RpcError> {
    let internal = |e: anyhow::Error| RpcError::new(RpcError::INTERNAL, format!("{e:#}"));
    match req.method.as_str() {
        "initialize" => {
            let p: InitializeParams = serde_json::from_value(req.params)
                .map_err(|e| RpcError::new(RpcError::INVALID_PARAMS, e.to_string()))?;
            if p.api != PLUGIN_API_VERSION {
                return Err(RpcError::new(
                    RpcError::INVALID_PARAMS,
                    format!("unsupported api {}", p.api),
                ));
            }
            let (pl, res) = Plugin::init(p).map_err(internal)?;
            *plugin = Some(pl);
            Ok(serde_json::to_value(res).unwrap())
        }
        "shutdown" => std::process::exit(0),
        m => {
            let pl = plugin
                .as_mut()
                .ok_or_else(|| RpcError::new(RpcError::INTERNAL, "not initialised"))?;
            match m {
                "sync.pull" => {
                    let p: SyncPull = serde_json::from_value(req.params).unwrap_or_default();
                    pl.pull(p)
                        .map(|b| serde_json::to_value(b).unwrap())
                        .map_err(internal)
                }
                "resolve" => {
                    let p: ResolveParams = serde_json::from_value(req.params)
                        .map_err(|e| RpcError::new(RpcError::INVALID_PARAMS, e.to_string()))?;
                    pl.resolve(p)
                        .map(|s| serde_json::to_value(s).unwrap())
                        .map_err(internal)
                }
                _ => Err(RpcError::new(RpcError::METHOD_NOT_FOUND, m.to_string())),
            }
        }
    }
}

fn main() {
    let stdin = std::io::stdin();
    let mut stdout = std::io::stdout().lock();
    let mut plugin: Option<Plugin> = None;

    for line in stdin.lock().lines() {
        let Ok(line) = line else { break };
        if line.trim().is_empty() {
            continue;
        }
        match serde_json::from_str::<Message>(&line) {
            Ok(Message::Request(req)) => {
                let id = req.id;
                let resp = match handle(&mut plugin, req) {
                    Ok(v) => Response::ok(id, v),
                    Err(e) => Response::err(id, e),
                };
                send(&mut stdout, &resp);
            }
            Ok(Message::Notification(_)) | Ok(Message::Response(_)) => {}
            Err(e) => log(&format!("bad message: {e}")),
        }
    }
    // stdin closed → core went away.
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    struct TempDir(PathBuf);

    impl TempDir {
        fn new() -> Self {
            static NEXT: AtomicU64 = AtomicU64::new(0);
            loop {
                let path = std::env::temp_dir().join(format!(
                    "lark-local-test-{}-{}",
                    std::process::id(),
                    NEXT.fetch_add(1, Ordering::Relaxed)
                ));
                match std::fs::create_dir(&path) {
                    Ok(()) => return Self(path),
                    Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
                    Err(e) => panic!("creating test directory: {e}"),
                }
            }
        }

        fn plugin(&self) -> Plugin {
            let root = self.0.join("music");
            std::fs::create_dir(&root).unwrap();
            let state_file = self.0.join("index.json");
            let index = BTreeMap::from([("existing.flac".to_string(), 123)]);
            std::fs::write(&state_file, serde_json::to_vec(&index).unwrap()).unwrap();
            Plugin {
                root,
                state_file,
                index,
                session: None,
            }
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn assert_failed_scan_preserves_state(plugin: &mut Plugin) {
        let index = plugin.index.clone();
        let saved = std::fs::read(&plugin.state_file).unwrap();
        for cursor in [Some("previous".to_string()), None] {
            assert!(plugin.pull(SyncPull { cursor }).is_err());
            assert_eq!(plugin.index, index);
            assert!(plugin.session.is_none());
            assert_eq!(std::fs::read(&plugin.state_file).unwrap(), saved);
        }
    }

    #[test]
    fn missing_root_preserves_index_and_can_be_retried() {
        let tmp = TempDir::new();
        let mut plugin = tmp.plugin();
        std::fs::remove_dir(&plugin.root).unwrap();
        assert_failed_scan_preserves_state(&mut plugin);

        // A genuinely empty directory still reports deletions after recovery.
        std::fs::create_dir(&plugin.root).unwrap();
        let batch = plugin
            .pull(SyncPull {
                cursor: Some("previous".to_string()),
            })
            .unwrap();
        assert!(batch.done);
        assert_eq!(batch.deletes, vec![Plugin::uri("existing.flac")]);
        assert!(plugin.index.is_empty());
    }

    #[test]
    fn non_directory_root_preserves_index() {
        let tmp = TempDir::new();
        let mut plugin = tmp.plugin();
        std::fs::remove_dir(&plugin.root).unwrap();
        std::fs::write(&plugin.root, b"not a directory").unwrap();
        assert_failed_scan_preserves_state(&mut plugin);
    }

    #[cfg(unix)]
    #[test]
    fn walk_error_preserves_index() {
        let tmp = TempDir::new();
        let mut plugin = tmp.plugin();
        // A loop reliably produces a WalkDir error, even when tests run as root.
        std::os::unix::fs::symlink(&plugin.root, plugin.root.join("loop")).unwrap();
        assert_failed_scan_preserves_state(&mut plugin);
    }

    #[cfg(unix)]
    #[test]
    fn scan_still_follows_directory_symlinks() {
        let tmp = TempDir::new();
        let plugin = tmp.plugin();
        let target = tmp.0.join("linked-music");
        std::fs::create_dir(&target).unwrap();
        std::fs::write(target.join("track.flac"), b"scan only").unwrap();
        std::os::unix::fs::symlink(&target, plugin.root.join("linked")).unwrap();
        let session = plugin.diff(false).unwrap();
        assert!(session.new_index.contains_key("linked/track.flac"));
        assert!(session
            .pending_upserts
            .contains(&"linked/track.flac".to_string()));
    }

    #[test]
    fn full_scan_replays_unchanged_files_without_deleting_old_index_entries() {
        let tmp = TempDir::new();
        let mut plugin = tmp.plugin();
        std::fs::write(plugin.root.join("existing.flac"), b"scan only").unwrap();
        plugin.index = plugin.diff(false).unwrap().new_index;
        plugin.index.insert("missing.flac".to_string(), 123);

        let incremental = plugin.diff(false).unwrap();
        assert!(incremental.pending_upserts.is_empty());
        assert_eq!(
            incremental.pending_deletes,
            vec![Plugin::uri("missing.flac")]
        );

        let full = plugin.diff(true).unwrap();
        assert_eq!(
            full.pending_upserts,
            VecDeque::from(["existing.flac".to_string()])
        );
        assert!(full.pending_deletes.is_empty());
        assert_eq!(plugin.index.len(), 2);
    }
}
