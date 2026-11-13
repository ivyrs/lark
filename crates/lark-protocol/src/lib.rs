//! Wire types shared by the daemon, clients and source plugins.
//!
//! Transport is newline-delimited JSON-RPC-ish messages, both on the client
//! socket and on plugin stdio. Either side may send requests; notifications
//! carry no `id`.

use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const CLIENT_API_VERSION: u32 = 1;
pub const PLUGIN_API_VERSION: u32 = 1;

// ───────────────────────────── framing ─────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Request {
    pub id: u64,
    pub method: String,
    #[serde(default, skip_serializing_if = "Value::is_null")]
    pub params: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Response {
    pub id: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<RpcError>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Notification {
    pub method: String,
    #[serde(default, skip_serializing_if = "Value::is_null")]
    pub params: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcError {
    pub code: i32,
    pub message: String,
}

impl RpcError {
    pub const PARSE: i32 = -32700;
    pub const METHOD_NOT_FOUND: i32 = -32601;
    pub const INVALID_PARAMS: i32 = -32602;
    pub const INTERNAL: i32 = -32603;

    pub fn new(code: i32, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}

impl std::fmt::Display for RpcError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "rpc error {}: {}", self.code, self.message)
    }
}
impl std::error::Error for RpcError {}

/// Any message on the wire. Order matters for untagged decoding.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Message {
    Request(Request),
    Response(Response),
    Notification(Notification),
}

impl Response {
    pub fn ok(id: u64, result: impl Serialize) -> Self {
        Self {
            id,
            result: Some(serde_json::to_value(result).unwrap_or(Value::Null)),
            error: None,
        }
    }
    pub fn err(id: u64, error: RpcError) -> Self {
        Self {
            id,
            result: None,
            error: Some(error),
        }
    }
}

/// Serialise a message as one line (with trailing `\n`).
pub fn encode(msg: &impl Serialize) -> String {
    let mut s = serde_json::to_string(msg).expect("serialisable");
    s.push('\n');
    s
}

// ───────────────────────────── shared models ─────────────────────────────

/// Track metadata as reported by a source and as served to clients.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Track {
    pub uri: String,
    pub title: String,
    #[serde(default)]
    pub artists: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub album: Option<String>,
    #[serde(default)]
    pub album_artists: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub track_no: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub disc_no: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub date: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mbid: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub release_mbid: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub isrc: Option<String>,
    /// Everything else, multi-valued (genre, label, …).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tags: Vec<(String, String)>,
}

impl Track {
    pub fn artist_line(&self) -> String {
        if self.artists.is_empty() {
            "Unknown Artist".into()
        } else {
            self.artists.join(", ")
        }
    }
}

// ───────────────────────────── client API ─────────────────────────────
//
// Methods (params → result):
//   hello            {}                              → Hello
//   subscribe        {}                              → null, then events on this connection
//   status           {}                              → Status
//   player.play      {index?}                        → null
//   player.pause / player.resume / player.toggle / player.stop / player.next / player.prev
//   player.seek      {position_ms}                   → null
//   queue.add        {uris, position?, play?}        → [QueueItem]
//   queue.remove     {ids}                           → null
//   queue.clear      {}                              → null
//   library.search   {query, limit?}                 → [Track]
//   library.artists  {}                              → [ArtistRef]
//   library.releases {artist_id?}                    → [ReleaseRef]
//   library.tracks   {release_id? | artist_id?}      → [Track]
//   library.sync     {}                              → null
//   sources          {}                              → [SourceInfo]
//   history          {limit?}                        → [Play]
//
// Events (notifications after `subscribe`):
//   player.state     PlayerState
//   queue.inserted   {position, items}
//   queue.removed    {ids}
//   queue.cleared    {}
//   library.updated  {source}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Hello {
    pub server: String,
    pub version: String,
    pub api: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum PlayState {
    #[default]
    Stopped,
    Playing,
    Paused,
}

/// Player snapshot. Clients extrapolate position while `state == Playing`
/// using `position_ms + (now - at_ms)`, so the daemon only emits this on
/// real changes (track, pause, seek), never as a ticking stream.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PlayerState {
    pub state: PlayState,
    /// Queue item id currently loaded.
    pub current: Option<u64>,
    pub position_ms: u64,
    pub duration_ms: Option<u64>,
    /// Unix epoch ms when `position_ms` was sampled.
    pub at_ms: u64,
}

impl PlayerState {
    pub fn position_now(&self, now_ms: u64) -> u64 {
        let pos = if self.state == PlayState::Playing {
            self.position_ms + now_ms.saturating_sub(self.at_ms)
        } else {
            self.position_ms
        };
        match self.duration_ms {
            Some(d) => pos.min(d),
            None => pos,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueItem {
    pub id: u64,
    pub track: Track,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Status {
    pub player: PlayerState,
    pub queue: Vec<QueueItem>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArtistRef {
    pub id: i64,
    pub name: String,
    pub releases: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReleaseRef {
    pub id: i64,
    pub title: String,
    pub artist: String,
    pub date: Option<String>,
    pub tracks: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceInfo {
    pub name: String,
    pub scheme: String,
    pub cache: CacheMode,
    pub running: bool,
    pub tracks: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Play {
    pub uri: String,
    pub started_at: u64,
    pub played_ms: u64,
    pub completed: bool,
    pub track: Track,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueInserted {
    pub position: usize,
    pub items: Vec<QueueItem>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueRemoved {
    pub ids: Vec<u64>,
}

// ───────────────────────────── plugin API ─────────────────────────────
//
// core → plugin requests:
//   initialize  InitializeParams            → InitializeResult
//   sync.pull   {cursor?}                   → SyncBatch      (cache = full)
//   resolve     {uri}                       → Stream
//   search      {query, limit}              → [Track]        (cache = partial | none)
//   shutdown    {}                          → null
//
// plugin → core notifications:
//   sync.dirty  {}                          the core should pull again
//   log         {level, message}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CacheMode {
    /// Core mirrors the whole catalogue via `sync.pull`.
    Full,
    /// Core caches only tracks that get queued / played / saved.
    Partial,
    /// Nothing cached; the source answers `search` live.
    None,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InitializeParams {
    pub api: u32,
    pub config: Value,
    /// Per-plugin directory for the plugin's own persistent state.
    pub state_dir: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InitializeResult {
    pub name: String,
    /// URI scheme this plugin owns, e.g. `local` → `local:track:…`.
    pub scheme: String,
    pub cache: CacheMode,
    pub version: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SyncPull {
    pub cursor: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SyncBatch {
    pub upserts: Vec<Track>,
    pub deletes: Vec<String>,
    pub next_cursor: Option<String>,
    pub done: bool,
}

/// What the core should play for a URI. Plugins never touch audio.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum Stream {
    File {
        path: String,
    },
    Http {
        url: String,
        #[serde(default)]
        headers: Vec<(String, String)>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResolveParams {
    pub uri: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchParams {
    pub query: String,
    #[serde(default = "default_limit")]
    pub limit: u32,
}

pub fn default_limit() -> u32 {
    200
}

pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Default client socket path: `$LARK_SOCKET`, else `$XDG_RUNTIME_DIR/lark.sock`,
/// else `$TMPDIR/lark-$USER.sock`.
pub fn socket_path() -> std::path::PathBuf {
    if let Some(p) = std::env::var_os("LARK_SOCKET") {
        return p.into();
    }
    if let Some(d) = std::env::var_os("XDG_RUNTIME_DIR") {
        return std::path::PathBuf::from(d).join("lark.sock");
    }
    let user = std::env::var("USER").unwrap_or_else(|_| "user".into());
    std::env::temp_dir().join(format!("lark-{user}.sock"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn untagged_message_roundtrip() {
        let req = r#"{"id":1,"method":"status"}"#;
        assert!(matches!(
            serde_json::from_str::<Message>(req).unwrap(),
            Message::Request(_)
        ));
        let resp = r#"{"id":1,"result":null}"#;
        assert!(matches!(
            serde_json::from_str::<Message>(resp).unwrap(),
            Message::Response(_)
        ));
        let note = r#"{"method":"sync.dirty"}"#;
        assert!(matches!(
            serde_json::from_str::<Message>(note).unwrap(),
            Message::Notification(_)
        ));
    }

    #[test]
    fn stream_tagging() {
        let s = serde_json::to_string(&Stream::File {
            path: "/a.flac".into(),
        })
        .unwrap();
        assert_eq!(s, r#"{"kind":"file","path":"/a.flac"}"#);
    }
}
