//! Client RPC over the Unix socket and frontend plugin stdio. Library reads
//! go straight to SQLite; player/queue calls go through the core actor.

use std::os::unix::fs::FileTypeExt;
use std::path::Path;
use std::sync::{Arc, Mutex, Weak};

use anyhow::{anyhow, Result};
use lark_protocol::{
    default_limit, encode, Hello, Message, Notification, PluginRole, Request, Response, RpcError,
    SearchParams, SourceInfo, CLIENT_API_VERSION,
};
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio::task::JoinSet;

use crate::core::CoreMsg;
use crate::db::Library;
use crate::plugin::Plugins;

#[derive(Clone)]
pub struct Ctx {
    pub lib: Arc<Mutex<Library>>,
    pub plugins: Arc<Plugins>,
    pub core: mpsc::UnboundedSender<CoreMsg>,
    pub events: broadcast::Sender<Notification>,
    pub sync: mpsc::UnboundedSender<Option<String>>,
}

/// A frontend bridge must not itself keep the daemon, core actor or sync loop alive.
#[derive(Clone)]
pub(crate) struct WeakCtx {
    lib: Weak<Mutex<Library>>,
    plugins: Weak<Plugins>,
    core: mpsc::WeakUnboundedSender<CoreMsg>,
    events: broadcast::WeakSender<Notification>,
    sync: mpsc::WeakUnboundedSender<Option<String>>,
}

impl Ctx {
    pub(crate) fn downgrade(&self) -> WeakCtx {
        WeakCtx {
            lib: Arc::downgrade(&self.lib),
            plugins: Arc::downgrade(&self.plugins),
            core: self.core.downgrade(),
            events: self.events.downgrade(),
            sync: self.sync.downgrade(),
        }
    }
}

impl WeakCtx {
    pub(crate) fn upgrade(&self) -> Option<Ctx> {
        Some(Ctx {
            lib: self.lib.upgrade()?,
            plugins: self.plugins.upgrade()?,
            core: self.core.upgrade()?,
            events: self.events.upgrade()?,
            sync: self.sync.upgrade()?,
        })
    }
}

async fn bind_listener(path: &Path) -> Result<UnixListener> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) => {
            // Only stale sockets are ours to clean up. In particular, do not
            // follow symlinks (including dangling ones) or delete regular files.
            if !metadata.file_type().is_socket() {
                return Err(anyhow!("refusing to replace non-socket {}", path.display()));
            }
            // Refuse to clobber a live daemon.
            if UnixStream::connect(path).await.is_ok() {
                return Err(anyhow!("another larkd is listening on {}", path.display()));
            }
            std::fs::remove_file(path)?;
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(e.into()),
    }
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)?;
    }
    Ok(UnixListener::bind(path)?)
}

pub async fn serve(path: &Path, ctx: Ctx) -> Result<()> {
    let listener = bind_listener(path).await?;
    tracing::info!("listening on {}", path.display());
    loop {
        let (stream, _) = listener.accept().await?;
        let ctx = ctx.clone();
        tokio::spawn(async move {
            if let Err(e) = connection(stream, ctx).await {
                tracing::debug!("client disconnected: {e:#}");
            }
        });
    }
}

async fn connection(stream: UnixStream, ctx: Ctx) -> Result<()> {
    let (rd, wr) = stream.into_split();
    let (out_tx, out_rx) = mpsc::channel(OUTPUT_CAPACITY);
    let reader = async move {
        let mut session = RpcSession::new(out_tx);
        let mut lines = BufReader::new(rd).lines();
        loop {
            tokio::select! {
                line = lines.next_line() => {
                    let Some(line) = line? else { break };
                    if line.trim().is_empty() {
                        continue;
                    }
                    match serde_json::from_str::<Message>(&line) {
                        Ok(Message::Request(req)) => session.request(&ctx, req)?,
                        Ok(_) => (),
                        Err(e) => send_line(&session.out, &Response::err(
                            0, RpcError::new(RpcError::PARSE, e.to_string()),
                        ))?,
                    }
                }
                progress = session.background() => progress?,
            }
        }
        // Accept write half-closes: drain outstanding replies but don't keep
        // the socket (or the subscription) alive for future notifications.
        session.subscription = None;
        while !session.tasks.is_empty() {
            session.background().await?;
        }
        Ok::<_, anyhow::Error>(())
    };
    tokio::try_join!(reader, write_lines(wr, out_rx))?;
    Ok(())
}

pub(crate) const OUTPUT_CAPACITY: usize = 128;
const IN_FLIGHT_LIMIT: usize = 64;

pub(crate) async fn write_lines(
    mut pipe: impl AsyncWrite + Unpin,
    mut out: mpsc::Receiver<String>,
) -> Result<()> {
    while let Some(line) = out.recv().await {
        pipe.write_all(line.as_bytes()).await?;
    }
    Ok(())
}

/// Disconnect a peer that cannot keep up; never silently drop a reply or
/// leave playback waiting for the peer's pipe. Broadcast lag, separately, can
/// be recovered from with `resync` while the peer can still receive output.
pub(crate) fn send_line(out: &mpsc::Sender<String>, msg: &impl serde::Serialize) -> Result<()> {
    out.try_send(encode(msg)).map_err(|e| match e {
        mpsc::error::TrySendError::Full(_) => anyhow!("RPC output buffer full"),
        mpsc::error::TrySendError::Closed(_) => anyhow!("RPC output pipe closed"),
    })
}

pub(crate) struct RpcSession {
    out: mpsc::Sender<String>,
    subscription: Option<broadcast::Receiver<Notification>>,
    // Dropping the session drops the subscription and aborts pending replies.
    tasks: JoinSet<Response>,
}

impl RpcSession {
    pub(crate) fn new(out: mpsc::Sender<String>) -> Self {
        Self {
            out,
            subscription: None,
            tasks: JoinSet::new(),
        }
    }

    pub(crate) fn request(&mut self, ctx: &Ctx, req: Request) -> Result<()> {
        if req.method == "subscribe" {
            if self.subscription.is_none() {
                self.subscription = Some(ctx.events.subscribe());
            }
            // Enqueue the acknowledgement before polling any notifications.
            return send_line(&self.out, &Response::ok(req.id, Value::Null));
        }
        if self.tasks.len() >= IN_FLIGHT_LIMIT {
            return Err(anyhow!("too many in-flight RPC requests"));
        }
        if is_local(&req.method) {
            // Library reads run concurrently so a slow query never blocks events.
            let ctx = ctx.clone();
            self.tasks.spawn(async move {
                match dispatch(&ctx, &req.method, req.params).await {
                    Ok(v) => Response::ok(req.id, v),
                    Err(e) => Response::err(req.id, e),
                }
            });
        } else {
            // Enqueue synchronously so e.g. queue.add then player.play cannot
            // be reordered even though their replies are handled asynchronously.
            let rx = core_call(ctx, req.method, req.params);
            self.tasks.spawn(async move {
                match rx.await {
                    Ok(Ok(v)) => Response::ok(req.id, v),
                    Ok(Err(e)) => {
                        let msg = format!("{e:#}");
                        let code = if msg.starts_with("unknown method") {
                            RpcError::METHOD_NOT_FOUND
                        } else {
                            RpcError::INTERNAL
                        };
                        Response::err(req.id, RpcError::new(code, msg))
                    }
                    Err(_) => Response::err(req.id, internal("core stopped")),
                }
            });
        }
        Ok(())
    }

    /// Advance one background response or event. Both receives are safe to
    /// cancel when a transport's next input line wins its select instead.
    pub(crate) async fn background(&mut self) -> Result<()> {
        tokio::select! {
            Some(resp) = self.tasks.join_next(), if !self.tasks.is_empty() => {
                send_line(&self.out, &resp?)
            }
            n = recv_event(&mut self.subscription), if self.subscription.is_some() => {
                match n {
                    Ok(n) => send_line(&self.out, &n),
                    Err(broadcast::error::RecvError::Lagged(k)) => send_line(&self.out, &Notification {
                        method: "resync".into(),
                        params: json!({ "missed": k }),
                    }),
                    Err(broadcast::error::RecvError::Closed) => {
                        self.subscription = None;
                        Ok(())
                    }
                }
            }
            else => std::future::pending().await,
        }
    }
}

async fn recv_event(
    subscription: &mut Option<broadcast::Receiver<Notification>>,
) -> Result<Notification, broadcast::error::RecvError> {
    subscription.as_mut().unwrap().recv().await
}

const LOCAL: &[&str] = &[
    "hello",
    "library.search",
    "library.artists",
    "library.releases",
    "library.tracks",
    "library.sync",
    "history",
    "sources",
];

fn is_local(method: &str) -> bool {
    LOCAL.contains(&method)
}

fn core_call(ctx: &Ctx, method: String, params: Value) -> oneshot::Receiver<anyhow::Result<Value>> {
    let (tx, rx) = oneshot::channel();
    let _ = ctx.core.send(CoreMsg::Rpc {
        method,
        params,
        reply: tx,
    });
    rx
}

#[derive(Deserialize, Default)]
struct BrowseParams {
    artist_id: Option<i64>,
    release_id: Option<i64>,
}

#[derive(Deserialize, Default)]
struct LimitParams {
    limit: Option<u32>,
}

#[derive(Deserialize, Default)]
struct TrackBrowseParams {
    #[serde(flatten)]
    filter: BrowseParams,
    limit: Option<u32>,
    after_uri: Option<String>,
}

fn bad(e: impl std::fmt::Display) -> RpcError {
    RpcError::new(RpcError::INVALID_PARAMS, e.to_string())
}

fn internal(e: impl std::fmt::Display) -> RpcError {
    RpcError::new(RpcError::INTERNAL, e.to_string())
}

fn params<T: for<'de> Deserialize<'de> + Default>(v: Value) -> Result<T, RpcError> {
    if v.is_null() {
        Ok(T::default())
    } else {
        serde_json::from_value(v).map_err(bad)
    }
}

async fn with_lib<T: Send + 'static>(
    ctx: &Ctx,
    f: impl FnOnce(&mut Library) -> Result<T> + Send + 'static,
) -> Result<T, RpcError> {
    let lib = ctx.lib.clone();
    tokio::task::spawn_blocking(move || f(&mut lib.lock().unwrap()))
        .await
        .map_err(internal)?
        .map_err(|e| internal(format!("{e:#}")))
}

async fn dispatch(ctx: &Ctx, method: &str, p: Value) -> Result<Value, RpcError> {
    let to = |v: Result<Value, serde_json::Error>| v.map_err(internal);
    match method {
        "hello" => to(serde_json::to_value(Hello {
            server: "larkd".into(),
            version: env!("CARGO_PKG_VERSION").into(),
            api: CLIENT_API_VERSION,
        })),
        "library.search" => {
            let sp: SearchParams = serde_json::from_value(p).map_err(bad)?;
            to(serde_json::to_value(
                with_lib(ctx, move |l| l.search(&sp.query, sp.limit)).await?,
            ))
        }
        "library.artists" => to(serde_json::to_value(with_lib(ctx, |l| l.artists()).await?)),
        "library.releases" => {
            let bp: BrowseParams = params(p)?;
            to(serde_json::to_value(
                with_lib(ctx, move |l| l.releases(bp.artist_id)).await?,
            ))
        }
        "library.tracks" => {
            let bp: TrackBrowseParams = params(p)?;
            let tracks = match (bp.filter.release_id, bp.filter.artist_id) {
                (Some(r), _) => with_lib(ctx, move |l| l.tracks_by_release(r)).await?,
                (None, Some(a)) => with_lib(ctx, move |l| l.tracks_by_artist(a)).await?,
                (None, None) => {
                    let limit = bp.limit.unwrap_or_else(default_limit);
                    if !(1..=1000).contains(&limit) {
                        return Err(bad("limit must be between 1 and 1000"));
                    }
                    with_lib(ctx, move |l| l.all_tracks(limit, bp.after_uri.as_deref())).await?
                }
            };
            to(serde_json::to_value(tracks))
        }
        "library.sync" => {
            let _ = ctx.sync.send(None);
            Ok(Value::Null)
        }
        "history" => {
            let lp: LimitParams = params(p)?;
            to(serde_json::to_value(
                with_lib(ctx, move |l| l.history(lp.limit.unwrap_or(50))).await?,
            ))
        }
        "sources" => {
            let mut out = Vec::new();
            for h in ctx.plugins.all() {
                let Some(info) = h.info().filter(|i| i.role == PluginRole::Source) else {
                    continue;
                };
                let name = h.name.clone();
                let tracks = with_lib(ctx, move |l| l.source_track_count(&name))
                    .await
                    .unwrap_or(0);
                out.push(SourceInfo {
                    name: h.name.clone(),
                    scheme: info.scheme,
                    cache: info.cache,
                    running: h.running(),
                    tracks,
                });
            }
            to(serde_json::to_value(out))
        }
        other => Err(RpcError::new(RpcError::METHOD_NOT_FOUND, other.to_string())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::symlink;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    struct Fixture(PathBuf);

    impl Fixture {
        fn new() -> Self {
            static NEXT: AtomicU64 = AtomicU64::new(0);
            let path = std::env::temp_dir().join(format!(
                "larkd-sock-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            std::fs::create_dir(&path).unwrap();
            Self(path)
        }

        fn socket(&self) -> PathBuf {
            self.0.join("lark.sock")
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    #[tokio::test]
    async fn subscription_acks_then_signals_lag_and_releases_receiver() {
        let (plugin_tx, _) = mpsc::unbounded_channel();
        let plugins = Plugins::start(&[], plugin_tx);
        let (core, _core_rx) = mpsc::unbounded_channel();
        let (events, _) = broadcast::channel(1);
        let (sync, _sync_rx) = mpsc::unbounded_channel();
        let ctx = Ctx {
            lib: Arc::new(Mutex::new(Library::memory().unwrap())),
            plugins,
            core,
            events,
            sync,
        };
        let (out, mut frames) = mpsc::channel(2);
        let mut session = RpcSession::new(out);
        session
            .request(
                &ctx,
                Request {
                    id: 9,
                    method: "subscribe".into(),
                    params: Value::Null,
                },
            )
            .unwrap();
        for position in 0..3 {
            ctx.events
                .send(Notification {
                    method: "player.state".into(),
                    params: json!({ "position_ms": position }),
                })
                .unwrap();
        }
        session.background().await.unwrap();
        assert_eq!(frames.try_recv().unwrap(), "{\"id\":9,\"result\":null}\n");
        let emitted: Notification = serde_json::from_str(&frames.try_recv().unwrap()).unwrap();
        assert_eq!(emitted.method, "resync");
        assert_eq!(emitted.params, json!({ "missed": 2 }));

        session.background().await.unwrap();
        for position in 3..5 {
            ctx.events
                .send(Notification {
                    method: "player.state".into(),
                    params: json!({ "position_ms": position }),
                })
                .unwrap();
        }
        session.background().await.unwrap(); // One event and one resync are queued.
        assert!(session
            .background()
            .await
            .unwrap_err()
            .to_string()
            .contains("buffer full"));
        drop(session);
        assert_eq!(ctx.events.receiver_count(), 0);
    }

    #[tokio::test]
    async fn socket_refuses_regular_file_without_modifying_it() {
        let fixture = Fixture::new();
        let path = fixture.socket();
        std::fs::write(&path, b"preserve me").unwrap();
        let error = bind_listener(&path).await.unwrap_err();
        assert!(error.to_string().contains("non-socket"));
        assert_eq!(std::fs::read(&path).unwrap(), b"preserve me");
    }

    #[tokio::test]
    async fn socket_refuses_symlinks_including_dangling_and_socket_targets() {
        let fixture = Fixture::new();
        let path = fixture.socket();
        let target = fixture.0.join("target");
        // A dangling symlink must be refused, not treated as an absent path.
        symlink(&target, &path).unwrap();
        assert!(bind_listener(&path)
            .await
            .unwrap_err()
            .to_string()
            .contains("non-socket"));
        assert_eq!(std::fs::read_link(&path).unwrap(), target);
        // Nor may we replace a symlink merely because its target is a socket.
        let listener = UnixListener::bind(&target).unwrap();
        assert!(bind_listener(&path)
            .await
            .unwrap_err()
            .to_string()
            .contains("non-socket"));
        assert_eq!(std::fs::read_link(&path).unwrap(), target);
        drop(listener);
        assert!(bind_listener(&path)
            .await
            .unwrap_err()
            .to_string()
            .contains("non-socket"));
        assert!(std::fs::symlink_metadata(&target)
            .unwrap()
            .file_type()
            .is_socket());
    }

    #[tokio::test]
    async fn socket_replaces_stale_socket_and_accepts_connections() {
        let fixture = Fixture::new();
        let path = fixture.socket();
        drop(UnixListener::bind(&path).unwrap());
        let listener = bind_listener(&path).await.unwrap();
        let _client = UnixStream::connect(&path).await.unwrap();
        listener.accept().await.unwrap();
    }

    #[tokio::test]
    async fn socket_refuses_live_listener_without_unlinking_it() {
        let fixture = Fixture::new();
        let path = fixture.socket();
        let listener = bind_listener(&path).await.unwrap();
        let error = bind_listener(&path).await.unwrap_err();
        assert!(error.to_string().contains("another larkd"));
        assert!(std::fs::symlink_metadata(&path)
            .unwrap()
            .file_type()
            .is_socket());
        let _client = UnixStream::connect(&path).await.unwrap();
        listener.accept().await.unwrap();
    }
}
