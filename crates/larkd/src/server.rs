//! Client socket: newline-delimited JSON-RPC over a Unix socket. Library
//! reads go straight to SQLite; player/queue calls go through the core actor.

use std::path::Path;
use std::sync::{Arc, Mutex};

use anyhow::{anyhow, Result};
use lark_protocol::{
    encode, Hello, Message, Notification, Response, RpcError, SearchParams, SourceInfo,
    CLIENT_API_VERSION,
};
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{broadcast, mpsc, oneshot};

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

pub async fn serve(path: &Path, ctx: Ctx) -> Result<()> {
    if path.exists() {
        // Refuse to clobber a live daemon.
        if UnixStream::connect(path).await.is_ok() {
            return Err(anyhow!("another larkd is listening on {}", path.display()));
        }
        std::fs::remove_file(path)?;
    }
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)?;
    }
    let listener = UnixListener::bind(path)?;
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
    let (rd, mut wr) = stream.into_split();
    let (out_tx, mut out_rx) = mpsc::unbounded_channel::<String>();
    let writer = tokio::spawn(async move {
        while let Some(line) = out_rx.recv().await {
            if wr.write_all(line.as_bytes()).await.is_err() {
                break;
            }
        }
    });

    let mut lines = BufReader::new(rd).lines();
    let mut subscribed = false;
    while let Some(line) = lines.next_line().await? {
        if line.trim().is_empty() {
            continue;
        }
        let req = match serde_json::from_str::<Message>(&line) {
            Ok(Message::Request(r)) => r,
            Ok(_) => continue,
            Err(e) => {
                let _ = out_tx.send(encode(&Response::err(
                    0,
                    RpcError::new(RpcError::PARSE, e.to_string()),
                )));
                continue;
            }
        };
        if req.method == "subscribe" && !subscribed {
            subscribed = true;
            let mut rx = ctx.events.subscribe();
            let tx = out_tx.clone();
            tokio::spawn(async move {
                loop {
                    match rx.recv().await {
                        Ok(n) => {
                            if tx.send(encode(&n)).is_err() {
                                break;
                            }
                        }
                        Err(broadcast::error::RecvError::Lagged(k)) => {
                            // Client fell behind; tell it to resync from `status`.
                            let n = Notification {
                                method: "resync".into(),
                                params: json!({ "missed": k }),
                            };
                            if tx.send(encode(&n)).is_err() {
                                break;
                            }
                        }
                        Err(broadcast::error::RecvError::Closed) => break,
                    }
                }
            });
        }
        let tx = out_tx.clone();
        if is_local(&req.method) {
            // Library reads run concurrently so a slow query never blocks events.
            let ctx = ctx.clone();
            tokio::spawn(async move {
                let resp = match dispatch(&ctx, &req.method, req.params).await {
                    Ok(v) => Response::ok(req.id, v),
                    Err(e) => Response::err(req.id, e),
                };
                let _ = tx.send(encode(&resp));
            });
        } else {
            // Player/queue calls are enqueued to the core *in order* right here,
            // so `queue.add` then `player.play` can never be reordered.
            let rx = core_call(&ctx, req.method, req.params);
            tokio::spawn(async move {
                let resp = match rx.await {
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
                };
                let _ = tx.send(encode(&resp));
            });
        }
    }
    drop(out_tx);
    let _ = writer.await;
    Ok(())
}

const LOCAL: &[&str] = &[
    "hello",
    "subscribe",
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
        "subscribe" => Ok(Value::Null),
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
            let bp: BrowseParams = params(p)?;
            let tracks = match (bp.release_id, bp.artist_id) {
                (Some(r), _) => with_lib(ctx, move |l| l.tracks_by_release(r)).await?,
                (None, Some(a)) => with_lib(ctx, move |l| l.tracks_by_artist(a)).await?,
                _ => return Err(bad("release_id or artist_id required")),
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
                let info = h.info();
                let name = h.name.clone();
                let tracks = with_lib(ctx, move |l| l.source_track_count(&name))
                    .await
                    .unwrap_or(0);
                out.push(SourceInfo {
                    name: h.name.clone(),
                    scheme: info.as_ref().map(|i| i.scheme.clone()).unwrap_or_default(),
                    cache: info
                        .map(|i| i.cache)
                        .unwrap_or(lark_protocol::CacheMode::None),
                    running: h.running(),
                    tracks,
                });
            }
            to(serde_json::to_value(out))
        }
        other => Err(RpcError::new(RpcError::METHOD_NOT_FOUND, other.to_string())),
    }
}
