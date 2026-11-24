mod config;
mod core;
mod db;
mod engine;
mod http;
mod output;
mod plugin;
mod server;

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use clap::Parser;
use lark_protocol::{CacheMode, Notification, PluginRole, SyncBatch, SyncPull};
use serde_json::json;
use tokio::sync::{broadcast, mpsc};
use tracing_subscriber::prelude::*;

use crate::config::{Config, OutputKind};
use crate::core::{Core, CoreMsg};
use crate::db::Library;
use crate::plugin::{PluginEvent, Plugins};

#[derive(Parser)]
#[command(name = "larkd", version, about = "lark music daemon")]
struct Args {
    /// Config file (default: ~/.config/lark/config.toml).
    #[arg(long)]
    config: Option<PathBuf>,
    /// Override the audio output.
    #[arg(long, value_enum)]
    output: Option<OutputKind>,
    /// Override the client socket path.
    #[arg(long)]
    socket: Option<PathBuf>,
    /// Override the library database path.
    #[arg(long)]
    db: Option<PathBuf>,
    /// Shortcut: use this directory for the `local` source.
    #[arg(long)]
    music_dir: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Signed media URLs and authentication headers must not leak even when a
    // caller enables dependency trace logging through LARK_LOG.
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_env("LARK_LOG")
                .unwrap_or_else(|_| "info".into()),
        )
        .with(
            tracing_subscriber::fmt::layer()
                .with_writer(std::io::stderr)
                .with_filter(tracing_subscriber::filter::filter_fn(|metadata| {
                    safe_log_target(metadata.target())
                })),
        )
        .init();

    let args = Args::parse();
    let cfg_path = args.config.clone().unwrap_or_else(Config::default_path);
    let mut cfg = Config::load(&cfg_path)?;
    if let Some(o) = args.output {
        cfg.output = o;
    }
    if let Some(dir) = &args.music_dir {
        for p in cfg.plugins.iter_mut().filter(|p| p.name == "local") {
            p.config["music_dir"] = json!(dir);
        }
    }
    let socket = args
        .socket
        .or(cfg.socket.clone())
        .unwrap_or_else(lark_protocol::socket_path);
    let db_path = args
        .db
        .unwrap_or_else(|| config::data_dir().join("library.db"));

    let mut library =
        Library::open(&db_path).with_context(|| format!("opening {}", db_path.display()))?;
    let configured: Vec<_> = cfg
        .plugins
        .iter()
        .filter(|plugin| plugin.enabled)
        .map(|plugin| plugin.name.clone())
        .collect();
    library.set_enabled_sources(&configured)?;
    let lib = Arc::new(Mutex::new(library));
    let (events_tx, _) = broadcast::channel::<Notification>(1024);

    let (plugin_tx, plugin_rx) = mpsc::unbounded_channel();
    let plugins = Plugins::start(&cfg.plugins, plugin_tx);

    let (core_tx, core_rx) = mpsc::unbounded_channel();
    let (engine_tx, mut engine_rx) = mpsc::unbounded_channel();
    let engine = engine::spawn(cfg.output, engine_tx);
    {
        let core_tx = core_tx.clone();
        tokio::spawn(async move {
            while let Some(ev) = engine_rx.recv().await {
                let _ = core_tx.send(CoreMsg::Engine(ev));
            }
        });
    }
    tokio::spawn(Core::new(lib.clone(), plugins.clone(), engine, events_tx.clone()).run(core_rx));

    let (sync_tx, sync_rx) = mpsc::unbounded_channel();
    tokio::spawn(sync_loop(
        lib.clone(),
        plugins.clone(),
        plugin_rx,
        sync_rx,
        events_tx.clone(),
    ));

    let ctx = server::Ctx {
        lib,
        plugins: plugins.clone(),
        core: core_tx,
        events: events_tx,
        sync: sync_tx,
    };
    plugins.attach_frontend(ctx.clone());
    let sock = socket.clone();
    tracing::info!(output = ?cfg.output, db = %db_path.display(), "larkd starting");

    tokio::select! {
        r = server::serve(&sock, ctx) => r?,
        _ = shutdown_signal() => tracing::info!("shutting down"),
    }
    plugins.shutdown().await;
    let _ = std::fs::remove_file(&socket);
    Ok(())
}

fn safe_log_target(target: &str) -> bool {
    !matches!(
        target.split("::").next(),
        Some("reqwest" | "hyper" | "hyper_util" | "h2" | "rustls" | "rustls_platform_verifier")
    )
}

async fn shutdown_signal() {
    use tokio::signal::unix::{signal, SignalKind};
    let mut term = signal(SignalKind::terminate()).expect("sigterm");
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = term.recv() => {}
    }
}

/// Serialises syncs: one source at a time, triggered by plugin readiness,
/// `sync.dirty` notifications, or a client `library.sync`.
async fn sync_loop(
    lib: Arc<Mutex<Library>>,
    plugins: Arc<Plugins>,
    mut plugin_rx: mpsc::UnboundedReceiver<PluginEvent>,
    mut sync_rx: mpsc::UnboundedReceiver<Option<String>>,
    events: broadcast::Sender<Notification>,
) {
    loop {
        let targets: Vec<String> = tokio::select! {
            Some(ev) = plugin_rx.recv() => match ev {
                PluginEvent::Ready { name, info } => {
                    if info.role == PluginRole::Frontend {
                        match lib.lock().unwrap().hide_source(&name) {
                            Ok(true) => {
                                let _ = events.send(Notification {
                                    method: "library.updated".into(),
                                    params: json!({ "source": name, "visibility_changed": true }),
                                });
                            }
                            Ok(false) => {}
                            Err(error) => tracing::warn!("filter frontend catalogue: {error:#}"),
                        }
                        continue;
                    }
                    if let Err(e) = lib.lock().unwrap().register_source(&name, &info.scheme, info.cache) {
                        tracing::warn!("register source {name}: {e:#}");
                    }
                    if info.cache == CacheMode::Full { vec![name] } else { vec![] }
                }
                PluginEvent::Dirty { name } => vec![name],
                PluginEvent::Down { name } => {
                    tracing::warn!(plugin = %name, "source down");
                    vec![]
                }
            },
            Some(req) = sync_rx.recv() => match req {
                Some(name) => vec![name],
                None => plugins
                    .all()
                    .iter()
                    .filter(|h| h.info().is_some_and(|i| i.cache == CacheMode::Full))
                    .map(|h| h.name.clone())
                    .collect(),
            },
            else => return,
        };
        for name in targets {
            match sync_source(&lib, &plugins, &name).await {
                Ok((up, del)) => {
                    tracing::info!(source = %name, upserts = up, deletes = del, "sync complete");
                    let _ = events.send(Notification {
                        method: "library.updated".into(),
                        params: json!({ "source": name, "upserts": up, "deletes": del }),
                    });
                }
                Err(e) => tracing::warn!(source = %name, "sync failed: {e:#}"),
            }
        }
    }
}

async fn sync_source(
    lib: &Arc<Mutex<Library>>,
    plugins: &Plugins,
    name: &str,
) -> Result<(usize, usize)> {
    let host = plugins.by_name(name).context("unknown source")?;
    let mut cursor = lib.lock().unwrap().source_cursor(name)?;
    let (mut up, mut del) = (0, 0);
    loop {
        let batch: SyncBatch = host
            .call(
                "sync.pull",
                SyncPull {
                    cursor: cursor.clone(),
                },
            )
            .await?;
        up += batch.upserts.len();
        del += batch.deletes.len();
        let done = batch.done;
        if done {
            cursor = batch.next_cursor.clone();
        }
        let (lib2, name2) = (lib.clone(), name.to_string());
        tokio::task::spawn_blocking(move || lib2.lock().unwrap().apply_batch(&name2, &batch))
            .await??;
        if done {
            break;
        }
    }
    let _ = cursor;
    Ok((up, del))
}

#[cfg(test)]
mod logging_tests {
    #[test]
    fn transport_targets_are_never_logged_but_core_logs_remain_available() {
        for target in [
            "reqwest",
            "reqwest::async_impl::client",
            "hyper::proto",
            "hyper_util::client",
            "h2",
            "rustls::client",
            "rustls_platform_verifier",
        ] {
            assert!(!super::safe_log_target(target));
        }
        assert!(super::safe_log_target("larkd::http"));
        assert!(super::safe_log_target("larkd::core"));
    }
}
