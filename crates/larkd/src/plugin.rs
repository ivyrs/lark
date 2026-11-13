//! Out-of-process plugin host: spawns each plugin, speaks JSON-RPC over its
//! stdio, and restarts it with backoff if it dies. A crashed plugin never
//! takes playback down with it.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context, Result};
use lark_protocol::{
    encode, InitializeParams, InitializeResult, Message, Request, RpcError, PLUGIN_API_VERSION,
};
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::Command;
use tokio::sync::{mpsc, oneshot};

use crate::config::{resolve_exec, state_dir, PluginConfig};

#[derive(Debug, Clone)]
pub enum PluginEvent {
    Ready {
        name: String,
        info: InitializeResult,
    },
    Dirty {
        name: String,
    },
    Down {
        name: String,
    },
}

type Pending = Arc<Mutex<HashMap<u64, oneshot::Sender<Result<Value, RpcError>>>>>;

struct Conn {
    tx: mpsc::UnboundedSender<String>,
    pending: Pending,
}

pub struct PluginHost {
    pub name: String,
    cfg: PluginConfig,
    conn: Mutex<Option<Arc<Conn>>>,
    info: RwLock<Option<InitializeResult>>,
    next_id: AtomicU64,
}

const CALL_TIMEOUT: Duration = Duration::from_secs(30);

impl PluginHost {
    pub fn info(&self) -> Option<InitializeResult> {
        self.info.read().unwrap().clone()
    }

    pub fn running(&self) -> bool {
        self.conn.lock().unwrap().is_some()
    }

    pub async fn call<P: Serialize, R: DeserializeOwned>(
        &self,
        method: &str,
        params: P,
    ) -> Result<R> {
        let conn = self
            .conn
            .lock()
            .unwrap()
            .clone()
            .ok_or_else(|| anyhow!("plugin {} is not running", self.name))?;
        self.call_on(&conn, method, params).await
    }

    async fn call_on<P: Serialize, R: DeserializeOwned>(
        &self,
        conn: &Conn,
        method: &str,
        params: P,
    ) -> Result<R> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        conn.pending.lock().unwrap().insert(id, tx);
        let req = Request {
            id,
            method: method.into(),
            params: serde_json::to_value(params)?,
        };
        conn.tx
            .send(encode(&req))
            .map_err(|_| anyhow!("plugin {} pipe closed", self.name))?;
        let res = tokio::time::timeout(CALL_TIMEOUT, rx)
            .await
            .map_err(|_| {
                conn.pending.lock().unwrap().remove(&id);
                anyhow!("plugin {} timed out on {method}", self.name)
            })?
            .map_err(|_| anyhow!("plugin {} exited during {method}", self.name))?
            .map_err(|e| anyhow!("plugin {}: {e}", self.name))?;
        serde_json::from_value(res).with_context(|| format!("decoding {method} result"))
    }

    /// Spawn the plugin and keep it alive forever.
    fn supervise(self: Arc<Self>, events: mpsc::UnboundedSender<PluginEvent>) {
        tokio::spawn(async move {
            let mut backoff = Duration::from_millis(500);
            loop {
                let started = Instant::now();
                if let Err(e) = self.run_once(&events).await {
                    tracing::warn!(plugin = %self.name, "plugin stopped: {e:#}");
                }
                *self.conn.lock().unwrap() = None;
                let _ = events.send(PluginEvent::Down {
                    name: self.name.clone(),
                });
                if started.elapsed() > Duration::from_secs(60) {
                    backoff = Duration::from_millis(500);
                }
                tracing::info!(plugin = %self.name, "restarting in {:?}", backoff);
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(30));
            }
        });
    }

    async fn run_once(&self, events: &mpsc::UnboundedSender<PluginEvent>) -> Result<()> {
        let (prog, args) = self.cfg.exec.split_first().context("empty exec")?;
        let prog = resolve_exec(prog);
        let mut child = Command::new(&prog)
            .args(args)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::inherit())
            .kill_on_drop(true)
            .spawn()
            .with_context(|| format!("spawning {}", prog.display()))?;

        let mut stdin = child.stdin.take().unwrap();
        let stdout = child.stdout.take().unwrap();
        let (tx, mut rx) = mpsc::unbounded_channel::<String>();
        let pending: Pending = Default::default();
        let conn = Arc::new(Conn {
            tx,
            pending: pending.clone(),
        });

        let writer = tokio::spawn(async move {
            while let Some(line) = rx.recv().await {
                if stdin.write_all(line.as_bytes()).await.is_err() {
                    break;
                }
            }
        });

        let name = self.name.clone();
        let ev = events.clone();
        let reader_pending = pending.clone();
        let reader = tokio::spawn(async move {
            let mut lines = BufReader::new(stdout).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                match serde_json::from_str::<Message>(&line) {
                    Ok(Message::Response(r)) => {
                        if let Some(tx) = reader_pending.lock().unwrap().remove(&r.id) {
                            let _ = tx.send(match r.error {
                                Some(e) => Err(e),
                                None => Ok(r.result.unwrap_or(Value::Null)),
                            });
                        }
                    }
                    Ok(Message::Notification(n)) => match n.method.as_str() {
                        "sync.dirty" => {
                            let _ = ev.send(PluginEvent::Dirty { name: name.clone() });
                        }
                        "log" => tracing::info!(plugin = %name, "{}", n.params),
                        other => tracing::debug!(plugin = %name, "ignoring notification {other}"),
                    },
                    Ok(Message::Request(r)) => {
                        tracing::debug!(plugin = %name, "plugin→core request {} not supported yet", r.method);
                    }
                    Err(e) => tracing::warn!(plugin = %name, "bad line from plugin: {e}"),
                }
            }
        });

        let dir: PathBuf = state_dir().join("plugins").join(&self.name);
        let init = InitializeParams {
            api: PLUGIN_API_VERSION,
            config: self.cfg.config.clone(),
            state_dir: dir.to_string_lossy().into_owned(),
        };
        let info: InitializeResult = self.call_on(&conn, "initialize", init).await?;
        tracing::info!(plugin = %self.name, scheme = %info.scheme, cache = ?info.cache, "plugin ready");
        *self.info.write().unwrap() = Some(info.clone());
        *self.conn.lock().unwrap() = Some(conn);
        let _ = events.send(PluginEvent::Ready {
            name: self.name.clone(),
            info,
        });

        let status = child.wait().await?;
        reader.abort();
        writer.abort();
        // Fail anything still waiting.
        for (_, tx) in pending.lock().unwrap().drain() {
            let _ = tx.send(Err(RpcError::new(RpcError::INTERNAL, "plugin exited")));
        }
        Err(anyhow!("exited with {status}"))
    }
}

pub struct Plugins {
    hosts: Vec<Arc<PluginHost>>,
}

impl Plugins {
    pub fn start(cfgs: &[PluginConfig], events: mpsc::UnboundedSender<PluginEvent>) -> Arc<Self> {
        let hosts = cfgs
            .iter()
            .filter(|c| c.enabled)
            .map(|cfg| {
                let h = Arc::new(PluginHost {
                    name: cfg.name.clone(),
                    cfg: cfg.clone(),
                    conn: Mutex::new(None),
                    info: RwLock::new(None),
                    next_id: AtomicU64::new(1),
                });
                h.clone().supervise(events.clone());
                h
            })
            .collect();
        Arc::new(Self { hosts })
    }

    pub fn all(&self) -> &[Arc<PluginHost>] {
        &self.hosts
    }

    pub fn by_name(&self, name: &str) -> Option<Arc<PluginHost>> {
        self.hosts.iter().find(|h| h.name == name).cloned()
    }

    /// Route a URI to its owning plugin by scheme prefix.
    pub fn for_uri(&self, uri: &str) -> Option<Arc<PluginHost>> {
        let scheme = uri.split(':').next()?;
        self.hosts
            .iter()
            .find(|h| h.info().is_some_and(|i| i.scheme == scheme))
            .cloned()
    }

    pub async fn resolve(&self, uri: &str) -> Result<lark_protocol::Stream> {
        let host = self
            .for_uri(uri)
            .ok_or_else(|| anyhow!("no running source for {uri}"))?;
        host.call("resolve", lark_protocol::ResolveParams { uri: uri.into() })
            .await
    }

    pub async fn shutdown(&self) {
        for h in &self.hosts {
            let _ = tokio::time::timeout(
                Duration::from_millis(300),
                h.call::<_, Value>("shutdown", Value::Null),
            )
            .await;
        }
    }
}
