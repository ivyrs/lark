//! Out-of-process plugin host: spawns each plugin, speaks JSON-RPC over its
//! stdio, and restarts it with backoff if it dies. A crashed plugin never
//! takes playback down with it.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use lark_protocol::{
    InitializeParams, InitializeResult, Message, PluginRole, Request, Response, RpcError,
    PLUGIN_API_VERSION,
};
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, BufReader, Lines};
use tokio::process::{ChildStdout, Command};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;

use crate::config::{resolve_exec, state_dir, PluginConfig};
use crate::server::{self, send_line, RpcSession, WeakCtx, OUTPUT_CAPACITY};

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

type Pending = Mutex<HashMap<u64, oneshot::Sender<Result<Value, RpcError>>>>;
type FrontendBridge = watch::Receiver<Option<WeakCtx>>;

struct Conn {
    tx: mpsc::Sender<String>,
    pending: Pending,
}

// A canceled/expired outbound call must not retain a pending entry for the
// lifetime of the plugin. The response path can remove it first.
struct PendingCall<'a> {
    pending: &'a Pending,
    id: u64,
}

impl Drop for PendingCall<'_> {
    fn drop(&mut self) {
        self.pending.lock().unwrap().remove(&self.id);
    }
}

pub struct PluginHost {
    pub name: String,
    cfg: PluginConfig,
    conn: Mutex<Option<Arc<Conn>>>,
    info: RwLock<Option<InitializeResult>>,
    next_id: AtomicU64,
}

const CALL_TIMEOUT: Duration = Duration::from_secs(30);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_millis(300);
const PENDING_LIMIT: usize = 64;

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
        let req = self.request(method, params)?;
        let (tx, rx) = oneshot::channel();
        {
            let mut pending = conn.pending.lock().unwrap();
            if pending.len() >= PENDING_LIMIT {
                bail!("too many pending calls to plugin {}", self.name);
            }
            pending.insert(req.id, tx);
        }
        let _remove = PendingCall {
            pending: &conn.pending,
            id: req.id,
        };
        send_line(&conn.tx, &req)?;
        let res = tokio::time::timeout(CALL_TIMEOUT, rx)
            .await
            .map_err(|_| anyhow!("plugin {} timed out on {method}", self.name))?
            .map_err(|_| anyhow!("plugin {} exited during {method}", self.name))?
            .map_err(|e| anyhow!("plugin {}: {e}", self.name))?;
        serde_json::from_value(res).with_context(|| format!("decoding {method} result"))
    }

    fn request(&self, method: &str, params: impl Serialize) -> Result<Request> {
        Ok(Request {
            id: self.next_id.fetch_add(1, Ordering::Relaxed),
            method: method.into(),
            params: serde_json::to_value(params)?,
        })
    }

    /// The supervisor owns each child, but only receives (doesn't own) the
    /// shutdown signal. Dropping Plugins also stops all of its supervisors.
    fn supervise(
        self: Arc<Self>,
        events: mpsc::UnboundedSender<PluginEvent>,
        bridge: FrontendBridge,
        mut stop: watch::Receiver<bool>,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut backoff = Duration::from_millis(500);
            while !stop.has_changed().unwrap_or(true) {
                let started = Instant::now();
                let result = self.run_once(&events, bridge.clone(), stop.clone()).await;
                if stop.has_changed().unwrap_or(true) {
                    break;
                }
                if let Err(e) = result {
                    tracing::warn!(plugin = %self.name, "plugin stopped: {e:#}");
                }
                let _ = events.send(PluginEvent::Down {
                    name: self.name.clone(),
                });
                if started.elapsed() > Duration::from_secs(60) {
                    backoff = Duration::from_millis(500);
                }
                tracing::info!(plugin = %self.name, "restarting in {:?}", backoff);
                tokio::select! {
                    _ = stop.changed() => break,
                    _ = tokio::time::sleep(backoff) => (),
                }
                backoff = (backoff * 2).min(Duration::from_secs(30));
            }
        })
    }

    async fn run_once(
        &self,
        events: &mpsc::UnboundedSender<PluginEvent>,
        bridge: FrontendBridge,
        mut stop: watch::Receiver<bool>,
    ) -> Result<()> {
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
        let stdin = child.stdin.take().unwrap();
        let stdout = child.stdout.take().unwrap();
        let (tx, rx) = mpsc::channel(OUTPUT_CAPACITY);
        let conn = Arc::new(Conn {
            tx,
            pending: Pending::default(),
        });

        // Keep both I/O futures here, not detached tasks. Any failure (even
        // during initialize) or shutdown closes both pipes and subscriptions.
        let result = tokio::select! {
            result = self.read_plugin(stdout, &conn, events, bridge) => result,
            result = server::write_lines(stdin, rx) => result.context("writing plugin stdin"),
            status = child.wait() => match status {
                Ok(status) => Err(anyhow!("exited with {status}")),
                Err(e) => Err(e).context("waiting for plugin child"),
            },
            _ = async {
                if stop.changed().await.is_ok() {
                    let _ = tokio::time::timeout(
                        SHUTDOWN_TIMEOUT, self.call::<_, Value>("shutdown", Value::Null),
                    ).await;
                }
            } => Ok(()),
        };
        *self.conn.lock().unwrap() = None;
        // Wake outstanding callers now, before reaping the child. Dropping the
        // writer above means a racing new call fails to enqueue.
        conn.pending.lock().unwrap().clear();
        let _ = child.start_kill();
        let _ = child.wait().await;
        result
    }

    async fn initialize(
        &self,
        conn: &Conn,
        lines: &mut Lines<BufReader<ChildStdout>>,
    ) -> Result<InitializeResult> {
        let dir = state_dir().join("plugins").join(&self.name);
        let req = self.request(
            "initialize",
            InitializeParams {
                api: PLUGIN_API_VERSION,
                config: self.cfg.config.clone(),
                state_dir: dir.to_string_lossy().into_owned(),
            },
        )?;
        send_line(&conn.tx, &req)?;
        while let Some(line) = lines.next_line().await? {
            match serde_json::from_str::<Message>(&line) {
                Ok(Message::Response(r)) if r.id == req.id => {
                    return serde_json::from_value(response_result(r)?)
                        .context("decoding initialize result");
                }
                Ok(Message::Request(r)) => send_line(
                    &conn.tx,
                    &Response::err(
                        r.id,
                        RpcError::new(
                            RpcError::METHOD_NOT_FOUND,
                            "client API unavailable before initialize reply",
                        ),
                    ),
                )?,
                Ok(Message::Notification(n)) if n.method == "log" => {
                    tracing::info!(plugin = %self.name, "{}", n.params)
                }
                _ => (),
            }
        }
        bail!("stdout closed before initialize reply")
    }

    async fn read_plugin(
        &self,
        stdout: ChildStdout,
        conn: &Arc<Conn>,
        events: &mpsc::UnboundedSender<PluginEvent>,
        mut bridge: FrontendBridge,
    ) -> Result<()> {
        let mut lines = BufReader::new(stdout).lines();
        // The same reader consumes initialize and subsequent input. The child
        // can send its first frontend request immediately after the reply.
        let info = tokio::time::timeout(CALL_TIMEOUT, self.initialize(conn, &mut lines))
            .await
            .context("initialize timed out")??;
        let role = info.role;
        tracing::info!(plugin = %self.name, ?role, scheme = %info.scheme, cache = ?info.cache, "plugin ready");
        *self.info.write().unwrap() = Some(info.clone());
        *self.conn.lock().unwrap() = Some(conn.clone());
        let _ = events.send(PluginEvent::Ready {
            name: self.name.clone(),
            info,
        });

        let mut frontend = if role == PluginRole::Frontend {
            let ctx = tokio::time::timeout(CALL_TIMEOUT, bridge.wait_for(Option::is_some))
                .await
                .context("waiting for frontend bridge timed out")??
                .clone()
                .unwrap();
            Some((ctx, RpcSession::new(conn.tx.clone())))
        } else {
            None
        };
        loop {
            tokio::select! {
                line = lines.next_line() => {
                    let line = line?.context("plugin stdout closed")?;
                    if line.trim().is_empty() {
                        continue;
                    }
                    match serde_json::from_str::<Message>(&line) {
                        Ok(Message::Response(r)) => {
                            if let Some(tx) = conn.pending.lock().unwrap().remove(&r.id) {
                                let _ = tx.send(response_result(r));
                            }
                        }
                        Ok(Message::Notification(n)) => match n.method.as_str() {
                            "sync.dirty" if role == PluginRole::Source => {
                                let _ = events.send(PluginEvent::Dirty { name: self.name.clone() });
                            }
                            "log" => tracing::info!(plugin = %self.name, "{}", n.params),
                            other => tracing::debug!(plugin = %self.name, "ignoring notification {other}"),
                        },
                        Ok(Message::Request(r)) => {
                            if let Some((ctx, session)) = &mut frontend {
                                session.request(&ctx.upgrade().context("daemon RPC is unavailable")?, r)?;
                            } else {
                                send_line(&conn.tx, &Response::err(r.id, RpcError::new(
                                    RpcError::METHOD_NOT_FOUND, "client API is only available to frontend plugins",
                                )))?;
                            }
                        }
                        Err(e) => tracing::warn!(plugin = %self.name, "bad line from plugin: {e}"),
                    }
                }
                progress = advance_frontend(&mut frontend), if frontend.is_some() => progress?,
            }
        }
    }
}

fn response_result(r: Response) -> Result<Value, RpcError> {
    match r.error {
        Some(e) => Err(e),
        None => Ok(r.result.unwrap_or(Value::Null)),
    }
}

async fn advance_frontend(frontend: &mut Option<(WeakCtx, RpcSession)>) -> Result<()> {
    frontend.as_mut().unwrap().1.background().await
}

pub struct Plugins {
    hosts: Vec<Arc<PluginHost>>,
    bridge: watch::Sender<Option<WeakCtx>>,
    stop: watch::Sender<bool>,
    supervisors: Mutex<Vec<JoinHandle<()>>>,
}

impl Plugins {
    pub fn start(cfgs: &[PluginConfig], events: mpsc::UnboundedSender<PluginEvent>) -> Arc<Self> {
        let (bridge, bridge_rx) = watch::channel(None);
        let (stop, stop_rx) = watch::channel(false);
        let mut supervisors = Vec::new();
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
                supervisors.push(h.clone().supervise(
                    events.clone(),
                    bridge_rx.clone(),
                    stop_rx.clone(),
                ));
                h
            })
            .collect();
        Arc::new(Self {
            hosts,
            bridge,
            stop,
            supervisors: Mutex::new(supervisors),
        })
    }

    pub fn attach_frontend(&self, ctx: server::Ctx) {
        self.bridge.send_replace(Some(ctx.downgrade()));
    }

    pub fn all(&self) -> &[Arc<PluginHost>] {
        &self.hosts
    }

    pub fn by_name(&self, name: &str) -> Option<Arc<PluginHost>> {
        self.hosts.iter().find(|h| h.name == name).cloned()
    }

    /// Route a URI to its owning source plugin by scheme prefix.
    pub fn for_uri(&self, uri: &str) -> Option<Arc<PluginHost>> {
        let (scheme, _) = uri.split_once(':')?;
        if scheme.is_empty() {
            return None;
        }
        self.hosts
            .iter()
            .find(|h| {
                h.info()
                    .is_some_and(|i| i.role == PluginRole::Source && i.scheme == scheme)
            })
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
        self.stop.send_replace(true);
        let supervisors = std::mem::take(&mut *self.supervisors.lock().unwrap());
        for task in supervisors {
            let _ = task.await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tokio::sync::broadcast;

    use crate::core::CoreMsg;
    use crate::db::Library;

    const TEST_TIMEOUT: Duration = Duration::from_secs(5);

    fn child(script: &str) -> PluginConfig {
        PluginConfig {
            name: "fixture".into(),
            exec: vec!["sh".into(), "-ec".into(), script.into()],
            config: Value::Null,
            enabled: true,
        }
    }

    #[tokio::test]
    async fn frontend_uses_client_api_on_its_supervised_connection() {
        tokio::time::timeout(TEST_TIMEOUT, async {
            let (plugin_events, mut plugins_rx) = mpsc::unbounded_channel();
            let plugins = Plugins::start(&[child(r#"
                IFS= read -r init
                # A claimed scheme/cache must not make a frontend a source.
                printf '%s\n' '{"id":1,"result":{"name":"fixture","role":"frontend","scheme":"fixture","cache":"full","version":"0.1.0"}}' \
                    '{"id":1,"method":"subscribe"}' \
                    '{"id":2,"method":"queue.add","params":{"uris":["fixture:track:1"]}}' \
                    '{"id":3,"method":"player.play"}' \
                    '{"id":4,"method":"sources"}'
                IFS= read -r ack
                [ "$ack" = '{"id":1,"result":null}' ]
                for i in 1 2 3 4; do
                    IFS= read -r line
                    case "$line" in
                        '{"id":2,"result":"queued"}') added=yes ;;
                        '{"id":3,"result":"playing"}') played=yes ;;
                        '{"id":4,"result":[]}') not_source=yes ;;
                        '{"method":"player.volume","params":{"volume":0.5}}') event=yes ;;
                        *) exit 1 ;;
                    esac
                done
                [ "$added$played$not_source$event" = yesyesyesyes ]
                printf '%s\n' '{"id":5,"method":"status"}'
                IFS= read -r result
                [ "$result" = '{"id":5,"result":"status"}' ]
                printf '%s\n' '{"id":6,"method":"library.sync"}'
                while IFS= read -r frame; do
                    [ "$frame" = '{"id":2,"method":"shutdown"}' ] && break
                done
                printf '%s\n' '{"id":7,"method":"queue.clear"}' '{"id":2,"result":null}'
                # Leave the child alive until the supervisor closes its pipes.
                while IFS= read -r line; do :; done
            "#)], plugin_events);
            let (core, mut commands) = mpsc::unbounded_channel();
            let (events, _) = broadcast::channel(8);
            let (sync, mut sync_rx) = mpsc::unbounded_channel();
            let ctx = server::Ctx {
                lib: Arc::new(Mutex::new(Library::memory().unwrap())),
                plugins: plugins.clone(),
                core,
                events,
                sync,
            };

            // The child has already sent requests before the bridge is attached.
            let Some(PluginEvent::Ready { info, .. }) = plugins_rx.recv().await else {
                panic!("frontend failed to initialize");
            };
            assert_eq!(info.role, PluginRole::Frontend);
            assert!(plugins.for_uri("fixture:track:1").is_none());
            plugins.attach_frontend(ctx.clone());

            let reply = |command: CoreMsg, expected, result: Value| {
                let CoreMsg::Rpc { method, reply, .. } = command else {
                    panic!("unexpected core message");
                };
                assert_eq!(method, expected);
                reply.send(Ok(result)).unwrap();
            };
            let added = commands.recv().await.unwrap();
            reply(commands.recv().await.unwrap(), "player.play", json!("playing"));
            reply(added, "queue.add", json!("queued"));
            ctx.events.send(lark_protocol::Notification {
                method: "player.volume".into(),
                params: json!({ "volume": 0.5 }),
            }).unwrap();
            reply(commands.recv().await.unwrap(), "status", json!("status"));
            assert_eq!(sync_rx.recv().await, Some(None));
            let ((), command) = tokio::join!(plugins.shutdown(), commands.recv());
            let CoreMsg::Rpc { method, .. } = command.expect("frontend saw graceful shutdown") else {
                panic!("unexpected core message");
            };
            assert_eq!(method, "queue.clear");
            assert!(!plugins.all()[0].running());
            assert_eq!(ctx.events.receiver_count(), 0);
        }).await.expect("frontend fixture timed out");
    }

    #[tokio::test]
    async fn legacy_source_gets_resolve_but_cannot_call_client_api() {
        tokio::time::timeout(TEST_TIMEOUT, async {
            let (events, mut plugin_events) = mpsc::unbounded_channel();
            let plugins = Plugins::start(&[child(r#"
                IFS= read -r init
                printf '%s\n' '{"id":1,"result":{"name":"fixture","scheme":"fixture","cache":"full","version":"0.1.0"}}' \
                    '{"id":1,"method":"status"}'
                denied=0
                resolved=0
                for i in 1 2; do
                    IFS= read -r line
                    case "$line" in
                        '{"id":1,"error":{"code":-32601,'*) denied=1 ;;
                        '{"id":2,"method":"resolve","params":{"uri":"fixture:track:1"}}') resolved=1 ;;
                        *) exit 1 ;;
                    esac
                done
                [ "$denied$resolved" = 11 ]
                printf '%s\n' '{"id":2,"result":{"kind":"file","path":"/fixture.wav"}}'
                IFS= read -r shutdown
                printf '%s\n' '{"id":3,"result":null}'
            "#)], events);
            let Some(PluginEvent::Ready { info, .. }) = plugin_events.recv().await else {
                panic!("source failed to initialize");
            };
            assert_eq!(info.role, PluginRole::Source);
            assert!(matches!(plugins.resolve("fixture:track:1").await.unwrap(),
                lark_protocol::Stream::File { path } if path == "/fixture.wav"
            ));
            plugins.shutdown().await;
        }).await.expect("source fixture timed out");
    }
}
