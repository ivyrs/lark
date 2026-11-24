//! The core actor: owns the queue and player state, talks to the engine,
//! and broadcasts typed events. Library reads bypass it (see server.rs).

use std::sync::{Arc, Mutex};

use anyhow::{anyhow, bail, Result};
use lark_protocol::{
    now_ms, Notification, PlayState, PlayerState, QueueInserted, QueueItem, QueueRemoved, Status,
    Stream, Track,
};
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::sync::{broadcast, mpsc, oneshot};

use crate::db::Library;
use crate::engine::{Cmd, EngineHandle, Event as EngineEvent, StampedEvent};
use crate::plugin::Plugins;

pub enum CoreMsg {
    Rpc {
        method: String,
        params: Value,
        reply: oneshot::Sender<Result<Value>>,
    },
    Engine(StampedEvent),
}

struct ActivePlay {
    row: i64,
    item: u64,
    duration_ms: Option<u64>,
}

pub struct Core {
    lib: Arc<Mutex<Library>>,
    plugins: Arc<Plugins>,
    engine: EngineHandle,
    events: broadcast::Sender<Notification>,
    queue: Vec<QueueItem>,
    next_id: u64,
    state: PlayState,
    current: Option<u64>,
    /// Last state we told clients about.
    player: PlayerState,
    next_sent: Option<u64>,
    active: Option<ActivePlay>,
    volume: f32,
}

#[derive(Deserialize)]
struct PlayParams {
    index: Option<usize>,
}
#[derive(Deserialize)]
struct SeekParams {
    position_ms: u64,
}
#[derive(Deserialize)]
struct AddParams {
    uris: Vec<String>,
    position: Option<usize>,
    #[serde(default)]
    play: bool,
}
#[derive(Deserialize)]
struct RemoveParams {
    ids: Vec<u64>,
}
#[derive(Deserialize)]
struct VolumeParams {
    volume: f32,
}

impl Core {
    pub fn new(
        lib: Arc<Mutex<Library>>,
        plugins: Arc<Plugins>,
        engine: EngineHandle,
        events: broadcast::Sender<Notification>,
    ) -> Self {
        Self {
            lib,
            plugins,
            engine,
            events,
            queue: Vec::new(),
            next_id: 1,
            state: PlayState::Stopped,
            current: None,
            player: PlayerState::default(),
            next_sent: None,
            active: None,
            volume: 1.0,
        }
    }

    pub async fn run(mut self, mut rx: mpsc::UnboundedReceiver<CoreMsg>) {
        while let Some(msg) = rx.recv().await {
            match msg {
                CoreMsg::Rpc {
                    method,
                    params,
                    reply,
                } => {
                    let r = self.rpc(&method, params).await;
                    let _ = reply.send(r);
                }
                CoreMsg::Engine(ev) => {
                    if let Some(ev) = ev.into_current() {
                        if let Err(e) = self.on_engine(ev).await {
                            tracing::warn!("engine event handling: {e:#}");
                        }
                    }
                }
            }
        }
    }

    fn emit(&self, method: &str, params: impl serde::Serialize) {
        let _ = self.events.send(Notification {
            method: method.into(),
            params: serde_json::to_value(params).unwrap(),
        });
    }

    fn idx(&self, id: u64) -> Option<usize> {
        self.queue.iter().position(|q| q.id == id)
    }

    fn snapshot(&self) -> PlayerState {
        let (position_ms, duration_ms) = match (self.current, self.engine.position()) {
            (Some(cur), Some((id, pos))) if id == cur => {
                let dur = self.idx(cur).and_then(|i| self.queue[i].track.duration_ms);
                (pos, dur.or(self.player.duration_ms))
            }
            (Some(_), _) => (self.player.position_ms, self.player.duration_ms),
            _ => (0, None),
        };
        PlayerState {
            state: self.state,
            current: self.current,
            position_ms,
            duration_ms,
            at_ms: now_ms(),
        }
    }

    fn publish(&mut self) {
        self.player = self.snapshot();
        self.emit("player.state", &self.player);
    }

    async fn resolve_stream(&self, uri: &str) -> Result<Stream> {
        self.plugins.resolve(uri).await
    }

    async fn rpc(&mut self, method: &str, params: Value) -> Result<Value> {
        let p = |v: Value| if v.is_null() { json!({}) } else { v };
        Ok(match method {
            "status" => serde_json::to_value(Status {
                player: self.snapshot(),
                queue: self.queue.clone(),
                volume: self.volume,
            })?,
            "player.play" => {
                let pp: PlayParams = serde_json::from_value(p(params))?;
                self.play(pp.index).await?;
                Value::Null
            }
            "player.pause" => {
                self.pause();
                Value::Null
            }
            "player.resume" => {
                self.resume().await?;
                Value::Null
            }
            "player.toggle" => {
                match self.state {
                    PlayState::Playing => self.pause(),
                    _ => self.resume().await?,
                }
                Value::Null
            }
            "player.stop" => {
                self.stop();
                Value::Null
            }
            "player.next" => {
                self.skip(1).await?;
                Value::Null
            }
            "player.prev" => {
                let pos = self.snapshot().position_ms;
                if pos > 3000 && self.current.is_some() {
                    self.seek(0).await?;
                } else {
                    self.skip(-1).await?;
                }
                Value::Null
            }
            "player.seek" => {
                let sp: SeekParams = serde_json::from_value(params)?;
                self.seek(sp.position_ms).await?;
                Value::Null
            }
            "player.volume" => {
                let vp: VolumeParams = serde_json::from_value(params)?;
                if !vp.volume.is_finite() {
                    bail!("volume must be finite");
                }
                self.volume = vp.volume.clamp(0.0, 1.5);
                self.engine.send(Cmd::Volume(self.volume));
                self.emit("player.volume", json!({ "volume": self.volume }));
                Value::Null
            }
            "queue.add" => {
                let ap: AddParams = serde_json::from_value(params)?;
                serde_json::to_value(self.add(ap).await?)?
            }
            "queue.remove" => {
                let rp: RemoveParams = serde_json::from_value(params)?;
                self.remove(rp.ids).await?;
                Value::Null
            }
            "queue.clear" => {
                self.current = None;
                self.stop();
                self.queue.clear();
                self.emit("queue.cleared", json!({}));
                Value::Null
            }
            other => bail!("unknown method {other}"),
        })
    }

    // ── transport ──

    async fn play(&mut self, index: Option<usize>) -> Result<()> {
        let id = match index {
            Some(i) => self
                .queue
                .get(i)
                .map(|q| q.id)
                .ok_or_else(|| anyhow!("no queue index {i}"))?,
            None => match (self.state, self.current) {
                (PlayState::Paused, Some(_)) => {
                    self.engine.send(Cmd::Resume);
                    self.state = PlayState::Playing;
                    self.publish();
                    return Ok(());
                }
                (_, Some(cur)) => cur,
                (_, None) => self
                    .queue
                    .first()
                    .map(|q| q.id)
                    .ok_or_else(|| anyhow!("queue is empty"))?,
            },
        };
        self.start(id, 0).await
    }

    async fn start(&mut self, id: u64, start_ms: u64) -> Result<()> {
        let i = self
            .idx(id)
            .ok_or_else(|| anyhow!("unknown queue item {id}"))?;
        let uri = self.queue[i].track.uri.clone();
        let stream = match self.resolve_stream(&uri).await {
            Ok(p) => p,
            Err(e) => {
                tracing::warn!("cannot resolve {uri}: {e:#}");
                return Err(e);
            }
        };
        if self.current != Some(id) {
            self.close_play(false);
        }
        self.engine.send(Cmd::Play {
            id,
            stream,
            start_ms,
        });
        self.next_sent = None;
        self.current = Some(id);
        self.state = PlayState::Playing;
        self.player.position_ms = start_ms;
        self.player.duration_ms = self.queue[i].track.duration_ms;
        self.refresh_next().await;
        // Started event will publish the authoritative state; publish an
        // optimistic one now so UIs react instantly.
        self.player = PlayerState {
            state: self.state,
            current: self.current,
            position_ms: start_ms,
            duration_ms: self.queue[i].track.duration_ms,
            at_ms: now_ms(),
        };
        self.emit("player.state", &self.player);
        Ok(())
    }

    fn pause(&mut self) {
        if self.state == PlayState::Playing {
            self.engine.send(Cmd::Pause);
            self.state = PlayState::Paused;
            self.publish();
        }
    }

    async fn resume(&mut self) -> Result<()> {
        match (self.state, self.current) {
            (PlayState::Paused, Some(_)) => {
                self.engine.send(Cmd::Resume);
                self.state = PlayState::Playing;
                self.publish();
                Ok(())
            }
            (PlayState::Playing, _) => Ok(()),
            _ => Box::pin(self.play(None)).await,
        }
    }

    fn stop(&mut self) {
        self.close_play(false);
        self.engine.send(Cmd::Stop);
        self.state = PlayState::Stopped;
        self.next_sent = None;
        self.player = PlayerState {
            current: self.current,
            ..Default::default()
        };
        self.publish();
    }

    async fn seek(&mut self, ms: u64) -> Result<()> {
        let Some(cur) = self.current else {
            return Ok(());
        };
        let was_paused = self.state == PlayState::Paused;
        self.start(cur, ms).await?;
        if was_paused {
            self.pause();
        }
        Ok(())
    }

    async fn skip(&mut self, delta: isize) -> Result<()> {
        let Some(cur) = self.current.and_then(|c| self.idx(c)) else {
            return self.play(None).await;
        };
        let target = cur as isize + delta;
        if target < 0 {
            return self.seek(0).await;
        }
        match self.queue.get(target as usize).map(|q| q.id) {
            Some(id) => self.start(id, 0).await,
            None => {
                self.stop();
                Ok(())
            }
        }
    }

    /// Tell the engine what follows the current item.
    async fn refresh_next(&mut self) {
        let want = self
            .current
            .and_then(|c| self.idx(c))
            .and_then(|i| self.queue.get(i + 1))
            .map(|q| q.id);
        if want == self.next_sent || self.state == PlayState::Stopped {
            return;
        }
        let cmd = match want {
            Some(id) => {
                let uri = self.queue[self.idx(id).unwrap()].track.uri.clone();
                match self.resolve_stream(&uri).await {
                    Ok(stream) => Some((id, stream)),
                    Err(e) => {
                        tracing::warn!("cannot pre-resolve {uri}: {e:#}");
                        None
                    }
                }
            }
            None => None,
        };
        self.engine.send(Cmd::SetNext(cmd));
        self.next_sent = want;
    }

    // ── queue ──

    async fn add(&mut self, ap: AddParams) -> Result<Vec<QueueItem>> {
        let items: Vec<QueueItem> = {
            let lib = self.lib.lock().unwrap();
            ap.uris
                .iter()
                .map(|uri| {
                    let track = lib.track(uri).ok().flatten().unwrap_or_else(|| Track {
                        uri: uri.clone(),
                        title: uri.rsplit(['/', ':']).next().unwrap_or(uri).to_string(),
                        ..Default::default()
                    });
                    let id = self.next_id;
                    self.next_id += 1;
                    QueueItem { id, track }
                })
                .collect()
        };
        let pos = ap
            .position
            .unwrap_or(self.queue.len())
            .min(self.queue.len());
        self.queue.splice(pos..pos, items.iter().cloned());
        self.emit(
            "queue.inserted",
            QueueInserted {
                position: pos,
                items: items.clone(),
            },
        );
        if ap.play {
            if let Some(first) = items.first() {
                self.start(first.id, 0).await?;
            }
        } else {
            self.refresh_next().await;
        }
        Ok(items)
    }

    async fn remove(&mut self, ids: Vec<u64>) -> Result<()> {
        let cur_idx = self.current.and_then(|c| self.idx(c));
        let removing_current = self.current.is_some_and(|c| ids.contains(&c));
        // Find the successor in the original order: removals before the current
        // item must not shift which surviving item we continue with.
        let successor = cur_idx.and_then(|i| {
            self.queue[i + 1..]
                .iter()
                .find(|q| !ids.contains(&q.id))
                .map(|q| q.id)
        });
        self.queue.retain(|q| !ids.contains(&q.id));
        self.emit("queue.removed", QueueRemoved { ids });
        if removing_current {
            let was_playing = self.state == PlayState::Playing;
            self.current = None;
            self.stop();
            if was_playing {
                // Continue with the first surviving item after the old current.
                if let Some(id) = successor {
                    self.start(id, 0).await?;
                }
            }
        } else {
            self.refresh_next().await;
        }
        Ok(())
    }

    // ── engine events / history ──

    fn close_play(&mut self, completed: bool) {
        if let Some(a) = self.active.take() {
            let played = match self.engine.position() {
                Some((id, pos)) if id == a.item => pos,
                _ if completed => a.duration_ms.unwrap_or(0),
                _ => self.player.position_ms,
            };
            let done = completed || a.duration_ms.is_some_and(|d| played * 2 >= d);
            if let Err(e) = self.lib.lock().unwrap().play_finish(a.row, played, done) {
                tracing::warn!("history: {e:#}");
            }
        }
    }

    async fn on_engine(&mut self, ev: EngineEvent) -> Result<()> {
        match ev {
            EngineEvent::Started {
                id,
                position_ms,
                duration_ms,
            } => {
                self.player.position_ms = position_ms;
                if self.active.as_ref().is_some_and(|a| a.item != id) {
                    // Gapless roll-over: the previous track finished.
                    self.close_play(true);
                }
                self.current = Some(id);
                if self.state == PlayState::Stopped {
                    self.state = PlayState::Playing;
                }
                if let Some(i) = self.idx(id) {
                    if duration_ms.is_some() && self.queue[i].track.duration_ms.is_none() {
                        self.queue[i].track.duration_ms = duration_ms;
                    }
                    if self.active.is_none() {
                        let row = self.lib.lock().unwrap().play_start(&self.queue[i].track)?;
                        self.active = Some(ActivePlay {
                            row,
                            item: id,
                            duration_ms: self.queue[i].track.duration_ms,
                        });
                    }
                }
                self.player.duration_ms = duration_ms;
                self.refresh_next().await;
                self.publish();
            }
            EngineEvent::Finished => {
                self.close_play(true);
                self.state = PlayState::Stopped;
                self.current = None;
                self.next_sent = None;
                self.player = PlayerState::default();
                self.publish();
            }
            EngineEvent::Error { id, message } => {
                tracing::warn!(item = id, "playback error: {message}");
                let next = self
                    .idx(id)
                    .and_then(|i| self.queue.get(i + 1))
                    .map(|q| q.id);
                match next {
                    Some(n) => self.start(n, 0).await?,
                    None => self.stop(),
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::OutputKind;

    fn fixture(state: PlayState, current: u64) -> Core {
        let lib = Arc::new(Mutex::new(Library::memory().unwrap()));
        let (plugin_tx, _) = mpsc::unbounded_channel();
        let plugins = Plugins::start(&[], plugin_tx);
        let (engine_tx, _) = mpsc::unbounded_channel();
        let engine = crate::engine::spawn(OutputKind::Null, engine_tx);
        let (events, _) = broadcast::channel(16);
        let mut core = Core::new(lib, plugins, engine, events);
        core.queue = (1..=6)
            .map(|id| QueueItem {
                id,
                track: Track {
                    uri: format!("test:{id}"),
                    title: format!("Track {id}"),
                    ..Default::default()
                },
            })
            .collect();
        core.next_id = 7;
        core.current = Some(current);
        core.state = state;
        core
    }

    #[tokio::test]
    async fn clear_drops_current_and_play_selects_new_first_item() {
        for state in [PlayState::Playing, PlayState::Paused, PlayState::Stopped] {
            let mut core = fixture(state, 3);
            let mut events = core.events.subscribe();
            core.rpc("queue.clear", Value::Null).await.unwrap();
            assert!(core.queue.is_empty());
            assert_eq!(core.current, None);
            assert_eq!(core.snapshot().current, None);
            assert_eq!(core.state, PlayState::Stopped);
            let event = events.try_recv().unwrap();
            assert_eq!(event.method, "player.state");
            let player: PlayerState = serde_json::from_value(event.params).unwrap();
            assert_eq!(player.current, None);
            core.rpc("queue.add", json!({"uris": ["test:new"]}))
                .await
                .unwrap();
            // The fixture has no source. Reaching its resolver for the new URI
            // proves default play selected it rather than the deleted item id.
            let error = core.rpc("player.play", Value::Null).await.unwrap_err();
            assert_eq!(error.to_string(), "no running source for test:new");
        }
    }

    #[tokio::test]
    async fn batch_remove_selects_first_surviving_successor_in_original_order() {
        for (removed, successor) in [(vec![1, 3], 4), (vec![1, 3, 4], 5)] {
            let mut core = fixture(PlayState::Playing, 3);
            let error = core.remove(removed.clone()).await.unwrap_err();
            // No source is installed: the resolver error identifies exactly
            // which successor playback tried to start, without audio or a plugin.
            assert_eq!(
                error.to_string(),
                format!("no running source for test:{successor}")
            );
            assert!(core.queue.iter().all(|item| !removed.contains(&item.id)));
            assert_eq!(core.current, None);
        }
    }

    #[tokio::test]
    async fn remove_current_without_successor_or_while_paused_stops() {
        for (state, removed) in [
            (PlayState::Playing, vec![1, 3, 4, 5, 6]),
            (PlayState::Paused, vec![1, 3]),
            (PlayState::Stopped, vec![1, 3]),
        ] {
            let mut core = fixture(state, 3);
            core.remove(removed).await.unwrap();
            assert_eq!(core.current, None);
            assert_eq!(core.state, PlayState::Stopped);
        }
    }

    #[tokio::test]
    async fn volume_is_shared_in_status_and_emits_a_typed_change() {
        let mut core = fixture(PlayState::Stopped, 3);
        let mut events = core.events.subscribe();
        core.rpc("player.volume", json!({"volume": 0.15}))
            .await
            .unwrap();
        let status: Status =
            serde_json::from_value(core.rpc("status", Value::Null).await.unwrap()).unwrap();
        assert!((status.volume - 0.15).abs() < f32::EPSILON);
        let event = events.try_recv().unwrap();
        assert_eq!(event.method, "player.volume");
        assert!((event.params["volume"].as_f64().unwrap() - 0.15).abs() < 0.00001);
        core.rpc("player.volume", json!({"volume": 2.0}))
            .await
            .unwrap();
        assert_eq!(core.volume, 1.5);
        core.rpc("player.volume", json!({"volume": -1.0}))
            .await
            .unwrap();
        assert_eq!(core.volume, 0.0);
        assert!(core
            .rpc("player.volume", json!({"volume": null}))
            .await
            .is_err());
    }

    #[tokio::test]
    async fn stop_still_retains_current_for_replay() {
        let mut core = fixture(PlayState::Playing, 3);
        core.rpc("player.stop", Value::Null).await.unwrap();
        assert_eq!(core.current, Some(3));
        assert_eq!(core.state, PlayState::Stopped);
    }
}
