//! TUI client. Pure socket client: everything it knows comes from `status`
//! once, then typed events. Position is extrapolated locally.

use std::collections::HashMap;
use std::time::Duration;

use anyhow::Result;
use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use lark_protocol::{
    now_ms, ArtistRef, Message, PlayState, PlayerState, QueueInserted, QueueItem, QueueRemoved,
    ReleaseRef, Status, Track,
};
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::{Color, Modifier, Style, Stylize};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, LineGauge, List, ListItem, ListState, Padding, Paragraph};
use ratatui::{DefaultTerminal, Frame};
use serde_json::{json, Value};

use crate::client::Live;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Tab {
    Queue,
    Library,
    Search,
}

#[derive(Clone, Copy)]
enum AddMode {
    /// Insert after the current track and start playing it.
    PlayNext,
    /// Append to the end.
    Append,
    /// Replace the queue and play.
    Replace,
}

enum Pending {
    Status,
    Artists,
    Releases(i64),
    Tracks(i64),
    Search(u64),
    Add(AddMode),
    Ignore,
}

struct App {
    conn: Live,
    pending: HashMap<u64, Pending>,
    tab: Tab,
    player: PlayerState,
    queue: Vec<QueueItem>,
    queue_st: ListState,

    artists: Vec<ArtistRef>,
    releases: Vec<ReleaseRef>,
    tracks: Vec<Track>,
    lib_col: usize,
    lib_st: [ListState; 3],

    query: String,
    editing: bool,
    search_seq: u64,
    results: Vec<Track>,
    results_st: ListState,

    volume: f32,
    flash: Option<(String, u64)>,
    quit: bool,
}

pub fn run() -> Result<()> {
    let conn = Live::connect()?;
    let mut app = App {
        conn,
        pending: HashMap::new(),
        tab: Tab::Queue,
        player: PlayerState::default(),
        queue: vec![],
        queue_st: ListState::default(),
        artists: vec![],
        releases: vec![],
        tracks: vec![],
        lib_col: 0,
        lib_st: Default::default(),
        query: String::new(),
        editing: false,
        search_seq: 0,
        results: vec![],
        results_st: ListState::default(),
        volume: 1.0,
        flash: None,
        quit: false,
    };
    app.req("subscribe", Value::Null, Pending::Ignore);
    app.req("status", Value::Null, Pending::Status);
    app.req("library.artists", Value::Null, Pending::Artists);

    let mut term = ratatui::init();
    let res = app.main_loop(&mut term);
    ratatui::restore();
    res
}

fn fmt_ms(ms: u64) -> String {
    let s = ms / 1000;
    format!("{}:{:02}", s / 60, s % 60)
}

fn move_sel(st: &mut ListState, len: usize, delta: isize) {
    if len == 0 {
        st.select(None);
        return;
    }
    let cur = st.selected().unwrap_or(0) as isize;
    st.select(Some((cur + delta).clamp(0, len as isize - 1) as usize));
}

impl App {
    fn req(&mut self, method: &str, params: Value, p: Pending) {
        let id = self.conn.send(method, params);
        self.pending.insert(id, p);
    }

    fn flash(&mut self, msg: impl Into<String>) {
        self.flash = Some((msg.into(), now_ms() + 4000));
    }

    fn main_loop(&mut self, term: &mut DefaultTerminal) -> Result<()> {
        while !self.quit {
            term.draw(|f| self.draw(f))?;
            if event::poll(Duration::from_millis(100))? {
                if let Event::Key(k) = event::read()? {
                    if k.kind == KeyEventKind::Press {
                        self.key(k);
                    }
                }
            }
            loop {
                match self.conn.rx.try_recv() {
                    Ok(m) => self.message(m),
                    Err(std::sync::mpsc::TryRecvError::Empty) => break,
                    Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                        return Err(anyhow::anyhow!("lost connection to larkd"));
                    }
                }
            }
        }
        Ok(())
    }

    // ───────────────────────── daemon → us ─────────────────────────

    fn message(&mut self, m: Message) {
        match m {
            Message::Response(r) => {
                let Some(p) = self.pending.remove(&r.id) else {
                    return;
                };
                if let Some(e) = r.error {
                    self.flash(format!("error: {}", e.message));
                    return;
                }
                let v = r.result.unwrap_or(Value::Null);
                self.response(p, v);
            }
            Message::Notification(n) => self.event(&n.method, n.params),
            Message::Request(_) => {}
        }
    }

    fn response(&mut self, p: Pending, v: Value) {
        match p {
            Pending::Status => {
                if let Ok(s) = serde_json::from_value::<Status>(v) {
                    self.player = s.player;
                    self.queue = s.queue;
                    self.volume = s.volume;
                    self.clamp_queue();
                }
            }
            Pending::Artists => {
                self.artists = serde_json::from_value(v).unwrap_or_default();
                if self.lib_st[0].selected().is_none() && !self.artists.is_empty() {
                    self.lib_st[0].select(Some(0));
                }
                move_sel(&mut self.lib_st[0], self.artists.len(), 0);
                self.load_releases();
            }
            Pending::Releases(aid) => {
                if self.sel_artist().map(|a| a.id) == Some(aid) {
                    self.releases = serde_json::from_value(v).unwrap_or_default();
                    self.lib_st[1].select(if self.releases.is_empty() {
                        None
                    } else {
                        Some(0)
                    });
                    self.load_tracks();
                }
            }
            Pending::Tracks(rid) => {
                if self.sel_release().map(|r| r.id) == Some(rid) {
                    self.tracks = serde_json::from_value(v).unwrap_or_default();
                    self.lib_st[2].select(if self.tracks.is_empty() {
                        None
                    } else {
                        Some(0)
                    });
                }
            }
            Pending::Search(seq) => {
                if seq == self.search_seq {
                    self.results = serde_json::from_value(v).unwrap_or_default();
                    self.results_st.select(if self.results.is_empty() {
                        None
                    } else {
                        Some(0)
                    });
                }
            }
            Pending::Add(mode) => {
                let tracks: Vec<Track> = serde_json::from_value(v).unwrap_or_default();
                self.enqueue(tracks.into_iter().map(|t| t.uri).collect(), mode);
            }
            Pending::Ignore => {}
        }
    }

    fn event(&mut self, method: &str, params: Value) {
        match method {
            "player.state" => {
                if let Ok(p) = serde_json::from_value(params) {
                    self.player = p;
                }
            }
            "player.volume" => {
                if let Some(volume) = params.get("volume").and_then(Value::as_f64) {
                    self.volume = volume as f32;
                }
            }
            "queue.inserted" => {
                if let Ok(q) = serde_json::from_value::<QueueInserted>(params) {
                    let pos = q.position.min(self.queue.len());
                    self.queue.splice(pos..pos, q.items);
                    self.clamp_queue();
                }
            }
            "queue.removed" => {
                if let Ok(q) = serde_json::from_value::<QueueRemoved>(params) {
                    self.queue.retain(|i| !q.ids.contains(&i.id));
                    self.clamp_queue();
                }
            }
            "queue.cleared" => {
                self.queue.clear();
                self.clamp_queue();
            }
            "library.updated" => self.req("library.artists", Value::Null, Pending::Artists),
            "resync" => self.req("status", Value::Null, Pending::Status),
            _ => {}
        }
    }

    fn clamp_queue(&mut self) {
        if self.queue_st.selected().is_none() && !self.queue.is_empty() {
            self.queue_st.select(Some(0));
        }
        move_sel(&mut self.queue_st, self.queue.len(), 0);
    }

    // ───────────────────────── library helpers ─────────────────────────

    fn sel_artist(&self) -> Option<&ArtistRef> {
        self.lib_st[0].selected().and_then(|i| self.artists.get(i))
    }
    fn sel_release(&self) -> Option<&ReleaseRef> {
        self.lib_st[1].selected().and_then(|i| self.releases.get(i))
    }

    fn load_releases(&mut self) {
        self.releases.clear();
        self.tracks.clear();
        if let Some(a) = self.sel_artist().map(|a| a.id) {
            self.req(
                "library.releases",
                json!({ "artist_id": a }),
                Pending::Releases(a),
            );
        }
    }

    fn load_tracks(&mut self) {
        self.tracks.clear();
        if let Some(r) = self.sel_release().map(|r| r.id) {
            self.req(
                "library.tracks",
                json!({ "release_id": r }),
                Pending::Tracks(r),
            );
        }
    }

    fn current_index(&self) -> Option<usize> {
        self.player
            .current
            .and_then(|c| self.queue.iter().position(|q| q.id == c))
    }

    fn enqueue(&mut self, uris: Vec<String>, mode: AddMode) {
        if uris.is_empty() {
            return;
        }
        let n = uris.len();
        match mode {
            AddMode::Append => {
                self.req("queue.add", json!({ "uris": uris }), Pending::Ignore);
                self.flash(format!("appended {n}"));
            }
            AddMode::PlayNext => {
                let pos = self
                    .current_index()
                    .map(|i| i + 1)
                    .unwrap_or(self.queue.len());
                self.req(
                    "queue.add",
                    json!({ "uris": uris, "position": pos, "play": true }),
                    Pending::Ignore,
                );
            }
            AddMode::Replace => {
                self.req("queue.clear", Value::Null, Pending::Ignore);
                self.req(
                    "queue.add",
                    json!({ "uris": uris, "play": true }),
                    Pending::Ignore,
                );
            }
        }
    }

    fn add_from_library(&mut self, mode: AddMode) {
        match self.lib_col {
            0 => {
                if let Some(a) = self.sel_artist().map(|a| a.id) {
                    self.req(
                        "library.tracks",
                        json!({ "artist_id": a }),
                        Pending::Add(mode),
                    );
                }
            }
            1 => {
                if let Some(r) = self.sel_release().map(|r| r.id) {
                    self.req(
                        "library.tracks",
                        json!({ "release_id": r }),
                        Pending::Add(mode),
                    );
                }
            }
            _ => {
                // From the track column, PlayNext/Replace take the rest of the
                // release from the cursor, like playing an album from a track.
                let i = self.lib_st[2].selected().unwrap_or(0);
                let uris: Vec<String> = match mode {
                    AddMode::Append => self
                        .tracks
                        .get(i)
                        .map(|t| vec![t.uri.clone()])
                        .unwrap_or_default(),
                    _ => self.tracks.iter().skip(i).map(|t| t.uri.clone()).collect(),
                };
                self.enqueue(uris, mode);
            }
        }
    }

    fn search(&mut self) {
        self.search_seq += 1;
        let seq = self.search_seq;
        if self.query.trim().is_empty() {
            self.results.clear();
            return;
        }
        self.req(
            "library.search",
            json!({ "query": self.query, "limit": 200 }),
            Pending::Search(seq),
        );
    }

    // ───────────────────────── keys ─────────────────────────

    fn key(&mut self, k: KeyEvent) {
        if k.modifiers.contains(KeyModifiers::CONTROL) && k.code == KeyCode::Char('c') {
            self.quit = true;
            return;
        }
        if self.editing {
            match k.code {
                KeyCode::Esc | KeyCode::Enter | KeyCode::Down => self.editing = false,
                KeyCode::Backspace => {
                    self.query.pop();
                    self.search();
                }
                KeyCode::Char(c) => {
                    self.query.push(c);
                    self.search();
                }
                _ => {}
            }
            return;
        }

        // Global.
        match k.code {
            KeyCode::Char('q') => self.quit = true,
            KeyCode::Char(' ') => self.req("player.toggle", Value::Null, Pending::Ignore),
            KeyCode::Char('n') => self.req("player.next", Value::Null, Pending::Ignore),
            KeyCode::Char('p') => self.req("player.prev", Value::Null, Pending::Ignore),
            KeyCode::Char('s') => self.req("player.stop", Value::Null, Pending::Ignore),
            KeyCode::Char('[') | KeyCode::Char(']') => {
                let delta: i64 = if k.code == KeyCode::Char('[') {
                    -10_000
                } else {
                    10_000
                };
                let pos = (self.player.position_now(now_ms()) as i64 + delta).max(0);
                self.req(
                    "player.seek",
                    json!({ "position_ms": pos }),
                    Pending::Ignore,
                );
            }
            KeyCode::Char('+') | KeyCode::Char('=') | KeyCode::Char('-') => {
                let d = if k.code == KeyCode::Char('-') {
                    -0.05
                } else {
                    0.05
                };
                self.volume = (self.volume + d).clamp(0.0, 1.5);
                self.req(
                    "player.volume",
                    json!({ "volume": self.volume }),
                    Pending::Ignore,
                );
                self.flash(format!("volume {:.0}%", self.volume * 100.0));
            }
            KeyCode::Char('1') => self.tab = Tab::Queue,
            KeyCode::Char('2') => self.tab = Tab::Library,
            KeyCode::Char('3') => self.tab = Tab::Search,
            KeyCode::Tab => {
                self.tab = match self.tab {
                    Tab::Queue => Tab::Library,
                    Tab::Library => Tab::Search,
                    Tab::Search => Tab::Queue,
                }
            }
            KeyCode::Char('/') => {
                self.tab = Tab::Search;
                self.editing = true;
            }
            KeyCode::Char('u') => {
                self.req("library.sync", Value::Null, Pending::Ignore);
                self.flash("library sync requested");
            }
            _ => match self.tab {
                Tab::Queue => self.key_queue(k),
                Tab::Library => self.key_library(k),
                Tab::Search => self.key_search(k),
            },
        }
    }

    fn nav(k: &KeyEvent) -> Option<isize> {
        match k.code {
            KeyCode::Char('j') | KeyCode::Down => Some(1),
            KeyCode::Char('k') | KeyCode::Up => Some(-1),
            KeyCode::PageDown => Some(20),
            KeyCode::PageUp => Some(-20),
            KeyCode::Char('g') | KeyCode::Home => Some(isize::MIN / 2),
            KeyCode::Char('G') | KeyCode::End => Some(isize::MAX / 2),
            _ => None,
        }
    }

    fn key_queue(&mut self, k: KeyEvent) {
        if let Some(d) = Self::nav(&k) {
            move_sel(&mut self.queue_st, self.queue.len(), d);
            return;
        }
        let sel = self.queue_st.selected();
        match k.code {
            KeyCode::Enter => {
                if let Some(i) = sel {
                    self.req("player.play", json!({ "index": i }), Pending::Ignore);
                }
            }
            KeyCode::Char('d') | KeyCode::Delete => {
                if let Some(q) = sel.and_then(|i| self.queue.get(i)) {
                    let id = q.id;
                    self.req("queue.remove", json!({ "ids": [id] }), Pending::Ignore);
                }
            }
            KeyCode::Char('C') => self.req("queue.clear", Value::Null, Pending::Ignore),
            KeyCode::Char('o') => {
                if let Some(i) = self.current_index() {
                    self.queue_st.select(Some(i));
                }
            }
            _ => {}
        }
    }

    fn key_library(&mut self, k: KeyEvent) {
        if let Some(d) = Self::nav(&k) {
            let col = self.lib_col;
            let len = [self.artists.len(), self.releases.len(), self.tracks.len()][col];
            let before = self.lib_st[col].selected();
            move_sel(&mut self.lib_st[col], len, d);
            if self.lib_st[col].selected() != before {
                match col {
                    0 => self.load_releases(),
                    1 => self.load_tracks(),
                    _ => {}
                }
            }
            return;
        }
        match k.code {
            KeyCode::Char('h') | KeyCode::Left => self.lib_col = self.lib_col.saturating_sub(1),
            KeyCode::Char('l') | KeyCode::Right => self.lib_col = (self.lib_col + 1).min(2),
            KeyCode::Enter => self.add_from_library(AddMode::PlayNext),
            KeyCode::Char('a') => self.add_from_library(AddMode::Append),
            KeyCode::Char('R') => self.add_from_library(AddMode::Replace),
            _ => {}
        }
    }

    fn key_search(&mut self, k: KeyEvent) {
        if let Some(d) = Self::nav(&k) {
            move_sel(&mut self.results_st, self.results.len(), d);
            return;
        }
        let sel = self
            .results_st
            .selected()
            .and_then(|i| self.results.get(i))
            .map(|t| t.uri.clone());
        match k.code {
            KeyCode::Char('i') => self.editing = true,
            KeyCode::Enter => {
                if let Some(u) = sel {
                    self.enqueue(vec![u], AddMode::PlayNext)
                }
            }
            KeyCode::Char('a') => {
                if let Some(u) = sel {
                    self.enqueue(vec![u], AddMode::Append)
                }
            }
            KeyCode::Char('A') => {
                let all = self.results.iter().map(|t| t.uri.clone()).collect();
                self.enqueue(all, AddMode::Append);
            }
            _ => {}
        }
    }

    // ───────────────────────── drawing ─────────────────────────

    fn draw(&mut self, f: &mut Frame) {
        let [top, body, now, help] = Layout::vertical([
            Constraint::Length(1),
            Constraint::Min(3),
            Constraint::Length(4),
            Constraint::Length(1),
        ])
        .areas(f.area());

        self.draw_tabs(f, top);
        match self.tab {
            Tab::Queue => self.draw_queue(f, body),
            Tab::Library => self.draw_library(f, body),
            Tab::Search => self.draw_search(f, body),
        }
        self.draw_now(f, now);
        self.draw_help(f, help);
    }

    fn draw_tabs(&self, f: &mut Frame, area: Rect) {
        let mut spans = vec![
            Span::styled(
                " lark ",
                Style::new().bold().fg(Color::Black).bg(Color::Cyan),
            ),
            " ".into(),
        ];
        for (i, (t, name)) in [
            (Tab::Queue, "queue"),
            (Tab::Library, "library"),
            (Tab::Search, "search"),
        ]
        .into_iter()
        .enumerate()
        {
            let s = format!(" {} {} ", i + 1, name);
            spans.push(if t == self.tab {
                Span::styled(s, Style::new().bold().reversed())
            } else {
                Span::raw(s).dim()
            });
        }
        f.render_widget(Line::from(spans), area);
    }

    fn track_row(t: &Track, playing: Option<PlayState>, width: u16) -> ListItem<'static> {
        let mark = match playing {
            Some(PlayState::Playing) => "▶ ",
            Some(PlayState::Paused) => "‖ ",
            _ => "  ",
        };
        let dur = t.duration_ms.map(fmt_ms).unwrap_or_default();
        let left = format!("{mark}{}", t.title);
        let mid = format!(
            "{}{}",
            t.artist_line(),
            t.album
                .as_deref()
                .map(|a| format!(" · {a}"))
                .unwrap_or_default()
        );
        let w = width as usize;
        let title_w = (w * 45 / 100).max(10);
        let mid_w = w.saturating_sub(title_w + dur.chars().count() + 2);
        let clip = |s: &str, n: usize| -> String {
            if s.chars().count() <= n {
                format!("{s:<n$}")
            } else {
                let mut o: String = s.chars().take(n.saturating_sub(1)).collect();
                o.push('…');
                o
            }
        };
        let mut style = Style::new();
        if playing.is_some() {
            style = style.fg(Color::Cyan).add_modifier(Modifier::BOLD);
        }
        ListItem::new(Line::from(vec![
            Span::styled(clip(&left, title_w), style),
            Span::styled(clip(&mid, mid_w), Style::new().dim()),
            Span::raw(format!(" {dur}")),
        ]))
    }

    fn draw_queue(&mut self, f: &mut Frame, area: Rect) {
        let block = Block::new()
            .borders(Borders::TOP)
            .title(format!(" queue · {} ", self.queue.len()))
            .padding(Padding::horizontal(1));
        let inner_w = area.width.saturating_sub(4);
        let items: Vec<ListItem> = self
            .queue
            .iter()
            .map(|q| {
                let playing = (Some(q.id) == self.player.current).then_some(self.player.state);
                Self::track_row(&q.track, playing, inner_w)
            })
            .collect();
        if items.is_empty() {
            let p = Paragraph::new("Queue is empty. Press 2 for the library or / to search.")
                .dim()
                .block(block);
            f.render_widget(p, area);
            return;
        }
        let list = List::new(items)
            .block(block)
            .highlight_style(Style::new().reversed());
        f.render_stateful_widget(list, area, &mut self.queue_st);
    }

    fn draw_library(&mut self, f: &mut Frame, area: Rect) {
        let cols = Layout::horizontal([
            Constraint::Percentage(25),
            Constraint::Percentage(30),
            Constraint::Percentage(45),
        ])
        .split(area);
        let title = |name: &str, i: usize, me: &Self| {
            let s = format!(" {name} ");
            if me.lib_col == i {
                Span::styled(s, Style::new().bold().fg(Color::Cyan))
            } else {
                Span::raw(s).dim()
            }
        };
        let hl = |i: usize, me: &Self| {
            if me.lib_col == i {
                Style::new().reversed()
            } else {
                Style::new().add_modifier(Modifier::UNDERLINED)
            }
        };

        let artists: Vec<ListItem> = self
            .artists
            .iter()
            .map(|a| ListItem::new(a.name.clone()))
            .collect();
        let l = List::new(artists)
            .block(
                Block::new()
                    .borders(Borders::TOP | Borders::RIGHT)
                    .title(title("artists", 0, self)),
            )
            .highlight_style(hl(0, self));
        f.render_stateful_widget(l, cols[0], &mut self.lib_st[0]);

        let releases: Vec<ListItem> = self
            .releases
            .iter()
            .map(|r| {
                let year = r
                    .date
                    .as_deref()
                    .map(|d| d.chars().take(4).collect::<String>())
                    .unwrap_or_default();
                ListItem::new(Line::from(vec![
                    Span::raw(format!("{year:<5}")).dim(),
                    Span::raw(r.title.clone()),
                ]))
            })
            .collect();
        let l = List::new(releases)
            .block(
                Block::new()
                    .borders(Borders::TOP | Borders::RIGHT)
                    .title(title("releases", 1, self)),
            )
            .highlight_style(hl(1, self));
        f.render_stateful_widget(l, cols[1], &mut self.lib_st[1]);

        let playing_uri = self
            .player
            .current
            .and_then(|c| self.queue.iter().find(|q| q.id == c))
            .map(|q| q.track.uri.clone());
        let tracks: Vec<ListItem> = self
            .tracks
            .iter()
            .map(|t| {
                let n = t
                    .track_no
                    .map(|n| format!("{n:>2} "))
                    .unwrap_or_else(|| "   ".into());
                let dur = t.duration_ms.map(fmt_ms).unwrap_or_default();
                let mut s = Style::new();
                if playing_uri.as_deref() == Some(&t.uri) {
                    s = s.fg(Color::Cyan).bold();
                }
                ListItem::new(Line::from(vec![
                    Span::raw(n).dim(),
                    Span::styled(t.title.clone(), s),
                    Span::raw(format!("  {dur}")).dim(),
                ]))
            })
            .collect();
        let l = List::new(tracks)
            .block(
                Block::new()
                    .borders(Borders::TOP)
                    .title(title("tracks", 2, self))
                    .padding(Padding::left(1)),
            )
            .highlight_style(hl(2, self));
        f.render_stateful_widget(l, cols[2], &mut self.lib_st[2]);
    }

    fn draw_search(&mut self, f: &mut Frame, area: Rect) {
        let [input, list] =
            Layout::vertical([Constraint::Length(2), Constraint::Min(1)]).areas(area);
        let cursor = if self.editing { "█" } else { "" };
        let prompt = Line::from(vec![
            Span::styled(" / ", Style::new().fg(Color::Cyan).bold()),
            Span::raw(self.query.clone()),
            Span::raw(cursor),
        ]);
        f.render_widget(
            Paragraph::new(prompt).block(Block::new().borders(Borders::TOP)),
            input,
        );
        let w = area.width.saturating_sub(4);
        let items: Vec<ListItem> = self
            .results
            .iter()
            .map(|t| Self::track_row(t, None, w))
            .collect();
        let block = Block::new()
            .borders(Borders::TOP)
            .title(format!(" {} results ", self.results.len()))
            .padding(Padding::horizontal(1));
        let l = List::new(items)
            .block(block)
            .highlight_style(Style::new().reversed());
        f.render_stateful_widget(l, list, &mut self.results_st);
    }

    fn draw_now(&self, f: &mut Frame, area: Rect) {
        let block = Block::new()
            .borders(Borders::TOP)
            .padding(Padding::horizontal(1));
        let inner = block.inner(area);
        f.render_widget(block, area);
        let [l1, l2, l3] = Layout::vertical([Constraint::Length(1); 3]).areas(inner);

        let cur = self
            .player
            .current
            .and_then(|c| self.queue.iter().find(|q| q.id == c));
        let state = match self.player.state {
            PlayState::Playing => Span::styled("▶ playing", Style::new().fg(Color::Green)),
            PlayState::Paused => Span::styled("‖ paused", Style::new().fg(Color::Yellow)),
            PlayState::Stopped => Span::styled("■ stopped", Style::new().dim()),
        };
        match cur {
            Some(q) => {
                f.render_widget(
                    Line::from(vec![
                        state,
                        "  ".into(),
                        Span::styled(q.track.title.clone(), Style::new().bold()),
                    ]),
                    l1,
                );
                let sub = format!(
                    "{}{}",
                    q.track.artist_line(),
                    q.track
                        .album
                        .as_deref()
                        .map(|a| format!(" · {a}"))
                        .unwrap_or_default()
                );
                f.render_widget(Line::from(sub).dim(), l2);
                let pos = self.player.position_now(now_ms());
                let dur = self.player.duration_ms.or(q.track.duration_ms);
                let ratio = dur
                    .map(|d| (pos as f64 / d.max(1) as f64).clamp(0.0, 1.0))
                    .unwrap_or(0.0);
                let label = format!(
                    "{} / {}",
                    fmt_ms(pos),
                    dur.map(fmt_ms).unwrap_or_else(|| "?".into())
                );
                let g = LineGauge::default()
                    .ratio(ratio)
                    .label(label)
                    .filled_style(Style::new().fg(Color::Cyan))
                    .unfilled_style(Style::new().dim());
                f.render_widget(g, l3);
            }
            None => f.render_widget(Line::from(vec![state]), l1),
        }
    }

    fn draw_help(&self, f: &mut Frame, area: Rect) {
        if let Some((msg, until)) = &self.flash {
            if now_ms() < *until {
                f.render_widget(Line::from(format!(" {msg}")).fg(Color::Yellow), area);
                return;
            }
        }
        let ctx = match (self.tab, self.editing) {
            (_, true) => "type to search · enter/esc done",
            (Tab::Queue, _) => "enter play · d remove · C clear · o jump to current",
            (Tab::Library, _) => "h/l column · enter play next · a append · R replace",
            (Tab::Search, _) => "/ or i edit · enter play next · a append · A append all",
        };
        let txt = format!(" {ctx} │ spc pause · n/p · [ ] seek · +/- vol · u sync · q");
        f.render_widget(Line::from(txt).dim(), area);
    }
}
