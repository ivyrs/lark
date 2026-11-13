mod client;
mod tui;

use anyhow::Result;
use clap::{Parser, Subcommand};
use lark_protocol::{now_ms, PlayState, SourceInfo, Status, Track};
use serde_json::{json, Value};

use crate::client::Client;

#[derive(Parser)]
#[command(
    name = "lark",
    version,
    about = "lark: TUI and control client for larkd"
)]
struct Cli {
    #[command(subcommand)]
    cmd: Option<Cmd>,
}

#[derive(Subcommand)]
enum Cmd {
    /// Open the TUI (default).
    Tui,
    /// Show what's playing.
    Status,
    Play {
        /// Queue index (0-based).
        index: Option<usize>,
    },
    Pause,
    Toggle,
    Stop,
    Next,
    Prev,
    /// Seek to seconds, or relative with +N / -N.
    Seek {
        to: String,
    },
    /// Volume 0–150 (percent).
    Volume {
        percent: f32,
    },
    /// Search the library.
    Search {
        query: Vec<String>,
    },
    /// Search and append all matches to the queue.
    Add {
        query: Vec<String>,
        /// Start playing the first added track.
        #[arg(short, long)]
        play: bool,
    },
    /// Print the queue.
    Queue,
    /// Clear the queue.
    Clear,
    /// Ask full-cache sources to resync.
    Sync,
    /// List sources.
    Sources,
    /// Recent plays.
    History {
        #[arg(default_value_t = 20)]
        limit: u32,
    },
    /// Stream daemon events as JSON lines.
    Watch,
    /// Send a raw request: `lark raw library.artists '{}'`.
    Raw {
        method: String,
        params: Option<String>,
    },
}

fn fmt_ms(ms: u64) -> String {
    let s = ms / 1000;
    format!("{}:{:02}", s / 60, s % 60)
}

fn line(t: &Track) -> String {
    let album = t
        .album
        .as_deref()
        .map(|a| format!(" · {a}"))
        .unwrap_or_default();
    format!("{} — {}{}", t.artist_line(), t.title, album)
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let cmd = cli.cmd.unwrap_or(Cmd::Tui);
    if let Cmd::Tui = cmd {
        return tui::run();
    }
    let mut c = Client::connect()?;
    let null = Value::Null;
    match cmd {
        Cmd::Tui => unreachable!(),
        Cmd::Status => {
            let s: Status = c.call("status", &null)?;
            let p = &s.player;
            let cur = p.current.and_then(|id| s.queue.iter().find(|q| q.id == id));
            match cur {
                Some(q) => {
                    let pos = p.position_now(now_ms());
                    let dur = p.duration_ms.map(fmt_ms).unwrap_or_else(|| "?".into());
                    println!(
                        "[{:?}] {}  {}/{}",
                        p.state,
                        line(&q.track),
                        fmt_ms(pos),
                        dur
                    );
                }
                None => println!(
                    "[{:?}] nothing loaded · {} in queue",
                    p.state,
                    s.queue.len()
                ),
            }
        }
        Cmd::Play { index } => c
            .call::<_, Value>("player.play", json!({ "index": index }))
            .map(drop)?,
        Cmd::Pause => c.call::<_, Value>("player.pause", &null).map(drop)?,
        Cmd::Toggle => c.call::<_, Value>("player.toggle", &null).map(drop)?,
        Cmd::Stop => c.call::<_, Value>("player.stop", &null).map(drop)?,
        Cmd::Next => c.call::<_, Value>("player.next", &null).map(drop)?,
        Cmd::Prev => c.call::<_, Value>("player.prev", &null).map(drop)?,
        Cmd::Seek { to } => {
            let s: Status = c.call("status", &null)?;
            let cur = s.player.position_now(now_ms()) as i64;
            let ms = if let Some(r) = to.strip_prefix('+') {
                cur + (r.parse::<f64>()? * 1000.0) as i64
            } else if let Some(r) = to.strip_prefix('-') {
                cur - (r.parse::<f64>()? * 1000.0) as i64
            } else {
                (to.parse::<f64>()? * 1000.0) as i64
            };
            c.call::<_, Value>("player.seek", json!({ "position_ms": ms.max(0) }))?;
        }
        Cmd::Volume { percent } => c
            .call::<_, Value>("player.volume", json!({ "volume": percent / 100.0 }))
            .map(drop)?,
        Cmd::Search { query } => {
            let ts: Vec<Track> = c.call(
                "library.search",
                json!({ "query": query.join(" "), "limit": 50 }),
            )?;
            for t in ts {
                println!("{}\t{}", line(&t), t.uri);
            }
        }
        Cmd::Add { query, play } => {
            let ts: Vec<Track> = c.call(
                "library.search",
                json!({ "query": query.join(" "), "limit": 500 }),
            )?;
            let uris: Vec<_> = ts.iter().map(|t| t.uri.clone()).collect();
            let n = uris.len();
            c.call::<_, Value>("queue.add", json!({ "uris": uris, "play": play }))?;
            println!("added {n} track(s)");
        }
        Cmd::Queue => {
            let s: Status = c.call("status", &null)?;
            for (i, q) in s.queue.iter().enumerate() {
                let mark = if Some(q.id) == s.player.current {
                    if s.player.state == PlayState::Playing {
                        "▶"
                    } else {
                        "‖"
                    }
                } else {
                    " "
                };
                let dur = q.track.duration_ms.map(fmt_ms).unwrap_or_default();
                println!("{mark} {i:>3}  {}  {dur}", line(&q.track));
            }
        }
        Cmd::Clear => c.call::<_, Value>("queue.clear", &null).map(drop)?,
        Cmd::Sync => c.call::<_, Value>("library.sync", &null).map(drop)?,
        Cmd::Sources => {
            let ss: Vec<SourceInfo> = c.call("sources", &null)?;
            for s in ss {
                println!(
                    "{:<12} {:<8} cache={:<8} {:<8} {} tracks",
                    s.name,
                    s.scheme,
                    format!("{:?}", s.cache).to_lowercase(),
                    if s.running { "running" } else { "down" },
                    s.tracks
                );
            }
        }
        Cmd::History { limit } => {
            let plays: Vec<lark_protocol::Play> = c.call("history", json!({ "limit": limit }))?;
            for p in plays {
                let mark = if p.completed { "✓" } else { " " };
                println!("{mark} {}  ({})", line(&p.track), fmt_ms(p.played_ms));
            }
        }
        Cmd::Watch => {
            c.call::<_, Value>("subscribe", &null)?;
            loop {
                let n = c.next_event()?;
                println!("{}", serde_json::to_string(&n)?);
            }
        }
        Cmd::Raw { method, params } => {
            let p: Value = params
                .map(|p| serde_json::from_str(&p))
                .transpose()?
                .unwrap_or(Value::Null);
            let v: Value = c.call(&method, p)?;
            println!("{}", serde_json::to_string_pretty(&v)?);
        }
    }
    Ok(())
}
