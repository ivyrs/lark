use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixStream;
use std::sync::mpsc;

use anyhow::{anyhow, Context, Result};
use lark_protocol::{encode, socket_path, Message, Notification, Request};
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::Value;

fn connect() -> Result<UnixStream> {
    let path = socket_path();
    UnixStream::connect(&path)
        .with_context(|| format!("connecting to larkd at {} (is it running?)", path.display()))
}

/// Blocking request/response client for one-shot commands.
pub struct Client {
    w: UnixStream,
    r: BufReader<UnixStream>,
    next: u64,
}

impl Client {
    pub fn connect() -> Result<Self> {
        let w = connect()?;
        let r = BufReader::new(w.try_clone()?);
        Ok(Self { w, r, next: 1 })
    }

    pub fn call<P: Serialize, R: DeserializeOwned>(
        &mut self,
        method: &str,
        params: P,
    ) -> Result<R> {
        let id = self.next;
        self.next += 1;
        let req = Request {
            id,
            method: method.into(),
            params: serde_json::to_value(params)?,
        };
        self.w.write_all(encode(&req).as_bytes())?;
        loop {
            match self.read()? {
                Message::Response(r) if r.id == id => {
                    if let Some(e) = r.error {
                        return Err(anyhow!("{}", e.message));
                    }
                    return Ok(serde_json::from_value(r.result.unwrap_or(Value::Null))?);
                }
                _ => continue,
            }
        }
    }

    pub fn next_event(&mut self) -> Result<Notification> {
        loop {
            if let Message::Notification(n) = self.read()? {
                return Ok(n);
            }
        }
    }

    fn read(&mut self) -> Result<Message> {
        let mut line = String::new();
        if self.r.read_line(&mut line)? == 0 {
            return Err(anyhow!("daemon closed the connection"));
        }
        Ok(serde_json::from_str(&line)?)
    }
}

/// Non-blocking client for the TUI: a reader thread feeds a channel.
pub struct Live {
    w: UnixStream,
    pub rx: mpsc::Receiver<Message>,
    next: u64,
}

impl Live {
    pub fn connect() -> Result<Self> {
        let w = connect()?;
        let r = BufReader::new(w.try_clone()?);
        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            for line in r.lines() {
                let Ok(line) = line else { break };
                if let Ok(m) = serde_json::from_str::<Message>(&line) {
                    if tx.send(m).is_err() {
                        break;
                    }
                }
            }
        });
        Ok(Self { w, rx, next: 1 })
    }

    pub fn send(&mut self, method: &str, params: Value) -> u64 {
        let id = self.next;
        self.next += 1;
        let req = Request {
            id,
            method: method.into(),
            params,
        };
        let _ = self.w.write_all(encode(&req).as_bytes());
        id
    }
}
