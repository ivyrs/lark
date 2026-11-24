//! Playback engine: one dedicated thread that decodes with symphonia and
//! feeds an `Output`. The next track is pre-opened and decoded straight into
//! the same ring buffer, so same-spec transitions are gapless. Track
//! boundaries are announced when they are *heard*, not when decoded.

use std::collections::VecDeque;
use std::fs::File;
use std::path::Path;
use std::sync::mpsc::{Receiver, RecvTimeoutError, Sender};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result};
use lark_protocol::Stream;
use symphonia::core::codecs::audio::{AudioDecoder, AudioDecoderOptions};
use symphonia::core::errors::Error as SymError;
use symphonia::core::formats::probe::Hint;
use symphonia::core::formats::{FormatOptions, FormatReader, SeekMode, SeekTo, TrackType};
use symphonia::core::io::{MediaSource, MediaSourceStream};
use symphonia::core::meta::MetadataOptions;
use symphonia::core::units::Time;
use tokio::sync::{mpsc::UnboundedSender, watch};

use crate::config::OutputKind;
use crate::http::{cancelled, HttpTransport, Playback};
use crate::output::{OutShared, Output};

pub enum Cmd {
    /// Start `id` from `stream` at `start_ms`, dropping whatever is buffered.
    Play {
        id: u64,
        stream: Stream,
        start_ms: u64,
    },
    /// What to roll into when the current track ends (gapless).
    SetNext(Option<(u64, Stream)>),
    Pause,
    Resume,
    Stop,
    Volume(f32),
}

#[derive(Debug)]
pub enum Event {
    /// `id` became audible at `position_ms`.
    Started {
        id: u64,
        position_ms: u64,
        duration_ms: Option<u64>,
    },
    /// Decoding `id` failed; the core should skip it.
    Error { id: u64, message: String },
    /// Queue ran dry and the buffer drained.
    Finished,
}

/// An event queued before a replacement Play/Stop can reach the core after
/// that command. Check its generation in the core actor before handling it.
pub struct StampedEvent {
    event: Event,
    playback: Playback,
}

impl StampedEvent {
    pub fn into_current(self) -> Option<Event> {
        (!cancelled(&self.playback)).then_some(self.event)
    }
}

struct Message {
    cmd: Cmd,
    playback: Option<Playback>,
}

struct Submit {
    tx: Sender<Message>,
    playback: watch::Sender<()>,
}

/// What clients need to compute a live position without asking the thread.
struct Clock {
    id: u64,
    base_ms: u64,
    start_sample: u64,
    samples_per_sec: u64,
    shared: Arc<OutShared>,
}

#[derive(Clone)]
pub struct EngineHandle {
    submit: Arc<Mutex<Submit>>,
    clock: Arc<Mutex<Option<Clock>>>,
}

impl EngineHandle {
    pub fn send(&self, cmd: Cmd) {
        // Serialize the notification and queue insertion with all senders. A
        // replacement cancels even a Play command not yet read by the engine.
        let submit = self.submit.lock().unwrap();
        if matches!(cmd, Cmd::Play { .. } | Cmd::Stop) {
            submit.playback.send_replace(());
        }
        let playback = matches!(cmd, Cmd::Play { .. }).then(|| submit.playback.subscribe());
        let _ = submit.tx.send(Message { cmd, playback });
    }

    /// (item id, position ms) of what is audible right now.
    pub fn position(&self) -> Option<(u64, u64)> {
        let g = self.clock.lock().unwrap();
        let c = g.as_ref()?;
        let consumed = c.shared.consumed.load(std::sync::atomic::Ordering::Acquire);
        let played = consumed.saturating_sub(c.start_sample);
        Some((c.id, c.base_ms + played * 1000 / c.samples_per_sec.max(1)))
    }
}

pub fn spawn(kind: OutputKind, events: UnboundedSender<StampedEvent>) -> EngineHandle {
    let (tx, rx) = std::sync::mpsc::channel();
    let (submit_playback, playback) = watch::channel(());
    let clock = Arc::new(Mutex::new(None));
    let mut eng = Engine {
        kind,
        events,
        playback,
        http: HttpTransport::new(tokio::runtime::Handle::current()),
        clock: clock.clone(),
        out: None,
        cur: None,
        next: None,
        segments: VecDeque::new(),
        pend: Vec::new(),
        pend_pos: 0,
        scratch: Vec::new(),
        resampler: None,
        reopen: None,
        draining: false,
        volume: 1.0,
    };
    std::thread::Builder::new()
        .name("lark-engine".into())
        .spawn(move || eng.run(rx))
        .expect("spawn engine");
    EngineHandle {
        submit: Arc::new(Mutex::new(Submit {
            tx,
            playback: submit_playback,
        })),
        clock,
    }
}

struct Decoding {
    id: u64,
    format: Box<dyn FormatReader>,
    decoder: Box<dyn AudioDecoder>,
    track_id: u32,
    rate: u32,
    channels: usize,
    duration_ms: Option<u64>,
    http_seekable: Option<bool>,
    eof: bool,
}

struct Segment {
    id: u64,
    start_sample: u64,
    base_ms: u64,
    duration_ms: Option<u64>,
}

struct Engine {
    kind: OutputKind,
    events: UnboundedSender<StampedEvent>,
    playback: Playback,
    http: HttpTransport,
    clock: Arc<Mutex<Option<Clock>>>,
    out: Option<Output>,
    cur: Option<Decoding>,
    next: Option<(u64, Stream)>,
    /// Segments written to the ring but not yet announced (front = oldest).
    segments: VecDeque<Segment>,
    pend: Vec<f32>,
    pend_pos: usize,
    scratch: Vec<f32>,
    resampler: Option<Linear>,
    /// Next track needs a different output spec; reopen once drained.
    reopen: Option<Decoding>,
    draining: bool,
    volume: f32,
}

impl Engine {
    fn run(&mut self, rx: Receiver<Message>) {
        loop {
            let busy = self.has_work();
            let timeout = if busy {
                Duration::from_millis(0)
            } else {
                Duration::from_millis(10)
            };
            match rx.recv_timeout(timeout) {
                Ok(cmd) => self.handle(cmd),
                Err(RecvTimeoutError::Timeout) => {}
                Err(RecvTimeoutError::Disconnected) => return,
            }
            while let Ok(cmd) = rx.try_recv() {
                self.handle(cmd);
            }
            // Fill the ring; yield briefly when it's full.
            let mut progressed = false;
            for _ in 0..64 {
                if !self.pump() {
                    break;
                }
                progressed = true;
            }
            self.tick();
            if busy && !progressed {
                std::thread::sleep(Duration::from_millis(5));
            }
        }
    }

    fn has_work(&self) -> bool {
        self.out.is_some() && (self.cur.is_some() || self.pend_pos < self.pend.len())
    }

    fn emit(&self, e: Event) {
        if !cancelled(&self.playback) {
            let _ = self.events.send(StampedEvent {
                event: e,
                playback: self.playback.clone(),
            });
        }
    }

    fn handle(&mut self, msg: Message) {
        match msg.cmd {
            Cmd::Play {
                id,
                stream,
                start_ms,
            } => {
                self.playback = msg.playback.expect("Play has a playback generation");
                if !cancelled(&self.playback) {
                    self.play(id, &stream, start_ms);
                }
            }
            Cmd::SetNext(n) => {
                if cancelled(&self.playback) {
                    return;
                }
                self.next = n;
                // We may already be draining the last track: the new next can
                // still go in gaplessly.
                if self.cur.is_none()
                    && self.reopen.is_none()
                    && self.draining
                    && self.next.is_some()
                {
                    self.advance();
                }
            }
            Cmd::Pause => {
                if let Some(o) = &self.out {
                    o.shared
                        .paused
                        .store(true, std::sync::atomic::Ordering::Relaxed);
                }
            }
            Cmd::Resume => {
                if let Some(o) = &self.out {
                    o.shared
                        .paused
                        .store(false, std::sync::atomic::Ordering::Relaxed);
                }
            }
            Cmd::Stop => self.stop(),
            Cmd::Volume(v) => {
                self.volume = v.clamp(0.0, 1.5);
                if let Some(o) = &self.out {
                    o.shared
                        .volume
                        .store(self.volume.to_bits(), std::sync::atomic::Ordering::Relaxed);
                }
            }
        }
    }

    fn stop(&mut self) {
        self.cur = None;
        self.next = None;
        self.reopen = None;
        self.segments.clear();
        self.pend.clear();
        self.pend_pos = 0;
        self.draining = false;
        self.out = None; // release the device
        *self.clock.lock().unwrap() = None;
    }

    fn play(&mut self, id: u64, stream: &Stream, start_ms: u64) {
        self.cur = None;
        self.next = None;
        self.reopen = None;
        self.draining = false;
        self.pend.clear();
        self.pend_pos = 0;
        self.segments.clear();
        if let Some(out) = &self.out {
            // Don't continue playing stale buffered audio while the new
            // HTTP response opens. The output can still be reused below.
            out.shared
                .paused
                .store(true, std::sync::atomic::Ordering::Relaxed);
        }
        let (dec, base_ms) = match self.prepare(id, stream, start_ms) {
            Ok(d) if !cancelled(&self.playback) => d,
            Ok(_) => return,
            Err(e) => {
                self.fail(id, format!("{e:#}"));
                return;
            }
        };

        let reuse = self
            .out
            .as_ref()
            .is_some_and(|o| o.rate == dec.rate && channels_ok(o.channels, dec.channels));
        if reuse {
            let o = self.out.as_mut().unwrap();
            o.flush();
            o.shared
                .paused
                .store(false, std::sync::atomic::Ordering::Relaxed);
        } else {
            self.out = None;
            match Output::open(self.kind, dec.rate, dec.channels, self.volume) {
                Ok(o) => self.out = Some(o),
                Err(e) => {
                    self.emit(Event::Error {
                        id,
                        message: format!("output: {e:#}"),
                    });
                    return;
                }
            }
        }
        self.begin(dec, base_ms);
    }

    fn fail(&mut self, id: u64, message: String) {
        self.stop();
        self.emit(Event::Error { id, message });
    }

    fn prepare(&mut self, id: u64, stream: &Stream, start_ms: u64) -> Result<(Decoding, u64)> {
        let mut dec = open_decoder(id, stream, &mut self.http, &self.playback)?;
        if start_ms == 0 {
            return Ok((dec, 0));
        }
        if dec.http_seekable == Some(false) {
            // Don't discard an arbitrarily long prefix to simulate a seek.
            tracing::warn!(
                item = id,
                "HTTP audio has no byte ranges; restarting at the beginning"
            );
            return Ok((dec, 0));
        }
        match dec.seek(start_ms) {
            Ok(()) => Ok((dec, start_ms)),
            Err(e) if dec.http_seekable.is_none() => {
                tracing::warn!("seek failed: {e}");
                Ok((dec, start_ms))
            }
            Err(e)
                if matches!(e, SymError::SeekError(_))
                    || matches!(&e, SymError::IoError(e) if e.kind() == std::io::ErrorKind::Unsupported) =>
            {
                tracing::warn!(
                    item = id,
                    "HTTP audio seek unsupported; restarting at the beginning"
                );
                // A demuxer may consume bytes before deciding it cannot seek.
                drop(dec);
                Ok((open_decoder(id, stream, &mut self.http, &self.playback)?, 0))
            }
            Err(SymError::IoError(e)) => Err(e).context("HTTP audio seek failed"),
            Err(_) => anyhow::bail!("HTTP audio seek failed"),
        }
    }

    /// Make `dec` the decoding track, starting at the current write position.
    fn begin(&mut self, dec: Decoding, base_ms: u64) {
        let out = self.out.as_ref().expect("output open");
        self.resampler =
            (dec.rate != out.rate).then(|| Linear::new(dec.rate, out.rate, out.channels));
        self.segments.push_back(Segment {
            id: dec.id,
            start_sample: out.written,
            base_ms,
            duration_ms: dec.duration_ms,
        });
        self.cur = Some(dec);
    }

    /// Current track hit EOF: roll into `next` if there is one.
    fn advance(&mut self) {
        self.cur = None;
        let Some((id, stream)) = self.next.take() else {
            self.draining = true;
            return;
        };
        self.draining = false;
        match open_decoder(id, &stream, &mut self.http, &self.playback) {
            Ok(dec) if !cancelled(&self.playback) => {
                let out = self.out.as_ref().expect("output open");
                if out.rate == dec.rate && channels_ok(out.channels, dec.channels) {
                    self.begin(dec, 0);
                } else {
                    self.reopen = Some(dec);
                }
            }
            Ok(_) => {}
            Err(e) => {
                self.draining = true;
                self.emit(Event::Error {
                    id,
                    message: format!("{e:#}"),
                });
            }
        }
    }

    /// Move decoded audio into the ring. Returns true if anything happened.
    fn pump(&mut self) -> bool {
        if cancelled(&self.playback) {
            return false;
        }
        // Publish an audible transition before a subsequent packet read can
        // block waiting on the network. Do not wait for the pump batch to end.
        if self.segments.front().is_some_and(|seg| {
            self.out.as_ref().is_some_and(|out| {
                out.written > seg.start_sample && out.consumed() >= seg.start_sample
            })
        }) {
            self.tick();
        }
        let Some(out) = self.out.as_mut() else {
            return false;
        };
        if self.pend_pos < self.pend.len() {
            let n = out.push(&self.pend[self.pend_pos..]);
            self.pend_pos += n;
            return n > 0;
        }
        let Some(dec) = self.cur.as_mut() else {
            return false;
        };
        if dec.eof {
            self.advance();
            return true;
        }
        if out.prod.slots() < out.channels {
            return false;
        }
        let packet = dec.format.next_packet();
        if cancelled(&self.playback) {
            return false;
        }
        match packet {
            Ok(Some(pkt)) => {
                if pkt.track_id != dec.track_id {
                    return true;
                }
                match dec.decoder.decode(&pkt) {
                    Ok(buf) => {
                        buf.copy_to_vec_interleaved(&mut self.scratch);
                        self.pend.clear();
                        self.pend_pos = 0;
                        remix(&self.scratch, dec.channels, out.channels, &mut self.pend);
                        if let Some(rs) = self.resampler.as_mut() {
                            let src = std::mem::take(&mut self.pend);
                            rs.process(&src, &mut self.pend);
                        }
                    }
                    Err(SymError::DecodeError(e)) => {
                        tracing::debug!("decode error (skipping packet): {e}")
                    }
                    Err(e) => {
                        tracing::warn!("decoder failed: {e}");
                        dec.eof = true;
                    }
                }
            }
            Ok(None) => dec.eof = true,
            Err(SymError::IoError(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                dec.eof = true
            }
            Err(SymError::ResetRequired) => dec.eof = true,
            Err(e) if dec.http_seekable.is_some() => {
                let id = dec.id;
                let message = match e {
                    SymError::IoError(e) => format!("HTTP audio read failed: {e}"),
                    _ => "HTTP audio demux failed".into(),
                };
                self.fail(id, message);
                return false;
            }
            Err(e) => {
                tracing::warn!("demux failed: {e}");
                dec.eof = true;
            }
        }
        true
    }

    /// Announce boundaries that became audible; finish when drained.
    fn tick(&mut self) {
        if cancelled(&self.playback) {
            return;
        }
        let Some(out) = self.out.as_ref() else { return };
        let consumed = out.consumed();
        let sps = out.rate as u64 * out.channels as u64;

        while let Some(seg) = self.segments.front() {
            if consumed < seg.start_sample {
                break;
            }
            let seg = self.segments.pop_front().unwrap();
            *self.clock.lock().unwrap() = Some(Clock {
                id: seg.id,
                base_ms: seg.base_ms,
                start_sample: seg.start_sample,
                samples_per_sec: sps,
                shared: out.shared.clone(),
            });
            self.emit(Event::Started {
                id: seg.id,
                position_ms: seg.base_ms,
                duration_ms: seg.duration_ms,
            });
        }

        let drained = out.drained() && self.pend_pos >= self.pend.len();
        if drained {
            if let Some(dec) = self.reopen.take() {
                self.out = None;
                match Output::open(self.kind, dec.rate, dec.channels, self.volume) {
                    Ok(o) => {
                        self.out = Some(o);
                        self.begin(dec, 0);
                    }
                    Err(e) => self.emit(Event::Error {
                        id: dec.id,
                        message: format!("output: {e:#}"),
                    }),
                }
            } else if self.draining && self.cur.is_none() {
                self.draining = false;
                self.stop();
                self.emit(Event::Finished);
            }
        }
    }
}

fn channels_ok(out: usize, src: usize) -> bool {
    out == src || (out == 2 && src <= 2)
}

fn open_decoder(
    id: u64,
    stream: &Stream,
    http: &mut HttpTransport,
    playback: &Playback,
) -> Result<Decoding> {
    let mut hint = Hint::new();
    let (source, http_seekable): (Box<dyn MediaSource>, _) = match stream {
        Stream::File { path } => {
            let path = Path::new(path);
            let file = File::open(path).with_context(|| format!("opening {}", path.display()))?;
            if let Some(ext) = path.extension().and_then(|e| e.to_str()) {
                hint.with_extension(ext);
            }
            (Box::new(file), None)
        }
        Stream::Http { url, headers } => {
            let source = http.open(url, headers, playback)?;
            let seekable = source.is_seekable();
            (Box::new(source), Some(seekable))
        }
    };
    let format = symphonia::default::get_probe()
        .probe(
            &hint,
            MediaSourceStream::new(source, Default::default()),
            FormatOptions::default(),
            MetadataOptions::default(),
        )
        .context("unrecognised format")?;
    let track = format
        .default_track(TrackType::Audio)
        .context("no audio track")?;
    let params = track
        .codec_params
        .as_ref()
        .and_then(|p| p.audio())
        .context("no audio codec parameters")?
        .clone();
    let track_id = track.id;
    let rate = params.sample_rate.context("unknown sample rate")?;
    let channels = params
        .channels
        .as_ref()
        .map(|c| c.count())
        .unwrap_or(2)
        .max(1);
    let duration_ms = track.num_frames.map(|n| n * 1000 / rate as u64);
    let decoder = symphonia::default::get_codecs()
        .make_audio_decoder(&params, &AudioDecoderOptions::default())
        .context("unsupported codec")?;
    Ok(Decoding {
        id,
        format,
        decoder,
        track_id,
        rate,
        channels,
        duration_ms,
        http_seekable,
        eof: false,
    })
}

impl Decoding {
    fn seek(&mut self, ms: u64) -> symphonia::core::errors::Result<()> {
        let time = Time::try_from_secs_f64(ms as f64 / 1000.0).ok_or(SymError::SeekError(
            symphonia::core::errors::SeekErrorKind::OutOfRange,
        ))?;
        self.format.seek(
            SeekMode::Accurate,
            SeekTo::Time {
                time,
                track_id: Some(self.track_id),
            },
        )?;
        self.decoder.reset();
        Ok(())
    }
}

/// Map `src_ch` interleaved audio onto `out_ch` channels.
fn remix(src: &[f32], src_ch: usize, out_ch: usize, dst: &mut Vec<f32>) {
    if src_ch == out_ch {
        dst.extend_from_slice(src);
        return;
    }
    let frames = src.len() / src_ch;
    dst.reserve(frames * out_ch);
    for f in src.chunks_exact(src_ch) {
        if out_ch == 1 {
            dst.push(f.iter().sum::<f32>() / src_ch as f32);
        } else if src_ch == 1 {
            dst.extend(std::iter::repeat_n(f[0], out_ch));
        } else {
            // TODO: proper downmix matrices (5.1 → stereo etc.)
            for c in 0..out_ch {
                dst.push(f[c % src_ch]);
            }
        }
    }
}

/// Minimal streaming linear resampler, used only when the device can't run
/// at the track's native rate. Swap for a windowed-sinc one later.
struct Linear {
    step: f64,
    t: f64,
    ch: usize,
    last: Vec<f32>,
    primed: bool,
}

impl Linear {
    fn new(from: u32, to: u32, ch: usize) -> Self {
        Self {
            step: from as f64 / to as f64,
            t: 0.0,
            ch,
            last: vec![0.0; ch],
            primed: false,
        }
    }

    fn process(&mut self, input: &[f32], out: &mut Vec<f32>) {
        let ch = self.ch;
        let n = input.len() / ch;
        if n == 0 {
            return;
        }
        if !self.primed {
            self.last.copy_from_slice(&input[..ch]);
            self.primed = true;
        }
        // Virtual sequence: [last, input[0], …, input[n-1]] → n+1 frames.
        let frame = |i: usize, c: usize| {
            if i == 0 {
                self.last[c]
            } else {
                input[(i - 1) * ch + c]
            }
        };
        while self.t < n as f64 {
            let i = self.t.floor() as usize;
            let frac = (self.t - i as f64) as f32;
            for c in 0..ch {
                let a = frame(i, c);
                let b = frame(i + 1, c);
                out.push(a + (b - a) * frac);
            }
            self.t += self.step;
        }
        self.t -= n as f64;
        self.last.copy_from_slice(&input[(n - 1) * ch..n * ch]);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn remix_mono_to_stereo() {
        let mut d = vec![];
        remix(&[0.1, 0.2], 1, 2, &mut d);
        assert_eq!(d, [0.1, 0.1, 0.2, 0.2]);
    }

    #[test]
    fn resampler_ratio() {
        let mut r = Linear::new(48_000, 44_100, 1);
        let input = vec![0.5f32; 48_000];
        let mut out = vec![];
        r.process(&input, &mut out);
        assert!((out.len() as i64 - 44_100).abs() < 3, "{}", out.len());
    }
}
