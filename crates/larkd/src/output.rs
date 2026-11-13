//! Audio outputs. Both read interleaved f32 from a lock-free ring buffer and
//! count every consumed sample, which is what position tracking keys off.

use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context, Result};
use cpal::traits::{DeviceTrait, HostTrait, StreamTrait};
use rtrb::{Consumer, Producer, RingBuffer};

use crate::config::OutputKind;

#[derive(Default)]
pub struct OutShared {
    /// Samples taken out of the ring (played *or* flushed). Always a multiple
    /// of the channel count, and always equal to `Output::written` once drained.
    pub consumed: AtomicU64,
    pub paused: AtomicBool,
    pub flush_req: AtomicU64,
    pub flush_ack: AtomicU64,
    /// f32 bits.
    pub volume: AtomicU32,
}

impl OutShared {
    fn new(volume: f32) -> Self {
        let s = Self::default();
        s.volume.store(volume.to_bits(), Ordering::Relaxed);
        s
    }
}

enum Keep {
    Cpal(#[allow(dead_code)] cpal::Stream),
    Null(Arc<AtomicBool>),
}

pub struct Output {
    pub prod: Producer<f32>,
    pub shared: Arc<OutShared>,
    pub rate: u32,
    pub channels: usize,
    /// Samples pushed into the ring so far.
    pub written: u64,
    keep: Keep,
}

impl Drop for Output {
    fn drop(&mut self) {
        if let Keep::Null(stop) = &self.keep {
            stop.store(true, Ordering::Relaxed);
        }
    }
}

impl Output {
    pub fn open(
        kind: OutputKind,
        want_rate: u32,
        want_channels: usize,
        volume: f32,
    ) -> Result<Self> {
        match kind {
            OutputKind::Null => Ok(open_null(want_rate, want_channels, volume)),
            OutputKind::Default => open_cpal(want_rate, want_channels, volume),
        }
    }

    pub fn consumed(&self) -> u64 {
        self.shared.consumed.load(Ordering::Acquire)
    }

    pub fn drained(&self) -> bool {
        self.consumed() >= self.written
    }

    /// Discard everything buffered. Blocks briefly until the consumer acks,
    /// after which `consumed == written`.
    pub fn flush(&mut self) {
        let req = self.shared.flush_req.fetch_add(1, Ordering::AcqRel) + 1;
        let deadline = Instant::now() + Duration::from_millis(500);
        while self.shared.flush_ack.load(Ordering::Acquire) != req && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(1));
        }
        // If the device stalled, at least keep our accounting consistent.
        if self.consumed() < self.written {
            self.shared.consumed.store(self.written, Ordering::Release);
        }
    }

    /// Push as many whole frames as fit. Returns samples written.
    pub fn push(&mut self, data: &[f32]) -> usize {
        let n = (self.prod.slots().min(data.len()) / self.channels) * self.channels;
        if n == 0 {
            return 0;
        }
        let mut chunk = self.prod.write_chunk(n).expect("slots checked");
        let (a, b) = chunk.as_mut_slices();
        a.copy_from_slice(&data[..a.len()]);
        b.copy_from_slice(&data[a.len()..n]);
        chunk.commit_all();
        self.written += n as u64;
        n
    }
}

fn ring(rate: u32, channels: usize) -> (Producer<f32>, Consumer<f32>) {
    // ~0.5 s of audio.
    RingBuffer::new((rate as usize / 2).max(4096) * channels)
}

/// Shared consumer logic for every backend.
fn fill(out: &mut [f32], cons: &mut Consumer<f32>, sh: &OutShared) {
    let req = sh.flush_req.load(Ordering::Acquire);
    if req != sh.flush_ack.load(Ordering::Relaxed) {
        let n = cons.slots();
        if n > 0 {
            if let Ok(c) = cons.read_chunk(n) {
                c.commit_all();
            }
            sh.consumed.fetch_add(n as u64, Ordering::AcqRel);
        }
        sh.flush_ack.store(req, Ordering::Release);
    }
    if sh.paused.load(Ordering::Relaxed) {
        out.fill(0.0);
        return;
    }
    let vol = f32::from_bits(sh.volume.load(Ordering::Relaxed));
    let n = out.len().min(cons.slots());
    if n > 0 {
        let c = cons.read_chunk(n).expect("slots checked");
        let (a, b) = c.as_slices();
        for (o, s) in out.iter_mut().zip(a.iter().chain(b.iter())) {
            *o = s * vol;
        }
        c.commit_all();
        sh.consumed.fetch_add(n as u64, Ordering::AcqRel);
    }
    out[n..].fill(0.0);
}

fn open_null(rate: u32, channels: usize, volume: f32) -> Output {
    let (prod, mut cons) = ring(rate, channels);
    let shared = Arc::new(OutShared::new(volume));
    let stop = Arc::new(AtomicBool::new(false));
    let (sh, st) = (shared.clone(), stop.clone());
    std::thread::Builder::new()
        .name("lark-null-out".into())
        .spawn(move || {
            let start = Instant::now();
            let mut done: u64 = 0;
            let mut scratch = vec![0f32; rate as usize * channels];
            while !st.load(Ordering::Relaxed) {
                std::thread::sleep(Duration::from_millis(10));
                let due = (start.elapsed().as_secs_f64() * rate as f64) as u64 * channels as u64;
                let n = ((due - done) as usize).min(scratch.len());
                let n = n / channels * channels;
                fill(&mut scratch[..n], &mut cons, &sh);
                done += n as u64;
            }
        })
        .expect("spawn null output");
    Output {
        prod,
        shared,
        rate,
        channels,
        written: 0,
        keep: Keep::Null(stop),
    }
}

fn open_cpal(want_rate: u32, want_channels: usize, volume: f32) -> Result<Output> {
    let host = cpal::default_host();
    let device = host
        .default_output_device()
        .ok_or_else(|| anyhow!("no default output device"))?;

    // Prefer a native config matching the track so we neither resample nor
    // remix; fall back to the device default.
    let want_ch = want_channels.min(u16::MAX as usize) as u16;
    let mut chosen = None;
    if let Ok(ranges) = device.supported_output_configs() {
        let ranges: Vec<_> = ranges.collect();
        for ch in [want_ch, 2] {
            let mut cands: Vec<_> = ranges
                .iter()
                .filter(|r| {
                    r.channels() == ch
                        && r.min_sample_rate() <= want_rate
                        && want_rate <= r.max_sample_rate()
                })
                .cloned()
                .collect();
            cands.sort_by_key(|r| r.sample_format() != cpal::SampleFormat::F32);
            if let Some(r) = cands.into_iter().next() {
                chosen = Some(r.with_sample_rate(want_rate));
                break;
            }
        }
    }
    let supported = match chosen {
        Some(c) => c,
        None => device
            .default_output_config()
            .context("default output config")?,
    };
    let rate = supported.sample_rate();
    let channels = supported.channels() as usize;
    let format = supported.sample_format();
    let config: cpal::StreamConfig = supported.config();

    let (prod, cons) = ring(rate, channels);
    let shared = Arc::new(OutShared::new(volume));

    let stream = match format {
        cpal::SampleFormat::F32 => build::<f32>(&device, config, cons, shared.clone()),
        cpal::SampleFormat::I16 => build::<i16>(&device, config, cons, shared.clone()),
        cpal::SampleFormat::I32 => build::<i32>(&device, config, cons, shared.clone()),
        cpal::SampleFormat::U16 => build::<u16>(&device, config, cons, shared.clone()),
        cpal::SampleFormat::F64 => build::<f64>(&device, config, cons, shared.clone()),
        other => Err(anyhow!("unsupported device sample format {other}")),
    }?;
    stream.play().context("starting output stream")?;
    tracing::info!(rate, channels, ?format, "opened output");
    Ok(Output {
        prod,
        shared,
        rate,
        channels,
        written: 0,
        keep: Keep::Cpal(stream),
    })
}

fn build<T>(
    device: &cpal::Device,
    config: cpal::StreamConfig,
    mut cons: Consumer<f32>,
    sh: Arc<OutShared>,
) -> Result<cpal::Stream>
where
    T: cpal::SizedSample + cpal::FromSample<f32>,
{
    let mut scratch: Vec<f32> = Vec::new();
    let stream = device.build_output_stream(
        config,
        move |data: &mut [T], _: &cpal::OutputCallbackInfo| {
            if scratch.len() < data.len() {
                scratch.resize(data.len(), 0.0);
            }
            let buf = &mut scratch[..data.len()];
            fill(buf, &mut cons, &sh);
            for (d, s) in data.iter_mut().zip(buf.iter()) {
                *d = T::from_sample(*s);
            }
        },
        |e| tracing::warn!("output stream error: {e}"),
        None,
    )?;
    Ok(stream)
}
