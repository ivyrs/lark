//! Pull-based HTTP media source for Symphonia's synchronous decoding thread.
//! The daemon's Tokio runtime drives socket I/O; only the decoder thread waits.
//! There is no download cache or producer queue. Besides HTTP/Symphonia's own
//! buffers, we retain only the unread part of one HTTP body chunk.

use std::future::Future;
use std::io::{self, Read, Seek, SeekFrom};
use std::time::Duration;

use bytes::Bytes;
use reqwest::header::{
    HeaderMap, HeaderName, HeaderValue, ACCEPT_ENCODING, ACCEPT_RANGES, CONTENT_ENCODING,
    CONTENT_RANGE, ETAG, IF_RANGE, LAST_MODIFIED, RANGE,
};
use reqwest::{Client, Response, StatusCode, Url};
use symphonia::core::io::MediaSource;
use tokio::runtime::Handle;
use tokio::sync::watch;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const IO_TIMEOUT: Duration = Duration::from_secs(10);

pub(crate) type Playback = watch::Receiver<()>;

pub(crate) fn cancelled(playback: &Playback) -> bool {
    playback.has_changed().unwrap_or(true)
}

fn cancel_error() -> io::Error {
    // Do not use Interrupted: Read::read_exact automatically retries it.
    io::Error::new(io::ErrorKind::ConnectionAborted, "HTTP audio cancelled")
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

fn unseekable() -> io::Error {
    io::Error::new(
        io::ErrorKind::Unsupported,
        "HTTP audio does not support byte ranges",
    )
}

/// Never attach a reqwest error as a cause. URL paths, query strings and headers
/// can all carry credentials, and errors can be shown outside the daemon.
fn request_error(error: reqwest::Error) -> io::Error {
    if error.is_timeout() {
        io::Error::new(io::ErrorKind::TimedOut, "HTTP audio request timed out")
    } else if error.is_connect() {
        io::Error::new(io::ErrorKind::NotConnected, "HTTP audio connection failed")
    } else {
        io::Error::other("HTTP audio transfer failed")
    }
}

fn wait<T>(
    runtime: &Handle,
    playback: &Playback,
    timeout: Duration,
    io: impl Future<Output = T>,
) -> io::Result<T> {
    let mut playback = playback.clone();
    runtime.block_on(async {
        tokio::select! {
            biased;
            _ = playback.changed() => Err(cancel_error()),
            r = tokio::time::timeout(timeout, io) => {
                r.map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "HTTP audio read timed out"))
            }
        }
    })
}

pub(crate) struct HttpTransport {
    runtime: Handle,
    client: Option<Client>,
    timeout: Duration,
}

impl HttpTransport {
    pub(crate) fn new(runtime: Handle) -> Self {
        Self {
            runtime,
            client: None,
            timeout: IO_TIMEOUT,
        }
    }

    pub(crate) fn open(
        &mut self,
        url: &str,
        headers: &[(String, String)],
        playback: &Playback,
    ) -> io::Result<HttpSource> {
        let url = Url::parse(url).map_err(|_| invalid("invalid HTTP audio URL"))?;
        if !matches!(url.scheme(), "http" | "https")
            || !url.has_host()
            || !url.username().is_empty()
            || url.password().is_some()
            || url.fragment().is_some()
        {
            return Err(invalid(
                "HTTP audio requires an HTTP(S) URL without userinfo or a fragment",
            ));
        }
        let headers = request_headers(headers)?;
        if self.client.is_none() {
            // Ring avoids a build-time dependency on CMake. Reqwest still uses
            // rustls's normal platform certificate and hostname verification.
            let _ = rustls::crypto::ring::default_provider().install_default();
            self.client = Some(
                Client::builder()
                    .connect_timeout(CONNECT_TIMEOUT)
                    .redirect(reqwest::redirect::Policy::none())
                    .retry(reqwest::retry::never())
                    // Audio URLs may carry credentials. Do not implicitly send
                    // them through a system/environment proxy or to redirects.
                    .no_proxy()
                    .pool_max_idle_per_host(0)
                    .build()
                    .map_err(|_| io::Error::other("cannot initialize HTTP audio client"))?,
            );
        }
        let mut source = HttpSource {
            runtime: self.runtime.clone(),
            client: self.client.as_ref().unwrap().clone(),
            timeout: self.timeout,
            playback: playback.clone(),
            url,
            headers,
            length: None,
            seekable: false,
            validator: None,
            pos: 0,
            response: None,
            chunk: Bytes::new(),
        };
        let response = source.request(None)?;
        source.length = response.content_length();
        source.seekable = source.length.is_some()
            && response
                .headers()
                .get_all(ACCEPT_RANGES)
                .iter()
                .filter_map(|value| value.to_str().ok())
                .flat_map(|value| value.split(','))
                .any(|unit| unit.trim().eq_ignore_ascii_case("bytes"));
        source.validator = response
            .headers()
            .get(ETAG)
            .filter(|etag| etag.as_bytes().starts_with(b"\""))
            .or_else(|| response.headers().get(LAST_MODIFIED))
            .cloned();
        source.response = Some(response);
        Ok(source)
    }
}

fn request_headers(headers: &[(String, String)]) -> io::Result<HeaderMap> {
    let mut result = HeaderMap::new();
    for (name, value) in headers {
        let name = HeaderName::from_bytes(name.as_bytes())
            .map_err(|_| invalid("invalid HTTP audio header name"))?;
        if matches!(
            name.as_str(),
            "accept-encoding"
                | "connection"
                | "content-length"
                | "host"
                | "if-range"
                | "keep-alive"
                | "proxy-authorization"
                | "proxy-connection"
                | "range"
                | "te"
                | "trailer"
                | "transfer-encoding"
                | "upgrade"
        ) {
            return Err(invalid("HTTP audio header is reserved by the transport"));
        }
        let mut value =
            HeaderValue::from_str(value).map_err(|_| invalid("invalid HTTP audio header value"))?;
        value.set_sensitive(true);
        result.append(name, value);
    }
    result.insert(ACCEPT_ENCODING, HeaderValue::from_static("identity"));
    Ok(result)
}

pub(crate) struct HttpSource {
    runtime: Handle,
    client: Client,
    timeout: Duration,
    playback: Playback,
    url: Url,
    headers: HeaderMap,
    length: Option<u64>,
    seekable: bool,
    validator: Option<HeaderValue>,
    pos: u64,
    response: Option<Response>,
    chunk: Bytes,
}

impl HttpSource {
    fn request(&self, start: Option<u64>) -> io::Result<Response> {
        let mut req = self
            .client
            .get(self.url.clone())
            .headers(self.headers.clone());
        if let Some(start) = start {
            req = req.header(RANGE, format!("bytes={start}-"));
            if let Some(validator) = &self.validator {
                req = req.header(IF_RANGE, validator.clone());
            }
        }
        let response = wait(&self.runtime, &self.playback, self.timeout, req.send())?
            .map_err(request_error)?;
        let status = response.status();
        if start.is_some() && status == StatusCode::OK {
            // A server may ignore Range, or If-Range signalled a changed resource.
            return Err(unseekable());
        }
        let expected = if start.is_some() {
            StatusCode::PARTIAL_CONTENT
        } else {
            StatusCode::OK
        };
        if status != expected {
            return Err(io::Error::other(format!(
                "HTTP audio returned status {}",
                status.as_u16()
            )));
        }
        if response
            .headers()
            .get_all(CONTENT_ENCODING)
            .iter()
            .any(|value| !value.as_bytes().eq_ignore_ascii_case(b"identity"))
        {
            return Err(invalid("encoded HTTP audio response is unsupported"));
        }
        if let Some(start) = start {
            let range = response
                .headers()
                .get(CONTENT_RANGE)
                .and_then(|raw| raw.to_str().ok())
                .and_then(parse_content_range);
            let valid = range.is_some_and(|(first, last, total)| {
                Some(total) == self.length
                    && first == start
                    && last.checked_add(1) == Some(total)
                    && first <= last
                    && response
                        .content_length()
                        .is_none_or(|len| len == total - first)
            });
            if !valid {
                return Err(invalid("invalid HTTP audio Content-Range"));
            }
        } else if response.headers().contains_key(CONTENT_RANGE) {
            return Err(invalid("unexpected HTTP audio Content-Range"));
        }
        Ok(response)
    }
}

fn parse_content_range(value: &str) -> Option<(u64, u64, u64)> {
    let (unit, rest) = value.split_once(' ')?;
    if !unit.eq_ignore_ascii_case("bytes") {
        return None;
    }
    let (span, total) = rest.split_once('/')?;
    let (first, last) = span.split_once('-')?;
    let decimal = |s: &str| {
        s.as_bytes()
            .iter()
            .all(u8::is_ascii_digit)
            .then(|| s.parse().ok())
            .flatten()
    };
    Some((decimal(first)?, decimal(last)?, decimal(total)?))
}

impl Read for HttpSource {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        if cancelled(&self.playback) {
            return Err(cancel_error());
        }
        while self.chunk.is_empty() {
            let Some(response) = self.response.as_mut() else {
                return Ok(0);
            };
            match wait(
                &self.runtime,
                &self.playback,
                self.timeout,
                response.chunk(),
            )?
            .map_err(request_error)?
            {
                Some(chunk) => {
                    if self
                        .length
                        .is_some_and(|len| chunk.len() as u64 > len - self.pos)
                    {
                        return Err(invalid("HTTP audio response exceeded its declared length"));
                    }
                    self.chunk = chunk;
                }
                None => {
                    self.response = None;
                    if self.length.is_some_and(|len| self.pos != len) {
                        return Err(invalid(
                            "HTTP audio response ended before its declared length",
                        ));
                    }
                    return Ok(0);
                }
            }
        }
        let count = buf.len().min(self.chunk.len());
        buf[..count].copy_from_slice(&self.chunk.split_to(count));
        self.pos += count as u64;
        Ok(count)
    }
}

impl Seek for HttpSource {
    fn seek(&mut self, to: SeekFrom) -> io::Result<u64> {
        if cancelled(&self.playback) {
            return Err(cancel_error());
        }
        let pos = match to {
            SeekFrom::Start(n) => Some(n),
            SeekFrom::Current(n) => self.pos.checked_add_signed(n),
            SeekFrom::End(n) => self.length.and_then(|len| len.checked_add_signed(n)),
        }
        .ok_or_else(|| invalid("invalid HTTP audio seek offset"))?;
        if pos != self.pos {
            if !self.seekable {
                return Err(unseekable());
            }
            let length = self.length.expect("seekable response has a length");
            if pos > length {
                return Err(invalid("HTTP audio seek exceeds response length"));
            }
            self.response = if pos == length {
                None
            } else {
                Some(self.request(Some(pos))?)
            };
            self.chunk = Bytes::new();
            self.pos = pos;
        }
        Ok(pos)
    }
}

impl MediaSource for HttpSource {
    fn is_seekable(&self) -> bool {
        self.seekable
    }

    fn byte_len(&self) -> Option<u64> {
        self.length
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{config::OutputKind, engine};
    use lark_protocol::Stream;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};
    use tokio::sync::mpsc;
    use tokio::time::timeout;

    const TEST_LIMIT: Duration = Duration::from_secs(3);

    async fn server() -> (TcpListener, String) {
        let socket = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let url = format!(
            "http://{}/audio?token=secret-url",
            socket.local_addr().unwrap()
        );
        (socket, url)
    }

    async fn accept(listener: &TcpListener) -> (TcpStream, String) {
        let (mut peer, _) = timeout(TEST_LIMIT, listener.accept())
            .await
            .unwrap()
            .unwrap();
        let mut req = Vec::new();
        timeout(TEST_LIMIT, async {
            while !req.ends_with(b"\r\n\r\n") {
                assert!(req.len() < 4096);
                req.push(peer.read_u8().await.unwrap());
            }
        })
        .await
        .unwrap();
        (peer, String::from_utf8(req).unwrap().to_ascii_lowercase())
    }

    fn auth() -> Vec<(String, String)> {
        vec![("Authorization".into(), "Bearer secret-header".into())]
    }

    fn assert_auth(req: &str) {
        assert!(req.starts_with("get /audio?token=secret-url http/1.1\r\n"));
        assert!(req.contains("\r\nauthorization: bearer secret-header\r\n"));
        assert!(req.contains("\r\naccept-encoding: identity\r\n"));
    }

    fn wav(frames: u32) -> Vec<u8> {
        let mut wav = b"RIFF".to_vec();
        wav.extend((36 + frames * 2).to_le_bytes());
        wav.extend(b"WAVEfmt \x10\0\0\0\x01\0\x01\0"); // PCM mono
        wav.extend(8_000u32.to_le_bytes());
        wav.extend(16_000u32.to_le_bytes());
        wav.extend(b"\x02\0\x10\0data");
        wav.extend((frames * 2).to_le_bytes());
        wav.resize(wav.len() + frames as usize * 2, 0);
        wav
    }

    async fn eventually(mut done: impl FnMut() -> bool) {
        timeout(TEST_LIMIT, async {
            while !done() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
    }

    async fn disconnected(mut peer: TcpStream) {
        assert_eq!(
            timeout(TEST_LIMIT, peer.read(&mut [0]))
                .await
                .unwrap()
                .unwrap(),
            0
        );
    }

    #[tokio::test]
    async fn starts_before_chunked_response_finishes_and_stop_interrupts_read() {
        let (listener, url) = server().await;
        let (events, mut rx) = mpsc::unbounded_channel();
        let engine = engine::spawn(OutputKind::Null, events);
        engine.send(engine::Cmd::Play {
            id: 7,
            stream: Stream::Http {
                url,
                headers: auth(),
            },
            start_ms: 0,
        });
        let (mut peer, req) = accept(&listener).await;
        assert_auth(&req);
        let wav = wav(32_000);
        let prefix = &wav[..3000]; // WAV headers + one Symphonia packet, not the complete file.
        peer.write_all(
            format!(
                "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n{:x}\r\n",
                prefix.len()
            )
            .as_bytes(),
        )
        .await
        .unwrap();
        peer.write_all(prefix).await.unwrap();
        peer.write_all(b"\r\n").await.unwrap();
        // The server deliberately retains the socket open and withholds even
        // the next chunk header. A whole-file download cannot pass this test.
        let event = timeout(TEST_LIMIT, rx.recv()).await.unwrap().unwrap();
        assert!(matches!(
            event.into_current(),
            Some(engine::Event::Started {
                id: 7,
                position_ms: 0,
                ..
            })
        ));
        eventually(|| engine.position().is_some_and(|(_, ms)| ms > 0)).await;
        engine.send(engine::Cmd::Stop);
        eventually(|| engine.position().is_none()).await;
        disconnected(peer).await;
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn replacement_cancels_stalled_headers_and_invalidates_queued_started() {
        let (listener, url) = server().await;
        let (events, mut rx) = mpsc::unbounded_channel();
        let engine = engine::spawn(OutputKind::Null, events);
        let stream = Stream::Http {
            url,
            headers: Vec::new(),
        };
        let play = |id, start_ms| {
            engine.send(engine::Cmd::Play {
                id,
                stream: stream.clone(),
                start_ms,
            })
        };
        play(1, 0);
        let (stalled, _) = accept(&listener).await;
        play(2, 1_000); // Non-range endpoints restart at zero rather than consuming arbitrarily many bytes.
        disconnected(stalled).await;
        let (mut next, _) = accept(&listener).await;
        let wav = wav(32_000);
        next.write_all(
            format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n", wav.len()).as_bytes(),
        )
        .await
        .unwrap();
        next.write_all(&wav[..3000]).await.unwrap();
        let started = timeout(TEST_LIMIT, rx.recv()).await.unwrap().unwrap();
        assert!(matches!(engine.position(), Some((2, _))));
        assert!(engine.position().unwrap().1 < 1_000);
        engine.send(engine::Cmd::Stop);
        assert!(started.into_current().is_none());
        disconnected(next).await;
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn validates_ranges_and_preserves_request_credentials_only_for_the_same_url() {
        let (listener, url) = server().await;
        let (_play, playback) = watch::channel(());
        let client = tokio::task::spawn_blocking(move || {
            let mut transport = HttpTransport::new(Handle::current());
            let mut src = transport.open(&url, &auth(), &playback).unwrap();
            assert!(src.is_seekable());
            assert_eq!(src.byte_len(), Some(8));
            assert_eq!(src.seek(SeekFrom::End(-4)).unwrap(), 4);
            let bad_seek = src.seek(SeekFrom::Start(0)).unwrap_err();
            assert_eq!(bad_seek.to_string(), "invalid HTTP audio Content-Range");
            let mut tail = Vec::new();
            src.read_to_end(&mut tail).unwrap();
            assert_eq!(tail, b"efgh");
        });
        let (mut initial, req) = accept(&listener).await;
        assert_auth(&req);
        assert!(!req.contains("\r\nrange:"));
        initial.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 8\r\nAccept-Ranges: bytes\r\nETag: \"version\"\r\n\r\nabcdefgh").await.unwrap();
        for (range, body) in [(4, b"efgh"), (0, b"bcde")] {
            let (mut peer, req) = accept(&listener).await;
            assert_auth(&req);
            assert!(req.contains(&format!("\r\nrange: bytes={range}-\r\n")));
            assert!(req.contains("\r\nif-range: \"version\"\r\n"));
            let first = if range == 4 { 4 } else { 1 };
            peer.write_all(format!("HTTP/1.1 206 Partial Content\r\nContent-Length: 4\r\nContent-Range: bytes {first}-7/8\r\n\r\n").as_bytes()).await.unwrap();
            peer.write_all(body).await.unwrap();
        }
        timeout(TEST_LIMIT, client).await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn redirects_are_not_followed_and_errors_do_not_leak_secrets() {
        let (listener, url) = server().await;
        let (target, redirect_url) = server().await;
        let (events, mut rx) = mpsc::unbounded_channel();
        let engine = engine::spawn(OutputKind::Null, events);
        engine.send(engine::Cmd::Play {
            id: 9,
            stream: Stream::Http {
                url,
                headers: auth(),
            },
            start_ms: 0,
        });
        let (mut peer, req) = accept(&listener).await;
        assert_auth(&req);
        peer.write_all(
            format!("HTTP/1.1 302 Found\r\nLocation: {redirect_url}\r\nContent-Length: 0\r\n\r\n")
                .as_bytes(),
        )
        .await
        .unwrap();
        let event = timeout(TEST_LIMIT, rx.recv())
            .await
            .unwrap()
            .unwrap()
            .into_current();
        let Some(engine::Event::Error { id: 9, message }) = event else {
            panic!("unexpected event: {event:?}")
        };
        assert_eq!(message, "HTTP audio returned status 302");
        assert!(timeout(Duration::from_millis(50), target.accept())
            .await
            .is_err());
    }

    #[tokio::test]
    async fn a_truncated_body_fails_instead_of_completing_the_track() {
        let (listener, url) = server().await;
        let (events, mut rx) = mpsc::unbounded_channel();
        let engine = engine::spawn(OutputKind::Null, events);
        engine.send(engine::Cmd::Play {
            id: 7,
            stream: Stream::Http {
                url,
                headers: auth(),
            },
            start_ms: 0,
        });
        let (mut peer, _) = accept(&listener).await;
        let wav = wav(32_000);
        peer.write_all(
            format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n", wav.len()).as_bytes(),
        )
        .await
        .unwrap();
        peer.write_all(&wav[..3000]).await.unwrap();
        let event = timeout(TEST_LIMIT, rx.recv())
            .await
            .unwrap()
            .unwrap()
            .into_current();
        assert!(matches!(event, Some(engine::Event::Started { id: 7, .. })));
        drop(peer);
        let event = timeout(TEST_LIMIT, rx.recv())
            .await
            .unwrap()
            .unwrap()
            .into_current();
        let Some(engine::Event::Error { id: 7, message }) = event else {
            panic!("unexpected event: {event:?}")
        };
        assert_eq!(
            message,
            "HTTP audio read failed: HTTP audio transfer failed"
        );
    }

    #[tokio::test]
    async fn request_headers_and_body_have_separate_stall_deadlines() {
        for stall_body in [false, true] {
            let (listener, url) = server().await;
            let (_play, playback) = watch::channel(());
            let client = tokio::task::spawn_blocking(move || {
                let mut transport = HttpTransport::new(Handle::current());
                transport.timeout = Duration::from_millis(500);
                let err = match transport.open(&url, &[], &playback) {
                    Ok(mut src) if stall_body => src.read(&mut [0]).unwrap_err(),
                    Err(e) if !stall_body => e,
                    _ => panic!("timed out in the wrong phase"),
                };
                assert_eq!(err.kind(), io::ErrorKind::TimedOut);
                assert!(!err.to_string().contains("secret"));
            });
            let (mut peer, _) = accept(&listener).await;
            if stall_body {
                peer.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\n")
                    .await
                    .unwrap();
            }
            timeout(TEST_LIMIT, client).await.unwrap().unwrap();
        }
    }
}
