//! An HTTP/2 client driven by bytes and explicit calls, without owning I/O.
//!
//! Drain [`Session::output`] into the transport, reporting accepted bytes with
//! [`Session::advance_output`]. Fill [`Session::input`] from the transport and
//! call [`Session::drive`], then drain [`Session::next_event`]. Repeat while
//! progress is possible; wait for transport readiness otherwise. There is no
//! background task, socket, runtime, or timer. Request deadlines belong to the
//! caller; cancel expired streams with [`Session::reset`]. Server push is disabled.
//!
//! The existing HTTP/2 state machine, HPACK codec, flow control and protocol
//! checks are shared with the async client. Internal stream handles still use
//! the existing synchronized store; this API does not promise allocation-free
//! stream creation.
use super::{Builder, Peer, ResponseFuture, SendRequest};
use crate::codec::{raw::Raw, FrameIo, UserError};
use crate::{proto, Error, Reason, RecvStream, SendStream, StreamId};
use bytes::{Bytes, BytesMut};
use fnv::FnvHashMap;
use http::{HeaderMap, Request, Response};
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{Context, Wake, Waker};
use std::time::Instant;

/// A received response event. DATA capacity is released explicitly.
#[derive(Debug)]
pub enum Event {
    /// Informational (1xx) response headers.
    Informational {
        /// The request stream.
        stream: StreamId,
        /// The response head.
        response: Response<()>,
    },
    /// Final response headers.
    Headers {
        /// The request stream.
        stream: StreamId,
        /// The response head.
        response: Response<()>,
    },
    /// Response bytes; call `release_capacity` when consumed.
    Data {
        /// The request stream.
        stream: StreamId,
        /// Received bytes, shared with the input buffer.
        bytes: Bytes,
    },
    /// End of the response, including optional trailers.
    End {
        /// The request stream.
        stream: StreamId,
        /// Optional trailing fields.
        trailers: Option<HeaderMap>,
    },
    /// A stream failed. Other streams may continue.
    Failed {
        /// The request stream.
        stream: StreamId,
        /// The stream error.
        error: Error,
    },
}

#[derive(Debug)]
enum Receiving {
    Head(ResponseFuture),
    Body(RecvStream),
}
#[derive(Debug)]
struct Exchange {
    receiving: Receiving,
    send: SendStream<Bytes>,
    unconsumed: usize,
    done: bool,
    sent: bool,
}

#[derive(Debug, Default)]
struct Progress(AtomicBool);
impl Wake for Progress {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.store(true, Ordering::Relaxed);
    }
}

/// One buffer-driven HTTP/2 client connection.
#[derive(Debug)]
pub struct Session {
    inner: proto::connection::Core<Raw<proto::Prioritized<Bytes>>, Peer, Bytes>,
    send: SendRequest<Bytes>,
    exchanges: FnvHashMap<u32, Exchange>,
    ready: VecDeque<u32>,
    progress: Arc<Progress>,
    waker: Waker,
    now: Instant,
    closed: bool,
    active: usize,
    peer_limit: usize,
    wire_available: usize,
    go_away: bool,
}

impl Session {
    /// Prepare a client preface and SETTINGS using the existing client builder.
    /// `now` is the caller's monotonic clock for reset retention.
    pub fn new(mut builder: Builder, now: Instant) -> Self {
        builder.enable_push(false);
        let mut codec = Raw::new();
        if let Some(max) = builder.settings.max_frame_size() {
            codec.set_max_recv_frame_size(max as usize);
        }
        if let Some(max) = builder.settings.max_header_list_size() {
            codec.set_max_recv_header_list_size(max as usize);
        }
        codec
            .buffer(builder.settings.clone().into())
            .expect("valid SETTINGS");
        let mut inner = proto::connection::Core::new(
            codec,
            proto::Config {
                next_stream_id: builder.stream_id,
                initial_max_send_streams: builder.initial_max_send_streams,
                max_send_buffer_size: builder.max_send_buffer_size,
                reset_stream_duration: builder.reset_stream_duration,
                reset_stream_max: builder.reset_stream_max,
                remote_reset_stream_max: builder.pending_accept_reset_stream_max,
                local_error_reset_streams_max: builder.local_max_error_reset_streams,
                settings: builder.settings,
                data_frame_budget: builder.data_frame_budget,
            },
        );
        if let Some(size) = builder.initial_target_connection_window_size {
            inner.set_target_window_size(size);
        }
        let mut streams = inner.streams().clone();
        streams.set_time(now);
        let send = SendRequest {
            inner: streams,
            pending: None,
        };
        let progress = Arc::new(Progress::default());
        let waker = Waker::from(progress.clone());
        Self {
            inner,
            send,
            exchanges: FnvHashMap::default(),
            ready: VecDeque::new(),
            progress,
            waker,
            now,
            closed: false,
            active: 0,
            peer_limit: 0,
            wire_available: 0,
            go_away: false,
        }
    }

    /// Buffer to fill directly from the transport. Do not append after EOF.
    pub fn input(&mut self) -> &mut BytesMut {
        self.inner.codec_mut().input()
    }
    /// Notify the session that the transport's read half has ended.
    pub fn end_input(&mut self) {
        self.inner.codec_mut().end_input();
    }
    /// Next contiguous output segment. This may borrow a DATA payload directly.
    pub fn output(&mut self) -> &[u8] {
        self.inner.codec_mut().output_chunk()
    }
    /// Report bytes accepted by the transport from the last output segment.
    ///
    /// # Panics
    /// Panics if `count` exceeds the available output segment.
    pub fn advance_output(&mut self, count: usize) {
        assert!(count <= self.output().len());
        self.inner.codec_mut().advance_output(count);
    }
    /// Whether the peer's initial SETTINGS have been processed.
    pub fn is_ready(&self) -> bool {
        self.inner.received_settings() && !self.closed && !self.go_away
    }
    /// Peer's current limit on client-initiated concurrent streams.
    pub fn max_streams(&self) -> usize {
        self.peer_limit
    }
    /// Whether another request can be opened without queuing behind the peer limit.
    pub fn can_open(&self) -> bool {
        self.available_streams() != 0
    }

    /// Requests that can be submitted without exceeding the peer's stream limit.
    /// The value reflects protocol progress and submissions, not socket readiness.
    pub fn available_streams(&self) -> usize {
        if !self.is_ready() {
            return 0;
        }
        self.peer_limit
            .saturating_sub(self.active)
            .min(self.wire_available)
    }

    /// Open a request. Returns `None` while waiting for SETTINGS or stream capacity.
    /// No stream or request is queued in that case.
    pub fn send_request(
        &mut self,
        request: Request<()>,
        end_stream: bool,
    ) -> Result<Option<StreamId>, Error> {
        if !self.can_open() {
            return Ok(None);
        }
        let mut cx = Context::from_waker(&self.waker);
        match self.send.poll_ready(&mut cx) {
            std::task::Poll::Pending => return Ok(None),
            std::task::Poll::Ready(result) => result?,
        }
        let (response, send) = self.send.send_request(request, end_stream)?;
        let id = response.stream_id();
        self.exchanges.insert(
            id.as_u32(),
            Exchange {
                receiving: Receiving::Head(response),
                send,
                unconsumed: 0,
                done: false,
                sent: end_stream,
            },
        );
        self.active += 1;
        self.wire_available -= 1;
        Ok(Some(id))
    }
    /// Queue body bytes. HTTP/2 flow control controls actual wire transmission.
    /// Callers streaming large bodies must bound how much they queue.
    pub fn send_data(
        &mut self,
        stream: StreamId,
        bytes: Bytes,
        end_stream: bool,
    ) -> Result<(), Error> {
        let exchange = self
            .exchanges
            .get_mut(&stream.as_u32())
            .ok_or(UserError::InactiveStreamId)?;
        exchange.send.send_data(bytes, end_stream)?;
        if end_stream && !exchange.sent {
            exchange.sent = true;
            if exchange.done {
                self.active -= 1;
            }
        }
        if exchange.done && exchange.sent && exchange.unconsumed == 0 {
            self.exchanges.remove(&stream.as_u32());
        }
        Ok(())
    }
    /// Begin GOAWAY shutdown. Unfinished streams are cancelled with `reason`.
    /// Continue driving and draining output until `drive` reports completion.
    pub fn close(&mut self, reason: Reason) {
        self.go_away = true;
        self.inner.go_away_from_user(reason);
    }

    /// Cancel a request without closing the connection or affecting other streams.
    pub fn reset(&mut self, stream: StreamId, reason: Reason) {
        if let Some(mut exchange) = self.exchanges.remove(&stream.as_u32()) {
            if !exchange.done || !exchange.sent {
                self.active -= 1;
            }
            exchange.send.send_reset(reason);
        }
    }
    /// Return receive-window credit after consuming a DATA event.
    pub fn release_capacity(&mut self, stream: StreamId, count: usize) -> Result<(), Error> {
        let exchange = self
            .exchanges
            .get_mut(&stream.as_u32())
            .ok_or(UserError::InactiveStreamId)?;
        if count > exchange.unconsumed {
            return Err(UserError::ReleaseCapacityTooBig.into());
        }
        if let Receiving::Body(body) = &mut exchange.receiving {
            body.flow_control().release_capacity(count)?;
        }
        exchange.unconsumed -= count;
        // The body handle remains until credit has been returned, even after End.
        if exchange.unconsumed == 0 && exchange.done && exchange.sent {
            self.exchanges.remove(&stream.as_u32());
        }
        Ok(())
    }
    /// Advance protocol work, using the supplied monotonic time. Returns `true`
    /// when the connection has finished. Transport readiness drives further calls.
    /// A connection error applies to all unfinished requests.
    ///
    /// # Panics
    /// Panics if `now` goes backwards.
    pub fn drive(&mut self, now: Instant) -> Result<bool, Error> {
        assert!(now >= self.now, "session time must be monotonic");
        self.now = now;
        self.send.inner.set_time(now);
        if self.closed {
            return Ok(true);
        }
        loop {
            self.progress.0.store(false, Ordering::Relaxed);
            let mut cx = Context::from_waker(&self.waker);
            let result = self.inner.poll(&mut cx);
            // Only protocol progress changes the peer's wire window. Cache it
            // once here instead of taking a store mutex in every can_open.
            (self.peer_limit, self.wire_available) = self.send.inner.send_window();
            let codec = self.inner.codec_mut();
            self.ready.append(&mut codec.received);
            if codec.go_away && !self.go_away {
                self.go_away = true;
                self.ready.extend(self.exchanges.keys().copied());
            }
            if let std::task::Poll::Ready(result) = result {
                self.closed = true;
                self.ready.extend(self.exchanges.keys().copied());
                result.map_err(Error::from)?;
                return Ok(true);
            }
            if !self.progress.0.swap(false, Ordering::Relaxed) {
                return Ok(false);
            }
        }
    }

    /// Pop a response event. Drain events before waiting for more input.
    pub fn next_event(&mut self) -> Option<Event> {
        while let Some(id) = self.ready.pop_front() {
            let exchange = match self.exchanges.get_mut(&id) {
                Some(exchange) => exchange,
                None => continue,
            };
            if exchange.done {
                continue;
            }
            let stream = StreamId::from_internal(id.into());
            let mut cx = Context::from_waker(&self.waker);
            let event = match &mut exchange.receiving {
                Receiving::Head(response) => {
                    if let std::task::Poll::Ready(Some(result)) =
                        response.poll_informational(&mut cx)
                    {
                        Some(result.map(|response| Event::Informational { stream, response }))
                    } else {
                        match Pin::new(response).poll(&mut cx) {
                            std::task::Poll::Ready(Ok(response)) => {
                                let (head, body) = response.into_parts();
                                exchange.receiving = Receiving::Body(body);
                                Some(Ok(Event::Headers {
                                    stream,
                                    response: Response::from_parts(head, ()),
                                }))
                            }
                            std::task::Poll::Ready(Err(error)) => Some(Err(error)),
                            std::task::Poll::Pending => None,
                        }
                    }
                }
                Receiving::Body(body) => match body.poll_data(&mut cx) {
                    std::task::Poll::Ready(Some(Ok(bytes))) => {
                        exchange.unconsumed += bytes.len();
                        Some(Ok(Event::Data { stream, bytes }))
                    }
                    std::task::Poll::Ready(Some(Err(error))) => Some(Err(error)),
                    std::task::Poll::Pending => None,
                    std::task::Poll::Ready(None) => match body.poll_trailers(&mut cx) {
                        std::task::Poll::Ready(Ok(trailers)) => {
                            Some(Ok(Event::End { stream, trailers }))
                        }
                        std::task::Poll::Ready(Err(error)) => Some(Err(error)),
                        std::task::Poll::Pending => None,
                    },
                },
            };
            match event {
                Some(Ok(event)) => {
                    if matches!(event, Event::End { .. }) {
                        exchange.done = true;
                        if exchange.sent {
                            self.active -= 1;
                        }
                        // Keep FlowControl alive until outstanding data is released.
                        if exchange.unconsumed == 0 && exchange.sent {
                            self.exchanges.remove(&id);
                        }
                    } else {
                        self.ready.push_front(id);
                    }
                    return Some(event);
                }
                Some(Err(error)) => {
                    if !exchange.done || !exchange.sent {
                        self.active -= 1;
                    }
                    self.exchanges.remove(&id);
                    return Some(Event::Failed { stream, error });
                }
                None => (),
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::encode::Encoder;
    use crate::frame::{self, Frame};
    use bytes::Buf;

    fn wire(frame: Frame) -> Vec<u8> {
        let mut encoder = Encoder::new(true);
        encoder.buffer(frame).unwrap();
        let mut bytes = Vec::new();
        loop {
            while encoder.output().has_remaining() {
                let mut output = encoder.output();
                let len = output.chunk().len();
                bytes.extend_from_slice(output.chunk());
                output.advance(len);
            }
            if matches!(
                encoder.unset_frame(),
                crate::codec::encode::ControlFlow::Break
            ) {
                break;
            }
        }
        bytes
    }
    fn drain(session: &mut Session) -> Vec<u8> {
        let mut bytes = Vec::new();
        loop {
            session.drive(session.now).unwrap();
            let len = session.output().len();
            if len == 0 {
                return bytes;
            }
            bytes.extend_from_slice(session.output());
            // Exercise partial output, including the client preface.
            let count = len.min(1);
            bytes.truncate(bytes.len() - len + count);
            session.advance_output(count);
        }
    }
    fn ready(limit: u32) -> Session {
        let mut session = Session::new(Builder::new(), Instant::now());
        assert!(!session.is_ready());
        let output = drain(&mut session);
        assert!(output.starts_with(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"));
        let mut settings = frame::Settings::default();
        settings.set_max_concurrent_streams(Some(limit));
        let mut input = wire(settings.into());
        input.extend(wire(frame::Settings::ack().into()));
        // Arbitrary byte fragmentation, with a drive between each fragment.
        for byte in input {
            session.input().extend_from_slice(&[byte]);
            session.drive(session.now).unwrap();
        }
        drain(&mut session);
        assert!(session.is_ready());
        session
    }
    fn open(session: &mut Session, end: bool) -> StreamId {
        session
            .send_request(
                Request::builder()
                    .uri("http://example.com/")
                    .body(())
                    .unwrap(),
                end,
            )
            .unwrap()
            .unwrap()
    }
    fn headers(id: StreamId, end: bool) -> Vec<u8> {
        let mut headers = frame::Headers::new(
            id.as_u32().into(),
            frame::Pseudo::response(http::StatusCode::OK),
            HeaderMap::new(),
        );
        if end {
            headers.set_end_stream();
        }
        wire(headers.into())
    }

    #[test]
    fn peer_limit_responses_and_credit_without_runtime() {
        let mut session = ready(1);
        let id = open(&mut session, true);
        assert!(!session.can_open());
        drain(&mut session);
        let mut response = headers(id, false);
        response.extend(wire(
            frame::Data::new(id.as_u32().into(), Bytes::from_static(b"ok")).into(),
        ));
        let mut last = frame::Data::new(id.as_u32().into(), Bytes::new());
        last.set_end_stream(true);
        response.extend(wire(last.into()));
        for byte in response {
            session.input().extend_from_slice(&[byte]);
            session.drive(session.now).unwrap();
        }
        assert!(matches!(session.next_event(), Some(Event::Headers { .. })));
        assert!(
            matches!(session.next_event(), Some(Event::Data { ref bytes, .. }) if bytes.as_ref() == b"ok")
        );
        // The credit handle must survive End until the application consumes DATA.
        while let Some(event) = session.next_event() {
            if matches!(event, Event::End { .. }) {
                break;
            }
        }
        assert!(session.next_event().is_none());
        session.release_capacity(id, 2).unwrap();
        assert!(!session.exchanges.contains_key(&id.as_u32()));
        assert!(session.can_open());
        drain(&mut session);
    }

    #[test]
    fn reset_isolated_and_followup_request_completes() {
        let mut session = ready(2);
        let first = open(&mut session, true);
        let second = open(&mut session, true);
        drain(&mut session);
        session.reset(first, Reason::CANCEL);
        session.input().extend_from_slice(&headers(second, true));
        session.drive(session.now).unwrap();
        assert!(
            matches!(session.next_event(), Some(Event::Headers { stream, .. }) if stream == second)
        );
        assert!(
            matches!(session.next_event(), Some(Event::End { stream, .. }) if stream == second)
        );
        assert!(session.next_event().is_none());
        drain(&mut session);
        session
            .drive(
                session.now + std::time::Duration::from_secs(proto::DEFAULT_RESET_STREAM_SECS + 1),
            )
            .unwrap();
    }

    #[test]
    fn informational_headers_and_trailers_keep_their_event_order() {
        let mut session = ready(1);
        let id = open(&mut session, true);
        drain(&mut session);
        let early = frame::Headers::new(
            id.as_u32().into(),
            frame::Pseudo::response(http::StatusCode::EARLY_HINTS),
            HeaderMap::new(),
        );
        session.input().extend_from_slice(&wire(early.into()));
        session.input().extend_from_slice(&headers(id, false));
        let mut fields = HeaderMap::new();
        fields.insert("x-finished", http::HeaderValue::from_static("yes"));
        let trailers = frame::Headers::trailers(id.as_u32().into(), fields);
        session.input().extend_from_slice(&wire(trailers.into()));
        session.drive(session.now).unwrap();
        assert!(
            matches!(session.next_event(), Some(Event::Informational { response, .. }) if response.status() == http::StatusCode::EARLY_HINTS)
        );
        assert!(
            matches!(session.next_event(), Some(Event::Headers { response, .. }) if response.status() == http::StatusCode::OK)
        );
        assert!(
            matches!(session.next_event(), Some(Event::End { trailers: Some(fields), .. }) if fields["x-finished"] == "yes")
        );
        assert!(session.next_event().is_none());
        drain(&mut session);
    }

    #[test]
    fn out_of_order_responses_and_settings_changes() {
        let mut session = ready(2);
        let first = open(&mut session, true);
        let second = open(&mut session, true);
        drain(&mut session);
        session.input().extend_from_slice(&headers(second, true));
        session.drive(session.now).unwrap();
        assert!(
            matches!(session.next_event(), Some(Event::Headers { stream, .. }) if stream == second)
        );
        assert!(
            matches!(session.next_event(), Some(Event::End { stream, .. }) if stream == second)
        );
        let mut settings = frame::Settings::default();
        settings.set_max_concurrent_streams(Some(0));
        session.input().extend_from_slice(&wire(settings.into()));
        session.drive(session.now).unwrap();
        assert!(!session.can_open());
        assert_eq!(session.max_streams(), 0);
        drain(&mut session);
        let mut settings = frame::Settings::default();
        settings.set_max_concurrent_streams(Some(2));
        session.input().extend_from_slice(&wire(settings.into()));
        session.input().extend_from_slice(&headers(first, true));
        session.drive(session.now).unwrap();
        assert!(
            matches!(session.next_event(), Some(Event::Headers { stream, .. }) if stream == first)
        );
        assert!(matches!(session.next_event(), Some(Event::End { stream, .. }) if stream == first));
        assert!(session.can_open());
        drain(&mut session);
    }

    #[test]
    fn remote_reset_and_goaway() {
        let mut session = ready(2);
        let first = open(&mut session, true);
        let second = open(&mut session, true);
        drain(&mut session);
        session.input().extend_from_slice(&wire(
            frame::Reset::new(first.as_u32().into(), Reason::REFUSED_STREAM).into(),
        ));
        session.input().extend_from_slice(&wire(
            frame::GoAway::new(first.as_u32().into(), Reason::NO_ERROR).into(),
        ));
        session.drive(session.now).unwrap();
        assert!(!session.can_open());
        let mut failed = Vec::new();
        while let Some(event) = session.next_event() {
            if let Event::Failed { stream, .. } = event {
                failed.push(stream);
            }
        }
        assert!(failed.contains(&first));
        assert!(failed.contains(&second));
        drain(&mut session);
    }

    #[test]
    fn eof_truncation_is_an_error() {
        let mut session = ready(1);
        let _id = open(&mut session, true);
        drain(&mut session);
        session.input().extend_from_slice(&[0, 0, 4]);
        session.end_input();
        let error = session.drive(session.now).unwrap_err();
        assert_eq!(
            error.get_io().unwrap().kind(),
            std::io::ErrorKind::UnexpectedEof
        );
    }

    #[test]
    fn oversized_frame_rejected_without_waiting_for_payload() {
        let mut session = ready(1);
        let _id = open(&mut session, true);
        drain(&mut session);
        session
            .input()
            .extend_from_slice(&[0, 64, 1, 0, 0, 0, 0, 0, 1]);
        // The GOAWAY must be drained before a graceful connection error resolves.
        let _ = session.drive(session.now);
        let mut output = Vec::new();
        loop {
            let len = session.output().len();
            if len == 0 {
                break;
            }
            output.extend_from_slice(session.output());
            session.advance_output(len);
            if let Err(error) = session.drive(session.now) {
                assert_eq!(error.reason(), Some(Reason::FRAME_SIZE_ERROR));
                return;
            }
        }
        panic!("oversized frame must fail after sending GOAWAY: {output:?}");
    }

    #[test]
    fn large_headers_continue_and_repeated_empty_drive_is_quiet() {
        let mut session = ready(1);
        let id = open(&mut session, true);
        drain(&mut session);
        let mut fields = HeaderMap::new();
        fields.insert(
            "x-large",
            http::HeaderValue::from_str(&"0123456789".repeat(4000)).unwrap(),
        );
        let mut head = frame::Headers::new(
            id.as_u32().into(),
            frame::Pseudo::response(http::StatusCode::OK),
            fields,
        );
        head.set_end_stream();
        let input = wire(head.into());
        assert!(input.len() > frame::DEFAULT_MAX_FRAME_SIZE as usize);
        for chunk in input.chunks(7) {
            session.input().extend_from_slice(chunk);
            session.drive(session.now).unwrap();
        }
        assert!(
            matches!(session.next_event(), Some(Event::Headers { response, .. }) if response.headers()["x-large"].len() == 40000)
        );
        assert!(matches!(session.next_event(), Some(Event::End { .. })));
        drain(&mut session);
        for _ in 0..10 {
            assert!(!session.drive(session.now).unwrap());
            assert!(session.output().is_empty());
            assert!(session.next_event().is_none());
        }
    }
}
