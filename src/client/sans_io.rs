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
use super::{Builder, Peer, SendRequest};
use crate::codec::{raw::Raw, FrameIo, UserError};
use crate::{proto, Error, Reason, StreamId};
use bytes::{Bytes, BytesMut};
use fnv::FnvHashMap;
use http::{HeaderMap, Request, Response};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};
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
enum ReceivePhase {
    Head,
    Body,
}

/// One stream reference owns both halves until send, receive and credit finish.
#[derive(Debug)]
struct Exchange {
    stream: proto::StreamRef<Bytes>,
    phase: ReceivePhase,
    unconsumed: usize,
    done: bool,
    sent: bool,
}

impl Exchange {
    fn new(stream: proto::StreamRef<Bytes>, sent: bool) -> Self {
        Self {
            stream,
            phase: ReceivePhase::Head,
            unconsumed: 0,
            done: false,
            sent,
        }
    }

    fn poll_next(&mut self, stream: StreamId, cx: &Context<'_>) -> Poll<Result<Event, Error>> {
        match self.phase {
            ReceivePhase::Head => match ready!(self.stream.opaque_mut().poll_response_head(cx))? {
                proto::ResponseHead::Informational(response) => {
                    Poll::Ready(Ok(Event::Informational { stream, response }))
                }
                proto::ResponseHead::Final(response) => {
                    self.phase = ReceivePhase::Body;
                    Poll::Ready(Ok(Event::Headers { stream, response }))
                }
            },
            ReceivePhase::Body => match ready!(self.stream.opaque_mut().poll_response_body(cx))? {
                proto::ResponseBody::Data(bytes) => Poll::Ready(Ok(Event::Data { stream, bytes })),
                proto::ResponseBody::End(trailers) => {
                    Poll::Ready(Ok(Event::End { stream, trailers }))
                }
            },
        }
    }

    fn release_capacity(&mut self, count: usize) -> Result<(), Error> {
        if matches!(self.phase, ReceivePhase::Head) {
            return Ok(());
        }
        if count > proto::MAX_WINDOW_SIZE as usize {
            return Err(UserError::ReleaseCapacityTooBig.into());
        }
        self.stream
            .opaque_mut()
            .release_capacity(count as proto::WindowSize)
            .map_err(Into::into)
    }

    fn release_capacity_and_poll_next(
        &mut self,
        stream: StreamId,
        cx: &Context<'_>,
        count: usize,
    ) -> Result<Poll<Result<Event, Error>>, Error> {
        if count > proto::MAX_WINDOW_SIZE as usize {
            return Err(UserError::ReleaseCapacityTooBig.into());
        }
        if matches!(self.phase, ReceivePhase::Head) {
            return Ok(self.poll_next(stream, cx));
        }
        let next = self
            .stream
            .opaque_mut()
            .release_capacity_and_poll_response_body(cx, count as proto::WindowSize)?;
        Ok(next.map(|result| {
            result
                .map(|body| match body {
                    proto::ResponseBody::Data(bytes) => Event::Data { stream, bytes },
                    proto::ResponseBody::End(trailers) => Event::End { stream, trailers },
                })
                .map_err(Into::into)
        }))
    }
}

impl Drop for Exchange {
    fn drop(&mut self) {
        // Before final headers, keep ResponseFuture's drop behavior. Afterwards,
        // discard unread DATA as RecvStream did, then release the owned reference.
        if matches!(self.phase, ReceivePhase::Body) {
            self.stream.opaque_mut().clear_recv_buffer();
        }
    }
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
        // Retain the protocol stream directly instead of constructing separate
        // async ResponseFuture/SendStream handles and cloning a receive ref.
        let stream = self.send.send_request_stream(request, end_stream)?;
        let id = StreamId::from_internal(stream.stream_id());
        self.exchanges
            .insert(id.as_u32(), Exchange::new(stream, end_stream));
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
        exchange.stream.send_data(bytes, end_stream)?;
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
            exchange.stream.send_reset(reason);
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
        exchange.release_capacity(count)?;
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
            if codec.go_away && !self.go_away {
                self.go_away = true;
                codec.received.extend(self.exchanges.keys().copied());
            }
            if let std::task::Poll::Ready(result) = result {
                self.closed = true;
                codec.received.extend(self.exchanges.keys().copied());
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
        self.next_event_inner(None)
            .expect("no capacity release requested")
    }

    /// Return consumed DATA credit, then pop the next response event.
    ///
    /// Call this only after consuming or validating the previous DATA bytes.
    /// Credit is returned immediately, even if no next event is ready. When
    /// the same stream is at the front of the ready queue, credit return and
    /// receive polling share one stream-store operation.
    ///
    /// `count` may not exceed this stream's delivered, unreleased DATA bytes.
    /// A release error consumes neither the credit nor the next ready entry.
    /// Protocol failures are returned as [`Event::Failed`], as in [`Self::next_event`].
    pub fn release_capacity_and_next_event(
        &mut self,
        stream: StreamId,
        count: usize,
    ) -> Result<Option<Event>, Error> {
        if self.inner.codec_mut().received.front() == Some(&stream.as_u32()) {
            self.next_event_inner(Some(count))
        } else {
            self.release_capacity(stream, count)?;
            Ok(self.next_event())
        }
    }

    #[inline]
    fn next_event_inner(&mut self, mut credit: Option<usize>) -> Result<Option<Event>, Error> {
        while let Some(&id) = self.inner.codec_mut().received.front() {
            let exchange = match self.exchanges.get_mut(&id) {
                Some(exchange) => exchange,
                None => {
                    if credit.is_some() {
                        return Err(UserError::InactiveStreamId.into());
                    }
                    self.inner.codec_mut().received.pop_front();
                    continue;
                }
            };
            let stream = StreamId::from_internal(id.into());
            let cx = Context::from_waker(&self.waker);
            let next = if let Some(count) = credit.take() {
                if count > exchange.unconsumed {
                    return Err(UserError::ReleaseCapacityTooBig.into());
                }
                // End can precede credit return, with the stream retained until
                // the caller releases its last delivered bytes.
                if exchange.done {
                    exchange.release_capacity(count)?;
                    exchange.unconsumed -= count;
                    if exchange.unconsumed == 0 && exchange.sent {
                        self.exchanges.remove(&id);
                    }
                    self.inner.codec_mut().received.pop_front();
                    continue;
                }
                let next = exchange.release_capacity_and_poll_next(stream, &cx, count)?;
                exchange.unconsumed -= count;
                next
            } else {
                if exchange.done {
                    self.inner.codec_mut().received.pop_front();
                    continue;
                }
                exchange.poll_next(stream, &cx)
            };
            self.inner.codec_mut().received.pop_front();
            let event = match next {
                Poll::Ready(Ok(event)) => {
                    if let Event::Data { bytes, .. } = &event {
                        exchange.unconsumed += bytes.len();
                    }
                    Some(Ok(event))
                }
                Poll::Ready(Err(error)) => Some(Err(error)),
                Poll::Pending => None,
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
                        self.inner.codec_mut().received.push_front(id);
                    }
                    return Ok(Some(event));
                }
                Some(Err(error)) => {
                    if !exchange.done || !exchange.sent {
                        self.active -= 1;
                    }
                    self.exchanges.remove(&id);
                    return Ok(Some(Event::Failed { stream, error }));
                }
                None => (),
            }
        }
        Ok(None)
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
        ready_with_builder(limit, Builder::new())
    }
    fn ready_with_builder(limit: u32, builder: Builder) -> Session {
        let mut session = Session::new(builder, Instant::now());
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
    fn adjacent_frames_schedule_one_ready_entry_per_response() {
        let mut session = ready(8);
        let ids: Vec<_> = (0..8).map(|_| open(&mut session, true)).collect();
        drain(&mut session);
        for &id in &ids {
            session.input().extend_from_slice(&headers(id, false));
            let mut data = frame::Data::new(id.as_u32().into(), Bytes::from_static(b"ok"));
            data.set_end_stream(true);
            session.input().extend_from_slice(&wire(data.into()));
        }
        session.drive(session.now).unwrap();
        assert_eq!(session.inner.codec_mut().received.len(), ids.len());
        for id in ids {
            assert!(matches!(session.next_event(),
                Some(Event::Headers { stream, .. }) if stream == id));
            assert!(matches!(session.next_event(),
                Some(Event::Data { stream, bytes }) if stream == id && bytes == "ok"));
            session.release_capacity(id, 2).unwrap();
            assert!(matches!(session.next_event(),
                Some(Event::End { stream, trailers: None }) if stream == id));
        }
        // Completing a response must consume its only ready entry, without
        // needing another next_event call to skip stale frame notifications.
        assert!(session.inner.codec_mut().received.is_empty());
        assert!(session.next_event().is_none());
        assert_eq!(session.active, 0);
    }

    #[test]
    fn unread_ready_entry_survives_multiple_drives_without_duplicates() {
        let mut session = ready(1);
        let id = open(&mut session, true);
        drain(&mut session);
        session.input().extend_from_slice(&headers(id, false));
        session.drive(session.now).unwrap();
        assert_eq!(session.inner.codec_mut().received.len(), 1);
        let mut data = frame::Data::new(id.as_u32().into(), Bytes::from_static(b"ok"));
        data.set_end_stream(true);
        let input = wire(data.into());
        for byte in input {
            session.input().extend_from_slice(&[byte]);
            session.drive(session.now).unwrap();
            assert_eq!(session.inner.codec_mut().received.len(), 1);
        }
        assert!(matches!(session.next_event(), Some(Event::Headers { .. })));
        // A drive between events must not consume the requeued entry.
        session.drive(session.now).unwrap();
        assert_eq!(session.inner.codec_mut().received.len(), 1);
        assert!(matches!(session.next_event(),
            Some(Event::Data { bytes, .. }) if bytes == "ok"));
        session.release_capacity(id, 2).unwrap();
        assert!(matches!(session.next_event(), Some(Event::End { .. })));
        assert!(session.inner.codec_mut().received.is_empty());
        assert!(session.next_event().is_none());
    }

    #[test]
    fn interleaved_streams_and_later_reset_remain_ready() {
        let mut session = ready(2);
        let first = open(&mut session, true);
        let second = open(&mut session, true);
        drain(&mut session);
        session.input().extend_from_slice(&headers(first, false));
        session.input().extend_from_slice(&headers(second, false));
        let mut data = frame::Data::new(first.as_u32().into(), Bytes::from_static(b"ok"));
        data.set_end_stream(true);
        session.input().extend_from_slice(&wire(data.into()));
        session.drive(session.now).unwrap();
        // Coalescing is adjacent only; it must not reorder interleaved IDs.
        assert_eq!(
            session
                .inner
                .codec_mut()
                .received
                .iter()
                .copied()
                .collect::<Vec<_>>(),
            [first.as_u32(), second.as_u32(), first.as_u32()]
        );
        assert!(matches!(session.next_event(),
            Some(Event::Headers { stream, .. }) if stream == first));
        assert!(matches!(session.next_event(),
            Some(Event::Data { stream, .. }) if stream == first));
        session.release_capacity(first, 2).unwrap();
        assert!(matches!(session.next_event(),
            Some(Event::End { stream, .. }) if stream == first));
        assert!(matches!(session.next_event(),
            Some(Event::Headers { stream, .. }) if stream == second));
        assert!(session.next_event().is_none());

        // After Pending empties the queue, the same ID must be scheduled again.
        let reset = wire(frame::Reset::new(second.as_u32().into(), Reason::CANCEL).into());
        for byte in reset {
            session.input().extend_from_slice(&[byte]);
            session.drive(session.now).unwrap();
        }
        assert_eq!(session.inner.codec_mut().received.len(), 1);
        assert!(matches!(session.next_event(),
            Some(Event::Failed { stream, error })
            if stream == second && error.reason() == Some(Reason::CANCEL)));
        assert!(session.next_event().is_none());
        assert_eq!(session.active, 0);
    }

    #[test]
    fn consumed_credit_advances_buffered_body_without_losing_partial_credit() {
        let mut session = ready(1);
        let id = open(&mut session, true);
        drain(&mut session);
        session.input().extend_from_slice(&headers(id, false));
        session.input().extend_from_slice(&wire(
            frame::Data::new(id.as_u32().into(), Bytes::from_static(b"ab")).into(),
        ));
        let mut last = frame::Data::new(id.as_u32().into(), Bytes::from_static(b"c"));
        last.set_end_stream(true);
        session.input().extend_from_slice(&wire(last.into()));
        session.drive(session.now).unwrap();
        assert!(matches!(session.next_event(), Some(Event::Headers { .. })));
        assert!(matches!(session.next_event(),
            Some(Event::Data { bytes, .. }) if bytes == "ab"));
        let queue = session.inner.codec_mut().received.clone();
        assert!(session.release_capacity_and_next_event(id, 3).is_err());
        assert_eq!(session.exchanges[&id.as_u32()].unconsumed, 2);
        assert_eq!(session.inner.codec_mut().received, queue);
        assert!(
            matches!(session.release_capacity_and_next_event(id, 1).unwrap(),
            Some(Event::Data { stream, bytes }) if stream == id && bytes == "c")
        );
        assert_eq!(session.exchanges[&id.as_u32()].unconsumed, 2);
        assert!(
            matches!(session.release_capacity_and_next_event(id, 2).unwrap(),
            Some(Event::End { stream, trailers: None }) if stream == id)
        );
        assert!(!session.exchanges.contains_key(&id.as_u32()));
        assert!(session.next_event().is_none());
        assert_eq!(session.active, 0);
    }

    #[test]
    fn credit_return_while_pending_wakes_updates_and_later_response_events() {
        for arrival in [0, 1, 2] {
            let mut builder = Builder::new();
            builder.initial_window_size(4);
            let mut session = ready_with_builder(1, builder);
            let id = open(&mut session, true);
            drain(&mut session);
            session.input().extend_from_slice(&headers(id, false));
            session.input().extend_from_slice(&wire(
                frame::Data::new(id.as_u32().into(), Bytes::from_static(b"ok")).into(),
            ));
            session.drive(session.now).unwrap();
            assert!(matches!(session.next_event(), Some(Event::Headers { .. })));
            assert!(matches!(session.next_event(), Some(Event::Data { .. })));
            session.progress.0.store(false, Ordering::Relaxed);
            assert!(session
                .release_capacity_and_next_event(id, 2)
                .unwrap()
                .is_none());
            assert_eq!(session.exchanges[&id.as_u32()].unconsumed, 0);
            assert!(session.progress.0.load(Ordering::Relaxed));
            let update = wire(frame::WindowUpdate::new(id.as_u32().into(), 2).into());
            let output = drain(&mut session);
            assert!(output.windows(update.len()).any(|bytes| bytes == update));

            let input = match arrival {
                0 => {
                    let mut data = frame::Data::new(id.as_u32().into(), Bytes::from_static(b"!"));
                    data.set_end_stream(true);
                    wire(data.into())
                }
                1 => {
                    let mut fields = HeaderMap::new();
                    fields.insert("x-end", http::HeaderValue::from_static("yes"));
                    wire(frame::Headers::trailers(id.as_u32().into(), fields).into())
                }
                _ => wire(frame::Reset::new(id.as_u32().into(), Reason::CANCEL).into()),
            };
            let progress = Arc::new(Progress::default());
            let waker = Waker::from(progress.clone());
            let cx = Context::from_waker(&waker);
            assert!(session
                .exchanges
                .get_mut(&id.as_u32())
                .unwrap()
                .poll_next(id, &cx)
                .is_pending());
            for byte in input {
                session.input().extend_from_slice(&[byte]);
                session.drive(session.now).unwrap();
            }
            // The next completed frame schedules the previously pending cursor.
            assert!(progress.0.load(Ordering::Relaxed));
            match arrival {
                0 => {
                    assert!(matches!(session.next_event(),
                        Some(Event::Data { stream, bytes }) if stream == id && bytes == "!"));
                    assert!(
                        matches!(session.release_capacity_and_next_event(id, 1).unwrap(),
                        Some(Event::End { stream, trailers: None }) if stream == id)
                    );
                }
                1 => assert!(matches!(session.next_event(),
                    Some(Event::End { stream, trailers: Some(fields) })
                    if stream == id && fields["x-end"] == "yes")),
                _ => assert!(matches!(session.next_event(),
                    Some(Event::Failed { stream, error })
                    if stream == id && error.reason() == Some(Reason::CANCEL))),
            }
            assert!(!session.exchanges.contains_key(&id.as_u32()));
            assert!(session.next_event().is_none());
        }
    }

    #[test]
    fn credit_return_after_end_preserves_open_send_half_and_handles_stale_ready() {
        for sent in [false, true] {
            for stale_ready in [false, true] {
                let mut session = ready(1);
                let id = open(&mut session, sent);
                drain(&mut session);
                session.input().extend_from_slice(&headers(id, false));
                let mut data = frame::Data::new(id.as_u32().into(), Bytes::from_static(b"ok"));
                data.set_end_stream(true);
                session.input().extend_from_slice(&wire(data.into()));
                session.drive(session.now).unwrap();
                assert!(matches!(session.next_event(), Some(Event::Headers { .. })));
                assert!(matches!(session.next_event(), Some(Event::Data { .. })));
                assert!(matches!(session.next_event(), Some(Event::End { .. })));
                if stale_ready {
                    // Interleaved wire notifications or GOAWAY may leave an
                    // entry after End, while outstanding credit keeps it alive.
                    session.inner.codec_mut().received.push_front(id.as_u32());
                }
                assert!(session
                    .release_capacity_and_next_event(id, 2)
                    .unwrap()
                    .is_none());
                if !sent {
                    assert_eq!(session.active, 1);
                    assert!(session.exchanges.contains_key(&id.as_u32()));
                    session.send_data(id, Bytes::new(), true).unwrap();
                }
                assert_eq!(session.active, 0);
                assert!(!session.exchanges.contains_key(&id.as_u32()));
                assert!(session.release_capacity_and_next_event(id, 0).is_err());
            }
        }
    }

    #[test]
    fn credit_return_for_another_stream_does_not_consume_its_ready_entry_on_error() {
        let mut session = ready(2);
        let first = open(&mut session, true);
        let second = open(&mut session, true);
        drain(&mut session);
        session.input().extend_from_slice(&headers(first, false));
        let mut data = frame::Data::new(first.as_u32().into(), Bytes::from_static(b"ok"));
        data.set_end_stream(true);
        session.input().extend_from_slice(&wire(data.into()));
        session.input().extend_from_slice(&headers(second, true));
        session.drive(session.now).unwrap();
        assert!(matches!(session.next_event(), Some(Event::Headers { .. })));
        assert!(matches!(session.next_event(), Some(Event::Data { .. })));
        assert!(matches!(session.next_event(), Some(Event::End { .. })));
        let queue = session.inner.codec_mut().received.clone();
        assert!(session.release_capacity_and_next_event(first, 3).is_err());
        assert_eq!(session.inner.codec_mut().received, queue);
        assert!(
            matches!(session.release_capacity_and_next_event(first, 2).unwrap(),
            Some(Event::Headers { stream, .. }) if stream == second)
        );
        assert!(!session.exchanges.contains_key(&first.as_u32()));
        assert!(matches!(session.next_event(),
            Some(Event::End { stream, .. }) if stream == second));
        assert!(session.next_event().is_none());
        assert_eq!(session.active, 0);
    }

    #[test]
    fn one_exchange_reference_owns_send_receive_and_unreleased_credit() {
        for sent in [false, true] {
            let mut session = ready(1);
            let id = open(&mut session, sent);
            // Only admission may retain an additional temporary reference.
            let pending = usize::from(session.send.pending.is_some());
            assert_eq!(
                session.exchanges[&id.as_u32()].stream.ref_count(),
                1 + pending
            );
            drain(&mut session);
            let mut cx = Context::from_waker(&session.waker);
            assert!(matches!(
                session.send.poll_ready(&mut cx),
                Poll::Ready(Ok(()))
            ));
            assert_eq!(session.exchanges[&id.as_u32()].stream.ref_count(), 1);

            session.input().extend_from_slice(&headers(id, false));
            let mut data = frame::Data::new(id.as_u32().into(), Bytes::from_static(b"ok"));
            data.set_end_stream(true);
            session.input().extend_from_slice(&wire(data.into()));
            session.drive(session.now).unwrap();
            assert!(matches!(session.next_event(), Some(Event::Headers { .. })));
            assert_eq!(session.exchanges[&id.as_u32()].stream.ref_count(), 1);
            assert!(matches!(session.next_event(),
                Some(Event::Data { stream, bytes }) if stream == id && bytes == "ok"));
            assert!(matches!(session.next_event(), Some(Event::End { .. })));
            assert_eq!(session.exchanges[&id.as_u32()].stream.ref_count(), 1);
            session.release_capacity(id, 2).unwrap();
            if !sent {
                assert_eq!(session.active, 1);
                assert_eq!(session.exchanges[&id.as_u32()].stream.ref_count(), 1);
                session
                    .send_data(id, Bytes::from_static(b"request"), true)
                    .unwrap();
            }
            assert_eq!(session.active, 0);
            assert!(!session.exchanges.contains_key(&id.as_u32()));
            drain(&mut session);
        }
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
    fn pending_cursor_wakes_for_data_trailers_and_reset() {
        #[derive(Clone, Copy)]
        enum Arrival {
            Data,
            Trailers,
            Reset,
        }
        for arrival in [Arrival::Data, Arrival::Trailers, Arrival::Reset] {
            let mut session = ready(1);
            let id = open(&mut session, true);
            drain(&mut session);
            session.input().extend_from_slice(&headers(id, false));
            session.drive(session.now).unwrap();
            assert!(matches!(session.next_event(), Some(Event::Headers { .. })));
            assert!(session.next_event().is_none());

            let progress = Arc::new(Progress::default());
            let waker = Waker::from(progress.clone());
            let cx = Context::from_waker(&waker);
            assert!(session
                .exchanges
                .get_mut(&id.as_u32())
                .unwrap()
                .poll_next(id, &cx)
                .is_pending());
            assert!(!progress.0.load(Ordering::Relaxed));

            let input = match arrival {
                Arrival::Data => {
                    let mut data = frame::Data::new(id.as_u32().into(), Bytes::from_static(b"ok"));
                    data.set_end_stream(true);
                    wire(data.into())
                }
                Arrival::Trailers => {
                    let mut fields = HeaderMap::new();
                    fields.insert("x-finished", http::HeaderValue::from_static("yes"));
                    wire(frame::Headers::trailers(id.as_u32().into(), fields).into())
                }
                Arrival::Reset => {
                    wire(frame::Reset::new(id.as_u32().into(), Reason::CANCEL).into())
                }
            };
            session.input().extend_from_slice(&input);
            session.drive(session.now).unwrap();
            assert!(progress.0.load(Ordering::Relaxed));
            match arrival {
                Arrival::Data => {
                    assert!(matches!(session.next_event(),
                        Some(Event::Data { stream, bytes }) if stream == id && bytes == "ok"));
                    assert!(matches!(session.next_event(),
                        Some(Event::End { stream, trailers: None }) if stream == id));
                    assert!(session.exchanges.contains_key(&id.as_u32()));
                    session.release_capacity(id, 2).unwrap();
                }
                Arrival::Trailers => {
                    assert!(matches!(session.next_event(),
                        Some(Event::End { stream, trailers: Some(fields) })
                        if stream == id && fields["x-finished"] == "yes"));
                }
                Arrival::Reset => {
                    assert!(matches!(session.next_event(),
                        Some(Event::Failed { stream, error })
                        if stream == id && error.reason() == Some(Reason::CANCEL)));
                }
            }
            assert!(session.next_event().is_none());
            assert!(!session.exchanges.contains_key(&id.as_u32()));
        }
    }

    #[test]
    fn cursor_keeps_credit_and_send_half_after_early_response_end() {
        let mut session = ready(1);
        let id = open(&mut session, false);
        session.release_capacity(id, 0).unwrap();
        drain(&mut session);
        let mut data = frame::Data::new(id.as_u32().into(), Bytes::from_static(b"ok"));
        data.set_end_stream(true);
        session.input().extend_from_slice(&headers(id, false));
        session.input().extend_from_slice(&wire(data.into()));
        session.drive(session.now).unwrap();
        assert!(matches!(session.next_event(), Some(Event::Headers { .. })));
        assert!(matches!(session.next_event(), Some(Event::Data { .. })));
        assert!(matches!(session.next_event(), Some(Event::End { .. })));
        assert_eq!(session.active, 1);
        session.release_capacity(id, 1).unwrap();
        assert!(session.release_capacity(id, 2).is_err());
        assert_eq!(session.exchanges[&id.as_u32()].unconsumed, 1);
        session.release_capacity(id, 1).unwrap();
        assert!(session.exchanges.contains_key(&id.as_u32()));
        session.send_data(id, Bytes::new(), true).unwrap();
        assert_eq!(session.active, 0);
        assert!(!session.exchanges.contains_key(&id.as_u32()));
        drain(&mut session);
        assert!(session.can_open());
    }

    #[test]
    fn cursor_reset_restores_data_frame_budget_before_or_after_consumption() {
        for (read_head, read_data) in [(false, false), (true, false), (true, true)] {
            let mut builder = Builder::new();
            // Enough for one small DATA frame, not two retained together.
            builder.data_frame_budget(proto::DEFAULT_DATA_FRAME_OVERHEAD_THRESHOLD);
            let mut session = ready_with_builder(2, builder);
            let first = open(&mut session, true);
            let second = open(&mut session, true);
            drain(&mut session);
            session.input().extend_from_slice(&headers(first, false));
            session.input().extend_from_slice(&wire(
                frame::Data::new(first.as_u32().into(), Bytes::from_static(b"a")).into(),
            ));
            session.drive(session.now).unwrap();
            if read_head {
                assert!(matches!(session.next_event(), Some(Event::Headers { .. })));
            }
            if read_data {
                assert!(matches!(session.next_event(), Some(Event::Data { .. })));
                // Reset must also reclaim delivered but uncredited bytes.
                assert_eq!(session.exchanges[&first.as_u32()].unconsumed, 1);
            }
            session.reset(first, Reason::CANCEL);

            session.input().extend_from_slice(&headers(second, false));
            let mut data = frame::Data::new(second.as_u32().into(), Bytes::from_static(b"b"));
            data.set_end_stream(true);
            session.input().extend_from_slice(&wire(data.into()));
            session.drive(session.now).unwrap();
            assert!(matches!(session.next_event(),
                Some(Event::Headers { stream, .. }) if stream == second));
            assert!(matches!(session.next_event(),
                Some(Event::Data { stream, bytes }) if stream == second && bytes == "b"));
            assert!(matches!(session.next_event(),
                Some(Event::End { stream, .. }) if stream == second));
            session.release_capacity(second, 1).unwrap();
            assert!(session.next_event().is_none());
            drain(&mut session);
        }
    }

    #[test]
    fn cursor_drop_returns_unread_credit_but_keeps_delivered_credit_until_last_ref() {
        for read_data in [false, true] {
            let mut session = ready(1);
            let id = open(&mut session, false);
            drain(&mut session);
            // Clear SendRequest's pending-open reference after the request is
            // on the wire, leaving only the exchange's stream reference.
            let mut cx = Context::from_waker(&session.waker);
            assert!(matches!(
                session.send.poll_ready(&mut cx),
                Poll::Ready(Ok(()))
            ));
            let chunk = Bytes::from(vec![b'x'; frame::DEFAULT_MAX_FRAME_SIZE as usize]);
            let credit = chunk.len() as u32 * 3;
            session.input().extend_from_slice(&headers(id, false));
            for _ in 0..3 {
                session.input().extend_from_slice(&wire(
                    frame::Data::new(id.as_u32().into(), chunk.clone()).into(),
                ));
            }
            session.drive(session.now).unwrap();
            assert!(matches!(session.next_event(), Some(Event::Headers { .. })));
            if read_data {
                for _ in 0..3 {
                    assert!(matches!(session.next_event(), Some(Event::Data { .. })));
                }
            }
            let exchange = session.exchanges.remove(&id.as_u32()).unwrap();
            // An explicit retained reference exercises the shared store's
            // last-reference credit rule without a second production handle.
            let retained = exchange.stream.clone_to_opaque();
            drop(exchange);
            let update = wire(frame::WindowUpdate::new(frame::StreamId::ZERO, credit).into());
            let output = drain(&mut session);
            let returned = output.windows(update.len()).any(|bytes| bytes == update);
            // Unread buffers are returned eagerly. Delivered, uncredited bytes
            // remain charged while the retained reference keeps it alive.
            assert_eq!(returned, !read_data);
            drop(retained);
            let output = drain(&mut session);
            if read_data {
                assert!(output.windows(update.len()).any(|bytes| bytes == update));
            } else {
                assert!(!output.windows(update.len()).any(|bytes| bytes == update));
            }
        }
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
    fn multiple_informational_heads_precede_final_headers_and_body() {
        let mut session = ready(1);
        let id = open(&mut session, true);
        drain(&mut session);
        for status in [http::StatusCode::CONTINUE, http::StatusCode::EARLY_HINTS] {
            let head = frame::Headers::new(
                id.as_u32().into(),
                frame::Pseudo::response(status),
                HeaderMap::new(),
            );
            session.input().extend_from_slice(&wire(head.into()));
        }
        session.input().extend_from_slice(&headers(id, false));
        session.input().extend_from_slice(&wire(
            frame::Data::new(id.as_u32().into(), Bytes::from_static(b"ok")).into(),
        ));
        let mut fields = HeaderMap::new();
        fields.insert("x-finished", http::HeaderValue::from_static("yes"));
        session.input().extend_from_slice(&wire(
            frame::Headers::trailers(id.as_u32().into(), fields).into(),
        ));
        session.drive(session.now).unwrap();

        for status in [http::StatusCode::CONTINUE, http::StatusCode::EARLY_HINTS] {
            assert!(matches!(session.next_event(),
                Some(Event::Informational { stream, response })
                if stream == id && response.status() == status));
        }
        assert!(matches!(session.next_event(),
            Some(Event::Headers { stream, response })
            if stream == id && response.status() == http::StatusCode::OK));
        assert!(matches!(session.next_event(),
            Some(Event::Data { stream, bytes }) if stream == id && bytes == "ok"));
        session.release_capacity(id, 2).unwrap();
        assert!(matches!(session.next_event(),
            Some(Event::End { stream, trailers: Some(fields) })
            if stream == id && fields["x-finished"] == "yes"));
        assert!(session.next_event().is_none());
    }

    #[test]
    fn informational_then_fragmented_final_head_stays_pending_until_complete() {
        let mut session = ready(1);
        let id = open(&mut session, true);
        drain(&mut session);
        let early = frame::Headers::new(
            id.as_u32().into(),
            frame::Pseudo::response(http::StatusCode::EARLY_HINTS),
            HeaderMap::new(),
        );
        session.input().extend_from_slice(&wire(early.into()));
        session.drive(session.now).unwrap();
        assert!(matches!(
            session.next_event(),
            Some(Event::Informational { .. })
        ));
        assert!(session.next_event().is_none());

        let final_head = headers(id, true);
        let last = final_head.len() - 1;
        for byte in &final_head[..last] {
            session.input().extend_from_slice(&[*byte]);
            session.drive(session.now).unwrap();
            assert!(session.next_event().is_none());
        }
        session.input().extend_from_slice(&final_head[last..]);
        session.drive(session.now).unwrap();
        assert!(matches!(session.next_event(),
            Some(Event::Headers { stream, .. }) if stream == id));
        assert!(matches!(session.next_event(),
            Some(Event::End { stream, trailers: None }) if stream == id));
        assert!(session.next_event().is_none());
    }

    #[test]
    fn pending_head_is_woken_by_final_headers_or_remote_reset() {
        for reset in [false, true] {
            let mut session = ready(1);
            let id = open(&mut session, true);
            drain(&mut session);
            let progress = Arc::new(Progress::default());
            let waker = Waker::from(progress.clone());
            let cx = Context::from_waker(&waker);
            assert!(session
                .exchanges
                .get_mut(&id.as_u32())
                .unwrap()
                .poll_next(id, &cx)
                .is_pending());
            assert!(!progress.0.load(Ordering::Relaxed));

            let input = if reset {
                wire(frame::Reset::new(id.as_u32().into(), Reason::REFUSED_STREAM).into())
            } else {
                headers(id, true)
            };
            session.input().extend_from_slice(&input);
            session.drive(session.now).unwrap();
            assert!(progress.0.load(Ordering::Relaxed));
            if reset {
                assert!(matches!(session.next_event(),
                    Some(Event::Failed { stream, error })
                    if stream == id && error.reason() == Some(Reason::REFUSED_STREAM)));
            } else {
                assert!(matches!(session.next_event(),
                    Some(Event::Headers { stream, .. }) if stream == id));
                assert!(matches!(session.next_event(),
                    Some(Event::End { stream, .. }) if stream == id));
            }
            assert!(session.next_event().is_none());
        }
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
