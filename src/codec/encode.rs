//! Frame encoding and HPACK state; socket progress is supplied by the caller.
use crate::codec::UserError;
use crate::codec::UserError::*;
use crate::frame::{self, Frame, FrameSize};
use crate::hpack;
use bytes::{Buf, BufMut, BytesMut};
use std::io::{Cursor, IoSlice};

// A macro to get around a method needing to borrow &mut self
macro_rules! limited_write_buf {
    ($self:expr) => {{
        let limit = $self.max_frame_size() + frame::HEADER_LEN;
        $self.buf.get_mut().limit(limit)
    }};
}

#[derive(Debug)]
pub(crate) struct Encoder<B> {
    /// HPACK encoder
    hpack: hpack::Encoder,

    /// Write buffer
    ///
    /// TODO: Should this be a ring buffer?
    buf: Cursor<BytesMut>,

    /// Next frame to encode
    next: Option<Next<B>>,

    /// Last data frame
    last_data_frame: Option<frame::Data<B>>,

    /// Max frame size, this is specified by the peer
    max_frame_size: FrameSize,

    /// Chain payloads bigger than this.
    chain_threshold: usize,

    /// Min buffer required to attempt to write a frame
    min_buffer_capacity: usize,
}

#[derive(Debug)]
enum Next<B> {
    Data(frame::Data<B>),
    Continuation(frame::Continuation),
}

/// Initialize the connection with this amount of write buffer.
///
/// The minimum MAX_FRAME_SIZE is 16kb, so always be able to send a HEADERS
/// frame that big.
const DEFAULT_BUFFER_CAPACITY: usize = 16 * 1_024;

/// Chain payloads bigger than this when vectored I/O is enabled. The remote
/// will never advertise a max frame size less than this (well, the spec says
/// the max frame size can't be less than 16kb, so not even close).
const CHAIN_THRESHOLD: usize = 256;

/// Chain payloads bigger than this when vectored I/O is **not** enabled.
/// A larger value in this scenario will reduce the number of small and
/// fragmented data being sent, and hereby improve the throughput.
const CHAIN_THRESHOLD_WITHOUT_VECTORED_IO: usize = 1024;

#[must_use]
pub(crate) enum ControlFlow {
    Continue,
    Break,
}

impl<B> Encoder<B>
where
    B: Buf,
{
    pub(crate) fn unset_frame(&mut self) -> ControlFlow {
        // Clear internal buffer
        self.buf.set_position(0);
        self.buf.get_mut().clear();

        // The data frame has been written, so unset it
        match self.next.take() {
            Some(Next::Data(frame)) => {
                self.last_data_frame = Some(frame);
                debug_assert!(self.is_empty());
                ControlFlow::Break
            }
            Some(Next::Continuation(frame)) => {
                // Buffer the continuation frame, then try to write again
                let mut buf = limited_write_buf!(self);
                if let Some(continuation) = frame.encode(&mut buf) {
                    self.next = Some(Next::Continuation(continuation));
                }
                ControlFlow::Continue
            }
            None => ControlFlow::Break,
        }
    }

    pub(crate) fn buffer(&mut self, item: Frame<B>) -> Result<(), UserError> {
        // Ensure that we have enough capacity to accept the write.
        assert!(self.has_capacity());
        let span = tracing::trace_span!("FramedWrite::buffer", frame = ?item);
        let _e = span.enter();

        tracing::debug!(frame = ?item, "send");

        match item {
            Frame::Data(mut v) => {
                // Ensure that the payload is not greater than the max frame.
                let len = v.payload().remaining();

                if len > self.max_frame_size() {
                    return Err(PayloadTooBig);
                }

                if len >= self.chain_threshold {
                    let head = v.head();

                    // Encode the frame head to the buffer
                    head.encode(len, self.buf.get_mut());

                    if self.buf.get_ref().remaining() < self.chain_threshold {
                        let extra_bytes = self.chain_threshold - self.buf.remaining();
                        self.buf.get_mut().put(v.payload_mut().take(extra_bytes));
                    }

                    // Save the data frame
                    self.next = Some(Next::Data(v));
                } else {
                    v.encode_chunk(self.buf.get_mut());

                    // The chunk has been fully encoded, so there is no need to
                    // keep it around
                    assert_eq!(v.payload().remaining(), 0, "chunk not fully encoded");

                    // Save off the last frame...
                    self.last_data_frame = Some(v);
                }
            }
            Frame::Headers(v) => {
                let mut buf = limited_write_buf!(self);
                if let Some(continuation) = v.encode(&mut self.hpack, &mut buf) {
                    self.next = Some(Next::Continuation(continuation));
                }
            }
            Frame::PushPromise(v) => {
                let mut buf = limited_write_buf!(self);
                if let Some(continuation) = v.encode(&mut self.hpack, &mut buf) {
                    self.next = Some(Next::Continuation(continuation));
                }
            }
            Frame::Settings(v) => {
                v.encode(self.buf.get_mut());
                tracing::trace!(rem = self.buf.remaining(), "encoded settings");
            }
            Frame::GoAway(v) => {
                v.encode(self.buf.get_mut());
                tracing::trace!(rem = self.buf.remaining(), "encoded go_away");
            }
            Frame::Ping(v) => {
                v.encode(self.buf.get_mut());
                tracing::trace!(rem = self.buf.remaining(), "encoded ping");
            }
            Frame::WindowUpdate(v) => {
                v.encode(self.buf.get_mut());
                tracing::trace!(rem = self.buf.remaining(), "encoded window_update");
            }

            Frame::Priority(_) => {
                /*
                v.encode(self.buf.get_mut());
                tracing::trace!("encoded priority; rem={:?}", self.buf.remaining());
                */
                unimplemented!();
            }
            Frame::Reset(v) => {
                v.encode(self.buf.get_mut());
                tracing::trace!(rem = self.buf.remaining(), "encoded reset");
            }
        }

        Ok(())
    }

    pub(crate) fn has_capacity(&self) -> bool {
        self.next.is_none()
            && (self.buf.get_ref().capacity() - self.buf.get_ref().len()
                >= self.min_buffer_capacity)
    }

    pub(crate) fn is_empty(&self) -> bool {
        if self.buf.has_remaining() {
            return false;
        }
        match self.next {
            Some(Next::Data(ref frame)) => !frame.payload().has_remaining(),
            _ => !self.buf.has_remaining(),
        }
    }
}

impl<B> Encoder<B> {
    pub(crate) fn max_frame_size(&self) -> usize {
        self.max_frame_size as usize
    }
}

impl<B> Encoder<B> {
    pub(crate) fn new(vectored: bool) -> Self {
        let chain_threshold = if vectored {
            CHAIN_THRESHOLD
        } else {
            CHAIN_THRESHOLD_WITHOUT_VECTORED_IO
        };
        Self {
            hpack: hpack::Encoder::default(),
            buf: Cursor::new(BytesMut::with_capacity(DEFAULT_BUFFER_CAPACITY)),
            next: None,
            last_data_frame: None,
            max_frame_size: frame::DEFAULT_MAX_FRAME_SIZE,
            chain_threshold,
            min_buffer_capacity: chain_threshold + frame::HEADER_LEN,
        }
    }

    #[cfg(feature = "sans-io")]
    pub(crate) fn output_chunk(&self) -> &[u8]
    where
        B: Buf,
    {
        if self.buf.has_remaining() {
            self.buf.chunk()
        } else if let Some(Next::Data(ref frame)) = self.next {
            frame.payload().chunk()
        } else {
            &[]
        }
    }

    pub(crate) fn output(&mut self) -> Output<'_, B> {
        Output(self)
    }

    pub(crate) fn set_max_frame_size(&mut self, value: usize) {
        assert!(value <= frame::MAX_MAX_FRAME_SIZE as usize);
        self.max_frame_size = value as FrameSize;
    }

    pub(crate) fn set_header_table_size(&mut self, value: usize) {
        self.hpack.update_max_size(value);
    }

    pub(crate) fn take_last_data_frame(&mut self) -> Option<frame::Data<B>> {
        self.last_data_frame.take()
    }
}

/// Borrowed encoded headers and DATA payload; advancing confirms bytes written.
pub(crate) struct Output<'a, B>(&'a mut Encoder<B>);

impl<B: Buf> Buf for Output<'_, B> {
    fn remaining(&self) -> usize {
        self.0.buf.remaining()
            + match &self.0.next {
                Some(Next::Data(frame)) => frame.payload().remaining(),
                _ => 0,
            }
    }
    fn chunk(&self) -> &[u8] {
        if self.0.buf.has_remaining() {
            return self.0.buf.chunk();
        }
        match &self.0.next {
            Some(Next::Data(frame)) => frame.payload().chunk(),
            _ => &[],
        }
    }
    fn advance(&mut self, count: usize) {
        assert!(count <= self.remaining());
        let head = count.min(self.0.buf.remaining());
        self.0.buf.advance(head);
        if count > head {
            if let Some(Next::Data(frame)) = &mut self.0.next {
                frame.payload_mut().advance(count - head);
            }
        }
    }
    fn chunks_vectored<'a>(&'a self, dst: &mut [IoSlice<'a>]) -> usize {
        let used = self.0.buf.chunks_vectored(dst);
        match &self.0.next {
            Some(Next::Data(frame)) if used < dst.len() => {
                used + frame.payload().chunks_vectored(&mut dst[used..])
            }
            _ => used,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn chained_data_survives_short_writes_without_copying_its_tail() {
        let payload = Bytes::from(vec![b'x'; CHAIN_THRESHOLD * 8]);
        let mut encoder = Encoder::new(true);
        encoder
            .buffer(frame::Data::new(1.into(), payload.clone()).into())
            .unwrap();
        let mut wire = Vec::new();
        // Consume only the header and copied prefix before checking the tail.
        let head = encoder.output().chunk().len();
        assert!(head < payload.len());
        wire.extend_from_slice(encoder.output().chunk());
        encoder.output().advance(head);
        {
            let output = encoder.output();
            assert_eq!(
                output.chunk().as_ptr(),
                payload[head - frame::HEADER_LEN..].as_ptr()
            );
        }
        while encoder.output().has_remaining() {
            let mut output = encoder.output();
            let count = output.chunk().len().min(7);
            wire.extend_from_slice(&output.chunk()[..count]);
            output.advance(count);
        }
        assert!(matches!(encoder.unset_frame(), ControlFlow::Break));
        assert_eq!(&wire[frame::HEADER_LEN..], &payload[..]);
        assert_eq!(wire.len(), payload.len() + frame::HEADER_LEN);
        assert!(encoder.take_last_data_frame().is_some());
        assert!(encoder.take_last_data_frame().is_none());
    }
}
