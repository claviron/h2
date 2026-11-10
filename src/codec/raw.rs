//! Buffer-driven frame transport. No socket, task, or timer is owned here.
use super::decode::FrameDecoder;
use super::encode::Encoder;
use super::{FrameIo, UserError};
use crate::frame::{self, Data, Frame, Reason};
use crate::proto::Error;
use bytes::{Buf, Bytes, BytesMut};
use futures_core::Stream;
use std::collections::VecDeque;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

const PREFACE: &[u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";

#[derive(Debug)]
pub(crate) struct Raw<B> {
    input: BytesMut,
    decoder: FrameDecoder,
    encoder: Encoder<B>,
    preface: Bytes,
    max_frame: usize,
    pending_len: Option<usize>,
    eof: bool,
    // Shared by frame decoding and Session's event drain. One entry keeps a
    // stream scheduled until its receive cursor reaches Pending or End.
    pub(crate) received: VecDeque<u32>,
    pub(crate) go_away: bool,
}

impl<B: Buf> Raw<B> {
    pub(crate) fn new() -> Self {
        Self {
            input: BytesMut::new(),
            decoder: FrameDecoder::new(frame::DEFAULT_MAX_FRAME_SIZE as usize),
            encoder: Encoder::new(true),
            preface: Bytes::from_static(PREFACE),
            max_frame: frame::DEFAULT_MAX_FRAME_SIZE as usize,
            pending_len: None,
            eof: false,
            received: VecDeque::new(),
            go_away: false,
        }
    }

    pub(crate) fn input(&mut self) -> &mut BytesMut {
        &mut self.input
    }
    pub(crate) fn end_input(&mut self) {
        self.eof = true;
    }

    pub(crate) fn output(&mut self) -> impl Buf + '_ {
        (&mut self.preface).chain(self.encoder.output())
    }

    pub(crate) fn output_chunk(&self) -> &[u8] {
        if !self.preface.is_empty() {
            &self.preface
        } else {
            self.encoder.output_chunk()
        }
    }

    pub(crate) fn advance_output(&mut self, count: usize) {
        self.output().advance(count);
        if self.preface.is_empty() && self.encoder.is_empty() {
            let _ = self.encoder.unset_frame();
        }
    }

    fn settle(&mut self) {
        if self.preface.is_empty() && self.encoder.is_empty() {
            let _ = self.encoder.unset_frame();
        }
    }
}

impl<B: Buf> Stream for Raw<B> {
    type Item = Result<Frame, Error>;

    fn poll_next(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // Raw does not pin B; frame payloads may move with the session.
        let me = self.get_mut();
        loop {
            let total = match me.pending_len {
                Some(total) => total,
                None if me.input.len() >= frame::HEADER_LEN => {
                    let len = (usize::from(me.input[0]) << 16)
                        | (usize::from(me.input[1]) << 8)
                        | usize::from(me.input[2]);
                    if len > me.max_frame {
                        return Poll::Ready(Some(Err(Error::library_go_away(
                            Reason::FRAME_SIZE_ERROR,
                        ))));
                    }
                    let total = len + frame::HEADER_LEN;
                    me.pending_len = Some(total);
                    total
                }
                None => {
                    return if me.eof {
                        if me.input.is_empty() {
                            Poll::Ready(None)
                        } else {
                            Poll::Ready(Some(Err(io::ErrorKind::UnexpectedEof.into())))
                        }
                    } else {
                        Poll::Pending
                    };
                }
            };
            if me.input.len() < total {
                return if me.eof {
                    Poll::Ready(Some(Err(io::ErrorKind::UnexpectedEof.into())))
                } else {
                    Poll::Pending
                };
            }
            let bytes = me.input.split_to(total);
            me.pending_len = None;
            let id = u32::from_be_bytes([bytes[5], bytes[6], bytes[7], bytes[8]]) & 0x7fff_ffff;
            match me.decoder.decode(bytes)? {
                Some(frame) => {
                    // A ready entry drains all buffered events for its stream.
                    // Preserve interleaving, but do not schedule adjacent
                    // HEADERS/DATA/trailers for the same stream repeatedly.
                    if id != 0 && me.received.back() != Some(&id) {
                        me.received.push_back(id);
                    }
                    if matches!(frame, Frame::GoAway(_)) {
                        me.go_away = true;
                    }
                    return Poll::Ready(Some(Ok(frame)));
                }
                None => continue,
            }
        }
    }
}

// No field is structurally pinned.
impl<B> Unpin for Raw<B> {}

impl<B: Buf> FrameIo<B> for Raw<B> {
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.settle();
        if self.encoder.has_capacity() {
            Poll::Ready(Ok(()))
        } else {
            self.flush(cx)
        }
    }
    fn has_send_capacity(&mut self) -> bool {
        self.settle();
        self.encoder.has_capacity()
    }
    fn buffer(&mut self, frame: Frame<B>) -> Result<(), UserError> {
        self.encoder.buffer(frame)
    }
    fn flush(&mut self, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.settle();
        if !self.output().has_remaining() {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }
    fn shutdown(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.flush(cx)
    }
    fn max_send_frame_size(&self) -> usize {
        self.encoder.max_frame_size()
    }
    fn set_max_send_frame_size(&mut self, value: usize) {
        self.encoder.set_max_frame_size(value);
    }
    fn set_max_recv_frame_size(&mut self, value: usize) {
        assert!(
            (frame::DEFAULT_MAX_FRAME_SIZE as usize..=frame::MAX_MAX_FRAME_SIZE as usize)
                .contains(&value)
        );
        self.max_frame = value;
        self.decoder.set_max_frame_size(value);
    }
    fn set_send_header_table_size(&mut self, value: usize) {
        self.encoder.set_header_table_size(value);
    }
    fn set_recv_header_table_size(&mut self, value: usize) {
        self.decoder.set_header_table_size(value);
    }
    fn set_max_recv_header_list_size(&mut self, value: usize) {
        self.decoder.set_max_header_list_size(value, self.max_frame);
    }
    fn take_last_data_frame(&mut self) -> Option<Data<B>> {
        self.encoder.take_last_data_frame()
    }
}
