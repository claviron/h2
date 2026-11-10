# h2-forked

**Drive HTTP/2 from your own event loop.**

A Sans-I/O client: supply bytes and monotonic time, drain output, and handle
response events. You own the transport, scheduling and request deadlines.

[![Crates.io](https://img.shields.io/crates/v/h2-forked.svg)](https://crates.io/crates/h2-forked)
[![Documentation](https://docs.rs/h2-forked/badge.svg)](https://docs.rs/h2-forked)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

`h2::client::sans_io::Session` creates no socket, runtime, background task or
timer. Use it with Mio or another event loop. Framing, HPACK, flow control and
protocol validation are shared with the retained async client and server.

## Install

Enable the `sans-io` feature, keeping `h2` as the dependency name:

```toml
[dependencies]
h2 = { package = "h2-forked", version = "0.4.21", features = ["sans-io"] }
```

## Sans-I/O client

```rust
use h2::client::{Builder, sans_io::Session};
use std::time::Instant;

let now = Instant::now();
let mut session = Session::new(Builder::new(), now);
session.drive(now)?;
let bytes_to_write = session.output();
```

The caller owns transport I/O, readiness and request deadlines:

1. Fill `input()` with received bytes and call `drive(now)`.
2. Drain `next_event()` and release consumed DATA with `release_capacity`.
3. Write borrowed `output()` segments, then report accepted bytes with
   `advance_output(count)` and drive again.
4. Continue while progress is possible; wait for transport readiness otherwise.

After the peer's SETTINGS arrive, `is_ready()` and `available_streams()` describe
request capacity. `send_request` opens a stream; `reset(stream, Reason::CANCEL)`
cancels that stream. `close` queues GOAWAY: keep driving and draining output
until shutdown completes. Notify the session of transport EOF with `end_input`.

Server push is disabled for this client. Internal stream handles retain the
synchronized store; stream creation is not allocation-free.

## Example: brk with Mio

In one split-host Linux comparison using `h2-forked 0.4.20`, brk's Mio +
Sans-I/O path delivered **47% more responses/s** with **33% less client CPU per
response** than its async path:

| Client | Responses/s | Client CPU µs/response | Mean outstanding |
| --- | ---: | ---: | ---: |
| Async | 185,868 | 10.06 | 1,020 |
| Mio + Sans-I/O | 273,984 | 6.73 | 765 |

H2c GET with a two-byte `ok` response. Target: 128 connections × 8 streams,
two client threads, four responder workers,
10-second phases and 100-ms deadlines; four runs per client, all error-free.
Achieved concurrency differed, so this is an observed result rather than a
demonstrated gain at equal achieved load.

## Tokio APIs

The existing async APIs remain available without enabling `sans-io`:

```toml
[dependencies]
h2 = { package = "h2-forked", version = "0.4.21" }
```

The existing `h2::client::handshake` and `h2::server::handshake` APIs accept an
async transport supplied by the caller. TCP, TLS/ALPN, HTTP/1.1 upgrades,
connection pooling and application routing remain outside this crate.
The crate still depends on Tokio for its async adapters and shared machinery.

## Fork scope

Based on [Hyperium h2](https://github.com/hyperium/h2) v0.4.20. The Sans-I/O
client is a fork extension, not an upstream h2 API. The fork has its own patch
releases and distinct Rust types. Keep dependencies that exchange h2 types on
the same package.

The original implementation and MIT license are retained. See the
[API documentation](https://docs.rs/h2-forked) and [changelog](CHANGELOG.md).
