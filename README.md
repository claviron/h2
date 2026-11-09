# h2-forked

HTTP/2 with Tokio client/server APIs and an optional buffer-driven client.
A fork of [Hyperium h2](https://github.com/hyperium/h2), based on v0.4.20.

[![Crates.io](https://img.shields.io/crates/v/h2-forked.svg)](https://crates.io/crates/h2-forked)
[![Documentation](https://docs.rs/h2-forked/badge.svg)](https://docs.rs/h2-forked)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

The async client and server retain the upstream API. The `sans-io` feature adds
`h2::client::sans_io::Session` for clients driven by Mio or another event loop.
Both paths share framing, HPACK, flow control and protocol validation.

## Install

Keep `h2` as the dependency name when replacing upstream:

```toml
[dependencies]
h2 = { package = "h2-forked", version = "0.4.20" }
```

For the buffer-driven client:

```toml
[dependencies]
h2 = { package = "h2-forked", version = "0.4.20", features = ["sans-io"] }
```

## Buffer-driven client

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

No socket, executor, background task or timer is created by this API. The crate
still depends on Tokio for its async adapters and shared protocol machinery.
Server push is disabled for the buffer-driven client. Internal stream handles
retain the synchronized store; stream creation is not allocation-free.

## Tokio APIs

The existing `h2::client::handshake` and `h2::server::handshake` APIs accept an
async transport supplied by the caller. TCP, TLS/ALPN, HTTP/1.1 upgrades,
connection pooling and application routing remain outside this crate.

## Fork scope

The buffer-driven client is a fork extension, not an upstream h2 API. A matching
base version identifies the upstream starting point; it does not make the two
packages interchangeable Rust types. Keep dependencies that exchange h2 types
on the same package.

The original implementation and MIT license are retained. See the
[API documentation](https://docs.rs/h2-forked) and [changelog](CHANGELOG.md).
