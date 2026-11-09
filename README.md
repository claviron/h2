# H2

A Tokio aware, HTTP/2 client & server implementation for Rust.

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![Crates.io](https://img.shields.io/crates/v/h2.svg)](https://crates.io/crates/h2)
[![Documentation](https://docs.rs/h2/badge.svg)][dox]

More information about this crate can be found in the [crate documentation][dox].

[dox]: https://docs.rs/h2

## Features

* Client and server HTTP/2 implementation.
* Implements the full HTTP/2 specification.
* Passes [h2spec](https://github.com/summerwind/h2spec).
* Focus on performance and correctness.
* Built on [Tokio](https://tokio.rs).

## Non goals

This crate is intended to only be an implementation of the HTTP/2
specification. It does not handle:

* Managing TCP connections
* HTTP 1.0 upgrade
* TLS
* Any feature not described by the HTTP/2 specification.

This crate is now used by [hyper](https://github.com/hyperium/hyper), which will provide all of these features.

## Usage

To use `h2`, first add this to your `Cargo.toml`:

```toml
[dependencies]
h2 = "0.4"
```

Next, add this to your crate:

```rust
extern crate h2;

use h2::server::Connection;

fn main() {
    // ...
}
```

## FAQ

**Is this an embedded Java SQL database engine?**

[No](https://www.h2database.com).

## Buffer-driven client (development)

The `sans-io` feature exposes `client::sans_io::Session`. The caller supplies
input bytes and monotonic time, drains borrowed output segments, and handles
response events. No socket, executor, or timer is created. The async client and
server continue to use the same framing, HPACK and protocol state machine.

Call `drive(now)` after input, output progress or a protocol operation. Drain
`next_event()` before waiting for transport readiness. `Data` events retain
receive-window credit until `release_capacity` is called. Request deadlines are
the caller's responsibility: `reset(stream, Reason::CANCEL)` affects that stream
alone. `close` queues GOAWAY; keep driving and draining output until complete.
Server push is disabled by this client API. Stream ownership still uses the
existing synchronized store; this is not an allocation-free implementation.

This feature is an experimental fork extension, not part of the published
upstream release. It is intended for readiness-driven clients such as Mio.
