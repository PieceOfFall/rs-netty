#![deny(unsafe_code)]
//! Tokio-native typed TCP/UDP pipeline framework inspired by Netty.
//!
//! `rs-netty` keeps the familiar channel, pipeline, and handler shape while
//! using Rust's type system to validate pipeline order and message transitions
//! at compile time.
//!
//! # Quick start
//!
//! ```no_run
//! use rs_netty::{codec::LineCodec, pipeline, Context, Handler, Result, TcpServer};
//!
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!     TcpServer::bind("127.0.0.1:9000")
//!         .pipeline(|| pipeline().codec(LineCodec::new()).handler(Echo))
//!         .run()
//!         .await
//! }
//!
//! struct Echo;
//!
//! impl Handler<String> for Echo {
//!     type Write = String;
//!
//!     async fn read(&mut self, ctx: &mut Context<Self::Write>, msg: String) -> Result<()> {
//!         ctx.write(msg).await
//!     }
//! }
//! ```
//!
//! # Pipeline shape
//!
//! TCP pipelines are built with [`pipeline()`] and UDP pipelines are built with
//! [`datagram_pipeline()`]. The builder only exposes methods that are valid in
//! the current state, so invalid orderings such as adding a handler before a
//! codec fail to compile.
//!
//! A TCP pipeline has this shape:
//!
//! ```text
//! codec -> inbound* -> business* -> handler -> outbound*
//! ```
//!
//! A UDP pipeline has the same typed stage model, but processes whole datagrams
//! rather than a byte stream.
//!
//! # Write and flush semantics
//!
//! Writes issued through [`Context`] or [`DatagramContext`] are staged in the
//! current handler's outbox and are flushed when the handler returns, or earlier
//! when `flush`/`write_and_flush` is awaited.
//!
//! Writes issued through [`Channel`], [`TcpClientHandle`], [`DatagramChannel`],
//! or [`UdpClientHandle`] are sent through the connection/socket command queue.
//! The queue is bounded by the configured outbound queue size.

pub mod channel;
pub mod client;
pub mod codec;
pub mod context;
pub mod error;
pub mod life;
pub mod pipeline;
pub mod server;
pub mod traits;
pub mod transport;

pub use channel::{Channel, DatagramChannel};
pub use context::{
    BusinessContext, ConnInfo, ConnectionStats, Context, DatagramContext, DatagramInfo,
    InboundContext, OutboundContext,
};
pub use error::{Error, Result};
pub use life::{CloseReason, Life, NoLife};
pub use pipeline::builder::pipeline;
pub use pipeline::datagram::builder::datagram_pipeline;
pub use traits::{Business, DatagramHandler, Flow, Handler, Inbound, Outbound};
pub use transport::tcp::client::{TcpClient, TcpClientHandle};
pub use transport::tcp::server::TcpServer;
pub use transport::udp::client::{UdpClient, UdpClientHandle};
pub use transport::udp::server::UdpServer;
