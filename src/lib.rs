#![deny(unsafe_code)]

pub mod channel;
pub mod codec;
pub mod context;
pub mod error;
pub mod pipeline;
pub mod server;
pub mod traits;

pub use channel::Channel;
pub use context::{BusinessContext, Context, InboundContext, OutboundContext};
pub use error::{Error, Result};
pub use pipeline::builder::pipeline;
pub use server::TcpServer;
pub use traits::{Business, Flow, Handler, Inbound, Outbound};
