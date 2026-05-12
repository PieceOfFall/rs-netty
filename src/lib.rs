#![deny(unsafe_code)]

pub mod channel;
pub mod client;
pub mod codec;
pub mod context;
pub mod error;
pub mod pipeline;
pub mod server;
pub mod traits;
pub mod transport;

pub use channel::{Channel, DatagramChannel};
pub use context::{BusinessContext, Context, DatagramContext, InboundContext, OutboundContext};
pub use error::{Error, Result};
pub use pipeline::builder::pipeline;
pub use pipeline::datagram::builder::datagram_pipeline;
pub use traits::{Business, DatagramHandler, Flow, Handler, Inbound, Outbound};
pub use transport::tcp::client::{TcpClient, TcpClientHandle};
pub use transport::tcp::server::TcpServer;
pub use transport::udp::client::{UdpClient, UdpClientHandle};
pub use transport::udp::server::UdpServer;
