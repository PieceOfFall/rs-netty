use std::time::Duration;

/// TCP connection runtime configuration.
///
/// The same settings are used by accepted server connections and client
/// connections.
#[derive(Clone)]
pub struct TcpConnectionConfig {
    /// Initial read buffer capacity in bytes.
    ///
    /// The buffer can grow as Tokio reads more bytes, but a connection is
    /// closed if buffered data exceeds [`Self::max_frame_size`] before the
    /// codec can produce a frame.
    pub read_buffer_capacity: usize,
    /// Initial write buffer capacity in bytes.
    ///
    /// Encoded outbound frames are accumulated here before being written to the
    /// socket.
    pub write_buffer_capacity: usize,
    /// Maximum buffered frame size before closing the connection.
    pub max_frame_size: usize,
    /// Bounded outbound command queue size for writes sent through channels.
    ///
    /// Calls such as [`crate::Channel::write`] wait for capacity when this
    /// queue is full.
    pub outbound_queue_size: usize,
    /// Whether `TCP_NODELAY` is enabled.
    pub tcp_nodelay: bool,
    /// Optional timeout for closing a connection after no reads are received.
    ///
    /// Outbound writes do not reset this timer.
    pub idle_timeout: Option<Duration>,
    /// Whether byte and frame counters are collected for the connection.
    ///
    /// Disabled by default so applications that do not need stats avoid the
    /// shared counter allocations and atomic updates.
    pub track_connection_stats: bool,
}

impl Default for TcpConnectionConfig {
    fn default() -> Self {
        Self {
            read_buffer_capacity: 8 * 1024,
            write_buffer_capacity: 8 * 1024,
            max_frame_size: 1024 * 1024,
            outbound_queue_size: 1024,
            tcp_nodelay: true,
            idle_timeout: None,
            track_connection_stats: false,
        }
    }
}
