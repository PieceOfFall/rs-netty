use std::time::Duration;

/// TCP connection runtime configuration.
///
/// The same settings are used by accepted server connections and client
/// connections.
#[derive(Clone)]
pub struct TcpConnectionConfig {
    /// Initial read buffer capacity.
    pub read_buffer_capacity: usize,
    /// Initial write buffer capacity.
    pub write_buffer_capacity: usize,
    /// Maximum buffered frame size before closing the connection.
    pub max_frame_size: usize,
    /// Bounded outbound command queue size.
    pub outbound_queue_size: usize,
    /// Whether `TCP_NODELAY` is enabled.
    pub tcp_nodelay: bool,
    /// Optional idle timeout for the connection loop.
    pub idle_timeout: Option<Duration>,
    /// Whether per-connection stats are collected.
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
