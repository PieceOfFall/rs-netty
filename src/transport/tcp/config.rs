use std::time::Duration;

#[derive(Clone)]
pub struct TcpConnectionConfig {
    pub read_buffer_capacity: usize,
    pub write_buffer_capacity: usize,
    pub max_frame_size: usize,
    pub outbound_queue_size: usize,
    pub tcp_nodelay: bool,
    pub idle_timeout: Option<Duration>,
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
