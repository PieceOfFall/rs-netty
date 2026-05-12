#[derive(Clone)]
pub struct UdpSocketConfig {
    pub read_buffer_capacity: usize,
    pub write_buffer_capacity: usize,
    pub max_datagram_size: usize,
    pub outbound_queue_size: usize,
}

impl Default for UdpSocketConfig {
    fn default() -> Self {
        Self {
            read_buffer_capacity: 64 * 1024,
            write_buffer_capacity: 8 * 1024,
            max_datagram_size: 64 * 1024,
            outbound_queue_size: 1024,
        }
    }
}
