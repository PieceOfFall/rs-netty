/// UDP socket runtime configuration.
#[derive(Clone)]
pub struct UdpSocketConfig {
    /// Receive buffer size used by the socket task.
    pub read_buffer_capacity: usize,
    /// Initial write buffer capacity.
    pub write_buffer_capacity: usize,
    /// Maximum accepted datagram payload size.
    pub max_datagram_size: usize,
    /// Bounded outbound command queue size.
    pub outbound_queue_size: usize,
}

impl UdpSocketConfig {
    pub(crate) fn normalize(&mut self) {
        self.max_datagram_size = self.max_datagram_size.max(1);
        self.read_buffer_capacity = self.read_buffer_capacity.max(self.max_datagram_size).max(1);
    }
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
