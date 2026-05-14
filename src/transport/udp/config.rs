/// UDP socket runtime configuration.
#[derive(Clone)]
pub struct UdpSocketConfig {
    /// Receive buffer size in bytes used by the socket task.
    ///
    /// The runtime normalizes this to at least [`Self::max_datagram_size`] so a
    /// configured maximum datagram can fit in the receive buffer.
    pub read_buffer_capacity: usize,
    /// Initial write buffer capacity in bytes.
    pub write_buffer_capacity: usize,
    /// Maximum accepted datagram payload size in bytes.
    ///
    /// Oversized datagrams are rejected before the datagram pipeline runs.
    pub max_datagram_size: usize,
    /// Bounded outbound command queue size for writes sent through channels.
    ///
    /// Calls such as [`crate::DatagramChannel::write_to`] wait for capacity
    /// when this queue is full.
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
