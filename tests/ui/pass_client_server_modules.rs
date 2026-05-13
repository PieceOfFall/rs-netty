use rs_netty::{client, server};

fn main() {
    let _tcp_server = server::TcpServer::bind("127.0.0.1:0");
    let _udp_server = server::UdpServer::bind("127.0.0.1:0");

    let _tcp_client = client::TcpClient::connect("127.0.0.1:0");
    let _udp_client = client::UdpClient::connect("127.0.0.1:0");
}
