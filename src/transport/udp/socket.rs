use bytes::BytesMut;
use tokio::{
    net::UdpSocket,
    sync::{mpsc, watch},
};

use crate::{
    channel::{command::DatagramCommand, DatagramChannel},
    context::{BusinessContext, DatagramContext, DatagramInfo, InboundContext, OutboundContext},
    life::Life,
    pipeline::datagram::runtime::{flush_datagram, DatagramRuntimePipeline},
    transport::{shutdown, udp::config::UdpSocketConfig},
    Error, Result,
};

pub(crate) struct DatagramSocketRuntime<P>
where
    P: DatagramRuntimePipeline,
{
    pub id: u64,
    pub socket: UdpSocket,
    pub pipeline: P,
    pub config: UdpSocketConfig,
    pub channel: DatagramChannel<P::Write>,
    pub rx: mpsc::Receiver<DatagramCommand<P::Write>>,
    pub shutdown_rx: Option<watch::Receiver<bool>>,
}

pub(crate) async fn run_datagram_socket_with_life<P, L>(
    runtime: DatagramSocketRuntime<P>,
    life: L,
) -> Result<()>
where
    P: DatagramRuntimePipeline,
    L: Life,
{
    let DatagramSocketRuntime {
        id,
        socket,
        mut pipeline,
        config,
        channel,
        mut rx,
        mut shutdown_rx,
    } = runtime;

    let local_addr = socket.local_addr()?;
    let read_buffer_capacity = config
        .read_buffer_capacity
        .max(config.max_datagram_size)
        .max(1);
    let mut read_buf = vec![0_u8; read_buffer_capacity];
    let mut write_buf = BytesMut::with_capacity(config.write_buffer_capacity);

    life.udp_socket_started(local_addr).await?;

    let result: Result<()> = async {
        loop {
            if shutdown::requested(&shutdown_rx) {
                break;
            }

            tokio::select! {
                read = socket.recv_from(&mut read_buf) => {
                    let (read_len, peer_addr) = read?;

                    if read_len > config.max_datagram_size {
                        return Err(Error::DatagramTooLarge {
                            current: read_len,
                            max: config.max_datagram_size,
                        });
                    }

                    let msg = pipeline.decode_datagram(&read_buf[..read_len])?;
                    let info = DatagramInfo::new(id, peer_addr, local_addr);
                    let mut inbound_ctx = InboundContext::new_datagram(info);
                    let mut business_ctx = BusinessContext::new_datagram(info);
                    let mut ctx = DatagramContext::new(info, channel.clone());
                    let mut outbound_ctx = OutboundContext::new_datagram(info);

                    pipeline
                        .process_inbound_flushable(
                            &mut inbound_ctx,
                            &mut business_ctx,
                            &mut outbound_ctx,
                            &mut ctx,
                            &socket,
                            &mut write_buf,
                            msg,
                        )
                        .await?;

                    if ctx.close_requested() {
                        return Ok(());
                    }
                }

                cmd = rx.recv() => {
                    match cmd {
                        Some(DatagramCommand::WriteTo(peer_addr, msg)) => {
                            let info = DatagramInfo::new(id, peer_addr, local_addr);
                            let mut outbound_ctx = OutboundContext::new_datagram(info);

                            pipeline
                                .process_outbound(&mut outbound_ctx, msg, &mut write_buf)
                                .await?;

                            flush_datagram(&socket, peer_addr, &mut write_buf).await?;
                        }
                        Some(DatagramCommand::WriteToAndFlush(peer_addr, msg, done)) => {
                            let info = DatagramInfo::new(id, peer_addr, local_addr);
                            let mut outbound_ctx = OutboundContext::new_datagram(info);

                            let result = async {
                                pipeline
                                    .process_outbound(&mut outbound_ctx, msg, &mut write_buf)
                                    .await?;
                                flush_datagram(&socket, peer_addr, &mut write_buf).await
                            }
                            .await;

                            let ack = match &result {
                                Ok(()) => Ok(()),
                                Err(err) => Err(Error::Pipeline(format!(
                                    "write_to_and_flush failed: {err}"
                                ))),
                            };
                            let _ = done.send(ack);
                            result?;
                        }
                        Some(DatagramCommand::Close) | None => {
                            break;
                        }
                    }
                }

                _ = shutdown::wait(&mut shutdown_rx) => {
                    break;
                }
            }
        }

        Ok(())
    }
    .await;

    match result {
        Ok(()) => life.udp_socket_stopped(local_addr).await,
        Err(err) => {
            if let Err(life_err) = life.udp_socket_stopped(local_addr).await {
                tracing::debug!(
                    local_addr = %local_addr,
                    error = ?life_err,
                    "udp life hook failed while stopping errored socket"
                );
            }

            Err(err)
        }
    }
}
