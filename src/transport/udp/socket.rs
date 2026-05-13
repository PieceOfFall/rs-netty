use std::{future, net::SocketAddr};

use bytes::BytesMut;
use tokio::{
    net::UdpSocket,
    sync::{mpsc, watch},
};

use crate::{
    channel::{command::DatagramCommand, DatagramChannel},
    context::{BusinessContext, DatagramContext, DatagramInfo, InboundContext, OutboundContext},
    life::Life,
    pipeline::datagram::runtime::DatagramRuntimePipeline,
    transport::udp::config::UdpSocketConfig,
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
    let mut read_buf = vec![0_u8; config.read_buffer_capacity.max(1)];
    let mut write_buf = BytesMut::with_capacity(config.write_buffer_capacity);

    life.udp_socket_started(local_addr).await?;

    let result: Result<()> = async {
        loop {
            if shutdown_requested(&shutdown_rx) {
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
                        .process_inbound(&mut inbound_ctx, &mut business_ctx, &mut ctx, msg)
                        .await?;

                    drain_pending_writes(
                        &socket,
                        &mut pipeline,
                        &mut outbound_ctx,
                        &mut ctx,
                        &mut write_buf,
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
                        Some(DatagramCommand::Close) | None => {
                            break;
                        }
                    }
                }

                _ = wait_for_shutdown(&mut shutdown_rx) => {
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

fn shutdown_requested(shutdown_rx: &Option<watch::Receiver<bool>>) -> bool {
    shutdown_rx
        .as_ref()
        .is_some_and(|shutdown_rx| *shutdown_rx.borrow())
}

async fn wait_for_shutdown(shutdown_rx: &mut Option<watch::Receiver<bool>>) {
    let Some(shutdown_rx) = shutdown_rx else {
        future::pending::<()>().await;
        return;
    };

    let _ = shutdown_rx.changed().await;
}

async fn drain_pending_writes<P>(
    socket: &UdpSocket,
    pipeline: &mut P,
    outbound_ctx: &mut OutboundContext,
    ctx: &mut DatagramContext<P::Write>,
    write_buf: &mut BytesMut,
) -> Result<()>
where
    P: DatagramRuntimePipeline,
{
    let writes = ctx.take_pending_writes();

    for (peer_addr, msg) in writes {
        pipeline
            .process_outbound(outbound_ctx, msg, write_buf)
            .await?;

        flush_datagram(socket, peer_addr, write_buf).await?;
    }

    Ok(())
}

async fn flush_datagram(
    socket: &UdpSocket,
    peer_addr: SocketAddr,
    write_buf: &mut BytesMut,
) -> Result<()> {
    if !write_buf.is_empty() {
        socket.send_to(write_buf, peer_addr).await?;
        write_buf.clear();
    }

    Ok(())
}
