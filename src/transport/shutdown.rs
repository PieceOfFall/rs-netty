use std::future;

use tokio::sync::watch;

pub(crate) fn requested(shutdown_rx: &Option<watch::Receiver<bool>>) -> bool {
    shutdown_rx
        .as_ref()
        .is_some_and(|shutdown_rx| *shutdown_rx.borrow())
}

pub(crate) async fn wait(shutdown_rx: &mut Option<watch::Receiver<bool>>) {
    let Some(shutdown_rx) = shutdown_rx else {
        future::pending::<()>().await;
        return;
    };

    let _ = shutdown_rx.changed().await;
}
