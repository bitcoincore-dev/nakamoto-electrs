use std::fs;
use std::net::TcpStream;
use std::path::Path;
use std::sync::Once;
use std::sync::mpsc;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::Result;
use crossbeam_channel::RecvTimeoutError;
use nakamoto_client::Event;
use nakamoto_client::{Client, handle::Handle as _};
use nakamoto_common::bitcoin::network::constants::ServiceFlags;
use nakamoto_net_poll::Reactor;
use tracing::{error, info, warn};
use tracing_subscriber::FmtSubscriber;

use crate::{
    config::{Config, NakamotoConfig},
    electrum_server::{
        ElectrumServer, FeeRateState, PendingChangeBroadcaster, TransactionBroadcaster,
        apply_tx_status_change,
    },
    indexer::Indexer,
    metrics::Metrics,
    nakamoto_source::NakamotoBlockSource,
};

type NodeReactor = Reactor<TcpStream>;

const CLIENT_STARTUP_WAIT: Duration = Duration::from_secs(2);
static SHUTDOWN_HANDLER_INSTALLED: Once = Once::new();

pub fn run_bridge(cfg: Config) -> Result<()> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(cfg.log_level)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    info!(
        "nakamoto-electrs starting (network={})",
        match cfg.network {
            crate::Network::Mainnet => "mainnet",
            crate::Network::Testnet => "testnet",
            crate::Network::Signet => "signet",
            crate::Network::Regtest => "regtest",
        }
    );

    let nk_network = match cfg.network {
        crate::Network::Mainnet => nakamoto_client::Network::Mainnet,
        crate::Network::Testnet => nakamoto_client::Network::Testnet,
        crate::Network::Signet => nakamoto_client::Network::Signet,
        crate::Network::Regtest => nakamoto_client::Network::Regtest,
    };
    let mut nk_cfg = nakamoto_client::Config::new(nk_network);
    nk_cfg.root = cfg.index_dir.join("nakamoto");
    nk_cfg.connect = cfg.nakamoto_peers.clone();
    let cache_dir = nk_cfg.root.join(".nakamoto");
    let legacy_cache_dir = cfg.index_dir.join(".nakamoto");
    let shutdown = Arc::new(AtomicBool::new(false));
    install_shutdown_handler(Arc::clone(&shutdown))?;
    let (handle, client_thread) = loop {
        if shutdown.load(Ordering::Relaxed) {
            return Ok(());
        }
        if clear_corrupt_nakamoto_cache(&cache_dir, &legacy_cache_dir, nk_network) {
            continue;
        }
        let client = Client::<NodeReactor>::new()?;
        let handle = client.handle();
        let (tx, rx) = mpsc::channel::<String>();
        let nk_cfg = nk_cfg.clone();
        let thread_handle = thread::Builder::new()
            .name("nakamoto".into())
            .spawn(move || {
                if let Err(e) = client.run(nk_cfg) {
                    let _ = tx.send(e.to_string());
                    error!("nakamoto client exited: {e:#}");
                }
            })?;

        match rx.recv_timeout(CLIENT_STARTUP_WAIT) {
            Ok(err) if err.contains("stored genesis header doesn't match network genesis") => {
                error!(
                    "nakamoto cache mismatch detected; clearing {:?} and retrying",
                    cache_dir
                );
                let _ = fs::remove_dir_all(&cache_dir);
                let _ = fs::remove_dir_all(&legacy_cache_dir);
                let _ = thread_handle.join();
                continue;
            }
            Ok(err) => {
                let _ = thread_handle.join();
                return Err(anyhow::anyhow!(err));
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {
                if shutdown.load(Ordering::Relaxed) {
                    let _ = handle.shutdown();
                    let _ = thread_handle.join();
                    return Ok(());
                }
                break (handle, thread_handle);
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                let _ = thread_handle.join();
                return Err(anyhow::anyhow!(
                    "nakamoto client startup channel disconnected"
                ));
            }
        }
    };
    let _client_shutdown_watcher = spawn_shutdown_watcher(handle.clone(), Arc::clone(&shutdown));

    let metrics = Metrics::new();
    let source = Arc::new(NakamotoBlockSource::new(
        handle.clone(),
        Arc::clone(&shutdown),
    ));
    let indexer = Indexer::new(cfg.index_dir.join("index"), metrics.clone())?;
    let broadcaster: Arc<dyn TransactionBroadcaster> = Arc::new(handle.clone());
    let fee_rate = Arc::new(FeeRateState::new());
    let pending_changes = PendingChangeBroadcaster::default();

    let _indexer_thread = indexer.clone().start(source.as_ref());
    let fee_events = handle.events();
    let fee_rate_thread = {
        let fee_rate = Arc::clone(&fee_rate);
        thread::Builder::new()
            .name("fee-rate".into())
            .spawn(move || {
                for event in &fee_events {
                    if let Event::FeeEstimated { fees, .. } = event {
                        fee_rate.update_sat_per_vb(fees.median);
                    }
                }
            })?
    };
    let tx_status_events = handle.events();
    let tx_status_thread = {
        let indexer = indexer.clone();
        let pending_changes = pending_changes.clone();
        let shutdown = Arc::clone(&shutdown);
        thread::Builder::new()
            .name("tx-status".into())
            .spawn(move || {
                loop {
                    if shutdown.load(Ordering::Relaxed) {
                        break;
                    }
                    match tx_status_events.recv_timeout(Duration::from_millis(200)) {
                        Ok(Event::TxStatusChanged { txid, status }) => {
                            if let Err(e) = apply_tx_status_change(
                                &indexer,
                                &pending_changes,
                                &txid.to_string(),
                                &status.to_string(),
                            ) {
                                error!(target: "nakamoto", "tx status handling failed: {e:#}");
                            }
                        }
                        Ok(_) => {}
                        Err(RecvTimeoutError::Timeout) => continue,
                        Err(RecvTimeoutError::Disconnected) => break,
                    }
                }
            })?
    };

    info!("waiting for nakamoto to connect to peers...");
    let (peers_tx, peers_rx) = mpsc::channel::<Result<(), String>>();
    let wait_handle = {
        let handle = handle.clone();
        let shutdown = Arc::clone(&shutdown);
        thread::Builder::new()
            .name("wait-for-peers".into())
            .spawn(move || {
                let result = loop {
                    if shutdown.load(Ordering::Relaxed) {
                        break Err("shutdown requested".to_string());
                    }
                    match handle.wait_for_peers(1, ServiceFlags::NONE) {
                        Ok(_) => break Ok(()),
                        Err(e) => break Err(e.to_string()),
                    }
                };
                let _ = peers_tx.send(result);
            })?
    };
    let started_at = Instant::now();
    let mut last_warn = Instant::now();
    loop {
        if shutdown.load(Ordering::Relaxed) {
            return Ok(());
        }
        match peers_rx.recv_timeout(Duration::from_secs(5)) {
            Ok(Ok(())) => {
                info!("nakamoto connected to peers");
                break;
            }
            Ok(Err(e)) => {
                error!("wait_for_peers failed: {e}; continuing anyway");
                break;
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {
                if shutdown.load(Ordering::Relaxed) {
                    return Ok(());
                }
                if started_at.elapsed() >= Duration::from_secs(15)
                    && last_warn.elapsed() >= Duration::from_secs(15)
                {
                    warn!(
                        "still waiting for nakamoto to connect to peers after {:?}",
                        started_at.elapsed()
                    );
                    last_warn = Instant::now();
                }
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                error!("wait_for_peers failed: command channel disconnected; continuing anyway");
                break;
            }
        }
    }

    if shutdown.load(Ordering::Relaxed) {
        return Ok(());
    }

    let server = ElectrumServer::bind(
        cfg.electrum_listen_addr,
        indexer,
        metrics,
        Some(broadcaster),
        fee_rate,
        pending_changes,
    )?;

    info!("Electrum server ready on {}", cfg.electrum_listen_addr);
    server.run(source, Arc::clone(&shutdown))?;

    if shutdown.load(Ordering::Relaxed) {
        return Ok(());
    }
    let _ = client_thread.join();
    let _ = fee_rate_thread.join();
    let _ = tx_status_thread.join();
    let _ = wait_handle.join();
    Ok(())
}

pub fn run_nakamoto(cfg: NakamotoConfig) -> Result<()> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(cfg.log_level)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    let nk_network = match cfg.network {
        crate::Network::Mainnet => nakamoto_client::Network::Mainnet,
        crate::Network::Testnet => nakamoto_client::Network::Testnet,
        crate::Network::Signet => nakamoto_client::Network::Signet,
        crate::Network::Regtest => nakamoto_client::Network::Regtest,
    };
    let mut config = nakamoto_client::Config::new(nk_network);
    config.root = cfg.index_dir.join("nakamoto");
    config.connect = cfg.nakamoto_peers.clone();

    let cache_dir = config.root.join(".nakamoto");
    let legacy_cache_dir = cfg.index_dir.join(".nakamoto");
    let shutdown = Arc::new(AtomicBool::new(false));
    install_shutdown_handler(Arc::clone(&shutdown))?;
    let (handle, _client_runner) = loop {
        if shutdown.load(Ordering::Relaxed) {
            return Ok(());
        }
        if clear_corrupt_nakamoto_cache(&cache_dir, &legacy_cache_dir, nk_network) {
            continue;
        }
        let client = Client::<NodeReactor>::new()?;
        let handle = client.handle();
        let (tx, rx) = mpsc::channel::<String>();
        let config = config.clone();
        let thread_handle = thread::Builder::new()
            .name("nakamoto".into())
            .spawn(move || {
                if let Err(e) = client.run(config) {
                    let _ = tx.send(e.to_string());
                    error!(target: "nakamoto", "client exited: {e:#}");
                }
            })?;

        match rx.recv_timeout(CLIENT_STARTUP_WAIT) {
            Ok(err) if err.contains("stored genesis header doesn't match network genesis") => {
                error!(
                    target: "nakamoto",
                    "nakamoto cache mismatch detected; clearing {:?} and retrying",
                    cache_dir
                );
                let _ = fs::remove_dir_all(&cache_dir);
                let _ = fs::remove_dir_all(&legacy_cache_dir);
                let _ = thread_handle.join();
                continue;
            }
            Ok(err) => {
                let _ = thread_handle.join();
                return Err(anyhow::anyhow!(err));
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {
                if shutdown.load(Ordering::Relaxed) {
                    let _ = handle.shutdown();
                    let _ = thread_handle.join();
                    return Ok(());
                }
                break (handle, thread_handle);
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                let _ = thread_handle.join();
                return Err(anyhow::anyhow!(
                    "nakamoto client startup channel disconnected"
                ));
            }
        }
    };
    let _client_shutdown_watcher = spawn_shutdown_watcher(handle.clone(), Arc::clone(&shutdown));

    let events = handle.events();

    let _event_logger = thread::Builder::new()
        .name("nk-events".into())
        .spawn(move || {
            for event in &events {
                info!(target: "nakamoto", ?event);
            }
        })?;

    wait_for_shutdown(Arc::clone(&shutdown));
    Ok(())
}

#[cfg(feature = "electrs-bin")]
pub fn run_electrs() -> Result<()> {
    electrs::run()
}

fn install_shutdown_handler(shutdown: Arc<AtomicBool>) -> Result<()> {
    SHUTDOWN_HANDLER_INSTALLED.call_once(|| {
        ctrlc::set_handler(move || {
            shutdown.store(true, Ordering::SeqCst);
        })
        .expect("failed to install Ctrl-C handler");
    });
    Ok(())
}

fn spawn_shutdown_watcher<H>(handle: H, shutdown: Arc<AtomicBool>) -> thread::JoinHandle<()>
where
    H: nakamoto_client::handle::Handle + Send + 'static,
{
    thread::Builder::new()
        .name("nakamoto-shutdown".into())
        .spawn(move || {
            while !shutdown.load(Ordering::Relaxed) {
                thread::sleep(Duration::from_millis(100));
            }
            let _ = handle.shutdown();
        })
        .expect("failed to spawn nakamoto shutdown watcher")
}

fn clear_corrupt_nakamoto_cache(
    cache_dir: &Path,
    legacy_cache_dir: &Path,
    network: nakamoto_client::Network,
) -> bool {
    let network_cache_dir = cache_dir.join(network.as_str());
    let headers = network_cache_dir.join("headers.db");
    let filters = network_cache_dir.join("filters.db");

    let headers_meta = fs::metadata(&headers);
    let filters_meta = fs::metadata(&filters);

    let headers_exists = headers_meta.is_ok();
    let filters_exists = filters_meta.is_ok();

    if !headers_exists && !filters_exists {
        return false;
    }

    let headers_len = headers_meta.as_ref().map(|m| m.len()).unwrap_or(0);
    let filters_len = filters_meta.as_ref().map(|m| m.len()).unwrap_or(0);
    let headers_ok = headers_exists && headers_len > 0 && headers_len % 80 == 0;
    let filters_ok = filters_exists && filters_len > 0 && filters_len % 64 == 0;
    let header_records = headers_len / 80;
    let filter_records = filters_len / 64;

    if headers_ok && filters_ok && header_records == filter_records {
        return false;
    }

    warn!(
        "nakamoto cache is corrupt at {:?}: headers.db exists={} len={} filters.db exists={} len={} records={} vs {}; clearing {:?} and retrying",
        network_cache_dir,
        headers_exists,
        headers_len,
        filters_exists,
        filters_len,
        header_records,
        filter_records,
        cache_dir
    );
    let _ = fs::remove_dir_all(cache_dir);
    let _ = fs::remove_dir_all(legacy_cache_dir);
    true
}

fn wait_for_shutdown(shutdown: Arc<AtomicBool>) {
    while !shutdown.load(Ordering::Relaxed) {
        thread::sleep(Duration::from_millis(100));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nakamoto_client::{Command, Link, Peer};
    use nakamoto_p2p::fsm::Event as FsmEvent;
    use std::net::SocketAddr;
    use std::sync::atomic::AtomicUsize;
    use tempfile::tempdir;

    #[derive(Clone)]
    struct MockHandle {
        shutdown_calls: Arc<AtomicUsize>,
    }

    impl MockHandle {
        fn new() -> Self {
            Self {
                shutdown_calls: Arc::new(AtomicUsize::new(0)),
            }
        }
    }

    impl nakamoto_client::handle::Handle for MockHandle {
        fn get_tip(
            &self,
        ) -> Result<
            (
                nakamoto_common::block::Height,
                nakamoto_common::block::BlockHeader,
                nakamoto_common::bitcoin::util::uint::Uint256,
            ),
            nakamoto_client::handle::Error,
        > {
            unreachable!()
        }
        fn get_block(
            &self,
            _hash: &nakamoto_common::block::BlockHash,
        ) -> Result<
            Option<(
                nakamoto_common::block::Height,
                nakamoto_common::block::BlockHeader,
            )>,
            nakamoto_client::handle::Error,
        > {
            unreachable!()
        }
        fn get_block_by_height(
            &self,
            _height: nakamoto_common::block::Height,
        ) -> Result<Option<nakamoto_common::block::BlockHeader>, nakamoto_client::handle::Error> {
            unreachable!()
        }
        fn request_block(
            &self,
            _hash: &nakamoto_common::block::BlockHash,
        ) -> Result<(), nakamoto_client::handle::Error> {
            unreachable!()
        }
        fn request_filters(
            &self,
            _range: std::ops::RangeInclusive<nakamoto_common::block::Height>,
        ) -> Result<(), nakamoto_client::handle::Error> {
            unreachable!()
        }
        fn query_tree(
            &self,
            _query: impl Fn(&dyn nakamoto_common::block::tree::BlockReader) + Send + Sync + 'static,
        ) -> Result<(), nakamoto_client::handle::Error> {
            unreachable!()
        }
        fn find_branch(
            &self,
            _to: &nakamoto_common::block::BlockHash,
        ) -> Result<
            Option<(
                nakamoto_common::block::Height,
                nakamoto_common::nonempty::NonEmpty<nakamoto_common::block::BlockHeader>,
            )>,
            nakamoto_client::handle::Error,
        > {
            unreachable!()
        }
        fn blocks(
            &self,
        ) -> crossbeam_channel::Receiver<(
            nakamoto_common::block::Block,
            nakamoto_common::block::Height,
        )> {
            unreachable!()
        }
        fn filters(
            &self,
        ) -> crossbeam_channel::Receiver<(
            nakamoto_common::block::filter::BlockFilter,
            nakamoto_common::block::BlockHash,
            nakamoto_common::block::Height,
        )> {
            unreachable!()
        }
        fn events(&self) -> crossbeam_channel::Receiver<nakamoto_client::Event> {
            unreachable!()
        }
        fn command(&self, _cmd: Command) -> Result<(), nakamoto_client::handle::Error> {
            unreachable!()
        }
        fn broadcast(
            &self,
            _msg: nakamoto_common::bitcoin::network::message::NetworkMessage,
            _predicate: fn(Peer) -> bool,
        ) -> Result<Vec<SocketAddr>, nakamoto_client::handle::Error> {
            unreachable!()
        }
        fn query(
            &self,
            _msg: nakamoto_common::bitcoin::network::message::NetworkMessage,
        ) -> Result<Option<SocketAddr>, nakamoto_client::handle::Error> {
            unreachable!()
        }
        fn connect(&self, _addr: SocketAddr) -> Result<Link, nakamoto_client::handle::Error> {
            unreachable!()
        }
        fn disconnect(&self, _addr: SocketAddr) -> Result<(), nakamoto_client::handle::Error> {
            unreachable!()
        }
        fn submit_transaction(
            &self,
            _tx: nakamoto_common::block::Transaction,
        ) -> Result<nakamoto_common::nonempty::NonEmpty<SocketAddr>, nakamoto_client::handle::Error>
        {
            unreachable!()
        }
        fn import_headers(
            &self,
            _headers: Vec<nakamoto_common::block::BlockHeader>,
        ) -> Result<
            Result<nakamoto_common::block::tree::ImportResult, nakamoto_common::block::tree::Error>,
            nakamoto_client::handle::Error,
        > {
            unreachable!()
        }
        fn import_addresses(
            &self,
            _addrs: Vec<nakamoto_common::bitcoin::network::Address>,
        ) -> Result<(), nakamoto_client::handle::Error> {
            unreachable!()
        }
        fn wait<F: FnMut(FsmEvent) -> Option<T>, T>(
            &self,
            _f: F,
        ) -> Result<T, nakamoto_client::handle::Error> {
            unreachable!()
        }
        fn wait_for_peers(
            &self,
            _count: usize,
            _required_services: impl Into<nakamoto_common::bitcoin::network::constants::ServiceFlags>,
        ) -> Result<
            Vec<(
                SocketAddr,
                nakamoto_common::block::Height,
                nakamoto_common::bitcoin::network::constants::ServiceFlags,
            )>,
            nakamoto_client::handle::Error,
        > {
            unreachable!()
        }
        fn wait_for_height(
            &self,
            _h: nakamoto_common::block::Height,
        ) -> Result<nakamoto_common::block::BlockHash, nakamoto_client::handle::Error> {
            unreachable!()
        }
        fn shutdown(self) -> Result<(), nakamoto_client::handle::Error> {
            self.shutdown_calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    #[test]
    fn shutdown_watcher_only_trips_after_flag_is_set() {
        let shutdown = Arc::new(AtomicBool::new(false));
        let handle = MockHandle::new();
        let calls = Arc::clone(&handle.shutdown_calls);
        let watcher = spawn_shutdown_watcher(handle, Arc::clone(&shutdown));

        thread::sleep(Duration::from_millis(150));
        assert_eq!(calls.load(Ordering::SeqCst), 0);

        shutdown.store(true, Ordering::SeqCst);
        watcher.join().unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn wait_for_shutdown_returns_when_flag_flips() {
        let shutdown = Arc::new(AtomicBool::new(false));
        let waiter = {
            let shutdown = Arc::clone(&shutdown);
            thread::spawn(move || wait_for_shutdown(shutdown))
        };

        thread::sleep(Duration::from_millis(100));
        shutdown.store(true, Ordering::SeqCst);
        waiter.join().unwrap();
    }

    #[test]
    fn clears_partial_nakamoto_cache() {
        let tmp = tempdir().unwrap();
        let cache_dir = tmp.path().join(".nakamoto-electrs");
        let legacy_cache_dir = tmp.path().join("legacy");
        let network_cache_dir = cache_dir.join(nakamoto_client::Network::Signet.as_str());
        fs::create_dir_all(&network_cache_dir).unwrap();
        fs::write(network_cache_dir.join("headers.db"), []).unwrap();
        fs::write(network_cache_dir.join("filters.db"), vec![0u8; 64]).unwrap();

        assert!(clear_corrupt_nakamoto_cache(
            &cache_dir,
            &legacy_cache_dir,
            nakamoto_client::Network::Signet
        ));
        assert!(!cache_dir.exists());
    }

    #[test]
    fn keeps_consistent_nakamoto_cache() {
        let tmp = tempdir().unwrap();
        let cache_dir = tmp.path().join(".nakamoto-electrs");
        let legacy_cache_dir = tmp.path().join("legacy");
        let network_cache_dir = cache_dir.join(nakamoto_client::Network::Signet.as_str());
        fs::create_dir_all(&network_cache_dir).unwrap();
        fs::write(network_cache_dir.join("headers.db"), vec![0u8; 80]).unwrap();
        fs::write(network_cache_dir.join("filters.db"), vec![0u8; 64]).unwrap();

        assert!(!clear_corrupt_nakamoto_cache(
            &cache_dir,
            &legacy_cache_dir,
            nakamoto_client::Network::Signet
        ));
        assert!(cache_dir.exists());
    }
}
