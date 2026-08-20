use std::net::TcpStream;
use std::fs;
use std::sync::{Arc, atomic::{AtomicBool, Ordering}};
use std::sync::Once;
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use anyhow::Result;
use nakamoto_client::{Client, handle::Handle as _};
use nakamoto_common::bitcoin::network::constants::ServiceFlags;
use nakamoto_client::Event;
use nakamoto_net_poll::Reactor;
use tracing::{error, info, warn};
use tracing_subscriber::FmtSubscriber;

use crate::{
    config::{Config, NakamotoConfig},
    electrum_server::{ElectrumServer, FeeRateState, TransactionBroadcaster},
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
            Err(mpsc::RecvTimeoutError::Timeout) => break (handle, thread_handle),
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                let _ = thread_handle.join();
                return Err(anyhow::anyhow!("nakamoto client startup channel disconnected"));
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
    )?;

    info!("Electrum server ready on {}", cfg.electrum_listen_addr);
    server.run(source, Arc::clone(&shutdown))?;

    if shutdown.load(Ordering::Relaxed) {
        return Ok(());
    }
    let _ = client_thread.join();
    let _ = fee_rate_thread.join();
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
    let (handle, client_runner) = loop {
        if shutdown.load(Ordering::Relaxed) {
            return Ok(());
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
            Err(mpsc::RecvTimeoutError::Timeout) => break (handle, thread_handle),
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                let _ = thread_handle.join();
                return Err(anyhow::anyhow!("nakamoto client startup channel disconnected"));
            }
        }
    };
    let _client_shutdown_watcher = spawn_shutdown_watcher(handle.clone(), Arc::clone(&shutdown));

    let events = handle.events();

    let event_logger = thread::Builder::new()
        .name("nk-events".into())
        .spawn(move || {
            for event in &events {
                info!(target: "nakamoto", ?event);
            }
        })?;

    if shutdown.load(Ordering::Relaxed) {
        return Ok(());
    }
    let _ = client_runner.join();
    let _ = event_logger.join();
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
