use std::net::TcpStream;
use std::sync::Arc;
use std::thread;

use anyhow::Result;
use nakamoto_client::{Client, handle::Handle as _};
use nakamoto_common::bitcoin::network::constants::ServiceFlags;
use nakamoto_net_poll::Reactor;
use tracing::{error, info};
use tracing_subscriber::FmtSubscriber;

use crate::{
    config::{Config, NakamotoConfig},
    electrum_server::ElectrumServer,
    indexer::Indexer,
    metrics::Metrics,
    nakamoto_source::NakamotoBlockSource,
};

type NodeReactor = Reactor<TcpStream>;

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
    nk_cfg.root = cfg.index_dir.clone();
    nk_cfg.connect = cfg.nakamoto_peers.clone();

    let client = Client::<NodeReactor>::new()?;
    let handle = client.handle();

    let client_thread = {
        let nk_cfg = nk_cfg.clone();
        thread::Builder::new()
            .name("nakamoto".into())
            .spawn(move || {
                if let Err(e) = client.run(nk_cfg) {
                    error!("nakamoto client exited: {e:#}");
                }
            })?
    };

    let metrics = Metrics::new();
    let source = Arc::new(NakamotoBlockSource::new(handle.clone()));
    let indexer = Indexer::new(metrics.clone());

    let _indexer_thread = indexer.clone().start(source.as_ref());

    info!("waiting for nakamoto to connect to peers...");
    match handle.wait_for_peers(1, ServiceFlags::NONE) {
        Ok(_) => info!("nakamoto connected to peers"),
        Err(e) => {
            error!("wait_for_peers failed: {e}; continuing anyway");
        }
    }

    let server = ElectrumServer::bind(cfg.electrum_listen_addr, indexer, metrics)?;

    info!("Electrum server ready on {}", cfg.electrum_listen_addr);
    server.run(source)?;

    let _ = client_thread.join();
    Ok(())
}

pub fn run_nakamoto(cfg: NakamotoConfig) -> Result<()> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(cfg.log_level)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    let client = Client::<NodeReactor>::new()?;
    let handle = client.handle();
    let events = handle.events();

    let nk_network = match cfg.network {
        crate::Network::Mainnet => nakamoto_client::Network::Mainnet,
        crate::Network::Testnet => nakamoto_client::Network::Testnet,
        crate::Network::Signet => nakamoto_client::Network::Signet,
        crate::Network::Regtest => nakamoto_client::Network::Regtest,
    };
    let mut config = nakamoto_client::Config::new(nk_network);
    config.root = cfg.index_dir.clone();
    config.connect = cfg.nakamoto_peers.clone();

    let event_logger = thread::Builder::new()
        .name("nk-events".into())
        .spawn(move || {
            for event in &events {
                info!(target: "nakamoto", ?event);
            }
        })?;

    let client_runner = thread::Builder::new()
        .name("nakamoto".into())
        .spawn(move || {
            if let Err(e) = client.run(config) {
                error!(target: "nakamoto", "client exited: {e:#}");
            }
        })?;

    let _ = client_runner.join();
    let _ = event_logger.join();
    Ok(())
}

pub fn run_electrs() -> Result<()> {
    electrs::run().map_err(Into::into)
}
