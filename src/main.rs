use std::net::TcpStream;
use std::sync::Arc;
use std::thread;

use anyhow::Result;
use nakamoto_client::{Client, handle::Handle as _};
use nakamoto_common::bitcoin::network::constants::ServiceFlags;
use nakamoto_net_poll::Reactor;
use tracing::{error, info};
use tracing_subscriber::FmtSubscriber;

use nakamoto_electrs::{
    config::Config, electrum_server::ElectrumServer, indexer::Indexer, metrics::Metrics,
    nakamoto_source::NakamotoBlockSource,
};

type NodeReactor = Reactor<TcpStream>;

fn main() -> Result<()> {
    // ---- 1. Parse config ------------------------------------------------
    let cfg = Config::from_args();

    // ---- 2. Initialise tracing ------------------------------------------
    let subscriber = FmtSubscriber::builder()
        .with_max_level(cfg.log_level)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    info!(
        "nakamoto-electrs starting (network={})",
        match cfg.network {
            nakamoto_electrs::Network::Mainnet => "mainnet",
            nakamoto_electrs::Network::Testnet => "testnet",
            nakamoto_electrs::Network::Signet => "signet",
            nakamoto_electrs::Network::Regtest => "regtest",
        }
    );

    // ---- 3. Build nakamoto client config --------------------------------
    let nk_network = match cfg.network {
        nakamoto_electrs::Network::Mainnet => nakamoto_client::Network::Mainnet,
        nakamoto_electrs::Network::Testnet => nakamoto_client::Network::Testnet,
        nakamoto_electrs::Network::Signet => nakamoto_client::Network::Signet,
        nakamoto_electrs::Network::Regtest => nakamoto_client::Network::Regtest,
    };
    let mut nk_cfg = nakamoto_client::Config::new(nk_network);
    nk_cfg.root = cfg.index_dir.clone();
    nk_cfg.connect = cfg.nakamoto_peers.clone();

    // ---- 4. Start nakamoto client in a background thread ----------------
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

    // ---- 5. Build shared components ------------------------------------
    let metrics = Metrics::new();
    let source = Arc::new(NakamotoBlockSource::new(handle.clone()));
    let indexer = Indexer::new(metrics.clone());

    // ---- 6. Start the indexer loop in its own thread --------------------
    let _indexer_thread = indexer.clone().start(source.as_ref());

    // ---- 7. Wait for at least one peer before serving -------------------
    info!("waiting for nakamoto to connect to peers...");
    match handle.wait_for_peers(1, ServiceFlags::NONE) {
        Ok(_) => info!("nakamoto connected to peers"),
        Err(e) => {
            error!("wait_for_peers failed: {e}; continuing anyway");
        }
    }

    // ---- 8. Start the Electrum TCP server -------------------------------
    let server = ElectrumServer::bind(cfg.electrum_listen_addr, indexer, metrics)?;

    info!("Electrum server ready on {}", cfg.electrum_listen_addr);

    // Run the accept loop (blocks until listener is closed).
    server.run(source)?;

    let _ = client_thread.join();
    Ok(())
}
