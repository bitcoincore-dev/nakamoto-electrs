/// Standalone nakamoto SPV node binary.
///
/// Connects to the Bitcoin P2P network and syncs block headers and compact
/// filters.  Useful for testing nakamoto independently of the Electrum bridge.
use std::net::TcpStream;
use std::thread;

use anyhow::Result;
use nakamoto_client::{Client, Config, Network, handle::Handle as _};
use nakamoto_net_poll::Reactor;
use tracing::{Level, error, info};
use tracing_subscriber::FmtSubscriber;

type NodeReactor = Reactor<TcpStream>;

fn main() -> Result<()> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    let client = Client::<NodeReactor>::new()?;
    let handle = client.handle();
    let events = handle.events();
    let config = Config::new(Network::Testnet);

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
