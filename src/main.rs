use anyhow::Result;
use nakamoto_client::handle::Handle as _;
use nakamoto_client::{Client, Config, Network};
use nakamoto_net_poll::Reactor;
use std::net::TcpStream;
use std::thread;
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

    let event_logger = thread::spawn(move || {
        while let Ok(event) = events.recv() {
            info!(target: "nakamoto", ?event);
        }
    });

    let client_runner = thread::spawn(move || {
        if let Err(e) = client.run(config) {
            error!(target: "nakamoto", "client exited: {e:?}");
        }
    });

    let _ = client_runner.join();
    let _ = event_logger.join();

    Ok(())
}
