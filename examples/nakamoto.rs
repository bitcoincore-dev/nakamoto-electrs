use anyhow::Result;
use nakamoto_electrs::{Network, app::run_nakamoto, config::NakamotoConfig};

fn main() -> Result<()> {
    run_nakamoto(NakamotoConfig::new(Network::Testnet))
}
