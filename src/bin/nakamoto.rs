use anyhow::Result;
use nakamoto_electrs::{
    app::run_nakamoto,
    config::NakamotoConfig,
    Network,
};

fn main() -> Result<()> {
    run_nakamoto(NakamotoConfig::new(Network::Testnet))
}
