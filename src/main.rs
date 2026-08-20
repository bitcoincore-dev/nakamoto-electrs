use anyhow::Result;
use clap::Parser;

use nakamoto_electrs::{
    app::{run_bridge, run_nakamoto},
    config::{Cli, Mode},
};

#[cfg(feature = "electrs-bin")]
use nakamoto_electrs::app::run_electrs;

fn main() -> Result<()> {
    match Cli::parse().into_mode() {
        Mode::Bridge(cfg) => run_bridge(cfg),
        Mode::Nakamoto(cfg) => run_nakamoto(cfg),
        #[cfg(feature = "electrs-bin")]
        Mode::Electrs => run_electrs(),
    }
}
