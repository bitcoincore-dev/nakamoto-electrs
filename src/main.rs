use anyhow::Result;
use clap::Parser;

use nakamoto_electrs::{
    app::{run_bridge, run_electrs, run_nakamoto},
    config::{Cli, Mode},
};

fn main() -> Result<()> {
    match Cli::parse().into_mode() {
        Mode::Bridge(cfg) => run_bridge(cfg),
        Mode::Nakamoto(cfg) => run_nakamoto(cfg),
        Mode::Electrs => run_electrs(),
    }
}
