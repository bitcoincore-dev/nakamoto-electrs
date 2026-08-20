//! Unified runtime configuration and CLI parsing for nakamoto-electrs.

use std::net::SocketAddr;
use std::path::PathBuf;

use clap::{Parser, Subcommand, ValueEnum};
use tracing::Level;

use crate::Network;

// ---------------------------------------------------------------------------
// Runtime configs
// ---------------------------------------------------------------------------

/// Runtime configuration for the nakamoto-electrs bridge.
#[derive(Debug, Clone)]
pub struct Config {
    /// Bitcoin network to operate on.
    pub network: Network,

    /// Address on which the Electrum JSON-RPC server will listen.
    pub electrum_listen_addr: SocketAddr,

    /// Optional list of seed peers for nakamoto. When empty nakamoto will use
    /// its built-in DNS seeds.
    pub nakamoto_peers: Vec<SocketAddr>,

    /// Base directory for runtime data. nakamoto caches live in `nakamoto/`
    /// and the Electrum index persists in `index/`.
    pub index_dir: PathBuf,

    /// Maximum log verbosity level.
    pub log_level: Level,

    /// Maximum number of chain reorganisation blocks to handle when rolling
    /// back the index. Defaults to [`DEFAULT_REORG_DEPTH`].
    pub max_reorg_depth: u32,
}

/// Runtime configuration for the standalone nakamoto node.
#[derive(Debug, Clone)]
pub struct NakamotoConfig {
    /// Bitcoin network to operate on.
    pub network: Network,

    /// Optional list of seed peers for nakamoto. When empty nakamoto will use
    /// its built-in DNS seeds.
    pub nakamoto_peers: Vec<SocketAddr>,

    /// Directory where nakamoto stores its block-header and filter caches.
    pub index_dir: PathBuf,

    /// Maximum log verbosity level.
    pub log_level: Level,
}

/// Sensible default maximum reorg depth for safety.
pub const DEFAULT_REORG_DEPTH: u32 = 100;

impl Config {
    /// Create a configuration with defaults for the given network.
    pub fn new(network: Network) -> Self {
        let port = network.default_electrum_port();
        Self {
            network,
            electrum_listen_addr: format!("127.0.0.1:{port}")
                .parse()
                .expect("default electrum addr is valid"),
            nakamoto_peers: Vec::new(),
            index_dir: default_index_dir(&network),
            log_level: Level::INFO,
            max_reorg_depth: DEFAULT_REORG_DEPTH,
        }
    }
}

impl NakamotoConfig {
    /// Create a standalone nakamoto config with defaults for the given network.
    pub fn new(network: Network) -> Self {
        Self {
            network,
            nakamoto_peers: Vec::new(),
            index_dir: default_index_dir(&network),
            log_level: Level::INFO,
        }
    }
}

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Parser)]
#[command(name = "nakamoto-electrs", version, about)]
pub struct Cli {
    /// Bitcoin network to operate on.
    #[arg(long, short = 'n', global = true, value_enum, default_value_t = NetworkArg::Testnet)]
    pub network: NetworkArg,

    /// Electrum listener address for the bridge mode.
    #[arg(long, short = 'l', global = true)]
    pub listen: Option<SocketAddr>,

    /// Directory where nakamoto stores its block-header and filter caches.
    #[arg(long = "data-dir", short = 'd', global = true)]
    pub data_dir: Option<PathBuf>,

    /// Explicit nakamoto peer (repeatable).
    #[arg(long = "peer", short = 'p', global = true)]
    pub peer: Vec<SocketAddr>,

    /// Log level.
    #[arg(long, global = true, value_enum, default_value_t = LevelArg::Info)]
    pub log: LevelArg,

    /// Maximum number of chain reorganisation blocks to handle when rolling
    /// back the index.
    #[arg(long, global = true, default_value_t = DEFAULT_REORG_DEPTH)]
    pub max_reorg_depth: u32,

    #[command(subcommand)]
    pub command: Option<Command>,
}

#[derive(Debug, Clone, Subcommand)]
pub enum Command {
    /// Run the standalone nakamoto SPV node.
    Nakamoto,
    #[cfg(feature = "electrs-bin")]
    /// Run the standalone electrs binary backed by Bitcoin Core.
    Electrs,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, ValueEnum)]
pub enum NetworkArg {
    Mainnet,
    Testnet,
    Signet,
    Regtest,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, ValueEnum)]
pub enum LevelArg {
    Error,
    Warn,
    Info,
    Debug,
    Trace,
}

impl From<NetworkArg> for Network {
    fn from(value: NetworkArg) -> Self {
        match value {
            NetworkArg::Mainnet => Self::Mainnet,
            NetworkArg::Testnet => Self::Testnet,
            NetworkArg::Signet => Self::Signet,
            NetworkArg::Regtest => Self::Regtest,
        }
    }
}

impl From<LevelArg> for Level {
    fn from(value: LevelArg) -> Self {
        match value {
            LevelArg::Error => Level::ERROR,
            LevelArg::Warn => Level::WARN,
            LevelArg::Info => Level::INFO,
            LevelArg::Debug => Level::DEBUG,
            LevelArg::Trace => Level::TRACE,
        }
    }
}

pub enum Mode {
    Bridge(Config),
    Nakamoto(NakamotoConfig),
    #[cfg(feature = "electrs-bin")]
    Electrs,
}

impl Cli {
    pub fn into_mode(self) -> Mode {
        let network: Network = self.network.into();
        let log_level: Level = self.log.into();
        let listen = self
            .listen
            .unwrap_or_else(|| default_electrum_addr(network));
        let index_dir = self.data_dir.unwrap_or_else(|| default_index_dir(&network));

        let bridge = Config {
            network,
            electrum_listen_addr: listen,
            nakamoto_peers: self.peer.clone(),
            index_dir: index_dir.clone(),
            log_level,
            max_reorg_depth: self.max_reorg_depth,
        };

        let nakamoto = NakamotoConfig {
            network,
            nakamoto_peers: self.peer,
            index_dir,
            log_level,
        };

        match self.command {
            Some(Command::Nakamoto) => Mode::Nakamoto(nakamoto),
            #[cfg(feature = "electrs-bin")]
            Some(Command::Electrs) => Mode::Electrs,
            None => Mode::Bridge(bridge),
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn default_index_dir(network: &Network) -> PathBuf {
    let net_str = match network {
        Network::Mainnet => "mainnet",
        Network::Testnet => "testnet",
        Network::Signet => "signet",
        Network::Regtest => "regtest",
    };
    let mut dir = dirs_or_home();
    dir.push(".nakamoto-electrs");
    dir.push(net_str);
    dir
}

fn default_electrum_addr(network: Network) -> SocketAddr {
    format!("127.0.0.1:{}", network.default_electrum_port())
        .parse()
        .expect("default electrum addr is valid")
}

fn dirs_or_home() -> PathBuf {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."))
}

// ---------------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_has_correct_port() {
        let cfg = Config::new(Network::Mainnet);
        assert_eq!(cfg.electrum_listen_addr.port(), 50001);

        let cfg = Config::new(Network::Testnet);
        assert_eq!(cfg.electrum_listen_addr.port(), 60001);
    }

    #[test]
    fn default_reorg_depth() {
        let cfg = Config::new(Network::Testnet);
        assert_eq!(cfg.max_reorg_depth, DEFAULT_REORG_DEPTH);
    }

    #[test]
    fn default_no_peers() {
        let cfg = Config::new(Network::Mainnet);
        assert!(cfg.nakamoto_peers.is_empty());
    }

    #[test]
    fn index_dir_contains_network_name() {
        let cfg = Config::new(Network::Regtest);
        assert!(cfg.index_dir.to_string_lossy().contains("regtest"));

        let cfg = Config::new(Network::Mainnet);
        assert!(cfg.index_dir.to_string_lossy().contains("mainnet"));
    }

    #[test]
    fn cli_defaults_bridge_mode() {
        let cli = Cli::parse_from(["nakamoto-electrs"]);
        match cli.into_mode() {
            Mode::Bridge(cfg) => {
                assert_eq!(cfg.network, Network::Testnet);
                assert_eq!(cfg.electrum_listen_addr.port(), 60001);
            }
            _ => panic!("expected bridge mode"),
        }
    }

    #[test]
    fn cli_nakamoto_subcommand_parses() {
        let cli = Cli::parse_from(["nakamoto-electrs", "nakamoto", "--network", "signet"]);
        match cli.into_mode() {
            Mode::Nakamoto(cfg) => assert_eq!(cfg.network, Network::Signet),
            _ => panic!("expected nakamoto mode"),
        }
    }

    #[test]
    fn parse_level_all_variants() {
        assert_eq!(Level::from(LevelArg::Error), Level::ERROR);
        assert_eq!(Level::from(LevelArg::Warn), Level::WARN);
        assert_eq!(Level::from(LevelArg::Info), Level::INFO);
        assert_eq!(Level::from(LevelArg::Debug), Level::DEBUG);
        assert_eq!(Level::from(LevelArg::Trace), Level::TRACE);
    }
}
