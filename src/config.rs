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
    fn signet_and_testnet_use_distinct_data_dirs() {
        let signet = Config::new(Network::Signet);
        let testnet = Config::new(Network::Testnet);

        assert!(signet.index_dir.to_string_lossy().contains("signet"));
        assert!(testnet.index_dir.to_string_lossy().contains("testnet"));
        assert_ne!(signet.index_dir, testnet.index_dir);
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

    // ---- NakamotoConfig::new ----------------------------------------------

    #[test]
    fn nakamoto_config_new_has_correct_network() {
        assert_eq!(NakamotoConfig::new(Network::Mainnet).network, Network::Mainnet);
        assert_eq!(NakamotoConfig::new(Network::Signet).network, Network::Signet);
    }

    #[test]
    fn nakamoto_config_new_has_empty_peers() {
        assert!(NakamotoConfig::new(Network::Testnet).nakamoto_peers.is_empty());
    }

    #[test]
    fn nakamoto_config_index_dir_contains_network_name() {
        let cfg = NakamotoConfig::new(Network::Signet);
        assert!(cfg.index_dir.to_string_lossy().contains("signet"));

        let cfg = NakamotoConfig::new(Network::Mainnet);
        assert!(cfg.index_dir.to_string_lossy().contains("mainnet"));
    }

    #[test]
    fn nakamoto_config_default_log_level_is_info() {
        assert_eq!(NakamotoConfig::new(Network::Testnet).log_level, Level::INFO);
    }

    // ---- Config::new for all networks ------------------------------------

    #[test]
    fn config_new_signet_port() {
        let cfg = Config::new(Network::Signet);
        assert_eq!(cfg.electrum_listen_addr.port(), 60601);
    }

    #[test]
    fn config_new_regtest_port() {
        let cfg = Config::new(Network::Regtest);
        assert_eq!(cfg.electrum_listen_addr.port(), 60401);
    }

    // ---- CLI: custom listen / peer args ----------------------------------

    #[test]
    fn cli_custom_listen_address_is_used() {
        let cli = Cli::parse_from([
            "nakamoto-electrs",
            "--network", "regtest",
            "--listen", "127.0.0.1:19999",
        ]);
        match cli.into_mode() {
            Mode::Bridge(cfg) => assert_eq!(cfg.electrum_listen_addr.port(), 19999),
            _ => panic!("expected bridge mode"),
        }
    }

    #[test]
    fn cli_custom_peers_are_forwarded() {
        let cli = Cli::parse_from([
            "nakamoto-electrs",
            "--network", "regtest",
            "--peer", "127.0.0.1:18444",
            "--peer", "127.0.0.1:18445",
        ]);
        match cli.into_mode() {
            Mode::Bridge(cfg) => assert_eq!(cfg.nakamoto_peers.len(), 2),
            _ => panic!("expected bridge mode"),
        }
    }

    #[test]
    fn cli_network_arg_all_variants_convert_to_network() {
        use clap::Parser;
        for (arg, expected) in [
            ("mainnet", Network::Mainnet),
            ("testnet", Network::Testnet),
            ("signet", Network::Signet),
            ("regtest", Network::Regtest),
        ] {
            let cli = Cli::parse_from(["nakamoto-electrs", "--network", arg]);
            let network: Network = cli.network.into();
            assert_eq!(network, expected, "failed for {arg}");
        }
    }
}
