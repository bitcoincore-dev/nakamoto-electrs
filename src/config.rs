//! Unified runtime configuration for nakamoto-electrs.
//!
//! A [`Config`] can be built programmatically (useful in tests) or parsed from
//! command-line arguments via [`Config::from_args`].

use std::net::SocketAddr;
use std::path::PathBuf;

use tracing::Level;

use crate::Network;

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

/// Runtime configuration for the nakamoto-electrs bridge.
#[derive(Debug, Clone)]
pub struct Config {
    /// Bitcoin network to operate on.
    pub network: Network,

    /// Address on which the Electrum JSON-RPC server will listen.
    pub electrum_listen_addr: SocketAddr,

    /// Optional list of seed peers for nakamoto.  When empty nakamoto will use
    /// its built-in DNS seeds.
    pub nakamoto_peers: Vec<SocketAddr>,

    /// Directory where nakamoto stores its block-header and filter caches, and
    /// where the Electrum index is persisted.
    pub index_dir: PathBuf,

    /// Maximum log verbosity level.
    pub log_level: Level,

    /// Maximum number of chain reorganisation blocks to handle when rolling
    /// back the index.  Defaults to [`DEFAULT_REORG_DEPTH`].
    pub max_reorg_depth: u32,
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

    /// Parse configuration from `std::env::args`.
    ///
    /// Supports the following flags (all optional):
    ///
    /// ```text
    /// --network  <mainnet|testnet|signet|regtest>  (default: testnet)
    /// --listen   <ip:port>                          (default: 127.0.0.1:<network-port>)
    /// --data-dir <path>                             (default: ~/.nakamoto-electrs/<network>)
    /// --peer     <ip:port>                          (repeatable)
    /// --log      <error|warn|info|debug|trace>      (default: info)
    /// ```
    pub fn from_args() -> Self {
        let args: Vec<String> = std::env::args().skip(1).collect();
        let mut cfg = Config::new(Network::Testnet);

        let mut i = 0usize;
        while i < args.len() {
            match args[i].as_str() {
                "--network" | "-n" => {
                    if let Some(val) = args.get(i + 1) {
                        if let Some(net) = Network::from_str(val) {
                            cfg = Config::new(net);
                        } else {
                            eprintln!("warn: unknown network '{val}', using testnet");
                        }
                        i += 2;
                    } else {
                        eprintln!("warn: --network requires a value");
                        i += 1;
                    }
                }
                "--listen" | "-l" => {
                    if let Some(val) = args.get(i + 1) {
                        match val.parse::<SocketAddr>() {
                            Ok(addr) => cfg.electrum_listen_addr = addr,
                            Err(e) => eprintln!("warn: invalid --listen '{val}': {e}"),
                        }
                        i += 2;
                    } else {
                        eprintln!("warn: --listen requires a value");
                        i += 1;
                    }
                }
                "--data-dir" | "-d" => {
                    if let Some(val) = args.get(i + 1) {
                        cfg.index_dir = PathBuf::from(val);
                        i += 2;
                    } else {
                        eprintln!("warn: --data-dir requires a value");
                        i += 1;
                    }
                }
                "--peer" | "-p" => {
                    if let Some(val) = args.get(i + 1) {
                        match val.parse::<SocketAddr>() {
                            Ok(addr) => cfg.nakamoto_peers.push(addr),
                            Err(e) => eprintln!("warn: invalid --peer '{val}': {e}"),
                        }
                        i += 2;
                    } else {
                        eprintln!("warn: --peer requires a value");
                        i += 1;
                    }
                }
                "--log" => {
                    if let Some(val) = args.get(i + 1) {
                        cfg.log_level = parse_level(val);
                        i += 2;
                    } else {
                        eprintln!("warn: --log requires a value");
                        i += 1;
                    }
                }
                unknown => {
                    eprintln!("warn: unknown flag '{unknown}'");
                    i += 1;
                }
            }
        }

        cfg
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn parse_level(s: &str) -> Level {
    match s.to_ascii_lowercase().as_str() {
        "error" => Level::ERROR,
        "warn" => Level::WARN,
        "info" => Level::INFO,
        "debug" => Level::DEBUG,
        "trace" => Level::TRACE,
        other => {
            eprintln!("warn: unknown log level '{other}', using info");
            Level::INFO
        }
    }
}

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
    fn parse_level_all_variants() {
        assert_eq!(parse_level("error"), Level::ERROR);
        assert_eq!(parse_level("warn"), Level::WARN);
        assert_eq!(parse_level("info"), Level::INFO);
        assert_eq!(parse_level("debug"), Level::DEBUG);
        assert_eq!(parse_level("trace"), Level::TRACE);
        // unknown falls back to INFO
        assert_eq!(parse_level("bogus"), Level::INFO);
    }
}
