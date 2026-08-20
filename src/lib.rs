// ---------------------------------------------------------------------------
// Modules
// ---------------------------------------------------------------------------

pub mod app;
pub mod block_source;
pub mod config;
pub mod electrum_server;
pub mod indexer;
pub mod metrics;
pub mod nakamoto_source;

// ---------------------------------------------------------------------------
// Utility functions (used by tests and downstream)
// ---------------------------------------------------------------------------

/// Adds two `u64` values, panicking on overflow in debug builds.
/// For wrapping or saturating behaviour use the standard library primitives
/// (`u64::wrapping_add`, `u64::saturating_add`) directly.
pub fn add(left: u64, right: u64) -> u64 {
    left + right
}

/// Saturating subtraction so callers don't have to handle underflow.
pub fn saturating_sub(left: u64, right: u64) -> u64 {
    left.saturating_sub(right)
}

// ---------------------------------------------------------------------------
// config
// ---------------------------------------------------------------------------

/// Supported Bitcoin networks that nakamoto-electrs can connect to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Network {
    Mainnet,
    Testnet,
    Signet,
    Regtest,
}

impl Network {
    /// Parse a network name from a string slice (case-insensitive).
    #[allow(clippy::should_implement_trait)]
    pub fn from_str(s: &str) -> Option<Self> {
        if s.eq_ignore_ascii_case("mainnet")
            || s.eq_ignore_ascii_case("bitcoin")
            || s.eq_ignore_ascii_case("main")
        {
            Some(Self::Mainnet)
        } else if s.eq_ignore_ascii_case("testnet") || s.eq_ignore_ascii_case("test") {
            Some(Self::Testnet)
        } else if s.eq_ignore_ascii_case("signet") {
            Some(Self::Signet)
        } else if s.eq_ignore_ascii_case("regtest") {
            Some(Self::Regtest)
        } else {
            None
        }
    }

    /// Returns `true` for networks where real value is at stake.
    pub fn is_production(&self) -> bool {
        matches!(self, Self::Mainnet)
    }

    /// Default electrum RPC port for the network.
    pub fn default_electrum_port(&self) -> u16 {
        match self {
            Self::Mainnet => 50001,
            Self::Testnet => 60001,
            Self::Signet => 60601,
            Self::Regtest => 60401,
        }
    }

    /// Default nakamoto P2P port for the network.
    pub fn default_p2p_port(&self) -> u16 {
        match self {
            Self::Mainnet => 8333,
            Self::Testnet => 18333,
            Self::Signet => 38333,
            Self::Regtest => 18444,
        }
    }
}

// ---------------------------------------------------------------------------
// electrum protocol helpers
// ---------------------------------------------------------------------------

/// Validate that a raw script hex string contains only hex characters and has
/// even length (i.e. it can be decoded into bytes).
pub fn is_valid_script_hex(hex: &str) -> bool {
    !hex.is_empty() && hex.len().is_multiple_of(2) && hex.chars().all(|c| c.is_ascii_hexdigit())
}

/// Convert a big-endian txid hex string (as returned by block explorers) into
/// the little-endian byte order required by the Electrum protocol.
///
/// Returns `None` when `hex` is not a valid 32-byte (64-char) hex string.
pub fn txid_to_electrum_bytes(hex: &str) -> Option<[u8; 32]> {
    if hex.len() != 64 {
        return None;
    }
    let mut bytes = [0u8; 32];
    for i in 0..32 {
        let byte_str = hex.get(i * 2..i * 2 + 2)?;
        bytes[31 - i] = u8::from_str_radix(byte_str, 16).ok()?;
    }
    Some(bytes)
}

/// Format a fee rate (satoshis per virtual byte) as a human-readable string.
pub fn format_fee_rate(sats_per_vbyte: f64) -> String {
    format!("{:.2} sat/vB", sats_per_vbyte)
}

// ---------------------------------------------------------------------------
// height helpers
// ---------------------------------------------------------------------------

/// Returns `true` if the given block height is a Bitcoin halving height
/// (mainnet schedule: every 210,000 blocks starting at 210,000).
pub fn is_halving_height(height: u32) -> bool {
    height > 0 && height.is_multiple_of(210_000)
}

/// Estimate the approximate Bitcoin block reward in satoshis for a given
/// block height (mainnet schedule).
pub fn block_reward_sats(height: u32) -> u64 {
    let halvings = height / 210_000;
    if halvings >= 64 {
        return 0;
    }
    5_000_000_000u64 >> halvings
}

// ---------------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // --- arithmetic ---

    #[test]
    fn add_zero() {
        assert_eq!(add(0, 0), 0);
    }

    #[test]
    fn add_basic() {
        assert_eq!(add(2, 2), 4);
    }

    #[test]
    fn add_large() {
        assert_eq!(add(u64::MAX - 1, 1), u64::MAX);
    }

    /// Documents the overflow behaviour: panics in debug, wraps in release.
    #[test]
    #[cfg(not(debug_assertions))]
    fn add_overflow_wraps_in_release() {
        assert_eq!(add(u64::MAX, 1), 0);
    }

    #[test]
    fn saturating_sub_normal() {
        assert_eq!(saturating_sub(10, 3), 7);
    }

    #[test]
    fn saturating_sub_underflow() {
        assert_eq!(saturating_sub(3, 10), 0);
    }

    #[test]
    fn saturating_sub_equal() {
        assert_eq!(saturating_sub(5, 5), 0);
    }

    // --- Network::from_str ---

    #[test]
    fn network_parse_mainnet_variants() {
        for s in &["mainnet", "bitcoin", "main", "MAINNET", "Bitcoin"] {
            assert_eq!(
                Network::from_str(s),
                Some(Network::Mainnet),
                "failed for {s}"
            );
        }
    }

    #[test]
    fn network_parse_testnet() {
        assert_eq!(Network::from_str("testnet"), Some(Network::Testnet));
        assert_eq!(Network::from_str("test"), Some(Network::Testnet));
    }

    #[test]
    fn network_parse_signet() {
        assert_eq!(Network::from_str("signet"), Some(Network::Signet));
    }

    #[test]
    fn network_parse_regtest() {
        assert_eq!(Network::from_str("regtest"), Some(Network::Regtest));
    }

    #[test]
    fn network_parse_unknown() {
        assert_eq!(Network::from_str(""), None);
        assert_eq!(Network::from_str("foonet"), None);
    }

    // --- Network::is_production ---

    #[test]
    fn mainnet_is_production() {
        assert!(Network::Mainnet.is_production());
    }

    #[test]
    fn non_mainnet_not_production() {
        assert!(!Network::Testnet.is_production());
        assert!(!Network::Signet.is_production());
        assert!(!Network::Regtest.is_production());
    }

    // --- Network port helpers ---

    #[test]
    fn electrum_ports_are_distinct() {
        let ports: Vec<u16> = [
            Network::Mainnet,
            Network::Testnet,
            Network::Signet,
            Network::Regtest,
        ]
        .iter()
        .map(|n| n.default_electrum_port())
        .collect();
        let unique: std::collections::HashSet<_> = ports.iter().collect();
        assert_eq!(unique.len(), ports.len());
    }

    #[test]
    fn mainnet_electrum_port() {
        assert_eq!(Network::Mainnet.default_electrum_port(), 50001);
    }

    #[test]
    fn mainnet_p2p_port() {
        assert_eq!(Network::Mainnet.default_p2p_port(), 8333);
    }

    #[test]
    fn p2p_ports_are_distinct() {
        let ports: Vec<u16> = [
            Network::Mainnet,
            Network::Testnet,
            Network::Signet,
            Network::Regtest,
        ]
        .iter()
        .map(|n| n.default_p2p_port())
        .collect();
        let unique: std::collections::HashSet<_> = ports.iter().collect();
        assert_eq!(unique.len(), ports.len());
    }

    // --- script hex validation ---

    #[test]
    fn valid_script_hex_p2pkh() {
        assert!(is_valid_script_hex(
            "76a914000000000000000000000000000000000000000088ac"
        ));
    }

    #[test]
    fn valid_script_hex_op_return() {
        assert!(is_valid_script_hex("6a"));
    }

    #[test]
    fn invalid_script_hex_odd_length() {
        assert!(!is_valid_script_hex("76a"));
    }

    #[test]
    fn invalid_script_hex_non_hex_chars() {
        assert!(!is_valid_script_hex("76zz"));
    }

    #[test]
    fn invalid_script_hex_empty() {
        assert!(!is_valid_script_hex(""));
    }

    // --- txid_to_electrum_bytes ---

    #[test]
    fn txid_reversal_all_zeros() {
        let hex = "0".repeat(64);
        let bytes = txid_to_electrum_bytes(&hex).unwrap();
        assert_eq!(bytes, [0u8; 32]);
    }

    #[test]
    fn txid_reversal_known_value() {
        // Build a hex where byte index 0 (MSB, leftmost pair) = 0xAB.
        // After reversal, it should land at position 31.
        let hex = format!("ab{}", "00".repeat(31));
        let bytes = txid_to_electrum_bytes(&hex).unwrap();
        assert_eq!(bytes[31], 0xab);
        assert!(bytes[..31].iter().all(|&b| b == 0));
    }

    #[test]
    fn txid_reversal_wrong_length() {
        assert!(txid_to_electrum_bytes("deadbeef").is_none());
        assert!(txid_to_electrum_bytes("").is_none());
    }

    #[test]
    fn txid_reversal_invalid_hex() {
        let bad = format!("zz{}", "00".repeat(31));
        assert!(txid_to_electrum_bytes(&bad).is_none());
    }

    // --- format_fee_rate ---

    #[test]
    fn fee_rate_formatting() {
        assert_eq!(format_fee_rate(1.0), "1.00 sat/vB");
        assert_eq!(format_fee_rate(10.5), "10.50 sat/vB");
        assert_eq!(format_fee_rate(0.0), "0.00 sat/vB");
    }

    // --- halving helpers ---

    #[test]
    fn halving_heights_are_multiples_of_210000() {
        assert!(is_halving_height(210_000));
        assert!(is_halving_height(420_000));
        assert!(is_halving_height(840_000));
    }

    #[test]
    fn non_halving_heights() {
        assert!(!is_halving_height(0));
        assert!(!is_halving_height(1));
        assert!(!is_halving_height(209_999));
        assert!(!is_halving_height(210_001));
    }

    #[test]
    fn genesis_block_reward() {
        assert_eq!(block_reward_sats(0), 5_000_000_000);
    }

    #[test]
    fn first_halving_block_reward() {
        assert_eq!(block_reward_sats(210_000), 2_500_000_000);
    }

    #[test]
    fn second_halving_block_reward() {
        assert_eq!(block_reward_sats(420_000), 1_250_000_000);
    }

    #[test]
    fn reward_after_64_halvings_is_zero() {
        assert_eq!(block_reward_sats(64 * 210_000), 0);
    }

    #[test]
    fn reward_decreases_monotonically() {
        let heights = [0u32, 210_000, 420_000, 630_000, 840_000];
        let rewards: Vec<u64> = heights.iter().map(|&h| block_reward_sats(h)).collect();
        for window in rewards.windows(2) {
            assert!(window[0] > window[1]);
        }
    }
}
