//! Integration tests for nakamoto-electrs.
//!
//! These tests exercise the public API of the library crate end-to-end,
//! covering interactions between multiple modules rather than individual
//! functions in isolation.

use nakamoto_electrs::{
    Network, block_reward_sats, format_fee_rate, is_halving_height, is_valid_script_hex,
    saturating_sub, txid_to_electrum_bytes,
};

// ---------------------------------------------------------------------------
// Network round-trip
// ---------------------------------------------------------------------------

#[test]
fn all_networks_parse_from_canonical_names() {
    let cases = [
        ("mainnet", Network::Mainnet),
        ("testnet", Network::Testnet),
        ("signet", Network::Signet),
        ("regtest", Network::Regtest),
    ];
    for (s, expected) in cases {
        assert_eq!(
            Network::from_str(s),
            Some(expected),
            "failed to parse '{s}'"
        );
    }
}

#[test]
fn network_ports_are_nonzero() {
    for net in [
        Network::Mainnet,
        Network::Testnet,
        Network::Signet,
        Network::Regtest,
    ] {
        assert!(net.default_electrum_port() > 0);
        assert!(net.default_p2p_port() > 0);
    }
}

#[test]
fn only_mainnet_is_production() {
    assert!(Network::Mainnet.is_production());
    for net in [Network::Testnet, Network::Signet, Network::Regtest] {
        assert!(!net.is_production());
    }
}

// ---------------------------------------------------------------------------
// Script-hex validation
// ---------------------------------------------------------------------------

#[test]
fn p2wpkh_script_is_valid_hex() {
    // OP_0 <20-byte-hash>  (22 bytes = 44 hex chars)
    let p2wpkh = "0014751e76e8199196f454f032d4f736f6e4b5f7e8c1";
    assert!(is_valid_script_hex(p2wpkh));
}

#[test]
fn p2wsh_script_is_valid_hex() {
    // OP_0 <32-byte-hash>  (34 bytes = 68 hex chars)
    let p2wsh = "0020701a8d401c84fb13e6baf169d59684e17abd9fa216c8cc5b9fc63d622ff8c58d";
    assert!(is_valid_script_hex(p2wsh));
}

#[test]
fn empty_string_is_invalid_hex() {
    assert!(!is_valid_script_hex(""));
}

#[test]
fn uppercase_hex_is_valid() {
    assert!(is_valid_script_hex("DEADBEEF"));
}

// ---------------------------------------------------------------------------
// txid byte-order conversion
// ---------------------------------------------------------------------------

#[test]
fn txid_conversion_is_byte_reversal() {
    // Construct a txid whose first byte (leftmost pair) is 0x01.
    let hex = format!("01{}", "00".repeat(31));
    let result = txid_to_electrum_bytes(&hex).unwrap();
    // After reversal, 0x01 moves to position 31 (last byte).
    assert_eq!(result[31], 0x01);
    assert!(result[..31].iter().all(|&b| b == 0x00));
}

#[test]
fn txid_conversion_rejects_short_hex() {
    assert!(txid_to_electrum_bytes("abcd").is_none());
}

#[test]
fn txid_conversion_rejects_odd_length() {
    let odd = "a".repeat(63);
    assert!(txid_to_electrum_bytes(&odd).is_none());
}

// ---------------------------------------------------------------------------
// Fee-rate formatting
// ---------------------------------------------------------------------------

#[test]
fn fee_rate_format_contains_unit() {
    let s = format_fee_rate(5.0);
    assert!(s.contains("sat/vB"), "expected 'sat/vB' in '{s}'");
}

#[test]
fn fee_rate_format_two_decimal_places() {
    // "1.00 sat/vB" — exactly two digits after the decimal point.
    let s = format_fee_rate(1.0);
    let decimal_part = s.split('.').nth(1).unwrap_or("");
    let digits: String = decimal_part
        .chars()
        .take_while(|c| c.is_ascii_digit())
        .collect();
    assert_eq!(digits.len(), 2);
}

// ---------------------------------------------------------------------------
// Halving schedule
// ---------------------------------------------------------------------------

#[test]
fn halving_schedule_first_eight_events() {
    let expected_halving_heights: Vec<u32> = (1..=8).map(|n| n * 210_000).collect();
    for h in &expected_halving_heights {
        assert!(
            is_halving_height(*h),
            "height {h} should be a halving height"
        );
    }
}

#[test]
fn block_rewards_halve_correctly() {
    let initial = block_reward_sats(0);
    assert_eq!(block_reward_sats(210_000), initial / 2);
    assert_eq!(block_reward_sats(420_000), initial / 4);
}

#[test]
fn total_supply_upper_bound() {
    // Sum rewards for every epoch; must not exceed 21 million BTC = 2.1e15 sats.
    let total: u64 = (0u32..64)
        .map(|epoch| {
            let reward = block_reward_sats(epoch * 210_000);
            reward.saturating_mul(210_000)
        })
        .sum();
    assert!(
        total <= 2_100_000_000_000_000,
        "total {total} exceeds 21M BTC"
    );
}

// ---------------------------------------------------------------------------
// Saturating arithmetic edge cases
// ---------------------------------------------------------------------------

#[test]
fn saturating_sub_zero_minus_large() {
    assert_eq!(saturating_sub(0, u64::MAX), 0);
}

#[test]
fn saturating_sub_max_minus_zero() {
    assert_eq!(saturating_sub(u64::MAX, 0), u64::MAX);
}
