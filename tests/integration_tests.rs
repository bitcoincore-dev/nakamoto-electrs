//! Integration tests for nakamoto-electrs.
//!
//! These tests exercise the public API of the library crate end-to-end,
//! covering interactions between multiple modules rather than individual
//! functions in isolation.

use nakamoto_electrs::{
    Network, block_reward_sats, format_fee_rate, is_halving_height, is_valid_script_hex,
    saturating_sub, txid_to_electrum_bytes,
};
use nakamoto_electrs::indexer::Indexer;
use nakamoto_electrs::metrics::Metrics;
use tempfile::tempdir;

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

// ---------------------------------------------------------------------------
// MockBlockSource + Indexer integration
// ---------------------------------------------------------------------------

mod mock {
    //! In-process block source for testing — drives the indexer with
    //! deterministic, hand-crafted blocks.

    use anyhow::Result;
    use bitcoin::{
        Block, BlockHash, CompactTarget, OutPoint, ScriptBuf, Sequence, Witness,
        absolute::LockTime,
        blockdata::block::Header as BlockHeader,
        blockdata::{
            block::Version,
            script::Builder,
            transaction::{Transaction, TxIn, TxOut},
        },
        hash_types::TxMerkleNode,
        hashes::Hash,
    };
    use crossbeam_channel::{Receiver, Sender, unbounded};
    use std::sync::{Arc, Mutex};

    use nakamoto_electrs::block_source::{BlockEvent, BlockSource};

    // Simple OP_RETURN script so every block/test can have a unique scriptPubKey.
    pub fn op_return_script(tag: u8) -> ScriptBuf {
        Builder::new()
            .push_opcode(bitcoin::blockdata::opcodes::all::OP_RETURN)
            .push_slice([tag])
            .into_script()
    }

    pub fn make_tx(scripts: Vec<ScriptBuf>) -> Transaction {
        let outputs = scripts
            .into_iter()
            .map(|s| TxOut {
                value: bitcoin::Amount::from_sat(1000),
                script_pubkey: s,
            })
            .collect();
        Transaction {
            version: bitcoin::transaction::Version::non_standard(1),
            lock_time: LockTime::ZERO,
            input: vec![TxIn {
                previous_output: OutPoint::null(),
                script_sig: ScriptBuf::new(),
                sequence: Sequence::MAX,
                witness: Witness::new(),
            }],
            output: outputs,
        }
    }

    pub fn make_block(prev: BlockHash, height: u32, scripts: Vec<ScriptBuf>) -> Block {
        let tx = make_tx(scripts);
        let header = BlockHeader {
            version: Version::ONE,
            prev_blockhash: prev,
            merkle_root: TxMerkleNode::all_zeros(),
            time: height,
            bits: CompactTarget::from_consensus(0x1d00ffff),
            nonce: height, // differentiate blocks by nonce
        };
        Block {
            header,
            txdata: vec![tx],
        }
    }

    /// A [`BlockSource`] that replays pre-built events from a channel.
    pub struct MockBlockSource {
        senders: Arc<Mutex<Vec<Sender<BlockEvent>>>>,
    }

    impl MockBlockSource {
        pub fn new() -> Self {
            Self {
                senders: Arc::new(Mutex::new(Vec::new())),
            }
        }

        /// Push an event to all current subscribers.
        pub fn push(&self, event: BlockEvent) {
            let mut guard = self.senders.lock().unwrap();
            guard.retain(|tx| tx.send(event.clone()).is_ok());
        }
    }

    impl BlockSource for MockBlockSource {
        fn subscribe(&self) -> Receiver<BlockEvent> {
            let (tx, rx) = unbounded();
            self.senders.lock().unwrap().push(tx);
            rx
        }

        fn tip(&self) -> Result<(u32, BlockHash)> {
            Ok((0, BlockHash::all_zeros()))
        }

        fn block_header(&self, _h: u32) -> Result<Option<BlockHeader>> {
            Ok(None)
        }

        fn block_by_hash(&self, _hash: &BlockHash) -> Result<Option<Block>> {
            Ok(None)
        }
    }
}

use bitcoin::{blockdata::script::Builder, hashes::Hash};
use nakamoto_electrs::{
    block_source::BlockEvent,
    indexer::{Indexer, ScriptHash},
    metrics::Metrics,
};

fn p2pkh_script() -> bitcoin::ScriptBuf {
    let mut s = vec![0x76u8, 0xa9, 0x14];
    s.extend_from_slice(&[0u8; 20]);
    s.extend_from_slice(&[0x88, 0xac]);
    Builder::from(s).into_script()
}

fn sh_of(script: &bitcoin::ScriptBuf) -> ScriptHash {
    ScriptHash::from_script(script)
}

fn make_indexer(metrics: Metrics) -> Indexer {
    let dir = tempdir().expect("temp index dir").into_path();
    Indexer::new(dir, metrics).expect("indexer")
}

#[test]
fn indexer_processes_block_from_mock_source() {
    let source = mock::MockBlockSource::new();
    let indexer = make_indexer(Metrics::new());
    let _handle = indexer.clone().start(&source);

    let script = p2pkh_script();
    let block = mock::make_block(bitcoin::BlockHash::all_zeros(), 1, vec![script.clone()]);

    source.push(BlockEvent::Connected {
        block: block.clone(),
        height: 1,
    });

    // Give the indexer thread time to process.
    std::thread::sleep(std::time::Duration::from_millis(50));

    let sh = sh_of(&script);
    let history = indexer.get_history(&sh);
    assert_eq!(history.len(), 1, "expected one history entry");
    assert_eq!(history[0].height, 1);
}

#[test]
fn indexer_rollback_on_disconnected_event() {
    let source = mock::MockBlockSource::new();
    let indexer = make_indexer(Metrics::new());
    let _handle = indexer.clone().start(&source);

    let script = p2pkh_script();
    let block = mock::make_block(bitcoin::BlockHash::all_zeros(), 5, vec![script.clone()]);
    let block_hash = block.block_hash();

    // Connect the block.
    source.push(BlockEvent::Connected { block, height: 5 });
    std::thread::sleep(std::time::Duration::from_millis(50));

    let sh = sh_of(&script);
    assert!(
        !indexer.get_history(&sh).is_empty(),
        "should have history after connect"
    );

    // Disconnect it.
    source.push(BlockEvent::Disconnected {
        hash: block_hash,
        height: 5,
    });
    std::thread::sleep(std::time::Duration::from_millis(50));

    assert!(
        indexer.get_history(&sh).is_empty(),
        "history should be empty after rollback"
    );
}

#[test]
fn indexer_reorg_replaces_history() {
    let source = mock::MockBlockSource::new();
    let indexer = make_indexer(Metrics::new());
    let _handle = indexer.clone().start(&source);

    let script_a = mock::op_return_script(0xAA);
    let script_b = mock::op_return_script(0xBB);

    let block_a = mock::make_block(bitcoin::BlockHash::all_zeros(), 10, vec![script_a.clone()]);
    let hash_a = block_a.block_hash();

    // Connect block A.
    source.push(BlockEvent::Connected {
        block: block_a,
        height: 10,
    });
    std::thread::sleep(std::time::Duration::from_millis(50));

    assert!(indexer.has_history(&sh_of(&script_a)));
    assert!(!indexer.has_history(&sh_of(&script_b)));

    // Reorg: disconnect A, connect B at the same height.
    source.push(BlockEvent::Disconnected {
        hash: hash_a,
        height: 10,
    });
    let block_b = mock::make_block(bitcoin::BlockHash::all_zeros(), 10, vec![script_b.clone()]);
    source.push(BlockEvent::Connected {
        block: block_b,
        height: 10,
    });
    std::thread::sleep(std::time::Duration::from_millis(50));

    assert!(
        !indexer.has_history(&sh_of(&script_a)),
        "old history should be gone after reorg"
    );
    assert!(
        indexer.has_history(&sh_of(&script_b)),
        "new history should exist after reorg"
    );
}

#[test]
fn indexer_tip_height_advances() {
    let source = mock::MockBlockSource::new();
    let indexer = make_indexer(Metrics::new());
    let _handle = indexer.clone().start(&source);

    for h in 1u32..=5 {
        let block = mock::make_block(
            bitcoin::BlockHash::all_zeros(),
            h,
            vec![mock::op_return_script(h as u8)],
        );
        source.push(BlockEvent::Connected { block, height: h });
    }
    std::thread::sleep(std::time::Duration::from_millis(100));

    assert_eq!(indexer.tip_height(), 5);
}

#[test]
fn metrics_track_indexed_blocks() {
    let metrics = Metrics::new();
    let source = mock::MockBlockSource::new();
    let indexer = make_indexer(metrics.clone());
    let _handle = indexer.clone().start(&source);

    for i in 0u8..3 {
        let block = mock::make_block(
            bitcoin::BlockHash::all_zeros(),
            i as u32 + 1,
            vec![mock::op_return_script(i)],
        );
        source.push(BlockEvent::Connected {
            block,
            height: i as u32 + 1,
        });
    }
    std::thread::sleep(std::time::Duration::from_millis(100));

    assert_eq!(metrics.blocks_indexed(), 3);
    assert_eq!(metrics.blocks_rolled_back(), 0);
}

#[test]
fn metrics_track_rolled_back_blocks() {
    let metrics = Metrics::new();
    let source = mock::MockBlockSource::new();
    let indexer = make_indexer(metrics.clone());
    let _handle = indexer.clone().start(&source);

    let block = mock::make_block(
        bitcoin::BlockHash::all_zeros(),
        1,
        vec![mock::op_return_script(0x01)],
    );
    let hash = block.block_hash();
    source.push(BlockEvent::Connected { block, height: 1 });
    source.push(BlockEvent::Disconnected { hash, height: 1 });
    std::thread::sleep(std::time::Duration::from_millis(100));

    assert_eq!(metrics.blocks_rolled_back(), 1);
}
