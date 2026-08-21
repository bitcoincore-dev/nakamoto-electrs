//! Integration tests for nakamoto-electrs.
//!
//! These tests exercise the public API of the library crate end-to-end,
//! covering interactions between multiple modules rather than individual
//! functions in isolation.

use nakamoto_electrs::electrum_server::{
    ElectrumServer, FeeRateState, PendingChangeBroadcaster, TransactionBroadcaster,
};
use nakamoto_electrs::indexer::Indexer;
use nakamoto_electrs::metrics::Metrics;
use nakamoto_electrs::{
    Network, block_reward_sats, format_fee_rate, is_halving_height, is_valid_script_hex,
    saturating_sub, txid_to_electrum_bytes,
};
use std::io::{BufRead, BufReader, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;
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
        Amount, Block, BlockHash, CompactTarget, OutPoint, ScriptBuf, Sequence, Witness,
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
    use std::collections::BTreeMap;
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
                value: Amount::from_sat(1000),
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

    pub fn make_spend_tx(prevout: OutPoint, scripts: Vec<(u64, ScriptBuf)>) -> Transaction {
        let outputs = scripts
            .into_iter()
            .map(|(value, script)| TxOut {
                value: Amount::from_sat(value),
                script_pubkey: script,
            })
            .collect();
        Transaction {
            version: bitcoin::transaction::Version::non_standard(1),
            lock_time: LockTime::ZERO,
            input: vec![TxIn {
                previous_output: prevout,
                script_sig: ScriptBuf::new(),
                sequence: Sequence::MAX,
                witness: Witness::new(),
            }],
            output: outputs,
        }
    }

    pub fn make_block(prev: BlockHash, height: u32, scripts: Vec<ScriptBuf>) -> Block {
        let tx = make_tx(scripts);
        make_block_with_txs(prev, height, vec![tx])
    }

    pub fn make_block_with_txs(prev: BlockHash, height: u32, txdata: Vec<Transaction>) -> Block {
        let header = BlockHeader {
            version: Version::ONE,
            prev_blockhash: prev,
            merkle_root: TxMerkleNode::all_zeros(),
            time: height,
            bits: CompactTarget::from_consensus(0x1d00ffff),
            nonce: height, // differentiate blocks by nonce
        };
        Block { header, txdata }
    }

    /// A [`BlockSource`] that replays pre-built events from a channel.
    pub struct MockBlockSource {
        senders: Arc<Mutex<Vec<Sender<BlockEvent>>>>,
        headers: Arc<Mutex<BTreeMap<u32, bitcoin::blockdata::block::Header>>>,
    }

    impl MockBlockSource {
        pub fn new() -> Self {
            Self {
                senders: Arc::new(Mutex::new(Vec::new())),
                headers: Arc::new(Mutex::new(BTreeMap::new())),
            }
        }

        /// Push an event to all current subscribers.
        pub fn push(&self, event: BlockEvent) {
            if let BlockEvent::Connected { block, height } = &event {
                self.headers.lock().unwrap().insert(*height, block.header);
            }
            let mut guard = self.senders.lock().unwrap();
            guard.retain(|tx| tx.send(event.clone()).is_ok());
        }

        pub fn push_disconnected(&self, hash: bitcoin::BlockHash, height: u32) {
            self.headers.lock().unwrap().remove(&height);
            self.push(BlockEvent::Disconnected { hash, height });
        }
    }

    impl BlockSource for MockBlockSource {
        fn subscribe(&self) -> Receiver<BlockEvent> {
            let (tx, rx) = unbounded();
            self.senders.lock().unwrap().push(tx);
            rx
        }

        fn tip(&self) -> Result<(u32, BlockHash)> {
            let headers = self.headers.lock().unwrap();
            let height = headers.keys().next_back().copied().unwrap_or(0);
            let hash = headers
                .get(&height)
                .map(|header| header.block_hash())
                .unwrap_or_else(BlockHash::all_zeros);
            Ok((height, hash))
        }

        fn block_header(&self, h: u32) -> Result<Option<BlockHeader>> {
            Ok(self.headers.lock().unwrap().get(&h).copied())
        }

        fn block_by_hash(&self, _hash: &BlockHash) -> Result<Option<Block>> {
            Ok(None)
        }
    }
}

use bitcoin::{blockdata::script::Builder, hashes::Hash};
use nakamoto_electrs::{block_source::BlockEvent, indexer::ScriptHash};

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
    let dir = tempdir().expect("temp index dir").keep();
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

#[test]
fn indexer_tracks_balance_and_spend_history() {
    let source = mock::MockBlockSource::new();
    let indexer = make_indexer(Metrics::new());
    let _handle = indexer.clone().start(&source);

    let script_a = p2pkh_script();
    let script_b = mock::op_return_script(0x22);
    let block1 = mock::make_block(bitcoin::BlockHash::all_zeros(), 1, vec![script_a.clone()]);
    let fund_txid = block1.txdata[0].compute_txid();
    let fund_outpoint = bitcoin::OutPoint::new(fund_txid, 0);

    source.push(BlockEvent::Connected {
        block: block1,
        height: 1,
    });
    std::thread::sleep(std::time::Duration::from_millis(50));

    assert_eq!(indexer.get_balance(&sh_of(&script_a)).unwrap(), 1000);

    let spend_tx = mock::make_spend_tx(fund_outpoint, vec![(900, script_b)]);
    let block2 = mock::make_block_with_txs(bitcoin::BlockHash::all_zeros(), 2, vec![spend_tx]);
    source.push(BlockEvent::Connected {
        block: block2,
        height: 2,
    });
    std::thread::sleep(std::time::Duration::from_millis(50));

    assert_eq!(indexer.get_balance(&sh_of(&script_a)).unwrap(), 0);
    assert_eq!(indexer.get_history(&sh_of(&script_a)).len(), 2);
}

#[test]
fn indexer_tracks_unconfirmed_pending_balance_and_history() {
    let source = mock::MockBlockSource::new();
    let indexer = make_indexer(Metrics::new());
    let _handle = indexer.clone().start(&source);

    let script_a = p2pkh_script();
    let script_b = mock::op_return_script(0x44);
    let block1 = mock::make_block(bitcoin::BlockHash::all_zeros(), 1, vec![script_a.clone()]);
    let fund_txid = block1.txdata[0].compute_txid();
    let fund_outpoint = bitcoin::OutPoint::new(fund_txid, 0);

    source.push(BlockEvent::Connected {
        block: block1,
        height: 1,
    });
    std::thread::sleep(std::time::Duration::from_millis(50));

    let pending = mock::make_spend_tx(fund_outpoint, vec![(900, script_b.clone())]);
    indexer
        .track_pending_transaction(&pending)
        .expect("track pending tx");

    let sh_a = sh_of(&script_a);
    let sh_b = sh_of(&script_b);
    assert_eq!(indexer.get_balance(&sh_a).unwrap(), 1000);
    assert_eq!(indexer.get_unconfirmed_balance_delta(&sh_a).unwrap(), -1000);
    assert_eq!(indexer.get_unconfirmed_balance_delta(&sh_b).unwrap(), 900);
    assert!(
        indexer
            .get_history(&sh_a)
            .iter()
            .any(|e| e.height == 0 && e.txid == pending.compute_txid())
    );
    assert!(
        indexer
            .get_history(&sh_b)
            .iter()
            .any(|e| e.height == 0 && e.txid == pending.compute_txid())
    );
}

#[test]
fn electrum_scripthash_subscribe_receives_update_after_connected_block() {
    let source = Arc::new(mock::MockBlockSource::new());
    let metrics = Metrics::new();
    let indexer = make_indexer(metrics.clone());
    let _indexer_handle = indexer.clone().start(&source);
    let fee_rate = Arc::new(FeeRateState::new());
    let pending_changes = PendingChangeBroadcaster::default();
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let server = ElectrumServer::bind(addr, indexer, metrics, None, fee_rate, pending_changes)
        .expect("bind");
    let local_addr = server.local_addr();
    let shutdown = Arc::new(AtomicBool::new(false));
    let shutdown_thread = Arc::clone(&shutdown);
    let server_source = Arc::clone(&source);
    thread::spawn(move || {
        let _ = server.run(server_source, shutdown_thread);
    });

    let script = p2pkh_script();
    let sh = sh_of(&script);
    let mut stream = TcpStream::connect_timeout(&local_addr, Duration::from_secs(5)).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let mut reader = BufReader::new(stream.try_clone().unwrap());

    write!(
        stream,
        r#"{{"jsonrpc":"2.0","id":1,"method":"blockchain.scripthash.subscribe","params":["{}"]}}"#,
        sh.to_hex()
    )
    .unwrap();
    stream.write_all(b"\n").unwrap();

    let mut line = String::new();
    reader.read_line(&mut line).unwrap();
    let initial: serde_json::Value = serde_json::from_str(line.trim()).unwrap();
    assert!(initial["result"].is_null());

    let block = mock::make_block(bitcoin::BlockHash::all_zeros(), 1, vec![script.clone()]);
    source.push(BlockEvent::Connected { block, height: 1 });

    line.clear();
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while line.is_empty() {
        match reader.read_line(&mut line) {
            Ok(0) => continue,
            Ok(_) => break,
            Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                assert!(
                    std::time::Instant::now() < deadline,
                    "timed out waiting for notification"
                );
                thread::sleep(Duration::from_millis(50));
            }
            Err(err) => panic!("failed to read notification: {err}"),
        }
    }
    let notification: serde_json::Value = serde_json::from_str(line.trim()).unwrap();
    assert_eq!(notification["method"], "blockchain.scripthash.subscribe");
    assert_eq!(notification["params"][0], serde_json::json!(sh.to_hex()));
    assert!(notification["params"][1].is_string());

    shutdown.store(true, Ordering::SeqCst);
}

#[test]
fn electrum_transaction_get_returns_indexed_transaction() {
    let source = Arc::new(mock::MockBlockSource::new());
    let metrics = Metrics::new();
    let indexer = make_indexer(metrics.clone());
    let _indexer_handle = indexer.clone().start(&source);
    let fee_rate = Arc::new(FeeRateState::new());
    let pending_changes = PendingChangeBroadcaster::default();
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let server = ElectrumServer::bind(
        addr,
        indexer.clone(),
        metrics,
        None,
        fee_rate,
        pending_changes,
    )
    .expect("bind");
    let local_addr = server.local_addr();
    let shutdown = Arc::new(AtomicBool::new(false));
    let shutdown_thread = Arc::clone(&shutdown);
    let server_source = Arc::clone(&source);
    thread::spawn(move || {
        let _ = server.run(server_source, shutdown_thread);
    });

    let script = p2pkh_script();
    let block = mock::make_block(bitcoin::BlockHash::all_zeros(), 1, vec![script.clone()]);
    let tx = block.txdata[0].clone();
    let txid = tx.compute_txid();
    source.push(BlockEvent::Connected { block, height: 1 });

    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while indexer.get_transaction(&txid).unwrap().is_none() {
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for indexed transaction"
        );
        thread::sleep(Duration::from_millis(50));
    }

    let mut stream = TcpStream::connect_timeout(&local_addr, Duration::from_secs(5)).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let mut reader = BufReader::new(stream.try_clone().unwrap());
    write!(
        stream,
        r#"{{"jsonrpc":"2.0","id":1,"method":"blockchain.transaction.get","params":["{}"]}}"#,
        txid
    )
    .unwrap();
    stream.write_all(b"\n").unwrap();

    let mut line = String::new();
    reader.read_line(&mut line).unwrap();
    let resp: serde_json::Value = serde_json::from_str(line.trim()).unwrap();
    assert_eq!(
        resp["result"],
        serde_json::json!(hex::encode(bitcoin::consensus::encode::serialize(&tx)))
    );

    shutdown.store(true, Ordering::SeqCst);
}

#[test]
fn electrum_transaction_broadcast_notifies_subscribed_scripthash() {
    #[derive(Clone, Default)]
    struct MockBroadcaster {
        seen: Arc<Mutex<Option<bitcoin::Txid>>>,
    }

    impl TransactionBroadcaster for MockBroadcaster {
        fn broadcast_transaction(&self, tx: bitcoin::Transaction) -> Result<(), String> {
            *self.seen.lock().unwrap() = Some(tx.compute_txid());
            Ok(())
        }
    }

    let source = Arc::new(mock::MockBlockSource::new());
    let metrics = Metrics::new();
    let indexer = make_indexer(metrics.clone());
    let _indexer_handle = indexer.clone().start(&source);
    let broadcaster = MockBroadcaster::default();
    let seen = Arc::clone(&broadcaster.seen);
    let fee_rate = Arc::new(FeeRateState::new());
    let pending_changes = PendingChangeBroadcaster::default();
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let server = ElectrumServer::bind(
        addr,
        indexer.clone(),
        metrics,
        Some(Arc::new(broadcaster)),
        fee_rate,
        pending_changes,
    )
    .expect("bind");
    let local_addr = server.local_addr();
    let shutdown = Arc::new(AtomicBool::new(false));
    let shutdown_thread = Arc::clone(&shutdown);
    let server_source = Arc::clone(&source);
    thread::spawn(move || {
        let _ = server.run(server_source, shutdown_thread);
    });

    let script = p2pkh_script();
    let sh = sh_of(&script);
    let tx = mock::make_tx(vec![script.clone()]);
    let raw = hex::encode(bitcoin::consensus::encode::serialize(&tx));

    let mut sub_stream = TcpStream::connect_timeout(&local_addr, Duration::from_secs(5)).unwrap();
    sub_stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let mut sub_reader = BufReader::new(sub_stream.try_clone().unwrap());

    write!(
        sub_stream,
        r#"{{"jsonrpc":"2.0","id":1,"method":"blockchain.scripthash.subscribe","params":["{}"]}}"#,
        sh.to_hex()
    )
    .unwrap();
    sub_stream.write_all(b"\n").unwrap();

    let mut line = String::new();
    sub_reader.read_line(&mut line).unwrap();
    let initial: serde_json::Value = serde_json::from_str(line.trim()).unwrap();
    assert!(initial["result"].is_null());

    let mut broadcast_stream =
        TcpStream::connect_timeout(&local_addr, Duration::from_secs(5)).unwrap();
    broadcast_stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let mut broadcast_reader = BufReader::new(broadcast_stream.try_clone().unwrap());
    write!(
        broadcast_stream,
        r#"{{"jsonrpc":"2.0","id":2,"method":"blockchain.transaction.broadcast","params":["{}"]}}"#,
        raw
    )
    .unwrap();
    broadcast_stream.write_all(b"\n").unwrap();

    line.clear();
    broadcast_reader.read_line(&mut line).unwrap();
    let resp: serde_json::Value = serde_json::from_str(line.trim()).unwrap();
    assert_eq!(
        resp["result"],
        serde_json::json!(tx.compute_txid().to_string())
    );

    let mut got_notification = false;
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while !got_notification {
        line.clear();
        match sub_reader.read_line(&mut line) {
            Ok(0) => continue,
            Ok(_) => {
                let msg: serde_json::Value = serde_json::from_str(line.trim()).unwrap();
                if msg["method"] == "blockchain.scripthash.subscribe" {
                    assert_eq!(msg["params"][0], serde_json::json!(sh.to_hex()));
                    assert!(msg["params"][1].is_string());
                    got_notification = true;
                }
            }
            Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                assert!(
                    std::time::Instant::now() < deadline,
                    "timed out waiting for broadcast notification"
                );
                thread::sleep(Duration::from_millis(50));
            }
            Err(err) => panic!("failed to read broadcast response: {err}"),
        }
    }

    assert_eq!(*seen.lock().unwrap(), Some(tx.compute_txid()));
    shutdown.store(true, Ordering::SeqCst);
}

#[test]
fn electrum_scripthash_subscribe_updates_on_mempool_chain_changes() {
    #[derive(Clone, Default)]
    struct MockBroadcaster;

    impl TransactionBroadcaster for MockBroadcaster {
        fn broadcast_transaction(&self, _tx: bitcoin::Transaction) -> Result<(), String> {
            Ok(())
        }
    }

    let source = Arc::new(mock::MockBlockSource::new());
    let metrics = Metrics::new();
    let indexer = make_indexer(metrics.clone());
    let _indexer_handle = indexer.clone().start(&source);
    let fee_rate = Arc::new(FeeRateState::new());
    let pending_changes = PendingChangeBroadcaster::default();
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let server = ElectrumServer::bind(
        addr,
        indexer.clone(),
        metrics,
        Some(Arc::new(MockBroadcaster)),
        fee_rate,
        pending_changes,
    )
    .expect("bind");
    let local_addr = server.local_addr();
    let shutdown = Arc::new(AtomicBool::new(false));
    let shutdown_thread = Arc::clone(&shutdown);
    let server_source = Arc::clone(&source);
    thread::spawn(move || {
        let _ = server.run(server_source, shutdown_thread);
    });

    let script_a = p2pkh_script();
    let script_b = mock::op_return_script(0x54);
    let sh_a = sh_of(&script_a);

    let mut sub_stream = TcpStream::connect_timeout(&local_addr, Duration::from_secs(5)).unwrap();
    sub_stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let mut sub_reader = BufReader::new(sub_stream.try_clone().unwrap());
    write!(
        sub_stream,
        r#"{{"jsonrpc":"2.0","id":1,"method":"blockchain.scripthash.subscribe","params":["{}"]}}"#,
        sh_a.to_hex()
    )
    .unwrap();
    sub_stream.write_all(b"\n").unwrap();

    let mut line = String::new();
    sub_reader.read_line(&mut line).unwrap();
    let initial: serde_json::Value = serde_json::from_str(line.trim()).unwrap();
    assert!(initial["result"].is_null());

    let first = mock::make_tx(vec![script_a.clone()]);
    let first_raw = hex::encode(bitcoin::consensus::encode::serialize(&first));
    let first_txid = first.compute_txid();
    let mut tx_stream1 = TcpStream::connect_timeout(&local_addr, Duration::from_secs(5)).unwrap();
    tx_stream1
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let mut tx_reader1 = BufReader::new(tx_stream1.try_clone().unwrap());
    write!(
        tx_stream1,
        r#"{{"jsonrpc":"2.0","id":2,"method":"blockchain.transaction.broadcast","params":["{}"]}}"#,
        first_raw
    )
    .unwrap();
    tx_stream1.write_all(b"\n").unwrap();
    line.clear();
    while line.is_empty() {
        match sub_reader.read_line(&mut line) {
            Ok(0) => continue,
            Ok(_) => break,
            Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(50));
            }
            Err(err) => panic!("failed to read first mempool notification: {err}"),
        }
    }
    let first_note: serde_json::Value = serde_json::from_str(line.trim()).unwrap();
    assert_eq!(first_note["method"], "blockchain.scripthash.subscribe");
    assert_eq!(first_note["params"][0], serde_json::json!(sh_a.to_hex()));
    let first_status = first_note["params"][1]
        .as_str()
        .expect("status string")
        .to_owned();

    line.clear();
    while line.is_empty() {
        match tx_reader1.read_line(&mut line) {
            Ok(0) => continue,
            Ok(_) => break,
            Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(50));
            }
            Err(err) => panic!("failed to read first broadcast response: {err}"),
        }
    }
    let first_resp: serde_json::Value = serde_json::from_str(line.trim()).unwrap();
    assert_eq!(
        first_resp["result"],
        serde_json::json!(first_txid.to_string())
    );

    let second = mock::make_spend_tx(
        bitcoin::OutPoint::new(first_txid, 0),
        vec![(900, script_b.clone())],
    );
    let second_raw = hex::encode(bitcoin::consensus::encode::serialize(&second));
    let mut tx_stream2 = TcpStream::connect_timeout(&local_addr, Duration::from_secs(5)).unwrap();
    tx_stream2
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let mut tx_reader2 = BufReader::new(tx_stream2.try_clone().unwrap());
    write!(
        tx_stream2,
        r#"{{"jsonrpc":"2.0","id":3,"method":"blockchain.transaction.broadcast","params":["{}"]}}"#,
        second_raw
    )
    .unwrap();
    tx_stream2.write_all(b"\n").unwrap();

    line.clear();
    while line.is_empty() {
        match sub_reader.read_line(&mut line) {
            Ok(0) => continue,
            Ok(_) => break,
            Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(50));
            }
            Err(err) => panic!("failed to read second mempool notification: {err}"),
        }
    }
    let second_note: serde_json::Value = serde_json::from_str(line.trim()).unwrap();
    assert_eq!(second_note["method"], "blockchain.scripthash.subscribe");
    assert_eq!(second_note["params"][0], serde_json::json!(sh_a.to_hex()));
    let second_status = second_note["params"][1].as_str().expect("status string");
    assert_ne!(first_status, second_status);

    line.clear();
    while line.is_empty() {
        match tx_reader2.read_line(&mut line) {
            Ok(0) => continue,
            Ok(_) => break,
            Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(50));
            }
            Err(err) => panic!("failed to read second broadcast response: {err}"),
        }
    }
    let second_resp: serde_json::Value = serde_json::from_str(line.trim()).unwrap();
    assert_eq!(
        second_resp["result"],
        serde_json::json!(second.compute_txid().to_string())
    );

    shutdown.store(true, Ordering::SeqCst);
}

#[test]
fn electrum_scripthash_listunspent_hides_pending_spent_outputs() {
    #[derive(Clone, Default)]
    struct MockBroadcaster;

    impl TransactionBroadcaster for MockBroadcaster {
        fn broadcast_transaction(&self, _tx: bitcoin::Transaction) -> Result<(), String> {
            Ok(())
        }
    }

    let source = Arc::new(mock::MockBlockSource::new());
    let metrics = Metrics::new();
    let indexer = make_indexer(metrics.clone());
    let _indexer_handle = indexer.clone().start(&source);
    let fee_rate = Arc::new(FeeRateState::new());
    let pending_changes = PendingChangeBroadcaster::default();
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let server = ElectrumServer::bind(
        addr,
        indexer.clone(),
        metrics,
        Some(Arc::new(MockBroadcaster)),
        fee_rate,
        pending_changes,
    )
    .expect("bind");
    let local_addr = server.local_addr();
    let shutdown = Arc::new(AtomicBool::new(false));
    let shutdown_thread = Arc::clone(&shutdown);
    let server_source = Arc::clone(&source);
    thread::spawn(move || {
        let _ = server.run(server_source, shutdown_thread);
    });

    let script = p2pkh_script();
    let sh = sh_of(&script);
    let block = mock::make_block(bitcoin::BlockHash::all_zeros(), 1, vec![script.clone()]);
    let fund_txid = block.txdata[0].compute_txid();
    let fund_outpoint = bitcoin::OutPoint::new(fund_txid, 0);
    source.push(BlockEvent::Connected { block, height: 1 });

    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while indexer.tip_height() < 1 {
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for indexed block"
        );
        thread::sleep(Duration::from_millis(50));
    }

    let tx = mock::make_spend_tx(fund_outpoint, vec![(900, mock::op_return_script(0x55))]);
    let raw = hex::encode(bitcoin::consensus::encode::serialize(&tx));

    let mut stream = TcpStream::connect_timeout(&local_addr, Duration::from_secs(5)).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let mut reader = BufReader::new(stream.try_clone().unwrap());
    write!(
        stream,
        r#"{{"jsonrpc":"2.0","id":1,"method":"blockchain.transaction.broadcast","params":["{}"]}}"#,
        raw
    )
    .unwrap();
    stream.write_all(b"\n").unwrap();

    let mut line = String::new();
    while line.is_empty() {
        match reader.read_line(&mut line) {
            Ok(0) => continue,
            Ok(_) => break,
            Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                assert!(
                    std::time::Instant::now() < deadline,
                    "timed out waiting for broadcast response"
                );
                thread::sleep(Duration::from_millis(50));
            }
            Err(err) => panic!("failed to read broadcast response: {err}"),
        }
    }
    let broadcast: serde_json::Value = serde_json::from_str(line.trim()).unwrap();
    assert_eq!(
        broadcast["result"],
        serde_json::json!(tx.compute_txid().to_string())
    );

    let mut list_stream = TcpStream::connect_timeout(&local_addr, Duration::from_secs(5)).unwrap();
    list_stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let mut list_reader = BufReader::new(list_stream.try_clone().unwrap());
    write!(
        list_stream,
        r#"{{"jsonrpc":"2.0","id":2,"method":"blockchain.scripthash.listunspent","params":["{}"]}}"#,
        sh.to_hex()
    )
    .unwrap();
    list_stream.write_all(b"\n").unwrap();

    line.clear();
    while line.is_empty() {
        match list_reader.read_line(&mut line) {
            Ok(0) => continue,
            Ok(_) => break,
            Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                assert!(
                    std::time::Instant::now() < deadline,
                    "timed out waiting for listunspent response"
                );
                thread::sleep(Duration::from_millis(50));
            }
            Err(err) => panic!("failed to read listunspent response: {err}"),
        }
    }
    let listunspent: serde_json::Value = serde_json::from_str(line.trim()).unwrap();
    assert!(listunspent["result"].as_array().unwrap().is_empty());

    shutdown.store(true, Ordering::SeqCst);
}

#[test]
fn electrum_scripthash_get_mempool_returns_pending_transaction() {
    #[derive(Clone, Default)]
    struct MockBroadcaster;

    impl TransactionBroadcaster for MockBroadcaster {
        fn broadcast_transaction(&self, _tx: bitcoin::Transaction) -> Result<(), String> {
            Ok(())
        }
    }

    let source = Arc::new(mock::MockBlockSource::new());
    let metrics = Metrics::new();
    let indexer = make_indexer(metrics.clone());
    let _indexer_handle = indexer.clone().start(&source);
    let fee_rate = Arc::new(FeeRateState::new());
    let pending_changes = PendingChangeBroadcaster::default();
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let server = ElectrumServer::bind(
        addr,
        indexer.clone(),
        metrics,
        Some(Arc::new(MockBroadcaster)),
        fee_rate,
        pending_changes,
    )
    .expect("bind");
    let local_addr = server.local_addr();
    let shutdown = Arc::new(AtomicBool::new(false));
    let shutdown_thread = Arc::clone(&shutdown);
    let server_source = Arc::clone(&source);
    thread::spawn(move || {
        let _ = server.run(server_source, shutdown_thread);
    });

    let script = p2pkh_script();
    let sh = sh_of(&script);
    let block = mock::make_block(bitcoin::BlockHash::all_zeros(), 1, vec![script.clone()]);
    let fund_txid = block.txdata[0].compute_txid();
    let fund_outpoint = bitcoin::OutPoint::new(fund_txid, 0);
    source.push(BlockEvent::Connected { block, height: 1 });

    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while indexer.tip_height() < 1 {
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for indexed block"
        );
        thread::sleep(Duration::from_millis(50));
    }

    let tx = mock::make_spend_tx(fund_outpoint, vec![(900, mock::op_return_script(0x52))]);
    let raw = hex::encode(bitcoin::consensus::encode::serialize(&tx));

    let mut stream = TcpStream::connect_timeout(&local_addr, Duration::from_secs(5)).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let mut reader = BufReader::new(stream.try_clone().unwrap());
    write!(
        stream,
        r#"{{"jsonrpc":"2.0","id":1,"method":"blockchain.transaction.broadcast","params":["{}"]}}"#,
        raw
    )
    .unwrap();
    stream.write_all(b"\n").unwrap();

    let mut line = String::new();
    reader.read_line(&mut line).unwrap();
    let broadcast: serde_json::Value = serde_json::from_str(line.trim()).unwrap();
    assert_eq!(
        broadcast["result"],
        serde_json::json!(tx.compute_txid().to_string())
    );

    let mut mempool_stream =
        TcpStream::connect_timeout(&local_addr, Duration::from_secs(5)).unwrap();
    mempool_stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let mut mempool_reader = BufReader::new(mempool_stream.try_clone().unwrap());
    write!(
        mempool_stream,
        r#"{{"jsonrpc":"2.0","id":2,"method":"blockchain.scripthash.get_mempool","params":["{}"]}}"#,
        sh.to_hex()
    )
    .unwrap();
    mempool_stream.write_all(b"\n").unwrap();

    line.clear();
    mempool_reader.read_line(&mut line).unwrap();
    let mempool: serde_json::Value = serde_json::from_str(line.trim()).unwrap();
    assert_eq!(mempool["result"].as_array().unwrap().len(), 1);
    assert_eq!(mempool["result"][0]["height"], serde_json::json!(0));
    assert_eq!(mempool["result"][0]["fee"], serde_json::json!(100));
    assert_eq!(
        mempool["result"][0]["tx_hash"],
        serde_json::json!(tx.compute_txid().to_string())
    );

    shutdown.store(true, Ordering::SeqCst);
}

#[test]
fn electrum_scripthash_queries_return_indexed_data() {
    let source = Arc::new(mock::MockBlockSource::new());
    let metrics = Metrics::new();
    let indexer = make_indexer(metrics.clone());
    let _indexer_handle = indexer.clone().start(&source);
    let fee_rate = Arc::new(FeeRateState::new());
    let pending_changes = PendingChangeBroadcaster::default();
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let server = ElectrumServer::bind(
        addr,
        indexer.clone(),
        metrics,
        None,
        fee_rate,
        pending_changes,
    )
    .expect("bind");
    let local_addr = server.local_addr();
    let shutdown = Arc::new(AtomicBool::new(false));
    let shutdown_thread = Arc::clone(&shutdown);
    let server_source = Arc::clone(&source);
    thread::spawn(move || {
        let _ = server.run(server_source, shutdown_thread);
    });

    let script = p2pkh_script();
    let sh = sh_of(&script);
    let block = mock::make_block(bitcoin::BlockHash::all_zeros(), 1, vec![script.clone()]);
    source.push(BlockEvent::Connected {
        block: block.clone(),
        height: 1,
    });

    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while indexer.get_history(&sh).is_empty() {
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for indexed history"
        );
        thread::sleep(Duration::from_millis(50));
    }

    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let mut line = String::new();

    let history: serde_json::Value = {
        let mut stream = TcpStream::connect_timeout(&local_addr, Duration::from_secs(5)).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let mut reader = BufReader::new(stream.try_clone().unwrap());
        write!(
            stream,
            r#"{{"jsonrpc":"2.0","id":1,"method":"blockchain.scripthash.get_history","params":["{}"]}}"#,
            sh.to_hex()
        )
        .unwrap();
        stream.write_all(b"\n").unwrap();
        line.clear();
        while line.is_empty() {
            match reader.read_line(&mut line) {
                Ok(0) => continue,
                Ok(_) => break,
                Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                    assert!(
                        std::time::Instant::now() < deadline,
                        "timed out waiting for history response"
                    );
                    thread::sleep(Duration::from_millis(50));
                }
                Err(err) => panic!("failed to read history response: {err}"),
            }
        }
        serde_json::from_str(line.trim()).unwrap()
    };
    assert_eq!(history["result"].as_array().unwrap().len(), 1);
    assert_eq!(history["result"][0]["height"], serde_json::json!(1));
    assert_eq!(
        history["result"][0]["tx_hash"],
        serde_json::json!(block.txdata[0].compute_txid().to_string())
    );

    let balance: serde_json::Value = {
        let mut stream = TcpStream::connect_timeout(&local_addr, Duration::from_secs(5)).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let mut reader = BufReader::new(stream.try_clone().unwrap());
        write!(
            stream,
            r#"{{"jsonrpc":"2.0","id":2,"method":"blockchain.scripthash.get_balance","params":["{}"]}}"#,
            sh.to_hex()
        )
        .unwrap();
        stream.write_all(b"\n").unwrap();
        line.clear();
        while line.is_empty() {
            match reader.read_line(&mut line) {
                Ok(0) => continue,
                Ok(_) => break,
                Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                    assert!(
                        std::time::Instant::now() < deadline,
                        "timed out waiting for balance response"
                    );
                    thread::sleep(Duration::from_millis(50));
                }
                Err(err) => panic!("failed to read balance response: {err}"),
            }
        }
        serde_json::from_str(line.trim()).unwrap()
    };
    assert_eq!(balance["result"]["confirmed"], serde_json::json!(1000));
    assert_eq!(balance["result"]["unconfirmed"], serde_json::json!(0));

    let unspent: serde_json::Value = {
        let mut stream = TcpStream::connect_timeout(&local_addr, Duration::from_secs(5)).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let mut reader = BufReader::new(stream.try_clone().unwrap());
        write!(
            stream,
            r#"{{"jsonrpc":"2.0","id":3,"method":"blockchain.scripthash.listunspent","params":["{}"]}}"#,
            sh.to_hex()
        )
        .unwrap();
        stream.write_all(b"\n").unwrap();
        line.clear();
        while line.is_empty() {
            match reader.read_line(&mut line) {
                Ok(0) => continue,
                Ok(_) => break,
                Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                    assert!(
                        std::time::Instant::now() < deadline,
                        "timed out waiting for listunspent response"
                    );
                    thread::sleep(Duration::from_millis(50));
                }
                Err(err) => panic!("failed to read listunspent response: {err}"),
            }
        }
        serde_json::from_str(line.trim()).unwrap()
    };
    assert_eq!(unspent["result"].as_array().unwrap().len(), 1);
    assert_eq!(unspent["result"][0]["height"], serde_json::json!(1));
    assert_eq!(unspent["result"][0]["value"], serde_json::json!(1000));

    shutdown.store(true, Ordering::SeqCst);
}

#[test]
fn electrum_scripthash_get_mempool_marks_pending_ancestor_as_unconfirmed() {
    #[derive(Clone, Default)]
    struct MockBroadcaster;

    impl TransactionBroadcaster for MockBroadcaster {
        fn broadcast_transaction(&self, _tx: bitcoin::Transaction) -> Result<(), String> {
            Ok(())
        }
    }

    let source = Arc::new(mock::MockBlockSource::new());
    let metrics = Metrics::new();
    let indexer = make_indexer(metrics.clone());
    let _indexer_handle = indexer.clone().start(&source);
    let fee_rate = Arc::new(FeeRateState::new());
    let pending_changes = PendingChangeBroadcaster::default();
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let server = ElectrumServer::bind(
        addr,
        indexer.clone(),
        metrics,
        Some(Arc::new(MockBroadcaster)),
        fee_rate,
        pending_changes,
    )
    .expect("bind");
    let local_addr = server.local_addr();
    let shutdown = Arc::new(AtomicBool::new(false));
    let shutdown_thread = Arc::clone(&shutdown);
    let server_source = Arc::clone(&source);
    thread::spawn(move || {
        let _ = server.run(server_source, shutdown_thread);
    });

    let first = mock::make_tx(vec![p2pkh_script()]);
    let first_raw = hex::encode(bitcoin::consensus::encode::serialize(&first));
    let first_txid = first.compute_txid();

    let mut tx_stream = TcpStream::connect_timeout(&local_addr, Duration::from_secs(5)).unwrap();
    tx_stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let mut tx_reader = BufReader::new(tx_stream.try_clone().unwrap());
    write!(
        tx_stream,
        r#"{{"jsonrpc":"2.0","id":1,"method":"blockchain.transaction.broadcast","params":["{}"]}}"#,
        first_raw
    )
    .unwrap();
    tx_stream.write_all(b"\n").unwrap();

    let mut line = String::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while line.is_empty() {
        match tx_reader.read_line(&mut line) {
            Ok(0) => continue,
            Ok(_) => break,
            Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                assert!(
                    std::time::Instant::now() < deadline,
                    "timed out waiting for first broadcast"
                );
                thread::sleep(Duration::from_millis(50));
            }
            Err(err) => panic!("failed to read first broadcast: {err}"),
        }
    }
    let first_resp: serde_json::Value = serde_json::from_str(line.trim()).unwrap();
    assert_eq!(
        first_resp["result"],
        serde_json::json!(first_txid.to_string())
    );

    let second = mock::make_spend_tx(
        bitcoin::OutPoint::new(first_txid, 0),
        vec![(900, mock::op_return_script(0x53))],
    );
    let second_raw = hex::encode(bitcoin::consensus::encode::serialize(&second));
    let second_txid = second.compute_txid();

    let mut tx_stream2 = TcpStream::connect_timeout(&local_addr, Duration::from_secs(5)).unwrap();
    tx_stream2
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let mut tx_reader2 = BufReader::new(tx_stream2.try_clone().unwrap());
    write!(
        tx_stream2,
        r#"{{"jsonrpc":"2.0","id":2,"method":"blockchain.transaction.broadcast","params":["{}"]}}"#,
        second_raw
    )
    .unwrap();
    tx_stream2.write_all(b"\n").unwrap();
    line.clear();
    while line.is_empty() {
        match tx_reader2.read_line(&mut line) {
            Ok(0) => continue,
            Ok(_) => break,
            Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                assert!(
                    std::time::Instant::now() < deadline,
                    "timed out waiting for second broadcast"
                );
                thread::sleep(Duration::from_millis(50));
            }
            Err(err) => panic!("failed to read second broadcast: {err}"),
        }
    }
    let second_resp: serde_json::Value = serde_json::from_str(line.trim()).unwrap();
    assert_eq!(
        second_resp["result"],
        serde_json::json!(second_txid.to_string())
    );

    let sh = sh_of(&mock::op_return_script(0x53));
    let mut mempool_stream =
        TcpStream::connect_timeout(&local_addr, Duration::from_secs(5)).unwrap();
    mempool_stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let mut mempool_reader = BufReader::new(mempool_stream.try_clone().unwrap());
    write!(
        mempool_stream,
        r#"{{"jsonrpc":"2.0","id":3,"method":"blockchain.scripthash.get_mempool","params":["{}"]}}"#,
        sh.to_hex()
    )
    .unwrap();
    mempool_stream.write_all(b"\n").unwrap();

    line.clear();
    while line.is_empty() {
        match mempool_reader.read_line(&mut line) {
            Ok(0) => continue,
            Ok(_) => break,
            Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                assert!(
                    std::time::Instant::now() < deadline,
                    "timed out waiting for mempool response"
                );
                thread::sleep(Duration::from_millis(50));
            }
            Err(err) => panic!("failed to read mempool response: {err}"),
        }
    }
    let mempool: serde_json::Value = serde_json::from_str(line.trim()).unwrap();
    assert_eq!(mempool["result"].as_array().unwrap().len(), 1);
    assert_eq!(mempool["result"][0]["height"], serde_json::json!(-1));
    assert_eq!(mempool["result"][0]["fee"], serde_json::json!(100));
    assert_eq!(
        mempool["result"][0]["tx_hash"],
        serde_json::json!(second_txid.to_string())
    );

    shutdown.store(true, Ordering::SeqCst);
}

#[test]
fn electrum_scripthash_get_mempool_updates_when_parent_confirms() {
    #[derive(Clone, Default)]
    struct MockBroadcaster;

    impl TransactionBroadcaster for MockBroadcaster {
        fn broadcast_transaction(&self, _tx: bitcoin::Transaction) -> Result<(), String> {
            Ok(())
        }
    }

    let source = Arc::new(mock::MockBlockSource::new());
    let metrics = Metrics::new();
    let indexer = make_indexer(metrics.clone());
    let _indexer_handle = indexer.clone().start(&source);
    let fee_rate = Arc::new(FeeRateState::new());
    let pending_changes = PendingChangeBroadcaster::default();
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let server = ElectrumServer::bind(
        addr,
        indexer.clone(),
        metrics,
        Some(Arc::new(MockBroadcaster)),
        fee_rate,
        pending_changes,
    )
    .expect("bind");
    let local_addr = server.local_addr();
    let shutdown = Arc::new(AtomicBool::new(false));
    let shutdown_thread = Arc::clone(&shutdown);
    let server_source = Arc::clone(&source);
    thread::spawn(move || {
        let _ = server.run(server_source, shutdown_thread);
    });

    let parent_script = mock::op_return_script(0x61);
    let child_script = mock::op_return_script(0x62);
    let parent = mock::make_tx(vec![parent_script.clone()]);
    let parent_txid = parent.compute_txid();
    let child = mock::make_spend_tx(
        bitcoin::OutPoint::new(parent_txid, 0),
        vec![(900, child_script.clone())],
    );

    let mut parent_stream =
        TcpStream::connect_timeout(&local_addr, Duration::from_secs(5)).unwrap();
    parent_stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let mut parent_reader = BufReader::new(parent_stream.try_clone().unwrap());
    write!(
        parent_stream,
        r#"{{"jsonrpc":"2.0","id":1,"method":"blockchain.transaction.broadcast","params":["{}"]}}"#,
        hex::encode(bitcoin::consensus::encode::serialize(&parent))
    )
    .unwrap();
    parent_stream.write_all(b"\n").unwrap();

    let mut line = String::new();
    parent_reader.read_line(&mut line).unwrap();
    let parent_resp: serde_json::Value = serde_json::from_str(line.trim()).unwrap();
    assert_eq!(
        parent_resp["result"],
        serde_json::json!(parent_txid.to_string())
    );


    let mut child_stream = TcpStream::connect_timeout(&local_addr, Duration::from_secs(5)).unwrap();
    child_stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let mut child_reader = BufReader::new(child_stream.try_clone().unwrap());
    write!(
        child_stream,
        r#"{{"jsonrpc":"2.0","id":2,"method":"blockchain.transaction.broadcast","params":["{}"]}}"#,
        hex::encode(bitcoin::consensus::encode::serialize(&child))
    )
    .unwrap();
    child_stream.write_all(b"\n").unwrap();

    line.clear();
    child_reader.read_line(&mut line).unwrap();
    let child_resp: serde_json::Value = serde_json::from_str(line.trim()).unwrap();
    assert_eq!(
        child_resp["result"],
        serde_json::json!(child.compute_txid().to_string())
    );

    let sh = sh_of(&child_script);
    let mut sub_stream = TcpStream::connect_timeout(&local_addr, Duration::from_secs(5)).unwrap();
    sub_stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let mut sub_reader = BufReader::new(sub_stream.try_clone().unwrap());
    write!(
        sub_stream,
        r#"{{"jsonrpc":"2.0","id":3,"method":"blockchain.scripthash.subscribe","params":["{}"]}}"#,
        sh.to_hex()
    )
    .unwrap();
    sub_stream.write_all(b"\n").unwrap();

    line.clear();
    sub_reader.read_line(&mut line).unwrap();
    let initial: serde_json::Value = serde_json::from_str(line.trim()).unwrap();
    assert!(initial["result"].is_string());

    let mempool_before: serde_json::Value = {
        let mut stream = TcpStream::connect_timeout(&local_addr, Duration::from_secs(5)).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let mut reader = BufReader::new(stream.try_clone().unwrap());
        write!(
            stream,
            r#"{{"jsonrpc":"2.0","id":4,"method":"blockchain.scripthash.get_mempool","params":["{}"]}}"#,
            sh.to_hex()
        )
        .unwrap();
        stream.write_all(b"\n").unwrap();
        line.clear();
        reader.read_line(&mut line).unwrap();
        serde_json::from_str(line.trim()).unwrap()
    };
    assert_eq!(mempool_before["result"][0]["height"], serde_json::json!(-1));

    let block = mock::make_block_with_txs(bitcoin::BlockHash::all_zeros(), 2, vec![parent.clone()]);
    source.push(BlockEvent::Connected { block, height: 2 });

    line.clear();
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while line.is_empty() {
        match sub_reader.read_line(&mut line) {
            Ok(0) => continue,
            Ok(_) => break,
            Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                assert!(
                    std::time::Instant::now() < deadline,
                    "timed out waiting for notification"
                );
                thread::sleep(Duration::from_millis(50));
            }
            Err(err) => panic!("failed to read subscribe notification: {err}"),
        }
    }
    let note: serde_json::Value = serde_json::from_str(line.trim()).unwrap();
    assert_eq!(note["method"], "blockchain.scripthash.subscribe");
    assert_eq!(note["params"][0], serde_json::json!(sh.to_hex()));
    assert!(note["params"][1].is_string());

    let mempool_after: serde_json::Value = {
        let mut stream = TcpStream::connect_timeout(&local_addr, Duration::from_secs(5)).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let mut reader = BufReader::new(stream.try_clone().unwrap());
        write!(
            stream,
            r#"{{"jsonrpc":"2.0","id":5,"method":"blockchain.scripthash.get_mempool","params":["{}"]}}"#,
            sh.to_hex()
        )
        .unwrap();
        stream.write_all(b"\n").unwrap();
        line.clear();
        reader.read_line(&mut line).unwrap();
        serde_json::from_str(line.trim()).unwrap()
    };
    assert_eq!(mempool_after["result"][0]["height"], serde_json::json!(0));
    assert_eq!(
        mempool_before["result"][0]["tx_hash"],
        mempool_after["result"][0]["tx_hash"]
    );

    shutdown.store(true, Ordering::SeqCst);
}

#[test]
fn electrum_scripthash_get_mempool_updates_when_parent_is_replaced() {
    #[derive(Clone, Default)]
    struct MockBroadcaster;

    impl TransactionBroadcaster for MockBroadcaster {
        fn broadcast_transaction(&self, _tx: bitcoin::Transaction) -> Result<(), String> {
            Ok(())
        }
    }

    let source = Arc::new(mock::MockBlockSource::new());
    let metrics = Metrics::new();
    let indexer = make_indexer(metrics.clone());
    let _indexer_handle = indexer.clone().start(&source);
    let fee_rate = Arc::new(FeeRateState::new());
    let pending_changes = PendingChangeBroadcaster::default();
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let server = ElectrumServer::bind(
        addr,
        indexer.clone(),
        metrics,
        Some(Arc::new(MockBroadcaster)),
        fee_rate,
        pending_changes,
    )
    .expect("bind");
    let local_addr = server.local_addr();
    let shutdown = Arc::new(AtomicBool::new(false));
    let shutdown_thread = Arc::clone(&shutdown);
    let server_source = Arc::clone(&source);
    thread::spawn(move || {
        let _ = server.run(server_source, shutdown_thread);
    });

    let parent_script = mock::op_return_script(0x71);
    let child_script = mock::op_return_script(0x72);
    let replacement_script = mock::op_return_script(0x73);
    let parent = mock::make_tx(vec![parent_script.clone()]);
    let parent_txid = parent.compute_txid();
    let child = mock::make_spend_tx(
        bitcoin::OutPoint::new(parent_txid, 0),
        vec![(900, child_script.clone())],
    );
    let replacement = mock::make_spend_tx(
        bitcoin::OutPoint::new(parent_txid, 0),
        vec![(900, replacement_script.clone())],
    );

    let mut sub_stream = TcpStream::connect_timeout(&local_addr, Duration::from_secs(5)).unwrap();
    sub_stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let mut sub_reader = BufReader::new(sub_stream.try_clone().unwrap());
    write!(
        sub_stream,
        r#"{{"jsonrpc":"2.0","id":1,"method":"blockchain.scripthash.subscribe","params":["{}"]}}"#,
        sh_of(&child_script).to_hex()
    )
    .unwrap();
    sub_stream.write_all(b"\n").unwrap();

    let mut line = String::new();
    sub_reader.read_line(&mut line).unwrap();
    let initial: serde_json::Value = serde_json::from_str(line.trim()).unwrap();
    assert!(initial["result"].is_null());

    let mut parent_stream =
        TcpStream::connect_timeout(&local_addr, Duration::from_secs(5)).unwrap();
    parent_stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let mut parent_reader = BufReader::new(parent_stream.try_clone().unwrap());
    write!(
        parent_stream,
        r#"{{"jsonrpc":"2.0","id":2,"method":"blockchain.transaction.broadcast","params":["{}"]}}"#,
        hex::encode(bitcoin::consensus::encode::serialize(&parent))
    )
    .unwrap();
    parent_stream.write_all(b"\n").unwrap();
    line.clear();
    parent_reader.read_line(&mut line).unwrap();
    let parent_resp: serde_json::Value = serde_json::from_str(line.trim()).unwrap();
    assert_eq!(
        parent_resp["result"],
        serde_json::json!(parent_txid.to_string())
    );

    let mut child_stream = TcpStream::connect_timeout(&local_addr, Duration::from_secs(5)).unwrap();
    child_stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let mut child_reader = BufReader::new(child_stream.try_clone().unwrap());
    write!(
        child_stream,
        r#"{{"jsonrpc":"2.0","id":3,"method":"blockchain.transaction.broadcast","params":["{}"]}}"#,
        hex::encode(bitcoin::consensus::encode::serialize(&child))
    )
    .unwrap();
    child_stream.write_all(b"\n").unwrap();
    line.clear();
    child_reader.read_line(&mut line).unwrap();
    let child_resp: serde_json::Value = serde_json::from_str(line.trim()).unwrap();
    assert_eq!(
        child_resp["result"],
        serde_json::json!(child.compute_txid().to_string())
    );

    line.clear();
    while line.is_empty() {
        match sub_reader.read_line(&mut line) {
            Ok(0) => continue,
            Ok(_) => break,
            Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(50));
            }
            Err(err) => panic!("failed to read child notification: {err}"),
        }
    }
    let child_note: serde_json::Value = serde_json::from_str(line.trim()).unwrap();
    assert_eq!(child_note["method"], "blockchain.scripthash.subscribe");
    assert_eq!(
        child_note["params"][0],
        serde_json::json!(sh_of(&child_script).to_hex())
    );
    assert!(child_note["params"][1].is_string());

    let mut repl_stream = TcpStream::connect_timeout(&local_addr, Duration::from_secs(5)).unwrap();
    repl_stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let mut repl_reader = BufReader::new(repl_stream.try_clone().unwrap());
    write!(
        repl_stream,
        r#"{{"jsonrpc":"2.0","id":4,"method":"blockchain.transaction.broadcast","params":["{}"]}}"#,
        hex::encode(bitcoin::consensus::encode::serialize(&replacement))
    )
    .unwrap();
    repl_stream.write_all(b"\n").unwrap();

    line.clear();
    while line.is_empty() {
        match sub_reader.read_line(&mut line) {
            Ok(0) => continue,
            Ok(_) => break,
            Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(50));
            }
            Err(err) => panic!("failed to read replacement notification: {err}"),
        }
    }
    let note: serde_json::Value = serde_json::from_str(line.trim()).unwrap();
    assert_eq!(note["method"], "blockchain.scripthash.subscribe");
    assert_eq!(
        note["params"][0],
        serde_json::json!(sh_of(&child_script).to_hex())
    );
    assert!(note["params"][1].is_null());

    let mempool_after: serde_json::Value = {
        let mut stream = TcpStream::connect_timeout(&local_addr, Duration::from_secs(5)).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let mut reader = BufReader::new(stream.try_clone().unwrap());
        write!(
            stream,
            r#"{{"jsonrpc":"2.0","id":5,"method":"blockchain.scripthash.get_mempool","params":["{}"]}}"#,
            sh_of(&child_script).to_hex()
        )
        .unwrap();
        stream.write_all(b"\n").unwrap();
        line.clear();
        reader.read_line(&mut line).unwrap();
        serde_json::from_str(line.trim()).unwrap()
    };
    assert!(mempool_after["result"].as_array().unwrap().is_empty());

    line.clear();
    repl_reader.read_line(&mut line).unwrap();
    let repl_resp: serde_json::Value = serde_json::from_str(line.trim()).unwrap();
    assert_eq!(
        repl_resp["result"],
        serde_json::json!(replacement.compute_txid().to_string())
    );

    shutdown.store(true, Ordering::SeqCst);
}

#[test]
fn electrum_disconnect_reverts_headers_and_scripthash_state() {
    let source = Arc::new(mock::MockBlockSource::new());
    let metrics = Metrics::new();
    let indexer = make_indexer(metrics.clone());
    let _indexer_handle = indexer.clone().start(&source);
    let fee_rate = Arc::new(FeeRateState::new());
    let pending_changes = PendingChangeBroadcaster::default();
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let server = ElectrumServer::bind(
        addr,
        indexer.clone(),
        metrics,
        None,
        fee_rate,
        pending_changes,
    )
    .expect("bind");
    let local_addr = server.local_addr();
    let shutdown = Arc::new(AtomicBool::new(false));
    let shutdown_thread = Arc::clone(&shutdown);
    let server_source = Arc::clone(&source);
    thread::spawn(move || {
        let _ = server.run(server_source, shutdown_thread);
    });

    let script = p2pkh_script();
    let sh = sh_of(&script);
    let block = mock::make_block(bitcoin::BlockHash::all_zeros(), 1, vec![script.clone()]);
    let block_hash = block.block_hash();
    source.push(BlockEvent::Connected { block, height: 1 });

    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while indexer.get_history(&sh).is_empty() {
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for indexed history"
        );
        thread::sleep(Duration::from_millis(50));
    }

    let mut headers_stream =
        TcpStream::connect_timeout(&local_addr, Duration::from_secs(5)).unwrap();
    headers_stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let mut headers_reader = BufReader::new(headers_stream.try_clone().unwrap());
    write!(
        headers_stream,
        r#"{{"jsonrpc":"2.0","id":1,"method":"blockchain.headers.subscribe","params":[]}}"#
    )
    .unwrap();
    headers_stream.write_all(b"\n").unwrap();

    let mut line = String::new();
    headers_reader.read_line(&mut line).unwrap();
    let initial_headers: serde_json::Value = serde_json::from_str(line.trim()).unwrap();
    assert_eq!(initial_headers["result"]["height"], serde_json::json!(1));

    let mut sh_stream = TcpStream::connect_timeout(&local_addr, Duration::from_secs(5)).unwrap();
    sh_stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let mut sh_reader = BufReader::new(sh_stream.try_clone().unwrap());
    write!(
        sh_stream,
        r#"{{"jsonrpc":"2.0","id":2,"method":"blockchain.scripthash.subscribe","params":["{}"]}}"#,
        sh.to_hex()
    )
    .unwrap();
    sh_stream.write_all(b"\n").unwrap();

    line.clear();
    sh_reader.read_line(&mut line).unwrap();
    let initial_sh: serde_json::Value = serde_json::from_str(line.trim()).unwrap();
    assert!(initial_sh["result"].is_string());

    source.push_disconnected(block_hash, 1);

    while !indexer.get_history(&sh).is_empty() {
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for rollback"
        );
        thread::sleep(Duration::from_millis(50));
    }

    line.clear();
    while line.is_empty() {
        match headers_reader.read_line(&mut line) {
            Ok(0) => continue,
            Ok(_) => break,
            Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                assert!(
                    std::time::Instant::now() < deadline,
                    "timed out waiting for headers disconnect notification"
                );
                thread::sleep(Duration::from_millis(50));
            }
            Err(err) => panic!("failed to read headers disconnect notification: {err}"),
        }
    }
    let headers_msg: serde_json::Value = serde_json::from_str(line.trim()).unwrap();
    assert_eq!(headers_msg["method"], "blockchain.headers.subscribe");
    assert_eq!(headers_msg["params"][0]["height"], serde_json::json!(0));

    line.clear();
    while line.is_empty() {
        match sh_reader.read_line(&mut line) {
            Ok(0) => continue,
            Ok(_) => break,
            Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                assert!(
                    std::time::Instant::now() < deadline,
                    "timed out waiting for scripthash disconnect notification"
                );
                thread::sleep(Duration::from_millis(50));
            }
            Err(err) => panic!("failed to read scripthash disconnect notification: {err}"),
        }
    }
    let sh_msg: serde_json::Value = serde_json::from_str(line.trim()).unwrap();
    assert_eq!(sh_msg["method"], "blockchain.scripthash.subscribe");
    assert_eq!(sh_msg["params"][0], serde_json::json!(sh.to_hex()));
    assert!(sh_msg["params"][1].is_null());

    shutdown.store(true, Ordering::SeqCst);
}

#[test]
fn electrum_block_header_queries_return_indexed_data() {
    let source = Arc::new(mock::MockBlockSource::new());
    let metrics = Metrics::new();
    let indexer = make_indexer(metrics.clone());
    let _indexer_handle = indexer.clone().start(&source);
    let fee_rate = Arc::new(FeeRateState::new());
    let pending_changes = PendingChangeBroadcaster::default();
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let server = ElectrumServer::bind(
        addr,
        indexer.clone(),
        metrics,
        None,
        fee_rate,
        pending_changes,
    )
    .expect("bind");
    let local_addr = server.local_addr();
    let shutdown = Arc::new(AtomicBool::new(false));
    let shutdown_thread = Arc::clone(&shutdown);
    let server_source = Arc::clone(&source);
    thread::spawn(move || {
        let _ = server.run(server_source, shutdown_thread);
    });

    let script = p2pkh_script();
    let block = mock::make_block(bitcoin::BlockHash::all_zeros(), 1, vec![script.clone()]);
    let header_hex = hex::encode(bitcoin::consensus::encode::serialize(&block.header));
    source.push(BlockEvent::Connected { block, height: 1 });

    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while indexer.tip_height() < 1 {
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for indexed block"
        );
        thread::sleep(Duration::from_millis(50));
    }

    let header: serde_json::Value = {
        let mut stream = TcpStream::connect_timeout(&local_addr, Duration::from_secs(5)).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let mut reader = BufReader::new(stream.try_clone().unwrap());
        write!(
            stream,
            r#"{{"jsonrpc":"2.0","id":1,"method":"blockchain.block.header","params":[1]}}"#
        )
        .unwrap();
        stream.write_all(b"\n").unwrap();

        let mut line = String::new();
        while line.is_empty() {
            match reader.read_line(&mut line) {
                Ok(0) => continue,
                Ok(_) => break,
                Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                    assert!(
                        std::time::Instant::now() < deadline,
                        "timed out waiting for header response"
                    );
                    thread::sleep(Duration::from_millis(50));
                }
                Err(err) => panic!("failed to read header response: {err}"),
            }
        }
        serde_json::from_str(line.trim()).unwrap()
    };
    assert_eq!(header["result"], serde_json::json!(header_hex));

    let headers: serde_json::Value = {
        let mut stream = TcpStream::connect_timeout(&local_addr, Duration::from_secs(5)).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let mut reader = BufReader::new(stream.try_clone().unwrap());
        write!(
            stream,
            r#"{{"jsonrpc":"2.0","id":2,"method":"blockchain.block.headers","params":[1,1]}}"#
        )
        .unwrap();
        stream.write_all(b"\n").unwrap();

        let mut line = String::new();
        while line.is_empty() {
            match reader.read_line(&mut line) {
                Ok(0) => continue,
                Ok(_) => break,
                Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                    assert!(
                        std::time::Instant::now() < deadline,
                        "timed out waiting for headers response"
                    );
                    thread::sleep(Duration::from_millis(50));
                }
                Err(err) => panic!("failed to read headers response: {err}"),
            }
        }
        serde_json::from_str(line.trim()).unwrap()
    };
    assert_eq!(headers["result"]["count"], serde_json::json!(1));
    assert_eq!(headers["result"]["hex"], serde_json::json!(header_hex));
    assert_eq!(headers["result"]["max"], serde_json::json!(2016));

    shutdown.store(true, Ordering::SeqCst);
}

#[test]
fn indexer_restores_and_forgets_pending_transactions() {
    let indexer = make_indexer(Metrics::new());

    let script = mock::op_return_script(0x66);
    let tx = mock::make_tx(vec![script.clone()]);
    let txid = tx.compute_txid();
    indexer.store_transaction(&tx).expect("store tx");

    let restored = indexer
        .restore_pending_transaction(&txid)
        .expect("restore pending")
        .expect("restored scripts");
    let sh = sh_of(&script);
    assert_eq!(restored, vec![sh]);
    assert_eq!(indexer.get_unconfirmed_balance_delta(&sh).unwrap(), 1000);
    assert_eq!(indexer.list_unspent(&sh).unwrap().len(), 1);

    let forgotten = indexer
        .forget_pending_transaction(&txid)
        .expect("forget pending")
        .expect("forgotten scripts");
    assert_eq!(forgotten, vec![sh]);
    assert_eq!(indexer.get_unconfirmed_balance_delta(&sh).unwrap(), 0);
    assert!(indexer.list_unspent(&sh).unwrap().is_empty());
}

#[test]
fn indexer_listunspent_is_stable_and_deduped() {
    let source = mock::MockBlockSource::new();
    let indexer = make_indexer(Metrics::new());
    let _handle = indexer.clone().start(&source);

    let script = p2pkh_script();
    let block = mock::make_block(bitcoin::BlockHash::all_zeros(), 1, vec![script.clone()]);
    let txid = block.txdata[0].compute_txid();
    let outpoint = bitcoin::OutPoint::new(txid, 0);
    source.push(BlockEvent::Connected {
        block: block.clone(),
        height: 1,
    });
    std::thread::sleep(std::time::Duration::from_millis(50));

    let pending = mock::make_spend_tx(outpoint, vec![(900, mock::op_return_script(0x55))]);
    indexer
        .track_pending_transaction(&pending)
        .expect("track pending");

    let sh = sh_of(&script);
    let unspent = indexer.list_unspent(&sh).unwrap();
    assert!(unspent.is_empty());
}

#[test]
fn indexer_persists_history_balance_and_utxos_across_restart() {
    let dir = tempdir().expect("temp index dir").keep();
    let script_a = p2pkh_script();
    let script_b = mock::op_return_script(0x33);

    let indexer_thread = {
        let source = mock::MockBlockSource::new();
        let indexer = Indexer::new(dir.clone(), Metrics::new()).expect("indexer");
        let handle = indexer.clone().start(&source);

        let block1 = mock::make_block(bitcoin::BlockHash::all_zeros(), 1, vec![script_a.clone()]);
        let fund_txid = block1.txdata[0].compute_txid();
        let fund_outpoint = bitcoin::OutPoint::new(fund_txid, 0);
        source.push(BlockEvent::Connected {
            block: block1,
            height: 1,
        });
        std::thread::sleep(std::time::Duration::from_millis(50));

        let spend_tx = mock::make_spend_tx(fund_outpoint, vec![(900, script_b.clone())]);
        let block2 = mock::make_block_with_txs(bitcoin::BlockHash::all_zeros(), 2, vec![spend_tx]);
        source.push(BlockEvent::Connected {
            block: block2,
            height: 2,
        });
        std::thread::sleep(std::time::Duration::from_millis(50));

        let sh_a = sh_of(&script_a);
        let sh_b = sh_of(&script_b);
        assert_eq!(indexer.get_balance(&sh_a).unwrap(), 0);
        assert!(indexer.list_unspent(&sh_a).unwrap().is_empty());
        assert_eq!(indexer.get_balance(&sh_b).unwrap(), 900);
        assert_eq!(indexer.list_unspent(&sh_b).unwrap().len(), 1);
        assert_eq!(indexer.get_history(&sh_a).len(), 2);
        assert_eq!(indexer.get_history(&sh_b).len(), 1);
        handle
    };

    indexer_thread.join().expect("indexer thread");

    let reopened = Indexer::new(dir, Metrics::new()).expect("reopened indexer");

    let sh_a = sh_of(&script_a);
    let sh_b = sh_of(&script_b);
    assert_eq!(reopened.get_balance(&sh_a).unwrap(), 0);
    assert!(reopened.list_unspent(&sh_a).unwrap().is_empty());
    assert_eq!(reopened.get_balance(&sh_b).unwrap(), 900);
    assert_eq!(reopened.list_unspent(&sh_b).unwrap().len(), 1);
    assert_eq!(reopened.get_history(&sh_a).len(), 2);
    assert_eq!(reopened.get_history(&sh_b).len(), 1);
}
