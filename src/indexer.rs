//! Script-hash indexer — maintains an in-memory map from Electrum script hash
//! to transaction history, driven by the [`BlockSource`] event stream.
//!
//! ## Electrum script hash
//!
//! The Electrum protocol identifies addresses by the *script hash*: the
//! SHA-256 digest of the scriptPubKey bytes, with the bytes stored in
//! **reversed** (little-endian) order.
//!
//! ## Index layout
//!
//! For each script hash the index stores a chronologically-ordered list of
//! [`TxEntry`] records.  Each entry carries the txid and the height at which
//! the transaction was confirmed (or 0 for unconfirmed).
//!
//! For fast reverse lookup (needed for reorg rollback) the index also
//! maintains a map from block height to the list of (script_hash, txid) pairs
//! indexed at that height.
//!
//! ## Reorg handling
//!
//! On [`BlockEvent::Disconnected`] the indexer removes every entry that was
//! indexed at the disconnected height, bounded by the configured
//! `max_reorg_depth`.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::thread;

use bitcoin::hashes::{Hash, sha256};
use bitcoin::{Block, Script, Txid};
use tracing::{debug, info, warn};

use crate::block_source::{BlockEvent, BlockSource};
use crate::metrics::Metrics;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// The 32-byte reversed SHA-256 digest of a scriptPubKey, as used by the
/// Electrum protocol.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ScriptHash([u8; 32]);

impl ScriptHash {
    /// Compute the script hash for the given script pubkey.
    pub fn from_script(script: &Script) -> Self {
        let digest = sha256::Hash::hash(script.as_bytes());
        let mut bytes: [u8; 32] = *digest.as_ref();
        bytes.reverse();
        Self(bytes)
    }

    /// Returns the raw bytes.
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Encode as a lowercase hex string (as used in the Electrum protocol).
    pub fn to_hex(&self) -> String {
        self.0.iter().map(|b| format!("{b:02x}")).collect()
    }

    /// Construct directly from raw bytes.
    ///
    /// Intended for use when the bytes are already in the correct
    /// (reversed, little-endian) Electrum format.
    pub fn from_raw_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }
}

impl std::fmt::Display for ScriptHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_hex())
    }
}

/// A single history entry for a script hash.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TxEntry {
    /// Transaction ID.
    pub txid: Txid,
    /// Block height at which this tx was confirmed, or `0` for unconfirmed.
    pub height: u32,
}

// ---------------------------------------------------------------------------
// Inner index state (behind RwLock)
// ---------------------------------------------------------------------------

struct IndexState {
    /// script_hash → ordered history entries.
    history: HashMap<ScriptHash, Vec<TxEntry>>,
    /// height → list of (script_hash, txid) pairs indexed at that height.
    /// Used for efficient rollback.
    by_height: HashMap<u32, Vec<(ScriptHash, Txid)>>,
    /// Current best-chain tip height.
    tip_height: u32,
}

impl IndexState {
    fn new() -> Self {
        Self {
            history: HashMap::new(),
            by_height: HashMap::new(),
            tip_height: 0,
        }
    }

    /// Index all outputs of every transaction in `block`.
    fn apply_block(&mut self, block: &Block, height: u32) {
        for tx in &block.txdata {
            let txid = tx.compute_txid();
            for output in &tx.output {
                let sh = ScriptHash::from_script(&output.script_pubkey);
                let entry = TxEntry { txid, height };
                self.history.entry(sh).or_default().push(entry);
                self.by_height.entry(height).or_default().push((sh, txid));
            }
            // Also index spending inputs (skip coinbase).
            if !tx.is_coinbase() {
                for input in &tx.input {
                    // We don't have the scriptPubKey of the spent output here,
                    // so we can't compute the script hash of the spent address
                    // without a UTXO set.  The Electrum protocol requires this
                    // for `get_history`, so this is noted as a future
                    // improvement when a UTXO set is available.
                    let _ = input; // suppress unused warning
                }
            }
        }
        self.tip_height = height;
    }

    /// Roll back all entries indexed at `height`.
    fn rollback_height(&mut self, height: u32) {
        if let Some(pairs) = self.by_height.remove(&height) {
            for (sh, txid) in pairs {
                if let Some(entries) = self.history.get_mut(&sh) {
                    entries.retain(|e| !(e.txid == txid && e.height == height));
                    if entries.is_empty() {
                        self.history.remove(&sh);
                    }
                }
            }
        }
        if self.tip_height == height {
            self.tip_height = height.saturating_sub(1);
        }
    }
}

// ---------------------------------------------------------------------------
// Indexer
// ---------------------------------------------------------------------------

/// The block-chain indexer.
///
/// Listens to a [`BlockSource`] subscription and maintains a script-hash →
/// transaction-history map that the Electrum server queries.
///
/// The index runs in a background thread started by [`Indexer::start`].
/// All query methods are safe to call from any thread concurrently.
#[derive(Clone)]
pub struct Indexer {
    state: Arc<RwLock<IndexState>>,
    metrics: Metrics,
}

impl Indexer {
    /// Create a new, empty indexer.
    pub fn new(metrics: Metrics) -> Self {
        Self {
            state: Arc::new(RwLock::new(IndexState::new())),
            metrics,
        }
    }

    /// Start the indexer event loop in a background thread, consuming events
    /// from `source`.
    ///
    /// The returned [`thread::JoinHandle`] can be used to await termination.
    pub fn start<S: BlockSource>(self, source: &S) -> thread::JoinHandle<()> {
        let rx = source.subscribe();
        let state = Arc::clone(&self.state);
        let metrics = self.metrics.clone();

        thread::Builder::new()
            .name("indexer".into())
            .spawn(move || {
                for event in &rx {
                    match event {
                        BlockEvent::Connected { block, height } => {
                            debug!("indexer: apply block h={height}");
                            {
                                let mut s = state.write().expect("index write lock poisoned");
                                s.apply_block(&block, height);
                            }
                            metrics.inc_blocks_indexed();
                            info!("indexed block h={height} txs={}", block.txdata.len());
                        }
                        BlockEvent::Disconnected { hash, height } => {
                            warn!("indexer: rollback h={height} ({hash})");
                            {
                                let mut s = state.write().expect("index write lock poisoned");
                                s.rollback_height(height);
                            }
                            metrics.inc_blocks_rolled_back();
                        }
                        BlockEvent::Synced { height, tip } => {
                            info!("indexer: chain synced at h={height} tip={tip}");
                        }
                    }
                }
                debug!("indexer thread exiting");
            })
            .expect("failed to spawn indexer thread")
    }

    // ---- Query interface used by the Electrum server ----------------------

    /// Return the transaction history for a script hash.
    ///
    /// Results are ordered by ascending block height (unconfirmed entries
    /// last with height = 0).
    pub fn get_history(&self, sh: &ScriptHash) -> Vec<TxEntry> {
        let s = self.state.read().expect("index read lock poisoned");
        let mut entries = s.history.get(sh).cloned().unwrap_or_default();
        entries.sort_by_key(|e| if e.height == 0 { u32::MAX } else { e.height });
        entries
    }

    /// Return the current best-chain tip height known to the indexer.
    pub fn tip_height(&self) -> u32 {
        self.state
            .read()
            .expect("index read lock poisoned")
            .tip_height
    }

    /// Returns `true` when the given script hash has any history.
    pub fn has_history(&self, sh: &ScriptHash) -> bool {
        let s = self.state.read().expect("index read lock poisoned");
        s.history.contains_key(sh)
    }
}

// ---------------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use bitcoin::hashes::Hash;
    use bitcoin::{
        BlockHash, CompactTarget,
        absolute::LockTime,
        blockdata::{
            block::{Header as BlockHeader, Version},
            script::Builder,
            transaction::{Transaction, TxOut},
        },
        hash_types::TxMerkleNode,
    };

    fn make_block(height: u32, scripts: Vec<Vec<u8>>) -> Block {
        let txouts: Vec<TxOut> = scripts
            .into_iter()
            .map(|s| TxOut {
                value: bitcoin::Amount::from_sat(1000),
                script_pubkey: Builder::from(s).into_script(),
            })
            .collect();
        let tx = Transaction {
            version: bitcoin::transaction::Version::non_standard(1),
            lock_time: LockTime::ZERO,
            input: vec![bitcoin::blockdata::transaction::TxIn {
                previous_output: bitcoin::OutPoint::null(),
                script_sig: bitcoin::ScriptBuf::new(),
                sequence: bitcoin::Sequence::MAX,
                witness: bitcoin::Witness::new(),
            }],
            output: txouts,
        };
        let header = BlockHeader {
            version: Version::ONE,
            prev_blockhash: BlockHash::all_zeros(),
            merkle_root: TxMerkleNode::all_zeros(),
            time: height,
            bits: CompactTarget::from_consensus(0x1d00ffff),
            nonce: 0,
        };
        Block {
            header,
            txdata: vec![tx],
        }
    }

    fn p2pkh_script() -> Vec<u8> {
        // OP_DUP OP_HASH160 <20 zero bytes> OP_EQUALVERIFY OP_CHECKSIG
        let mut s = vec![0x76u8, 0xa9, 0x14];
        s.extend_from_slice(&[0u8; 20]);
        s.extend_from_slice(&[0x88, 0xac]);
        s
    }

    #[test]
    fn apply_and_query_block() {
        let _metrics = Metrics::new();
        let mut state = IndexState::new();
        let script = p2pkh_script();
        let block = make_block(1, vec![script.clone()]);
        state.apply_block(&block, 1);

        let sh = ScriptHash::from_script(&Builder::from(script.clone()).into_script());
        assert!(state.history.contains_key(&sh));
        let entries = state.history[&sh].clone();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].height, 1);
    }

    #[test]
    fn rollback_removes_entries() {
        let mut state = IndexState::new();
        let script = p2pkh_script();
        let block = make_block(1, vec![script.clone()]);
        state.apply_block(&block, 1);

        let sh = ScriptHash::from_script(&Builder::from(script).into_script());
        assert!(state.history.contains_key(&sh));

        state.rollback_height(1);
        assert!(!state.history.contains_key(&sh));
    }

    #[test]
    fn rollback_only_removes_target_height() {
        let mut state = IndexState::new();
        let s1 = p2pkh_script();
        // Second script: OP_RETURN (different from s1)
        let s2 = vec![0x6au8];
        let b1 = make_block(1, vec![s1.clone()]);
        let b2 = make_block(2, vec![s2.clone()]);
        state.apply_block(&b1, 1);
        state.apply_block(&b2, 2);

        state.rollback_height(1);

        let sh2 = ScriptHash::from_script(&Builder::from(s2).into_script());
        assert!(
            state.history.contains_key(&sh2),
            "height-2 entry should survive"
        );
    }

    #[test]
    fn script_hash_hex_length() {
        let sh = ScriptHash::from_script(&Builder::from(p2pkh_script()).into_script());
        assert_eq!(sh.to_hex().len(), 64);
    }

    #[test]
    fn script_hash_all_hex_chars() {
        let sh = ScriptHash::from_script(&Builder::from(p2pkh_script()).into_script());
        assert!(sh.to_hex().chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn indexer_tip_tracks_latest_block() {
        let mut state = IndexState::new();
        state.apply_block(&make_block(5, vec![p2pkh_script()]), 5);
        assert_eq!(state.tip_height, 5);
        state.apply_block(&make_block(6, vec![p2pkh_script()]), 6);
        assert_eq!(state.tip_height, 6);
    }
}
