//! Script-hash indexer backed by an embedded persistent store.
//!
//! The in-memory cache is kept for fast reads, but all indexed history and raw
//! transactions are also mirrored to disk so the index survives restarts.
//!
//! ## Electrum script hash
//!
//! The Electrum protocol identifies addresses by the *script hash*: the
//! SHA-256 digest of the scriptPubKey bytes, with the bytes stored in
//! **reversed** (little-endian) order.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::thread;

use anyhow::Result;
use bitcoin::hashes::{Hash, sha256};
use bitcoin::{Block, Script, Transaction, Txid};
use tracing::{debug, info, warn};

use crate::block_source::{BlockEvent, BlockSource};
use crate::metrics::Metrics;
use crate::store::PersistentIndex;

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

struct IndexState {
    history: HashMap<ScriptHash, Vec<TxEntry>>,
    by_height: HashMap<u32, Vec<(ScriptHash, Txid, Vec<u8>)>>,
    tip_height: u32,
    store: PersistentIndex,
}

impl IndexState {
    fn new(index_dir: PathBuf) -> Result<Self> {
        let store = PersistentIndex::open(index_dir)?;
        let tip_height = store.tip_height();
        let mut state = Self {
            history: HashMap::new(),
            by_height: HashMap::new(),
            tip_height,
            store,
        };
        state.load_history_from_store()?;
        Ok(state)
    }

    fn load_history_from_store(&mut self) -> Result<()> {
        for entry in self.store.load_history_entries()? {
            self.history
                .entry(entry.script_hash)
                .or_default()
                .push(TxEntry {
                    txid: entry.txid,
                    height: entry.height,
                });
            self.by_height.entry(entry.height).or_default().push((
                entry.script_hash,
                entry.txid,
                entry.history_key,
            ));
        }
        Ok(())
    }

    fn apply_block(&mut self, block: &Block, height: u32) -> Result<()> {
        for tx in &block.txdata {
            let txid = tx.compute_txid();
            self.store.store_tx(tx)?;
            for (output_index, output) in tx.output.iter().enumerate() {
                let sh = ScriptHash::from_script(&output.script_pubkey);
                let entry = TxEntry { txid, height };
                self.history.entry(sh).or_default().push(entry);
                let history_key =
                    self.store
                        .store_history_entry(sh, height, txid, output_index as u32)?;
                self.by_height
                    .entry(height)
                    .or_default()
                    .push((sh, txid, history_key));
            }
        }
        self.tip_height = height;
        self.store.set_tip_height(height)?;
        Ok(())
    }

    fn rollback_height(&mut self, height: u32) -> Result<()> {
        if let Some(entries) = self.by_height.remove(&height) {
            for (sh, txid, history_key) in entries.into_iter().rev() {
                self.store.delete_history_key(&history_key)?;
                self.store.delete_tx(&txid)?;
                if let Some(script_entries) = self.history.get_mut(&sh) {
                    script_entries.retain(|e| !(e.txid == txid && e.height == height));
                    if script_entries.is_empty() {
                        self.history.remove(&sh);
                    }
                }
            }
        }
        if self.tip_height == height {
            self.tip_height = height.saturating_sub(1);
            self.store.set_tip_height(self.tip_height)?;
        }
        Ok(())
    }

    fn get_history(&self, sh: &ScriptHash) -> Vec<TxEntry> {
        let mut entries = self.history.get(sh).cloned().unwrap_or_default();
        entries.sort_by_key(|e| if e.height == 0 { u32::MAX } else { e.height });
        entries
    }

    fn has_history(&self, sh: &ScriptHash) -> bool {
        self.history.contains_key(sh)
    }

    fn tip_height(&self) -> u32 {
        self.tip_height
    }

    fn get_transaction(&self, txid: &Txid) -> Result<Option<Transaction>> {
        self.store.load_tx(txid)
    }
}

/// The block-chain indexer.
#[derive(Clone)]
pub struct Indexer {
    state: Arc<RwLock<IndexState>>,
    metrics: Metrics,
}

impl Indexer {
    /// Create a new indexer rooted at `index_dir`.
    pub fn new(index_dir: PathBuf, metrics: Metrics) -> Result<Self> {
        Ok(Self {
            state: Arc::new(RwLock::new(IndexState::new(index_dir)?)),
            metrics,
        })
    }

    /// Start the indexer event loop in a background thread, consuming events
    /// from `source`.
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
                            let result = {
                                let mut s = state.write().expect("index write lock poisoned");
                                s.apply_block(&block, height)
                            };
                            match result {
                                Ok(()) => {
                                    metrics.inc_blocks_indexed();
                                    info!("indexed block h={height} txs={}", block.txdata.len());
                                }
                                Err(e) => warn!("failed to persist indexed block h={height}: {e:#}"),
                            }
                        }
                        BlockEvent::Disconnected { hash, height } => {
                            warn!("indexer: rollback h={height} ({hash})");
                            let result = {
                                let mut s = state.write().expect("index write lock poisoned");
                                s.rollback_height(height)
                            };
                            match result {
                                Ok(()) => metrics.inc_blocks_rolled_back(),
                                Err(e) => warn!("failed to rollback block h={height}: {e:#}"),
                            }
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

    /// Return the transaction history for a script hash.
    pub fn get_history(&self, sh: &ScriptHash) -> Vec<TxEntry> {
        self.state.read().expect("index read lock poisoned").get_history(sh)
    }

    /// Return the current best-chain tip height known to the indexer.
    pub fn tip_height(&self) -> u32 {
        self.state.read().expect("index read lock poisoned").tip_height()
    }

    /// Returns `true` when the given script hash has any history.
    pub fn has_history(&self, sh: &ScriptHash) -> bool {
        self.state.read().expect("index read lock poisoned").has_history(sh)
    }

    /// Return a raw transaction by txid, if it has been indexed.
    pub fn get_transaction(&self, txid: &Txid) -> Result<Option<Transaction>> {
        self.state
            .read()
            .expect("index read lock poisoned")
            .get_transaction(txid)
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
        BlockHash, CompactTarget, absolute::LockTime, blockdata::{
            block::{Header as BlockHeader, Version},
            script::Builder,
            transaction::{Transaction, TxOut},
        }, hash_types::TxMerkleNode,
    };

    fn make_state() -> IndexState {
        let dir = tempfile::tempdir().expect("temp dir").into_path();
        IndexState::new(dir).expect("state")
    }

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
        Block { header, txdata: vec![tx] }
    }

    fn p2pkh_script() -> Vec<u8> {
        let mut s = vec![0x76u8, 0xa9, 0x14];
        s.extend_from_slice(&[0u8; 20]);
        s.extend_from_slice(&[0x88, 0xac]);
        s
    }

    #[test]
    fn apply_and_query_block() {
        let mut state = make_state();
        let script = p2pkh_script();
        let block = make_block(1, vec![script.clone()]);
        state.apply_block(&block, 1).expect("apply");

        let sh = ScriptHash::from_script(&Builder::from(script.clone()).into_script());
        assert!(state.history.contains_key(&sh));
        let entries = state.history[&sh].clone();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].height, 1);
    }

    #[test]
    fn rollback_removes_entries() {
        let mut state = make_state();
        let script = p2pkh_script();
        let block = make_block(1, vec![script.clone()]);
        state.apply_block(&block, 1).expect("apply");

        let sh = ScriptHash::from_script(&Builder::from(script).into_script());
        assert!(state.history.contains_key(&sh));

        state.rollback_height(1).expect("rollback");
        assert!(!state.history.contains_key(&sh));
    }

    #[test]
    fn rollback_only_removes_target_height() {
        let mut state = make_state();
        let s1 = p2pkh_script();
        let s2 = vec![0x6au8];
        let b1 = make_block(1, vec![s1.clone()]);
        let b2 = make_block(2, vec![s2.clone()]);
        state.apply_block(&b1, 1).expect("apply b1");
        state.apply_block(&b2, 2).expect("apply b2");

        state.rollback_height(1).expect("rollback");

        let sh2 = ScriptHash::from_script(&Builder::from(s2).into_script());
        assert!(state.history.contains_key(&sh2), "height-2 entry should survive");
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
        let mut state = make_state();
        state.apply_block(&make_block(5, vec![p2pkh_script()]), 5).expect("apply 5");
        assert_eq!(state.tip_height, 5);
        state.apply_block(&make_block(6, vec![p2pkh_script()]), 6).expect("apply 6");
        assert_eq!(state.tip_height, 6);
    }

    #[test]
    fn persisted_transaction_round_trips() {
        let mut state = make_state();
        let block = make_block(1, vec![p2pkh_script()]);
        let txid = block.txdata[0].compute_txid();
        state.apply_block(&block, 1).expect("apply");
        let tx = state.get_transaction(&txid).expect("query tx");
        assert!(tx.is_some());
    }
}
