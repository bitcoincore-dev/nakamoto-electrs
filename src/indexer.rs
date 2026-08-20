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
use bitcoin::{Block, OutPoint, Script, Transaction, Txid};
use tracing::{debug, info, warn};

use crate::block_source::{BlockEvent, BlockSource};
use crate::metrics::Metrics;
use crate::store::{JournalActionKind, PersistentIndex, StoredOutput, StoredUnspent};

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
    /// Stable ordering within a block for deterministic history/status hashes.
    pub sequence: u32,
}

struct IndexState {
    history: HashMap<ScriptHash, Vec<TxEntry>>,
    by_height: HashMap<u32, Vec<BlockAction>>,
    pending_txs: HashMap<Txid, Transaction>,
    pending_outputs: HashMap<OutPoint, StoredOutput>,
    tip_height: u32,
    store: PersistentIndex,
}

#[derive(Debug, Clone)]
enum BlockAction {
    Tx {
        txid: Txid,
        journal_key: Vec<u8>,
    },
    History {
        history_key: Vec<u8>,
        journal_key: Vec<u8>,
    },
    Output {
        outpoint: OutPoint,
        journal_key: Vec<u8>,
    },
    Spend {
        outpoint: OutPoint,
        journal_key: Vec<u8>,
    },
}

impl IndexState {
    fn new(index_dir: PathBuf) -> Result<Self> {
        let store = PersistentIndex::open(index_dir)?;
        let tip_height = store.tip_height();
        let mut state = Self {
            history: HashMap::new(),
            by_height: HashMap::new(),
            pending_txs: HashMap::new(),
            pending_outputs: HashMap::new(),
            tip_height,
            store,
        };
        state.load_history_from_store()?;
        state.load_journal_from_store()?;
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
                    sequence: entry.sequence,
                });
        }
        Ok(())
    }

    fn load_journal_from_store(&mut self) -> Result<()> {
        for action in self.store.load_journal_actions()? {
            let entry = match action.kind {
                JournalActionKind::Tx => BlockAction::Tx {
                    txid: parse_txid(&action.payload)?,
                    journal_key: action.journal_key,
                },
                JournalActionKind::History => BlockAction::History {
                    history_key: action.payload,
                    journal_key: action.journal_key,
                },
                JournalActionKind::Output => BlockAction::Output {
                    outpoint: parse_outpoint(&action.payload)?,
                    journal_key: action.journal_key,
                },
                JournalActionKind::Spend => BlockAction::Spend {
                    outpoint: parse_outpoint(&action.payload)?,
                    journal_key: action.journal_key,
                },
            };
            self.by_height.entry(action.height).or_default().push(entry);
        }
        for actions in self.by_height.values_mut() {
            actions.sort_by_key(action_sequence);
        }
        Ok(())
    }

    fn rebuild_pending_view(&mut self) {
        use std::collections::HashSet;

        self.pending_outputs.clear();

        let mut spent = HashSet::new();
        for tx in self.pending_txs.values() {
            for input in &tx.input {
                let prevout = input.previous_output;
                if !prevout.is_null() {
                    spent.insert(prevout);
                }
            }
        }

        for tx in self.pending_txs.values() {
            let txid = tx.compute_txid();
            for (vout, output) in tx.output.iter().enumerate() {
                let outpoint = OutPoint::new(txid, vout as u32);
                if spent.contains(&outpoint) {
                    continue;
                }
                self.pending_outputs.insert(
                    outpoint,
                    StoredOutput {
                        script_hash: ScriptHash::from_script(&output.script_pubkey),
                        txid,
                        vout: vout as u32,
                        value: output.value.to_sat(),
                        height: 0,
                    },
                );
            }
        }
    }

    fn track_pending_transaction_internal(&mut self, tx: &Transaction) -> Result<()> {
        let txid = tx.compute_txid();
        self.store.store_tx(tx)?;
        self.pending_txs.insert(txid, tx.clone());
        self.rebuild_pending_view();
        Ok(())
    }

    fn forget_pending_transaction(&mut self, txid: &Txid) {
        self.pending_txs.remove(txid);
        self.rebuild_pending_view();
    }

    fn pending_history_for_script(&self, sh: &ScriptHash) -> Vec<TxEntry> {
        let mut out = Vec::new();

        for tx in self.pending_txs.values() {
            if self.pending_tx_touches_script(tx, sh) {
                out.push(TxEntry {
                    txid: tx.compute_txid(),
                    height: 0,
                    sequence: 0,
                });
            }
        }

        out
    }

    fn pending_tx_touches_script(&self, tx: &Transaction, sh: &ScriptHash) -> bool {
        for input in &tx.input {
            let prevout = input.previous_output;
            if prevout.is_null() {
                continue;
            }
            if let Some(script_hash) = self.script_hash_for_outpoint(&prevout).ok().flatten() {
                if &script_hash == sh {
                    return true;
                }
            }
        }

        tx.output
            .iter()
            .any(|output| &ScriptHash::from_script(&output.script_pubkey) == sh)
    }

    fn script_hash_for_outpoint(&self, outpoint: &OutPoint) -> Result<Option<ScriptHash>> {
        if let Some(output) = self.pending_outputs.get(outpoint) {
            return Ok(Some(output.script_hash));
        }
        Ok(self.store.load_output(outpoint)?.map(|output| output.script_hash))
    }

    fn unconfirmed_balance_delta_for_script(&self, sh: &ScriptHash) -> Result<i64> {
        use std::collections::HashSet;

        let mut delta: i64 = 0;
        let mut seen_spends = HashSet::new();

        for tx in self.pending_txs.values() {
            for input in &tx.input {
                let prevout = input.previous_output;
                if prevout.is_null() || !seen_spends.insert(prevout) {
                    continue;
                }
                if let Some(script_hash) = self.script_hash_for_outpoint(&prevout)? {
                    if &script_hash == sh {
                        let value = self
                            .store
                            .load_output(&prevout)?
                            .or_else(|| self.pending_outputs.get(&prevout).copied())
                            .map(|o| o.value)
                            .unwrap_or(0);
                        delta -= value as i64;
                    }
                }
            }
        }

        for output in self.pending_outputs.values() {
            if &output.script_hash == sh {
                delta += output.value as i64;
            }
        }

        Ok(delta)
    }

    fn apply_block(&mut self, block: &Block, height: u32) -> Result<()> {
        let mut sequence = 0u32;
        for tx in &block.txdata {
            let txid = tx.compute_txid();
            self.forget_pending_transaction(&txid);
            self.store.store_tx(tx)?;
            let tx_key = self.store.store_journal_action(
                height,
                sequence,
                JournalActionKind::Tx,
                txid.as_byte_array(),
            )?;
            sequence += 1;
            self.by_height
                .entry(height)
                .or_default()
                .push(BlockAction::Tx {
                    txid,
                    journal_key: tx_key,
                });
            for (input_index, input) in tx.input.iter().enumerate() {
                let prevout = input.previous_output;
                if prevout.is_null() {
                    continue;
                }
                if let Some(stored_output) = self.store.load_output(&prevout)? {
                    let sh = stored_output.script_hash;
                    let entry = TxEntry {
                        txid,
                        height,
                        sequence: input_index as u32,
                    };
                    self.history.entry(sh).or_default().push(entry);
                    let history_key = self.store.store_history_entry(
                        sh,
                        height,
                        txid,
                        input_index as u32,
                        crate::store::HistoryKind::Spend,
                    )?;
                    let journal_key = self.store.store_journal_action(
                        height,
                        sequence,
                        JournalActionKind::History,
                        &history_key,
                    )?;
                    sequence += 1;
                    let spend_key = self.store.store_journal_action(
                        height,
                        sequence,
                        JournalActionKind::Spend,
                        &outpoint_bytes(&prevout),
                    )?;
                    sequence += 1;
                    self.store
                        .delete_utxo(sh, stored_output.txid, stored_output.vout)?;
                    self.by_height
                        .entry(height)
                        .or_default()
                        .push(BlockAction::History {
                            history_key,
                            journal_key,
                        });
                    self.by_height
                        .entry(height)
                        .or_default()
                        .push(BlockAction::Spend {
                            outpoint: prevout,
                            journal_key: spend_key,
                        });
                }
            }
            for (output_index, output) in tx.output.iter().enumerate() {
                let sh = ScriptHash::from_script(&output.script_pubkey);
                let entry = TxEntry {
                    txid,
                    height,
                    sequence: output_index as u32,
                };
                self.history.entry(sh).or_default().push(entry);
                let history_key = self.store.store_history_entry(
                    sh,
                    height,
                    txid,
                    output_index as u32,
                    crate::store::HistoryKind::Fund,
                )?;
                let journal_key = self.store.store_journal_action(
                    height,
                    sequence,
                    JournalActionKind::History,
                    &history_key,
                )?;
                sequence += 1;
                let outpoint = OutPoint::new(txid, output_index as u32);
                self.store
                    .store_output(outpoint, sh, output.value.to_sat(), height)?;
                let output_key = self.store.store_journal_action(
                    height,
                    sequence,
                    JournalActionKind::Output,
                    &outpoint_bytes(&outpoint),
                )?;
                sequence += 1;
                self.by_height
                    .entry(height)
                    .or_default()
                    .push(BlockAction::History {
                        history_key,
                        journal_key,
                    });
                self.by_height
                    .entry(height)
                    .or_default()
                    .push(BlockAction::Output {
                        outpoint,
                        journal_key: output_key,
                    });
            }
        }
        self.tip_height = height;
        self.store.set_tip_height(height)?;
        Ok(())
    }

    fn rollback_height(&mut self, height: u32) -> Result<()> {
        if let Some(entries) = self.by_height.remove(&height) {
            for action in entries.into_iter().rev() {
                match action {
                    BlockAction::Tx { txid, journal_key } => {
                        self.store.delete_journal_key(&journal_key)?;
                        self.store.delete_tx(&txid)?;
                    }
                    BlockAction::History {
                        history_key,
                        journal_key,
                    } => {
                        self.store.delete_journal_key(&journal_key)?;
                        self.store.delete_history_key(&history_key)?;
                    }
                    BlockAction::Output {
                        outpoint,
                        journal_key,
                    } => {
                        self.store.delete_journal_key(&journal_key)?;
                        if let Some(stored_output) = self.store.load_output(&outpoint)? {
                            self.store.delete_utxo(
                                stored_output.script_hash,
                                stored_output.txid,
                                stored_output.vout,
                            )?;
                        }
                        self.store.delete_output(&outpoint)?;
                    }
                    BlockAction::Spend {
                        outpoint,
                        journal_key,
                    } => {
                        self.store.delete_journal_key(&journal_key)?;
                        if let Some(stored_output) = self.store.load_output(&outpoint)? {
                            self.store.store_output(
                                outpoint,
                                stored_output.script_hash,
                                stored_output.value,
                                stored_output.height,
                            )?;
                        }
                    }
                }
            }
            for script_entries in self.history.values_mut() {
                script_entries.retain(|e| e.height != height);
            }
            self.history.retain(|_, entries| !entries.is_empty());
            self.rebuild_pending_view();
        }
        if self.tip_height == height {
            self.tip_height = height.saturating_sub(1);
            self.store.set_tip_height(self.tip_height)?;
        }
        Ok(())
    }

    fn get_history(&self, sh: &ScriptHash) -> Vec<TxEntry> {
        let mut entries = self.history.get(sh).cloned().unwrap_or_default();
        entries.extend(self.pending_history_for_script(sh));
        entries.sort_by_key(|e| {
            (
                if e.height == 0 { u32::MAX } else { e.height },
                e.sequence,
                e.txid.to_string(),
            )
        });
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

    fn store_transaction(&self, tx: &Transaction) -> Result<()> {
        self.store.store_tx(tx)
    }

    fn get_balance(&self, sh: &ScriptHash) -> Result<u64> {
        self.store.balance_for_script(sh)
    }

    fn get_unconfirmed_balance_delta(&self, sh: &ScriptHash) -> Result<i64> {
        self.unconfirmed_balance_delta_for_script(sh)
    }

    fn list_unspent(&self, sh: &ScriptHash) -> Result<Vec<StoredUnspent>> {
        self.store.list_unspent_for_script(sh)
    }
}

fn action_sequence(action: &BlockAction) -> u32 {
    match action {
        BlockAction::Tx { journal_key, .. }
        | BlockAction::History { journal_key, .. }
        | BlockAction::Output { journal_key, .. }
        | BlockAction::Spend { journal_key, .. } => {
            let mut seq = [0u8; 4];
            seq.copy_from_slice(&journal_key[4..8]);
            u32::from_be_bytes(seq)
        }
    }
}

fn parse_txid(payload: &[u8]) -> Result<Txid> {
    let bytes: [u8; 32] = payload
        .try_into()
        .map_err(|_| anyhow::anyhow!("invalid txid payload length: {}", payload.len()))?;
    Ok(Txid::from_byte_array(bytes))
}

fn parse_outpoint(payload: &[u8]) -> Result<OutPoint> {
    if payload.len() != 36 {
        anyhow::bail!("invalid outpoint payload length: {}", payload.len());
    }
    let mut txid = [0u8; 32];
    txid.copy_from_slice(&payload[0..32]);
    let mut vout = [0u8; 4];
    vout.copy_from_slice(&payload[32..36]);
    Ok(OutPoint::new(
        Txid::from_byte_array(txid),
        u32::from_be_bytes(vout),
    ))
}

fn outpoint_bytes(outpoint: &OutPoint) -> [u8; 36] {
    let mut bytes = [0u8; 36];
    bytes[0..32].copy_from_slice(outpoint.txid.as_byte_array());
    bytes[32..36].copy_from_slice(&outpoint.vout.to_be_bytes());
    bytes
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
                                Err(e) => {
                                    warn!("failed to persist indexed block h={height}: {e:#}")
                                }
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
        self.state
            .read()
            .expect("index read lock poisoned")
            .get_history(sh)
    }

    /// Return the current best-chain tip height known to the indexer.
    pub fn tip_height(&self) -> u32 {
        self.state
            .read()
            .expect("index read lock poisoned")
            .tip_height()
    }

    /// Returns `true` when the given script hash has any history.
    pub fn has_history(&self, sh: &ScriptHash) -> bool {
        self.state
            .read()
            .expect("index read lock poisoned")
            .has_history(sh)
    }

    /// Return a raw transaction by txid, if it has been indexed.
    pub fn get_transaction(&self, txid: &Txid) -> Result<Option<Transaction>> {
        self.state
            .read()
            .expect("index read lock poisoned")
            .get_transaction(txid)
    }

    pub fn store_transaction(&self, tx: &Transaction) -> Result<()> {
        self.state
            .read()
            .expect("index read lock poisoned")
            .store_transaction(tx)
    }

    pub fn track_pending_transaction(&self, tx: &Transaction) -> Result<()> {
        self.state
            .write()
            .expect("index write lock poisoned")
            .track_pending_transaction_internal(tx)
    }

    pub fn get_balance(&self, sh: &ScriptHash) -> Result<u64> {
        self.state
            .read()
            .expect("index read lock poisoned")
            .get_balance(sh)
    }

    pub fn get_unconfirmed_balance_delta(&self, sh: &ScriptHash) -> Result<i64> {
        self.state
            .read()
            .expect("index read lock poisoned")
            .get_unconfirmed_balance_delta(sh)
    }

    pub fn list_unspent(&self, sh: &ScriptHash) -> Result<Vec<StoredUnspent>> {
        self.state
            .read()
            .expect("index read lock poisoned")
            .list_unspent(sh)
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

    fn make_state() -> IndexState {
        let dir = tempfile::tempdir().expect("temp dir").keep();
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
        Block {
            header,
            txdata: vec![tx],
        }
    }

    fn make_spend_block(height: u32, prevout: bitcoin::OutPoint, script: Vec<u8>) -> Block {
        let tx = Transaction {
            version: bitcoin::transaction::Version::non_standard(1),
            lock_time: LockTime::ZERO,
            input: vec![bitcoin::blockdata::transaction::TxIn {
                previous_output: prevout,
                script_sig: bitcoin::ScriptBuf::new(),
                sequence: bitcoin::Sequence::MAX,
                witness: bitcoin::Witness::new(),
            }],
            output: vec![TxOut {
                value: bitcoin::Amount::from_sat(900),
                script_pubkey: Builder::from(script).into_script(),
            }],
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
        let mut state = make_state();
        state
            .apply_block(&make_block(5, vec![p2pkh_script()]), 5)
            .expect("apply 5");
        assert_eq!(state.tip_height, 5);
        state
            .apply_block(&make_block(6, vec![p2pkh_script()]), 6)
            .expect("apply 6");
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

    #[test]
    fn list_unspent_returns_funded_output() {
        let mut state = make_state();
        let script = p2pkh_script();
        let block = make_block(1, vec![script.clone()]);
        state.apply_block(&block, 1).expect("apply");

        let sh = ScriptHash::from_script(&Builder::from(script).into_script());
        let unspent = state.list_unspent(&sh).expect("unspent");
        assert_eq!(unspent.len(), 1);
        assert_eq!(unspent[0].value, 1000);
        assert_eq!(unspent[0].height, 1);
    }

    #[test]
    fn restart_preserves_rollback_state() {
        let dir = tempfile::tempdir().expect("temp dir").keep();
        let mut state = IndexState::new(dir.clone()).expect("state");

        let script = p2pkh_script();
        let fund_block = make_block(1, vec![script.clone()]);
        let fund_txid = fund_block.txdata[0].compute_txid();
        let prevout = bitcoin::OutPoint::new(fund_txid, 0);
        state.apply_block(&fund_block, 1).expect("apply fund");

        let sh = ScriptHash::from_script(&Builder::from(script.clone()).into_script());
        assert_eq!(state.get_balance(&sh).unwrap(), 1000);

        let spend_block = make_spend_block(2, prevout, vec![0x6au8]);
        state.apply_block(&spend_block, 2).expect("apply spend");
        assert_eq!(state.get_balance(&sh).unwrap(), 0);
        drop(state);

        let mut reopened = IndexState::new(dir).expect("reopen");
        assert_eq!(reopened.get_balance(&sh).unwrap(), 0);
        reopened.rollback_height(2).expect("rollback after restart");
        assert_eq!(reopened.get_balance(&sh).unwrap(), 1000);
        assert_eq!(reopened.get_history(&sh).len(), 1);
    }
}
