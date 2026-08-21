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

/// A mempool entry exposed to Electrum clients.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MempoolEntry {
    /// Transaction ID.
    pub txid: Txid,
    /// `0` if all inputs are confirmed, `-1` otherwise.
    pub height: i32,
    /// Estimated fee in sats.
    pub fee: u64,
}

/// In-memory index state, backed by a [`PersistentIndex`] store.
///
/// The `history` and `by_height` caches are rebuilt from the store on every
/// startup.  The `pending_*` maps track unconfirmed (mempool) transactions
/// that are not yet in any block.
struct IndexState {
    /// Confirmed transaction history, keyed by script hash.
    history: HashMap<ScriptHash, Vec<TxEntry>>,
    /// Per-block journal actions, keyed by block height.  Used to roll back
    /// a block in O(1) by replaying its actions in reverse.
    by_height: HashMap<u32, Vec<BlockAction>>,
    /// Unconfirmed transactions currently in the mempool view, keyed by txid.
    pending_txs: HashMap<Txid, Transaction>,
    /// Unconfirmed UTXOs that are outputs of `pending_txs` and have not yet
    /// been spent by another pending transaction.  Rebuilt from scratch
    /// whenever `pending_txs` changes via [`IndexState::rebuild_pending_view`].
    pending_outputs: HashMap<OutPoint, StoredOutput>,
    /// Best-chain tip height as last seen by `apply_block`.
    tip_height: u32,
    /// Persistent on-disk store.
    store: PersistentIndex,
}

/// A single undo record written into the per-block journal.
///
/// On block connect each record is appended in order; on disconnect the
/// records are replayed in reverse so every store mutation can be undone.
#[derive(Debug, Clone)]
enum BlockAction {
    /// A transaction was mined in this block.
    Tx {
        txid: Txid,
        journal_key: Vec<u8>,
    },
    /// A history entry (fund or spend) was added for a script hash.
    History {
        history_key: Vec<u8>,
        journal_key: Vec<u8>,
    },
    /// An output was stored as a UTXO.
    Output {
        outpoint: OutPoint,
        journal_key: Vec<u8>,
    },
    /// A UTXO was removed because it was spent.
    Spend {
        outpoint: OutPoint,
        journal_key: Vec<u8>,
    },
}

impl IndexState {
    /// Open (or create) the persistent index at `index_dir` and rebuild all
    /// in-memory caches from the stored data.
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
        let confirmed_txids = state.load_journal_from_store()?;
        state.load_pending_from_store(&confirmed_txids)?;
        state.rebuild_pending_view();
        Ok(state)
    }

    /// Populate `self.history` from the persistent history entries.
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

    /// Populate `self.by_height` from the persistent journal.
    ///
    /// Returns the set of txids that already have confirmed journal entries so
    /// that [`load_pending_from_store`] can skip them.
    fn load_journal_from_store(&mut self) -> Result<std::collections::HashSet<Txid>> {
        let mut confirmed_txids = std::collections::HashSet::new();
        for action in self.store.load_journal_actions()? {
            let entry = match action.kind {
                JournalActionKind::Tx => BlockAction::Tx {
                    txid: {
                        let txid = parse_txid(&action.payload)?;
                        confirmed_txids.insert(txid);
                        txid
                    },
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
        Ok(confirmed_txids)
    }

    /// Restore `self.pending_txs` from the persistent pending-txid list.
    ///
    /// Any txid that already appears in `confirmed_txids` is skipped — it was
    /// confirmed in the last indexed block and should not be treated as pending.
    fn load_pending_from_store(
        &mut self,
        confirmed_txids: &std::collections::HashSet<Txid>,
    ) -> Result<()> {
        for txid in self.store.load_pending_txids()? {
            if confirmed_txids.contains(&txid) {
                continue;
            }
            if let Some(tx) = self.store.load_tx(&txid)? {
                self.pending_txs.insert(txid, tx);
            }
        }
        Ok(())
    }

    /// Recompute `self.pending_outputs` from scratch.
    ///
    /// An output of a pending transaction is included in `pending_outputs` only
    /// when it is *not* already spent by another pending transaction.  This
    /// gives a live unspent-output view of the current mempool.
    ///
    /// This method must be called after any mutation of `self.pending_txs`.
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

    /// Add `tx` to the pending mempool view.
    ///
    /// Any existing pending transaction that spends the same inputs (i.e. an
    /// RBF conflict) and its entire descendant chain are evicted first.
    /// Returns the set of script hashes whose mempool status changed.
    fn track_pending_transaction_internal(&mut self, tx: &Transaction) -> Result<Vec<ScriptHash>> {
        use std::collections::HashSet;

        let txid = tx.compute_txid();
        let mut affected = HashSet::new();
        for conflict in self.pending_conflicting_txids(tx) {
            affected.extend(self.forget_pending_transaction_chain_internal(&conflict));
        }
        self.store.store_tx(tx)?;
        self.store.store_pending_txid(txid)?;
        self.pending_txs.insert(txid, tx.clone());
        self.rebuild_pending_view();
        affected.extend(self.pending_affected_scripts_for_tx(tx));
        Ok(affected.into_iter().collect())
    }

    /// Remove a single pending transaction without cascading to descendants.
    ///
    /// Called by [`apply_block`] when a transaction is confirmed so it is no
    /// longer treated as pending.
    fn forget_pending_transaction_internal(&mut self, txid: &Txid) {
        self.pending_txs.remove(txid);
        let _ = self.store.delete_pending_txid(txid);
        self.rebuild_pending_view();
    }

    /// Remove a pending transaction and every transaction that descends from it
    /// (i.e. directly or indirectly spends one of its outputs).
    ///
    /// Returns the union of all script hashes whose mempool status changed.
    /// Used when a transaction is replaced (RBF) or goes stale.
    fn forget_pending_transaction_chain_internal(&mut self, txid: &Txid) -> Vec<ScriptHash> {
        use std::collections::{HashSet, VecDeque};

        let mut affected = HashSet::new();
        let mut removed = HashSet::new();
        let mut queue = VecDeque::new();
        queue.push_back(*txid);

        while let Some(current_txid) = queue.pop_front() {
            if !removed.insert(current_txid) {
                continue;
            }

            if let Some(tx) = self.pending_txs.get(&current_txid).cloned() {
                affected.extend(self.pending_affected_scripts_for_tx(&tx));
            }

            let descendants: Vec<Txid> = self
                .pending_txs
                .iter()
                .filter(|(candidate_txid, _)| !removed.contains(*candidate_txid))
                .filter(|(_, candidate)| {
                    candidate.input.iter().any(|input| {
                        !input.previous_output.is_null()
                            && input.previous_output.txid == current_txid
                    })
                })
                .map(|(candidate_txid, _)| *candidate_txid)
                .collect();

            for descendant in descendants {
                queue.push_back(descendant);
            }
        }

        for txid in &removed {
            self.pending_txs.remove(txid);
            let _ = self.store.delete_pending_txid(txid);
        }
        self.rebuild_pending_view();
        affected.into_iter().collect()
    }

    /// Re-add a previously stored transaction to the pending mempool view.
    ///
    /// Returns `None` when the transaction is not found in the store (it was
    /// never broadcast through this node).  On success returns the set of
    /// affected script hashes, just like [`track_pending_transaction_internal`].
    fn restore_pending_transaction(&mut self, txid: &Txid) -> Result<Option<Vec<ScriptHash>>> {
        let Some(tx) = self.store.load_tx(txid)? else {
            return Ok(None);
        };
        Ok(Some(self.track_pending_transaction_internal(&tx)?))
    }

    /// Return unconfirmed history entries for `sh`.
    ///
    /// Each pending transaction that touches `sh` contributes one entry with
    /// `height = 0`.  This is used to construct the combined confirmed +
    /// unconfirmed history returned by `get_history`.
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

    /// Build the [`MempoolEntry`] list for `sh` as required by the Electrum
    /// `blockchain.scripthash.get_mempool` RPC.
    ///
    /// For each pending transaction that touches `sh` we compute:
    ///
    /// * **`height`** — `0` when every input spends a confirmed UTXO; `-1`
    ///   when at least one input spends another unconfirmed output (i.e. a
    ///   child-pays-for-parent chain).
    /// * **`fee`** — `input_value − output_value` in satoshis.  When an input
    ///   spends an output whose value cannot be looked up (unknown prevout) the
    ///   contribution of that input is treated as zero, so the fee may be
    ///   under-reported.
    ///
    /// The result is sorted by txid string for a stable, deterministic order.
    fn pending_mempool_for_script(&self, sh: &ScriptHash) -> Result<Vec<MempoolEntry>> {
        use std::collections::HashSet;

        let mut out = Vec::new();
        for tx in self.pending_txs.values() {
            if !self.pending_tx_touches_script(tx, sh) {
                continue;
            }
            let txid = tx.compute_txid();
            let mut input_value = 0u64;
            let mut has_unconfirmed_input = false;
            let mut seen_spends = HashSet::new();
            for input in &tx.input {
                let prevout = input.previous_output;
                if prevout.is_null() || !seen_spends.insert(prevout) {
                    continue;
                }
                if let Some(output) = self.store.load_output(&prevout)? {
                    input_value = input_value.saturating_add(output.value);
                } else if let Some(output) = self.pending_output_for_outpoint(&prevout) {
                    input_value = input_value.saturating_add(output.value);
                    has_unconfirmed_input = true;
                } else {
                    has_unconfirmed_input = true;
                }
            }
            let output_value = tx.output.iter().fold(0u64, |acc, output| {
                acc.saturating_add(output.value.to_sat())
            });
            out.push(MempoolEntry {
                txid,
                height: if has_unconfirmed_input { -1 } else { 0 },
                fee: input_value.saturating_sub(output_value),
            });
        }
        out.sort_by_key(|entry| entry.txid.to_string());
        Ok(out)
    }

    /// Return all script hashes that would have their mempool status changed if
    /// `tx` were added or removed.
    ///
    /// This walks the pending transaction graph transitively so that ancestor
    /// and descendant scripts are also included in subscription notifications.
    fn pending_affected_scripts_for_tx(&self, tx: &Transaction) -> Vec<ScriptHash> {
        use std::collections::{HashSet, VecDeque};

        let mut touched = HashSet::new();
        let mut seen_txs = HashSet::new();
        let mut queue = VecDeque::new();
        queue.push_back(tx.compute_txid());

        while let Some(txid) = queue.pop_front() {
            if !seen_txs.insert(txid) {
                continue;
            }

            let current = if txid == tx.compute_txid() {
                tx
            } else if let Some(current) = self.pending_txs.get(&txid) {
                current
            } else {
                continue;
            };

            for input in &current.input {
                let prevout = input.previous_output;
                if prevout.is_null() {
                    continue;
                }
                if let Some(script_hash) = self.script_hash_for_outpoint(&prevout).ok().flatten() {
                    touched.insert(script_hash);
                }
            }
            for output in &current.output {
                touched.insert(ScriptHash::from_script(&output.script_pubkey));
            }

            for candidate in self.pending_txs.values() {
                let candidate_txid = candidate.compute_txid();
                if seen_txs.contains(&candidate_txid) {
                    continue;
                }
                if candidate.input.iter().any(|input| {
                    !input.previous_output.is_null() && input.previous_output.txid == txid
                }) {
                    queue.push_back(candidate_txid);
                }
            }
        }

        touched.into_iter().collect()
    }

    /// Return the txids of pending transactions that conflict with `tx`.
    ///
    /// A conflict is any pending transaction that spends at least one of the
    /// same outpoints as `tx` (i.e. a double-spend / RBF candidate), excluding
    /// `tx` itself as an ancestor of its own inputs.
    fn pending_conflicting_txids(&self, tx: &Transaction) -> Vec<Txid> {
        use std::collections::HashSet;

        let new_prevouts: HashSet<OutPoint> = tx
            .input
            .iter()
            .map(|input| input.previous_output)
            .filter(|prevout| !prevout.is_null())
            .collect();
        let new_prevout_txids: HashSet<Txid> =
            new_prevouts.iter().map(|prevout| prevout.txid).collect();

        self.pending_txs
            .iter()
            .filter(|(candidate_txid, _)| !new_prevout_txids.contains(*candidate_txid))
            .filter(|(_, candidate)| {
                candidate
                    .input
                    .iter()
                    .any(|input| new_prevouts.contains(&input.previous_output))
            })
            .map(|(candidate_txid, _)| *candidate_txid)
            .collect()
    }

    /// Return `true` when `tx` has an input that spends from `sh` or an output
    /// that pays to `sh`.
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

    /// Resolve `outpoint` to its script hash, checking `pending_outputs` first
    /// and then falling back to the persistent store.
    fn script_hash_for_outpoint(&self, outpoint: &OutPoint) -> Result<Option<ScriptHash>> {
        if let Some(output) = self.pending_output_for_outpoint(outpoint) {
            return Ok(Some(output.script_hash));
        }
        Ok(self
            .store
            .load_output(outpoint)?
            .map(|output| output.script_hash))
    }

    /// Look up `outpoint` in the pending mempool view.
    ///
    /// Checks the pre-built `pending_outputs` index first (which only contains
    /// *unspent* outputs of pending transactions) and falls back to scanning
    /// `pending_txs` directly for outputs that are spent within the same
    /// mempool chain.
    fn pending_output_for_outpoint(&self, outpoint: &OutPoint) -> Option<StoredOutput> {
        self.pending_outputs.get(outpoint).copied().or_else(|| {
            self.pending_txs
                .get(&outpoint.txid)
                .and_then(|tx| tx.output.get(outpoint.vout as usize))
                .map(|output| StoredOutput {
                    script_hash: ScriptHash::from_script(&output.script_pubkey),
                    txid: outpoint.txid,
                    vout: outpoint.vout,
                    value: output.value.to_sat(),
                    height: 0,
                })
        })
    }

    /// Compute the net balance change for `sh` due to unconfirmed transactions.
    ///
    /// Positive values mean pending inflows exceed pending outflows; negative
    /// values mean more confirmed UTXOs are being spent than are being received.
    /// This is the value returned by `blockchain.scripthash.get_balance` in the
    /// `unconfirmed` field.
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
                            .or_else(|| self.pending_output_for_outpoint(&prevout))
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

    /// Apply a connected block, updating history, the UTXO set, and the
    /// persistent store.  Any pending transactions that are confirmed by this
    /// block are removed from the mempool view.
    fn apply_block(&mut self, block: &Block, height: u32) -> Result<()> {
        let mut sequence = 0u32;
        for tx in &block.txdata {
            let txid = tx.compute_txid();
            self.forget_pending_transaction_internal(&txid);
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

    /// Undo every store action that was applied at `height`, restoring the
    /// UTXO set and history to the state just before that block was connected.
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

    /// Return the combined confirmed + unconfirmed history for `sh`, sorted by
    /// height (unconfirmed entries sort last), then sequence, then txid.
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

    /// Return `true` when `sh` has any confirmed history.
    fn has_history(&self, sh: &ScriptHash) -> bool {
        self.history.contains_key(sh)
    }

    fn tip_height(&self) -> u32 {
        self.tip_height
    }

    /// Load a raw transaction from the persistent store.
    fn get_transaction(&self, txid: &Txid) -> Result<Option<Transaction>> {
        self.store.load_tx(txid)
    }

    /// Persist a raw transaction to the store without adding it to the pending
    /// view.  Used to cache transactions that may need to be restored later.
    fn store_transaction(&self, tx: &Transaction) -> Result<()> {
        self.store.store_tx(tx)
    }

    /// Return the confirmed balance (sum of confirmed UTXOs) for `sh`.
    fn get_balance(&self, sh: &ScriptHash) -> Result<u64> {
        self.store.balance_for_script(sh)
    }

    /// Return the net unconfirmed balance delta for `sh`.
    fn get_unconfirmed_balance_delta(&self, sh: &ScriptHash) -> Result<i64> {
        self.unconfirmed_balance_delta_for_script(sh)
    }

    /// Return confirmed UTXOs for `sh` from the persistent store.
    fn list_unspent(&self, sh: &ScriptHash) -> Result<Vec<StoredUnspent>> {
        self.store.list_unspent_for_script(sh)
    }

    /// Return pending mempool entries for `sh`.
    fn mempool(&self, sh: &ScriptHash) -> Result<Vec<MempoolEntry>> {
        self.pending_mempool_for_script(sh)
    }

    /// Return unspent outputs from `pending_txs` that pay to `sh`.
    fn pending_unspent_for_script(&self, sh: &ScriptHash) -> Vec<StoredUnspent> {
        self.pending_outputs
            .values()
            .filter(|output| &output.script_hash == sh)
            .map(|output| StoredUnspent {
                txid: output.txid,
                vout: output.vout,
                value: output.value,
                height: output.height,
            })
            .collect()
    }

    /// Return all outpoints that are spent by pending transactions.
    ///
    /// Used by [`Indexer::list_unspent`] to filter confirmed UTXOs that have
    /// already been consumed in the mempool.
    fn pending_spent_outpoints(&self) -> std::collections::HashSet<OutPoint> {
        let mut spent = std::collections::HashSet::new();
        for tx in self.pending_txs.values() {
            for input in &tx.input {
                let prevout = input.previous_output;
                if !prevout.is_null() {
                    spent.insert(prevout);
                }
            }
        }
        spent
    }
}

/// Extract the 4-byte sequence number embedded in a `journal_key`.
///
/// Journal keys are prefixed with a height (4 bytes) followed by a sequence
/// number (4 bytes).  The sequence number is used to restore the original
/// insertion order when replaying or rolling back a block.
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

/// Deserialise a 32-byte txid from a raw journal payload.
fn parse_txid(payload: &[u8]) -> Result<Txid> {
    let bytes: [u8; 32] = payload
        .try_into()
        .map_err(|_| anyhow::anyhow!("invalid txid payload length: {}", payload.len()))?;
    Ok(Txid::from_byte_array(bytes))
}

/// Deserialise a 36-byte outpoint (32-byte txid + 4-byte big-endian vout)
/// from a raw journal payload.
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

/// Serialise an outpoint to the 36-byte format used in journal payloads.
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

    /// Persist a raw transaction to the store without adding it to the pending
    /// mempool view.
    ///
    /// This allows a transaction to be stored so it can be restored later via
    /// [`Indexer::restore_pending_transaction`] if, for example, the nakamoto peer
    /// reports that it was reverted.
    pub fn store_transaction(&self, tx: &Transaction) -> Result<()> {
        self.state
            .read()
            .expect("index read lock poisoned")
            .store_transaction(tx)
    }

    /// Add `tx` to the pending mempool view, evicting any conflicting
    /// transactions and their descendants first (RBF semantics).
    ///
    /// The transaction is also persisted to the store so it survives restarts.
    /// Returns the set of script hashes whose mempool status changed; these
    /// should be used to send subscription notifications to connected clients.
    pub fn track_pending_transaction(&self, tx: &Transaction) -> Result<Vec<ScriptHash>> {
        self.state
            .write()
            .expect("index write lock poisoned")
            .track_pending_transaction_internal(tx)
    }

    /// Re-add a previously stored transaction to the active pending mempool
    /// view.
    ///
    /// Returns `None` when the transaction cannot be found in the persistent
    /// store.  On success returns the affected script hashes.
    pub fn restore_pending_transaction(&self, txid: &Txid) -> Result<Option<Vec<ScriptHash>>> {
        self.state
            .write()
            .expect("index write lock poisoned")
            .restore_pending_transaction(txid)
    }

    /// Remove a single pending transaction from the mempool view without
    /// cascading to its descendants.
    ///
    /// Returns `None` when the txid is not currently pending.  Used when a
    /// transaction is confirmed in a block (descendants may still be pending).
    pub fn forget_pending_transaction(&self, txid: &Txid) -> Result<Option<Vec<ScriptHash>>> {
        let mut state = self.state.write().expect("index write lock poisoned");
        let Some(tx) = state.pending_txs.remove(txid) else {
            return Ok(None);
        };
        let affected = state.pending_affected_scripts_for_tx(&tx);
        let _ = state.store.delete_pending_txid(txid);
        state.rebuild_pending_view();
        Ok(Some(affected))
    }

    /// Remove a pending transaction and all of its descendants from the mempool
    /// view.
    ///
    /// Returns `None` when the root txid is not currently pending.  Used when a
    /// transaction is replaced (RBF) or goes stale so the entire dependent
    /// chain is invalid.
    pub fn forget_pending_transaction_chain(&self, txid: &Txid) -> Result<Option<Vec<ScriptHash>>> {
        let mut state = self.state.write().expect("index write lock poisoned");
        if !state.pending_txs.contains_key(txid) {
            return Ok(None);
        }
        let affected = state.forget_pending_transaction_chain_internal(txid);
        Ok(Some(affected))
    }

    /// Return the confirmed balance for `sh` (sum of confirmed UTXOs).
    pub fn get_balance(&self, sh: &ScriptHash) -> Result<u64> {
        self.state
            .read()
            .expect("index read lock poisoned")
            .get_balance(sh)
    }

    /// Return the net unconfirmed balance delta for `sh`.
    ///
    /// A positive value means the script hash is receiving more in the mempool
    /// than it is spending; a negative value means confirmed UTXOs are being
    /// spent by pending transactions that pay to other scripts.
    pub fn get_unconfirmed_balance_delta(&self, sh: &ScriptHash) -> Result<i64> {
        self.state
            .read()
            .expect("index read lock poisoned")
            .get_unconfirmed_balance_delta(sh)
    }

    /// Return the combined confirmed and unconfirmed UTXOs for `sh`.
    ///
    /// Confirmed UTXOs that are already spent by pending transactions are
    /// filtered out, and unspent pending outputs are appended.
    pub fn list_unspent(&self, sh: &ScriptHash) -> Result<Vec<StoredUnspent>> {
        let state = self.state.read().expect("index read lock poisoned");
        let spent = state.pending_spent_outpoints();
        let mut out = state
            .list_unspent(sh)?
            .into_iter()
            .filter(|entry| !spent.contains(&OutPoint::new(entry.txid, entry.vout)))
            .collect::<Vec<_>>();
        out.extend(state.pending_unspent_for_script(sh));
        out.sort_by_key(|e| (e.height, e.txid.to_string(), e.vout));
        out.dedup_by_key(|e| (e.txid, e.vout));
        Ok(out)
    }

    /// Return the pending mempool entries for `sh`.
    ///
    /// Each entry describes an unconfirmed transaction that pays to or spends
    /// from `sh`, including its estimated fee and whether any of its inputs are
    /// also unconfirmed.  This is the data returned by the Electrum
    /// `blockchain.scripthash.get_mempool` RPC.
    pub fn get_mempool(&self, sh: &ScriptHash) -> Result<Vec<MempoolEntry>> {
        self.state
            .read()
            .expect("index read lock poisoned")
            .mempool(sh)
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
    fn list_unspent_hides_confirmed_outputs_spent_in_pending_tx() {
        let mut state = make_state();
        let script = p2pkh_script();
        let block = make_block(1, vec![script.clone()]);
        let fund_txid = block.txdata[0].compute_txid();
        let prevout = bitcoin::OutPoint::new(fund_txid, 0);
        state.apply_block(&block, 1).expect("apply");

        let spend = make_spend_block(2, prevout, vec![0x6au8])
            .txdata
            .into_iter()
            .next()
            .expect("spend tx");
        state
            .track_pending_transaction_internal(&spend)
            .expect("track pending");

        let sh = ScriptHash::from_script(&Builder::from(script).into_script());
        let indexer = Indexer::new(tempfile::tempdir().expect("temp").keep(), Metrics::new())
            .expect("indexer");
        {
            let mut live = indexer.state.write().expect("index write lock poisoned");
            *live = state;
        }
        assert!(indexer.list_unspent(&sh).expect("list").is_empty());
    }

    #[test]
    fn get_mempool_marks_unconfirmed_ancestors() {
        let mut state = make_state();
        let script_a = p2pkh_script();
        let script_b = vec![0x51];
        let fund_block = make_block(1, vec![script_a.clone()]);
        let fund_txid = fund_block.txdata[0].compute_txid();
        let prevout = bitcoin::OutPoint::new(fund_txid, 0);
        state.apply_block(&fund_block, 1).expect("apply fund");

        let pending = make_spend_block(2, prevout, script_b.clone())
            .txdata
            .into_iter()
            .next()
            .expect("pending tx");
        state
            .track_pending_transaction_internal(&pending)
            .expect("track pending");

        let sh_a = ScriptHash::from_script(&Builder::from(script_a).into_script());
        let sh_b = ScriptHash::from_script(&Builder::from(script_b).into_script());

        let mempool_a = state.mempool(&sh_a).expect("mempool a");
        assert_eq!(mempool_a.len(), 1);
        assert_eq!(mempool_a[0].height, 0);
        assert_eq!(mempool_a[0].fee, 100);

        let mempool_b = state.mempool(&sh_b).expect("mempool b");
        assert_eq!(mempool_b.len(), 1);
        assert_eq!(mempool_b[0].height, 0);
        assert_eq!(mempool_b[0].fee, 100);
    }

    #[test]
    fn get_mempool_marks_pending_input_as_unconfirmed() {
        let mut state = make_state();
        let script_a = p2pkh_script();
        let script_b = vec![0x51];
        let first_pending = Transaction {
            version: bitcoin::transaction::Version::non_standard(1),
            lock_time: LockTime::ZERO,
            input: vec![bitcoin::blockdata::transaction::TxIn {
                previous_output: bitcoin::OutPoint::null(),
                script_sig: bitcoin::ScriptBuf::new(),
                sequence: bitcoin::Sequence::MAX,
                witness: bitcoin::Witness::new(),
            }],
            output: vec![TxOut {
                value: bitcoin::Amount::from_sat(1000),
                script_pubkey: Builder::from(script_a.clone()).into_script(),
            }],
        };
        state
            .track_pending_transaction_internal(&first_pending)
            .expect("track first");
        let spend = make_spend_block(
            2,
            bitcoin::OutPoint::new(first_pending.compute_txid(), 0),
            script_b.clone(),
        )
        .txdata
        .into_iter()
        .next()
        .expect("spend tx");
        state
            .track_pending_transaction_internal(&spend)
            .expect("track spend");

        let sh_b = ScriptHash::from_script(&Builder::from(script_b).into_script());
        let mempool_b = state.mempool(&sh_b).expect("mempool b");
        assert_eq!(mempool_b.len(), 1);
        assert_eq!(mempool_b[0].height, -1);
        assert_eq!(mempool_b[0].fee, 100);
    }

    #[test]
    fn track_pending_transaction_replaces_conflicting_pending_chain() {
        let mut state = make_state();
        let fund_script = p2pkh_script();
        let block = make_block(1, vec![fund_script.clone()]);
        let fund_txid = block.txdata[0].compute_txid();
        let fund_outpoint = OutPoint::new(fund_txid, 0);
        state.apply_block(&block, 1).expect("apply fund");

        let first_script = vec![0x51];
        let first = make_spend_block(2, fund_outpoint, first_script.clone())
            .txdata
            .into_iter()
            .next()
            .expect("first tx");
        state
            .track_pending_transaction_internal(&first)
            .expect("track first");
        let child_script = vec![0x52];
        let child = make_spend_block(
            3,
            OutPoint::new(first.compute_txid(), 0),
            child_script.clone(),
        )
        .txdata
        .into_iter()
        .next()
        .expect("child tx");
        state
            .track_pending_transaction_internal(&child)
            .expect("track child");

        let replacement_script = vec![0x53];
        let replacement = make_spend_block(4, fund_outpoint, replacement_script.clone())
            .txdata
            .into_iter()
            .next()
            .expect("replacement tx");
        state
            .track_pending_transaction_internal(&replacement)
            .expect("track replacement");

        let sh_first = ScriptHash::from_script(&Builder::from(first_script).into_script());
        let sh_child = ScriptHash::from_script(&Builder::from(child_script).into_script());
        let sh_replacement =
            ScriptHash::from_script(&Builder::from(replacement_script).into_script());

        assert!(state.mempool(&sh_first).expect("mempool first").is_empty());
        assert!(state.mempool(&sh_child).expect("mempool child").is_empty());
        assert_eq!(
            state.mempool(&sh_replacement).expect("mempool repl").len(),
            1
        );
    }

    #[test]
    fn track_pending_transaction_replaces_deep_conflicting_chain() {
        let mut state = make_state();
        let fund_script = p2pkh_script();
        let block = make_block(1, vec![fund_script.clone()]);
        let fund_txid = block.txdata[0].compute_txid();
        let fund_outpoint = OutPoint::new(fund_txid, 0);
        state.apply_block(&block, 1).expect("apply fund");

        let first = make_spend_block(2, fund_outpoint, vec![0x51])
            .txdata
            .into_iter()
            .next()
            .expect("first tx");
        state
            .track_pending_transaction_internal(&first)
            .expect("track first");
        let second = make_spend_block(3, OutPoint::new(first.compute_txid(), 0), vec![0x52])
            .txdata
            .into_iter()
            .next()
            .expect("second tx");
        state
            .track_pending_transaction_internal(&second)
            .expect("track second");
        let third = make_spend_block(4, OutPoint::new(second.compute_txid(), 0), vec![0x53])
            .txdata
            .into_iter()
            .next()
            .expect("third tx");
        state
            .track_pending_transaction_internal(&third)
            .expect("track third");

        let replacement = make_spend_block(5, fund_outpoint, vec![0x54])
            .txdata
            .into_iter()
            .next()
            .expect("replacement tx");
        state
            .track_pending_transaction_internal(&replacement)
            .expect("track replacement");

        for script in [0x51u8, 0x52, 0x53] {
            let sh = ScriptHash::from_script(&Builder::from(vec![script]).into_script());
            assert!(state.mempool(&sh).expect("mempool").is_empty());
        }
        let sh_replacement = ScriptHash::from_script(&Builder::from(vec![0x54u8]).into_script());
        assert_eq!(state.mempool(&sh_replacement).expect("mempool repl").len(), 1);
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

    #[test]
    fn restart_restores_pending_transactions() {
        let dir = tempfile::tempdir().expect("temp dir").keep();
        let reopen_dir = dir.clone();
        let mut state = IndexState::new(dir.clone()).expect("state");

        let script = p2pkh_script();
        let tx = Transaction {
            version: bitcoin::transaction::Version::non_standard(1),
            lock_time: LockTime::ZERO,
            input: vec![bitcoin::blockdata::transaction::TxIn {
                previous_output: bitcoin::OutPoint::null(),
                script_sig: bitcoin::ScriptBuf::new(),
                sequence: bitcoin::Sequence::MAX,
                witness: bitcoin::Witness::new(),
            }],
            output: vec![TxOut {
                value: bitcoin::Amount::from_sat(900),
                script_pubkey: Builder::from(script.clone()).into_script(),
            }],
        };
        let sh = ScriptHash::from_script(&Builder::from(script).into_script());
        state
            .track_pending_transaction_internal(&tx)
            .expect("track pending");
        assert_eq!(state.mempool(&sh).expect("mempool").len(), 1);
        assert_eq!(
            state.store.load_pending_txids().expect("pending ids").len(),
            1
        );
        drop(state);

        let reopened = IndexState::new(dir).expect("reopen");
        assert_eq!(
            reopened
                .store
                .load_pending_txids()
                .expect("pending ids")
                .len(),
            1
        );
        assert_eq!(reopened.pending_txs.len(), 1);
        assert_eq!(reopened.pending_outputs.len(), 1);
        let mempool = reopened.mempool(&sh).expect("mempool after restart");
        assert_eq!(mempool.len(), 1);
        assert_eq!(mempool[0].height, 0);
        assert_eq!(mempool[0].fee, 0);
        assert_eq!(reopened.pending_unspent_for_script(&sh).len(), 1);
        drop(reopened);
        let reopened_indexer = Indexer::new(reopen_dir, Metrics::new()).expect("reopen indexer");
        let unspent = reopened_indexer
            .list_unspent(&sh)
            .expect("unspent after restart");
        assert_eq!(unspent.len(), 1);
        assert_eq!(unspent[0].height, 0);
        assert_eq!(unspent[0].value, 900);
    }

    #[test]
    fn restore_and_forget_pending_transaction_round_trip() {
        let indexer = Indexer::new(tempfile::tempdir().expect("temp").keep(), Metrics::new())
            .expect("indexer");
        let script = Builder::from(p2pkh_script()).into_script();
        let tx = Transaction {
            version: bitcoin::transaction::Version::non_standard(1),
            lock_time: LockTime::ZERO,
            input: vec![bitcoin::blockdata::transaction::TxIn {
                previous_output: bitcoin::OutPoint::null(),
                script_sig: bitcoin::ScriptBuf::new(),
                sequence: bitcoin::Sequence::MAX,
                witness: bitcoin::Witness::new(),
            }],
            output: vec![TxOut {
                value: bitcoin::Amount::from_sat(750),
                script_pubkey: script.clone(),
            }],
        };
        let txid = tx.compute_txid();
        indexer.store_transaction(&tx).expect("store tx");

        let restored = indexer
            .restore_pending_transaction(&txid)
            .expect("restore pending")
            .expect("restored tx");
        let sh = ScriptHash::from_script(&script);
        assert_eq!(restored, vec![sh]);
        assert_eq!(indexer.get_balance(&sh).unwrap(), 0);
        assert_eq!(indexer.get_unconfirmed_balance_delta(&sh).unwrap(), 750);
        assert_eq!(indexer.list_unspent(&sh).unwrap().len(), 1);

        let forgotten = indexer
            .forget_pending_transaction(&txid)
            .expect("forget pending")
            .expect("forgotten tx");
        assert_eq!(forgotten, vec![sh]);
        assert_eq!(indexer.get_unconfirmed_balance_delta(&sh).unwrap(), 0);
        assert!(indexer.list_unspent(&sh).unwrap().is_empty());
    }

    #[test]
    fn restore_and_forget_missing_pending_transaction_are_none() {
        let indexer = Indexer::new(tempfile::tempdir().expect("temp").keep(), Metrics::new())
            .expect("indexer");
        let txid = bitcoin::Txid::all_zeros();
        assert!(
            indexer
                .restore_pending_transaction(&txid)
                .unwrap()
                .is_none()
        );
        assert!(indexer.forget_pending_transaction(&txid).unwrap().is_none());
    }
}
