use std::path::Path;
use std::thread;
use std::time::Duration;

use anyhow::{Context, Result};
use bitcoin::consensus::encode::{deserialize, serialize};
use bitcoin::hashes::Hash;
use bitcoin::{OutPoint, Transaction, Txid};
use sled::Tree;

use crate::indexer::{ScriptHash, TxEntry};

const PENDING_TXIDS_KEY: &[u8] = b"pending_txids";

#[derive(Clone)]
pub struct PersistentIndex {
    history: Tree,
    outputs: Tree,
    txs: Tree,
    utxos: Tree,
    journal: Tree,
    meta: Tree,
}

#[derive(Debug, Clone, Copy)]
pub enum HistoryKind {
    Fund = 0,
    Spend = 1,
}

#[derive(Debug, Clone)]
pub struct StoredHistoryEntry {
    pub script_hash: ScriptHash,
    pub txid: Txid,
    pub height: u32,
    pub sequence: u32,
    pub kind: u8,
    pub history_key: Vec<u8>,
}

#[derive(Debug, Clone, Copy)]
pub struct StoredOutput {
    pub script_hash: ScriptHash,
    pub txid: Txid,
    pub vout: u32,
    pub value: u64,
    pub height: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredUnspent {
    pub txid: Txid,
    pub vout: u32,
    pub value: u64,
    pub height: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JournalActionKind {
    Tx = 0,
    History = 1,
    Output = 2,
    Spend = 3,
}

#[derive(Debug, Clone)]
pub struct StoredJournalAction {
    pub journal_key: Vec<u8>,
    pub height: u32,
    pub sequence: u32,
    pub kind: JournalActionKind,
    pub payload: Vec<u8>,
}

impl PersistentIndex {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let mut last_err = None;
        for attempt in 0..10 {
            match sled::open(path.as_ref()) {
                Ok(db) => {
                    let history = db.open_tree("history")?;
                    let outputs = db.open_tree("outputs")?;
                    let txs = db.open_tree("txs")?;
                    let utxos = db.open_tree("utxos")?;
                    let journal = db.open_tree("journal")?;
                    let meta = db.open_tree("meta")?;
                    return Ok(Self {
                        history,
                        outputs,
                        txs,
                        utxos,
                        journal,
                        meta,
                    });
                }
                Err(e) => {
                    last_err = Some(e);
                    if attempt < 9 {
                        thread::sleep(Duration::from_millis(25));
                    }
                }
            }
        }
        let err = last_err.expect("sled open failed without error");
        Err(err).context("failed to open persistent index db")
    }

    pub fn set_tip_height(&self, height: u32) -> Result<()> {
        self.meta
            .insert(b"tip_height", height.to_be_bytes().to_vec())?;
        Ok(())
    }

    pub fn tip_height(&self) -> u32 {
        self.meta
            .get(b"tip_height")
            .ok()
            .flatten()
            .and_then(|v| <[u8; 4]>::try_from(v.as_ref()).ok())
            .map(u32::from_be_bytes)
            .unwrap_or(0)
    }

    pub fn store_tx(&self, tx: &Transaction) -> Result<()> {
        let txid = tx.compute_txid();
        self.txs.insert(txid.as_byte_array(), serialize(tx))?;
        Ok(())
    }

    pub fn load_tx(&self, txid: &Txid) -> Result<Option<Transaction>> {
        let Some(raw) = self.txs.get(txid.as_byte_array())? else {
            return Ok(None);
        };
        let tx = deserialize(raw.as_ref()).context("failed to decode stored tx")?;
        Ok(Some(tx))
    }

    pub fn delete_tx(&self, txid: &Txid) -> Result<()> {
        self.txs.remove(txid.as_byte_array())?;
        Ok(())
    }

    pub fn store_pending_txid(&self, txid: Txid) -> Result<()> {
        let mut txids = self.load_pending_txids()?;
        if !txids.iter().any(|existing| existing == &txid) {
            txids.push(txid);
        }
        self.meta
            .insert(PENDING_TXIDS_KEY, encode_txid_list(&txids))?;
        Ok(())
    }

    pub fn delete_pending_txid(&self, txid: &Txid) -> Result<()> {
        let mut txids = self.load_pending_txids()?;
        txids.retain(|existing| existing != txid);
        self.meta
            .insert(PENDING_TXIDS_KEY, encode_txid_list(&txids))?;
        Ok(())
    }

    pub fn load_pending_txids(&self) -> Result<Vec<Txid>> {
        let Some(raw) = self.meta.get(PENDING_TXIDS_KEY)? else {
            return Ok(Vec::new());
        };
        decode_txid_list(raw.as_ref())
    }

    pub fn store_output(
        &self,
        outpoint: OutPoint,
        script_hash: ScriptHash,
        value: u64,
        height: u32,
    ) -> Result<()> {
        let output = StoredOutput {
            script_hash,
            txid: outpoint.txid,
            vout: outpoint.vout,
            value,
            height,
        };
        self.outputs
            .insert(outpoint_key(&outpoint), encode_output(&output))?;
        self.utxos.insert(
            utxo_key(script_hash, outpoint.txid, outpoint.vout),
            &value.to_be_bytes(),
        )?;
        Ok(())
    }

    pub fn load_output(&self, outpoint: &OutPoint) -> Result<Option<StoredOutput>> {
        let Some(raw) = self.outputs.get(outpoint_key(outpoint))? else {
            return Ok(None);
        };
        Ok(Some(decode_output(outpoint, raw.as_ref())?))
    }

    pub fn delete_output(&self, outpoint: &OutPoint) -> Result<()> {
        self.outputs.remove(outpoint_key(outpoint))?;
        Ok(())
    }

    pub fn delete_utxo(&self, script_hash: ScriptHash, txid: Txid, vout: u32) -> Result<()> {
        self.utxos.remove(utxo_key(script_hash, txid, vout))?;
        Ok(())
    }

    pub fn store_journal_action(
        &self,
        height: u32,
        sequence: u32,
        kind: JournalActionKind,
        payload: &[u8],
    ) -> Result<Vec<u8>> {
        let key = journal_key(height, sequence);
        let mut value = Vec::with_capacity(1 + payload.len());
        value.push(kind as u8);
        value.extend_from_slice(payload);
        self.journal.insert(&key, value)?;
        Ok(key)
    }

    pub fn delete_journal_key(&self, key: &[u8]) -> Result<()> {
        self.journal.remove(key)?;
        Ok(())
    }

    pub fn load_journal_actions(&self) -> Result<Vec<StoredJournalAction>> {
        let mut out = Vec::new();
        for item in self.journal.iter() {
            let (key, value) = item?;
            out.push(parse_journal_action(&key, &value)?);
        }
        Ok(out)
    }

    pub fn balance_for_script(&self, script_hash: &ScriptHash) -> Result<u64> {
        let mut total = 0u64;
        for item in self.utxos.scan_prefix(script_hash.as_bytes()) {
            let (_, value) = item?;
            let amount = <[u8; 8]>::try_from(value.as_ref()).context("invalid utxo value")?;
            total = total.saturating_add(u64::from_be_bytes(amount));
        }
        Ok(total)
    }

    pub fn list_unspent_for_script(&self, script_hash: &ScriptHash) -> Result<Vec<StoredUnspent>> {
        let mut out = Vec::new();
        for item in self.utxos.scan_prefix(script_hash.as_bytes()) {
            let (key, value) = item?;
            let mut txid = [0u8; 32];
            txid.copy_from_slice(&key[32..64]);
            let mut vout = [0u8; 4];
            vout.copy_from_slice(&key[64..68]);
            let amount = <[u8; 8]>::try_from(value.as_ref()).context("invalid utxo value")?;
            let outpoint = OutPoint::new(Txid::from_byte_array(txid), u32::from_be_bytes(vout));
            let stored = self
                .load_output(&outpoint)?
                .ok_or_else(|| anyhow::anyhow!("missing output for unspent {}", outpoint))?;
            out.push(StoredUnspent {
                txid: outpoint.txid,
                vout: outpoint.vout,
                value: u64::from_be_bytes(amount),
                height: stored.height,
            });
        }
        Ok(out)
    }

    pub fn store_history_entry(
        &self,
        script_hash: ScriptHash,
        height: u32,
        txid: Txid,
        sequence: u32,
        kind: HistoryKind,
    ) -> Result<Vec<u8>> {
        let key = history_key(script_hash, height, kind, txid, sequence);
        self.history.insert(&key, &[])?;
        Ok(key)
    }

    pub fn delete_history_key(&self, key: &[u8]) -> Result<()> {
        self.history.remove(key)?;
        Ok(())
    }

    pub fn load_history_entries(&self) -> Result<Vec<StoredHistoryEntry>> {
        let mut out = Vec::new();
        for item in self.history.iter() {
            let (key, _) = item?;
            out.push(parse_history_key(&key)?);
        }
        Ok(out)
    }

    pub fn load_history_for_script(&self, script_hash: &ScriptHash) -> Result<Vec<TxEntry>> {
        let mut out = Vec::new();
        for item in self.history.scan_prefix(script_hash.as_bytes()) {
            let (key, _) = item?;
            let entry = parse_history_key(&key)?;
            out.push(TxEntry {
                txid: entry.txid,
                height: entry.height,
                sequence: entry.sequence,
            });
        }
        out.sort_by_key(|e| (if e.height == 0 { u32::MAX } else { e.height }, e.sequence));
        Ok(out)
    }

    pub fn has_history(&self, script_hash: &ScriptHash) -> bool {
        self.history
            .scan_prefix(script_hash.as_bytes())
            .next()
            .is_some()
    }
}

fn outpoint_key(outpoint: &OutPoint) -> Vec<u8> {
    let mut key = Vec::with_capacity(36);
    key.extend_from_slice(outpoint.txid.as_byte_array());
    key.extend_from_slice(&outpoint.vout.to_be_bytes());
    key
}

fn utxo_key(script_hash: ScriptHash, txid: Txid, vout: u32) -> Vec<u8> {
    let mut key = Vec::with_capacity(68);
    key.extend_from_slice(script_hash.as_bytes());
    key.extend_from_slice(txid.as_byte_array());
    key.extend_from_slice(&vout.to_be_bytes());
    key
}

fn journal_key(height: u32, sequence: u32) -> Vec<u8> {
    let mut key = Vec::with_capacity(8);
    key.extend_from_slice(&height.to_be_bytes());
    key.extend_from_slice(&sequence.to_be_bytes());
    key
}

fn encode_output(output: &StoredOutput) -> Vec<u8> {
    let mut value = Vec::with_capacity(52);
    value.extend_from_slice(output.script_hash.as_bytes());
    value.extend_from_slice(&output.value.to_be_bytes());
    value.extend_from_slice(&output.height.to_be_bytes());
    value
}

fn decode_output(outpoint: &OutPoint, raw: &[u8]) -> Result<StoredOutput> {
    if raw.len() != 44 {
        anyhow::bail!("invalid output encoding length: {}", raw.len());
    }
    let mut script_hash = [0u8; 32];
    script_hash.copy_from_slice(&raw[0..32]);

    let mut value = [0u8; 8];
    value.copy_from_slice(&raw[32..40]);

    let mut height = [0u8; 4];
    height.copy_from_slice(&raw[40..44]);

    Ok(StoredOutput {
        script_hash: ScriptHash::from_raw_bytes(script_hash),
        txid: outpoint.txid,
        vout: outpoint.vout,
        value: u64::from_be_bytes(value),
        height: u32::from_be_bytes(height),
    })
}

fn history_key(
    script_hash: ScriptHash,
    height: u32,
    kind: HistoryKind,
    txid: Txid,
    sequence: u32,
) -> Vec<u8> {
    let mut key = Vec::with_capacity(73);
    key.extend_from_slice(script_hash.as_bytes());
    key.extend_from_slice(&height.to_be_bytes());
    key.push(kind as u8);
    key.extend_from_slice(txid.as_byte_array());
    key.extend_from_slice(&sequence.to_be_bytes());
    key
}

fn parse_history_key(key: &[u8]) -> Result<StoredHistoryEntry> {
    if key.len() != 73 {
        anyhow::bail!("invalid history key length: {}", key.len());
    }

    let mut sh = [0u8; 32];
    sh.copy_from_slice(&key[0..32]);

    let mut height = [0u8; 4];
    height.copy_from_slice(&key[32..36]);

    let kind = key[36];

    let mut txid = [0u8; 32];
    txid.copy_from_slice(&key[37..69]);

    let mut sequence = [0u8; 4];
    sequence.copy_from_slice(&key[69..73]);

    Ok(StoredHistoryEntry {
        script_hash: ScriptHash::from_raw_bytes(sh),
        txid: Txid::from_byte_array(txid),
        height: u32::from_be_bytes(height),
        sequence: u32::from_be_bytes(sequence),
        kind,
        history_key: key.to_vec(),
    })
}

fn parse_journal_action(key: &[u8], value: &[u8]) -> Result<StoredJournalAction> {
    if key.len() != 8 {
        anyhow::bail!("invalid journal key length: {}", key.len());
    }
    if value.is_empty() {
        anyhow::bail!("invalid journal value length: 0");
    }

    let mut height = [0u8; 4];
    height.copy_from_slice(&key[0..4]);

    let mut sequence = [0u8; 4];
    sequence.copy_from_slice(&key[4..8]);

    let kind = match value[0] {
        0 => JournalActionKind::Tx,
        1 => JournalActionKind::History,
        2 => JournalActionKind::Output,
        3 => JournalActionKind::Spend,
        other => anyhow::bail!("invalid journal action kind: {other}"),
    };

    Ok(StoredJournalAction {
        journal_key: key.to_vec(),
        height: u32::from_be_bytes(height),
        sequence: u32::from_be_bytes(sequence),
        kind,
        payload: value[1..].to_vec(),
    })
}

fn encode_txid_list(txids: &[Txid]) -> Vec<u8> {
    let mut out = Vec::with_capacity(txids.len() * 32);
    for txid in txids {
        out.extend_from_slice(txid.as_byte_array());
    }
    out
}

fn decode_txid_list(raw: &[u8]) -> Result<Vec<Txid>> {
    if raw.len() % 32 != 0 {
        anyhow::bail!("invalid pending txid list length: {}", raw.len());
    }
    let mut out = Vec::with_capacity(raw.len() / 32);
    for chunk in raw.chunks_exact(32) {
        let bytes: [u8; 32] = chunk
            .try_into()
            .map_err(|_| anyhow::anyhow!("invalid pending txid chunk"))?;
        out.push(Txid::from_byte_array(bytes));
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use bitcoin::{
        absolute::LockTime,
        blockdata::transaction::{TxIn, TxOut, Version},
    };

    fn make_store() -> PersistentIndex {
        let dir = tempfile::tempdir().expect("temp dir").keep();
        PersistentIndex::open(dir).expect("open store")
    }

    fn make_txid(b: u8) -> Txid {
        Txid::from_byte_array([b; 32])
    }

    fn make_script_hash(b: u8) -> ScriptHash {
        ScriptHash::from_raw_bytes([b; 32])
    }

    fn make_tx() -> Transaction {
        Transaction {
            version: Version::non_standard(1),
            lock_time: LockTime::ZERO,
            input: vec![TxIn {
                previous_output: OutPoint::null(),
                script_sig: bitcoin::ScriptBuf::new(),
                sequence: bitcoin::Sequence::MAX,
                witness: bitcoin::Witness::new(),
            }],
            output: vec![TxOut {
                value: bitcoin::Amount::from_sat(1000),
                script_pubkey: bitcoin::ScriptBuf::new(),
            }],
        }
    }

    // ---- tip height -------------------------------------------------------

    #[test]
    fn tip_height_is_zero_when_unset() {
        assert_eq!(make_store().tip_height(), 0);
    }

    #[test]
    fn tip_height_round_trips() {
        let store = make_store();
        store.set_tip_height(42).expect("set");
        assert_eq!(store.tip_height(), 42);
    }

    #[test]
    fn tip_height_can_be_overwritten() {
        let store = make_store();
        store.set_tip_height(1).expect("set 1");
        store.set_tip_height(100).expect("set 100");
        assert_eq!(store.tip_height(), 100);
    }

    // ---- store_tx / load_tx / delete_tx -----------------------------------

    #[test]
    fn store_and_load_tx_round_trip() {
        let store = make_store();
        let tx = make_tx();
        let txid = tx.compute_txid();
        store.store_tx(&tx).expect("store");
        let loaded = store.load_tx(&txid).expect("load").expect("found");
        assert_eq!(loaded.compute_txid(), txid);
    }

    #[test]
    fn load_tx_returns_none_for_unknown_txid() {
        let store = make_store();
        assert!(store.load_tx(&make_txid(0xff)).expect("load").is_none());
    }

    #[test]
    fn delete_tx_removes_entry() {
        let store = make_store();
        let tx = make_tx();
        let txid = tx.compute_txid();
        store.store_tx(&tx).expect("store");
        store.delete_tx(&txid).expect("delete");
        assert!(store.load_tx(&txid).expect("load after delete").is_none());
    }

    // ---- pending txids ----------------------------------------------------

    #[test]
    fn load_pending_txids_empty_by_default() {
        assert!(make_store().load_pending_txids().expect("load").is_empty());
    }

    #[test]
    fn store_and_load_pending_txid_round_trip() {
        let store = make_store();
        let txid = make_txid(0x01);
        store.store_pending_txid(txid).expect("store");
        let ids = store.load_pending_txids().expect("load");
        assert_eq!(ids, vec![txid]);
    }

    #[test]
    fn store_pending_txid_deduplicates() {
        let store = make_store();
        let txid = make_txid(0x01);
        store.store_pending_txid(txid).expect("store 1");
        store.store_pending_txid(txid).expect("store 2");
        assert_eq!(store.load_pending_txids().expect("load").len(), 1);
    }

    #[test]
    fn delete_pending_txid_removes_entry() {
        let store = make_store();
        let txid = make_txid(0x01);
        store.store_pending_txid(txid).expect("store");
        store.delete_pending_txid(&txid).expect("delete");
        assert!(store.load_pending_txids().expect("load").is_empty());
    }

    #[test]
    fn pending_txid_list_preserves_order() {
        let store = make_store();
        let ids: Vec<Txid> = (1u8..=3).map(make_txid).collect();
        for &id in &ids {
            store.store_pending_txid(id).expect("store");
        }
        assert_eq!(store.load_pending_txids().expect("load"), ids);
    }

    // ---- decode_txid_list error -------------------------------------------

    #[test]
    fn decode_txid_list_rejects_misaligned_data() {
        assert!(decode_txid_list(&[0u8; 31]).is_err());
        assert!(decode_txid_list(&[0u8; 33]).is_err());
    }

    #[test]
    fn decode_txid_list_accepts_empty() {
        assert_eq!(decode_txid_list(&[]).expect("empty"), vec![]);
    }

    // ---- store_output / load_output / delete_output / delete_utxo ---------

    #[test]
    fn store_and_load_output_round_trip() {
        let store = make_store();
        let txid = make_txid(0x02);
        let outpoint = OutPoint::new(txid, 0);
        let sh = make_script_hash(0x10);
        store.store_output(outpoint, sh, 5000, 1).expect("store");
        let loaded = store.load_output(&outpoint).expect("load").expect("found");
        assert_eq!(loaded.value, 5000);
        assert_eq!(loaded.height, 1);
        assert_eq!(loaded.script_hash, sh);
    }

    #[test]
    fn load_output_returns_none_for_unknown_outpoint() {
        let store = make_store();
        let op = OutPoint::new(make_txid(0xaa), 0);
        assert!(store.load_output(&op).expect("load").is_none());
    }

    #[test]
    fn delete_output_removes_entry() {
        let store = make_store();
        let txid = make_txid(0x03);
        let op = OutPoint::new(txid, 0);
        let sh = make_script_hash(0x11);
        store.store_output(op, sh, 1000, 1).expect("store");
        store.delete_output(&op).expect("delete");
        assert!(store.load_output(&op).expect("load after delete").is_none());
    }

    #[test]
    fn delete_utxo_removes_entry_from_balance() {
        let store = make_store();
        let txid = make_txid(0x04);
        let op = OutPoint::new(txid, 0);
        let sh = make_script_hash(0x12);
        store.store_output(op, sh, 2000, 1).expect("store");
        assert_eq!(store.balance_for_script(&sh).expect("balance"), 2000);
        store.delete_utxo(sh, txid, 0).expect("delete utxo");
        assert_eq!(store.balance_for_script(&sh).expect("balance"), 0);
    }

    // ---- balance_for_script -----------------------------------------------

    #[test]
    fn balance_for_script_is_zero_when_empty() {
        let store = make_store();
        assert_eq!(
            store
                .balance_for_script(&make_script_hash(0xaa))
                .expect("balance"),
            0
        );
    }

    #[test]
    fn balance_for_script_sums_multiple_outputs() {
        let store = make_store();
        let sh = make_script_hash(0x20);
        for (i, v) in [1000u64, 2000, 3000].iter().enumerate() {
            let op = OutPoint::new(make_txid(i as u8 + 1), 0);
            store.store_output(op, sh, *v, 1).expect("store");
        }
        assert_eq!(store.balance_for_script(&sh).expect("balance"), 6000);
    }

    // ---- list_unspent_for_script ------------------------------------------

    #[test]
    fn list_unspent_for_script_returns_stored_outputs() {
        let store = make_store();
        let sh = make_script_hash(0x30);
        let txid = make_txid(0x05);
        let op = OutPoint::new(txid, 0);
        store.store_output(op, sh, 9000, 2).expect("store");
        let unspent = store.list_unspent_for_script(&sh).expect("list");
        assert_eq!(unspent.len(), 1);
        assert_eq!(unspent[0].txid, txid);
        assert_eq!(unspent[0].vout, 0);
        assert_eq!(unspent[0].value, 9000);
        assert_eq!(unspent[0].height, 2);
    }

    #[test]
    fn list_unspent_for_script_empty_when_none_stored() {
        let store = make_store();
        assert!(
            store
                .list_unspent_for_script(&make_script_hash(0xbb))
                .expect("list")
                .is_empty()
        );
    }

    // ---- decode_output error ----------------------------------------------

    #[test]
    fn decode_output_rejects_wrong_length() {
        let op = OutPoint::null();
        assert!(decode_output(&op, &[0u8; 43]).is_err());
        assert!(decode_output(&op, &[0u8; 45]).is_err());
        assert!(decode_output(&op, &[]).is_err());
    }

    #[test]
    fn decode_output_accepts_exactly_44_bytes() {
        let op = OutPoint::null();
        assert!(decode_output(&op, &[0u8; 44]).is_ok());
    }

    // ---- history entries --------------------------------------------------

    #[test]
    fn has_history_returns_false_when_empty() {
        let store = make_store();
        assert!(!store.has_history(&make_script_hash(0xcc)));
    }

    #[test]
    fn store_and_has_history_round_trip() {
        let store = make_store();
        let sh = make_script_hash(0x40);
        let txid = make_txid(0x06);
        store
            .store_history_entry(sh, 1, txid, 0, HistoryKind::Fund)
            .expect("store");
        assert!(store.has_history(&sh));
    }

    #[test]
    fn load_history_for_script_returns_entries_sorted_by_height() {
        let store = make_store();
        let sh = make_script_hash(0x41);
        let txid_a = make_txid(0x0a);
        let txid_b = make_txid(0x0b);
        store
            .store_history_entry(sh, 5, txid_b, 0, HistoryKind::Fund)
            .expect("store 5");
        store
            .store_history_entry(sh, 1, txid_a, 0, HistoryKind::Fund)
            .expect("store 1");
        let entries = store.load_history_for_script(&sh).expect("load");
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].height, 1);
        assert_eq!(entries[1].height, 5);
    }

    #[test]
    fn delete_history_key_removes_entry() {
        let store = make_store();
        let sh = make_script_hash(0x42);
        let txid = make_txid(0x07);
        let key = store
            .store_history_entry(sh, 1, txid, 0, HistoryKind::Fund)
            .expect("store");
        store.delete_history_key(&key).expect("delete");
        assert!(!store.has_history(&sh));
    }

    #[test]
    fn load_history_entries_returns_all_entries() {
        let store = make_store();
        let sh_a = make_script_hash(0x50);
        let sh_b = make_script_hash(0x51);
        store
            .store_history_entry(sh_a, 1, make_txid(0x08), 0, HistoryKind::Fund)
            .expect("a");
        store
            .store_history_entry(sh_b, 2, make_txid(0x09), 0, HistoryKind::Spend)
            .expect("b");
        let all = store.load_history_entries().expect("load all");
        assert_eq!(all.len(), 2);
    }

    // ---- parse_history_key error ------------------------------------------

    #[test]
    fn parse_history_key_rejects_wrong_length() {
        assert!(parse_history_key(&[0u8; 72]).is_err());
        assert!(parse_history_key(&[0u8; 74]).is_err());
    }

    #[test]
    fn parse_history_key_accepts_exactly_73_bytes() {
        assert!(parse_history_key(&[0u8; 73]).is_ok());
    }

    // ---- journal actions --------------------------------------------------

    #[test]
    fn store_and_load_journal_action_round_trip() {
        let store = make_store();
        let key = store
            .store_journal_action(10, 0, JournalActionKind::Tx, &[0x01, 0x02])
            .expect("store");
        let actions = store.load_journal_actions().expect("load");
        assert_eq!(actions.len(), 1);
        assert_eq!(actions[0].height, 10);
        assert_eq!(actions[0].sequence, 0);
        assert_eq!(actions[0].kind, JournalActionKind::Tx);
        assert_eq!(actions[0].payload, vec![0x01, 0x02]);
        assert_eq!(actions[0].journal_key, key);
    }

    #[test]
    fn delete_journal_key_removes_entry() {
        let store = make_store();
        let key = store
            .store_journal_action(1, 0, JournalActionKind::History, &[])
            .expect("store");
        store.delete_journal_key(&key).expect("delete");
        assert!(store.load_journal_actions().expect("load").is_empty());
    }

    #[test]
    fn journal_actions_sorted_by_height_then_sequence() {
        let store = make_store();
        store
            .store_journal_action(2, 1, JournalActionKind::Output, &[])
            .expect("2:1");
        store
            .store_journal_action(1, 0, JournalActionKind::Tx, &[])
            .expect("1:0");
        store
            .store_journal_action(2, 0, JournalActionKind::Spend, &[])
            .expect("2:0");
        let actions = store.load_journal_actions().expect("load");
        assert_eq!(actions[0].height, 1);
        assert_eq!(actions[1].height, 2);
        assert_eq!(actions[1].sequence, 0);
        assert_eq!(actions[2].height, 2);
        assert_eq!(actions[2].sequence, 1);
    }

    // ---- parse_journal_action error ---------------------------------------

    #[test]
    fn parse_journal_action_rejects_wrong_key_length() {
        assert!(parse_journal_action(&[0u8; 7], &[0u8]).is_err());
        assert!(parse_journal_action(&[0u8; 9], &[0u8]).is_err());
    }

    #[test]
    fn parse_journal_action_rejects_empty_value() {
        assert!(parse_journal_action(&[0u8; 8], &[]).is_err());
    }

    #[test]
    fn parse_journal_action_rejects_unknown_kind() {
        assert!(parse_journal_action(&[0u8; 8], &[0xffu8]).is_err());
    }

    #[test]
    fn parse_journal_action_recognises_all_kinds() {
        for (byte, expected) in [
            (0u8, JournalActionKind::Tx),
            (1u8, JournalActionKind::History),
            (2u8, JournalActionKind::Output),
            (3u8, JournalActionKind::Spend),
        ] {
            let action = parse_journal_action(&[0u8; 8], &[byte]).expect("parse");
            assert_eq!(action.kind, expected);
        }
    }
}
