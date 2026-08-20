use std::path::Path;
use std::thread;
use std::time::Duration;

use anyhow::{Context, Result};
use bitcoin::consensus::encode::{deserialize, serialize};
use bitcoin::hashes::Hash;
use bitcoin::{OutPoint, Transaction, Txid};
use sled::Tree;

use crate::indexer::{ScriptHash, TxEntry};

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
