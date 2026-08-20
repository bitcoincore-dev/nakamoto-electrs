use std::path::Path;

use anyhow::{Context, Result};
use bitcoin::consensus::encode::{deserialize, serialize};
use bitcoin::hashes::Hash;
use bitcoin::{Transaction, Txid};
use sled::{Db, Tree};

use crate::indexer::{ScriptHash, TxEntry};

#[derive(Clone)]
pub struct PersistentIndex {
    history: Tree,
    txs: Tree,
    meta: Tree,
}

#[derive(Debug, Clone)]
pub struct StoredHistoryEntry {
    pub script_hash: ScriptHash,
    pub txid: Txid,
    pub height: u32,
    pub output_index: u32,
    pub history_key: Vec<u8>,
}

impl PersistentIndex {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let db: Db = sled::open(path).context("failed to open persistent index db")?;
        let history = db.open_tree("history")?;
        let txs = db.open_tree("txs")?;
        let meta = db.open_tree("meta")?;
        Ok(Self { history, txs, meta })
    }

    pub fn set_tip_height(&self, height: u32) -> Result<()> {
        self.meta.insert(b"tip_height", height.to_be_bytes().to_vec())?;
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

    pub fn store_history_entry(
        &self,
        script_hash: ScriptHash,
        height: u32,
        txid: Txid,
        output_index: u32,
    ) -> Result<Vec<u8>> {
        let key = history_key(script_hash, height, txid, output_index);
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
            });
        }
        out.sort_by_key(|e| if e.height == 0 { u32::MAX } else { e.height });
        Ok(out)
    }

    pub fn has_history(&self, script_hash: &ScriptHash) -> bool {
        self.history
            .scan_prefix(script_hash.as_bytes())
            .next()
            .is_some()
    }
}

fn history_key(script_hash: ScriptHash, height: u32, txid: Txid, output_index: u32) -> Vec<u8> {
    let mut key = Vec::with_capacity(72);
    key.extend_from_slice(script_hash.as_bytes());
    key.extend_from_slice(&height.to_be_bytes());
    key.extend_from_slice(txid.as_byte_array());
    key.extend_from_slice(&output_index.to_be_bytes());
    key
}

fn parse_history_key(key: &[u8]) -> Result<StoredHistoryEntry> {
    if key.len() != 72 {
        anyhow::bail!("invalid history key length: {}", key.len());
    }

    let mut sh = [0u8; 32];
    sh.copy_from_slice(&key[0..32]);

    let mut height = [0u8; 4];
    height.copy_from_slice(&key[32..36]);

    let mut txid = [0u8; 32];
    txid.copy_from_slice(&key[36..68]);

    let mut output_index = [0u8; 4];
    output_index.copy_from_slice(&key[68..72]);

    Ok(StoredHistoryEntry {
        script_hash: ScriptHash::from_raw_bytes(sh),
        txid: Txid::from_byte_array(txid),
        height: u32::from_be_bytes(height),
        output_index: u32::from_be_bytes(output_index),
        history_key: key.to_vec(),
    })
}
