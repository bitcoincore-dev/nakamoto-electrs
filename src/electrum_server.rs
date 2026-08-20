//! Electrum JSON-RPC TCP server.
//!
//! Implements a subset of the [Electrum protocol v1.4][spec] over plain TCP.
//! Each connected client gets its own thread.  All query results are read
//! from the shared [`Indexer`] and the underlying [`BlockSource`].
//!
//! ## Supported methods
//!
//! | Method | Description |
//! |---|---|
//! | `server.version` | Protocol negotiation |
//! | `server.ping` | Keepalive |
//! | `server.banner` | Server banner string |
//! | `blockchain.headers.subscribe` | Subscribe to new block headers |
//! | `blockchain.scripthash.get_history` | Transaction history for a script hash |
//! | `blockchain.scripthash.get_balance` | Balance for a script hash |
//! | `blockchain.scripthash.subscribe` | Subscribe to script-hash status changes |
//! | `blockchain.transaction.get` | Fetch a raw transaction by txid |
//! | `blockchain.transaction.broadcast` | Broadcast a raw transaction |
//! | `blockchain.estimatefee` | Estimate fee rate |
//! | `blockchain.block.header` | Fetch a block header by height |
//! | `blockchain.block.headers` | Fetch a range of block headers |
//!
//! [spec]: https://electrumx.readthedocs.io/en/latest/protocol-methods.html

use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::thread;

use anyhow::{Context, Result};
use bitcoin::consensus::encode::{serialize, serialize_hex};
use bitcoin::hashes::{Hash, sha256};
use bitcoin::{Transaction, consensus::deserialize};
use nakamoto_common::bitcoin::consensus::encode::deserialize as nk_deserialize;
use serde_json::{Value, json};
use tracing::{debug, error, info, warn};

use crate::block_source::BlockSource;
use crate::indexer::{Indexer, ScriptHash};
use crate::metrics::Metrics;

pub trait TransactionBroadcaster: Send + Sync {
    fn broadcast_transaction(&self, tx: Transaction) -> Result<(), String>;
}

impl<T> TransactionBroadcaster for T
where
    T: nakamoto_client::handle::Handle,
{
    fn broadcast_transaction(&self, tx: Transaction) -> Result<(), String> {
        let nk_tx = nk_deserialize(&serialize(&tx))
            .map_err(|e| format!("transaction conversion failed: {e}"))?;
        self.submit_transaction(nk_tx)
            .map(|_| ())
            .map_err(|e| format!("{e:#}"))
    }
}

const PROTOCOL_VERSION: &str = "1.4";
const SERVER_VERSION: &str = concat!("nakamoto-electrs/", env!("CARGO_PKG_VERSION"));
const BANNER: &str = "nakamoto-electrs: nakamoto SPV + Electrum bridge";

// ---------------------------------------------------------------------------
// ElectrumServer
// ---------------------------------------------------------------------------

/// Listens for incoming Electrum TCP connections and dispatches each to a
/// dedicated handler thread.
pub struct ElectrumServer {
    listener: TcpListener,
    indexer: Indexer,
    metrics: Metrics,
    broadcaster: Option<Arc<dyn TransactionBroadcaster>>,
}

impl ElectrumServer {
    /// Bind the server to the given address.
    pub fn bind(
        addr: std::net::SocketAddr,
        indexer: Indexer,
        metrics: Metrics,
        broadcaster: Option<Arc<dyn TransactionBroadcaster>>,
    ) -> Result<Self> {
        let listener = TcpListener::bind(addr).context("failed to bind Electrum listener")?;
        info!("Electrum server listening on {addr}");
        Ok(Self {
            listener,
            indexer,
            metrics,
            broadcaster,
        })
    }

    /// Return the local socket address the server is bound to.
    ///
    /// Useful in tests when the server was bound to port 0 and the OS
    /// assigned a free port.
    pub fn local_addr(&self) -> std::net::SocketAddr {
        self.listener
            .local_addr()
            .expect("failed to get local_addr")
    }

    /// Run the accept loop.  Blocks until the listener is closed or an
    /// unrecoverable error occurs.
    pub fn run<S: BlockSource + Sync>(self, source: Arc<S>) -> Result<()> {
        let indexer = Arc::new(self.indexer);
        let metrics = Arc::new(self.metrics);
        let broadcaster = self.broadcaster.clone();

        for stream in self.listener.incoming() {
            match stream {
                Ok(stream) => {
                    let peer = stream
                        .peer_addr()
                        .map(|a| a.to_string())
                        .unwrap_or_else(|_| "unknown".into());
                    info!("new Electrum connection from {peer}");

                    let indexer = Arc::clone(&indexer);
                    let source = Arc::clone(&source);
                    let metrics = Arc::clone(&metrics);
                    let broadcaster = broadcaster.clone();

                    metrics.inc_electrum_connections();

                    thread::Builder::new()
                        .name(format!("electrum-{peer}"))
                        .spawn(move || {
                            if let Err(e) =
                                handle_client(stream, &indexer, &source, &metrics, broadcaster)
                            {
                                debug!("client {peer} disconnected: {e:#}");
                            }
                            metrics.dec_electrum_connections();
                        })
                        .expect("failed to spawn electrum client thread");
                }
                Err(e) => {
                    error!("accept error: {e}");
                }
            }
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Per-client handler
// ---------------------------------------------------------------------------

/// State kept per Electrum TCP connection.
struct ClientState {
    /// Script hashes subscribed by this client.
    subscribed_scripthashes: Vec<ScriptHash>,
}

impl ClientState {
    fn new() -> Self {
        Self {
            subscribed_scripthashes: Vec::new(),
        }
    }
}

fn handle_client<S: BlockSource>(
    stream: TcpStream,
    indexer: &Indexer,
    source: &S,
    metrics: &Metrics,
    broadcaster: Option<Arc<dyn TransactionBroadcaster>>,
) -> Result<()> {
    let peer = stream.peer_addr()?.to_string();
    let mut writer = stream.try_clone().context("clone stream for write")?;
    let reader = BufReader::new(stream);
    let mut state = ClientState::new();

    for line in reader.lines() {
        let line = line.context("read line")?;
        if line.is_empty() {
            continue;
        }
        debug!("← {peer}: {line}");
        metrics.inc_electrum_requests();

        let response = dispatch_request(
            &line,
            &mut state,
            indexer,
            source,
            metrics,
            broadcaster.as_ref(),
        );
        let response_str = serde_json::to_string(&response)? + "\n";
        debug!("→ {peer}: {}", response_str.trim_end());
        writer
            .write_all(response_str.as_bytes())
            .context("write response")?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Request dispatch
// ---------------------------------------------------------------------------

fn dispatch_request<S: BlockSource>(
    raw: &str,
    state: &mut ClientState,
    indexer: &Indexer,
    source: &S,
    metrics: &Metrics,
    broadcaster: Option<&Arc<dyn TransactionBroadcaster>>,
) -> Value {
    let req: Value = match serde_json::from_str(raw) {
        Ok(v) => v,
        Err(e) => {
            return json!({
                "jsonrpc": "2.0",
                "id": null,
                "error": {"code": -32700, "message": format!("parse error: {e}")}
            });
        }
    };

    let id = req.get("id").cloned().unwrap_or(Value::Null);
    let method = req
        .get("method")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let params = req.get("params").cloned().unwrap_or(Value::Null);

    let result = match method.as_str() {
        "server.version" => handle_server_version(&params),
        "server.ping" => Ok(Value::Null),
        "server.banner" => Ok(Value::String(BANNER.into())),
        "blockchain.headers.subscribe" => handle_headers_subscribe(indexer, source),
        "blockchain.scripthash.get_history" => handle_scripthash_get_history(&params, indexer),
        "blockchain.scripthash.get_balance" => handle_scripthash_get_balance(&params, indexer),
        "blockchain.scripthash.subscribe" => handle_scripthash_subscribe(&params, state, indexer),
        "blockchain.transaction.get" => handle_transaction_get(&params, indexer),
        "blockchain.transaction.broadcast" => {
            handle_transaction_broadcast(&params, metrics, broadcaster)
        }
        "blockchain.estimatefee" => handle_estimatefee(&params),
        "blockchain.block.header" => handle_block_header(&params, source),
        "blockchain.block.headers" => handle_block_headers(&params, source),
        unknown => {
            warn!("unknown method: {unknown}");
            Err(format!("unknown method '{unknown}'"))
        }
    };

    match result {
        Ok(v) => json!({"jsonrpc": "2.0", "id": id, "result": v}),
        Err(msg) => json!({
            "jsonrpc": "2.0",
            "id": id,
            "error": {"code": -32601, "message": msg}
        }),
    }
}

// ---------------------------------------------------------------------------
// Individual method handlers
// ---------------------------------------------------------------------------

fn handle_server_version(params: &Value) -> std::result::Result<Value, String> {
    // Params: [client_name, protocol_version]  (both optional in our impl)
    let _client = params.get(0).and_then(Value::as_str).unwrap_or("unknown");
    Ok(json!([SERVER_VERSION, PROTOCOL_VERSION]))
}

fn handle_headers_subscribe<S: BlockSource>(
    indexer: &Indexer,
    source: &S,
) -> std::result::Result<Value, String> {
    let height = indexer.tip_height();
    match source.block_header(height) {
        Ok(Some(header)) => {
            let hex = serialize_hex(&header);
            Ok(json!({"height": height, "hex": hex}))
        }
        Ok(None) => {
            // Index hasn't processed any blocks yet.
            Ok(json!({"height": 0, "hex": ""}))
        }
        Err(e) => Err(format!("block_header error: {e}")),
    }
}

fn handle_scripthash_get_history(
    params: &Value,
    indexer: &Indexer,
) -> std::result::Result<Value, String> {
    let sh = parse_scripthash(params)?;
    let entries = indexer.get_history(&sh);
    let list: Vec<Value> = entries
        .iter()
        .map(|e| {
            json!({
                "tx_hash": e.txid.to_string(),
                "height": e.height
            })
        })
        .collect();
    Ok(Value::Array(list))
}

fn handle_scripthash_get_balance(
    params: &Value,
    indexer: &Indexer,
) -> std::result::Result<Value, String> {
    let sh = parse_scripthash(params)?;
    let confirmed = indexer
        .get_balance(&sh)
        .map_err(|e| format!("balance lookup failed: {e:#}"))?;
    Ok(json!({
        "confirmed": confirmed,
        "unconfirmed": 0
    }))
}

fn handle_scripthash_subscribe(
    params: &Value,
    state: &mut ClientState,
    indexer: &Indexer,
) -> std::result::Result<Value, String> {
    let sh = parse_scripthash(params)?;
    if !state.subscribed_scripthashes.contains(&sh) {
        state.subscribed_scripthashes.push(sh);
    }
    let history = indexer.get_history(&sh);
    Ok(match compute_status_hash(&history) {
        Some(status) => Value::String(status),
        None => Value::Null,
    })
}

fn handle_transaction_get(params: &Value, indexer: &Indexer) -> std::result::Result<Value, String> {
    let txid_str = params
        .get(0)
        .and_then(Value::as_str)
        .ok_or("missing txid parameter")?;
    let txid: bitcoin::Txid = txid_str.parse().map_err(|e| format!("invalid txid: {e}"))?;
    match indexer.get_transaction(&txid) {
        Ok(Some(tx)) => Ok(Value::String(serialize_hex(&tx))),
        Ok(None) => Err(format!("transaction {txid_str} not in local cache")),
        Err(e) => Err(format!("transaction lookup failed: {e:#}")),
    }
}

fn handle_transaction_broadcast(
    params: &Value,
    metrics: &Metrics,
    broadcaster: Option<&Arc<dyn TransactionBroadcaster>>,
) -> std::result::Result<Value, String> {
    let raw_hex = params
        .get(0)
        .and_then(Value::as_str)
        .ok_or("missing raw transaction hex")?;
    let raw_bytes = hex::decode(raw_hex).map_err(|e| format!("invalid hex: {e}"))?;
    let tx: Transaction =
        deserialize(&raw_bytes).map_err(|e| format!("invalid transaction: {e}"))?;
    let txid = tx.compute_txid();

    metrics.inc_transactions_broadcast();
    match broadcaster {
        Some(broadcaster) => {
            broadcaster.broadcast_transaction(tx).map_err(|e| format!("broadcast failed: {e}"))?;
            Ok(Value::String(txid.to_string()))
        }
        None => Err("transaction broadcast not available in this mode".into()),
    }
}

fn handle_estimatefee(params: &Value) -> std::result::Result<Value, String> {
    let _blocks = params.get(0).and_then(Value::as_u64).unwrap_or(6);
    // Nakamoto does not yet expose a fee estimator; return -1 as per the
    // Electrum protocol spec for "unknown".
    Ok(json!(-1))
}

fn handle_block_header<S: BlockSource>(
    params: &Value,
    source: &S,
) -> std::result::Result<Value, String> {
    let height = params
        .get(0)
        .and_then(Value::as_u64)
        .ok_or("missing height parameter")? as u32;
    match source.block_header(height) {
        Ok(Some(header)) => Ok(Value::String(serialize_hex(&header))),
        Ok(None) => Err(format!("no header at height {height}")),
        Err(e) => Err(format!("block_header error: {e}")),
    }
}

fn handle_block_headers<S: BlockSource>(
    params: &Value,
    source: &S,
) -> std::result::Result<Value, String> {
    let start = params
        .get(0)
        .and_then(Value::as_u64)
        .ok_or("missing start_height")? as u32;
    let count = params
        .get(1)
        .and_then(Value::as_u64)
        .ok_or("missing count")? as u32;

    let mut headers_hex = String::new();
    let mut returned = 0u32;
    for h in start..start.saturating_add(count) {
        match source.block_header(h) {
            Ok(Some(header)) => {
                headers_hex.push_str(&serialize_hex(&header));
                returned += 1;
            }
            Ok(None) => break,
            Err(e) => return Err(format!("block_header({h}) error: {e}")),
        }
    }
    Ok(json!({"count": returned, "hex": headers_hex, "max": 2016}))
}

// ---------------------------------------------------------------------------
// Parameter helpers
// ---------------------------------------------------------------------------

fn parse_scripthash(params: &Value) -> std::result::Result<ScriptHash, String> {
    let hex_str = params
        .get(0)
        .and_then(Value::as_str)
        .ok_or("missing script_hash parameter")?;
    if hex_str.len() != 64 {
        return Err(format!(
            "script_hash must be 64 hex chars, got {}",
            hex_str.len()
        ));
    }
    let mut bytes = [0u8; 32];
    for (i, byte) in bytes.iter_mut().enumerate() {
        let byte_str = hex_str
            .get(i * 2..i * 2 + 2)
            .ok_or("script_hash too short")?;
        *byte = u8::from_str_radix(byte_str, 16).map_err(|e| format!("invalid hex byte: {e}"))?;
    }
    Ok(ScriptHash::from_raw_bytes(bytes))
}

fn compute_status_hash(history: &[crate::indexer::TxEntry]) -> Option<String> {
    if history.is_empty() {
        return None;
    }

    let mut data = String::new();
    for entry in history {
        data.push_str(&entry.txid.to_string());
        data.push(':');
        data.push_str(&entry.height.to_string());
        data.push(':');
    }

    Some(sha256::Hash::hash(data.as_bytes()).to_string())
}

// ---------------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn server_version_response() {
        let params = json!(["Electrum/4.0", "1.4"]);
        let r = handle_server_version(&params).unwrap();
        let arr = r.as_array().unwrap();
        assert!(arr[0].as_str().unwrap().contains("nakamoto-electrs"));
        assert_eq!(arr[1].as_str().unwrap(), "1.4");
    }

    #[test]
    fn parse_scripthash_valid() {
        let hex = "a".repeat(64);
        // Should parse without error.
        assert!(parse_scripthash(&json!([hex])).is_ok());
    }

    #[test]
    fn parse_scripthash_wrong_length() {
        let short = "abcd";
        assert!(parse_scripthash(&json!([short])).is_err());
    }

    #[test]
    fn parse_scripthash_missing_param() {
        assert!(parse_scripthash(&json!([])).is_err());
    }

    #[test]
    fn dispatch_ping_returns_null() {
        use crate::block_source::{BlockEvent, BlockSource};
        use crossbeam_channel::Receiver;

        struct FakeSource;
        impl BlockSource for FakeSource {
            fn subscribe(&self) -> Receiver<BlockEvent> {
                crossbeam_channel::never()
            }
            fn tip(&self) -> anyhow::Result<(u32, bitcoin::BlockHash)> {
                use bitcoin::hashes::Hash;
                Ok((0, bitcoin::BlockHash::all_zeros()))
            }
            fn block_header(
                &self,
                _h: u32,
            ) -> anyhow::Result<Option<bitcoin::blockdata::block::Header>> {
                Ok(None)
            }
            fn block_by_hash(
                &self,
                _hash: &bitcoin::BlockHash,
            ) -> anyhow::Result<Option<bitcoin::Block>> {
                Ok(None)
            }
        }

        let dir = tempfile::tempdir().expect("temp index dir").keep();
        let indexer = Indexer::new(dir, Metrics::new()).expect("indexer");
        let source = FakeSource;
        let mut state = ClientState::new();
        let raw = r#"{"jsonrpc":"2.0","id":1,"method":"server.ping","params":[]}"#;
        let resp = dispatch_request(raw, &mut state, &indexer, &source, &Metrics::new(), None);
        assert_eq!(resp["result"], Value::Null);
        assert_eq!(resp["id"], json!(1));
    }

    #[test]
    fn compute_status_hash_uses_electrum_format() {
        let txid = "0".repeat(64).parse().unwrap();
        let history = vec![crate::indexer::TxEntry { txid, height: 1 }];
        assert_eq!(
            compute_status_hash(&history).as_deref(),
            Some("12b132b4f9cac2ddb0a05030bf14ab07a46352fe787aa4f0e245fac197dd5b48")
        );
    }
}
