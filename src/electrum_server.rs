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
//! | `blockchain.scripthash.get_mempool` | Mempool transactions touching a script hash |
//! | `blockchain.scripthash.listunspent` | List unspent outputs for a script hash |
//! | `blockchain.scripthash.subscribe` | Subscribe to script-hash status changes |
//! | `blockchain.transaction.get` | Fetch a raw transaction by txid |
//! | `blockchain.transaction.broadcast` | Broadcast a raw transaction |
//! | `blockchain.estimatefee` | Estimate fee rate |
//! | `blockchain.block.header` | Fetch a block header by height |
//! | `blockchain.block.headers` | Fetch a range of block headers |
//!
//! [spec]: https://electrumx.readthedocs.io/en/latest/protocol-methods.html

use std::collections::HashMap;
use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
use std::thread;

use anyhow::{Context, Result};
use bitcoin::consensus::encode::{serialize, serialize_hex};
use bitcoin::hashes::{Hash, sha256};
use bitcoin::{Transaction, consensus::deserialize};
use crossbeam_channel::{Receiver as CbReceiver, Sender as CbSender, select, unbounded};
use nakamoto_common::bitcoin::consensus::encode::deserialize as nk_deserialize;
use serde_json::{Value, json};
use tracing::{debug, error, info, warn};

use crate::block_source::BlockSource;
use crate::indexer::{Indexer, MempoolEntry, ScriptHash, TxEntry};
use crate::metrics::Metrics;

pub trait TransactionBroadcaster: Send + Sync {
    fn broadcast_transaction(&self, tx: Transaction) -> Result<(), String>;
}

#[derive(Clone, Default)]
pub struct PendingChangeBroadcaster {
    senders: Arc<Mutex<Vec<CbSender<Vec<ScriptHash>>>>>,
}

impl PendingChangeBroadcaster {
    fn subscribe(&self) -> CbReceiver<Vec<ScriptHash>> {
        let (tx, rx) = unbounded();
        self.senders
            .lock()
            .expect("pending change lock poisoned")
            .push(tx);
        rx
    }

    fn broadcast(&self, affected: Vec<ScriptHash>) {
        let mut guard = self.senders.lock().expect("pending change lock poisoned");
        guard.retain(|tx| tx.send(affected.clone()).is_ok());
    }
}

pub(crate) fn apply_tx_status_change(
    indexer: &Indexer,
    pending_changes: &PendingChangeBroadcaster,
    txid: &str,
    status: &str,
) -> Result<()> {
    let txid: bitcoin::Txid = txid.parse().context("invalid txid in tx status event")?;
    let affected = if status.contains("reverted") {
        indexer.restore_pending_transaction(&txid)?
    } else if status.contains("included in block") || status.contains("replaced by") {
        indexer.forget_pending_transaction(&txid)?
    } else {
        None
    };

    if let Some(affected) = affected {
        pending_changes.broadcast(affected);
    }

    Ok(())
}

#[derive(Debug, Default)]
pub struct FeeRateState {
    sat_per_vb: AtomicU64,
}

impl FeeRateState {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn update_sat_per_vb(&self, sat_per_vb: u64) {
        self.sat_per_vb.store(sat_per_vb, Ordering::Relaxed);
    }

    pub fn current_sat_per_vb(&self) -> Option<u64> {
        match self.sat_per_vb.load(Ordering::Relaxed) {
            0 => None,
            value => Some(value),
        }
    }
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
    fee_rate: Arc<FeeRateState>,
    pending_changes: PendingChangeBroadcaster,
}

impl ElectrumServer {
    /// Bind the server to the given address.
    pub fn bind(
        addr: std::net::SocketAddr,
        indexer: Indexer,
        metrics: Metrics,
        broadcaster: Option<Arc<dyn TransactionBroadcaster>>,
        fee_rate: Arc<FeeRateState>,
        pending_changes: PendingChangeBroadcaster,
    ) -> Result<Self> {
        let listener = TcpListener::bind(addr).context("failed to bind Electrum listener")?;
        info!("Electrum server listening on {addr}");
        Ok(Self {
            listener,
            indexer,
            metrics,
            broadcaster,
            fee_rate,
            pending_changes,
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
    pub fn run<S: BlockSource + Sync>(
        self,
        source: Arc<S>,
        shutdown: Arc<AtomicBool>,
    ) -> Result<()> {
        let indexer = Arc::new(self.indexer);
        let metrics = Arc::new(self.metrics);
        let broadcaster = self.broadcaster.clone();
        let fee_rate = Arc::clone(&self.fee_rate);
        self.listener
            .set_nonblocking(true)
            .context("failed to set Electrum listener nonblocking")?;

        while !shutdown.load(Ordering::Relaxed) {
            match self.listener.accept() {
                Ok((stream, peer_addr)) => {
                    let peer = peer_addr.to_string();
                    info!("new Electrum connection from {peer}");

                    let indexer = Arc::clone(&indexer);
                    let source = Arc::clone(&source);
                    let metrics = Arc::clone(&metrics);
                    let broadcaster = broadcaster.clone();
                    let fee_rate = Arc::clone(&fee_rate);
                    let pending_changes = self.pending_changes.clone();

                    metrics.inc_electrum_connections();

                    thread::Builder::new()
                        .name(format!("electrum-{peer}"))
                        .spawn(move || {
                            if let Err(e) = handle_client(
                                stream,
                                &indexer,
                                Arc::clone(&source),
                                &metrics,
                                broadcaster,
                                &fee_rate,
                                &pending_changes,
                            ) {
                                debug!("client {peer} disconnected: {e:#}");
                            }
                            metrics.dec_electrum_connections();
                        })
                        .expect("failed to spawn electrum client thread");
                }
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    std::thread::sleep(std::time::Duration::from_millis(200));
                }
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
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
    /// Whether this client subscribed to block headers.
    headers_subscribed: bool,
    /// Last status sent to the client for each subscribed script hash.
    status_by_scripthash: HashMap<ScriptHash, Option<String>>,
    /// Last headers.subscribe payload sent to the client.
    header_subscription: Option<(u32, String)>,
}

impl ClientState {
    fn new() -> Self {
        Self {
            subscribed_scripthashes: Vec::new(),
            headers_subscribed: false,
            status_by_scripthash: HashMap::new(),
            header_subscription: None,
        }
    }
}

fn handle_client<S: BlockSource + Sync + 'static>(
    stream: TcpStream,
    indexer: &Indexer,
    source: Arc<S>,
    metrics: &Metrics,
    broadcaster: Option<Arc<dyn TransactionBroadcaster>>,
    fee_rate: &Arc<FeeRateState>,
    pending_changes: &PendingChangeBroadcaster,
) -> Result<()> {
    let peer = stream.peer_addr()?.to_string();
    let writer = Arc::new(Mutex::new(
        stream.try_clone().context("clone stream for write")?,
    ));
    let reader = BufReader::new(stream);
    let state = Arc::new(Mutex::new(ClientState::new()));
    let events = source.subscribe();
    let pending_events = pending_changes.subscribe();
    let notifications = {
        let writer = Arc::clone(&writer);
        let indexer = indexer.clone();
        let state = Arc::clone(&state);
        let source = Arc::clone(&source);
        thread::Builder::new()
            .name(format!("electrum-notify-{peer}"))
            .spawn(move || {
                loop {
                    select! {
                        recv(events) -> msg => {
                            let Ok(event) = msg else { break };
                            if matches!(
                                event,
                                crate::block_source::BlockEvent::Connected { .. }
                                    | crate::block_source::BlockEvent::Disconnected { .. }
                            ) {
                                wait_for_indexer_tip(&indexer, &event);
                                let current_headers = match &event {
                                    crate::block_source::BlockEvent::Connected { block, height } => {
                                        Some((*height, serialize_hex(&block.header)))
                                    }
                                    _ => current_header_status(&indexer, source.as_ref())
                                        .ok()
                                        .flatten(),
                                };
                                let mut changed = Vec::new();
                                {
                                    let mut state = state.lock().expect("electrum state poisoned");
                                    let subs = state.subscribed_scripthashes.clone();
                                    for sh in subs {
                                        let status = match script_status(&indexer, &sh) {
                                            Ok(status) => status,
                                            Err(e) => {
                                                error!("status lookup failed: {e:#}");
                                                continue;
                                            }
                                        };
                                        let entry = state.status_by_scripthash.entry(sh).or_insert(None);
                                        if *entry != status {
                                            *entry = status.clone();
                                            changed.push((sh, status));
                                        }
                                    }
                                }
                                let mut send_header = false;
                                {
                                    let mut state = state.lock().expect("electrum state poisoned");
                                    if state.headers_subscribed
                                        && state.header_subscription != current_headers
                                    {
                                        state.header_subscription = current_headers.clone();
                                        send_header = true;
                                    }
                                }
                                if send_header {
                                    let response = match current_headers {
                                        Some((height, hex)) => json!({
                                            "jsonrpc": "2.0",
                                            "method": "blockchain.headers.subscribe",
                                            "params": [{"height": height, "hex": hex}],
                                        }),
                                        None => json!({
                                            "jsonrpc": "2.0",
                                            "method": "blockchain.headers.subscribe",
                                            "params": [{"height": 0, "hex": ""}],
                                        }),
                                    };
                                    if write_json(&writer, &response).is_err() {
                                        break;
                                    }
                                }
                                for (sh, status) in changed {
                                    let response = json!({
                                        "jsonrpc": "2.0",
                                        "method": "blockchain.scripthash.subscribe",
                                        "params": [sh.to_hex(), status],
                                    });
                                    if write_json(&writer, &response).is_err() {
                                        break;
                                    }
                                }
                            }
                        },
                        recv(pending_events) -> msg => {
                            let Ok(affected) = msg else { break };
                            if affected.is_empty() {
                                continue;
                            }
                            let mut changed = Vec::new();
                            {
                                let mut state = state.lock().expect("electrum state poisoned");
                                let subs = state.subscribed_scripthashes.clone();
                                for sh in subs {
                                    if !affected.contains(&sh) {
                                        continue;
                                    }
                                    let status = match script_status(&indexer, &sh) {
                                        Ok(status) => status,
                                        Err(e) => {
                                            error!("status lookup failed: {e:#}");
                                            continue;
                                        }
                                    };
                                    let entry = state.status_by_scripthash.entry(sh).or_insert(None);
                                    if *entry != status {
                                        *entry = status.clone();
                                        changed.push((sh, status));
                                    }
                                }
                            }
                            for (sh, status) in changed {
                                let response = json!({
                                    "jsonrpc": "2.0",
                                    "method": "blockchain.scripthash.subscribe",
                                    "params": [sh.to_hex(), status],
                                });
                                if write_json(&writer, &response).is_err() {
                                    break;
                                }
                            }
                        }
                    }
                }
            })
            .ok()
    };

    for line in reader.lines() {
        let line = line.context("read line")?;
        if line.is_empty() {
            continue;
        }
        debug!("← {peer}: {line}");
        metrics.inc_electrum_requests();

        let response = {
            let mut state = state.lock().expect("electrum state poisoned");
            dispatch_request(
                &line,
                &mut state,
                indexer,
                source.as_ref(),
                metrics,
                broadcaster.as_ref(),
                fee_rate,
                pending_changes,
            )
        };
        debug!("→ {peer}: {}", serde_json::to_string(&response)?.trim_end());
        write_json(&writer, &response)?;
    }
    let _ = notifications;
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
    fee_rate: &Arc<FeeRateState>,
    pending_changes: &PendingChangeBroadcaster,
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
        "blockchain.headers.subscribe" => handle_headers_subscribe(state, indexer, source),
        "blockchain.scripthash.get_history" => handle_scripthash_get_history(&params, indexer),
        "blockchain.scripthash.get_balance" => handle_scripthash_get_balance(&params, indexer),
        "blockchain.scripthash.get_mempool" => handle_scripthash_get_mempool(&params, indexer),
        "blockchain.scripthash.listunspent" => handle_scripthash_listunspent(&params, indexer),
        "blockchain.scripthash.subscribe" => handle_scripthash_subscribe(&params, state, indexer),
        "blockchain.transaction.get" => handle_transaction_get(&params, indexer),
        "blockchain.transaction.broadcast" => {
            handle_transaction_broadcast(&params, metrics, broadcaster, indexer, pending_changes)
        }
        "blockchain.estimatefee" => handle_estimatefee(&params, fee_rate),
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
    state: &mut ClientState,
    indexer: &Indexer,
    source: &S,
) -> std::result::Result<Value, String> {
    state.headers_subscribed = true;
    let current =
        current_header_status(indexer, source).map_err(|e| format!("block_header error: {e}"))?;
    state.header_subscription = current.clone();
    Ok(match current {
        Some((height, hex)) => json!({"height": height, "hex": hex}),
        None => json!({"height": 0, "hex": ""}),
    })
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
    let unconfirmed = indexer
        .get_unconfirmed_balance_delta(&sh)
        .map_err(|e| format!("balance lookup failed: {e:#}"))?;
    Ok(json!({
        "confirmed": confirmed,
        "unconfirmed": unconfirmed
    }))
}

fn handle_scripthash_get_mempool(
    params: &Value,
    indexer: &Indexer,
) -> std::result::Result<Value, String> {
    let sh = parse_scripthash(params)?;
    let entries = indexer
        .get_mempool(&sh)
        .map_err(|e| format!("mempool lookup failed: {e:#}"))?;
    Ok(Value::Array(
        entries
            .into_iter()
            .map(|e| {
                json!({
                    "tx_hash": e.txid.to_string(),
                    "height": e.height,
                    "fee": e.fee,
                })
            })
            .collect(),
    ))
}

fn handle_scripthash_listunspent(
    params: &Value,
    indexer: &Indexer,
) -> std::result::Result<Value, String> {
    let sh = parse_scripthash(params)?;
    let entries = indexer
        .list_unspent(&sh)
        .map_err(|e| format!("listunspent failed: {e:#}"))?;
    Ok(Value::Array(
        entries
            .into_iter()
            .map(|e| {
                json!({
                    "tx_hash": e.txid.to_string(),
                    "tx_pos": e.vout,
                    "height": e.height,
                    "value": e.value,
                })
            })
            .collect(),
    ))
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
    let status = script_status(indexer, &sh).map_err(|e| format!("status lookup failed: {e:#}"))?;
    state.status_by_scripthash.insert(sh, status.clone());
    Ok(match status {
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
    indexer: &Indexer,
    pending_changes: &PendingChangeBroadcaster,
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
            broadcaster
                .broadcast_transaction(tx.clone())
                .map_err(|e| format!("broadcast failed: {e}"))?;
            let affected = indexer
                .track_pending_transaction(&tx)
                .map_err(|e| format!("failed to cache broadcast transaction: {e:#}"))?;
            pending_changes.broadcast(affected);
            Ok(Value::String(txid.to_string()))
        }
        None => Err("transaction broadcast is only available in bridge mode".into()),
    }
}

fn handle_estimatefee(
    params: &Value,
    fee_rate: &Arc<FeeRateState>,
) -> std::result::Result<Value, String> {
    let _blocks = params.get(0).and_then(Value::as_u64).unwrap_or(6);
    // Return -1 until we have seen at least one fee estimate from nakamoto.
    match fee_rate.current_sat_per_vb() {
        Some(sat_per_vb) => {
            let btc_per_kvb = (sat_per_vb as f64) * 0.00001f64;
            Ok(json!(btc_per_kvb))
        }
        None => Ok(json!(-1)),
    }
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

fn script_status(indexer: &Indexer, sh: &ScriptHash) -> Result<Option<String>> {
    let history = indexer.get_history(sh);
    let mempool = indexer.get_mempool(sh)?;
    Ok(compute_status_hash(&history, &mempool))
}

fn compute_status_hash(history: &[TxEntry], mempool: &[MempoolEntry]) -> Option<String> {
    if history.is_empty() && mempool.is_empty() {
        return None;
    }

    let mut data = String::new();
    for entry in history {
        data.push_str(&entry.txid.to_string());
        data.push(':');
        data.push_str(&entry.height.to_string());
        data.push(':');
    }
    for entry in mempool {
        data.push_str(&entry.txid.to_string());
        data.push(':');
        data.push_str(&entry.height.to_string());
        data.push(':');
    }

    Some(sha256::Hash::hash(data.as_bytes()).to_string())
}

fn current_header_status<S: BlockSource>(
    indexer: &Indexer,
    source: &S,
) -> Result<Option<(u32, String)>> {
    let height = indexer.tip_height();
    match source.block_header(height)? {
        Some(header) => Ok(Some((height, serialize_hex(&header)))),
        None => Ok(None),
    }
}

fn wait_for_indexer_tip(indexer: &Indexer, source_event: &crate::block_source::BlockEvent) {
    use std::time::{Duration, Instant};

    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let ready = match source_event {
            crate::block_source::BlockEvent::Connected { height, .. } => {
                indexer.tip_height() >= *height
            }
            crate::block_source::BlockEvent::Disconnected { height, .. } => {
                indexer.tip_height() < *height
            }
            crate::block_source::BlockEvent::Synced { .. } => true,
        };
        if ready || Instant::now() >= deadline {
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

fn write_json(writer: &Arc<std::sync::Mutex<TcpStream>>, value: &Value) -> Result<()> {
    let mut writer = writer.lock().expect("electrum writer poisoned");
    let response_str = serde_json::to_string(value)? + "\n";
    writer.write_all(response_str.as_bytes())?;
    Ok(())
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
        let fee_rate = Arc::new(FeeRateState::new());
        let pending_changes = PendingChangeBroadcaster::default();
        let raw = r#"{"jsonrpc":"2.0","id":1,"method":"server.ping","params":[]}"#;
        let resp = dispatch_request(
            raw,
            &mut state,
            &indexer,
            &source,
            &Metrics::new(),
            None,
            &fee_rate,
            &pending_changes,
        );
        assert_eq!(resp["result"], Value::Null);
        assert_eq!(resp["id"], json!(1));
    }

    #[test]
    fn transaction_get_returns_persisted_tx_hex() {
        let dir = tempfile::tempdir().expect("temp").keep();
        let indexer = Indexer::new(dir, Metrics::new()).expect("indexer");
        let tx = Transaction {
            version: bitcoin::transaction::Version::non_standard(1),
            lock_time: bitcoin::absolute::LockTime::ZERO,
            input: vec![bitcoin::blockdata::transaction::TxIn {
                previous_output: bitcoin::OutPoint::null(),
                script_sig: bitcoin::ScriptBuf::new(),
                sequence: bitcoin::Sequence::MAX,
                witness: bitcoin::Witness::new(),
            }],
            output: vec![bitcoin::blockdata::transaction::TxOut {
                value: bitcoin::Amount::from_sat(1000),
                script_pubkey: bitcoin::ScriptBuf::from_bytes(vec![0x51]),
            }],
        };
        indexer.store_transaction(&tx).expect("store tx");

        let resp = handle_transaction_get(&json!([tx.compute_txid().to_string()]), &indexer)
            .expect("transaction get");
        assert_eq!(resp, Value::String(serialize_hex(&tx)));
    }

    #[test]
    fn transaction_get_missing_tx_fails() {
        let indexer = Indexer::new(tempfile::tempdir().expect("temp").keep(), Metrics::new())
            .expect("indexer");
        let err = handle_transaction_get(&json!(["0".repeat(64)]), &indexer)
            .expect_err("missing tx should fail");
        assert!(err.contains("not in local cache"));
    }

    #[test]
    fn block_header_and_headers_return_serialized_data() {
        use crate::block_source::{BlockEvent, BlockSource};
        use crossbeam_channel::Receiver;
        use std::collections::BTreeMap;

        #[derive(Clone)]
        struct FakeSource {
            headers: BTreeMap<u32, bitcoin::blockdata::block::Header>,
        }

        impl FakeSource {
            fn header(height: u32) -> bitcoin::blockdata::block::Header {
                bitcoin::blockdata::block::Header {
                    version: bitcoin::blockdata::block::Version::ONE,
                    prev_blockhash: bitcoin::BlockHash::all_zeros(),
                    merkle_root: bitcoin::TxMerkleNode::all_zeros(),
                    time: height,
                    bits: bitcoin::CompactTarget::from_consensus(0x1d00ffff),
                    nonce: height,
                }
            }
        }

        impl BlockSource for FakeSource {
            fn subscribe(&self) -> Receiver<BlockEvent> {
                crossbeam_channel::never()
            }
            fn tip(&self) -> anyhow::Result<(u32, bitcoin::BlockHash)> {
                let height = self.headers.keys().next_back().copied().unwrap_or(0);
                let header = self
                    .headers
                    .get(&height)
                    .copied()
                    .unwrap_or_else(|| Self::header(0));
                Ok((height, header.block_hash()))
            }
            fn block_header(
                &self,
                h: u32,
            ) -> anyhow::Result<Option<bitcoin::blockdata::block::Header>> {
                Ok(self.headers.get(&h).copied())
            }
            fn block_by_hash(
                &self,
                _hash: &bitcoin::BlockHash,
            ) -> anyhow::Result<Option<bitcoin::Block>> {
                Ok(None)
            }
        }

        let source = FakeSource {
            headers: BTreeMap::from([(1, FakeSource::header(1)), (2, FakeSource::header(2))]),
        };

        let header = handle_block_header(&json!([1]), &source).expect("block header");
        assert!(header.is_string());
        assert_eq!(header, Value::String(serialize_hex(&FakeSource::header(1))));

        let range = handle_block_headers(&json!([1, 2]), &source).expect("block headers");
        assert_eq!(range["count"], json!(2));
        assert_eq!(
            range["hex"],
            json!(format!(
                "{}{}",
                serialize_hex(&FakeSource::header(1)),
                serialize_hex(&FakeSource::header(2))
            ))
        );
    }

    #[test]
    fn block_header_missing_height_errors() {
        use crate::block_source::{BlockEvent, BlockSource};
        use crossbeam_channel::Receiver;

        struct FakeSource;
        impl BlockSource for FakeSource {
            fn subscribe(&self) -> Receiver<BlockEvent> {
                crossbeam_channel::never()
            }
            fn tip(&self) -> anyhow::Result<(u32, bitcoin::BlockHash)> {
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

        let err = handle_block_header(&json!([0]), &FakeSource).expect_err("missing header");
        assert!(err.contains("no header"));
    }

    #[test]
    fn compute_status_hash_uses_electrum_format() {
        let txid = "0".repeat(64).parse().unwrap();
        let history = vec![crate::indexer::TxEntry {
            txid,
            height: 1,
            sequence: 0,
        }];
        assert_eq!(
            compute_status_hash(&history, &[]).as_deref(),
            Some("12b132b4f9cac2ddb0a05030bf14ab07a46352fe787aa4f0e245fac197dd5b48")
        );

        let mempool = vec![crate::indexer::MempoolEntry {
            txid,
            height: 0,
            fee: 10,
        }];
        assert_ne!(
            compute_status_hash(&history, &[]),
            compute_status_hash(&history, &mempool)
        );
    }

    #[test]
    fn transaction_broadcast_returns_txid_when_supported() {
        use std::sync::{Arc, Mutex};

        #[derive(Clone, Default)]
        struct MockBroadcaster {
            seen: Arc<Mutex<Option<bitcoin::Txid>>>,
        }

        impl TransactionBroadcaster for MockBroadcaster {
            fn broadcast_transaction(&self, tx: Transaction) -> Result<(), String> {
                *self.seen.lock().unwrap() = Some(tx.compute_txid());
                Ok(())
            }
        }

        let tx = Transaction {
            version: bitcoin::transaction::Version::non_standard(1),
            lock_time: bitcoin::absolute::LockTime::ZERO,
            input: vec![bitcoin::blockdata::transaction::TxIn {
                previous_output: bitcoin::OutPoint::null(),
                script_sig: bitcoin::ScriptBuf::new(),
                sequence: bitcoin::Sequence::MAX,
                witness: bitcoin::Witness::new(),
            }],
            output: vec![bitcoin::blockdata::transaction::TxOut {
                value: bitcoin::Amount::from_sat(1000),
                script_pubkey: bitcoin::ScriptBuf::new(),
            }],
        };
        let txid = tx.compute_txid().to_string();
        let params = json!([hex::encode(bitcoin::consensus::encode::serialize(&tx))]);
        let mock = MockBroadcaster::default();
        let broadcaster: Arc<dyn TransactionBroadcaster> = Arc::new(mock.clone());
        let dir = tempfile::tempdir().expect("temp").keep();
        let indexer = Indexer::new(dir, Metrics::new()).expect("indexer");
        let pending_changes = PendingChangeBroadcaster::default();
        let resp = handle_transaction_broadcast(
            &params,
            &Metrics::new(),
            Some(&broadcaster),
            &indexer,
            &pending_changes,
        )
        .expect("broadcast");
        assert_eq!(resp, Value::String(txid));
        assert_eq!(*mock.seen.lock().unwrap(), Some(tx.compute_txid()));
        assert_eq!(
            indexer.get_transaction(&tx.compute_txid()).unwrap(),
            Some(tx)
        );
    }

    #[test]
    fn listunspent_uses_expected_fields() {
        let params = json!(["0".repeat(64)]);
        assert!(
            handle_scripthash_listunspent(
                &params,
                &Indexer::new(tempfile::tempdir().expect("temp").keep(), Metrics::new())
                    .expect("indexer")
            )
            .is_ok()
        );
    }

    #[test]
    fn estimatefee_returns_latest_fee_when_available() {
        let fee_rate = Arc::new(FeeRateState::new());
        fee_rate.update_sat_per_vb(25);
        let value = handle_estimatefee(&json!([6]), &fee_rate).expect("estimate");
        assert_eq!(value, json!(0.00025f64));
    }

    #[test]
    fn estimatefee_returns_unknown_before_first_update() {
        let fee_rate = Arc::new(FeeRateState::new());
        let value = handle_estimatefee(&json!([6]), &fee_rate).expect("estimate");
        assert_eq!(value, json!(-1));
    }

    #[test]
    fn scripthash_subscribe_records_status() {
        let params = json!(["0".repeat(64)]);
        let mut state = ClientState::new();
        let dir = tempfile::tempdir().expect("temp").keep();
        let indexer = Indexer::new(dir, Metrics::new()).expect("indexer");
        let resp = handle_scripthash_subscribe(&params, &mut state, &indexer).expect("subscribe");
        assert!(resp.is_null());
        assert_eq!(state.subscribed_scripthashes.len(), 1);
        assert_eq!(state.status_by_scripthash.len(), 1);
    }

    #[test]
    fn scripthash_get_balance_includes_pending_delta() {
        let dir = tempfile::tempdir().expect("temp").keep();
        let indexer = Indexer::new(dir, Metrics::new()).expect("indexer");
        let tx = Transaction {
            version: bitcoin::transaction::Version::non_standard(1),
            lock_time: bitcoin::absolute::LockTime::ZERO,
            input: vec![bitcoin::blockdata::transaction::TxIn {
                previous_output: bitcoin::OutPoint::null(),
                script_sig: bitcoin::ScriptBuf::new(),
                sequence: bitcoin::Sequence::MAX,
                witness: bitcoin::Witness::new(),
            }],
            output: vec![bitcoin::blockdata::transaction::TxOut {
                value: bitcoin::Amount::from_sat(900),
                script_pubkey: bitcoin::ScriptBuf::from_bytes(vec![0x51]),
            }],
        };
        indexer
            .track_pending_transaction(&tx)
            .expect("track pending");
        let sh = parse_scripthash(&json!([ScriptHash::from_script(
            &bitcoin::ScriptBuf::from_bytes(vec![0x51])
        )
        .to_hex()]))
        .expect("script hash");
        let resp = handle_scripthash_get_balance(&json!([sh.to_hex()]), &indexer).expect("balance");
        assert_eq!(resp["confirmed"], json!(0));
        assert_eq!(resp["unconfirmed"], json!(900));
    }

    #[test]
    fn scripthash_get_mempool_reports_pending_fee() {
        let dir = tempfile::tempdir().expect("temp").keep();
        let indexer = Indexer::new(dir, Metrics::new()).expect("indexer");
        let script = bitcoin::ScriptBuf::from_bytes(vec![0x51]);
        let tx = Transaction {
            version: bitcoin::transaction::Version::non_standard(1),
            lock_time: bitcoin::absolute::LockTime::ZERO,
            input: vec![bitcoin::blockdata::transaction::TxIn {
                previous_output: bitcoin::OutPoint::null(),
                script_sig: bitcoin::ScriptBuf::new(),
                sequence: bitcoin::Sequence::MAX,
                witness: bitcoin::Witness::new(),
            }],
            output: vec![bitcoin::blockdata::transaction::TxOut {
                value: bitcoin::Amount::from_sat(900),
                script_pubkey: script.clone(),
            }],
        };
        indexer
            .track_pending_transaction(&tx)
            .expect("track pending");
        let sh = ScriptHash::from_script(&script);
        let resp = handle_scripthash_get_mempool(&json!([sh.to_hex()]), &indexer).expect("mempool");
        let arr = resp.as_array().expect("array");
        assert_eq!(arr.len(), 1);
        assert_eq!(arr[0]["height"], json!(0));
        assert_eq!(arr[0]["fee"], json!(0));
        assert_eq!(arr[0]["tx_hash"], json!(tx.compute_txid().to_string()));
    }

    #[test]
    fn scripthash_listunspent_includes_pending_outputs() {
        let dir = tempfile::tempdir().expect("temp").keep();
        let indexer = Indexer::new(dir, Metrics::new()).expect("indexer");
        let script = bitcoin::ScriptBuf::from_bytes(vec![0x51]);
        let tx = Transaction {
            version: bitcoin::transaction::Version::non_standard(1),
            lock_time: bitcoin::absolute::LockTime::ZERO,
            input: vec![bitcoin::blockdata::transaction::TxIn {
                previous_output: bitcoin::OutPoint::null(),
                script_sig: bitcoin::ScriptBuf::new(),
                sequence: bitcoin::Sequence::MAX,
                witness: bitcoin::Witness::new(),
            }],
            output: vec![bitcoin::blockdata::transaction::TxOut {
                value: bitcoin::Amount::from_sat(900),
                script_pubkey: script.clone(),
            }],
        };
        indexer
            .track_pending_transaction(&tx)
            .expect("track pending");
        let sh = ScriptHash::from_script(&script);
        let resp =
            handle_scripthash_listunspent(&json!([sh.to_hex()]), &indexer).expect("listunspent");
        let arr = resp.as_array().expect("array");
        assert_eq!(arr.len(), 1);
        assert_eq!(arr[0]["height"], json!(0));
        assert_eq!(arr[0]["value"], json!(900));
    }

    #[test]
    fn pending_change_broadcaster_delivers_affected_scripts() {
        let broadcaster = PendingChangeBroadcaster::default();
        let rx = broadcaster.subscribe();
        let sh = ScriptHash::from_raw_bytes([1u8; 32]);
        broadcaster.broadcast(vec![sh]);
        let received = rx.recv().expect("pending change");
        assert_eq!(received, vec![sh]);
    }

    #[test]
    fn tx_status_reverted_restores_pending_transaction() {
        let indexer = Indexer::new(tempfile::tempdir().expect("temp").keep(), Metrics::new())
            .expect("indexer");
        let broadcaster = PendingChangeBroadcaster::default();
        let rx = broadcaster.subscribe();
        let script = bitcoin::ScriptBuf::from_bytes(vec![0x51]);
        let tx = Transaction {
            version: bitcoin::transaction::Version::non_standard(1),
            lock_time: bitcoin::absolute::LockTime::ZERO,
            input: vec![bitcoin::blockdata::transaction::TxIn {
                previous_output: bitcoin::OutPoint::null(),
                script_sig: bitcoin::ScriptBuf::new(),
                sequence: bitcoin::Sequence::MAX,
                witness: bitcoin::Witness::new(),
            }],
            output: vec![bitcoin::blockdata::transaction::TxOut {
                value: bitcoin::Amount::from_sat(900),
                script_pubkey: script.clone(),
            }],
        };
        indexer.store_transaction(&tx).expect("store");

        apply_tx_status_change(
            &indexer,
            &broadcaster,
            &tx.compute_txid().to_string(),
            "transaction has been reverted",
        )
        .expect("restore");

        let sh = ScriptHash::from_script(&script);
        assert_eq!(indexer.get_unconfirmed_balance_delta(&sh).unwrap(), 900);
        assert_eq!(rx.recv().expect("notification"), vec![sh]);
    }

    #[test]
    fn tx_status_confirmed_clears_pending_transaction() {
        let indexer = Indexer::new(tempfile::tempdir().expect("temp").keep(), Metrics::new())
            .expect("indexer");
        let broadcaster = PendingChangeBroadcaster::default();
        let rx = broadcaster.subscribe();
        let script = bitcoin::ScriptBuf::from_bytes(vec![0x51]);
        let tx = Transaction {
            version: bitcoin::transaction::Version::non_standard(1),
            lock_time: bitcoin::absolute::LockTime::ZERO,
            input: vec![bitcoin::blockdata::transaction::TxIn {
                previous_output: bitcoin::OutPoint::null(),
                script_sig: bitcoin::ScriptBuf::new(),
                sequence: bitcoin::Sequence::MAX,
                witness: bitcoin::Witness::new(),
            }],
            output: vec![bitcoin::blockdata::transaction::TxOut {
                value: bitcoin::Amount::from_sat(900),
                script_pubkey: script.clone(),
            }],
        };
        let txid = tx.compute_txid();
        indexer.store_transaction(&tx).expect("store");
        indexer.restore_pending_transaction(&txid).expect("restore");

        apply_tx_status_change(
            &indexer,
            &broadcaster,
            &txid.to_string(),
            "transaction was included in block 0000000000000000000000000000000000000000000000000000000000000000 at height 1",
        )
        .expect("forget");

        let sh = ScriptHash::from_script(&script);
        assert_eq!(indexer.get_unconfirmed_balance_delta(&sh).unwrap(), 0);
        assert!(rx.recv().map(|affected| affected == vec![sh]).unwrap());
    }

    #[test]
    fn tx_status_stale_clears_pending_transaction() {
        let indexer = Indexer::new(tempfile::tempdir().expect("temp").keep(), Metrics::new())
            .expect("indexer");
        let broadcaster = PendingChangeBroadcaster::default();
        let rx = broadcaster.subscribe();
        let script = bitcoin::ScriptBuf::from_bytes(vec![0x51]);
        let tx = Transaction {
            version: bitcoin::transaction::Version::non_standard(1),
            lock_time: bitcoin::absolute::LockTime::ZERO,
            input: vec![bitcoin::blockdata::transaction::TxIn {
                previous_output: bitcoin::OutPoint::null(),
                script_sig: bitcoin::ScriptBuf::new(),
                sequence: bitcoin::Sequence::MAX,
                witness: bitcoin::Witness::new(),
            }],
            output: vec![bitcoin::blockdata::transaction::TxOut {
                value: bitcoin::Amount::from_sat(900),
                script_pubkey: script.clone(),
            }],
        };
        let txid = tx.compute_txid();
        indexer.store_transaction(&tx).expect("store");
        indexer.restore_pending_transaction(&txid).expect("restore");

        apply_tx_status_change(
            &indexer,
            &broadcaster,
            &txid.to_string(),
            "transaction was replaced by deadbeef in block 0000000000000000000000000000000000000000000000000000000000000000",
        )
        .expect("forget");

        let sh = ScriptHash::from_script(&script);
        assert_eq!(indexer.get_unconfirmed_balance_delta(&sh).unwrap(), 0);
        assert!(rx.recv().map(|affected| affected == vec![sh]).unwrap());
    }

    #[test]
    fn tx_status_ignored_for_unconfirmed_and_acknowledged() {
        let indexer = Indexer::new(tempfile::tempdir().expect("temp").keep(), Metrics::new())
            .expect("indexer");
        let broadcaster = PendingChangeBroadcaster::default();
        let rx = broadcaster.subscribe();
        let script = bitcoin::ScriptBuf::from_bytes(vec![0x51]);
        let tx = Transaction {
            version: bitcoin::transaction::Version::non_standard(1),
            lock_time: bitcoin::absolute::LockTime::ZERO,
            input: vec![bitcoin::blockdata::transaction::TxIn {
                previous_output: bitcoin::OutPoint::null(),
                script_sig: bitcoin::ScriptBuf::new(),
                sequence: bitcoin::Sequence::MAX,
                witness: bitcoin::Witness::new(),
            }],
            output: vec![bitcoin::blockdata::transaction::TxOut {
                value: bitcoin::Amount::from_sat(900),
                script_pubkey: script.clone(),
            }],
        };
        let txid = tx.compute_txid();
        indexer.store_transaction(&tx).expect("store");
        indexer.restore_pending_transaction(&txid).expect("restore");
        let sh = ScriptHash::from_script(&script);

        apply_tx_status_change(
            &indexer,
            &broadcaster,
            &txid.to_string(),
            "transaction is unconfirmed",
        )
        .expect("ignore");
        apply_tx_status_change(
            &indexer,
            &broadcaster,
            &txid.to_string(),
            "transaction was acknowledged by peer 127.0.0.1:8333",
        )
        .expect("ignore");

        assert_eq!(indexer.get_unconfirmed_balance_delta(&sh).unwrap(), 900);
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn tx_status_unknown_string_is_ignored() {
        let indexer = Indexer::new(tempfile::tempdir().expect("temp").keep(), Metrics::new())
            .expect("indexer");
        let broadcaster = PendingChangeBroadcaster::default();
        let rx = broadcaster.subscribe();
        let script = bitcoin::ScriptBuf::from_bytes(vec![0x51]);
        let tx = Transaction {
            version: bitcoin::transaction::Version::non_standard(1),
            lock_time: bitcoin::absolute::LockTime::ZERO,
            input: vec![bitcoin::blockdata::transaction::TxIn {
                previous_output: bitcoin::OutPoint::null(),
                script_sig: bitcoin::ScriptBuf::new(),
                sequence: bitcoin::Sequence::MAX,
                witness: bitcoin::Witness::new(),
            }],
            output: vec![bitcoin::blockdata::transaction::TxOut {
                value: bitcoin::Amount::from_sat(900),
                script_pubkey: script.clone(),
            }],
        };
        let txid = tx.compute_txid();
        indexer.store_transaction(&tx).expect("store");
        indexer.restore_pending_transaction(&txid).expect("restore");
        let sh = ScriptHash::from_script(&script);

        apply_tx_status_change(
            &indexer,
            &broadcaster,
            &txid.to_string(),
            "something unexpected happened",
        )
        .expect("ignore");

        assert_eq!(indexer.get_unconfirmed_balance_delta(&sh).unwrap(), 900);
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn tx_status_invalid_txid_is_rejected() {
        let indexer = Indexer::new(tempfile::tempdir().expect("temp").keep(), Metrics::new())
            .expect("indexer");
        let broadcaster = PendingChangeBroadcaster::default();
        let err = apply_tx_status_change(
            &indexer,
            &broadcaster,
            "not-a-txid",
            "transaction has been reverted",
        )
        .expect_err("invalid txid should fail");
        assert!(err.to_string().contains("invalid txid"));
    }

    #[test]
    fn transaction_broadcast_emits_pending_change_notification() {
        use std::sync::{Arc, Mutex};

        #[derive(Clone, Default)]
        struct MockBroadcaster {
            seen: Arc<Mutex<Option<bitcoin::Txid>>>,
        }

        impl TransactionBroadcaster for MockBroadcaster {
            fn broadcast_transaction(&self, tx: Transaction) -> Result<(), String> {
                *self.seen.lock().unwrap() = Some(tx.compute_txid());
                Ok(())
            }
        }

        let tx = Transaction {
            version: bitcoin::transaction::Version::non_standard(1),
            lock_time: bitcoin::absolute::LockTime::ZERO,
            input: vec![bitcoin::blockdata::transaction::TxIn {
                previous_output: bitcoin::OutPoint::null(),
                script_sig: bitcoin::ScriptBuf::new(),
                sequence: bitcoin::Sequence::MAX,
                witness: bitcoin::Witness::new(),
            }],
            output: vec![bitcoin::blockdata::transaction::TxOut {
                value: bitcoin::Amount::from_sat(1000),
                script_pubkey: bitcoin::ScriptBuf::from_bytes(vec![0x51]),
            }],
        };
        let txid = tx.compute_txid().to_string();
        let params = json!([hex::encode(bitcoin::consensus::encode::serialize(&tx))]);
        let mock = MockBroadcaster::default();
        let broadcaster: Arc<dyn TransactionBroadcaster> = Arc::new(mock.clone());
        let dir = tempfile::tempdir().expect("temp").keep();
        let indexer = Indexer::new(dir, Metrics::new()).expect("indexer");
        let pending_changes = PendingChangeBroadcaster::default();
        let rx = pending_changes.subscribe();
        let resp = handle_transaction_broadcast(
            &params,
            &Metrics::new(),
            Some(&broadcaster),
            &indexer,
            &pending_changes,
        )
        .expect("broadcast");
        assert_eq!(resp, Value::String(txid));
        assert_eq!(*mock.seen.lock().unwrap(), Some(tx.compute_txid()));
        let affected = rx.recv().expect("pending notification");
        assert_eq!(
            affected,
            vec![ScriptHash::from_script(&bitcoin::ScriptBuf::from_bytes(
                vec![0x51]
            ))]
        );
    }

    #[test]
    fn tx_status_for_parent_notifies_descendant_scripts() {
        let indexer = Indexer::new(tempfile::tempdir().expect("temp").keep(), Metrics::new())
            .expect("indexer");
        let broadcaster = PendingChangeBroadcaster::default();
        let rx = broadcaster.subscribe();

        let parent_script = bitcoin::ScriptBuf::from_bytes(vec![0x51]);
        let child_script = bitcoin::ScriptBuf::from_bytes(vec![0x52]);
        let parent = Transaction {
            version: bitcoin::transaction::Version::non_standard(1),
            lock_time: bitcoin::absolute::LockTime::ZERO,
            input: vec![bitcoin::blockdata::transaction::TxIn {
                previous_output: bitcoin::OutPoint::null(),
                script_sig: bitcoin::ScriptBuf::new(),
                sequence: bitcoin::Sequence::MAX,
                witness: bitcoin::Witness::new(),
            }],
            output: vec![bitcoin::blockdata::transaction::TxOut {
                value: bitcoin::Amount::from_sat(1000),
                script_pubkey: parent_script.clone(),
            }],
        };
        let parent_txid = parent.compute_txid();
        let child = Transaction {
            version: bitcoin::transaction::Version::non_standard(1),
            lock_time: bitcoin::absolute::LockTime::ZERO,
            input: vec![bitcoin::blockdata::transaction::TxIn {
                previous_output: bitcoin::OutPoint::new(parent_txid, 0),
                script_sig: bitcoin::ScriptBuf::new(),
                sequence: bitcoin::Sequence::MAX,
                witness: bitcoin::Witness::new(),
            }],
            output: vec![bitcoin::blockdata::transaction::TxOut {
                value: bitcoin::Amount::from_sat(900),
                script_pubkey: child_script.clone(),
            }],
        };
        indexer.track_pending_transaction(&parent).expect("track parent");
        indexer.track_pending_transaction(&child).expect("track child");

        apply_tx_status_change(
            &indexer,
            &broadcaster,
            &parent_txid.to_string(),
            "transaction was included in block 0000000000000000000000000000000000000000000000000000000000000000 at height 1",
        )
        .expect("forget parent");

        let affected = rx.recv().expect("pending notification");
        assert!(affected.contains(&ScriptHash::from_script(&parent_script)));
        assert!(affected.contains(&ScriptHash::from_script(&child_script)));
    }

    #[test]
    fn headers_subscribe_returns_current_tip_shape() {
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

        let dir = tempfile::tempdir().expect("temp").keep();
        let indexer = Indexer::new(dir, Metrics::new()).expect("indexer");
        let mut state = ClientState::new();
        let resp = handle_headers_subscribe(&mut state, &indexer, &FakeSource).expect("headers");
        assert_eq!(resp["height"], json!(0));
    }

    #[test]
    fn headers_subscribe_receives_update_after_connected_block() {
        use crate::block_source::{BlockEvent, BlockSource};
        use crossbeam_channel::Receiver;
        use std::collections::BTreeMap;
        use std::io::ErrorKind;
        use std::net::SocketAddr;
        use std::sync::{Arc, Mutex};
        use std::time::Duration;

        #[derive(Clone, Default)]
        struct LiveSource {
            senders: Arc<Mutex<Vec<crossbeam_channel::Sender<BlockEvent>>>>,
            headers: Arc<Mutex<BTreeMap<u32, bitcoin::blockdata::block::Header>>>,
        }

        impl LiveSource {
            fn push_connected(&self, block: bitcoin::Block, height: u32) {
                let mut headers = self.headers.lock().unwrap();
                headers.insert(height, block.header);
                let event = BlockEvent::Connected { block, height };
                self.senders
                    .lock()
                    .unwrap()
                    .retain(|tx| tx.send(event.clone()).is_ok());
            }
        }

        impl BlockSource for LiveSource {
            fn subscribe(&self) -> Receiver<BlockEvent> {
                let (tx, rx) = crossbeam_channel::unbounded();
                self.senders.lock().unwrap().push(tx);
                rx
            }

            fn tip(&self) -> anyhow::Result<(u32, bitcoin::BlockHash)> {
                let headers = self.headers.lock().unwrap();
                let height = headers.keys().next_back().copied().unwrap_or(0);
                let header = headers.get(&height).copied().unwrap_or_else(|| {
                    bitcoin::blockdata::block::Header {
                        version: bitcoin::blockdata::block::Version::ONE,
                        prev_blockhash: bitcoin::BlockHash::all_zeros(),
                        merkle_root: bitcoin::TxMerkleNode::all_zeros(),
                        time: 0,
                        bits: bitcoin::CompactTarget::from_consensus(0x1d00ffff),
                        nonce: 0,
                    }
                });
                Ok((height, header.block_hash()))
            }

            fn block_header(
                &self,
                height: u32,
            ) -> anyhow::Result<Option<bitcoin::blockdata::block::Header>> {
                Ok(self.headers.lock().unwrap().get(&height).copied())
            }

            fn block_by_hash(
                &self,
                _hash: &bitcoin::BlockHash,
            ) -> anyhow::Result<Option<bitcoin::Block>> {
                Ok(None)
            }
        }

        fn start_server(source: Arc<LiveSource>) -> (SocketAddr, Arc<AtomicBool>) {
            let metrics = Metrics::new();
            let dir = tempfile::tempdir().expect("temp index dir").keep();
            let indexer = Indexer::new(dir, metrics.clone()).expect("indexer");
            let fee_rate = Arc::new(FeeRateState::new());
            let pending_changes = PendingChangeBroadcaster::default();
            let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
            let server =
                ElectrumServer::bind(addr, indexer, metrics, None, fee_rate, pending_changes)
                    .expect("bind");
            let local_addr = server.local_addr();
            let shutdown = Arc::new(AtomicBool::new(false));
            let shutdown_thread = Arc::clone(&shutdown);
            thread::spawn(move || {
                let _ = server.run(source, shutdown_thread);
            });
            (local_addr, shutdown)
        }

        let source = Arc::new(LiveSource::default());
        let (addr, shutdown) = start_server(Arc::clone(&source));

        let mut stream = TcpStream::connect_timeout(&addr, Duration::from_secs(5)).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        stream
            .write_all(
                br#"{"jsonrpc":"2.0","id":1,"method":"blockchain.headers.subscribe","params":[]}"#,
            )
            .unwrap();
        stream.write_all(b"\n").unwrap();
        let mut reader = BufReader::new(stream.try_clone().unwrap());
        let mut line = String::new();
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while line.is_empty() {
            match reader.read_line(&mut line) {
                Ok(0) => continue,
                Ok(_) => break,
                Err(err) if err.kind() == ErrorKind::WouldBlock => {
                    assert!(
                        std::time::Instant::now() < deadline,
                        "timed out waiting for response"
                    );
                    std::thread::sleep(Duration::from_millis(50));
                }
                Err(err) => panic!("failed to read response: {err}"),
            }
        }
        let initial: Value = serde_json::from_str(line.trim()).unwrap();
        assert_eq!(initial["result"]["height"], json!(0));

        let block = bitcoin::Block {
            header: bitcoin::blockdata::block::Header {
                version: bitcoin::blockdata::block::Version::ONE,
                prev_blockhash: bitcoin::BlockHash::all_zeros(),
                merkle_root: bitcoin::TxMerkleNode::all_zeros(),
                time: 1,
                bits: bitcoin::CompactTarget::from_consensus(0x1d00ffff),
                nonce: 1,
            },
            txdata: vec![],
        };
        source.push_connected(block, 1);

        line.clear();
        while line.is_empty() {
            match reader.read_line(&mut line) {
                Ok(0) => continue,
                Ok(_) => break,
                Err(err) if err.kind() == ErrorKind::WouldBlock => {
                    assert!(
                        std::time::Instant::now() < deadline,
                        "timed out waiting for notification"
                    );
                    std::thread::sleep(Duration::from_millis(50));
                }
                Err(err) => panic!("failed to read notification: {err}"),
            }
        }
        let notification: Value = serde_json::from_str(line.trim()).unwrap();
        assert_eq!(notification["method"], "blockchain.headers.subscribe");
        assert_eq!(notification["params"][0]["height"], json!(1));

        shutdown.store(true, Ordering::SeqCst);
    }

    #[test]
    fn scripthash_subscribe_reflects_pending_transaction_state() {
        let indexer = Indexer::new(tempfile::tempdir().expect("temp").keep(), Metrics::new())
            .expect("indexer");
        let mut state = ClientState::new();
        let script = bitcoin::ScriptBuf::from_bytes(vec![0x51]);
        let sh = ScriptHash::from_script(&script);
        let tx = Transaction {
            version: bitcoin::transaction::Version::non_standard(1),
            lock_time: bitcoin::absolute::LockTime::ZERO,
            input: vec![bitcoin::blockdata::transaction::TxIn {
                previous_output: bitcoin::OutPoint::null(),
                script_sig: bitcoin::ScriptBuf::new(),
                sequence: bitcoin::Sequence::MAX,
                witness: bitcoin::Witness::new(),
            }],
            output: vec![bitcoin::blockdata::transaction::TxOut {
                value: bitcoin::Amount::from_sat(1000),
                script_pubkey: script.clone(),
            }],
        };

        let empty = handle_scripthash_subscribe(&json!([sh.to_hex()]), &mut state, &indexer)
            .expect("subscribe");
        assert!(empty.is_null());

        indexer
            .track_pending_transaction(&tx)
            .expect("track pending");
        let pending = handle_scripthash_subscribe(&json!([sh.to_hex()]), &mut state, &indexer)
            .expect("subscribe pending");
        assert!(pending.is_string());
        assert_eq!(state.subscribed_scripthashes, vec![sh]);
        assert_eq!(
            state
                .status_by_scripthash
                .get(&sh)
                .and_then(|status| status.as_deref()),
            pending.as_str()
        );
    }
}
