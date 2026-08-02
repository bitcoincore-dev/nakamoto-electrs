use anyhow::{anyhow, Context, Result};
use clap::{Parser, ValueEnum};
use bitcoin::consensus::encode::serialize_hex;
use bitcoin::{Address, Block, BlockHash, Network, OutPoint, Script, Txid};
use crossbeam_channel::{select, Receiver};
use nakamoto_client::handle::Handle as _;
use nakamoto_client::{Client, Config, Network as NakamotoNetwork};
use nakamoto_net_poll::Reactor;
use serde::Serialize;
use std::collections::{BTreeMap, HashMap};
use std::io::{BufRead, BufReader, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::{Arc, Mutex, RwLock};
use std::thread;
use std::time::Duration;
use tracing::{debug, error, info, warn};
use tracing::level_filters::LevelFilter;
use tracing_subscriber::FmtSubscriber;

type NodeReactor = Reactor<TcpStream>;

#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Cli {
    #[arg(long, value_enum, default_value_t = LogLevel::Info)]
    logging: LogLevel,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum)]
enum LogLevel {
    Error,
    Warn,
    Info,
    Debug,
    Trace,
}

impl From<LogLevel> for LevelFilter {
    fn from(level: LogLevel) -> Self {
        match level {
            LogLevel::Error => LevelFilter::ERROR,
            LogLevel::Warn => LevelFilter::WARN,
            LogLevel::Info => LevelFilter::INFO,
            LogLevel::Debug => LevelFilter::DEBUG,
            LogLevel::Trace => LevelFilter::TRACE,
        }
    }
}

#[derive(Clone, Debug, Serialize)]
struct UtxoView {
    outpoint: String,
    txid: String,
    vout: u32,
    value_sat: u64,
    address: String,
    script_pubkey: String,
}

#[derive(Clone, Debug, Serialize)]
struct TxInputView {
    previous_output: String,
    value_sat: Option<u64>,
    address: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
struct TxOutputView {
    outpoint: String,
    vout: u32,
    value_sat: u64,
    address: Option<String>,
    script_pubkey: String,
}

#[derive(Clone, Debug, Serialize)]
struct TxView {
    txid: String,
    block_hash: String,
    height: u64,
    version: i32,
    lock_time: u32,
    raw_hex: String,
    inputs: Vec<TxInputView>,
    outputs: Vec<TxOutputView>,
}

#[derive(Clone, Debug, Serialize, Default)]
struct AddressView {
    address: String,
    balance_sat: u64,
    received_sat: u64,
    spent_sat: u64,
    utxos: Vec<UtxoView>,
}

#[derive(Clone, Debug, Serialize)]
struct BlockView {
    hash: String,
    prev_hash: String,
    height: u64,
    tx_count: usize,
    txids: Vec<String>,
}

#[derive(Clone, Debug, Serialize)]
struct TipView {
    height: u64,
    hash: String,
}

#[derive(Clone)]
struct TxDelta {
    txid: Txid,
    created: Vec<UtxoRecord>,
    spent: Vec<UtxoRecord>,
}

#[derive(Clone)]
struct BlockDelta {
    hash: BlockHash,
    height: u64,
    prev_hash: BlockHash,
    txs: Vec<TxDelta>,
}

#[derive(Clone)]
struct UtxoRecord {
    txid: Txid,
    vout: u32,
    value_sat: u64,
    address: String,
    script_pubkey: String,
}

impl UtxoRecord {
    fn outpoint(&self) -> String {
        format!("{}:{}", self.txid, self.vout)
    }

    fn view(&self) -> UtxoView {
        UtxoView {
            outpoint: self.outpoint(),
            txid: self.txid.to_string(),
            vout: self.vout,
            value_sat: self.value_sat,
            address: self.address.clone(),
            script_pubkey: self.script_pubkey.clone(),
        }
    }
}

#[derive(Default)]
struct AddressRecord {
    balance_sat: u64,
    received_sat: u64,
    spent_sat: u64,
    utxos: HashMap<OutPoint, UtxoRecord>,
}

struct ChainState {
    network: Network,
    tip: Option<TipView>,
    blocks: HashMap<BlockHash, BlockView>,
    block_deltas: HashMap<BlockHash, BlockDelta>,
    txs: HashMap<Txid, TxView>,
    utxos: HashMap<OutPoint, UtxoRecord>,
    addresses: HashMap<String, AddressRecord>,
    next_height: u64,
    pending: BTreeMap<u64, (BlockHash, Block)>,
}

impl Default for ChainState {
    fn default() -> Self {
        Self {
            network: Network::Testnet,
            tip: None,
            blocks: HashMap::new(),
            block_deltas: HashMap::new(),
            txs: HashMap::new(),
            utxos: HashMap::new(),
            addresses: HashMap::new(),
            next_height: 0,
            pending: BTreeMap::new(),
        }
    }
}

impl ChainState {
    fn new(network: Network) -> Self {
        Self {
            network,
            tip: None,
            blocks: HashMap::new(),
            block_deltas: HashMap::new(),
            txs: HashMap::new(),
            utxos: HashMap::new(),
            addresses: HashMap::new(),
            next_height: 0,
            pending: BTreeMap::new(),
        }
    }

    fn next_missing_height(&self) -> u64 {
        self.next_height
    }

    fn current_tip(&self) -> Option<TipView> {
        self.tip.clone()
    }

    fn block(&self, hash: &BlockHash) -> Option<BlockView> {
        self.blocks.get(hash).cloned()
    }

    fn tx(&self, txid: &Txid) -> Option<TxView> {
        self.txs.get(txid).cloned()
    }

    fn address(&self, address: &str) -> Option<AddressView> {
        let record = self.addresses.get(address)?;
        Some(AddressView {
            address: address.to_string(),
            balance_sat: record.balance_sat,
            received_sat: record.received_sat,
            spent_sat: record.spent_sat,
            utxos: record.utxos.values().map(UtxoRecord::view).collect(),
        })
    }

    fn ingest(&mut self, height: u64, block: Block) {
        let hash = block.block_hash();
        if self.blocks.contains_key(&hash) || self.block_deltas.contains_key(&hash) {
            return;
        }
        self.pending.insert(height, (hash, block));
        self.drain_pending();
    }

    fn disconnect(&mut self, hash: &BlockHash) {
        if let Some(delta) = self.block_deltas.remove(hash) {
            self.revert_block(delta);
            self.drain_pending();
        }
    }

    fn drain_pending(&mut self) {
        while let Some((hash, block)) = self.pending.remove(&self.next_height) {
            let height = self.next_height;
            self.apply_block(height, hash, block);
            self.next_height += 1;
        }
    }

    fn apply_block(&mut self, height: u64, hash: BlockHash, block: Block) {
        let prev_hash = block.header.prev_blockhash;
        let mut tx_deltas = Vec::with_capacity(block.txdata.len());
        let mut txids = Vec::with_capacity(block.txdata.len());

        for tx in block.txdata {
            let txid = tx.txid();
            let mut spent = Vec::new();
            let mut created = Vec::new();
            let mut spent_inputs = Vec::new();

            for input in &tx.input {
                if input.previous_output.is_null() {
                    spent_inputs.push(TxInputView {
                        previous_output: input.previous_output.to_string(),
                        value_sat: None,
                        address: None,
                    });
                    continue;
                }
                if let Some(utxo) = self.utxos.remove(&input.previous_output) {
                    if let Some(address) = self.addresses.get_mut(&utxo.address) {
                        address.balance_sat = address.balance_sat.saturating_sub(utxo.value_sat);
                        address.spent_sat = address.spent_sat.saturating_add(utxo.value_sat);
                        address.utxos.remove(&input.previous_output);
                    }
                    spent_inputs.push(TxInputView {
                        previous_output: input.previous_output.to_string(),
                        value_sat: Some(utxo.value_sat),
                        address: Some(utxo.address.clone()),
                    });
                    spent.push(utxo);
                } else {
                    spent_inputs.push(TxInputView {
                        previous_output: input.previous_output.to_string(),
                        value_sat: None,
                        address: None,
                    });
                }
            }

            for (vout, output) in tx.output.iter().enumerate() {
                if let Some(address) = address_from_script(&output.script_pubkey, self.network) {
                    let outpoint = OutPoint::new(txid, vout as u32);
                    let record = UtxoRecord {
                        txid,
                        vout: vout as u32,
                        value_sat: output.value,
                        address: address.clone(),
                        script_pubkey: hex_bytes(output.script_pubkey.as_bytes()),
                    };
                    self.utxos.insert(outpoint, record.clone());
                    let entry = self.addresses.entry(address).or_default();
                    entry.balance_sat = entry.balance_sat.saturating_add(output.value);
                    entry.received_sat = entry.received_sat.saturating_add(output.value);
                    entry.utxos.insert(outpoint, record.clone());
                    created.push(record);
                }
            }

            let inputs = tx
                .input
                .iter()
                .zip(spent_inputs.into_iter())
                .map(|(_, view)| view)
                .collect();

            let outputs = tx
                .output
                .iter()
                .enumerate()
                .map(|(vout, output)| {
                    let outpoint = OutPoint::new(txid, vout as u32).to_string();
                    TxOutputView {
                        outpoint,
                        vout: vout as u32,
                        value_sat: output.value,
                        address: address_from_script(&output.script_pubkey, self.network),
                        script_pubkey: hex_bytes(output.script_pubkey.as_bytes()),
                    }
                })
                .collect();

            self.txs.insert(
                txid,
                TxView {
                    txid: txid.to_string(),
                    block_hash: hash.to_string(),
                    height,
                    version: tx.version,
                    lock_time: u32::from(tx.lock_time),
                    raw_hex: serialize_hex(&tx),
                    inputs,
                    outputs,
                },
            );
            txids.push(txid.to_string());
            tx_deltas.push(TxDelta { txid, created, spent });
        }

        self.blocks.insert(
            hash,
            BlockView {
                hash: hash.to_string(),
                prev_hash: prev_hash.to_string(),
                height,
                tx_count: txids.len(),
                txids,
            },
        );
        self.block_deltas.insert(
            hash,
            BlockDelta {
                hash,
                height,
                prev_hash,
                txs: tx_deltas,
            },
        );
        self.tip = Some(TipView {
            height,
            hash: hash.to_string(),
        });
        self.next_height = height.saturating_add(1);
    }

    fn revert_block(&mut self, delta: BlockDelta) {
        for tx in delta.txs.into_iter().rev() {
            self.txs.remove(&tx.txid);
            for created in tx.created.into_iter().rev() {
                let outpoint = OutPoint::new(created.txid, created.vout);
                self.utxos.remove(&outpoint);
                if let Some(address) = self.addresses.get_mut(&created.address) {
                    address.balance_sat = address.balance_sat.saturating_sub(created.value_sat);
                    address.received_sat = address.received_sat.saturating_sub(created.value_sat);
                    address.utxos.remove(&outpoint);
                }
            }
            for spent in tx.spent.into_iter().rev() {
                let outpoint = OutPoint::new(spent.txid, spent.vout);
                self.utxos.insert(outpoint, spent.clone());
                let address = self.addresses.entry(spent.address.clone()).or_default();
                address.balance_sat = address.balance_sat.saturating_add(spent.value_sat);
                address.spent_sat = address.spent_sat.saturating_sub(spent.value_sat);
                address.utxos.insert(outpoint, spent);
            }
        }
        self.blocks.remove(&delta.hash);
        self.tip = if delta.height == 0 {
            None
        } else {
            self.blocks.get(&delta.prev_hash).map(|prev| TipView {
                height: prev.height,
                hash: prev.hash.clone(),
            })
        };
        self.next_height = delta.height;
    }

}

pub fn run() -> Result<()> {
    let cli = Cli::parse();
    let subscriber = FmtSubscriber::builder()
        .with_max_level(cli.logging)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    let network = match std::env::var("NAKAMOTO_NETWORK").as_deref() {
        Ok("bitcoin") => Network::Bitcoin,
        Ok("regtest") => Network::Regtest,
        Ok("signet") => Network::Signet,
        _ => Network::Testnet,
    };

    let api_addr: SocketAddr = std::env::var("NAKAMOTO_API_ADDR")
        .ok()
        .as_deref()
        .map(SocketAddr::from_str)
        .transpose()?
        .unwrap_or_else(|| SocketAddr::from(([127, 0, 0, 1], 38301)));

    let mut config = Config::new(NakamotoNetwork::from(network));
    config.root = PathBuf::from(std::env::var("NAKAMOTO_ROOT").unwrap_or_else(|_| ".nakamoto-electrs".into()));
    config.listen = vec![];

    let client = Client::<NodeReactor>::new().context("failed to create Nakamoto client")?;
    let handle = client.handle();
    let block_rx = handle.blocks();
    let events = handle.events();

    let state = Arc::new(RwLock::new(ChainState::new(network)));

    let state_for_index = Arc::clone(&state);
    let handle_for_index = handle.clone();
    let indexer = thread::spawn(move || {
        if let Err(err) = index_chain(handle_for_index, block_rx, events, state_for_index) {
            error!("indexer stopped: {err:?}");
        }
    });

    let state_for_server = Arc::clone(&state);
    let server = thread::spawn(move || {
        if let Err(err) = run_http_server(api_addr, state_for_server) {
            error!("http server stopped: {err:?}");
        }
    });

    let runner = thread::spawn(move || {
        if let Err(err) = client.run(config) {
            error!("nakamoto client exited: {err:?}");
        }
    });

    let _ = runner.join();
    let _ = indexer.join();
    let _ = server.join();

    Ok(())
}

fn index_chain<H>(
    handle: H,
    block_rx: Receiver<(Block, u64)>,
    events: Receiver<nakamoto_client::Event>,
    state: Arc<RwLock<ChainState>>,
) -> Result<()>
where
    H: nakamoto_client::handle::Handle + Clone + Send + Sync + 'static,
{
    let state_for_sync = Arc::clone(&state);
    let handle_for_sync = handle.clone();
    thread::spawn(move || {
        loop {
            if let Err(err) = request_missing_blocks(&handle_for_sync, &state_for_sync) {
                warn!(target: "indexer", "sync request failed: {err:?}");
            }
            thread::sleep(Duration::from_secs(1));
        }
    });

    loop {
        select! {
            recv(block_rx) -> result => match result {
                Ok((block, height)) => {
                    let hash = block.block_hash();
                    info!(target: "indexer", %hash, height, "indexed block");
                    state.write().unwrap().ingest(height, block);
                }
                Err(_) => return Err(anyhow!("block receiver disconnected")),
            },
            recv(events) -> result => match result {
                Ok(event) => {
                    match event {
                        nakamoto_client::Event::Ready { tip, .. } => {
                            info!(target: "node", tip, "node ready");
                        }
                        nakamoto_client::Event::BlockConnected { hash, height, .. } => {
                            debug!(target: "node", %hash, height, "block connected");
                            if let Err(err) = handle.get_block(&hash) {
                                warn!(target: "node", %hash, ?err, "failed to request block");
                            }
                        }
                        nakamoto_client::Event::BlockDisconnected { hash, height, .. } => {
                            debug!(target: "node", %hash, height, "block disconnected");
                            state.write().unwrap().disconnect(&hash);
                        }
                        nakamoto_client::Event::BlockMatched { hash, header, height, transactions } => {
                            let block = Block { header, txdata: transactions };
                            info!(target: "indexer", %hash, height, "matched block");
                            state.write().unwrap().ingest(height, block);
                        }
                        _ => {}
                    }
                }
                Err(_) => return Err(anyhow!("event receiver disconnected")),
            },
            default(Duration::from_secs(1)) => (),
        }
    }
}

fn request_missing_blocks<H>(handle: &H, state: &Arc<RwLock<ChainState>>) -> Result<()>
where
    H: nakamoto_client::handle::Handle,
{
    let headers = collect_headers(handle)?;
    let next_height = state.read().unwrap().next_missing_height();
    for (height, hash) in headers.into_iter().filter(|(height, _)| *height >= next_height) {
        if height == 0 {
            continue;
        }
        handle.get_block(&hash).context("failed to request block")?;
    }
    Ok(())
}

fn collect_headers<H>(handle: &H) -> Result<Vec<(u64, BlockHash)>>
where
    H: nakamoto_client::handle::Handle,
{
    let headers = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&headers);
    handle.query_tree(move |tree| {
        let mut collected = Vec::new();
        for (height, header) in tree.iter() {
            collected.push((height, header.block_hash()));
        }
        *sink.lock().unwrap() = collected;
    })?;
    Ok(headers.lock().unwrap().clone())
}

fn run_http_server(addr: SocketAddr, state: Arc<RwLock<ChainState>>) -> Result<()> {
    let listener = TcpListener::bind(addr).with_context(|| format!("failed to bind {addr}"))?;
    info!(target: "api", %addr, "serving query API");
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let state = Arc::clone(&state);
                thread::spawn(move || {
                    if let Err(err) = handle_http(stream, state) {
                        warn!(target: "api", "request failed: {err:?}");
                    }
                });
            }
            Err(err) => warn!(target: "api", "accept failed: {err}"),
        }
    }
    Ok(())
}

fn handle_http(mut stream: TcpStream, state: Arc<RwLock<ChainState>>) -> Result<()> {
    let request_line = {
        let mut reader = BufReader::new(stream.try_clone()?);
        let mut line = String::new();
        reader.read_line(&mut line)?;
        line
    };

    let path = request_line
        .split_whitespace()
        .nth(1)
        .ok_or_else(|| anyhow!("malformed request line"))?;

    let (status, body) = route(path, &state)?;
    write_response(&mut stream, status, &body)?;
    Ok(())
}

fn route(path: &str, state: &Arc<RwLock<ChainState>>) -> Result<(u16, String)> {
    let parts: Vec<&str> = path.trim_start_matches('/').split('/').collect();
    let (status, json) = match parts.as_slice() {
        ["health"] | ["tip"] => {
            let tip = state.read().unwrap().current_tip();
        (
            200,
            serde_json::to_string(&serde_json::json!({
                "tip": tip,
            }))?,
        )
    }
    ["block", hash] => {
        let hash = BlockHash::from_str(hash).context("invalid block hash")?;
        let block = state.read().unwrap().block(&hash).ok_or_else(|| anyhow!("block not found"))?;
        (200, serde_json::to_string(&block)?)
    }
    ["tx", txid] => {
        let txid = Txid::from_str(txid).context("invalid txid")?;
        let tx = state.read().unwrap().tx(&txid).ok_or_else(|| anyhow!("tx not found"))?;
        (200, serde_json::to_string(&tx)?)
    }
    ["address", address] => {
        let address = Address::from_str(address).context("invalid address")?;
        let address = address.to_string();
        let view = state.read().unwrap().address(&address).unwrap_or_else(|| AddressView {
            address,
            ..Default::default()
        });
        (200, serde_json::to_string(&view)?)
    }
    _ => (
        404,
        serde_json::to_string(&serde_json::json!({
            "error": "not found",
        }))?,
    ),
    };
    Ok((status, json))
}

fn write_response(stream: &mut TcpStream, status: u16, body: &str) -> Result<()> {
    let status_text = match status {
        200 => "OK",
        404 => "Not Found",
        500 => "Internal Server Error",
        _ => "OK",
    };
    let response = format!(
        "HTTP/1.1 {status} {status_text}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        body.len(),
        body
    );
    stream.write_all(response.as_bytes())?;
    stream.flush()?;
    Ok(())
}

fn address_from_script(script: &Script, network: Network) -> Option<String> {
    Address::from_script(script, network).ok().map(|addr| addr.to_string())
}

fn hex_bytes(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(&mut s, "{:02x}", byte);
    }
    s
}
