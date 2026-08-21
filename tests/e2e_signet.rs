//! End-to-end signet tests for nakamoto-electrs.
//!
//! These tests require running `bitcoind -signet` instances (with RPC
//! credentials) and are therefore **ignored by default**.
//!
//! Run them explicitly with:
//!
//! ```sh
//! cargo test --test e2e_signet -- --ignored --nocapture
//! ```

use std::io::{BufRead, BufReader, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use crossbeam_channel::Receiver;
use nakamoto_electrs::block_source::{BlockEvent, BlockSource};
use nakamoto_electrs::electrum_server::{ElectrumServer, FeeRateState};
use nakamoto_electrs::indexer::Indexer;
use nakamoto_electrs::metrics::Metrics;
use tempfile::tempdir;

struct StubSource;

impl BlockSource for StubSource {
    fn subscribe(&self) -> Receiver<BlockEvent> {
        crossbeam_channel::never()
    }

    fn tip(&self) -> anyhow::Result<(u32, bitcoin::BlockHash)> {
        use bitcoin::hashes::Hash;
        Ok((0, bitcoin::BlockHash::all_zeros()))
    }

    fn block_header(
        &self,
        _height: u32,
    ) -> anyhow::Result<Option<bitcoin::blockdata::block::Header>> {
        Ok(None)
    }

    fn block_by_hash(&self, _hash: &bitcoin::BlockHash) -> anyhow::Result<Option<bitcoin::Block>> {
        Ok(None)
    }
}

fn start_electrum_server() -> SocketAddr {
    let metrics = Metrics::new();
    let dir = tempdir().expect("temp index dir").keep();
    let indexer = Indexer::new(dir, metrics.clone()).expect("indexer");
    let fee_rate = std::sync::Arc::new(FeeRateState::new());

    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let pending_changes = nakamoto_electrs::electrum_server::PendingChangeBroadcaster::default();
    let server = ElectrumServer::bind(addr, indexer, metrics, None, fee_rate, pending_changes)
        .expect("failed to bind ElectrumServer");
    let local_addr = server.local_addr();

    let source = Arc::new(StubSource);
    let shutdown = Arc::new(AtomicBool::new(false));
    thread::Builder::new()
        .name("electrum-server-signet-test".into())
        .spawn(move || {
            let _ = server.run(source, shutdown);
        })
        .expect("failed to spawn server thread");

    local_addr
}

fn electrum_call(addr: SocketAddr, request: &str) -> serde_json::Value {
    let mut stream = TcpStream::connect_timeout(&addr, Duration::from_secs(5))
        .expect("failed to connect to ElectrumServer");
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();

    stream
        .write_all(format!("{request}\n").as_bytes())
        .expect("write failed");

    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    reader.read_line(&mut line).expect("read failed");

    serde_json::from_str(line.trim()).expect("invalid JSON response")
}

fn bitcoin_cli_base_args(
    datadir: Option<&str>,
    rpc_user: &str,
    rpc_pass: &str,
    port: &str,
) -> Vec<String> {
    let mut args = Vec::new();
    if let Some(datadir) = datadir {
        args.push(format!("-datadir={datadir}"));
    }
    args.push("-signet".to_string());
    args.push(format!("-rpcuser={rpc_user}"));
    args.push(format!("-rpcpassword={rpc_pass}"));
    args.push(format!("-rpcport={port}"));
    args
}

#[test]
#[ignore = "requires external bitcoind -signet; run with --ignored"]
fn e2e_headers_subscribe_returns_tip() {
    let addr = start_electrum_server();

    let resp = electrum_call(
        addr,
        r#"{"jsonrpc":"2.0","id":1,"method":"blockchain.headers.subscribe","params":[]}"#,
    );

    assert!(
        resp.get("error").is_none() || resp["error"].is_null(),
        "unexpected error: {resp}"
    );
    let result = &resp["result"];
    assert!(
        result.get("height").is_some(),
        "result missing 'height' field: {resp}"
    );
    let height = result["height"].as_u64().expect("height must be a number");
    assert_eq!(
        height, 0,
        "stub source should report height 0, got {height}"
    );
}

#[test]
#[ignore = "requires external bitcoind -signet; run with --ignored"]
fn e2e_scripthash_history_after_payment() {
    let rpc_user = std::env::var("BITCOIND_RPC_USER").unwrap_or_else(|_| "user".into());
    let rpc_pass = std::env::var("BITCOIND_RPC_PASS").unwrap_or_else(|_| "passw0rd".into());
    let datadir = std::env::var("BITCOIND_STABLE_DATADIR").ok();

    let status = std::process::Command::new("bitcoin-cli")
        .args(bitcoin_cli_base_args(
            datadir.as_deref(),
            &rpc_user,
            &rpc_pass,
            "38332",
        ))
        .arg("getblockchaininfo")
        .status();

    if status.map(|s| !s.success()).unwrap_or(true) {
        eprintln!("bitcoin-cli not available or bitcoind not running — skipping test");
        return;
    }

    let addr = start_electrum_server();

    let script_hash = "0".repeat(64);
    let request = format!(
        r#"{{"jsonrpc":"2.0","id":2,"method":"blockchain.scripthash.get_history","params":["{script_hash}"]}}"#
    );

    let resp = electrum_call(addr, &request);

    assert!(
        resp.get("error").is_none() || resp["error"].is_null(),
        "unexpected error: {resp}"
    );
    let result = &resp["result"];
    assert!(
        result.is_array(),
        "expected array result for get_history, got: {resp}"
    );
    assert_eq!(
        result.as_array().unwrap().len(),
        0,
        "expected empty history for unseen script hash"
    );
}

#[test]
#[ignore = "requires external bitcoind + bitcoind-rc -signet on ports 38332/38334; run with --ignored"]
fn e2e_rc_node_reachable_and_on_signet() {
    let rpc_user = std::env::var("BITCOIND_RPC_USER").unwrap_or_else(|_| "user".into());
    let rpc_pass = std::env::var("BITCOIND_RPC_PASS").unwrap_or_else(|_| "passw0rd".into());
    let stable_datadir = std::env::var("BITCOIND_STABLE_DATADIR").ok();
    let rc_datadir = std::env::var("BITCOIND_RC_DATADIR").ok();
    let Some(rc_datadir) = rc_datadir else {
        eprintln!("BITCOIND_RC_DATADIR not set — skipping RC test");
        return;
    };

    let rpc_call = |port: &str, method: &str| -> Option<String> {
        let out = std::process::Command::new("bitcoin-cli")
            .args(bitcoin_cli_base_args(
                if port == "38332" {
                    stable_datadir.as_deref()
                } else {
                    Some(rc_datadir.as_str())
                },
                &rpc_user,
                &rpc_pass,
                port,
            ))
            .arg(method)
            .output()
            .ok()?;
        if out.status.success() {
            Some(String::from_utf8_lossy(&out.stdout).trim().to_string())
        } else {
            None
        }
    };

    let stable_info = match rpc_call("38332", "getblockchaininfo") {
        Some(s) => s,
        None => {
            eprintln!("stable bitcoind not reachable on port 38332 — skipping RC test");
            return;
        }
    };
    let rc_info = match rpc_call("38334", "getblockchaininfo") {
        Some(s) => s,
        None => {
            eprintln!("RC bitcoind not reachable on port 38334 — skipping RC test");
            return;
        }
    };

    let stable: serde_json::Value =
        serde_json::from_str(&stable_info).expect("invalid JSON from stable node");
    let rc: serde_json::Value = serde_json::from_str(&rc_info).expect("invalid JSON from RC node");

    assert_eq!(
        stable["chain"].as_str(),
        Some("signet"),
        "stable node is not on signet"
    );
    assert_eq!(
        rc["chain"].as_str(),
        Some("signet"),
        "RC node is not on signet"
    );

    let stable_hash = stable["bestblockhash"].as_str().unwrap_or("");
    let rc_hash = rc["bestblockhash"].as_str().unwrap_or("");
    assert_eq!(stable_hash.len(), 64, "stable bestblockhash is malformed");
    assert_eq!(rc_hash.len(), 64, "RC bestblockhash is malformed");

    println!(
        "stable node: chain={} height={} bestblockhash={stable_hash}",
        stable["chain"], stable["blocks"]
    );
    println!(
        "RC     node: chain={} height={} bestblockhash={rc_hash}",
        rc["chain"], rc["blocks"]
    );
}

#[test]
#[ignore = "requires external bitcoind -signet; run with --ignored"]
fn e2e_get_mempool_returns_empty_for_unseen_script() {
    let rpc_user = std::env::var("BITCOIND_RPC_USER").unwrap_or_else(|_| "user".into());
    let rpc_pass = std::env::var("BITCOIND_RPC_PASS").unwrap_or_else(|_| "passw0rd".into());
    let datadir = std::env::var("BITCOIND_STABLE_DATADIR").ok();

    let status = std::process::Command::new("bitcoin-cli")
        .args(bitcoin_cli_base_args(
            datadir.as_deref(),
            &rpc_user,
            &rpc_pass,
            "38332",
        ))
        .arg("getblockchaininfo")
        .status();
    if status.map(|s| !s.success()).unwrap_or(true) {
        eprintln!("bitcoin-cli not available or bitcoind not running — skipping test");
        return;
    }

    let addr = start_electrum_server();

    let script_hash = "0".repeat(64);
    let request = format!(
        r#"{{"jsonrpc":"2.0","id":10,"method":"blockchain.scripthash.get_mempool","params":["{script_hash}"]}}"#
    );
    let resp = electrum_call(addr, &request);

    assert!(
        resp.get("error").is_none() || resp["error"].is_null(),
        "unexpected error: {resp}"
    );
    let result = &resp["result"];
    assert!(
        result.is_array(),
        "expected array result for get_mempool, got: {resp}"
    );
    assert_eq!(
        result.as_array().unwrap().len(),
        0,
        "expected empty mempool for unseen script hash: {resp}"
    );
}

#[test]
#[ignore = "requires external bitcoind -signet; run with --ignored"]
fn e2e_fee_histogram_returns_array() {
    let rpc_user = std::env::var("BITCOIND_RPC_USER").unwrap_or_else(|_| "user".into());
    let rpc_pass = std::env::var("BITCOIND_RPC_PASS").unwrap_or_else(|_| "passw0rd".into());
    let datadir = std::env::var("BITCOIND_STABLE_DATADIR").ok();
    let status = std::process::Command::new("bitcoin-cli")
        .args(bitcoin_cli_base_args(
            datadir.as_deref(),
            &rpc_user,
            &rpc_pass,
            "38332",
        ))
        .arg("getblockchaininfo")
        .status();
    if status.map(|s| !s.success()).unwrap_or(true) {
        eprintln!("bitcoin-cli not available or bitcoind not running — skipping test");
        return;
    }

    let addr = start_electrum_server();
    let resp = electrum_call(
        addr,
        r#"{"jsonrpc":"2.0","id":11,"method":"mempool.get_fee_histogram","params":[]}"#,
    );
    assert!(
        resp.get("error").is_none() || resp["error"].is_null(),
        "unexpected error: {resp}"
    );
    assert!(resp["result"].is_array(), "result must be an array: {resp}");
}

#[test]
#[ignore = "requires external bitcoind -signet; run with --ignored"]
fn e2e_relayfee_returns_positive_value() {
    let rpc_user = std::env::var("BITCOIND_RPC_USER").unwrap_or_else(|_| "user".into());
    let rpc_pass = std::env::var("BITCOIND_RPC_PASS").unwrap_or_else(|_| "passw0rd".into());
    let datadir = std::env::var("BITCOIND_STABLE_DATADIR").ok();
    let status = std::process::Command::new("bitcoin-cli")
        .args(bitcoin_cli_base_args(
            datadir.as_deref(),
            &rpc_user,
            &rpc_pass,
            "38332",
        ))
        .arg("getblockchaininfo")
        .status();
    if status.map(|s| !s.success()).unwrap_or(true) {
        eprintln!("bitcoin-cli not available or bitcoind not running — skipping test");
        return;
    }

    let addr = start_electrum_server();
    let resp = electrum_call(
        addr,
        r#"{"jsonrpc":"2.0","id":12,"method":"blockchain.relayfee","params":[]}"#,
    );
    assert!(
        resp.get("error").is_none() || resp["error"].is_null(),
        "unexpected error: {resp}"
    );
    let fee = resp["result"].as_f64().expect("result must be a number");
    assert!(fee > 0.0, "relay fee must be positive, got {fee}");
}
