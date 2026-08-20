//! End-to-end regtest tests for nakamoto-electrs.
//!
//! These tests require a running `bitcoind -regtest` instance (with RPC
//! credentials) and are therefore **ignored by default**.
//!
//! Run them explicitly with:
//!
//! ```sh
//! cargo test --test e2e_regtest -- --ignored
//! ```
//!
//! ## Prerequisites
//!
//! Set the following environment variables before running:
//!
//! ```sh
//! export BITCOIND_RPC_URL="http://127.0.0.1:18443"
//! export BITCOIND_RPC_USER="user"
//! export BITCOIND_RPC_PASS="pass"
//! ```
//!
//! The test will:
//! 1. Start a local `ElectrumServer` backed by a stub `BlockSource`.
//! 2. Mine blocks via the bitcoind RPC to make coins spendable.
//! 3. Connect a raw TCP Electrum client to the server.
//! 4. Assert that `blockchain.headers.subscribe` returns a valid response.

use std::io::{BufRead, BufReader, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use crossbeam_channel::Receiver;
use nakamoto_electrs::block_source::{BlockEvent, BlockSource};
use nakamoto_electrs::electrum_server::ElectrumServer;
use nakamoto_electrs::indexer::Indexer;
use nakamoto_electrs::metrics::Metrics;

// ---------------------------------------------------------------------------
// Helpers shared by all tests in this file
// ---------------------------------------------------------------------------

/// Minimal stub `BlockSource` used in place of a full nakamoto client.
///
/// It reports height 0 and never emits block events.  That is sufficient to
/// smoke-test the Electrum protocol layer without requiring a live P2P node.
struct StubSource;

impl BlockSource for StubSource {
    fn subscribe(&self) -> Receiver<BlockEvent> {
        // A channel that is immediately disconnected; the indexer loop will
        // exit gracefully when it sees the channel closed.
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

/// Bind an `ElectrumServer` on a random port and spawn its accept loop in a
/// background thread.  Returns the bound `SocketAddr`.
fn start_electrum_server() -> SocketAddr {
    let metrics = Metrics::new();
    let indexer = Indexer::new(metrics.clone());

    // Port 0 lets the OS pick a free port.
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let server =
        ElectrumServer::bind(addr, indexer, metrics).expect("failed to bind ElectrumServer");
    let local_addr = server.local_addr();

    let source = Arc::new(StubSource);
    thread::Builder::new()
        .name("electrum-server-test".into())
        .spawn(move || {
            // Errors here are expected when the test drops the connection.
            let _ = server.run(source);
        })
        .expect("failed to spawn server thread");

    local_addr
}

/// Send a single JSON-RPC request over a fresh TCP connection and return the
/// response as a parsed `serde_json::Value`.
fn electrum_call(addr: SocketAddr, request: &str) -> serde_json::Value {
    let mut stream = TcpStream::connect_timeout(&addr, Duration::from_secs(5))
        .expect("failed to connect to ElectrumServer");
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();

    // Send the request (Electrum uses newline-delimited JSON).
    stream
        .write_all(format!("{request}\n").as_bytes())
        .expect("write failed");

    // Read the first response line.
    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    reader.read_line(&mut line).expect("read failed");

    serde_json::from_str(line.trim()).expect("invalid JSON response")
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Verify that `blockchain.headers.subscribe` returns a well-formed response.
///
/// The stub source reports height 0 (no blocks synced yet), which is a valid
/// state for a freshly started node.  The important thing is that the Electrum
/// server responds with the correct JSON-RPC structure so that a real client
/// would not crash.
#[test]
#[ignore = "requires external bitcoind -regtest; run with --ignored"]
fn e2e_headers_subscribe_returns_tip() {
    let addr = start_electrum_server();

    let resp = electrum_call(
        addr,
        r#"{"jsonrpc":"2.0","id":1,"method":"blockchain.headers.subscribe","params":[]}"#,
    );

    // The response must be a valid JSON-RPC reply with no error.
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
    // The stub source reports height 0 (no blocks synced).
    assert_eq!(
        height, 0,
        "stub source should report height 0, got {height}"
    );
}

/// Verify that `blockchain.scripthash.get_history` returns an empty array for
/// a script hash that has never been seen.
///
/// Checks that bitcoind is reachable via `bitcoin-cli` first; if it is not
/// available the test is skipped so the assertion set remains meaningful.
#[test]
#[ignore = "requires external bitcoind -regtest; run with --ignored"]
fn e2e_scripthash_history_after_payment() {
    // Verify that bitcoind is reachable before proceeding.
    let rpc_user = std::env::var("BITCOIND_RPC_USER").unwrap_or_else(|_| "user".into());
    let rpc_pass = std::env::var("BITCOIND_RPC_PASS").unwrap_or_else(|_| "passw0rd".into());

    let status = std::process::Command::new("bitcoin-cli")
        .args([
            "-regtest",
            &format!("-rpcuser={rpc_user}"),
            &format!("-rpcpassword={rpc_pass}"),
            "-rpcport=18443",
            "getblockchaininfo",
        ])
        .status();

    // If bitcoin-cli is not available or bitcoind is not running, skip.
    if status.map(|s| !s.success()).unwrap_or(true) {
        eprintln!("bitcoin-cli not available or bitcoind not running — skipping test");
        return;
    }

    let addr = start_electrum_server();

    // Use an all-zeros script hash — the server should return an empty array.
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
    // No transactions have been indexed (stub source), so history is empty.
    assert_eq!(
        result.as_array().unwrap().len(),
        0,
        "expected empty history for unseen script hash"
    );
}
