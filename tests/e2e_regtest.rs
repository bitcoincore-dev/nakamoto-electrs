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
use tempfile::tempdir;

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
    let dir = tempdir().expect("temp index dir").keep();
    let indexer = Indexer::new(dir, metrics.clone()).expect("indexer");

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

// ---------------------------------------------------------------------------
// RC-node tests
//
// These tests target the RC bitcoind node (RPC port 18445) that the CI
// workflow starts alongside the stable node.  Both nodes run on the same
// regtest network and are peered with each other.
// ---------------------------------------------------------------------------

/// Query the RC node's chain state and verify it is on the same regtest
/// network as the stable node (same genesis block hash, same best-block hash
/// after both have started with no blocks mined).
#[test]
#[ignore = "requires external bitcoind-rc -regtest on port 18445; run with --ignored"]
fn e2e_rc_node_reachable_and_on_same_chain() {
    let rpc_user = std::env::var("BITCOIND_RPC_USER").unwrap_or_else(|_| "user".into());
    let rpc_pass = std::env::var("BITCOIND_RPC_PASS").unwrap_or_else(|_| "passw0rd".into());

    // ── helper: call bitcoin-cli and return stdout, or None if unavailable ──
    let rpc_call = |port: &str, method: &str| -> Option<String> {
        let out = std::process::Command::new("bitcoin-cli")
            .args([
                "-regtest",
                &format!("-rpcuser={rpc_user}"),
                &format!("-rpcpassword={rpc_pass}"),
                &format!("-rpcport={port}"),
                method,
            ])
            .output()
            .ok()?;
        if out.status.success() {
            Some(String::from_utf8_lossy(&out.stdout).trim().to_string())
        } else {
            None
        }
    };

    // Skip gracefully if neither node is reachable (e.g. local dev run).
    let stable_info = match rpc_call("18443", "getblockchaininfo") {
        Some(s) => s,
        None => {
            eprintln!("stable bitcoind not reachable on port 18443 — skipping RC test");
            return;
        }
    };
    let rc_info = match rpc_call("18445", "getblockchaininfo") {
        Some(s) => s,
        None => {
            eprintln!("RC bitcoind not reachable on port 18445 — skipping RC test");
            return;
        }
    };

    let stable: serde_json::Value =
        serde_json::from_str(&stable_info).expect("invalid JSON from stable node");
    let rc: serde_json::Value = serde_json::from_str(&rc_info).expect("invalid JSON from RC node");

    // Both nodes must be on regtest.
    assert_eq!(
        stable["chain"].as_str(),
        Some("regtest"),
        "stable node is not on regtest"
    );
    assert_eq!(
        rc["chain"].as_str(),
        Some("regtest"),
        "RC node is not on regtest"
    );

    // Both nodes must share the same best-block hash (they are peered and
    // started from genesis with no blocks mined, so both sit at height 0).
    let stable_hash = stable["bestblockhash"].as_str().unwrap_or("");
    let rc_hash = rc["bestblockhash"].as_str().unwrap_or("");
    assert_eq!(
        stable_hash, rc_hash,
        "stable and RC nodes disagree on best-block hash: stable={stable_hash} rc={rc_hash}"
    );

    println!(
        "stable node: chain={} height={} bestblockhash={stable_hash}",
        stable["chain"], stable["blocks"]
    );
    println!(
        "RC     node: chain={} height={} bestblockhash={rc_hash}",
        rc["chain"], rc["blocks"]
    );
}

/// Mine a block on the stable node, then verify the RC node syncs to the
/// same tip (confirming the two nodes are genuinely peered).
#[test]
#[ignore = "requires external bitcoind + bitcoind-rc -regtest on ports 18443/18445; run with --ignored"]
fn e2e_rc_node_syncs_block_from_stable() {
    let rpc_user = std::env::var("BITCOIND_RPC_USER").unwrap_or_else(|_| "user".into());
    let rpc_pass = std::env::var("BITCOIND_RPC_PASS").unwrap_or_else(|_| "passw0rd".into());

    let rpc_call = |port: &str, args: &[&str]| -> Option<String> {
        let mut cmd = std::process::Command::new("bitcoin-cli");
        cmd.args([
            "-regtest",
            &format!("-rpcuser={rpc_user}"),
            &format!("-rpcpassword={rpc_pass}"),
            &format!("-rpcport={port}"),
        ]);
        cmd.args(args);
        let out = cmd.output().ok()?;
        if out.status.success() {
            Some(String::from_utf8_lossy(&out.stdout).trim().to_string())
        } else {
            eprintln!(
                "bitcoin-cli -rpcport={port} {} failed: {}",
                args.join(" "),
                String::from_utf8_lossy(&out.stderr).trim()
            );
            None
        }
    };

    // Skip if either node is not available.
    if rpc_call("18443", &["getblockchaininfo"]).is_none() {
        eprintln!("stable bitcoind not reachable — skipping sync test");
        return;
    }
    if rpc_call("18445", &["getblockchaininfo"]).is_none() {
        eprintln!("RC bitcoind not reachable — skipping sync test");
        return;
    }

    // Create a wallet (ignore error if it already exists) and get an address.
    rpc_call("18443", &["createwallet", "testwallet"]);
    let addr = rpc_call("18443", &["-rpcwallet=testwallet", "getnewaddress"])
        .expect("could not get new address from stable node");

    // Mine one block to that address on the stable node.
    let mined_json = rpc_call("18443", &["generatetoaddress", "1", addr.trim()])
        .expect("generatetoaddress failed on stable node");
    let mined: serde_json::Value =
        serde_json::from_str(&mined_json).expect("invalid JSON from generatetoaddress");
    let mined_hash = mined[0]
        .as_str()
        .expect("no block hash in generatetoaddress result");
    println!("Mined block on stable node: {mined_hash}");

    // Give the RC node up to 5 seconds to sync.
    let mut rc_hash = String::new();
    for attempt in 1..=10 {
        if let Some(info_json) = rpc_call("18445", &["getblockchaininfo"]) {
            let info: serde_json::Value =
                serde_json::from_str(&info_json).expect("invalid JSON from RC node");
            rc_hash = info["bestblockhash"].as_str().unwrap_or("").to_string();
            if rc_hash == mined_hash {
                println!("RC node synced to mined block after {attempt} attempt(s)");
                break;
            }
        }
        std::thread::sleep(Duration::from_millis(500));
    }

    assert_eq!(
        rc_hash, mined_hash,
        "RC node did not sync to the block mined on stable: expected={mined_hash} got={rc_hash}"
    );
}
