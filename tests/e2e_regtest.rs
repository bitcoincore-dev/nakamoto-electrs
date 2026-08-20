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
//! 1. Start a `nakamoto` node connected to the local regtest peer.
//! 2. Mine 101 blocks via the bitcoind RPC to make coins spendable.
//! 3. Connect a raw TCP Electrum client to nakamoto-electrs.
//! 4. Assert that `blockchain.headers.subscribe` returns a tip at height ≥ 101.

// All tests in this file are ignored by default (require external bitcoind).

/// Placeholder end-to-end test — ignored until the full regtest harness is
/// wired up in a follow-up.
#[test]
#[ignore = "requires external bitcoind -regtest; run with --ignored"]
fn e2e_headers_subscribe_returns_tip() {
    // TODO: implement full regtest harness.
    //
    // Steps:
    //   1. Read BITCOIND_RPC_URL/USER/PASS from env.
    //   2. Start nakamoto-electrs with Network::Regtest and a local peer.
    //   3. Mine 101 blocks via the RPC.
    //   4. Open a TCP connection to the Electrum listener.
    //   5. Send `blockchain.headers.subscribe` and parse the response.
    //   6. Assert height >= 101.
    todo!("regtest harness not yet implemented");
}

/// Placeholder — ignored.
#[test]
#[ignore = "requires external bitcoind -regtest; run with --ignored"]
fn e2e_scripthash_history_after_payment() {
    // TODO: implement.
    //
    // Steps:
    //   1. Generate a new address.
    //   2. Send a payment to that address via the bitcoind RPC.
    //   3. Mine a confirming block.
    //   4. Query `blockchain.scripthash.get_history` via Electrum.
    //   5. Assert the transaction appears in history.
    todo!("regtest harness not yet implemented");
}
