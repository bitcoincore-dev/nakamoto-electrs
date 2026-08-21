//! Atomic metrics counters for nakamoto-electrs.
//!
//! Provides lightweight, lock-free counters that any module can increment.
//! Values can be read by the main thread for logging or future export (e.g.
//! Prometheus).

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

// ---------------------------------------------------------------------------
// Metrics
// ---------------------------------------------------------------------------

/// Shared, atomically-updated metrics for the bridge.
///
/// Clone is cheap — the inner state is reference-counted.
#[derive(Clone, Default)]
pub struct Metrics(Arc<Inner>);

#[derive(Default)]
struct Inner {
    /// Total number of blocks successfully indexed.
    blocks_indexed: AtomicU64,
    /// Total number of blocks rolled back due to reorgs.
    blocks_rolled_back: AtomicU64,
    /// Current number of active Electrum TCP connections.
    electrum_connections: AtomicU64,
    /// Total Electrum JSON-RPC requests handled since startup.
    electrum_requests: AtomicU64,
    /// Total number of transactions broadcast via the Electrum
    /// `blockchain.transaction.broadcast` method.
    transactions_broadcast: AtomicU64,
    /// Total unique txids observed as acknowledged by peers.
    peer_seen_transactions: AtomicU64,
}

impl Metrics {
    /// Create a new zeroed-out metrics instance.
    pub fn new() -> Self {
        Self::default()
    }

    // ---- blocks ------------------------------------------------------------

    /// Record that one block was indexed.
    pub fn inc_blocks_indexed(&self) {
        self.0.blocks_indexed.fetch_add(1, Ordering::Relaxed);
    }

    /// Number of blocks indexed so far.
    pub fn blocks_indexed(&self) -> u64 {
        self.0.blocks_indexed.load(Ordering::Relaxed)
    }

    /// Record that one block was rolled back.
    pub fn inc_blocks_rolled_back(&self) {
        self.0.blocks_rolled_back.fetch_add(1, Ordering::Relaxed);
    }

    /// Number of blocks rolled back so far.
    pub fn blocks_rolled_back(&self) -> u64 {
        self.0.blocks_rolled_back.load(Ordering::Relaxed)
    }

    // ---- connections -------------------------------------------------------

    /// Record a new Electrum TCP connection.
    pub fn inc_electrum_connections(&self) {
        self.0.electrum_connections.fetch_add(1, Ordering::Relaxed);
    }

    /// Record that an Electrum TCP connection closed.
    pub fn dec_electrum_connections(&self) {
        self.0.electrum_connections.fetch_sub(1, Ordering::Relaxed);
    }

    /// Current number of active Electrum connections.
    pub fn electrum_connections(&self) -> u64 {
        self.0.electrum_connections.load(Ordering::Relaxed)
    }

    // ---- requests ----------------------------------------------------------

    /// Record that one Electrum RPC request was handled.
    pub fn inc_electrum_requests(&self) {
        self.0.electrum_requests.fetch_add(1, Ordering::Relaxed);
    }

    /// Total Electrum requests handled.
    pub fn electrum_requests(&self) -> u64 {
        self.0.electrum_requests.load(Ordering::Relaxed)
    }

    // ---- broadcast ---------------------------------------------------------

    /// Record a successful transaction broadcast.
    pub fn inc_transactions_broadcast(&self) {
        self.0
            .transactions_broadcast
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Total transactions broadcast.
    pub fn transactions_broadcast(&self) -> u64 {
        self.0.transactions_broadcast.load(Ordering::Relaxed)
    }

    /// Record a peer-seen transaction.
    pub fn inc_peer_seen_transactions(&self) {
        self.0
            .peer_seen_transactions
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Total peer-seen transactions.
    pub fn peer_seen_transactions(&self) -> u64 {
        self.0.peer_seen_transactions.load(Ordering::Relaxed)
    }
}

// ---------------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counters_start_at_zero() {
        let m = Metrics::new();
        assert_eq!(m.blocks_indexed(), 0);
        assert_eq!(m.blocks_rolled_back(), 0);
        assert_eq!(m.electrum_connections(), 0);
        assert_eq!(m.electrum_requests(), 0);
        assert_eq!(m.transactions_broadcast(), 0);
        assert_eq!(m.peer_seen_transactions(), 0);
    }

    #[test]
    fn inc_blocks_indexed() {
        let m = Metrics::new();
        m.inc_blocks_indexed();
        m.inc_blocks_indexed();
        assert_eq!(m.blocks_indexed(), 2);
    }

    #[test]
    fn inc_blocks_rolled_back() {
        let m = Metrics::new();
        m.inc_blocks_rolled_back();
        assert_eq!(m.blocks_rolled_back(), 1);
    }

    #[test]
    fn connection_lifecycle() {
        let m = Metrics::new();
        m.inc_electrum_connections();
        m.inc_electrum_connections();
        assert_eq!(m.electrum_connections(), 2);
        m.dec_electrum_connections();
        assert_eq!(m.electrum_connections(), 1);
    }

    #[test]
    fn requests_counter() {
        let m = Metrics::new();
        for _ in 0..5 {
            m.inc_electrum_requests();
        }
        assert_eq!(m.electrum_requests(), 5);
    }

    #[test]
    fn clone_shares_state() {
        let m1 = Metrics::new();
        let m2 = m1.clone();
        m1.inc_blocks_indexed();
        m1.inc_peer_seen_transactions();
        assert_eq!(m2.blocks_indexed(), 1);
        assert_eq!(m2.peer_seen_transactions(), 1);
    }
}
