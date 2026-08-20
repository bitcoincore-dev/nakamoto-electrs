//! Abstract block-source interface used by the indexer.
//!
//! Decouples the indexing pipeline from any particular backend (nakamoto,
//! mock data for tests, future full-node RPC, …).

use anyhow::Result;
use crossbeam_channel::Receiver;

// We deliberately use the `bitcoin` 0.30 types that are already a direct
// dependency of this crate for our own data model.  When bridging to
// nakamoto (which uses `bitcoin` 0.29 via `nakamoto_chain`) we convert at
// the boundary inside `nakamoto_source.rs`.
use bitcoin::{Block, BlockHash, blockdata::block::Header as BlockHeader};

// ---------------------------------------------------------------------------
// BlockEvent
// ---------------------------------------------------------------------------

/// An event emitted by a [`BlockSource`] subscription.
#[derive(Debug, Clone)]
pub enum BlockEvent {
    /// A new block has been connected to the best chain.
    Connected {
        block: Block,
        height: u32,
    },
    /// A block has been disconnected during a chain reorganisation.
    ///
    /// The indexer should roll back any data it indexed for this block.
    Disconnected {
        hash: BlockHash,
        height: u32,
    },
    /// The block source has caught up with the current chain tip.
    ///
    /// Emitted once after initial sync completes, and again after each
    /// subsequent block that advances the tip.
    Synced {
        /// Current best-chain height.
        height: u32,
        /// Hash of the current best-chain tip block.
        tip: BlockHash,
    },
}

// ---------------------------------------------------------------------------
// BlockSource trait
// ---------------------------------------------------------------------------

/// An abstract source of Bitcoin blocks consumed by the indexer.
///
/// Implementors provide:
/// - A push-based event stream ([`subscribe`][BlockSource::subscribe]) that
///   delivers [`BlockEvent`]s as the chain progresses.
/// - Point queries for headers and full blocks by height or hash.
pub trait BlockSource: Send + 'static {
    /// Subscribe to the block event stream.
    ///
    /// Each call returns a fresh [`Receiver`] whose channel is kept alive for
    /// as long as the `BlockSource` is running.  Dropping the receiver simply
    /// unsubscribes this particular listener.
    fn subscribe(&self) -> Receiver<BlockEvent>;

    /// Return the current chain tip as `(height, hash)`.
    fn tip(&self) -> Result<(u32, BlockHash)>;

    /// Fetch the block header at the given height.
    ///
    /// Returns `None` when the height is beyond the current tip.
    fn block_header(&self, height: u32) -> Result<Option<BlockHeader>>;

    /// Fetch the full block with the given hash.
    ///
    /// Returns `None` when the block is not (yet) available locally.
    fn block_by_hash(&self, hash: &BlockHash) -> Result<Option<Block>>;
}

/// Blanket implementation so an `Arc<S>` can be used wherever a `&S` is
/// accepted.  This lets the [`ElectrumServer`] hold and share a `Arc<dyn
/// BlockSource>` without losing object-safety.
impl<S: BlockSource + Sync + ?Sized> BlockSource for std::sync::Arc<S> {
    fn subscribe(&self) -> Receiver<BlockEvent> {
        (**self).subscribe()
    }
    fn tip(&self) -> Result<(u32, BlockHash)> {
        (**self).tip()
    }
    fn block_header(&self, height: u32) -> Result<Option<BlockHeader>> {
        (**self).block_header(height)
    }
    fn block_by_hash(&self, hash: &BlockHash) -> Result<Option<Block>> {
        (**self).block_by_hash(hash)
    }
}

// ---------------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// A trivial smoke-test: `BlockEvent` variants are constructable and
    /// hold the expected fields.
    #[test]
    fn block_event_connected_fields() {
        use bitcoin::blockdata::block::Header as BH;
        use bitcoin::blockdata::transaction::Transaction;
        use bitcoin::hashes::Hash;

        let header = BH {
            version: bitcoin::blockdata::block::Version::ONE,
            prev_blockhash: BlockHash::all_zeros(),
            merkle_root: bitcoin::hash_types::TxMerkleNode::all_zeros(),
            time: 0,
            bits: bitcoin::CompactTarget::from_consensus(0x1d00ffff),
            nonce: 0,
        };
        let block = Block {
            header,
            txdata: vec![Transaction {
                version: 1i32,
                lock_time: bitcoin::absolute::LockTime::ZERO,
                input: vec![],
                output: vec![],
            }],
        };
        let ev = BlockEvent::Connected {
            block: block.clone(),
            height: 1,
        };
        if let BlockEvent::Connected { height, .. } = ev {
            assert_eq!(height, 1);
        } else {
            panic!("unexpected variant");
        }
    }

    #[test]
    fn block_event_disconnected_fields() {
        use bitcoin::hashes::Hash;
        let ev = BlockEvent::Disconnected {
            hash: BlockHash::all_zeros(),
            height: 42,
        };
        if let BlockEvent::Disconnected { height, .. } = ev {
            assert_eq!(height, 42);
        } else {
            panic!("unexpected variant");
        }
    }

    #[test]
    fn block_event_synced_fields() {
        use bitcoin::hashes::Hash;
        let ev = BlockEvent::Synced {
            height: 800_000,
            tip: BlockHash::all_zeros(),
        };
        if let BlockEvent::Synced { height, tip } = ev {
            assert_eq!(height, 800_000);
            assert_eq!(tip, BlockHash::all_zeros());
        } else {
            panic!("unexpected variant");
        }
    }
}
