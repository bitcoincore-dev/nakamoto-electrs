//! [`NakamotoBlockSource`] — bridges a running nakamoto light-client node to
//! the [`BlockSource`] abstraction used by the indexer.
//!
//! ## Design
//!
//! nakamoto and this crate use **different versions** of the `bitcoin` crate:
//! * nakamoto: `nakamoto_common::bitcoin` (0.29)
//! * nakamoto-electrs: `bitcoin` (0.30)
//!
//! All cross-version conversion is done at this boundary via Bitcoin consensus
//! (wire) encoding: serialize with the 0.29 encoder, then deserialize with the
//! 0.30 decoder.  Because both versions encode the same on-wire format this is
//! lossless.
//!
//! ## Event threading model
//!
//! Construction spawns two background threads:
//!
//! 1. **event thread** — drains `handle.events()`.  On `BlockConnected` it
//!    calls `handle.get_block(hash)` to queue a full-block download.  On
//!    `BlockDisconnected` it emits a `BlockEvent::Disconnected`.  On `Synced`
//!    it emits `BlockEvent::Synced`.
//!
//! 2. **blocks thread** — drains `handle.blocks()`.  Every full block that
//!    nakamoto downloads (in response to `get_block` calls queued by thread 1)
//!    is converted to bitcoin 0.30 types, stored in a local cache, and
//!    broadcast to all current subscribers as `BlockEvent::Connected`.
//!
//! Subscribers call [`NakamotoBlockSource::subscribe`] which atomically adds a
//! fresh `crossbeam_channel` sender to the subscriber list and returns the
//! corresponding receiver.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::thread;

use anyhow::{Context, Result};
use crossbeam_channel::{Receiver, Sender, unbounded};
use tracing::{debug, error, warn};

use nakamoto_client::handle::Handle;
use nakamoto_client::Event as NkEvent;
// nakamoto's bitcoin (0.29) – used only here for the version bridge.
use nakamoto_common::bitcoin::consensus::encode::serialize as nk_serialize;

// Our crate's bitcoin (0.30)
use bitcoin::{Block, BlockHash, blockdata::block::Header as BlockHeader, hashes::Hash};
use bitcoin::consensus::deserialize as btc_deserialize;

use crate::block_source::{BlockEvent, BlockSource};

// ---------------------------------------------------------------------------
// Subscriber list helper
// ---------------------------------------------------------------------------

type Subscribers = Arc<Mutex<Vec<Sender<BlockEvent>>>>;

fn broadcast(subs: &Subscribers, event: &BlockEvent) {
    let mut guard = subs.lock().expect("subscriber lock poisoned");
    // Retain only senders whose receiver is still alive.
    guard.retain(|tx| tx.send(event.clone()).is_ok());
}

// ---------------------------------------------------------------------------
// Conversion helpers (bitcoin 0.29 → 0.30 via wire encoding)
// ---------------------------------------------------------------------------

fn conv_block(nk_block: &nakamoto_common::bitcoin::blockdata::block::Block) -> Result<Block> {
    let bytes = nk_serialize(nk_block);
    btc_deserialize(&bytes).context("failed to deserialise nakamoto block into bitcoin 0.30 Block")
}

fn conv_header(
    nk_hdr: &nakamoto_common::bitcoin::blockdata::block::BlockHeader,
) -> Result<BlockHeader> {
    let bytes = nk_serialize(nk_hdr);
    btc_deserialize(&bytes)
        .context("failed to deserialise nakamoto header into bitcoin 0.30 BlockHeader")
}

fn conv_hash(
    nk_hash: &nakamoto_common::bitcoin::hash_types::BlockHash,
) -> Result<BlockHash> {
    // BlockHash is a newtype over a 32-byte array in both versions.
    let bytes: [u8; 32] = nk_hash
        .as_ref()
        .try_into()
        .context("nakamoto BlockHash was not 32 bytes")?;
    Ok(BlockHash::from_byte_array(bytes))
}

// ---------------------------------------------------------------------------
// NakamotoBlockSource
// ---------------------------------------------------------------------------

/// Block source backed by a running nakamoto SPV client.
///
/// See module docs for the threading model and version-bridge approach.
pub struct NakamotoBlockSource {
    /// Handle to the running nakamoto client, kept for point queries.
    handle: Arc<dyn HandleErased>,
    /// Dynamic subscriber list — shared with both background threads.
    subscribers: Subscribers,
    /// Cache of recently delivered full blocks (bitcoin 0.30 types),
    /// keyed by block hash.  Shared with the blocks thread.
    block_cache: Arc<Mutex<HashMap<BlockHash, Block>>>,
}

// ---------------------------------------------------------------------------
// Type-erased handle wrapper
// ---------------------------------------------------------------------------
// The nakamoto `Handle` trait is generic over the reactor type.  We erase
// that parameter so `NakamotoBlockSource` has a concrete type.

trait HandleErased: Send + Sync {
    fn tip_erased(
        &self,
    ) -> std::result::Result<
        (u64, nakamoto_common::bitcoin::blockdata::block::BlockHeader),
        nakamoto_client::handle::Error,
    >;
    fn get_block_erased(
        &self,
        hash: &nakamoto_common::bitcoin::hash_types::BlockHash,
    ) -> std::result::Result<(), nakamoto_client::handle::Error>;
    fn query_tree_erased(
        &self,
        height: u64,
        result_tx: Sender<Option<nakamoto_common::bitcoin::blockdata::block::BlockHeader>>,
    ) -> std::result::Result<(), nakamoto_client::handle::Error>;
}

struct HandleWrap<H: Handle>(H);

impl<H: Handle + Send + Sync> HandleErased for HandleWrap<H> {
    fn tip_erased(
        &self,
    ) -> std::result::Result<
        (u64, nakamoto_common::bitcoin::blockdata::block::BlockHeader),
        nakamoto_client::handle::Error,
    > {
        self.0.get_tip()
    }

    fn get_block_erased(
        &self,
        hash: &nakamoto_common::bitcoin::hash_types::BlockHash,
    ) -> std::result::Result<(), nakamoto_client::handle::Error> {
        self.0.get_block(hash)
    }

    fn query_tree_erased(
        &self,
        height: u64,
        result_tx: Sender<Option<nakamoto_common::bitcoin::blockdata::block::BlockHeader>>,
    ) -> std::result::Result<(), nakamoto_client::handle::Error> {
        self.0.query_tree(move |tree| {
            let hdr = tree.get_block_by_height(height).cloned();
            let _ = result_tx.send(hdr);
        })
    }
}

// ---------------------------------------------------------------------------
// Construction
// ---------------------------------------------------------------------------

impl NakamotoBlockSource {
    /// Create a new `NakamotoBlockSource` wrapping the given nakamoto handle.
    ///
    /// Background threads are started immediately.  The returned value is
    /// cheap to clone and share across threads.
    pub fn new<H: Handle + 'static>(handle: H) -> Self {
        let subscribers: Subscribers = Arc::new(Mutex::new(Vec::new()));
        let block_cache: Arc<Mutex<HashMap<BlockHash, Block>>> =
            Arc::new(Mutex::new(HashMap::new()));

        let events_rx = handle.events();
        let blocks_rx = handle.blocks();

        let erased: Arc<dyn HandleErased> = Arc::new(HandleWrap(handle));

        // ---- blocks thread ------------------------------------------------
        {
            let subs = Arc::clone(&subscribers);
            let cache = Arc::clone(&block_cache);
            thread::Builder::new()
                .name("nk-blocks".into())
                .spawn(move || {
                    for (nk_block, height) in &blocks_rx {
                        match conv_block(&nk_block) {
                            Ok(block) => {
                                let hash = block.block_hash();
                                debug!("nakamoto delivered full block {} h={}", hash, height);
                                cache
                                    .lock()
                                    .expect("block_cache lock poisoned")
                                    .insert(hash, block.clone());
                                broadcast(
                                    &subs,
                                    &BlockEvent::Connected {
                                        block,
                                        height: height as u32,
                                    },
                                );
                            }
                            Err(e) => error!("block conversion failed: {e:#}"),
                        }
                    }
                    debug!("nk-blocks thread exiting");
                })
                .expect("failed to spawn nk-blocks thread");
        }

        // ---- events thread ------------------------------------------------
        {
            let subs = Arc::clone(&subscribers);
            let handle_ref = Arc::clone(&erased);
            thread::Builder::new()
                .name("nk-events".into())
                .spawn(move || {
                    for event in &events_rx {
                        match event {
                            NkEvent::BlockConnected { hash, height, .. } => {
                                debug!("BlockConnected h={height} {hash}");
                                // Ask nakamoto to download the full block.  It
                                // will be delivered to `blocks_rx` handled above.
                                if let Err(e) = handle_ref.get_block_erased(&hash) {
                                    warn!("get_block({hash}) failed: {e}");
                                }
                            }
                            NkEvent::BlockDisconnected { hash, height, .. } => {
                                debug!("BlockDisconnected h={height} {hash}");
                                match conv_hash(&hash) {
                                    Ok(bh) => broadcast(
                                        &subs,
                                        &BlockEvent::Disconnected {
                                            hash: bh,
                                            height: height as u32,
                                        },
                                    ),
                                    Err(e) => error!("hash conversion failed: {e:#}"),
                                }
                            }
                            NkEvent::Synced { height, tip } => {
                                // nakamoto's Synced reports filter-sync progress; use
                                // `tip` as the best-chain height.
                                debug!("Synced h={height} tip={tip}");
                                // We need the hash of the tip; query the tree.
                                let (result_tx, result_rx) = unbounded();
                                if let Err(e) =
                                    handle_ref.query_tree_erased(tip, result_tx)
                                {
                                    warn!("query_tree for Synced failed: {e}");
                                    continue;
                                }
                                match result_rx.recv() {
                                    Ok(Some(nk_hdr)) => {
                                        match conv_header(&nk_hdr) {
                                            Ok(hdr) => broadcast(
                                                &subs,
                                                &BlockEvent::Synced {
                                                    height: tip as u32,
                                                    tip: hdr.block_hash(),
                                                },
                                            ),
                                            Err(e) => error!("header conversion: {e:#}"),
                                        }
                                    }
                                    Ok(None) => {
                                        warn!("tip header not found at height {tip}")
                                    }
                                    Err(_) => warn!("query_tree result channel closed"),
                                }
                            }
                            _ => {}
                        }
                    }
                    debug!("nk-events thread exiting");
                })
                .expect("failed to spawn nk-events thread");
        }

        Self {
            handle: erased,
            subscribers,
            block_cache,
        }
    }
}

// ---------------------------------------------------------------------------
// BlockSource implementation
// ---------------------------------------------------------------------------

impl BlockSource for NakamotoBlockSource {
    fn subscribe(&self) -> Receiver<BlockEvent> {
        let (tx, rx) = unbounded();
        self.subscribers
            .lock()
            .expect("subscriber lock poisoned")
            .push(tx);
        rx
    }

    fn tip(&self) -> Result<(u32, BlockHash)> {
        let (height, nk_hdr) = self
            .handle
            .tip_erased()
            .context("nakamoto get_tip failed")?;
        let hdr = conv_header(&nk_hdr)?;
        Ok((height as u32, hdr.block_hash()))
    }

    fn block_header(&self, height: u32) -> Result<Option<BlockHeader>> {
        let (result_tx, result_rx) = unbounded();
        self.handle
            .query_tree_erased(height as u64, result_tx)
            .context("nakamoto query_tree failed")?;
        let opt = result_rx.recv().context("query_tree result channel closed")?;
        match opt {
            None => Ok(None),
            Some(nk_hdr) => Ok(Some(conv_header(&nk_hdr)?)),
        }
    }

    fn block_by_hash(&self, hash: &BlockHash) -> Result<Option<Block>> {
        // Serve from cache; a full block lands here only after nakamoto has
        // downloaded it in response to a prior get_block() call.
        let cache = self.block_cache.lock().expect("block_cache lock poisoned");
        Ok(cache.get(hash).cloned())
    }
}
