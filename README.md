# nakamoto-electrs

A bridge between [nakamoto](https://github.com/cloudhead/nakamoto) (a Bitcoin SPV light-client)
and the [Electrum protocol](https://electrumx.readthedocs.io/en/latest/protocol-methods.html),
so Electrum wallets can connect to a node that does **not** require a full `bitcoind` installation.

## Architecture

```
Bitcoin P2P Network
        │
        ▼
┌───────────────────┐
│  nakamoto (SPV)   │  Syncs block headers + compact filters (BIP 157/158)
│                   │  Downloads full blocks on demand
└────────┬──────────┘
         │  BlockEvent stream
         ▼
┌───────────────────┐
│     Indexer       │  script-hash → tx-history/UTXO map (persistent)
│                   │  Handles reorgs via BlockDisconnected rollback
└────────┬──────────┘
         │  queries
         ▼
┌───────────────────┐
│  ElectrumServer   │  TCP JSON-RPC (Electrum protocol v1.4)
│                   │  Answers: get_history, get_balance, listunspent, subscribe, broadcast, …
│                   │  Locally broadcast txs also update pending balance/history/subscribe state
└───────────────────┘
         │
         ▼
  Electrum Wallets
```

## Binaries

| Binary | Description |
|---|---|
| `nakamoto-electrs` | **Main bridge** — nakamoto SPV + Electrum server (default) |
| `nakamoto` | Standalone nakamoto node (useful for testing) |
| `electrs` | Standalone electrs backed by Bitcoin Core (upstream behaviour) |

## Build

```sh
cargo build --release
```

### sccache (optional)

Install `sccache` and add it to `PATH` to speed up incremental builds:

```sh
brew install sccache        # or: cargo install sccache
export RUSTC_WRAPPER=sccache
cargo build
```

## Run

### Default (nakamoto-electrs bridge)

```sh
cargo run -- [OPTIONS]
```

Options:

| Flag | Default | Description |
|---|---|---|
| `--network <net>` | `testnet` | Bitcoin network (`mainnet`, `testnet`, `signet`, `regtest`) |
| `--listen <addr>` | `127.0.0.1:<port>` | Electrum listener address |
| `--data-dir <path>` | `~/.nakamoto-electrs/<network>` | Base runtime data directory (`nakamoto/` + `index/`) |
| `--peer <addr>` | *(DNS seeds)* | Explicit nakamoto peer (repeatable) |
| `--log <level>` | `info` | Log level (`error`, `warn`, `info`, `debug`, `trace`) |

### Subcommands

```sh
cargo run -- nakamoto [OPTIONS]
cargo run -- electrs
```

### Examples

```sh
# Testnet with default settings
cargo run -- --network testnet

# Signet, custom listen address and data directory
cargo run -- --network signet --listen 0.0.0.0:60601 --data-dir /data/nakamoto-signet

# Regtest with an explicit local peer
cargo run -- --network regtest --peer 127.0.0.1:18444

# Standalone nakamoto node with custom signet peers
cargo run -- nakamoto --network signet --peer 127.0.0.1:38333

# Standalone electrs backed by a local Bitcoin Core node
cargo run -- electrs
```

## Connect a wallet

Point any Electrum-compatible wallet at the listener address, e.g.:

```
Server: 127.0.0.1
Port:   60001   (testnet default)
```

## Test

```sh
# Unit + integration tests (fast, no network)
cargo test

# End-to-end regtest tests (starts a local repo-local regtest node if needed)
./scripts/run-e2e-regtest-ignored.sh --verbose
```

## Module overview

| Module | Description |
|---|---|
| `block_source` | `BlockSource` trait — abstract block/header provider |
| `nakamoto_source` | `NakamotoBlockSource` — bridges nakamoto Handle to `BlockSource` |
| `config` | Unified `Config` struct + CLI arg parser |
| `indexer` | Script-hash indexer driven by `BlockEvent` stream |
| `electrum_server` | Electrum JSON-RPC TCP server |
| `metrics` | Atomic metrics counters |

## Known limitations

* **Partial mempool balance** — locally broadcast transactions are tracked as
  pending and notify subscribed clients, but full peer-observed mempool
  modeling is still incomplete.
* **Persistent index** — history, raw transaction lookups, and confirmed UTXOs
  survive restarts via the embedded store.
* **nakamoto is SPV** — nakamoto downloads compact block filters (BIP 157/158)
  and fetches full blocks only for matching filters.  This means the indexer
  only sees blocks that match watched scripts.  Watching all scripts requires
  downloading all blocks.
