# nakamoto-electrs

## sccache

Install `sccache` and keep it on your `PATH`, then Cargo will use it via `.cargo/config.toml`.

Example:

```sh
brew install sccache
export RUSTC_WRAPPER=sccache
```

## Build

```sh
cargo build
```
