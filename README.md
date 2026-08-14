# nakamoto-electrs

## sccache

`sccache` is optional. If you want to use it, install `sccache` and set
`RUSTC_WRAPPER=sccache` in your shell before running Cargo.

Example:

```sh
brew install sccache
export RUSTC_WRAPPER=sccache
```

## Build

```sh
cargo build
```
