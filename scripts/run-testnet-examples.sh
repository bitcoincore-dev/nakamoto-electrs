#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(git rev-parse --show-toplevel)"
cd "$ROOT_DIR"

NAKAMOTO_TIMEOUT_SECONDS="${NAKAMOTO_TIMEOUT_SECONDS:-10}"
ELECTRS_TIMEOUT_SECONDS="${ELECTRS_TIMEOUT_SECONDS:-10}"
ELECTRS_NETWORK="${ELECTRS_NETWORK:-testnet}"
if [ -n "${BITCOIN_DATADIR:-}" ]; then
  : # use the caller-provided datadir
elif case "${OSTYPE:-}" in darwin*) true ;; *) false ;; esac; then
  BITCOIN_DATADIR="$HOME/Library/Application Support/Bitcoin"
else
  BITCOIN_DATADIR="$HOME/.bitcoin"
fi

BITCOIN_TESTNET_DIR="$BITCOIN_DATADIR/testnet3"

if [ ! -f "$BITCOIN_TESTNET_DIR/.cookie" ]; then
  cat >&2 <<EOF
testnet cookie file is missing: $BITCOIN_TESTNET_DIR/.cookie
Start your testnet node with that datadir, or set BITCOIN_DATADIR explicitly.
EOF
  exit 1
fi

if ! bitcoin-cli -datadir="$BITCOIN_DATADIR" -testnet -rpcwait getblockchaininfo >/dev/null 2>&1; then
  cat >&2 <<'EOF'
testnet bitcoind is not reachable.
Start a local testnet bitcoind first, then rerun this script.
EOF
  exit 1
fi

python3 - "$NAKAMOTO_TIMEOUT_SECONDS" "$ELECTRS_TIMEOUT_SECONDS" "$ELECTRS_NETWORK" "$BITCOIN_DATADIR" <<'PY'
import subprocess
import sys

nakamoto_timeout = int(sys.argv[1])
electrs_timeout = int(sys.argv[2])
electrs_network = sys.argv[3]
bitcoind_datadir = sys.argv[4]


def run(cmd, timeout_seconds, label):
    print(f"==> {label}: {' '.join(cmd)}")
    proc = subprocess.Popen(cmd)
    try:
        returncode = proc.wait(timeout=timeout_seconds)
    except subprocess.TimeoutExpired:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()
        returncode = 124

    if returncode not in (0, 124):
        raise SystemExit(returncode)


run(
    ["cargo", "run", "--example", "nakamoto"],
    nakamoto_timeout,
    "nakamoto example",
)
run(
    [
        "cargo",
        "run",
        "--features",
        "electrs-bin",
        "--example",
        "electrs",
        "--",
        "--network",
        electrs_network,
        "--daemon-dir",
        bitcoind_datadir,
    ],
    electrs_timeout,
    "electrs example",
)
PY
