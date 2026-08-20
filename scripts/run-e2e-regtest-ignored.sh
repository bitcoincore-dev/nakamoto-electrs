#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(git rev-parse --show-toplevel)"
cd "$ROOT_DIR"

VERBOSE=0
BITCOIND_STABLE_DATADIR=""
BITCOIND_RC_DATADIR=""
while [ "$#" -gt 0 ]; do
  case "$1" in
    --verbose)
      VERBOSE=1
      ;;
    --datadir)
      shift
      if [ "$#" -eq 0 ]; then
        cat >&2 <<EOF
missing value for --datadir
usage: $0 [--verbose] [--datadir PATH]
EOF
        exit 1
      fi
      BITCOIND_STABLE_DATADIR="$1"
      ;;
    --datadir=*)
      BITCOIND_STABLE_DATADIR="${1#--datadir=}"
      ;;
    --rc-datadir)
      shift
      if [ "$#" -eq 0 ]; then
        cat >&2 <<EOF
missing value for --rc-datadir
usage: $0 [--verbose] [--datadir PATH] [--rc-datadir PATH]
EOF
        exit 1
      fi
      BITCOIND_RC_DATADIR="$1"
      ;;
    --rc-datadir=*)
      BITCOIND_RC_DATADIR="${1#--rc-datadir=}"
      ;;
    *)
      cat >&2 <<EOF
unknown argument: $1
usage: $0 [--verbose] [--datadir PATH] [--rc-datadir PATH]

# for the ignored regtest tests
bitcoind -regtest -datadir="$ROOT_DIR/.bitcoin-regtest"
$0 --verbose
EOF
      exit 1
      ;;
  esac
  shift
done

if [ "$VERBOSE" = "1" ]; then
  set -x
fi

RPC_USER="${BITCOIND_RPC_USER:-user}"
RPC_PASS="${BITCOIND_RPC_PASS:-passw0rd}"

if [ -z "$BITCOIND_STABLE_DATADIR" ]; then
  for candidate in \
    "$ROOT_DIR/.bitcoin-regtest" \
    "$ROOT_DIR/.bitcoin/regtest" \
    "$ROOT_DIR/bitcoin/regtest"
  do
    if [ -d "$candidate" ]; then
      BITCOIND_STABLE_DATADIR="$candidate"
      break
    fi
  done
  BITCOIND_STABLE_DATADIR="${BITCOIND_STABLE_DATADIR:-$ROOT_DIR/.bitcoin-regtest}"
fi

mkdir -p "$BITCOIND_STABLE_DATADIR"

if [ -n "${BITCOIND_RC_DATADIR:-}" ]; then
  mkdir -p "$BITCOIND_RC_DATADIR"
fi

export BITCOIND_STABLE_DATADIR
export BITCOIND_RPC_USER="$RPC_USER"
export BITCOIND_RPC_PASS="$RPC_PASS"
if [ -n "$BITCOIND_RC_DATADIR" ]; then
  export BITCOIND_RC_DATADIR
fi

cleanup() {
  if [ -n "${BITCOIND_STARTED_BY_SCRIPT:-}" ] && [ -x "$(command -v bitcoin-cli)" ]; then
    bitcoin-cli -datadir="$BITCOIND_STABLE_DATADIR" -regtest \
      -rpcuser="$RPC_USER" -rpcpassword="$RPC_PASS" -rpcport=18443 stop >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

wait_for_regtest_rpc() {
  local attempts=60
  local i
  for i in $(seq 1 "$attempts"); do
    if bitcoin-cli -datadir="$BITCOIND_STABLE_DATADIR" -regtest \
      -rpcuser="$RPC_USER" -rpcpassword="$RPC_PASS" -rpcport=18443 getblockchaininfo >/dev/null 2>&1; then
      return 0
    fi
    if [ "$VERBOSE" = "1" ] && [ $((i % 5)) -eq 0 ]; then
      echo "waiting for local regtest bitcoind RPC... (${i}/${attempts})"
    fi
    sleep 1
  done
  return 1
}

echo "Running ignored regtest e2e tests with BITCOIND_STABLE_DATADIR=$BITCOIND_STABLE_DATADIR"
if [ -n "${BITCOIND_RC_DATADIR:-}" ]; then
  echo "Running ignored regtest e2e tests with BITCOIND_RC_DATADIR=$BITCOIND_RC_DATADIR"
fi

if ! bitcoin-cli -datadir="$BITCOIND_STABLE_DATADIR" -regtest \
  -rpcuser="$RPC_USER" -rpcpassword="$RPC_PASS" -rpcport=18443 getblockchaininfo >/dev/null 2>&1; then
  echo "starting local regtest bitcoind in $BITCOIND_STABLE_DATADIR"
  cat >"$BITCOIND_STABLE_DATADIR/bitcoin.conf" <<EOF
[regtest]
server=1
daemon=1
txindex=1
fallbackfee=0.0001
rpcuser=$RPC_USER
rpcpassword=$RPC_PASS
rpcport=18443
port=18444
bind=127.0.0.1
EOF
  bitcoind -regtest -datadir="$BITCOIND_STABLE_DATADIR"
  BITCOIND_STARTED_BY_SCRIPT=1
  if ! wait_for_regtest_rpc; then
    cat >&2 <<EOF
local regtest bitcoind did not become ready on 127.0.0.1:18443
see $BITCOIND_STABLE_DATADIR/debug.log for details
EOF
    exit 1
  fi
fi

cargo test --test e2e_regtest -- --ignored --nocapture
