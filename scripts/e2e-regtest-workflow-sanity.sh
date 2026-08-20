#!/usr/bin/env bash
set -euo pipefail

WORKFLOW_FILE="${1:-.github/workflows/e2e-regtest.yml}"

ruby -e 'require "yaml"; YAML.load_file(ARGV[0]); puts "YAML OK"' "$WORKFLOW_FILE"

python3 - <<'PY'
from pathlib import Path

text = Path(".github/workflows/e2e-regtest.yml").read_text()
for needle in [
    'did not become ready',
    'STABLE_PEER_COUNT=',
    'RC_PEER_COUNT=',
    'detect-bitcoin-core-versions.sh',
    'stable_base_url',
    'rc_base_url',
    'matrix.os',
    'macos-latest',
    'Determine Bitcoin Core archive suffix',
]:
    assert needle in text, needle
print('Content checks OK')
PY

./scripts/detect-bitcoin-core-versions.sh
