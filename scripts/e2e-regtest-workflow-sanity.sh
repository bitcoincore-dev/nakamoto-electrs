#!/usr/bin/env bash
set -euo pipefail

WORKFLOW_FILE="${1:-.github/workflows/e2e-regtest.yml}"

ruby -e 'require "yaml"; YAML.load_file(ARGV[0]); puts "YAML OK"' "$WORKFLOW_FILE"

python3 - <<'PY'
from pathlib import Path

text = Path(".github/workflows/e2e-regtest.yml").read_text()
for needle in [
    'rc\\d+$',
    'did not become ready',
    'STABLE_PEER_COUNT=',
    'RC_PEER_COUNT=',
    "printf '%s\\n'",
]:
    assert needle in text, needle
print('Content checks OK')
PY

RELEASES='[{"tag_name":"v28.0","draft":false,"published_at":"2025-01-02T00:00:00Z"},{"tag_name":"v29.0rc1","draft":false,"published_at":"2025-02-01T00:00:00Z"}]'
eval "$(echo "$RELEASES" | python3 -c 'import json, sys, re; releases = json.load(sys.stdin); non_draft = [r for r in releases if not r.get("draft", False)]; non_draft.sort(key=lambda r: r.get("published_at", ""), reverse=True); assert non_draft, "No Bitcoin Core releases found"; RC_RE = re.compile(r"rc\d+$", re.IGNORECASE); stable = next((r for r in non_draft if not RC_RE.search(r["tag_name"])), None); rc = next((r for r in non_draft if RC_RE.search(r["tag_name"])), None); stable_ver = stable["tag_name"].lstrip("v") if stable else non_draft[0]["tag_name"].lstrip("v"); rc_ver = rc["tag_name"].lstrip("v") if rc else stable_ver; print(f"STABLE_VERSION={stable_ver}"); print(f"RC_VERSION={rc_ver}")')"
printf '%s %s\n' "${STABLE_VERSION}" "${RC_VERSION}"
