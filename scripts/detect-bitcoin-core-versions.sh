#!/usr/bin/env bash
set -euo pipefail

tmp="$(mktemp)"
trap 'rm -f "$tmp"' EXIT

curl -fsSL \
  -H "Accept: application/vnd.github+json" \
  "https://api.github.com/repos/bitcoin/bitcoin/releases?per_page=30" \
  -o "$tmp"

python3 - "$tmp" <<'PY'
import json
import re
import sys

releases = json.load(open(sys.argv[1]))
non_draft = [r for r in releases if not r.get("draft", False)]
non_draft.sort(key=lambda r: r.get("published_at", ""), reverse=True)
assert non_draft, "No Bitcoin Core releases found"

is_rc = lambda r: r.get("prerelease", False) or re.search(
    r"rc\d+$", r.get("tag_name", ""), re.IGNORECASE
)

stable = next((r for r in non_draft if not is_rc(r)), None)
rc = next((r for r in non_draft if is_rc(r)), None)

stable_ver = stable["tag_name"].lstrip("v") if stable else non_draft[0]["tag_name"].lstrip("v")
rc_ver = rc["tag_name"].lstrip("v") if rc else stable_ver

print(f"STABLE_VERSION={stable_ver}")
print(f"RC_VERSION={rc_ver}")
PY
