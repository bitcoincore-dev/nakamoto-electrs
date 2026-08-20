#!/usr/bin/env bash
set -euo pipefail

python3 - <<'PY'
import re
import sys
import urllib.request
from html.parser import HTMLParser

BASE_URL = "https://bitcoincore.org/bin/"


class LinkParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.hrefs: list[str] = []

    def handle_starttag(self, tag, attrs):
        if tag.lower() != "a":
            return
        for name, value in attrs:
            if name == "href" and value:
                self.hrefs.append(value)
                break


def fetch(url: str) -> str:
    req = urllib.request.Request(url, headers={"User-Agent": "curl/8.0"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return resp.read().decode("utf-8", "replace")


def parse_links(url: str) -> list[str]:
    parser = LinkParser()
    parser.feed(fetch(url))
    return parser.hrefs


def version_key(version: str) -> tuple[int, ...]:
    return tuple(int(part) for part in version.split("."))


top_links = parse_links(BASE_URL)
versions = sorted(
    {
        m.group(1)
        for href in top_links
        if (m := re.fullmatch(r"bitcoin-core-([0-9]+(?:\.[0-9]+)*)/", href))
    },
    key=version_key,
    reverse=True,
)

if not versions:
    sys.exit("No Bitcoin Core versions found at /bin/")

stable_version = versions[0]
stable_base_url = f"https://bitcoincore.org/bin/bitcoin-core-{stable_version}"

rc_version = stable_version
rc_base_url = stable_base_url

for version in versions:
    links = parse_links(f"https://bitcoincore.org/bin/bitcoin-core-{version}/")
    rc_dirs = [
        int(m.group(1))
        for href in links
        if (m := re.fullmatch(r"test\.rc([0-9]+)/", href))
    ]
    if rc_dirs:
        rc_num = max(rc_dirs)
        rc_version = f"{version}rc{rc_num}"
        rc_base_url = f"https://bitcoincore.org/bin/bitcoin-core-{version}/test.rc{rc_num}"
        break

print(f"STABLE_VERSION={stable_version}")
print(f"STABLE_BASE_URL={stable_base_url}")
print(f"RC_VERSION={rc_version}")
print(f"RC_BASE_URL={rc_base_url}")
PY
