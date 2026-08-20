#!/usr/bin/env python3
import hashlib
import pathlib
import re
import sys


def main() -> int:
    if len(sys.argv) != 2:
        raise SystemExit("usage: verify-sha256.py <file>")

    tarball = pathlib.Path(sys.argv[1])
    expected = None
    for line in pathlib.Path("SHA256SUMS").read_text(encoding="utf-8").splitlines():
        match = re.match(r"^([0-9a-fA-F]{64})\s+\*?(.+)$", line)
        if match and match.group(2) == tarball.name:
            expected = match.group(1).lower()
            break

    if expected is None:
        raise SystemExit(f"missing checksum for {tarball.name}")

    actual = hashlib.sha256(tarball.read_bytes()).hexdigest()
    if actual != expected:
        raise SystemExit(f"checksum mismatch for {tarball.name}: {actual} != {expected}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
