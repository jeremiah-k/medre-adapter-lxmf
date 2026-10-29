"""Repeat native LXMF restore campaigns without changing their deadlines.

Usage: python scripts/qualify_native_link.py --runs 5
The process must have the pinned LXMF/RNS SDK and MEDRE installed.
"""

from __future__ import annotations

import argparse
import importlib.util
import re
import subprocess
import sys
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runs", type=int, default=5)
    args = parser.parse_args()
    if not 1 <= args.runs <= 100:
        parser.error("--runs must be between 1 and 100")
    if any(importlib.util.find_spec(module) is None for module in ("LXMF", "RNS")):
        parser.error(
            "pinned LXMF and RNS SDKs are required; skipped tests cannot qualify"
        )
    repository = Path(__file__).resolve().parents[1]
    case = (
        "tests/test_stopped_application_sdk.py"
        "::test_shared_reticulum_chat_and_endpoint_restore"
    )
    failed = 0
    for number in range(1, args.runs + 1):
        command = [
            sys.executable,
            "-m",
            "pytest",
            "-q",
            "-ra",
            "--color=no",
            "-o",
            "addopts=",
            case,
        ]
        print(f"Native LXMF restore campaign {number}/{args.runs}", flush=True)
        try:
            result = subprocess.run(
                command,
                cwd=repository,
                check=False,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                timeout=180,
            )
            print(result.stdout, end="" if result.stdout.endswith("\n") else "\n")
            complete = (
                re.search(r"(?m)^2 passed(?:, \d+ warnings?)? in ", result.stdout)
                is not None
            )
            if result.returncode != 0 or not complete:
                failed += 1
                print(
                    "Native campaign did not execute and pass both directions",
                    file=sys.stderr,
                )
        except subprocess.TimeoutExpired:
            print(
                "Campaign process exceeded its independent 180-second bound",
                file=sys.stderr,
            )
            failed += 1
    print(f"Native restore campaigns: {args.runs - failed} passed, {failed} failed")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
