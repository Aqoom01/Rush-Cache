#!/usr/bin/env python3
"""Validate checkpoint completeness before an EC2 campaign is marked complete."""

from __future__ import annotations

import argparse
import csv
import json
from collections import Counter
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--plan", type=Path, required=True)
    parser.add_argument("--status", type=Path, required=True)
    parser.add_argument("--expected", type=int, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    with args.plan.open(encoding="utf-8", newline="") as source:
        plan = list(csv.DictReader(source, delimiter="\t"))
    with args.status.open(encoding="utf-8", newline="") as source:
        status = list(csv.DictReader(source))

    passed = {int(row["sequence"]): row for row in status if row["status"] == "passed"}
    failures = [row for row in status if row["status"] == "failed"]
    profile_counts = Counter(row["profile"] for row in passed.values())
    fault_counts = Counter(row["fault"] for row in passed.values())

    checks = {
        "plan_total": len(plan) == args.expected,
        "passed_total": len(passed) == args.expected,
        "profile_count": len(profile_counts) == 5,
        "fault_count": len(fault_counts) == 11,
    }
    result = {
        "status": "passed" if all(checks.values()) else "failed",
        "checks": checks,
        "expected": args.expected,
        "passed": len(passed),
        "historical_failed_records": len(failures),
        "profiles": dict(sorted(profile_counts.items())),
        "faults": dict(sorted(fault_counts.items())),
    }
    args.output.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result, indent=2))
    if result["status"] != "passed":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
