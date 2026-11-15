"""Compile operating-suite inputs and combine verified GitLab batch results."""

import argparse
import csv
import json
from pathlib import Path
from types import SimpleNamespace

from core_pipeline import read_receipts
from integration import PLAN_COLUMNS, save
from operating_suite import SCENARIOS, WORKLOAD_DEFAULTS, case_inputs, select_strategies


def prepare(matrix, output, limit):
    strategies = select_strategies(matrix, 0, limit)
    if len(strategies) != limit:
        raise ValueError("strategy CSV has fewer candidates than requested")
    settings = SimpleNamespace(**WORKLOAD_DEFAULTS)
    rows, per_row = [], []
    for number, strategy in enumerate(strategies, start=1):
        for name in SCENARIOS:
            selectors, arguments = case_inputs(settings, strategy, name, 1)
            rows.append({**selectors, "operating_case": name, "strategy_number": number})
            per_row.append(arguments)
    output.mkdir(parents=True, exist_ok=False)
    fields = [key for key in PLAN_COLUMNS if any(key in row for row in rows)]
    with (output / "cases.csv").open("x", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)
    save(output / "cases.json", {"expectedCases": len(rows), "perRowArguments": per_row})
    return len(rows)


def case_key(selectors):
    return int(selectors["strategy_number"]), selectors["operating_case"]


def report(matrix, cloud_state, output):
    with matrix.open(newline="") as stream:
        expected = {case_key(row): row for row in csv.DictReader(stream)}
    collected = {}
    for record in read_receipts(cloud_state, require_cleanup=False):
        key = case_key(record["selectors"])
        if record["phase"] != "operating" or key in collected or expected.get(key) != record["selectors"]:
            raise ValueError(f"unexpected or duplicate operating result: {key}")
        collected[key] = record
    outcomes = []
    for number in sorted({number for number, _ in expected}):
        cases = {}
        for name in SCENARIOS:
            record = collected.get((number, name))
            if not record:
                cases[name] = {"status": "MISSING"}
            else:
                cases[name] = {"status": record["status"], "backup": record["backupManifest"],
                               "cleanupConfirmed": record["cleanupConfirmed"], "checks": record.get("operatingChecks")}
        complete = all(case.get("cleanupConfirmed") and case["status"] != "RUN_ERROR" for case in cases.values())
        status = "INCOMPLETE" if not complete else "PASS" if all(case["status"] == "PASS" for case in cases.values()) else "REJECTED"
        outcomes.append({"strategy": number, "status": status, "cases": cases})
    summary = {"expectedCases": len(expected), "verifiedCases": len(collected), "outcomes": outcomes}
    save(output, summary)
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    build = sub.add_parser("prepare")
    build.add_argument("--matrix", type=Path, required=True)
    build.add_argument("--output", type=Path, required=True)
    build.add_argument("--limit", type=int, choices=range(1, 31), default=30)
    read = sub.add_parser("report")
    read.add_argument("--matrix", type=Path, required=True)
    read.add_argument("--cloud-state", type=Path, required=True)
    read.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.command == "prepare":
        print(json.dumps({"plannedCases": prepare(args.matrix, args.output, args.limit)}))
        return 0
    summary = report(args.matrix, args.cloud_state, args.output)
    counts = {status: sum(row["status"] == status for row in summary["outcomes"])
              for status in ("PASS", "REJECTED", "INCOMPLETE")}
    print(json.dumps({"verifiedCases": summary["verifiedCases"], **counts}))
    return 1 if counts["INCOMPLETE"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
