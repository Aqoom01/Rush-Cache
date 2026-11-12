#!/usr/bin/env python3
"""Validate one product matrix or the merged campaign report before advancing."""

from __future__ import annotations

import argparse
import csv
import json
from collections import Counter
from pathlib import Path


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8", newline="") as source:
        return list(csv.DictReader(source))


def read_properties(path: Path) -> dict[str, str]:
    result: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if line and not line.startswith("#") and "=" in line:
            key, value = line.split("=", 1)
            result[key] = value
    return result


def require(condition: bool, message: str) -> None:
    if not condition:
        raise SystemExit(message)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--stage-input", type=Path)
    parser.add_argument("--expected-total", type=int, required=True)
    parser.add_argument("--expected-profiles", type=int, default=5)
    parser.add_argument("--expected-faults", type=int, required=True)
    parser.add_argument("--expected-runs-per-profile", type=int, required=True)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()

    runs = read_csv(args.report / "runs.csv")
    summaries = read_csv(args.report / "summary.csv")
    require(len(runs) == args.expected_total, f"run total mismatch: {len(runs)}")
    require(
        all(row["valid_observation_window"] == "True" for row in runs),
        "report contains an invalid observation window",
    )
    require(
        sum(int(row["pre_fault_suspect_events"]) for row in runs) == 0,
        "pre-fault SUSPECT events are not zero",
    )
    require(
        sum(int(row["pre_fault_dead_events"]) for row in runs) == 0,
        "pre-fault DEAD events are not zero",
    )

    profile_counts = Counter(row["profile"] for row in runs)
    fault_counts = Counter(row["fault"] for row in runs)
    require(len(profile_counts) == args.expected_profiles, "profile count mismatch")
    require(len(fault_counts) == args.expected_faults, "fault count mismatch")
    require(
        all(value == args.expected_runs_per_profile for value in profile_counts.values()),
        f"profile run counts mismatch: {dict(profile_counts)}",
    )
    expected_per_fault = args.expected_total // args.expected_faults
    require(
        all(value == expected_per_fault for value in fault_counts.values()),
        f"fault run counts mismatch: {dict(fault_counts)}",
    )
    require(
        len(summaries) == args.expected_profiles * args.expected_faults,
        "summary row count mismatch",
    )
    require(
        all(int(row["invalid_observation_runs"]) == 0 for row in summaries),
        "summary contains invalid observation runs",
    )

    stage_evidence: dict[str, object] | None = None
    if args.stage_input is not None:
        completed = read_properties(args.stage_input / "COMPLETED")
        with (args.stage_input / "plan.tsv").open(encoding="utf-8", newline="") as source:
            plan = list(csv.DictReader(source, delimiter="\t"))
        status = read_csv(args.stage_input / "execution-status.csv")
        passed = {
            int(row["sequence"]): row["run_dir"]
            for row in status
            if row.get("status") == "passed"
        }
        require(len(plan) == args.expected_total, "stage plan count mismatch")
        require(len(passed) == args.expected_total, "stage passed checkpoint count mismatch")
        require(
            int(completed.get("runs", "-1")) == args.expected_total,
            "COMPLETED marker run count mismatch",
        )
        require(
            all((Path(run_dir) / "result.json").exists() for run_dir in passed.values()),
            "a passed checkpoint is missing result.json",
        )
        stage_evidence = {
            "plan_runs": len(plan),
            "passed_sequences": len(passed),
            "historical_failed_records": sum(
                row.get("status") == "failed" for row in status
            ),
        }

    result = {
        "status": "passed",
        "report": str(args.report),
        "runs": len(runs),
        "profiles": dict(sorted(profile_counts.items())),
        "faults": dict(sorted(fault_counts.items())),
        "pre_fault_suspect_events": 0,
        "pre_fault_dead_events": 0,
        "stage_evidence": stage_evidence,
    }
    rendered = json.dumps(result, ensure_ascii=False, indent=2) + "\n"
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered, encoding="utf-8")
    print(rendered, end="")


if __name__ == "__main__":
    main()
