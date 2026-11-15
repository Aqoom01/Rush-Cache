"""Run the seven operating scenarios for a slice of joint strategy combinations."""

import argparse
import csv
import itertools
import json
from pathlib import Path
import subprocess
import sys
import time

from integration import row_plan_arguments, save, verify_bundle


SCENARIOS = {
    "steady": ("NORMAL", "UNIFORM", 1),
    "hotspot": ("AUTO", "HOT_RANGE", 2),
    "slow-replica": ("SLOW_REPLICA", "UNIFORM", 1),
    "scale-out": ("AUTO", "UNIFORM", 1),
    "scale-in": ("AUTO", "UNIFORM", 2),
    "network-degraded": ("NETWORK_DEGRADED", "UNIFORM", 1),
    "leader-crash": ("CRASH_LEADER", "UNIFORM", 1),
}
TOOL = Path(__file__).with_name("integration.py")
WORKLOAD_DEFAULTS = dict(base_rate=100, low_rate=20, high_rate=400, phase_seconds=30,
                         fault_seconds=8, keys=128, value_bytes=1024, concurrency=64, read=80)
DEFAULT_MIN_SUCCESS = .95


def select_strategies(matrix, start, limit):
    selected = []
    seen = set()
    with matrix.open(newline="", encoding="utf-8-sig") as stream:
        for row in itertools.islice(csv.DictReader(stream), start, start + limit):
            if row.pop("scenario", None) not in (None, "", "AUTO") or row.pop("workload", None) not in (None, "", "UNIFORM") or row.pop("seed", None) not in (None, "", "1"):
                raise ValueError("input must contain strategies only, or a matrix generated with --workloads UNIFORM --seeds 1")
            strategy = {key: value for key, value in row.items() if value not in (None, "")}
            if not {"a1", "a2", "fd", "distribution", "split", "placement", "scaler", "scheduler", "copy", "drain"} <= strategy.keys():
                raise ValueError("each strategy row must identify all A1–B3 methods")
            row_plan_arguments(strategy)
            identity = tuple(sorted(strategy.items()))
            if identity in seen:
                raise ValueError("duplicate strategy in selected rows")
            seen.add(identity)
            selected.append(strategy)
    if not selected:
        raise ValueError("no selected strategy rows")
    return selected


def case_inputs(args, strategy, name, seed):
    scenario, workload, initial = SCENARIOS[name]
    rates = {"scale-out": (args.base_rate, args.high_rate), "scale-in": (args.high_rate, args.low_rate)}
    rate, after = rates.get(name, (args.base_rate, None))
    change_at = rate * args.phase_seconds
    requests = change_at + (after or rate) * args.phase_seconds
    selectors = {**strategy, "scenario": scenario, "workload": workload, "seed": seed}
    arguments = []
    settings = {
        "initial-groups": initial, "min-groups": 1, "rate": rate, "requests": requests,
        "keys": args.keys, "value-bytes": args.value_bytes, "concurrency": args.concurrency,
        "read": args.read, "warmup": rate * 4, "fault-seconds": args.fault_seconds,
    }
    if after is not None:
        settings.update({"rate-change-at": change_at, "rate-after": after})
    for key, value in settings.items():
        arguments.extend([f"--{key}", str(value)])
    return selectors, arguments


def case_arguments(args, strategy, name, seed):
    selectors, arguments = case_inputs(args, strategy, name, seed)
    command = [*row_plan_arguments(selectors), *arguments]
    if args.topology:
        command.extend(["--topology", str(args.topology)])
    for flag in ("calibration", "history"):
        if (path := getattr(args, flag)) is not None:
            command.extend([f"--{flag}", str(path)])
    return command


def read_json(path):
    return json.loads(path.read_text()) if path.is_file() else None


def assess_case(name, result, gateway, fault, load_dir, min_success, max_p99_ms=None):
    checks = {key: result.get(key) is True for key in
              ("correctnessValid", "migrationValid", "coverageValid", "jointComponentsValid", "fdRuntimeValid")}
    offered = result["offered"]
    checks["success_fraction"] = offered > 0 and result["ok"] / offered >= min_success
    if max_p99_ms is not None:
        checks["p99"] = 0 <= result["successP99FromScheduleNanos"] <= max_p99_ms * 1_000_000
    if name in ("scale-out", "scale-in"):
        expected = 2 if name == "scale-out" else 1
        before = read_json(load_dir / "before-rate-change.json")
        checks["automatic_scale_completed"] = (
            bool(before) and len(before["activeGroups"]) == 3 - expected
            and before["pendingJobs"] == 0 and before["requestedCount"] == -1
            and gateway["scalingDecisions"] > before["scalingDecisions"] and len(gateway["activeGroups"]) == expected
            and gateway["pendingJobs"] == 0 and gateway["requestedCount"] == -1
            and all(owner["owner"] in gateway["activeGroups"] for owner in gateway["owners"])
        )
    if name in ("slow-replica", "network-degraded"):
        checks["impairment_applied_and_released"] = bool(fault and fault["evidence"]["released"])
        if name == "network-degraded" and fault:
            stats = fault["evidence"]["stats"]
            checks["traffic_exposed"] = stats.get("packets", 0) + stats.get("drops", 0) > 0
    if name == "leader-crash":
        repair = fault and fault["evidence"]["repair_admission"]["repair"]
        checks["crash_repair_completed"] = bool(repair and any(
            item["id"] == repair and item["state"] == "DONE" for item in gateway["repairs"]
        ))
        marker = load_dir / "first-post-crash-index"
        checks["post_crash_write_succeeded"] = False
        if marker.is_file():
            index = int(marker.read_text())
            with (load_dir / "requests.csv").open(newline="") as stream:
                checks["post_crash_write_succeeded"] = any(
                    row["phase"] == "measure" and int(row["index"]) >= index
                    and row["command"] == "Set" and row["status"] == "0"
                    for row in csv.DictReader(stream)
                )
    return checks


def run_case(args, strategy, name, seed, directory):
    directory.mkdir()
    plan, run = directory / "plan", directory / "run"
    started = time.monotonic()
    with (directory / "console.log").open("x", encoding="utf-8") as log:
        plan_cmd = ["plan", "--output", str(plan), *case_arguments(args, strategy, name, seed)]
        exit_code = subprocess.run([sys.executable, str(TOOL), *plan_cmd], stdout=log, stderr=subprocess.STDOUT).returncode
        if exit_code == 0:
            run_cmd = ["ec2", "--execute", "--bundle", str(args.bundle), "--plan", str(plan), "--output", str(run),
                       "--identity", str(args.identity), "--known-hosts", str(args.known_hosts), "--java", args.java,
                       "--timeout", str(args.timeout), "--sample-seconds", str(args.sample_seconds)]
            exit_code = subprocess.run([sys.executable, str(TOOL), *run_cmd], stdout=log, stderr=subprocess.STDOUT).returncode
    cleanup = read_json(run / "cleanup.json")
    result = read_json(run / "result.json")
    failure = read_json(run / "RUN_FAILED.json")
    record = {"scenario": name, "seed": seed, "wall_seconds": round(time.monotonic() - started, 3),
              "path": str(directory), "exit_code": exit_code}
    if cleanup and cleanup.get("complete") is not True:
        return {**record, "status": "CLEANUP_INCOMPLETE", "cleanup": cleanup}
    if exit_code or not result:
        unsupported = failure and failure["reason"].startswith("UNSUPPORTED:")
        return {**record, "status": "UNSUPPORTED" if unsupported else "RUN_ERROR",
                "failure": failure, "cleanup": cleanup}
    checks = assess_case(name, result, read_json(run / "gateway-final-controller.json"), read_json(run / "fault.json"),
                         run / "collected/load/load-result", args.min_success, args.max_p99_ms)
    status = "PASS" if all(checks.values()) else "FAILED_VALIDATION"
    return {**record, "status": status, "checks": checks, "result": result}


def execute(args, strategies):
    for path in (args.topology, args.identity, args.known_hosts):
        if path is None or not path.is_file():
            raise ValueError("execution requires an existing topology, SSH identity and known_hosts")
    topology = read_json(args.topology)
    if len(set([*topology["replica_hosts"], topology["gateway_host"], topology["load_host"]])) != 8 or len(topology["replica_hosts"]) != 6:
        raise ValueError("operating suite requires two groups of three replica hosts, a gateway and a load host")
    verify_bundle(args.bundle)
    args.output.mkdir(parents=True, exist_ok=False)
    save(args.output / "suite-input.json", {**vars(args), "selected": len(strategies), "scenarios": list(SCENARIOS),
                                         "identity": "provided"} | {
        name: str(value) for name, value in vars(args).items() if isinstance(value, Path) and name != "identity"
    })
    outcomes = []
    for number, strategy in enumerate(strategies, start=args.start):
        directory = args.output / f"strategy-{number:08}"
        directory.mkdir()
        cases = []
        stop_campaign = False
        for name, seed in itertools.product(SCENARIOS, args.seeds):
            case = run_case(args, strategy, name, seed, directory / f"{name}-seed-{seed}")
            cases.append(case)
            save(directory / "cases.json", cases)
            print(json.dumps({"strategy": number, **{k: case[k] for k in ("scenario", "seed", "status", "wall_seconds")}}), flush=True)
            if case["status"] == "CLEANUP_INCOMPLETE" or case["status"] == "RUN_ERROR" and not case["cleanup"]:
                stop_campaign = True
                break
            if args.stop_on_failed_case and case["status"] != "PASS":
                break
        passed = len(cases) == len(SCENARIOS) * len(args.seeds) and all(case["status"] == "PASS" for case in cases)
        incomplete = stop_campaign or any(case["status"] == "RUN_ERROR" for case in cases)
        status = "PASS" if passed else "INCONCLUSIVE" if incomplete else "REJECTED"
        outcome = {"strategy": number, "selectors": strategy, "status": status,
                   "collected": len(cases), "planned": len(SCENARIOS) * len(args.seeds),
                   "wall_seconds": round(sum(case["wall_seconds"] for case in cases), 3)}
        save(directory / "outcome.json", outcome)
        outcomes.append(outcome)
        save(args.output / "suite-results.json", {"planned_strategies": len(strategies), "outcomes": outcomes})
        if stop_campaign:
            break
    return 2 if len(outcomes) != len(strategies) or any(row["status"] == "INCONCLUSIVE" for row in outcomes) else 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--matrix", type=Path, required=True)
    parser.add_argument("--start", type=int, default=0)
    parser.add_argument("--limit", type=int, default=1)
    parser.add_argument("--seeds", type=int, nargs="+", default=[1])
    parser.add_argument("--topology", type=Path)
    parser.add_argument("--bundle", type=Path)
    parser.add_argument("--identity", type=Path)
    parser.add_argument("--known-hosts", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--calibration", type=Path)
    parser.add_argument("--history", type=Path)
    parser.add_argument("--java", default="java")
    parser.add_argument("--timeout", type=float, default=900)
    parser.add_argument("--sample-seconds", type=float, default=2)
    for name, default in WORKLOAD_DEFAULTS.items():
        parser.add_argument("--" + name.replace("_", "-"), type=float if name == "fault_seconds" else int, default=default)
    parser.add_argument("--min-success", type=float, default=DEFAULT_MIN_SUCCESS)
    parser.add_argument("--max-p99-ms", type=float)
    parser.add_argument("--stop-on-failed-case", action="store_true")
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args()
    if not (args.start >= 0 and 1 <= args.limit <= 100_000 and len(set(args.seeds)) == len(args.seeds)
            and 0 < args.low_rate < args.base_rate < args.high_rate <= 100_000
            and 0 < args.fault_seconds <= 30 and args.phase_seconds >= 15
            and 2 * args.phase_seconds > args.fault_seconds + 15
            and (args.high_rate + args.base_rate) * args.phase_seconds <= 10_000_000
            and 0 < args.min_success <= 1 and 0 <= args.read < 100
            and 8 <= args.keys and 1 <= args.concurrency <= 1024
            and (args.max_p99_ms is None or args.max_p99_ms > 0)):
        parser.error("invalid slice, seeds, workload, timing, or success thresholds")
    strategies = select_strategies(args.matrix, args.start, args.limit)
    if not args.execute:
        print(json.dumps({"dry_run": True, "strategies": len(strategies), "scenarios": list(SCENARIOS),
                          "seeds": args.seeds, "planned_runs": len(strategies) * len(SCENARIOS) * len(args.seeds),
                          "first_strategy": strategies[0]}, indent=2))
        return 0
    if args.bundle is None or args.output is None:
        parser.error("--execute requires --bundle and --output")
    return execute(args, strategies)


if __name__ == "__main__":
    raise SystemExit(main())
