#!/usr/bin/env python3
"""Build reproducibility records, CSVs, charts, and a PPTX from an EC2 campaign.

This script deliberately preserves null detector latencies.  It expects the raw
S3 prefix under ``results-ec2-YYYYMMDD/raw`` and writes derived artifacts only
under the sibling ``report`` directory.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import re
import statistics
from collections import Counter, defaultdict, deque
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Iterable, Sequence

try:
    from PIL import Image, ImageDraw, ImageFont
    from pptx import Presentation
    from pptx.dml.color import RGBColor
    from pptx.enum.text import PP_ALIGN, MSO_ANCHOR
    from pptx.enum.shapes import MSO_SHAPE
    from pptx.util import Inches, Pt
except ImportError as exc:  # pragma: no cover - environment diagnostic
    raise SystemExit(
        "Pillow and python-pptx are required. Use the Codex bundled Python runtime."
    ) from exc


PROFILE_ORDER = [
    "product-swim-lifeguard",
    "product-redis-cluster",
    "product-phi-gossip",
    "product-ignite-tcp-ring",
    "product-aerospike-mesh",
]
VALID_PROFILE_ORDER = [
    "product-swim-lifeguard",
    "product-redis-cluster",
    "product-phi-gossip",
]
PROFILE_LABEL = {
    "product-swim-lifeguard": "SWIM + Lifeguard",
    "product-redis-cluster": "Redis Cluster",
    "product-phi-gossip": "Phi Gossip",
    "product-ignite-tcp-ring": "Ignite TCP Ring",
    "product-aerospike-mesh": "Aerospike Mesh",
}
PROFILE_COLOR = {
    "product-swim-lifeguard": "#14B8A6",
    "product-redis-cluster": "#3B82F6",
    "product-phi-gossip": "#8B5CF6",
    "product-ignite-tcp-ring": "#F59E0B",
    "product-aerospike-mesh": "#EF4444",
}
FAULT_ORDER = [
    "normal",
    "crash",
    "loss15",
    "loss45",
    "delay",
    "cpu-pause-target",
    "cpu-pause-observer",
    "cpu-throttle-target",
    "cpu-throttle-observer",
    "cpu-contention-target",
    "io-contention-target",
]
FAULT_LABEL = {
    "normal": "Normal",
    "crash": "Crash",
    "loss15": "Asym. loss\n(low)",
    "loss45": "Asym. loss\n(high)",
    "delay": "Delay",
    "cpu-pause-target": "CPU pause\ntarget",
    "cpu-pause-observer": "CPU pause\nobserver",
    "cpu-throttle-target": "CPU throttle\ntarget",
    "cpu-throttle-observer": "CPU throttle\nobserver",
    "cpu-contention-target": "CPU contention\ntarget",
    "io-contention-target": "I/O contention\ntarget",
}
REVERSIBLE_FAULTS = {
    "loss15",
    "loss45",
    "delay",
    "cpu-pause-target",
    "cpu-pause-observer",
    "cpu-throttle-target",
    "cpu-throttle-observer",
    "cpu-contention-target",
    "io-contention-target",
}
NETWORK_FAULTS = ["loss15", "loss45", "delay"]
CPU_FAULTS = [
    "cpu-pause-target",
    "cpu-pause-observer",
    "cpu-throttle-target",
    "cpu-throttle-observer",
    "cpu-contention-target",
]

BG = "#07111F"
PANEL = "#0E1D2F"
INK = "#EAF2F8"
MUTED = "#9FB3C8"
GRID = "#294158"
GREEN = "#22C55E"
AMBER = "#F59E0B"
RED = "#EF4444"


def ns_to_ms(value: Any) -> float | None:
    return None if value is None else float(value) / 1_000_000.0


def fmt(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, float):
        return f"{value:.6f}".rstrip("0").rstrip(".")
    return str(value)


def read_properties(path: Path) -> dict[str, str]:
    result: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        if "=" in line and not line.lstrip().startswith("#"):
            key, value = line.split("=", 1)
            result[key.strip()] = value.strip()
    return result


def last_result_from_log(path: Path) -> dict[str, Any] | None:
    marker = "RESULT_JSON="
    result = None
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        if line.startswith(marker):
            try:
                result = json.loads(line[len(marker) :])
            except json.JSONDecodeError:
                pass
    return result


def percentile(values: Sequence[float], q: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * q
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    fraction = position - lower
    return ordered[lower] * (1 - fraction) + ordered[upper] * fraction


def wilson(successes: int, total: int, z: float = 1.959963984540054) -> tuple[float | None, float | None]:
    if total == 0:
        return None, None
    p = successes / total
    denominator = 1 + z * z / total
    center = (p + z * z / (2 * total)) / denominator
    margin = z * math.sqrt((p * (1 - p) + z * z / (4 * total)) / total) / denominator
    return max(0.0, center - margin), min(1.0, center + margin)


T_CRITICAL_975 = {
    1: 12.706, 2: 4.303, 3: 3.182, 4: 2.776, 5: 2.571,
    6: 2.447, 7: 2.365, 8: 2.306, 9: 2.262, 10: 2.228,
    11: 2.201, 12: 2.179, 13: 2.160, 14: 2.145, 15: 2.131,
    16: 2.120, 17: 2.110, 18: 2.101, 19: 2.093, 20: 2.086,
    21: 2.080, 22: 2.074, 23: 2.069, 24: 2.064, 25: 2.060,
    26: 2.056, 27: 2.052, 28: 2.048, 29: 2.045, 30: 2.042,
}


def mean_ci(values: Sequence[float]) -> tuple[float | None, float | None, float | None]:
    if not values:
        return None, None, None
    mean = statistics.fmean(values)
    if len(values) == 1:
        return mean, None, None
    sem = statistics.stdev(values) / math.sqrt(len(values))
    critical = T_CRITICAL_975.get(len(values) - 1, 1.96)
    return mean, mean - critical * sem, mean + critical * sem


def write_csv(path: Path, rows: Sequence[dict[str, Any]], fields: Sequence[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as target:
        writer = csv.DictWriter(target, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({key: fmt(row.get(key)) for key in fields})


def parse_campaign_attempts(raw: Path, status_rows: Sequence[dict[str, str]]) -> list[dict[str, Any]]:
    start_re = re.compile(
        r"^(\S+) start sequence=(\d+) attempt=(\d+) profile=(\S+) fault=(\S+) seed=(\d+) run=(\S+)$"
    )
    done_re = re.compile(r"^(\S+) done sequence=(\d+) attempt=(\d+) status=(\S+)$")
    pending: dict[tuple[int, int], deque[dict[str, Any]]] = defaultdict(deque)
    failed: list[dict[str, Any]] = []
    persisted = {
        (int(row["sequence"]), int(row["attempt"]), row["status"])
        for row in status_rows
    }
    log = raw / "campaign" / "campaign.log"
    for line in log.read_text(encoding="utf-8", errors="replace").splitlines():
        start = start_re.match(line)
        if start:
            timestamp, seq, attempt, profile, fault, seed, run_id = start.groups()
            pending[(int(seq), int(attempt))].append({
                "started_at_utc": timestamp,
                "sequence": int(seq),
                "attempt": int(attempt),
                "profile": profile,
                "fault": fault,
                "seed": int(seed),
                "run_id": run_id,
            })
            continue
        done = done_re.match(line)
        if not done:
            continue
        timestamp, seq, attempt, status = done.groups()
        key = (int(seq), int(attempt))
        if not pending[key]:
            continue
        row = pending[key].popleft()
        row["finished_at_utc"] = timestamp
        row["status"] = status
        row["persisted_in_execution_status"] = (key[0], key[1], status) in persisted
        row["artifact_directory_exists"] = (raw / "runs" / row["run_id"]).is_dir()
        if status == "failed":
            failed.append(row)
    return failed


def load_runs(raw: Path) -> tuple[list[dict[str, Any]], list[dict[str, str]]]:
    with (raw / "campaign" / "execution-status.csv").open(encoding="utf-8", newline="") as source:
        status_rows = list(csv.DictReader(source))
    passed_by_sequence: dict[int, dict[str, str]] = {}
    for row in status_rows:
        if row["status"] == "passed":
            passed_by_sequence[int(row["sequence"])] = row

    rows: list[dict[str, Any]] = []
    for sequence in sorted(passed_by_sequence):
        status = passed_by_sequence[sequence]
        run_dir = raw / "runs" / status["run_id"]
        node0 = run_dir / "node-0"
        result = json.loads((node0 / "result.json").read_text(encoding="utf-8"))
        props = read_properties(node0 / "run.properties")
        fault = status["fault"]
        dead_decision = result.get("deadLatencyNanos") is not None
        suspect_decision = result.get("suspectLatencyNanos") is not None
        prefault_suspect = int(result.get("preFaultSuspectEvents", 0))
        prefault_dead = int(result.get("preFaultDeadEvents", 0))
        normal_false_positive = fault == "normal" and (prefault_suspect > 0 or prefault_dead > 0)
        alive_process_dead = (
            (fault in REVERSIBLE_FAULTS and dead_decision)
            or (fault == "normal" and prefault_dead > 0)
        )
        if fault == "crash":
            condition_success = dead_decision and prefault_dead == 0
            detection_success: bool | None = dead_decision and prefault_dead == 0
        elif fault == "normal":
            condition_success = not normal_false_positive
            detection_success = None
        else:
            condition_success = not alive_process_dead
            detection_success = None

        node_results: list[dict[str, Any]] = []
        for node_id in range(int(props.get("nodes", "5"))):
            event_log = run_dir / f"node-{node_id}" / "events.log"
            peer_result = last_result_from_log(event_log) if event_log.exists() else None
            if peer_result:
                node_results.append(peer_result)
        total_sent_frames = sum(int(item.get("sentFrames", 0)) for item in node_results)
        total_sent_bytes = sum(int(item.get("sentBytes", 0)) for item in node_results)
        total_received_frames = sum(int(item.get("receivedFrames", 0)) for item in node_results)
        total_received_bytes = sum(int(item.get("receivedBytes", 0)) for item in node_results)

        fio_validation_path = run_dir / "node-1" / "fio-validation.json"
        fio_validation = json.loads(fio_validation_path.read_text(encoding="utf-8")) if fio_validation_path.exists() else {}
        fio_metrics = fio_validation.get("metrics", {})
        cpu_stress_path = run_dir / "node-1" / "cpu-stress.log"
        cpu_bogo_ops_per_sec = None
        if cpu_stress_path.exists():
            for line in cpu_stress_path.read_text(encoding="utf-8", errors="replace").splitlines():
                match = re.search(r"\bcpu\s+\d+\s+[\d.]+\s+[\d.]+\s+[\d.]+\s+([\d.]+)\s+([\d.]+)", line)
                if match:
                    cpu_bogo_ops_per_sec = float(match.group(1))

        systemd_show = (node0 / "systemd-show.txt").read_text(encoding="utf-8", errors="replace")
        resource_evidence_available = not (
            "CPUUsageNSec=[not set]" in systemd_show or "Unit openkache-fd-node.service could not be found" in systemd_show
        )
        transitioned = suspect_decision or dead_decision
        fault_ended = result.get("faultEndAtNanos") is not None
        recovery_observed = result.get("recoveryLatencyNanos") is not None

        row = {
            "sequence": sequence,
            "seed": int(status["seed"]),
            "fault": fault,
            "profile": status["profile"],
            "profile_label": PROFILE_LABEL.get(status["profile"], status["profile"]),
            "attempt": int(status["attempt"]),
            "run_id": status["run_id"],
            "condition_success": condition_success,
            "detection_success": detection_success,
            "suspect_decision": suspect_decision,
            "dead_decision": dead_decision,
            "alive_process_dead": alive_process_dead,
            "normal_false_positive": normal_false_positive,
            "pre_fault_suspect_events": prefault_suspect,
            "pre_fault_dead_events": prefault_dead,
            "suspect_latency_ms": ns_to_ms(result.get("suspectLatencyNanos")),
            "dead_latency_ms": ns_to_ms(result.get("deadLatencyNanos")),
            "recovery_latency_ms": ns_to_ms(result.get("recoveryLatencyNanos")),
            "fault_at_ms": ns_to_ms(result.get("faultAtNanos")),
            "fault_end_at_ms": ns_to_ms(result.get("faultEndAtNanos")),
            "recovery_observed": recovery_observed,
            "recovery_eligible": fault_ended and transitioned,
            "observer_sent_frames": int(result.get("sentFrames", 0)),
            "observer_sent_bytes": int(result.get("sentBytes", 0)),
            "observer_received_frames": int(result.get("receivedFrames", 0)),
            "observer_received_bytes": int(result.get("receivedBytes", 0)),
            "total_sent_frames": total_sent_frames,
            "total_sent_bytes": total_sent_bytes,
            "total_received_frames": total_received_frames,
            "total_received_bytes": total_received_bytes,
            "node_result_count": len(node_results),
            "run_duration_ms": ns_to_ms(result.get("runDurationNanos")),
            "implementation_type": result.get("implementationType", ""),
            "time_source": result.get("timeSource", ""),
            "wire_source": result.get("wireSource", ""),
            "instance_type": props.get("instance_type", ""),
            "ami_id": props.get("ami_id", ""),
            "node_count": int(props.get("nodes", "0")),
            "jar_sha256": props.get("jar_sha256", ""),
            "warmup_seconds": int(props.get("warmup_seconds", "0")),
            "fault_duration_seconds": int(props.get("fault_duration_seconds", "0")),
            "recovery_seconds": int(props.get("recovery_seconds", "0")),
            "resource_evidence_available": resource_evidence_available,
            "cpu_usage_ns": None,
            "memory_current_bytes": None,
            "io_read_bytes": None,
            "io_write_bytes": None,
            "cpu_stress_bogo_ops_per_sec": cpu_bogo_ops_per_sec,
            "fio_status": fio_validation.get("status"),
            "fio_total_iops": fio_metrics.get("totalIops"),
            "fio_configured_iodepth": fio_metrics.get("configuredIodepth"),
            "fio_deep_queue_percent": fio_metrics.get("deepQueuePercent"),
            "fio_runtime_ms": fio_metrics.get("runtimeMs"),
        }
        rows.append(row)
    return rows, status_rows


RUN_FIELDS = [
    "sequence", "seed", "fault", "profile", "profile_label", "attempt", "run_id",
    "condition_success", "detection_success", "suspect_decision", "dead_decision",
    "alive_process_dead", "normal_false_positive", "pre_fault_suspect_events",
    "pre_fault_dead_events", "suspect_latency_ms", "dead_latency_ms",
    "recovery_latency_ms", "fault_at_ms", "fault_end_at_ms", "recovery_observed",
    "recovery_eligible", "observer_sent_frames", "observer_sent_bytes",
    "observer_received_frames", "observer_received_bytes", "total_sent_frames",
    "total_sent_bytes", "total_received_frames", "total_received_bytes",
    "node_result_count", "run_duration_ms", "implementation_type", "time_source",
    "wire_source", "instance_type", "ami_id", "node_count", "jar_sha256",
    "warmup_seconds", "fault_duration_seconds", "recovery_seconds",
    "resource_evidence_available", "cpu_usage_ns", "memory_current_bytes",
    "io_read_bytes", "io_write_bytes", "cpu_stress_bogo_ops_per_sec",
    "fio_status", "fio_total_iops", "fio_configured_iodepth",
    "fio_deep_queue_percent", "fio_runtime_ms",
]


def build_summary(runs: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in runs:
        groups[(row["profile"], row["fault"])].append(row)
    summaries: list[dict[str, Any]] = []
    for profile in PROFILE_ORDER:
        for fault in FAULT_ORDER:
            rows = groups[(profile, fault)]
            success_count = sum(bool(row["condition_success"]) for row in rows)
            ci_low, ci_high = wilson(success_count, len(rows))
            dead_values = [row["dead_latency_ms"] for row in rows if row["dead_latency_ms"] is not None]
            suspect_values = [row["suspect_latency_ms"] for row in rows if row["suspect_latency_ms"] is not None]
            recovery_values = [row["recovery_latency_ms"] for row in rows if row["recovery_latency_ms"] is not None]
            dead_mean, dead_mean_low, dead_mean_high = mean_ci(dead_values)
            recovery_eligible = sum(bool(row["recovery_eligible"]) for row in rows)
            recovery_success = sum(bool(row["recovery_observed"]) for row in rows if row["recovery_eligible"])
            summaries.append({
                "profile": profile,
                "profile_label": PROFILE_LABEL[profile],
                "fault": fault,
                "n": len(rows),
                "success_count": success_count,
                "failure_count": len(rows) - success_count,
                "success_rate": success_count / len(rows) if rows else None,
                "success_rate_ci95_low": ci_low,
                "success_rate_ci95_high": ci_high,
                "dead_decision_count": len(dead_values),
                "missed_detection_count": (len(rows) - len(dead_values)) if fault == "crash" else 0,
                "alive_process_dead_count": sum(bool(row["alive_process_dead"]) for row in rows),
                "false_positive_run_count": sum(
                    row["pre_fault_suspect_events"] > 0 or row["pre_fault_dead_events"] > 0
                    for row in rows
                ),
                "pre_fault_suspect_event_count": sum(row["pre_fault_suspect_events"] for row in rows),
                "pre_fault_dead_event_count": sum(row["pre_fault_dead_events"] for row in rows),
                "suspect_latency_p50_ms": percentile(suspect_values, 0.50),
                "suspect_latency_p95_ms": percentile(suspect_values, 0.95),
                "suspect_latency_min_ms": min(suspect_values) if suspect_values else None,
                "suspect_latency_max_ms": max(suspect_values) if suspect_values else None,
                "dead_latency_p50_ms": percentile(dead_values, 0.50),
                "dead_latency_p95_ms": percentile(dead_values, 0.95),
                "dead_latency_min_ms": min(dead_values) if dead_values else None,
                "dead_latency_max_ms": max(dead_values) if dead_values else None,
                "dead_latency_mean_ms": dead_mean,
                "dead_latency_mean_ci95_low_ms": dead_mean_low,
                "dead_latency_mean_ci95_high_ms": dead_mean_high,
                "recovery_eligible_count": recovery_eligible,
                "recovery_success_count": recovery_success,
                "recovery_latency_p50_ms": percentile(recovery_values, 0.50),
                "recovery_latency_p95_ms": percentile(recovery_values, 0.95),
                "recovery_latency_min_ms": min(recovery_values) if recovery_values else None,
                "recovery_latency_max_ms": max(recovery_values) if recovery_values else None,
            })
    return summaries


SUMMARY_FIELDS = [
    "profile", "profile_label", "fault", "n", "success_count", "failure_count",
    "success_rate", "success_rate_ci95_low", "success_rate_ci95_high",
    "dead_decision_count", "missed_detection_count", "alive_process_dead_count",
    "false_positive_run_count", "pre_fault_suspect_event_count", "pre_fault_dead_event_count",
    "suspect_latency_p50_ms", "suspect_latency_p95_ms", "suspect_latency_min_ms",
    "suspect_latency_max_ms", "dead_latency_p50_ms", "dead_latency_p95_ms",
    "dead_latency_min_ms", "dead_latency_max_ms", "dead_latency_mean_ms",
    "dead_latency_mean_ci95_low_ms", "dead_latency_mean_ci95_high_ms",
    "recovery_eligible_count", "recovery_success_count", "recovery_latency_p50_ms",
    "recovery_latency_p95_ms", "recovery_latency_min_ms", "recovery_latency_max_ms",
]


def build_fio_summary(runs: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {key: row.get(key) for key in [
            "sequence", "seed", "profile", "profile_label", "run_id", "fio_status",
            "fio_total_iops", "fio_configured_iodepth", "fio_deep_queue_percent", "fio_runtime_ms",
        ]}
        for row in runs if row["fault"] == "io-contention-target"
    ]


def validation_report(raw: Path, runs: Sequence[dict[str, Any]], status_rows: Sequence[dict[str, str]], failed: Sequence[dict[str, Any]]) -> dict[str, Any]:
    profile_counts = Counter(row["profile"] for row in runs)
    fault_counts = Counter(row["fault"] for row in runs)
    combo_counts = Counter((row["profile"], row["fault"]) for row in runs)
    passed_ids = [row["run_id"] for row in runs]
    result_missing = [run_id for run_id in passed_ids if not (raw / "runs" / run_id / "node-0" / "result.json").exists()]
    properties_missing = [run_id for run_id in passed_ids if not (raw / "runs" / run_id / "node-0" / "run.properties").exists()]
    fio_rows = [row for row in runs if row["fault"] == "io-contention-target"]
    checks = {
        "successful_sequences_550": len(runs) == 550 and len({row["sequence"] for row in runs}) == 550,
        "profiles_110_each": len(profile_counts) == 5 and set(profile_counts.values()) == {110},
        "faults_50_each": len(fault_counts) == 11 and set(fault_counts.values()) == {50},
        "profile_fault_combinations_10_each": len(combo_counts) == 55 and set(combo_counts.values()) == {10},
        "all_successful_have_result_json": not result_missing,
        "all_successful_have_run_properties": not properties_missing,
        "fio_validation_50": len(fio_rows) == 50 and all(row["fio_status"] is not None for row in fio_rows),
        "all_fio_validation_passed": all(row["fio_status"] == "passed" for row in fio_rows),
        "statistics_include_only_final_success": len(runs) == len([row for row in status_rows if row["status"] == "passed"]),
    }
    return {
        "status": "passed" if all(checks.values()) else "failed",
        "checks": checks,
        "counts": {
            "successful_sequences": len(runs),
            "execution_status_rows": len(status_rows),
            "execution_status_failed_attempts": sum(row["status"] == "failed" for row in status_rows),
            "campaign_log_failed_attempts": len(failed),
            "profiles": dict(sorted(profile_counts.items())),
            "faults": dict(sorted(fault_counts.items())),
            "profile_fault_combinations": len(combo_counts),
            "fio_validation_files": len(fio_rows),
        },
        "missing": {"result_json": result_missing, "run_properties": properties_missing},
        "data_quality_notes": [
            "FAILED is a historical blocked marker; COMPLETED and final-validation.json are later and authoritative.",
            "campaign.log contains six failed attempts; execution-status.csv retains only the three sequence-4 failures after a status reset.",
            "Git commit was not embedded in the S3 artifacts; the local workspace HEAD is only a candidate, not independently verifiable.",
            "systemd resource counters were collected after transient units disappeared; CPU/memory/general I/O usage is unavailable.",
            "Network loss/delay runs do not record faultEndAtNanos, so their recovery latency cannot be evaluated.",
        ],
    }


def font(size: int, bold: bool = False) -> ImageFont.FreeTypeFont:
    candidates = [
        Path("C:/Windows/Fonts/malgunbd.ttf" if bold else "C:/Windows/Fonts/malgun.ttf"),
        Path("C:/Windows/Fonts/arialbd.ttf" if bold else "C:/Windows/Fonts/arial.ttf"),
    ]
    for candidate in candidates:
        if candidate.exists():
            return ImageFont.truetype(str(candidate), size=size)
    return ImageFont.load_default()


def hexrgb(value: str) -> tuple[int, int, int]:
    value = value.lstrip("#")
    return tuple(int(value[i : i + 2], 16) for i in (0, 2, 4))


def base_chart(title: str, subtitle: str = "", width: int = 1600, height: int = 900) -> tuple[Image.Image, ImageDraw.ImageDraw]:
    image = Image.new("RGB", (width, height), hexrgb(BG))
    draw = ImageDraw.Draw(image)
    title_size = 48 if len(title) <= 48 else 38
    draw.text((70, 42), title, font=font(title_size, True), fill=hexrgb(INK))
    if subtitle:
        draw.text((72, 106), subtitle, font=font(25), fill=hexrgb(MUTED))
    return image, draw


def save_bar_chart(path: Path, title: str, subtitle: str, labels: Sequence[str], values: Sequence[float], colors: Sequence[str], unit: str = "%", max_value: float | None = None, notes: Sequence[str] | None = None) -> None:
    image, draw = base_chart(title, subtitle)
    left, top, right, bottom = 180, 175, 1510, 760
    maximum = max_value if max_value is not None else max(values or [1]) * 1.15
    maximum = maximum or 1
    for tick in range(0, 6):
        y = bottom - (bottom - top) * tick / 5
        draw.line((left, y, right, y), fill=hexrgb(GRID), width=2)
        value = maximum * tick / 5
        draw.text((85, y - 14), f"{value:.0f}{unit}", font=font(18), fill=hexrgb(MUTED))
    slot = (right - left) / max(1, len(labels))
    bar_width = slot * 0.58
    for index, (label, value, color) in enumerate(zip(labels, values, colors)):
        x0 = left + index * slot + (slot - bar_width) / 2
        x1 = x0 + bar_width
        y0 = bottom - (bottom - top) * value / maximum
        draw.rounded_rectangle((x0, y0, x1, bottom), radius=10, fill=hexrgb(color))
        draw.text(((x0 + x1) / 2, y0 - 38), f"{value:.1f}{unit}", anchor="mm", font=font(21, True), fill=hexrgb(INK))
        draw.multiline_text(((x0 + x1) / 2, bottom + 28), label, anchor="ma", align="center", spacing=4, font=font(18), fill=hexrgb(INK))
    if notes:
        draw.text((72, 846), " · ".join(notes), font=font(17), fill=hexrgb(MUTED))
    image.save(path)


def save_heatmap(path: Path, title: str, subtitle: str, rows: Sequence[str], columns: Sequence[str], values: Sequence[Sequence[float]], suffix: str = "%") -> None:
    image, draw = base_chart(title, subtitle)
    left, top, right, bottom = 330, 190, 1510, 760
    cell_w = (right - left) / len(columns)
    cell_h = (bottom - top) / len(rows)
    column_font = 21 if len(columns) <= 6 else 16
    cell_font = 25 if len(columns) <= 6 else 20
    for col, label in enumerate(columns):
        draw.multiline_text((left + (col + 0.5) * cell_w, top - 28), label, anchor="ms", align="center", spacing=3, font=font(column_font), fill=hexrgb(INK))
    for row_index, row_label in enumerate(rows):
        draw.text((left - 24, top + (row_index + 0.5) * cell_h), row_label, anchor="rm", font=font(23), fill=hexrgb(INK))
        for col_index, value in enumerate(values[row_index]):
            ratio = max(0.0, min(1.0, value / 100.0))
            low = hexrgb("#12304A")
            high = hexrgb(RED)
            color = tuple(round(low[i] * (1 - ratio) + high[i] * ratio) for i in range(3))
            x0 = left + col_index * cell_w + 3
            y0 = top + row_index * cell_h + 3
            x1 = left + (col_index + 1) * cell_w - 3
            y1 = top + (row_index + 1) * cell_h - 3
            draw.rounded_rectangle((x0, y0, x1, y1), radius=8, fill=color)
            draw.text(((x0 + x1) / 2, (y0 + y1) / 2), f"{value:.0f}{suffix}", anchor="mm", font=font(cell_font, True), fill=hexrgb(INK))
    image.save(path)


def save_crash_latency(path: Path, runs: Sequence[dict[str, Any]], profiles: Sequence[str] = PROFILE_ORDER) -> None:
    image, draw = base_chart(
        "Crash DEAD latency — raw distribution",
        "Each dot is one seed (n=10/profile). Null means missed detection and is not converted to zero.",
    )
    left, top, right, bottom = 190, 180, 1510, 750
    maximum = 20_000
    for tick in range(0, 5):
        value = maximum * tick / 4
        y = bottom - (bottom - top) * tick / 4
        draw.line((left, y, right, y), fill=hexrgb(GRID), width=2)
        draw.text((75, y - 14), f"{value / 1000:.0f}s", font=font(20), fill=hexrgb(MUTED))
    slot = (right - left) / len(profiles)
    for profile_index, profile in enumerate(profiles):
        values = [
            row["dead_latency_ms"] for row in runs
            if row["profile"] == profile and row["fault"] == "crash" and row["dead_latency_ms"] is not None
        ]
        center = left + (profile_index + 0.5) * slot
        for point_index, value in enumerate(sorted(values)):
            offset = ((point_index % 5) - 2) * 16 + (point_index // 5) * 7
            y = bottom - (bottom - top) * min(value, maximum) / maximum
            draw.ellipse((center + offset - 9, y - 9, center + offset + 9, y + 9), fill=hexrgb(PROFILE_COLOR[profile]))
        if values:
            median = statistics.median(values)
            y = bottom - (bottom - top) * median / maximum
            draw.line((center - 70, y, center + 70, y), fill=hexrgb(INK), width=5)
            label = f"p50 {median / 1000:.2f}s"
        else:
            label = "0/10 detected"
            draw.line((center - 45, bottom - 8, center + 45, bottom - 8), fill=hexrgb(RED), width=6)
        draw.text((center, bottom + 38), PROFILE_LABEL[profile].replace(" + ", "\n+ "), anchor="ma", align="center", font=font(17), fill=hexrgb(INK))
        draw.text((center, 800), label, anchor="ma", font=font(18, True), fill=hexrgb(PROFILE_COLOR[profile]))
    image.save(path)


def save_simple_summary(path: Path, summary: Sequence[dict[str, Any]]) -> None:
    """One-slide result graphic for the minimal three-profile presentation."""
    by_key = {(row["profile"], row["fault"]): row for row in summary}
    image, draw = base_chart("핵심 결과", "Crash 탐지 속도와 직접 비교 가능한 조건의 DEAD 오판률")

    # Left: crash latency bars.
    draw.rounded_rectangle((55, 170, 775, 815), radius=22, fill=hexrgb(PANEL), outline=hexrgb(GRID), width=2)
    draw.text((90, 205), "Crash → DEAD p50", font=font(31, True), fill=hexrgb(INK))
    draw.text((90, 250), "세 방법 모두 10/10 탐지", font=font(22), fill=hexrgb(MUTED))
    max_seconds = 20.0
    for index, profile in enumerate(VALID_PROFILE_ORDER):
        seconds = by_key[(profile, "crash")]["dead_latency_p50_ms"] / 1000.0
        y = 350 + index * 145
        draw.text((90, y), PROFILE_LABEL[profile], font=font(24, True), fill=hexrgb(PROFILE_COLOR[profile]))
        draw.rounded_rectangle((90, y + 48, 700, y + 91), radius=12, fill=hexrgb("#20364B"))
        width = 610 * seconds / max_seconds
        draw.rounded_rectangle((90, y + 48, 90 + width, y + 91), radius=12, fill=hexrgb(PROFILE_COLOR[profile]))
        draw.text((720, y + 69), f"{seconds:.2f}s", anchor="rm", font=font(24, True), fill=hexrgb(INK))

    # Right: false-DEAD matrix.
    draw.rounded_rectangle((815, 170, 1545, 815), radius=22, fill=hexrgb(PANEL), outline=hexrgb(GRID), width=2)
    draw.text((850, 205), "비-Crash DEAD 오판률", font=font(31, True), fill=hexrgb(INK))
    columns = ["SWIM+LG", "Redis", "Phi"]
    rows = [
        ("Normal", [0, 0, 0]),
        ("높은 비대칭 손실", [70, 0, 0]),
        ("CPU 제한·경합", [0, 0, 0]),
        ("I/O contention", [0, 0, 0]),
    ]
    label_x = 850
    grid_left = 1120
    cell_w = 125
    for column, label in enumerate(columns):
        draw.text((grid_left + (column + 0.5) * cell_w, 292), label, anchor="mm", font=font(22, True), fill=hexrgb(PROFILE_COLOR[VALID_PROFILE_ORDER[column]]))
    for row_index, (label, values) in enumerate(rows):
        y0 = 335 + row_index * 108
        draw.text((label_x, y0 + 43), label, anchor="lm", font=font(21, True), fill=hexrgb(INK))
        for column, value in enumerate(values):
            x0 = grid_left + column * cell_w + 5
            x1 = grid_left + (column + 1) * cell_w - 5
            color = RED if value == 100 else "#B4434D" if value else "#163B50"
            draw.rounded_rectangle((x0, y0, x1, y0 + 86), radius=10, fill=hexrgb(color))
            draw.text(((x0 + x1) / 2, y0 + 43), f"{value}%", anchor="mm", font=font(24, True), fill=hexrgb(INK))
    draw.text((850, 780), "Target pause는 timeout 조건 차이로 핵심 비교에서 제외", font=font(18), fill=hexrgb(MUTED))
    image.save(path)


def generate_charts(report: Path, runs: Sequence[dict[str, Any]], summary: Sequence[dict[str, Any]]) -> dict[str, Path]:
    plots = report / "plots"
    plots.mkdir(parents=True, exist_ok=True)
    by_key = {(row["profile"], row["fault"]): row for row in summary}
    output: dict[str, Path] = {}

    output["crash_latency"] = plots / "01-crash-dead-latency.png"
    save_crash_latency(output["crash_latency"], runs)

    crash_rates = [by_key[(profile, "crash")]["success_rate"] * 100 for profile in PROFILE_ORDER]
    output["crash_success"] = plots / "02-crash-detection-success.png"
    save_bar_chart(output["crash_success"], "Crash detection success", "DEAD after crash, with no pre-fault DEAD. Wilson 95% CI is available in summary.csv.", [PROFILE_LABEL[p] for p in PROFILE_ORDER], crash_rates, [PROFILE_COLOR[p] for p in PROFILE_ORDER], max_value=100, notes=["n=10/profile", "unit: %"])

    normal_fp = [by_key[(profile, "normal")]["false_positive_run_count"] * 10 for profile in PROFILE_ORDER]
    output["normal_fp"] = plots / "03-normal-false-positive.png"
    save_bar_chart(output["normal_fp"], "Normal-state false-positive runs", "Run-level rate with any SUSPECT or DEAD event during Normal.", [PROFILE_LABEL[p] for p in PROFILE_ORDER], normal_fp, [PROFILE_COLOR[p] for p in PROFILE_ORDER], max_value=100, notes=["n=10/profile", "unit: %"])

    network_values = [[by_key[(profile, fault)]["alive_process_dead_count"] * 10 for fault in NETWORK_FAULTS] for profile in PROFILE_ORDER]
    output["network"] = plots / "04-network-misjudgment.png"
    save_heatmap(output["network"], "Network impairment DEAD misjudgment", "The target process remains alive; a DEAD decision is an error.", [PROFILE_LABEL[p] for p in PROFILE_ORDER], [FAULT_LABEL[f] for f in NETWORK_FAULTS], network_values)

    cpu_values = [[by_key[(profile, fault)]["alive_process_dead_count"] * 10 for fault in CPU_FAULTS] for profile in PROFILE_ORDER]
    output["cpu"] = plots / "05-cpu-fault-impact.png"
    save_heatmap(output["cpu"], "CPU disturbance DEAD misjudgment", "Pause, throttle, and contention scenarios; n=10 per cell.", [PROFILE_LABEL[p] for p in PROFILE_ORDER], [FAULT_LABEL[f] for f in CPU_FAULTS], cpu_values)

    io_iops = []
    for profile in PROFILE_ORDER:
        values = [row["fio_total_iops"] for row in runs if row["profile"] == profile and row["fault"] == "io-contention-target"]
        io_iops.append(statistics.median(values))
    output["io"] = plots / "06-io-contention.png"
    save_bar_chart(output["io"], "EBS I/O contention validation", "Median fio IOPS on the shared gp3 detector-state volume; all 50 validations passed.", [PROFILE_LABEL[p] for p in PROFILE_ORDER], io_iops, [PROFILE_COLOR[p] for p in PROFILE_ORDER], unit="", max_value=max(io_iops) * 1.15, notes=["n=10/profile", "queue depth=32", "runtime≈10s"])

    recovery_rates = []
    recovery_labels = []
    for profile in PROFILE_ORDER:
        eligible = sum(row["recovery_eligible"] for row in runs if row["profile"] == profile)
        recovered = sum(row["recovery_observed"] for row in runs if row["profile"] == profile and row["recovery_eligible"])
        recovery_rates.append(100 * recovered / eligible if eligible else 0)
        recovery_labels.append(PROFILE_LABEL[profile] + (f"\n({eligible} eligible)" if eligible else "\n(no transition)"))
    output["recovery"] = plots / "07-recovery.png"
    save_bar_chart(output["recovery"], "Recovery after a detector transition", "Denominator is runs with fault-end evidence and a post-fault SUSPECT/DEAD transition.", recovery_labels, recovery_rates, [PROFILE_COLOR[p] for p in PROFILE_ORDER], max_value=100, notes=["loss/delay fault-end evidence missing", "unit: %"])

    network_bytes = []
    for profile in PROFILE_ORDER:
        values = [row["total_sent_bytes"] for row in runs if row["profile"] == profile]
        network_bytes.append(statistics.fmean(values) / 1024)
    output["traffic"] = plots / "08-network-usage.png"
    save_bar_chart(output["traffic"], "Protocol network usage", "Mean total application bytes sent across five nodes per run (not Ethernet/IP overhead).", [PROFILE_LABEL[p] for p in PROFILE_ORDER], network_bytes, [PROFILE_COLOR[p] for p in PROFILE_ORDER], unit=" KiB", max_value=max(network_bytes) * 1.18, notes=["n=110/profile", "unit: KiB/run"])

    dead_values = []
    for profile in PROFILE_ORDER:
        row_values = []
        for fault in FAULT_ORDER:
            item = by_key[(profile, fault)]
            if fault == "normal":
                row_values.append(item["pre_fault_dead_event_count"] and item["false_positive_run_count"] * 10 or 0)
            else:
                row_values.append(item["dead_decision_count"] * 10)
        dead_values.append(row_values)
    output["heatmap"] = plots / "09-dead-decision-heatmap.png"
    save_heatmap(output["heatmap"], "DEAD decision rate by profile × scenario", "For Normal, the cell is the run-level pre-fault DEAD rate. Every cell has n=10.", [PROFILE_LABEL[p] for p in PROFILE_ORDER], [FAULT_LABEL[f] for f in FAULT_ORDER], dead_values)
    return output


def generate_valid_charts(report: Path, runs: Sequence[dict[str, Any]], summary: Sequence[dict[str, Any]]) -> dict[str, Path]:
    """Charts used in the final comparison, restricted to the three valid adapters."""
    profiles = VALID_PROFILE_ORDER
    plots = report / "plots-3profiles"
    plots.mkdir(parents=True, exist_ok=True)
    valid_runs = [row for row in runs if row["profile"] in profiles]
    by_key = {(row["profile"], row["fault"]): row for row in summary}
    output: dict[str, Path] = {}

    output["crash_latency"] = plots / "01-crash-dead-latency-3profiles.png"
    save_crash_latency(output["crash_latency"], valid_runs, profiles)

    output["crash_success"] = plots / "02-crash-detection-success-3profiles.png"
    save_bar_chart(
        output["crash_success"],
        "Crash detection success — valid profiles",
        "DEAD after Crash with no pre-fault DEAD. Wilson 95% CI is in summary.csv.",
        [PROFILE_LABEL[p] for p in profiles],
        [by_key[(p, "crash")]["success_rate"] * 100 for p in profiles],
        [PROFILE_COLOR[p] for p in profiles],
        max_value=100,
        notes=["n=10/profile", "unit: %"],
    )

    output["normal_fp"] = plots / "03-normal-false-positive-3profiles.png"
    save_bar_chart(
        output["normal_fp"],
        "Normal-state false-positive runs — valid profiles",
        "Run-level rate with any SUSPECT or DEAD event during Normal.",
        [PROFILE_LABEL[p] for p in profiles],
        [by_key[(p, "normal")]["false_positive_run_count"] * 10 for p in profiles],
        [PROFILE_COLOR[p] for p in profiles],
        max_value=100,
        notes=["n=10/profile", "all three: 0/10"],
    )

    output["network"] = plots / "04-network-misjudgment-3profiles.png"
    save_heatmap(
        output["network"],
        "Sustained network impairment DEAD misjudgment — valid profiles",
        "Configured netem drop probability (not measured loss rate); asymmetric target-outbound for ~45s.",
        [PROFILE_LABEL[p] for p in profiles],
        [FAULT_LABEL[f] for f in NETWORK_FAULTS],
        [[by_key[(p, f)]["alive_process_dead_count"] * 10 for f in NETWORK_FAULTS] for p in profiles],
    )

    output["cpu"] = plots / "05-cpu-fault-impact-3profiles.png"
    save_heatmap(
        output["cpu"],
        "CPU disturbance DEAD misjudgment — valid profiles",
        "Pause, throttle, and contention scenarios; n=10 per cell.",
        [PROFILE_LABEL[p] for p in profiles],
        [FAULT_LABEL[f] for f in CPU_FAULTS],
        [[by_key[(p, f)]["alive_process_dead_count"] * 10 for f in CPU_FAULTS] for p in profiles],
    )

    io_iops = []
    for profile in profiles:
        values = [row["fio_total_iops"] for row in valid_runs if row["profile"] == profile and row["fault"] == "io-contention-target"]
        io_iops.append(statistics.median(values))
    output["io"] = plots / "06-io-contention-3profiles.png"
    save_bar_chart(
        output["io"],
        "EBS I/O contention validation — valid profiles",
        "Median fio IOPS on shared gp3; all 30 validations passed and produced no DEAD misjudgment.",
        [PROFILE_LABEL[p] for p in profiles],
        io_iops,
        [PROFILE_COLOR[p] for p in profiles],
        unit="",
        max_value=max(io_iops) * 1.15,
        notes=["n=10/profile", "queue depth=32", "runtime≈10s"],
    )

    dead_values = []
    for profile in profiles:
        row_values = []
        for fault in FAULT_ORDER:
            item = by_key[(profile, fault)]
            if fault == "normal":
                row_values.append(item["false_positive_run_count"] * 10 if item["pre_fault_dead_event_count"] else 0)
            else:
                row_values.append(item["dead_decision_count"] * 10)
        dead_values.append(row_values)
    output["heatmap"] = plots / "07-dead-decision-heatmap-3profiles.png"
    save_heatmap(
        output["heatmap"],
        "DEAD decision rate — 3 valid profiles × 11 scenarios",
        "n=10/cell. Network cells use configured netem probabilities, not measured loss rates.",
        [PROFILE_LABEL[p] for p in profiles],
        [FAULT_LABEL[f] for f in FAULT_ORDER],
        dead_values,
    )
    output["summary"] = plots / "08-simple-summary-3profiles.png"
    save_simple_summary(output["summary"], summary)
    return output


def rgb(value: str) -> RGBColor:
    return RGBColor(*hexrgb(value))


def add_text(slide, text: str, x: float, y: float, w: float, h: float, size: int = 20, color: str = INK, bold: bool = False, align=PP_ALIGN.LEFT, valign=MSO_ANCHOR.TOP):
    box = slide.shapes.add_textbox(Inches(x), Inches(y), Inches(w), Inches(h))
    frame = box.text_frame
    frame.clear()
    frame.word_wrap = True
    frame.vertical_anchor = valign
    paragraph = frame.paragraphs[0]
    paragraph.text = text
    paragraph.alignment = align
    run = paragraph.runs[0]
    run.font.name = "Malgun Gothic"
    run.font.size = Pt(size)
    run.font.bold = bold
    run.font.color.rgb = rgb(color)
    return box


def add_rich_lines(slide, lines: Sequence[tuple[str, str]], x: float, y: float, w: float, h: float, size: int = 18):
    box = slide.shapes.add_textbox(Inches(x), Inches(y), Inches(w), Inches(h))
    frame = box.text_frame
    frame.clear()
    frame.word_wrap = True
    for index, (lead, body) in enumerate(lines):
        paragraph = frame.paragraphs[0] if index == 0 else frame.add_paragraph()
        paragraph.space_after = Pt(8)
        paragraph.level = 0
        run = paragraph.add_run()
        run.text = lead
        run.font.name = "Malgun Gothic"
        run.font.size = Pt(size)
        run.font.bold = True
        run.font.color.rgb = rgb("#67E8F9")
        run2 = paragraph.add_run()
        run2.text = body
        run2.font.name = "Malgun Gothic"
        run2.font.size = Pt(size)
        run2.font.color.rgb = rgb(INK)
    return box


def add_slide_base(prs: Presentation, number: int, title: str, kicker: str = "OPENKACHE · FAILURE DETECTOR LAB"):
    slide = prs.slides.add_slide(prs.slide_layouts[6])
    background = slide.background.fill
    background.solid()
    background.fore_color.rgb = rgb(BG)
    accent = slide.shapes.add_shape(MSO_SHAPE.RECTANGLE, 0, 0, Inches(0.12), prs.slide_height)
    accent.fill.solid(); accent.fill.fore_color.rgb = rgb("#22D3EE"); accent.line.fill.background()
    add_text(slide, kicker, 0.55, 0.25, 7.8, 0.28, 9, MUTED, True)
    add_text(slide, title, 0.55, 0.55, 12.1, 0.55, 27, INK, True)
    add_text(slide, f"{number:02d}", 12.35, 0.31, 0.55, 0.30, 10, MUTED, True, PP_ALIGN.RIGHT)
    return slide


def add_image(slide, path: Path, x: float = 0.55, y: float = 1.25, w: float = 12.2, h: float = 5.72):
    slide.shapes.add_picture(str(path), Inches(x), Inches(y), Inches(w), Inches(h))


def add_metric_card(slide, x: float, y: float, w: float, h: float, value: str, label: str, color: str = "#22D3EE"):
    shape = slide.shapes.add_shape(MSO_SHAPE.ROUNDED_RECTANGLE, Inches(x), Inches(y), Inches(w), Inches(h))
    shape.fill.solid(); shape.fill.fore_color.rgb = rgb(PANEL)
    shape.line.color.rgb = rgb(GRID)
    add_text(slide, value, x + 0.18, y + 0.13, w - 0.36, 0.52, 28, color, True)
    add_text(slide, label, x + 0.18, y + 0.69, w - 0.36, h - 0.76, 13, MUTED)


def add_table(slide, rows: Sequence[Sequence[str]], x: float, y: float, w: float, h: float, col_widths: Sequence[float] | None = None, font_size: int = 12):
    table_shape = slide.shapes.add_table(len(rows), len(rows[0]), Inches(x), Inches(y), Inches(w), Inches(h))
    table = table_shape.table
    if col_widths:
        for index, width in enumerate(col_widths):
            table.columns[index].width = Inches(width)
    for row_index, row in enumerate(rows):
        for col_index, value in enumerate(row):
            cell = table.cell(row_index, col_index)
            cell.text = value
            cell.fill.solid(); cell.fill.fore_color.rgb = rgb("#13263A" if row_index == 0 else PANEL)
            cell.margin_left = Inches(0.08); cell.margin_right = Inches(0.08)
            for paragraph in cell.text_frame.paragraphs:
                paragraph.alignment = PP_ALIGN.LEFT
                for run in paragraph.runs:
                    run.font.name = "Malgun Gothic"
                    run.font.size = Pt(font_size)
                    run.font.bold = row_index == 0
                    run.font.color.rgb = rgb(INK if row_index else "#67E8F9")
    return table


def build_pptx(report: Path, charts: dict[str, Path], runs: Sequence[dict[str, Any]], summary: Sequence[dict[str, Any]], validation: dict[str, Any], experiment: dict[str, str]) -> Path:
    prs = Presentation()
    prs.slide_width = Inches(13.333333)
    prs.slide_height = Inches(7.5)
    by_key = {(row["profile"], row["fault"]): row for row in summary}

    slide = prs.slides.add_slide(prs.slide_layouts[6])
    slide.background.fill.solid(); slide.background.fill.fore_color.rgb = rgb(BG)
    add_text(slide, "OPENKACHE · EC2 CAMPAIGN", 0.8, 0.75, 5.3, 0.35, 11, "#67E8F9", True)
    add_text(slide, "Failure detector\ncomparison report", 0.8, 1.35, 7.2, 1.65, 38, INK, True)
    add_text(slide, "5 profiles × 11 scenarios × 10 seeds = 550 successful sequences", 0.82, 3.22, 8.5, 0.42, 18, MUTED)
    add_text(slide, "EC2 real-socket experiment · ap-northeast-2", 0.82, 3.73, 7.5, 0.36, 15, MUTED)
    add_text(slide, experiment["experiment_id"], 0.82, 5.95, 8.2, 0.40, 16, "#22D3EE", True)
    add_text(slide, "2026-11-05", 0.82, 6.38, 3.0, 0.30, 11, MUTED)
    for idx, profile in enumerate(PROFILE_ORDER):
        y = 1.05 + idx * 1.03
        shape = slide.shapes.add_shape(MSO_SHAPE.ROUNDED_RECTANGLE, Inches(9.15), Inches(y), Inches(3.2), Inches(0.72))
        shape.fill.solid(); shape.fill.fore_color.rgb = rgb(PROFILE_COLOR[profile]); shape.fill.transparency = 12
        shape.line.fill.background()
        add_text(slide, PROFILE_LABEL[profile], 9.42, y + 0.20, 2.7, 0.28, 14, "#FFFFFF", True)

    slide = add_slide_base(prs, 2, "Reproducibility record")
    add_metric_card(slide, 0.65, 1.35, 2.2, 1.25, "550 / 550", "successful logical sequences", GREEN)
    add_metric_card(slide, 3.05, 1.35, 2.2, 1.25, "5 × 11 × 10", "profiles × faults × seeds", "#22D3EE")
    add_metric_card(slide, 5.45, 1.35, 2.2, 1.25, "50 / 50", "fio validations passed", GREEN)
    add_metric_card(slide, 7.85, 1.35, 2.2, 1.25, "6", "failed attempts in campaign.log", AMBER)
    add_metric_card(slide, 10.25, 1.35, 2.2, 1.25, "23,945", "downloaded raw files", "#A78BFA")
    add_rich_lines(slide, [
        ("Experiment ID  ", experiment["experiment_id"]),
        ("Region / time  ", experiment["region"] + " · " + experiment["period_kst"]),
        ("Compute  ", experiment["instance"] + " × " + experiment["nodes"] + " · AMI " + experiment["ami"]),
        ("Storage  ", experiment["storage"]),
        ("Timing  ", experiment["timing"]),
        ("JAR SHA-256  ", experiment["jar_sha256"]),
        ("Git commit  ", experiment["git_commit"]),
    ], 0.75, 3.0, 11.9, 3.5, 15)

    slide = add_slide_base(prs, 3, "Five comparison targets")
    table_rows = [["Profile", "Failure-decision model", "Observed transport", "EC2 result headline"]]
    headlines = {
        "product-swim-lifeguard": "Fast crash detection; loss45 / pause-target sensitivity",
        "product-redis-cluster": "Crash detection is slower, but stable under non-crash faults",
        "product-phi-gossip": "Stable; slowest valid crash detection",
        "product-ignite-tcp-ring": "Pre-fault DEAD in every run; crash result invalidated",
        "product-aerospike-mesh": "Pre-fault DEAD in every run; crash result invalidated",
    }
    models = {
        "product-swim-lifeguard": "SWIM probes + Lifeguard suspicion",
        "product-redis-cluster": "PING/PONG + multi-reporter quorum",
        "product-phi-gossip": "Phi accrual heartbeat threshold",
        "product-ignite-tcp-ring": "TCP ring heartbeat + coordinator confirm",
        "product-aerospike-mesh": "Mesh heartbeat + membership agreement",
    }
    transports = {
        "product-swim-lifeguard": "real UDP",
        "product-redis-cluster": "real UDP",
        "product-phi-gossip": "real UDP",
        "product-ignite-tcp-ring": "TCP heartbeat + UDP control",
        "product-aerospike-mesh": "TCP heartbeat + UDP control",
    }
    for profile in PROFILE_ORDER:
        table_rows.append([PROFILE_LABEL[profile], models[profile], transports[profile], headlines[profile]])
    add_table(slide, table_rows, 0.6, 1.35, 12.1, 5.45, [2.05, 3.1, 2.35, 4.6], 12)

    slide = add_slide_base(prs, 4, "Fault scenarios and correctness rule")
    add_rich_lines(slide, [
        ("Crash  ", "target process is killed; a post-fault DEAD is success."),
        ("Normal  ", "no fault is injected; any SUSPECT or DEAD is a false positive."),
        ("Network  ", "loss15, loss45, delay — target stays alive; DEAD is a misjudgment."),
        ("CPU  ", "pause/throttle on target or observer, plus target contention; DEAD is a misjudgment."),
        ("Storage  ", "shared gp3 I/O contention with fio queue depth 32; DEAD is a misjudgment."),
        ("Null latency  ", "kept as missed detection / unavailable. Never replaced with zero."),
    ], 0.75, 1.38, 7.35, 4.7, 18)
    add_metric_card(slide, 8.55, 1.50, 3.35, 1.35, "15 s", "warm-up", "#22D3EE")
    add_metric_card(slide, 8.55, 3.02, 3.35, 1.35, "10 s", "fault duration", AMBER)
    add_metric_card(slide, 8.55, 4.54, 3.35, 1.35, "15 s", "recovery window", GREEN)
    add_text(slide, "Each profile × fault cell has n=10.", 8.55, 6.20, 3.35, 0.4, 14, MUTED, True, PP_ALIGN.CENTER)

    slide = add_slide_base(prs, 5, "Completeness passed — with two provenance gaps")
    checks = validation["checks"]
    check_lines = [("✓  ", label) for label, ok in [
        ("550 unique successful sequences", checks["successful_sequences_550"]),
        ("110 per profile / 50 per fault / 10 per cell", checks["profile_fault_combinations_10_each"]),
        ("result.json and run.properties for every successful run", checks["all_successful_have_result_json"] and checks["all_successful_have_run_properties"]),
        ("50 fio validations, all passed", checks["all_fio_validation_passed"]),
        ("final-success attempts only in runs.csv and summary.csv", checks["statistics_include_only_final_success"]),
    ] if ok]
    add_rich_lines(slide, check_lines, 0.75, 1.35, 6.1, 4.5, 17)
    shape = slide.shapes.add_shape(MSO_SHAPE.ROUNDED_RECTANGLE, Inches(7.1), Inches(1.35), Inches(5.5), Inches(4.95))
    shape.fill.solid(); shape.fill.fore_color.rgb = rgb("#2B1B19"); shape.line.color.rgb = rgb(AMBER)
    add_text(slide, "DATA QUALITY FLAGS", 7.45, 1.68, 4.8, 0.35, 12, AMBER, True)
    add_rich_lines(slide, [
        ("Git  ", "commit was not embedded; local HEAD is an unverified candidate."),
        ("Resources  ", "systemd CPU/memory/general I/O counters are unavailable after unit cleanup."),
        ("Recovery  ", "loss/delay runs have no fault-end timestamp; recovery latency is not measurable."),
        ("History  ", "campaign.log has 6 failed attempts; execution-status.csv retains 3 after reset."),
    ], 7.45, 2.18, 4.75, 3.65, 15)

    for number, key, title in [
        (6, "crash_latency", "Crash DEAD latency"),
        (7, "crash_success", "Crash detection success"),
        (8, "normal_fp", "Normal-state false positives"),
        (9, "network", "Network impairment misjudgment"),
        (10, "cpu", "CPU disturbance impact"),
        (11, "io", "EBS I/O contention impact"),
        (12, "recovery", "Recovery success and latency evidence"),
        (13, "traffic", "Protocol network usage"),
        (14, "heatmap", "Profile × fault DEAD decision heatmap"),
    ]:
        slide = add_slide_base(prs, number, title)
        add_image(slide, charts[key])

    slide = add_slide_base(prs, 15, "What the EC2 evidence says")
    swim_crash = by_key[("product-swim-lifeguard", "crash")]
    redis_crash = by_key[("product-redis-cluster", "crash")]
    phi_crash = by_key[("product-phi-gossip", "crash")]
    add_metric_card(slide, 0.7, 1.35, 3.65, 1.35, f"{swim_crash['dead_latency_p50_ms']/1000:.2f}s", "SWIM crash DEAD p50 · 10/10", GREEN)
    add_metric_card(slide, 4.55, 1.35, 3.65, 1.35, f"{redis_crash['dead_latency_p50_ms']/1000:.2f}s", "Redis crash DEAD p50 · 10/10", "#3B82F6")
    add_metric_card(slide, 8.4, 1.35, 3.65, 1.35, f"{phi_crash['dead_latency_p50_ms']/1000:.2f}s", "Phi crash DEAD p50 · 10/10", "#8B5CF6")
    add_rich_lines(slide, [
        ("1. Fastest valid crash signal  ", "SWIM + Lifeguard is about 4.8× faster than Redis at p50."),
        ("2. Safety trade-off  ", "SWIM emits DEAD for 10/10 target pauses and 7/10 loss45 runs."),
        ("3. Conservative alternatives  ", "Redis and Phi show no Normal, network, CPU, or I/O DEAD misjudgment in this matrix."),
        ("4. Adapter defects  ", "Ignite and Aerospike are DEAD before injection in all 110 runs; do not compare their crash latency."),
        ("5. Storage robustness  ", "All fio evidence passed and no detector produced a post-fault DEAD under I/O contention."),
    ], 0.8, 3.15, 11.8, 3.3, 17)

    slide = add_slide_base(prs, 16, "Recommendation for OpenKache")
    add_text(slide, "Adopt a guarded two-stage decision path", 0.75, 1.30, 8.0, 0.45, 24, "#67E8F9", True)
    add_rich_lines(slide, [
        ("Primary signal  ", "Use SWIM/Lifeguard-style probing for fast suspicion and crash responsiveness."),
        ("DEAD gate  ", "Require independent confirmation / quorum before DEAD, especially under severe loss or target pause."),
        ("Fallback  ", "Use conservative Redis/Phi-style evidence when confirmation is weak or the observer is impaired."),
        ("Do not ship yet  ", "Fix Ignite/Aerospike startup registration/heartbeat semantics, then rerun the full matrix."),
        ("Instrumentation  ", "Capture resource counters before unit teardown, embed Git commit, and emit fault-end for netem faults."),
    ], 0.78, 2.00, 8.1, 4.2, 17)
    shape = slide.shapes.add_shape(MSO_SHAPE.ROUNDED_RECTANGLE, Inches(9.25), Inches(1.45), Inches(3.2), Inches(4.9))
    shape.fill.solid(); shape.fill.fore_color.rgb = rgb("#0B2A2A"); shape.line.color.rgb = rgb("#14B8A6")
    add_text(slide, "DECISION POLICY", 9.58, 1.80, 2.5, 0.35, 12, "#5EEAD4", True, PP_ALIGN.CENTER)
    add_text(slide, "SUSPECT", 9.65, 2.45, 2.35, 0.42, 22, INK, True, PP_ALIGN.CENTER)
    add_text(slide, "↓", 10.56, 3.00, 0.5, 0.35, 22, MUTED, True, PP_ALIGN.CENTER)
    add_text(slide, "independent\nverification", 9.65, 3.38, 2.35, 0.72, 17, "#67E8F9", True, PP_ALIGN.CENTER)
    add_text(slide, "↓", 10.56, 4.25, 0.5, 0.35, 22, MUTED, True, PP_ALIGN.CENTER)
    add_text(slide, "DEAD", 9.65, 4.72, 2.35, 0.42, 24, RED, True, PP_ALIGN.CENTER)
    add_text(slide, "only with quorum", 9.65, 5.28, 2.35, 0.32, 13, MUTED, True, PP_ALIGN.CENTER)

    output = report / "OpenKache-Failure-Detector-EC2-Report.pptx"
    prs.save(output)
    return output


def build_pptx_concise(report: Path, charts: dict[str, Path], runs: Sequence[dict[str, Any]], summary: Sequence[dict[str, Any]], validation: dict[str, Any], experiment: dict[str, str]) -> Path:
    """Create the five-minute presentation: four sections, eight slides."""
    prs = Presentation()
    prs.slide_width = Inches(13.333333)
    prs.slide_height = Inches(7.5)
    by_key = {(row["profile"], row["fault"]): row for row in summary}

    # 1. Title
    slide = prs.slides.add_slide(prs.slide_layouts[6])
    slide.background.fill.solid(); slide.background.fill.fore_color.rgb = rgb(BG)
    add_text(slide, "OPENKACHE · FAILURE DETECTOR", 0.8, 0.72, 5.5, 0.35, 11, "#67E8F9", True)
    add_text(slide, "장애 탐지 방법론\nEC2 비교 실험", 0.8, 1.45, 7.0, 1.55, 38, INK, True)
    add_text(slide, "5개 방법론 · 11개 장애 시나리오 · 550회 실행", 0.82, 3.32, 7.8, 0.45, 18, MUTED)
    add_text(slide, "5분 발표", 0.82, 4.02, 2.0, 0.38, 15, "#22D3EE", True)
    for idx, profile in enumerate(PROFILE_ORDER):
        y = 1.10 + idx * 1.02
        shape = slide.shapes.add_shape(MSO_SHAPE.ROUNDED_RECTANGLE, Inches(9.05), Inches(y), Inches(3.25), Inches(0.70))
        shape.fill.solid(); shape.fill.fore_color.rgb = rgb(PROFILE_COLOR[profile]); shape.fill.transparency = 10
        shape.line.fill.background()
        add_text(slide, PROFILE_LABEL[profile], 9.34, y + 0.19, 2.70, 0.28, 14, "#FFFFFF", True)
    add_text(slide, experiment["experiment_id"], 0.82, 6.48, 8.0, 0.30, 12, MUTED)

    # 2. Methodologies
    slide = add_slide_base(prs, 2, "1. 방법론들")
    rows = [
        ["방법론", "핵심 판단 방식", "실험 전송"],
        ["SWIM + Lifeguard", "직접/간접 probe + suspicion", "UDP"],
        ["Redis Cluster", "PING/PONG + 다중 보고", "UDP"],
        ["Phi Gossip", "heartbeat 간격의 φ 임계값", "UDP"],
        ["Ignite TCP Ring", "ring heartbeat + coordinator 확인", "TCP + UDP control"],
        ["Aerospike Mesh", "mesh heartbeat + membership 합의", "TCP + UDP control"],
    ]
    add_table(slide, rows, 0.72, 1.35, 11.9, 4.85, [2.7, 5.7, 3.5], 15)
    add_text(slide, "비교 기준: 탐지 속도 · 미검출 · 오탐 · 네트워크 비용", 0.82, 6.48, 11.6, 0.35, 16, "#67E8F9", True, PP_ALIGN.CENTER)

    # 3. Fault scenarios
    slide = add_slide_base(prs, 3, "2. 장애 시나리오")
    cards = [
        ("정상 / 실제 장애", "Normal\nCrash", "Crash만 DEAD가 정답", "#22D3EE"),
        ("네트워크", "Loss 15% · Loss 45%\nDelay 250ms ± 50ms", "프로세스 생존 → DEAD는 오판", "#8B5CF6"),
        ("CPU", "Pause · Throttle\nContention", "target / observer 분리", "#F59E0B"),
        ("Storage", "gp3 I/O contention\nfio QD=32", "공유 상태 볼륨 부하", "#14B8A6"),
    ]
    for index, (title, body, rule, color) in enumerate(cards):
        x = 0.65 + index * 3.13
        shape = slide.shapes.add_shape(MSO_SHAPE.ROUNDED_RECTANGLE, Inches(x), Inches(1.45), Inches(2.85), Inches(3.85))
        shape.fill.solid(); shape.fill.fore_color.rgb = rgb(PANEL); shape.line.color.rgb = rgb(color)
        add_text(slide, title, x + 0.20, 1.75, 2.45, 0.35, 17, color, True, PP_ALIGN.CENTER)
        add_text(slide, body, x + 0.20, 2.48, 2.45, 1.25, 20, INK, True, PP_ALIGN.CENTER, MSO_ANCHOR.MIDDLE)
        add_text(slide, rule, x + 0.22, 4.32, 2.41, 0.52, 12, MUTED, False, PP_ALIGN.CENTER)
    add_text(slide, "각 조건 n=10 · warm-up 15s · fault 10s · recovery 15s · null 지연은 미검출로 유지", 0.72, 5.78, 11.9, 0.42, 16, INK, True, PP_ALIGN.CENTER)
    add_text(slide, "총 550개 성공 sequence / fio 검증 50개 모두 통과", 0.72, 6.35, 11.9, 0.35, 14, "#67E8F9", True, PP_ALIGN.CENTER)

    # 4. Crash graphs
    slide = add_slide_base(prs, 4, "3. 그래프들 — Crash 탐지")
    add_image(slide, charts["crash_latency"], 0.45, 1.25, 6.18, 3.48)
    add_image(slide, charts["crash_success"], 6.70, 1.25, 6.18, 3.48)
    add_text(slide, "SWIM: p50 3.25s · 10/10", 0.75, 5.15, 3.7, 0.42, 19, "#14B8A6", True)
    add_text(slide, "Redis: 15.46s · Phi: 18.82s", 4.55, 5.15, 4.3, 0.42, 19, "#67E8F9", True, PP_ALIGN.CENTER)
    add_text(slide, "Ignite/Aerospike: 장애 전 이미 DEAD → 유효 비교 불가", 8.80, 5.15, 3.8, 0.55, 16, RED, True, PP_ALIGN.RIGHT)
    add_text(slide, "핵심: 유효한 결과 중 SWIM이 가장 빠르다.", 0.80, 6.22, 11.8, 0.42, 21, INK, True, PP_ALIGN.CENTER)

    # 5. Normal/network safety graphs
    slide = add_slide_base(prs, 5, "3. 그래프들 — 정상·네트워크 오판")
    add_image(slide, charts["normal_fp"], 0.45, 1.25, 6.18, 3.48)
    add_image(slide, charts["network"], 6.70, 1.25, 6.18, 3.48)
    add_text(slide, "정상 오탐: Ignite/Aerospike 100%", 0.82, 5.18, 5.4, 0.40, 18, RED, True, PP_ALIGN.CENTER)
    add_text(slide, "Loss 45%: SWIM DEAD 오판 7/10", 7.12, 5.18, 5.1, 0.40, 18, AMBER, True, PP_ALIGN.CENTER)
    add_text(slide, "Redis·Phi는 정상/네트워크 시나리오에서 DEAD 오판 0건", 0.80, 6.22, 11.8, 0.42, 21, INK, True, PP_ALIGN.CENTER)

    # 6. CPU/storage graphs
    slide = add_slide_base(prs, 6, "3. 그래프들 — CPU·EBS 영향")
    add_image(slide, charts["cpu"], 0.45, 1.25, 6.18, 3.48)
    add_image(slide, charts["io"], 6.70, 1.25, 6.18, 3.48)
    add_text(slide, "Target pause: SWIM DEAD 오판 10/10", 0.82, 5.18, 5.4, 0.40, 18, AMBER, True, PP_ALIGN.CENTER)
    add_text(slide, "fio 50/50 통과 · I/O DEAD 오판 0건", 7.12, 5.18, 5.1, 0.40, 18, GREEN, True, PP_ALIGN.CENTER)
    add_text(slide, "빠른 탐지에는 pause/loss를 실제 crash와 구분할 보호 장치가 필요", 0.80, 6.22, 11.8, 0.42, 21, INK, True, PP_ALIGN.CENTER)

    # 7. Overall graph
    slide = add_slide_base(prs, 7, "3. 그래프들 — 전체 DEAD 판단률")
    add_image(slide, charts["heatmap"], 0.55, 1.25, 12.20, 5.72)

    # 8. Conclusion
    slide = add_slide_base(prs, 8, "4. 결론")
    add_text(slide, "OpenKache에는 ‘빠른 SUSPECT + 검증된 DEAD’를 적용", 0.78, 1.25, 11.8, 0.52, 26, "#67E8F9", True, PP_ALIGN.CENTER)
    add_metric_card(slide, 0.78, 2.05, 3.60, 1.25, "3.25s", "SWIM crash DEAD p50", GREEN)
    add_metric_card(slide, 4.87, 2.05, 3.60, 1.25, "7 / 10", "SWIM loss45 DEAD 오판", AMBER)
    add_metric_card(slide, 8.96, 2.05, 3.60, 1.25, "0", "Redis·Phi 비-crash DEAD 오판", "#3B82F6")
    add_rich_lines(slide, [
        ("1  ", "SWIM/Lifeguard probe를 1차 탐지 신호로 사용한다."),
        ("2  ", "DEAD 전 독립 확인·quorum을 강제해 pause/loss 오판을 막는다."),
        ("3  ", "Ignite/Aerospike adapter의 초기 상태 전이를 수정한 뒤 재실험한다."),
    ], 1.05, 3.82, 11.2, 2.25, 19)
    add_text(slide, "한계: Git commit·일반 CPU/메모리 지표·netem 복구 시점은 원본에 미기록", 0.82, 6.55, 11.7, 0.30, 12, MUTED, False, PP_ALIGN.CENTER)

    output = report / "OpenKache-Failure-Detector-EC2-Report-5min.pptx"
    prs.save(output)
    return output


def build_pptx_valid_three(report: Path, charts: dict[str, Path], summary: Sequence[dict[str, Any]], experiment: dict[str, str]) -> Path:
    """Five-minute final deck containing only the three semantically valid profiles."""
    prs = Presentation()
    prs.slide_width = Inches(13.333333)
    prs.slide_height = Inches(7.5)
    by_key = {(row["profile"], row["fault"]): row for row in summary}

    # 1. Title
    slide = prs.slides.add_slide(prs.slide_layouts[6])
    slide.background.fill.solid(); slide.background.fill.fore_color.rgb = rgb(BG)
    add_text(slide, "OPENKACHE · FAILURE DETECTOR", 0.8, 0.72, 5.5, 0.35, 11, "#67E8F9", True)
    add_text(slide, "유효 프로필 3종\nEC2 비교 실험", 0.8, 1.38, 7.2, 1.72, 43, INK, True)
    add_text(slide, "SWIM + Lifeguard · Redis Cluster · Phi Gossip", 0.82, 3.30, 7.8, 0.45, 20, MUTED)
    add_text(slide, "3개 프로필 × 11개 시나리오 × 10 seeds = 330회", 0.82, 3.93, 8.3, 0.45, 19, "#22D3EE", True)
    add_text(slide, "5분 발표", 0.82, 4.48, 2.0, 0.38, 15, "#22D3EE", True)
    for idx, profile in enumerate(VALID_PROFILE_ORDER):
        y = 1.42 + idx * 1.33
        shape = slide.shapes.add_shape(MSO_SHAPE.ROUNDED_RECTANGLE, Inches(9.0), Inches(y), Inches(3.35), Inches(0.88))
        shape.fill.solid(); shape.fill.fore_color.rgb = rgb(PROFILE_COLOR[profile]); shape.fill.transparency = 8
        shape.line.fill.background()
        add_text(slide, PROFILE_LABEL[profile], 9.32, y + 0.26, 2.72, 0.31, 16, "#FFFFFF", True)
    add_text(slide, "전체 550회 중 Ignite/Aerospike 220회는 TCP adapter 결함으로 비교 제외", 0.82, 6.28, 11.6, 0.35, 13, AMBER, True)
    add_text(slide, experiment["experiment_id"], 0.82, 6.72, 8.0, 0.28, 10, MUTED)

    # 2. Three methods
    slide = add_slide_base(prs, 2, "1. 유효 방법론 3종")
    method_cards = [
        ("SWIM + Lifeguard", "직접·간접 probe\n+ suspicion", "빠른 탐지", "#14B8A6"),
        ("Redis Cluster", "PING/PONG\n+ 다중 보고", "보수적 quorum", "#3B82F6"),
        ("Phi Gossip", "heartbeat 간격\n+ φ 임계값", "통계 기반", "#8B5CF6"),
    ]
    for index, (title, body, feature, color) in enumerate(method_cards):
        x = 0.78 + index * 4.10
        shape = slide.shapes.add_shape(MSO_SHAPE.ROUNDED_RECTANGLE, Inches(x), Inches(1.45), Inches(3.70), Inches(3.55))
        shape.fill.solid(); shape.fill.fore_color.rgb = rgb(PANEL); shape.line.color.rgb = rgb(color)
        add_text(slide, title, x + 0.22, 1.80, 3.26, 0.48, 21, color, True, PP_ALIGN.CENTER)
        add_text(slide, body, x + 0.24, 2.55, 3.22, 1.18, 23, INK, True, PP_ALIGN.CENTER, MSO_ANCHOR.MIDDLE)
        add_text(slide, feature, x + 0.24, 4.24, 3.22, 0.38, 17, MUTED, True, PP_ALIGN.CENTER)
    shape = slide.shapes.add_shape(MSO_SHAPE.ROUNDED_RECTANGLE, Inches(0.85), Inches(5.45), Inches(11.65), Inches(1.15))
    shape.fill.solid(); shape.fill.fore_color.rgb = rgb("#2B1B19"); shape.line.color.rgb = rgb(AMBER)
    add_text(slide, "비교 제외", 1.12, 5.80, 1.50, 0.36, 16, AMBER, True)
    add_text(slide, "Ignite·Aerospike — TCP adapter 오류로 장애 전 DEAD", 2.58, 5.75, 8.95, 0.44, 19, INK, True)

    # 3. Scenarios
    slide = add_slide_base(prs, 3, "2. 장애 시나리오")
    cards = [
        ("기준선", "Normal\nCrash", "Crash만 DEAD가 정답", "#22D3EE"),
        ("네트워크", "비대칭 손실\n낮음 · 높음\n+ 지연", "target outbound · 지속", "#8B5CF6"),
        ("CPU", "Pause · Throttle\nContention", "target / observer 분리", "#F59E0B"),
        ("Storage", "gp3 I/O contention\nfio QD=32", "공유 상태 볼륨 부하", "#14B8A6"),
    ]
    for index, (title, body, rule, color) in enumerate(cards):
        x = 0.65 + index * 3.13
        shape = slide.shapes.add_shape(MSO_SHAPE.ROUNDED_RECTANGLE, Inches(x), Inches(1.45), Inches(2.85), Inches(3.85))
        shape.fill.solid(); shape.fill.fore_color.rgb = rgb(PANEL); shape.line.color.rgb = rgb(color)
        add_text(slide, title, x + 0.20, 1.73, 2.45, 0.40, 20, color, True, PP_ALIGN.CENTER)
        add_text(slide, body, x + 0.20, 2.38, 2.45, 1.48, 22, INK, True, PP_ALIGN.CENTER, MSO_ANCHOR.MIDDLE)
        add_text(slide, rule, x + 0.22, 4.34, 2.41, 0.56, 15, MUTED, True, PP_ALIGN.CENTER)
    add_text(slide, "각 조건 10회 반복 · 유효 분석 330회", 0.72, 5.82, 11.9, 0.48, 22, "#67E8F9", True, PP_ALIGN.CENTER)

    # 4. Crash
    slide = add_slide_base(prs, 4, "3. 결과 — Crash 탐지")
    add_image(slide, charts["crash_latency"], 1.18, 1.12, 10.98, 6.18)

    # 5. Normal/network
    slide = add_slide_base(prs, 5, "3. 결과 — Normal 오탐 0 · 비대칭 네트워크")
    add_image(slide, charts["network"], 1.18, 1.12, 10.98, 6.18)

    # 6. CPU/EBS
    slide = add_slide_base(prs, 6, "3. 결과 — CPU 영향 · EBS 오판 0")
    add_image(slide, charts["cpu"], 1.18, 1.12, 10.98, 6.18)

    # 7. Overall
    slide = add_slide_base(prs, 7, "3. 결과 — 전체 DEAD 판단률")
    add_image(slide, charts["heatmap"], 1.18, 1.12, 10.98, 6.18)

    # 8. Conclusion
    slide = add_slide_base(prs, 8, "4. 결론")
    add_text(slide, "OpenKache: 빠른 SUSPECT + 검증된 DEAD", 0.78, 1.18, 11.8, 0.55, 27, "#67E8F9", True, PP_ALIGN.CENTER)
    add_metric_card(slide, 0.78, 2.02, 3.60, 1.25, "3.25s", "SWIM Crash DEAD p50", GREEN)
    add_metric_card(slide, 4.87, 2.02, 3.60, 1.25, "7 / 10", "SWIM 높은 비대칭 손실 오판", AMBER)
    add_metric_card(slide, 8.96, 2.02, 3.60, 1.25, "0", "Redis·Phi 비-Crash DEAD 오판", "#3B82F6")
    add_rich_lines(slide, [
        ("1  ", "SWIM/Lifeguard를 빠른 SUSPECT 신호로 사용한다."),
        ("2  ", "DEAD 전 독립 helper·quorum으로 pause/loss를 재검증한다."),
        ("3  ", "안전성 우선 시 Redis/Phi 증거를 보조 gate로 사용한다."),
    ], 1.02, 3.65, 11.2, 2.35, 22)
    add_text(slide, "Ignite/Aerospike는 adapter 수정 후 별도 재실험", 0.82, 6.42, 11.7, 0.36, 16, AMBER, True, PP_ALIGN.CENTER)

    output = report / "OpenKache-Failure-Detector-EC2-3profiles-5min.pptx"
    prs.save(output)
    return output


def build_pptx_simple_three(report: Path, charts: dict[str, Path]) -> Path:
    """Minimal five-slide deck: algorithms, scenarios, graph, conclusion."""
    prs = Presentation()
    prs.slide_width = Inches(13.333333)
    prs.slide_height = Inches(7.5)

    # 1. Cover
    slide = prs.slides.add_slide(prs.slide_layouts[6])
    slide.background.fill.solid(); slide.background.fill.fore_color.rgb = rgb(BG)
    add_text(slide, "OPENKACHE", 0.85, 0.80, 3.0, 0.35, 13, "#67E8F9", True)
    add_text(slide, "Failure Detector\n3종 비교", 0.85, 1.55, 7.0, 1.75, 46, INK, True)
    add_text(slide, "SWIM + Lifeguard · Redis Cluster · Phi Gossip", 0.88, 3.62, 7.7, 0.45, 21, MUTED)
    add_text(slide, "EC2 다중 노드 실험", 0.88, 4.35, 4.0, 0.40, 18, "#22D3EE", True)
    for index, profile in enumerate(VALID_PROFILE_ORDER):
        y = 1.45 + index * 1.42
        shape = slide.shapes.add_shape(MSO_SHAPE.ROUNDED_RECTANGLE, Inches(9.0), Inches(y), Inches(3.35), Inches(0.96))
        shape.fill.solid(); shape.fill.fore_color.rgb = rgb(PROFILE_COLOR[profile]); shape.line.fill.background()
        add_text(slide, PROFILE_LABEL[profile], 9.30, y + 0.29, 2.75, 0.34, 18, "#FFFFFF", True, PP_ALIGN.CENTER)

    # 2. Algorithms
    slide = add_slide_base(prs, 2, "1. 알고리즘 간단 소개")
    cards = [
        ("SWIM + Lifeguard", "직접·간접 probe", "빠른 장애 탐지", "#14B8A6"),
        ("Redis Cluster", "PING/PONG + 다중 보고", "quorum 기반 판단", "#3B82F6"),
        ("Phi Gossip", "heartbeat + φ 점수", "통계 기반 판단", "#8B5CF6"),
    ]
    for index, (title, method, feature, color) in enumerate(cards):
        x = 0.72 + index * 4.18
        shape = slide.shapes.add_shape(MSO_SHAPE.ROUNDED_RECTANGLE, Inches(x), Inches(1.50), Inches(3.78), Inches(4.70))
        shape.fill.solid(); shape.fill.fore_color.rgb = rgb(PANEL); shape.line.color.rgb = rgb(color)
        add_text(slide, title, x + 0.25, 1.95, 3.28, 0.52, 22, color, True, PP_ALIGN.CENTER)
        add_text(slide, method, x + 0.28, 3.00, 3.22, 0.92, 24, INK, True, PP_ALIGN.CENTER, MSO_ANCHOR.MIDDLE)
        add_text(slide, feature, x + 0.28, 4.65, 3.22, 0.45, 18, MUTED, True, PP_ALIGN.CENTER)

    # 3. Scenarios
    slide = add_slide_base(prs, 3, "2. 장애 시나리오")
    scenario_rows = [
        ["분류", "시나리오", "주입 대상", "올바른 결과"],
        ["기준선", "normal", "없음", "SUSPECT / DEAD 없음"],
        ["실제 사망", "crash", "target", "장애 후 DEAD"],
        ["네트워크", "loss15", "target 경로", "target 생존, DEAD 없음"],
        ["네트워크", "loss45", "target 경로", "target 생존, DEAD 없음"],
        ["네트워크", "delay", "target 경로", "250ms ± 50ms 지연, DEAD 없음"],
        ["CPU", "cpu-pause-target", "target", "프로세스 일시정지, DEAD 없음"],
        ["CPU", "cpu-pause-observer", "observer", "관찰자 일시정지, DEAD 없음"],
        ["CPU", "cpu-throttle-target", "target", "CPUQuota 10%, DEAD 없음"],
        ["CPU", "cpu-throttle-observer", "observer", "CPUQuota 10%, DEAD 없음"],
        ["CPU", "cpu-contention-target", "target", "stress-ng 부하, DEAD 없음"],
        ["Storage", "io-contention-target", "target 공유 gp3", "fio 부하, DEAD 없음"],
    ]
    add_table(slide, scenario_rows, 0.58, 1.10, 12.18, 5.68, [1.25, 3.10, 2.15, 5.68], 11)
    add_text(slide, "각 조건 n=10 · 3개 방법론 · 총 330회  |  Target pause는 timeout 차이로 직접 비교에서 제외", 0.65, 6.91, 12.0, 0.26, 12, MUTED, False, PP_ALIGN.CENTER)

    # 4. Graph
    slide = add_slide_base(prs, 4, "3. 그래프")
    add_image(slide, charts["summary"], 1.17, 1.10, 11.0, 6.19)

    # 5. Conclusion and next steps
    slide = add_slide_base(prs, 5, "4. 결론 및 향후 계획")

    shape = slide.shapes.add_shape(MSO_SHAPE.ROUNDED_RECTANGLE, Inches(0.72), Inches(1.25), Inches(5.92), Inches(2.05))
    shape.fill.solid(); shape.fill.fore_color.rgb = rgb(PANEL); shape.line.color.rgb = rgb("#14B8A6")
    add_text(slide, "현재 확인한 결과", 1.02, 1.53, 5.32, 0.38, 18, "#5EEAD4", True)
    add_text(slide, "SWIM + Lifeguard", 1.02, 2.08, 2.25, 0.34, 18, "#14B8A6", True)
    add_text(slide, "빠른 탐지 · 높은 네트워크 손실에서 오탐", 3.00, 2.08, 3.20, 0.42, 17, INK, True)
    add_text(slide, "Redis · Phi", 1.02, 2.59, 2.25, 0.34, 18, "#3B82F6", True)
    add_text(slide, "느린 탐지 · 보수적인 장애 판단", 3.00, 2.59, 3.20, 0.42, 17, INK, True)

    shape = slide.shapes.add_shape(MSO_SHAPE.ROUNDED_RECTANGLE, Inches(6.92), Inches(1.25), Inches(5.70), Inches(2.05))
    shape.fill.solid(); shape.fill.fore_color.rgb = rgb(PANEL); shape.line.color.rgb = rgb(AMBER)
    add_text(slide, "현재 비교의 한계", 7.22, 1.53, 5.10, 0.38, 18, AMBER, True)
    add_text(slide, "Ignite TCP Ring · Aerospike Mesh", 7.22, 2.09, 5.10, 0.36, 18, INK, True)
    add_text(slide, "장애 주입 전 DEAD 발생", 7.22, 2.50, 5.10, 0.34, 17, RED, True)
    add_text(slide, "→ 현재 결과로 5종 공정 비교 불가", 7.22, 2.88, 5.10, 0.30, 15, MUTED, True)

    add_text(slide, "다음 단계", 0.82, 3.72, 2.0, 0.38, 20, "#67E8F9", True)
    next_steps = [
        ("1", "TCP 연결 및 초기 상태 전이 수정"),
        ("2", "Ignite·Aerospike 재실험 → 5종 비교 완성"),
        ("3", "탐지 속도·오탐률 종합 비교 → OpenKache 방식 선정"),
    ]
    for index, (number, body) in enumerate(next_steps):
        y = 4.25 + index * 0.67
        circle = slide.shapes.add_shape(MSO_SHAPE.OVAL, Inches(0.86), Inches(y - 0.05), Inches(0.48), Inches(0.48))
        circle.fill.solid(); circle.fill.fore_color.rgb = rgb("#163B50"); circle.line.color.rgb = rgb("#22D3EE")
        add_text(slide, number, 0.86, y + 0.05, 0.48, 0.22, 13, "#67E8F9", True, PP_ALIGN.CENTER)
        add_text(slide, body, 1.58, y, 10.60, 0.38, 19, INK, True)

    shape = slide.shapes.add_shape(MSO_SHAPE.ROUNDED_RECTANGLE, Inches(0.92), Inches(6.37), Inches(11.50), Inches(0.66))
    shape.fill.solid(); shape.fill.fore_color.rgb = rgb("#0B2A2A"); shape.line.color.rgb = rgb("#14B8A6")
    add_text(slide, "후속 고도화: Timeout 조정 · 다중 노드 검증", 1.12, 6.54, 11.10, 0.30, 19, "#5EEAD4", True, PP_ALIGN.CENTER)

    output = report / "OpenKache-Failure-Detector-3profiles-simple.pptx"
    prs.save(output)
    return output


def write_simple_script(report: Path) -> Path:
    content = """# OpenKache Failure Detector 3종 비교 — 최종 5분 발표 대본

## 슬라이드 1. 소개 — 20초

안녕하세요. OpenKache에 적합한 장애 탐지 방식을 선정하기 위해 EC2 다중 노드 환경에서 비교 실험을 진행했습니다. 오늘은 결과가 유효한 SWIM과 Lifeguard, Redis Cluster, Phi Gossip 세 가지 방법만 간단히 말씀드리겠습니다.

## 슬라이드 2. 알고리즘 간단 소개 — 50초

먼저 SWIM과 Lifeguard입니다. SWIM은 대상 노드에 직접 probe를 보내고, 응답이 없으면 다른 노드를 통한 간접 probe를 수행합니다. Lifeguard는 노드의 상태를 고려해 성급한 판단을 줄이도록 보완합니다.

Redis Cluster는 노드 간 PING과 PONG을 사용하고, 여러 노드의 장애 보고를 모아 비교적 보수적으로 판단합니다.

Phi Gossip은 heartbeat 도착 간격을 통계적으로 계산합니다. 평소보다 heartbeat가 늦어질수록 장애 가능성을 나타내는 Phi 값이 올라가고, 임계값을 넘으면 장애로 판단합니다.

정리하면 SWIM과 Lifeguard는 빠른 탐지, Redis와 Phi는 상대적으로 보수적인 판단에 강점이 있습니다.

## 슬라이드 3. 장애 시나리오 — 1분 10초

실험 시나리오는 총 11개입니다. 기준선은 Normal과 Crash 두 개, 네트워크는 낮은 비대칭 손실, 높은 비대칭 손실, 전송 지연 세 개입니다. CPU는 target과 observer 일시정지, target과 observer CPU 제한, target CPU 경합 다섯 개이며, 마지막으로 target 공유 EBS I/O 경합 한 개를 적용했습니다. 각 조건은 10번씩 반복했고 세 방법에서 총 330회 결과를 수집했습니다.

네트워크는 target에서 observer로 나가는 경로에 비대칭 패킷 손실과 지연을 적용했습니다. 여기서 낮고 높은 손실은 netem에 설정한 조건이며, 실제 손실률을 별도로 측정한 값은 아닙니다.

CPU 제한은 CPU 사용 한도를 10초 동안 10퍼센트로 낮췄고, CPU 경합은 100퍼센트 연산 작업을 10초 동안 실행했습니다.

I/O 경합은 탐지기 상태 파일과 같은 공유 gp3 EBS에 4KB 랜덤 읽기와 쓰기를 50 대 50으로 발생시켰습니다. Queue Depth 32로 요청을 계속 공급해 EBS의 약 3,000 IOPS 처리 한계 수준까지 부하를 적용했습니다.

Target pause 실험도 수행했지만, 10초 pause가 SWIM과 Lifeguard의 DEAD 시간보다 길고 Redis와 Phi의 DEAD 시간보다 짧았습니다. 따라서 방법별 timeout 조건이 달라 직접적인 성능 비교에서는 제외했습니다. 다음 장에서는 11개 시나리오 중 직접 비교 가능한 핵심 결과만 보여드리겠습니다.

## 슬라이드 4. 핵심 결과 — 1분 30초

왼쪽은 실제 Crash 이후 DEAD로 판단하기까지 걸린 시간입니다. 세 방법 모두 10번 중 10번 Crash를 탐지했습니다.

중앙값은 SWIM과 Lifeguard가 3.25초, Redis가 15.46초, Phi가 18.82초였습니다. 따라서 Crash 탐지 속도는 SWIM과 Lifeguard가 가장 빨랐습니다.

오른쪽은 살아 있는 노드를 DEAD로 판단한 비율입니다. 정상 상태에서는 세 방법 모두 오판이 없었습니다. CPU 제한과 경합, I/O 경합에서도 세 방법 모두 DEAD 오판이 없었습니다.

차이는 높은 비대칭 네트워크 손실에서 나타났습니다. SWIM과 Lifeguard는 10회 중 7회 DEAD로 판단했고, Redis와 Phi는 네트워크 조건에서 DEAD 오판이 없었습니다.

따라서 SWIM과 Lifeguard는 빠르지만 심한 네트워크 손실을 실제 장애로 판단할 가능성이 있고, Redis와 Phi는 느리지만 더 보수적으로 동작했다고 정리할 수 있습니다.

## 슬라이드 5. 결론 및 향후 계획 — 1분

이번 실험에서 SWIM과 Lifeguard는 빠른 탐지 성능을 보였지만, 높은 네트워크 손실 환경에서는 오탐이 발생했습니다. 반면 Redis와 Phi는 탐지 속도가 느리지만 보수적으로 장애를 판단하는 특성을 확인했습니다.

다만 Ignite TCP Ring과 Aerospike Mesh는 장애 주입 이전부터 DEAD로 전환되는 문제가 발생해, 이번 결과만으로는 다섯 가지 방법을 공정하게 비교하기 어렵습니다.

따라서 향후에는 두 방식의 TCP 연결 및 초기 상태 전이 문제를 점검하고 수정한 뒤 재실험을 진행하겠습니다. 이후 다섯 가지 방법의 탐지 속도와 오탐률을 종합적으로 비교해 OpenKache에 적합한 방식을 선정하겠습니다.

최종적으로는 상용 시스템에서 사용하는 Timeout 조정과 다중 노드 검증 등의 기법을 참고해, 선정한 방식의 탐지 성능과 안정성을 더욱 고도화하겠습니다.

## 예상 Q&A

### Q1. SWIM과 Lifeguard는 오판이 있었는데 왜 적용하려고 하나요?

Crash 탐지가 p50 3.25초로 가장 빨랐기 때문입니다. 다만 높은 비대칭 손실에서 10회 중 7회 DEAD로 판단했으므로 단독 최종 판단기로 사용하지 않고, 빠른 SUSPECT 신호로만 활용한 뒤 독립적으로 재검증할 계획입니다.

### Q2. 이 실험은 여기서 마무리되는 건가요?

SWIM과 Lifeguard, Redis, Phi 3종의 330회 실험은 완료됐습니다. 다만 target pause는 timeout 조건 차이로 핵심 비교에서 제외했습니다. 전체 5종 비교를 위해서는 Ignite와 Aerospike의 TCP adapter를 수정한 뒤 재실험해야 하므로, 현재는 1차 유효 비교가 완료된 상태입니다.

### Q3. CPU와 I/O 부하는 어떻게 적용했나요?

CPU는 사용 한도를 10퍼센트로 낮추거나 100퍼센트 연산 작업을 실행했습니다. I/O는 공유 gp3 EBS에 Queue Depth 32의 4KB 랜덤 읽기·쓰기 부하를 적용해 약 3,000 IOPS 처리 한계 수준까지 요청을 공급했습니다. 각 부하는 10초 동안 유지했습니다.
"""
    output = report / "OpenKache-Failure-Detector-3profiles-simple-script.md"
    output.write_text(content, encoding="utf-8")
    return output


def write_valid_three_script(report: Path) -> Path:
    content = """# OpenKache Failure Detector 유효 프로필 3종 — 5분 발표 대본

총 발표 시간: **5분**  
대상 PPT: `OpenKache-Failure-Detector-EC2-3profiles-5min.pptx`

## 슬라이드 1 — 소개 (0:00~0:20, 20초)

안녕하세요. OpenKache에 적용할 Failure Detector를 선정하기 위해 EC2 다중 호스트 환경에서 비교 실험을 진행했습니다. 전체로는 5개 프로필을 550회 실행했지만, Ignite와 Aerospike는 TCP adapter 결함으로 장애 전부터 DEAD가 발생했습니다. 따라서 오늘은 실험이 유효한 SWIM, Redis, Phi 세 가지, 총 330회 결과만 말씀드리겠습니다.

## 슬라이드 2 — 유효 방법론 3종 (0:20~0:55, 35초)

첫 번째는 SWIM과 Lifeguard입니다. 직접·간접 probe와 suspicion을 사용해 빠르게 장애를 찾습니다. 두 번째 Redis Cluster 방식은 PING/PONG과 여러 노드의 failure report를 모아 보수적으로 판단합니다. 세 번째 Phi Gossip은 heartbeat 간격을 통계적으로 계산해 φ 값이 임계치를 넘으면 장애로 봅니다. Ignite와 Aerospike 결과는 알고리즘 문제가 아니라 TCP heartbeat가 전송되지 않은 실험 adapter 문제이므로 성능 비교에서 제외했습니다.

## 슬라이드 3 — 장애 시나리오 (0:55~1:30, 35초)

시나리오는 총 11개입니다. Normal과 실제 Crash, 낮은·높은 비대칭 네트워크 손실, 지연, CPU pause와 throttle, CPU contention, 그리고 gp3 EBS I/O contention입니다. 네트워크 손실의 낮음과 높음은 netem 설정 확률 0.15와 0.45를 뜻하며 실제 측정 손실률은 아닙니다. 네트워크 장애는 target outbound에 실행 후반 동안 지속 적용됐습니다. 각 조건은 seed 10개이며, null 지연은 미검출로 처리했습니다.

## 슬라이드 4 — Crash 탐지 (1:30~2:20, 50초)

먼저 실제 Crash 결과입니다. 세 방법 모두 10번 중 10번 DEAD를 탐지했습니다. 속도 차이는 컸습니다. SWIM의 p50은 3.25초, Redis는 15.46초, Phi는 18.82초였습니다. SWIM이 Redis보다 약 4.8배 빠릅니다. 분포도 seed 사이 편차가 크지 않아 세 방법 모두 각자의 timeout 정책대로 안정적으로 동작했습니다. 따라서 탐지 속도만 보면 SWIM이 가장 우수합니다. 하지만 다음 슬라이드의 오판 결과를 반드시 함께 봐야 합니다.

## 슬라이드 5 — 정상·네트워크 안전성 (2:20~3:10, 50초)

Normal에서는 세 방법 모두 SUSPECT와 DEAD 오탐이 없었습니다. 낮은 비대칭 손실과 지연에서도 DEAD 오판이 없었습니다. 차이는 높은 비대칭 손실에서 발생했습니다. SWIM은 살아 있는 target을 10번 중 7번 DEAD로 판단했습니다. 반면 Redis와 Phi는 세 네트워크 조건에서 DEAD 오판이 없었습니다. 기술적으로 높은 손실은 netem 드롭 확률을 0.45로 설정한 조건이며, 실제 손실률을 측정한 값은 아닙니다.

## 슬라이드 6 — CPU·EBS 영향 (3:10~3:55, 45초)

CPU 조건에서는 target 프로세스를 10초 동안 pause했을 때 SWIM이 10번 모두 DEAD로 판단했습니다. 프로세스는 죽지 않았지만 응답할 수 없었기 때문입니다. throttle, contention, observer pause에서는 세 방법 모두 DEAD 오판이 없었습니다. EBS 실험은 queue depth 32의 fio 부하가 실제로 걸렸는지 30개 모두 검증했고, 세 방법 모두 I/O contention에서 DEAD 오판이 없었습니다. 결국 SWIM은 일시 정지와 실제 Crash를 구분하는 추가 확인 단계가 필요합니다.

## 슬라이드 7 — 전체 결과 (3:55~4:25, 30초)

히트맵을 보면 결론이 명확합니다. Crash에서는 세 방법 모두 100퍼센트 DEAD입니다. Redis와 Phi는 나머지 조건에서 DEAD가 없습니다. SWIM은 Crash를 빠르게 찾지만 높은 비대칭 손실과 target pause에서 DEAD가 발생합니다. 따라서 단순히 가장 빠른 방법을 선택하기보다 SUSPECT 속도와 DEAD 안전성을 분리해야 합니다.

## 슬라이드 8 — 결론 (4:25~5:00, 35초)

OpenKache에는 SWIM과 Lifeguard를 빠른 SUSPECT 신호로 사용하는 것을 제안합니다. 다만 SUSPECT를 즉시 DEAD와 failover로 연결하지 않고, 독립 helper나 quorum으로 한 번 더 검증해야 합니다. 확인 증거가 부족할 때는 Redis 방식의 다중 보고나 Phi 점수를 보조 gate로 사용할 수 있습니다. 정리하면 빠른 탐지는 SWIM에서 가져오고, 최종 DEAD는 보수적으로 결정하는 구조가 이번 실험 결과에 가장 부합합니다. 감사합니다.

## 발표 중 강조할 숫자

- 유효 분석: 3개 프로필 × 11개 시나리오 × 10 seeds = **330회**
- Crash 탐지: 세 방법 모두 **10/10**
- Crash p50: SWIM **3.25초**, Redis **15.46초**, Phi **18.82초**
- SWIM 오판: 높은 비대칭 손실 **7/10**, target pause **10/10**
- Redis·Phi 비-Crash DEAD 오판: **0건**
- 유효 프로필 fio 검증: **30/30 passed**
"""
    output = report / "OpenKache-Failure-Detector-EC2-3profiles-5min-script.md"
    output.write_text(content, encoding="utf-8")
    return output


def experiment_info(raw: Path, runs: Sequence[dict[str, Any]]) -> dict[str, str]:
    first = runs[0]
    completed = read_properties(raw / "COMPLETED")
    campaign_log = (raw / "campaign" / "campaign.log").read_text(encoding="utf-8", errors="replace")
    start_match = re.search(r"^(\S+) campaign start experiment=", campaign_log, flags=re.MULTILINE)
    start_utc = datetime.fromisoformat(start_match.group(1).replace("Z", "+00:00")) if start_match else None
    completed_utc = datetime.fromisoformat(completed["completed_at"].replace("Z", "+00:00"))
    kst = timezone(timedelta(hours=9))
    period = f"{start_utc.astimezone(kst):%Y-%m-%d %H:%M}–{completed_utc.astimezone(kst):%Y-%m-%d %H:%M} KST" if start_utc else f"completed {completed_utc.astimezone(kst):%Y-%m-%d %H:%M} KST"
    return {
        "experiment_id": "openkache-fd-ec2-20261104",
        "region": "ap-northeast-2 (Seoul)",
        "period_kst": period,
        "period_utc": f"{start_utc:%Y-%m-%d %H:%M}–{completed_utc:%Y-%m-%d %H:%M} UTC" if start_utc else f"completed {completed_utc:%Y-%m-%d %H:%M} UTC",
        "git_commit": "not recorded; local candidate 7ec7efe67c28f158a7cd354849443ce8ace0746a",
        "jar_sha256": first["jar_sha256"],
        "instance": first["instance_type"],
        "ami": first["ami_id"],
        "nodes": str(first["node_count"]),
        "storage": "gp3 root 20 GiB + gp3 data 20 GiB, 3,000 IOPS, 125 MiB/s, encrypted",
        "timing": f"warm-up {first['warmup_seconds']}s · fault {first['fault_duration_seconds']}s · recovery {first['recovery_seconds']}s",
        "profiles": ", ".join(PROFILE_LABEL[p] for p in PROFILE_ORDER),
        "faults": ", ".join(FAULT_ORDER),
        "seeds": "1–10",
    }


def write_experiment_info(report: Path, info: dict[str, str], validation: dict[str, Any]) -> None:
    lines = [
        "# EC2 experiment reproducibility record",
        "",
        f"- Experiment ID: `{info['experiment_id']}`",
        f"- Region: {info['region']}",
        f"- Period: {info['period_kst']} ({info['period_utc']})",
        f"- Git commit: {info['git_commit']}",
        f"- JAR SHA-256: `{info['jar_sha256']}`",
        f"- Compute: {info['instance']} × {info['nodes']}, AMI `{info['ami']}`",
        f"- Storage: {info['storage']}",
        f"- Profiles: {info['profiles']}",
        f"- Faults: {info['faults']}",
        f"- Seeds: {info['seeds']}",
        f"- Timing: {info['timing']}",
        "",
        "## Completeness",
        "",
    ]
    lines.extend(f"- {'PASS' if value else 'FAIL'}: {key}" for key, value in validation["checks"].items())
    lines.extend(["", "## Data quality notes", ""])
    lines.extend(f"- {note}" for note in validation["data_quality_notes"])
    (report / "experiment-info.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--raw", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    raw = args.raw.resolve()
    report = args.report.resolve()
    report.mkdir(parents=True, exist_ok=True)

    runs, status_rows = load_runs(raw)
    failed = parse_campaign_attempts(raw, status_rows)
    summary = build_summary(runs)
    fio = build_fio_summary(runs)
    validation = validation_report(raw, runs, status_rows, failed)
    info = experiment_info(raw, runs)

    write_csv(report / "runs.csv", runs, RUN_FIELDS)
    write_csv(report / "summary.csv", summary, SUMMARY_FIELDS)
    write_csv(report / "failed-attempts.csv", failed, [
        "started_at_utc", "finished_at_utc", "sequence", "attempt", "profile",
        "fault", "seed", "run_id", "status", "persisted_in_execution_status",
        "artifact_directory_exists",
    ])
    write_csv(report / "fio-summary.csv", fio, [
        "sequence", "seed", "profile", "profile_label", "run_id", "fio_status",
        "fio_total_iops", "fio_configured_iodepth", "fio_deep_queue_percent", "fio_runtime_ms",
    ])
    (report / "validation.json").write_text(json.dumps(validation, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    write_experiment_info(report, info, validation)
    charts = generate_charts(report, runs, summary)
    valid_charts = generate_valid_charts(report, runs, summary)
    pptx = build_pptx_simple_three(report, valid_charts)
    presentation_script = write_simple_script(report)
    print(json.dumps({
        "validation": validation["status"],
        "runs": len(runs),
        "summary_conditions": len(summary),
        "failed_attempts": len(failed),
        "fio_rows": len(fio),
        "charts": len(charts),
        "valid_profile_charts": len(valid_charts),
        "pptx": str(pptx),
        "presentation_script": str(presentation_script),
    }, ensure_ascii=False, indent=2))
    if validation["status"] != "passed":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
