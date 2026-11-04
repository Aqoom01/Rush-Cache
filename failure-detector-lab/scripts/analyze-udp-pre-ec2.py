#!/usr/bin/env python3
"""Aggregate real UDP/Docker runs without mixing them with simulator results."""

from __future__ import annotations

import argparse
import csv
import html
import json
import math
import random
import zlib
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterable


def properties(path: Path) -> dict[str, str]:
    result: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if line and not line.startswith("#") and "=" in line:
            key, value = line.split("=", 1)
            result[key] = value
    return result


def millis(value: Any) -> float | None:
    return None if value is None else float(value) / 1_000_000.0


MINIMUM_POST_FAULT_OBSERVATION_MS = {
    "redis-cluster": 18_000.0,
    "phi-gossip": 22_000.0,
    "ignite-tcp-ring": 6_000.0,
    "aerospike-mesh": 4_000.0,
    "swim-lifeguard": 5_000.0,
}
NORMAL_MINIMUM_OBSERVATION_MS = 60_000.0


def percentage(value: Any) -> float | None:
    if not isinstance(value, str) or not value.endswith("%"):
        return None
    try:
        return float(value[:-1])
    except ValueError:
        return None


def percentile(values: Iterable[float], quantile: float) -> float | None:
    ordered = sorted(values)
    if not ordered:
        return None
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * quantile
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower)


def wilson_interval(successes: int, total: int, z: float = 1.959963984540054) -> tuple[float | None, float | None]:
    if total <= 0:
        return None, None
    rate = successes / total
    denominator = 1.0 + z * z / total
    center = (rate + z * z / (2.0 * total)) / denominator
    margin = (
        z
        * math.sqrt(rate * (1.0 - rate) / total + z * z / (4.0 * total * total))
        / denominator
    )
    return max(0.0, center - margin), min(1.0, center + margin)


def bootstrap_percentile_interval(
    values: list[float], quantile: float, seed: int, resamples: int = 10_000
) -> tuple[float | None, float | None]:
    if len(values) < 2:
        return None, None
    generator = random.Random(seed)
    size = len(values)
    estimates = [
        percentile((values[generator.randrange(size)] for _ in range(size)), quantile)
        for _ in range(resamples)
    ]
    return percentile(estimates, 0.025), percentile(estimates, 0.975)


def resource_peaks(path: Path) -> dict[str, dict[str, float]]:
    peaks: dict[str, dict[str, float]] = defaultdict(
        lambda: {"cpu_percent": 0.0, "memory_percent": 0.0}
    )
    if not path.exists():
        return peaks
    for line in path.read_text(encoding="utf-8").splitlines():
        try:
            sample = json.loads(line)
        except json.JSONDecodeError:
            continue
        service = sample.get("service")
        stats = sample.get("stats", {})
        if not isinstance(service, str) or not isinstance(stats, dict):
            continue
        cpu = percentage(stats.get("CPUPerc"))
        memory = percentage(stats.get("MemPerc"))
        if cpu is not None:
            peaks[service]["cpu_percent"] = max(peaks[service]["cpu_percent"], cpu)
        if memory is not None:
            peaks[service]["memory_percent"] = max(
                peaks[service]["memory_percent"], memory
            )
    return peaks


def actual_fault_duration(path: Path) -> float | None:
    applied: int | None = None
    released: int | None = None
    if not path.exists():
        return None
    for line in path.read_text(encoding="utf-8").splitlines():
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        if event.get("event") == "applied":
            applied = int(event["wallEpochNanos"])
        elif event.get("event") == "released":
            released = int(event["wallEpochNanos"])
    if applied is None or released is None or released < applied:
        return None
    return (released - applied) / 1_000_000.0


def detector_event_count(
    path: Path, status: str, before_nanos: int | None
) -> int:
    """Count observer transitions from legacy logs when result.json lacks counters."""
    if not path.exists():
        return 0
    count = 0
    prefix = "EVENT_JSON="
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        if not line.startswith(prefix):
            continue
        try:
            event = json.loads(line[len(prefix) :])
        except json.JSONDecodeError:
            continue
        if event.get("kind") != "detector" or event.get("newStatus") != status:
            continue
        at_nanos = event.get("atNanos")
        if not isinstance(at_nanos, int):
            continue
        if before_nanos is None or at_nanos < before_nanos:
            count += 1
    return count


def cgroup_metrics(path: Path) -> dict[str, int]:
    result = {
        "cpu_usage_usec": 0,
        "cpu_throttled_usec": 0,
        "cpu_nr_throttled": 0,
        "io_read_bytes": 0,
        "io_write_bytes": 0,
        "io_read_ops": 0,
        "io_write_ops": 0,
    }
    if not path.exists():
        return result
    section = ""
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.startswith("### "):
            section = line[4:]
            continue
        if section.endswith("/cpu.stat"):
            parts = line.split()
            if len(parts) != 2 or not parts[1].isdigit():
                continue
            mapping = {
                "usage_usec": "cpu_usage_usec",
                "throttled_usec": "cpu_throttled_usec",
                "nr_throttled": "cpu_nr_throttled",
            }
            if parts[0] in mapping:
                result[mapping[parts[0]]] = int(parts[1])
        elif section.endswith("/io.stat"):
            for item in line.split()[1:]:
                if "=" not in item:
                    continue
                key, value = item.split("=", 1)
                mapping = {
                    "rbytes": "io_read_bytes",
                    "wbytes": "io_write_bytes",
                    "rios": "io_read_ops",
                    "wios": "io_write_ops",
                }
                if key in mapping and value.isdigit():
                    result[mapping[key]] += int(value)
    return result


def cgroup_delta(run_dir: Path, service: str) -> dict[str, int]:
    before = cgroup_metrics(run_dir / "cgroup" / f"{service}-before-fault.txt")
    after = cgroup_metrics(run_dir / "cgroup" / f"{service}-after-fault.txt")
    return {key: max(0, after[key] - before[key]) for key in before}


def load_runs(input_dir: Path) -> list[dict[str, Any]]:
    runs: list[dict[str, Any]] = []
    passed_directories: set[Path] | None = None
    execution_status = input_dir / "execution-status.csv"
    if execution_status.exists():
        with execution_status.open(encoding="utf-8", newline="") as source:
            passed_directories = {
                Path(row["run_dir"]).resolve()
                for row in csv.DictReader(source)
                if row.get("status") == "passed" and row.get("run_dir")
            }
    for result_path in sorted(input_dir.rglob("result.json")):
        run_dir = result_path.parent
        if passed_directories is not None and run_dir.resolve() not in passed_directories:
            continue
        property_path = run_dir / "run.properties"
        if not property_path.exists():
            continue
        config = properties(property_path)
        compose_config = properties(run_dir / "compose.env") if (run_dir / "compose.env").exists() else {}
        result = json.loads(result_path.read_text(encoding="utf-8"))
        peaks = resource_peaks(run_dir / "resource-stats.jsonl")
        observer_cgroup = cgroup_delta(run_dir, "node0")
        target_cgroup = cgroup_delta(run_dir, "node1")
        fault = config.get("fault", "unknown")
        process_alive = fault != "crash"
        dead_latency = millis(result.get("deadLatencyNanos"))
        profile = config.get("profile", result.get("profile", "unknown"))
        run_duration_ms = millis(result.get("runDurationNanos"))
        if run_duration_ms is None:
            duration_seconds = config.get("observer_duration_seconds") or compose_config.get(
                "FD_OBSERVER_DURATION_SECONDS"
            )
            run_duration_ms = (
                float(duration_seconds) * 1_000.0 if duration_seconds is not None else None
            )
        fault_at_ms = millis(result.get("faultAtNanos"))
        observation_window_ms = (
            run_duration_ms
            if fault == "normal"
            else run_duration_ms - fault_at_ms
            if run_duration_ms is not None and fault_at_ms is not None
            else None
        )
        required_observation_ms = (
            NORMAL_MINIMUM_OBSERVATION_MS
            if fault == "normal"
            else MINIMUM_POST_FAULT_OBSERVATION_MS.get(profile, 0.0)
        )
        valid_observation_window = (
            observation_window_ms is not None
            and observation_window_ms >= required_observation_ms
        )
        fault_at_nanos = result.get("faultAtNanos")
        pre_fault_suspect_events = (
            int(result["preFaultSuspectEvents"])
            if "preFaultSuspectEvents" in result
            else detector_event_count(
                run_dir / "events.log", "SUSPECT", fault_at_nanos
            )
        )
        pre_fault_dead_events = int(result.get("preFaultDeadEvents", 0))
        dead_decision = dead_latency is not None or (
            fault == "normal" and pre_fault_dead_events > 0
        )
        run = {
            "run_dir": str(run_dir),
            "profile": profile,
            "fault": fault,
            "seed": config.get("netem_seed", ""),
            "node_count": int(config.get("nodes", 0)),
            "source": config.get("source", "unknown"),
            "environment": config.get("environment", "unknown"),
            "time_source": result.get("timeSource"),
            "wire_source": result.get("wireSource"),
            "run_duration_ms": run_duration_ms,
            "post_fault_observation_ms": observation_window_ms,
            "required_observation_ms": required_observation_ms,
            "valid_observation_window": valid_observation_window,
            "suspect_latency_ms": millis(result.get("suspectLatencyNanos")),
            "dead_latency_ms": dead_latency,
            "recovery_latency_ms": millis(result.get("recoveryLatencyNanos")),
            "dead_decision": dead_decision,
            "crash_detected": fault == "crash" and dead_latency is not None,
            "dead_while_process_alive": process_alive and dead_decision,
            "pre_fault_suspect_events": pre_fault_suspect_events,
            "pre_fault_dead_events": pre_fault_dead_events,
            "normal_false_suspect": fault == "normal" and pre_fault_suspect_events > 0,
            "normal_false_dead": fault == "normal" and pre_fault_dead_events > 0,
            "fault_duration_actual_ms": actual_fault_duration(
                run_dir / "fault-events.jsonl"
            ),
            "observer_peak_cpu_percent": peaks["node0"]["cpu_percent"],
            "target_peak_cpu_percent": peaks["node1"]["cpu_percent"],
            "observer_peak_memory_percent": peaks["node0"]["memory_percent"],
            "target_peak_memory_percent": peaks["node1"]["memory_percent"],
            "observer_cpu_usage_delta_ms": observer_cgroup["cpu_usage_usec"] / 1_000.0,
            "target_cpu_usage_delta_ms": target_cgroup["cpu_usage_usec"] / 1_000.0,
            "observer_cpu_throttled_delta_ms": observer_cgroup["cpu_throttled_usec"] / 1_000.0,
            "target_cpu_throttled_delta_ms": target_cgroup["cpu_throttled_usec"] / 1_000.0,
            "target_throttle_events_delta": target_cgroup["cpu_nr_throttled"],
            "target_io_read_bytes_delta": target_cgroup["io_read_bytes"],
            "target_io_write_bytes_delta": target_cgroup["io_write_bytes"],
            "target_io_read_ops_delta": target_cgroup["io_read_ops"],
            "target_io_write_ops_delta": target_cgroup["io_write_ops"],
            "sent_frames": int(result.get("sentFrames", 0)),
            "sent_bytes": int(result.get("sentBytes", 0)),
            "received_frames": int(result.get("receivedFrames", 0)),
            "received_bytes": int(result.get("receivedBytes", 0)),
        }
        runs.append(run)
    return runs


def write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str]) -> None:
    with path.open("w", encoding="utf-8", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def deduplicate_runs(runs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Keep the latest input for one logical condition, preferring a valid rerun."""
    selected: dict[tuple[str, str, str, int], dict[str, Any]] = {}
    for run in runs:
        key = (run["profile"], run["fault"], run["seed"], run["node_count"])
        current = selected.get(key)
        if current is None or run["valid_observation_window"] or not current["valid_observation_window"]:
            selected[key] = run
    return sorted(
        selected.values(),
        key=lambda run: (run["profile"], run["fault"], run["seed"], run["node_count"]),
    )


def summarize(runs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for run in runs:
        groups[(run["profile"], run["fault"])].append(run)
    summaries: list[dict[str, Any]] = []
    for (profile, fault), selected in sorted(groups.items()):
        valid = [run for run in selected if run["valid_observation_window"]]
        dead = [run["dead_latency_ms"] for run in valid if run["dead_latency_ms"] is not None]
        recovery = [
            run["recovery_latency_ms"]
            for run in valid
            if run["recovery_latency_ms"] is not None
        ]
        observer_cpu = [run["observer_peak_cpu_percent"] for run in valid]
        target_cpu = [run["target_peak_cpu_percent"] for run in valid]
        observer_memory = [run["observer_peak_memory_percent"] for run in valid]
        target_memory = [run["target_peak_memory_percent"] for run in valid]
        sent_frames = [float(run["sent_frames"]) for run in valid]
        received_frames = [float(run["received_frames"]) for run in valid]
        observer_wire_bytes = [
            float(run["sent_bytes"] + run["received_bytes"]) for run in valid
        ]
        decisions = sum(bool(run["dead_decision"]) for run in valid)
        alive_dead = sum(bool(run["dead_while_process_alive"]) for run in valid)
        total = len(valid)
        attempted = len(selected)
        normal_false_suspect_runs = sum(
            bool(run["normal_false_suspect"]) for run in valid
        )
        normal_false_dead_runs = sum(
            bool(run["normal_false_dead"]) for run in valid
        )
        suspect_ci_low, suspect_ci_high = wilson_interval(
            normal_false_suspect_runs, total
        )
        dead_ci_low, dead_ci_high = wilson_interval(normal_false_dead_runs, total)
        crash_ci_low, crash_ci_high = (
            wilson_interval(decisions, total) if fault == "crash" else (None, None)
        )
        alive_dead_ci_low, alive_dead_ci_high = wilson_interval(alive_dead, total)
        recovery_ci_low, recovery_ci_high = wilson_interval(
            len(recovery), alive_dead
        )
        bootstrap_seed = zlib.crc32(f"{profile}:{fault}:dead".encode("utf-8"))
        dead_p50_ci_low, dead_p50_ci_high = bootstrap_percentile_interval(
            dead, 0.50, bootstrap_seed
        )
        dead_p95_ci_low, dead_p95_ci_high = bootstrap_percentile_interval(
            dead, 0.95, bootstrap_seed ^ 0x5F3759DF
        )
        summaries.append(
            {
                "profile": profile,
                "fault": fault,
                "runs": total,
                "attempted_runs": attempted,
                "invalid_observation_runs": attempted - total,
                "dead_decisions": decisions,
                "dead_decision_rate": decisions / total if total else None,
                "crash_detection_success_rate": decisions / total if fault == "crash" and total else None,
                "crash_detection_ci95_low": crash_ci_low,
                "crash_detection_ci95_high": crash_ci_high,
                "dead_while_process_alive_runs": alive_dead,
                "dead_while_process_alive_rate": alive_dead / total if total else None,
                "dead_while_process_alive_ci95_low": alive_dead_ci_low,
                "dead_while_process_alive_ci95_high": alive_dead_ci_high,
                "pre_fault_dead_events": sum(run["pre_fault_dead_events"] for run in valid),
                "pre_fault_suspect_events": sum(
                    run["pre_fault_suspect_events"] for run in valid
                ),
                "normal_false_suspect_runs": normal_false_suspect_runs,
                "normal_false_suspect_rate": normal_false_suspect_runs / total if total else None,
                "normal_false_suspect_ci95_low": suspect_ci_low,
                "normal_false_suspect_ci95_high": suspect_ci_high,
                "normal_false_dead_runs": normal_false_dead_runs,
                "normal_false_dead_rate": normal_false_dead_runs / total if total else None,
                "normal_false_dead_ci95_low": dead_ci_low,
                "normal_false_dead_ci95_high": dead_ci_high,
                "dead_latency_p50_ms": percentile(dead, 0.50),
                "dead_latency_p50_ci95_low_ms": dead_p50_ci_low,
                "dead_latency_p50_ci95_high_ms": dead_p50_ci_high,
                "dead_latency_p95_ms": percentile(dead, 0.95),
                "dead_latency_p95_ci95_low_ms": dead_p95_ci_low,
                "dead_latency_p95_ci95_high_ms": dead_p95_ci_high,
                "dead_latency_p99_ms": percentile(dead, 0.99),
                "dead_latency_missing": total - len(dead),
                "recovery_observed_runs": len(recovery),
                "recovery_observed_rate_of_dead": len(recovery) / alive_dead if alive_dead else None,
                "recovery_observed_ci95_low": recovery_ci_low,
                "recovery_observed_ci95_high": recovery_ci_high,
                "recovery_latency_p50_ms": percentile(recovery, 0.50),
                "recovery_latency_p95_ms": percentile(recovery, 0.95),
                "observer_peak_cpu_p50_percent": percentile(observer_cpu, 0.50),
                "observer_peak_cpu_p95_percent": percentile(observer_cpu, 0.95),
                "target_peak_cpu_p50_percent": percentile(target_cpu, 0.50),
                "target_peak_cpu_p95_percent": percentile(target_cpu, 0.95),
                "observer_peak_memory_p95_percent": percentile(observer_memory, 0.95),
                "target_peak_memory_p95_percent": percentile(target_memory, 0.95),
                "observer_sent_frames_p50": percentile(sent_frames, 0.50),
                "observer_sent_frames_p95": percentile(sent_frames, 0.95),
                "observer_received_frames_p50": percentile(received_frames, 0.50),
                "observer_received_frames_p95": percentile(received_frames, 0.95),
                "observer_wire_bytes_p50": percentile(observer_wire_bytes, 0.50),
                "observer_wire_bytes_p95": percentile(observer_wire_bytes, 0.95),
                "target_cpu_throttled_p95_ms": percentile(
                    [run["target_cpu_throttled_delta_ms"] for run in valid], 0.95
                ),
                "target_io_write_p95_bytes": percentile(
                    [float(run["target_io_write_bytes_delta"]) for run in valid], 0.95
                ),
            }
        )
    return summaries


def svg_message(path: Path, title: str, message: str) -> None:
    path.write_text(
        f'<svg xmlns="http://www.w3.org/2000/svg" width="960" height="180" viewBox="0 0 960 180">'
        '<rect width="960" height="180" fill="#fff"/>'
        f'<text x="30" y="45" font-family="sans-serif" font-size="24" font-weight="700">{html.escape(title)}</text>'
        f'<text x="30" y="95" font-family="sans-serif" font-size="17" fill="#555">{html.escape(message)}</text>'
        '</svg>',
        encoding="utf-8",
    )


def bar_chart(path: Path, title: str, rows: list[tuple[str, float]], unit: str) -> None:
    rows = [(label, value) for label, value in rows if value is not None]
    if not rows:
        svg_message(path, title, "측정된 값이 없습니다.")
        return
    width = 1000
    row_height = 54
    height = 100 + row_height * len(rows)
    left = 290
    chart_width = 620
    maximum = max(value for _, value in rows) or 1.0
    elements = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        f'<rect width="{width}" height="{height}" fill="#fff"/>',
        f'<text x="30" y="42" font-family="sans-serif" font-size="24" font-weight="700">{html.escape(title)}</text>',
    ]
    for index, (label, value) in enumerate(rows):
        y = 75 + index * row_height
        bar = chart_width * value / maximum
        elements.extend(
            [
                f'<text x="{left - 12}" y="{y + 20}" text-anchor="end" font-family="sans-serif" font-size="14">{html.escape(label)}</text>',
                f'<rect x="{left}" y="{y}" width="{bar:.1f}" height="28" rx="4" fill="#3578e5"/>',
                f'<text x="{left + bar + 8:.1f}" y="{y + 20}" font-family="sans-serif" font-size="14">{value:.2f}{html.escape(unit)}</text>',
            ]
        )
    elements.append("</svg>")
    path.write_text("".join(elements), encoding="utf-8")


def paired_bar_chart(
    path: Path,
    title: str,
    rows: list[tuple[str, float | None, float | None]],
    first_label: str,
    second_label: str,
    unit: str,
) -> None:
    if not rows:
        svg_message(path, title, "측정된 값이 없습니다.")
        return
    width, left, chart_width = 1100, 285, 690
    row_height = 74
    height = 115 + row_height * len(rows)
    values = [value for _, first, second in rows for value in (first, second) if value is not None]
    maximum = max(values) if values else 1.0
    output = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        f'<rect width="{width}" height="{height}" fill="#fff"/>',
        f'<text x="30" y="40" font-family="sans-serif" font-size="24" font-weight="700">{html.escape(title)}</text>',
        f'<rect x="{left}" y="57" width="16" height="16" fill="#f3a43b"/><text x="{left + 24}" y="70" font-family="sans-serif" font-size="13">{html.escape(first_label)}</text>',
        f'<rect x="{left + 130}" y="57" width="16" height="16" fill="#3578e5"/><text x="{left + 154}" y="70" font-family="sans-serif" font-size="13">{html.escape(second_label)}</text>',
    ]
    for index, (label, first, second) in enumerate(rows):
        y = 91 + index * row_height
        output.append(
            f'<text x="{left - 12}" y="{y + 29}" text-anchor="end" font-family="sans-serif" font-size="14">{html.escape(label)}</text>'
        )
        for offset, value, color in ((0, first, "#f3a43b"), (30, second, "#3578e5")):
            if value is None:
                output.append(
                    f'<text x="{left + 8}" y="{y + offset + 18}" font-family="sans-serif" font-size="13" fill="#777">N/A</text>'
                )
                continue
            bar = chart_width * value / maximum
            output.extend(
                [
                    f'<rect x="{left}" y="{y + offset}" width="{bar:.1f}" height="22" rx="4" fill="{color}"/>',
                    f'<text x="{left + bar + 8:.1f}" y="{y + offset + 16}" font-family="sans-serif" font-size="13">{value:.2f}{html.escape(unit)}</text>',
                ]
            )
    output.append("</svg>")
    path.write_text("".join(output), encoding="utf-8")


def interval_chart(
    path: Path,
    title: str,
    rows: list[tuple[str, float | None, float | None, float | None]],
    unit: str,
) -> None:
    rows = [row for row in rows if all(value is not None for value in row[1:])]
    if not rows:
        svg_message(path, title, "측정된 값이 없습니다.")
        return
    width, left, chart_width = 1100, 285, 690
    row_height = 58
    height = 100 + row_height * len(rows)
    maximum = max(high for _, _, _, high in rows) or 1.0
    output = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        f'<rect width="{width}" height="{height}" fill="#fff"/>',
        f'<text x="30" y="42" font-family="sans-serif" font-size="24" font-weight="700">{html.escape(title)}</text>',
        f'<text x="{left}" y="68" font-family="sans-serif" font-size="13" fill="#666">점: 추정치 · 선: bootstrap 95% CI</text>',
    ]
    for index, (label, point, low, high) in enumerate(rows):
        y = 92 + index * row_height
        low_x = left + chart_width * low / maximum
        point_x = left + chart_width * point / maximum
        high_x = left + chart_width * high / maximum
        output.extend(
            [
                f'<text x="{left - 12}" y="{y + 5}" text-anchor="end" font-family="sans-serif" font-size="14">{html.escape(label)}</text>',
                f'<line x1="{left}" y1="{y}" x2="{left + chart_width}" y2="{y}" stroke="#e5e7eb" stroke-width="2"/>',
                f'<line x1="{low_x:.1f}" y1="{y}" x2="{high_x:.1f}" y2="{y}" stroke="#3578e5" stroke-width="6" stroke-linecap="round"/>',
                f'<circle cx="{point_x:.1f}" cy="{y}" r="7" fill="#f3a43b" stroke="#fff" stroke-width="2"/>',
                f'<text x="{high_x + 10:.1f}" y="{y + 5}" font-family="sans-serif" font-size="13">{point:.2f}{html.escape(unit)} [{low:.2f}, {high:.2f}]</text>',
            ]
        )
    output.append("</svg>")
    path.write_text("".join(output), encoding="utf-8")


def heatmap(path: Path, summaries: list[dict[str, Any]]) -> None:
    profiles = sorted({row["profile"] for row in summaries})
    faults = sorted({row["fault"] for row in summaries})
    values = {(row["profile"], row["fault"]): row["dead_decision_rate"] for row in summaries}
    cell_w, cell_h, left, top = 155, 45, 255, 100
    width = left + cell_w * len(profiles) + 30
    height = top + cell_h * len(faults) + 30
    output = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        f'<rect width="{width}" height="{height}" fill="#fff"/>',
        '<text x="24" y="38" font-family="sans-serif" font-size="24" font-weight="700">DEAD 판단 실행 비율</text>',
    ]
    for column, profile in enumerate(profiles):
        x = left + column * cell_w + cell_w / 2
        output.append(
            f'<text x="{x}" y="76" text-anchor="middle" font-family="sans-serif" font-size="12">{html.escape(profile)}</text>'
        )
    for row_index, fault in enumerate(faults):
        y = top + row_index * cell_h
        output.append(
            f'<text x="{left - 10}" y="{y + 28}" text-anchor="end" font-family="sans-serif" font-size="13">{html.escape(fault)}</text>'
        )
        for column, profile in enumerate(profiles):
            value = values.get((profile, fault))
            x = left + column * cell_w
            if value is None:
                color, label = "#eeeeee", "N/A"
            else:
                red = int(245 - 115 * value)
                green = int(250 - 105 * value)
                blue = int(255 - 25 * value)
                color, label = f"#{red:02x}{green:02x}{blue:02x}", f"{value * 100:.1f}%"
            output.extend(
                [
                    f'<rect x="{x + 2}" y="{y + 2}" width="{cell_w - 4}" height="{cell_h - 4}" rx="4" fill="{color}"/>',
                    f'<text x="{x + cell_w / 2}" y="{y + 28}" text-anchor="middle" font-family="sans-serif" font-size="13">{label}</text>',
                ]
            )
    output.append("</svg>")
    path.write_text("".join(output), encoding="utf-8")


def report(path: Path, runs: list[dict[str, Any]], summaries: list[dict[str, Any]]) -> None:
    crash = [row for row in summaries if row["fault"] == "crash"]
    normal = [row for row in summaries if row["fault"] == "normal"]
    loss45 = [row for row in summaries if row["fault"] == "loss45"]
    delay = [row for row in summaries if row["fault"] == "delay"]
    target_pause = [row for row in summaries if row["fault"] == "cpu-pause-target"]
    valid_runs = [run for run in runs if run["valid_observation_window"]]
    pre_fault_suspect_events = sum(run["pre_fault_suspect_events"] for run in runs)
    pre_fault_dead_events = sum(run["pre_fault_dead_events"] for run in runs)
    profiles = sorted({run["profile"] for run in runs})
    summary_by_key = {(row["profile"], row["fault"]): row for row in summaries}
    lines = [
        "# OpenKache pre-EC2 Docker 실험 보고서",
        "",
        f"- 집계 실행: {len(runs)}회",
        f"- 통계 유효 실행: {len(valid_runs)}회 / 관찰창 미달: {len(runs) - len(valid_runs)}회",
        f"- fault 이전 SUSPECT 이벤트: {pre_fault_suspect_events}건",
        f"- fault 이전 DEAD 이벤트: {pre_fault_dead_events}건",
        "- 계층: 독립 컨테이너/JVM + 실제 UDP/TCP socket (profile별 wire source 기록)",
        "- 주의: 단일 Docker 호스트이므로 EC2 다중 호스트의 커널·NIC·스토리지 변동성을 재현하지 않는다.",
        "- `io-contention-target`은 Docker 가상 디스크 경합이며 실제 SSD 고정 latency 실험이 아니다.",
        "",
        "## 그래프",
        "",
        "![Crash DEAD p95](charts/crash-dead-latency-p95.svg)",
        "",
        "![Crash DEAD p95 bootstrap CI](charts/crash-dead-latency-p95-ci.svg)",
        "",
        "![Crash observer CPU p95](charts/crash-observer-cpu-p95.svg)",
        "",
        "![Crash observer wire bytes p50](charts/crash-observer-wire-bytes-p50.svg)",
        "",
        "![Crash SUSPECT vs DEAD](charts/crash-suspect-vs-dead.svg)",
        "",
        "![Observation window](charts/post-fault-observation-window.svg)",
        "",
        "![Fault DEAD p95](charts/fault-dead-latency-p95.svg)",
        "",
        "![Recovery p95](charts/recovery-latency-p95.svg)",
        "",
        "![DEAD 판단 비율](charts/dead-decision-rate.svg)",
        "",
        "![Target CPU p95](charts/target-peak-cpu-p95.svg)",
        "",
        "![Target CPU throttled p95](charts/target-cpu-throttled-p95.svg)",
        "",
        "![Target I/O write p95](charts/target-io-write-p95.svg)",
        "",
        "![Normal false-positive runs](charts/normal-false-positive-runs.svg)",
        "",
        "![Normal observer CPU p95](charts/normal-observer-cpu-p95.svg)",
        "",
        "![Normal observer wire bytes p50](charts/normal-observer-wire-bytes-p50.svg)",
        "",
        "![Loss45 live-process DEAD rate](charts/loss45-live-process-dead-rate-ci.svg)",
        "",
        "![Delay live-process DEAD rate](charts/delay-live-process-dead-rate-ci.svg)",
        "",
        "![Target pause live-process DEAD rate](charts/target-pause-live-process-dead-rate-ci.svg)",
        "",
        "![Target pause recovery rate](charts/target-pause-recovery-rate-ci.svg)",
        "",
        "## Crash 결과",
        "",
        "| Profile | 탐지/유효 | 탐지율 (Wilson 95% CI) | DEAD p50 (bootstrap 95% CI) | DEAD p95 (bootstrap 95% CI) | DEAD p99 | 미검출 |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]
    for row in crash:
        rate = row["crash_detection_success_rate"]
        rate_low = row["crash_detection_ci95_low"]
        rate_high = row["crash_detection_ci95_high"]
        p50 = row["dead_latency_p50_ms"]
        p50_low = row["dead_latency_p50_ci95_low_ms"]
        p50_high = row["dead_latency_p50_ci95_high_ms"]
        p95 = row["dead_latency_p95_ms"]
        p95_low = row["dead_latency_p95_ci95_low_ms"]
        p95_high = row["dead_latency_p95_ci95_high_ms"]
        p99 = row["dead_latency_p99_ms"]
        p50_text = (
            "N/A" if p50 is None else f"{p50:.2f} ms ({p50_low:.2f}–{p50_high:.2f})"
        )
        p95_text = (
            "N/A" if p95 is None else f"{p95:.2f} ms ({p95_low:.2f}–{p95_high:.2f})"
        )
        lines.append(
            f"| {row['profile']} | {row['dead_decisions']}/{row['runs']} | "
            f"{'N/A' if rate is None else f'{rate * 100:.1f}% ({rate_low * 100:.1f}–{rate_high * 100:.1f}%)'} | "
            f"{p50_text} | {p95_text} | "
            f"{'N/A' if p99 is None else f'{p99:.2f} ms'} | {row['dead_latency_missing']} |"
        )
    if not crash:
        lines.append("| N/A | 0/0 | 0 | N/A | N/A | N/A | N/A |")
    lines.extend(
        [
            "",
            "지연 신뢰구간은 profile별 10,000회 고정-seed 비모수 bootstrap percentile 구간이다. "
            "이는 표본 추출 불확실성을 나타내며 단일 호스트 환경의 체계적 편향까지 포함하지 않는다.",
            "",
            "### Crash observer overhead",
            "",
            "wire 값은 cluster 전체가 아니라 observer process의 application frame 송수신 합계다.",
            "",
            "| Profile | CPU peak p95 | Memory peak p95 | Sent frames p50 | Received frames p50 | Wire bytes p50 | Wire bytes p95 |",
            "|---|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for row in crash:
        lines.append(
            f"| {row['profile']} | {row['observer_peak_cpu_p95_percent']:.3f}% | "
            f"{row['observer_peak_memory_p95_percent']:.3f}% | "
            f"{row['observer_sent_frames_p50']:.0f} | "
            f"{row['observer_received_frames_p50']:.0f} | "
            f"{row['observer_wire_bytes_p50']:.0f} B | "
            f"{row['observer_wire_bytes_p95']:.0f} B |"
        )
    if not crash:
        lines.append("| N/A | N/A | N/A | N/A | N/A | N/A | N/A |")
    lines.extend(
        [
            "",
            "## 무장애 정상 상태 false positive",
            "",
            "`normal`은 fault marker와 장애 주입 없이 60초 전체를 관찰한다.",
            "",
            "| Profile | 유효/시도 | false SUSPECT | Wilson 95% CI | false DEAD | Wilson 95% CI |",
            "|---|---:|---:|---:|---:|---:|",
        ]
    )
    for row in normal:
        suspect_rate = row["normal_false_suspect_rate"] * 100.0
        suspect_low = row["normal_false_suspect_ci95_low"] * 100.0
        suspect_high = row["normal_false_suspect_ci95_high"] * 100.0
        dead_rate = row["normal_false_dead_rate"] * 100.0
        dead_low = row["normal_false_dead_ci95_low"] * 100.0
        dead_high = row["normal_false_dead_ci95_high"] * 100.0
        lines.append(
            f"| {row['profile']} | {row['runs']}/{row['attempted_runs']} | "
            f"{row['normal_false_suspect_runs']} ({suspect_rate:.1f}%) | "
            f"{suspect_low:.1f}–{suspect_high:.1f}% | "
            f"{row['normal_false_dead_runs']} ({dead_rate:.1f}%) | "
            f"{dead_low:.1f}–{dead_high:.1f}% |"
        )
    if not normal:
        lines.append("| N/A | 0/0 | N/A | N/A | N/A | N/A |")
    lines.extend(
        [
            "",
            "관측값이 0/30이어도 실제 확률이 정확히 0이라는 뜻은 아니다. "
            "profile별 95% Wilson 상한은 11.35%다.",
            "",
            "### 정상 상태 observer overhead",
            "",
            "wire 값은 cluster 전체가 아니라 observer process의 application frame 송수신 합계다.",
            "",
            "| Profile | CPU peak p50 | CPU peak p95 | Memory peak p95 | Sent frames p50 | Received frames p50 | Wire bytes p50 |",
            "|---|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for row in normal:
        lines.append(
            f"| {row['profile']} | {row['observer_peak_cpu_p50_percent']:.3f}% | "
            f"{row['observer_peak_cpu_p95_percent']:.3f}% | "
            f"{row['observer_peak_memory_p95_percent']:.3f}% | "
            f"{row['observer_sent_frames_p50']:.0f} | "
            f"{row['observer_received_frames_p50']:.0f} | "
            f"{row['observer_wire_bytes_p50']:.0f} B |"
        )
    if not normal:
        lines.append("| N/A | N/A | N/A | N/A | N/A | N/A | N/A |")
    lines.extend(
        [
            "",
            "## 유효 표본 범위",
            "",
            "| Profile | 유효/시도 | 관찰창 미달 |",
            "|---|---:|---:|",
        ]
    )
    for profile in profiles:
        selected = [run for run in runs if run["profile"] == profile]
        valid = sum(run["valid_observation_window"] for run in selected)
        lines.append(f"| {profile} | {valid}/{len(selected)} | {len(selected) - valid} |")
    lines.extend(
        [
            "",
            "## 살아 있는 process에 대한 DEAD 판단",
            "",
            "분자는 DEAD 판단 실행 수, 분모는 관찰창이 유효한 실행 수다.",
            "",
            "| Profile | loss15 | loss45 | target pause | observer pause |",
            "|---|---:|---:|---:|---:|",
        ]
    )
    for profile in profiles:
        cells = []
        for fault in ("loss15", "loss45", "cpu-pause-target", "cpu-pause-observer"):
            row = summary_by_key.get((profile, fault))
            cells.append(
                "N/A"
                if row is None
                else f"{row['dead_while_process_alive_runs']}/{row['runs']}"
            )
        lines.append(f"| {profile} | {' | '.join(cells)} |")
    if loss45:
        lines.extend(
            [
                "",
                "### Loss45 생존 process DEAD — 30-seed 통계",
                "",
                "target process는 종료되지 않았으므로 아래 DEAD는 crash 탐지 성공이 아니라 "
                "45% packet loss에 대한 강한 장애 판정이다.",
                "",
                "| Profile | DEAD/유효 (Wilson 95% CI) | DEAD p50 | DEAD p95 | ALIVE recovery/DEAD |",
                "|---|---:|---:|---:|---:|",
            ]
        )
        for row in loss45:
            rate = row["dead_while_process_alive_rate"]
            low = row["dead_while_process_alive_ci95_low"]
            high = row["dead_while_process_alive_ci95_high"]
            p50 = row["dead_latency_p50_ms"]
            p95 = row["dead_latency_p95_ms"]
            lines.append(
                f"| {row['profile']} | {row['dead_while_process_alive_runs']}/{row['runs']} "
                f"({rate * 100:.1f}%, {low * 100:.1f}–{high * 100:.1f}%) | "
                f"{'N/A' if p50 is None else f'{p50:.2f} ms'} | "
                f"{'N/A' if p95 is None else f'{p95:.2f} ms'} | "
                f"{row['recovery_observed_runs']}/{row['dead_while_process_alive_runs']} |"
            )
    if delay:
        lines.extend(
            [
                "",
                "### Delay 생존 process DEAD — 30-seed 통계",
                "",
                "target process는 종료되지 않았고 target network에 250±50ms netem delay를 "
                "적용했다. 아래 DEAD는 crash 탐지 성공이 아니라 지연에 대한 강한 장애 판정이다.",
                "",
                "| Profile | DEAD/유효 | Wilson 95% CI |",
                "|---|---:|---:|",
            ]
        )
        for row in delay:
            rate = row["dead_while_process_alive_rate"]
            low = row["dead_while_process_alive_ci95_low"]
            high = row["dead_while_process_alive_ci95_high"]
            lines.append(
                f"| {row['profile']} | {row['dead_while_process_alive_runs']}/{row['runs']} "
                f"({rate * 100:.1f}%) | {low * 100:.1f}–{high * 100:.1f}% |"
            )
    if target_pause:
        lines.extend(
            [
                "",
                "### Target pause 생존 process DEAD와 회복 — 30-seed 통계",
                "",
                "target process를 10초간 freezer pause했다. DEAD는 crash 탐지 성공이 아니라 "
                "일시 정지를 membership failure로 확정한 비율이며, recovery는 pause 종료 후 "
                "같은 incarnation이 ALIVE로 돌아온 비율이다.",
                "",
                "| Profile | DEAD/유효 (Wilson 95% CI) | DEAD p95 | ALIVE recovery/DEAD (Wilson 95% CI) | Recovery p95 |",
                "|---|---:|---:|---:|---:|",
            ]
        )
        for row in target_pause:
            dead_rate = row["dead_while_process_alive_rate"]
            dead_low = row["dead_while_process_alive_ci95_low"]
            dead_high = row["dead_while_process_alive_ci95_high"]
            recovery_rate = row["recovery_observed_rate_of_dead"]
            recovery_low = row["recovery_observed_ci95_low"]
            recovery_high = row["recovery_observed_ci95_high"]
            dead_p95 = row["dead_latency_p95_ms"]
            recovery_p95 = row["recovery_latency_p95_ms"]
            recovery_text = (
                "N/A"
                if recovery_rate is None
                else f"{row['recovery_observed_runs']}/{row['dead_while_process_alive_runs']} "
                f"({recovery_rate * 100:.1f}%, {recovery_low * 100:.1f}–{recovery_high * 100:.1f}%)"
            )
            lines.append(
                f"| {row['profile']} | {row['dead_while_process_alive_runs']}/{row['runs']} "
                f"({dead_rate * 100:.1f}%, {dead_low * 100:.1f}–{dead_high * 100:.1f}%) | "
                f"{'N/A' if dead_p95 is None else f'{dead_p95:.2f} ms'} | "
                f"{recovery_text} | "
                f"{'N/A' if recovery_p95 is None else f'{recovery_p95:.2f} ms'} |"
            )
    lines.extend(
        [
            "",
            "## 해석 규칙",
            "",
            "- Crash가 아닌 시나리오의 `dead_decision_rate`는 탐지 성공률이 아니다.",
            "- pause·throttle 중 DEAD는 프로세스 생존 상태에서의 강한 장애 판정으로 별도 집계한다.",
            "- 감지 실패 실행은 지연 분위수에서 제외되므로 성공률과 `dead_latency_missing`을 함께 본다.",
            "- detector별 최소 사후 관찰창을 충족하지 못한 실행은 성공률과 지연 통계에서 제외한다.",
            "- 상세 수치는 `runs.csv`와 `summary.csv`를 사용한다.",
            "",
        ]
    )
    path.write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, action="append", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    runs = []
    for input_dir in args.input:
        runs.extend(load_runs(input_dir))
    runs = deduplicate_runs(runs)
    if not runs:
        raise SystemExit(f"no real UDP result.json files under {args.input}")
    args.output.mkdir(parents=True, exist_ok=True)
    charts = args.output / "charts"
    charts.mkdir(exist_ok=True)
    run_fields = list(runs[0].keys())
    write_csv(args.output / "runs.csv", runs, run_fields)
    summaries = summarize(runs)
    summary_fields = list(summaries[0].keys())
    write_csv(args.output / "summary.csv", summaries, summary_fields)
    crash_rows = [
        (row["profile"], row["dead_latency_p95_ms"])
        for row in summaries
        if row["fault"] == "crash" and row["dead_latency_p95_ms"] is not None
    ]
    bar_chart(
        charts / "crash-dead-latency-p95.svg",
        "Crash → DEAD p95 (성공 실행)",
        crash_rows,
        " ms",
    )
    crash_p95_interval_rows = [
        (
            row["profile"],
            row["dead_latency_p95_ms"],
            row["dead_latency_p95_ci95_low_ms"],
            row["dead_latency_p95_ci95_high_ms"],
        )
        for row in summaries
        if row["fault"] == "crash"
    ]
    interval_chart(
        charts / "crash-dead-latency-p95-ci.svg",
        "Crash → DEAD p95와 bootstrap 95% CI",
        crash_p95_interval_rows,
        " ms",
    )
    crash_observer_cpu_rows = [
        (row["profile"], row["observer_peak_cpu_p95_percent"])
        for row in summaries
        if row["fault"] == "crash"
        and row["observer_peak_cpu_p95_percent"] is not None
    ]
    bar_chart(
        charts / "crash-observer-cpu-p95.svg",
        "Crash observer peak CPU p95",
        crash_observer_cpu_rows,
        "%",
    )
    crash_observer_wire_rows = [
        (row["profile"], row["observer_wire_bytes_p50"])
        for row in summaries
        if row["fault"] == "crash" and row["observer_wire_bytes_p50"] is not None
    ]
    bar_chart(
        charts / "crash-observer-wire-bytes-p50.svg",
        "Crash observer application wire bytes p50",
        crash_observer_wire_rows,
        " B",
    )
    crash_paired = [
        (run["profile"], run["suspect_latency_ms"], run["dead_latency_ms"])
        for run in runs
        if run["fault"] == "crash" and run["valid_observation_window"]
    ]
    paired_bar_chart(
        charts / "crash-suspect-vs-dead.svg",
        "Crash → SUSPECT / DEAD (실행별)",
        crash_paired,
        "SUSPECT",
        "DEAD",
        " ms",
    )
    observation_rows = [
        (
            run["profile"],
            run["post_fault_observation_ms"],
            run["required_observation_ms"],
        )
        for run in runs
        if run["fault"] == "crash"
    ]
    paired_bar_chart(
        charts / "post-fault-observation-window.svg",
        "Fault 이후 실제 관찰창 / 최소 요구 관찰창",
        observation_rows,
        "실제 관찰",
        "최소 요구",
        " ms",
    )
    fault_dead_rows = [
        (f"{row['profile']} / {row['fault']}", row["dead_latency_p95_ms"])
        for row in summaries
        if row["dead_latency_p95_ms"] is not None
    ]
    bar_chart(
        charts / "fault-dead-latency-p95.svg",
        "Fault start → DEAD p95 (성공 실행)",
        fault_dead_rows,
        " ms",
    )
    recovery_rows = [
        (f"{row['profile']} / {row['fault']}", row["recovery_latency_p95_ms"])
        for row in summaries
        if row["recovery_latency_p95_ms"] is not None
    ]
    bar_chart(
        charts / "recovery-latency-p95.svg",
        "Fault end marker → ALIVE p95",
        recovery_rows,
        " ms",
    )
    cpu_rows = [
        (f"{row['profile']} / {row['fault']}", row["target_peak_cpu_p95_percent"])
        for row in summaries
        if row["fault"].startswith("cpu-") or row["fault"].startswith("io-")
    ]
    bar_chart(
        charts / "target-peak-cpu-p95.svg",
        "Target container peak CPU p95",
        cpu_rows,
        "%",
    )
    throttled_rows = [
        (f"{row['profile']} / {row['fault']}", row["target_cpu_throttled_p95_ms"])
        for row in summaries
        if row["fault"].startswith("cpu-") or row["fault"].startswith("io-")
    ]
    bar_chart(
        charts / "target-cpu-throttled-p95.svg",
        "Target cgroup CPU throttled time p95",
        throttled_rows,
        " ms",
    )
    io_rows = [
        (f"{row['profile']} / {row['fault']}", row["target_io_write_p95_bytes"] / 1_000_000.0)
        for row in summaries
        if row["fault"].startswith("io-")
        and row["target_io_write_p95_bytes"] is not None
    ]
    bar_chart(
        charts / "target-io-write-p95.svg",
        "Target cgroup bytes written p95",
        io_rows,
        " MB",
    )
    normal_false_positive_rows = []
    for row in summaries:
        if row["fault"] != "normal" or not row["runs"]:
            continue
        normal_false_positive_rows.extend(
            [
                (
                    f"{row['profile']} / SUSPECT",
                    row["normal_false_suspect_runs"] / row["runs"] * 100.0,
                ),
                (
                    f"{row['profile']} / DEAD",
                    row["normal_false_dead_runs"] / row["runs"] * 100.0,
                ),
            ]
        )
    bar_chart(
        charts / "normal-false-positive-runs.svg",
        "Normal 60s false-positive run rate",
        normal_false_positive_rows,
        "%",
    )
    normal_observer_cpu_rows = [
        (row["profile"], row["observer_peak_cpu_p95_percent"])
        for row in summaries
        if row["fault"] == "normal"
        and row["observer_peak_cpu_p95_percent"] is not None
    ]
    bar_chart(
        charts / "normal-observer-cpu-p95.svg",
        "Normal observer peak CPU p95",
        normal_observer_cpu_rows,
        "%",
    )
    normal_observer_wire_rows = [
        (row["profile"], row["observer_wire_bytes_p50"])
        for row in summaries
        if row["fault"] == "normal" and row["observer_wire_bytes_p50"] is not None
    ]
    bar_chart(
        charts / "normal-observer-wire-bytes-p50.svg",
        "Normal observer application wire bytes p50 (60s)",
        normal_observer_wire_rows,
        " B",
    )
    loss45_alive_dead_rows = [
        (
            row["profile"],
            row["dead_while_process_alive_rate"] * 100.0,
            row["dead_while_process_alive_ci95_low"] * 100.0,
            row["dead_while_process_alive_ci95_high"] * 100.0,
        )
        for row in summaries
        if row["fault"] == "loss45" and row["runs"]
    ]
    interval_chart(
        charts / "loss45-live-process-dead-rate-ci.svg",
        "Loss45: live-process DEAD rate와 Wilson 95% CI",
        loss45_alive_dead_rows,
        "%",
    )
    delay_alive_dead_rows = [
        (
            row["profile"],
            row["dead_while_process_alive_rate"] * 100.0,
            row["dead_while_process_alive_ci95_low"] * 100.0,
            row["dead_while_process_alive_ci95_high"] * 100.0,
        )
        for row in summaries
        if row["fault"] == "delay" and row["runs"]
    ]
    interval_chart(
        charts / "delay-live-process-dead-rate-ci.svg",
        "Delay 250±50ms: live-process DEAD rate와 Wilson 95% CI",
        delay_alive_dead_rows,
        "%",
    )
    target_pause_dead_rows = [
        (
            row["profile"],
            row["dead_while_process_alive_rate"] * 100.0,
            row["dead_while_process_alive_ci95_low"] * 100.0,
            row["dead_while_process_alive_ci95_high"] * 100.0,
        )
        for row in summaries
        if row["fault"] == "cpu-pause-target" and row["runs"]
    ]
    interval_chart(
        charts / "target-pause-live-process-dead-rate-ci.svg",
        "Target pause: live-process DEAD rate와 Wilson 95% CI",
        target_pause_dead_rows,
        "%",
    )
    target_pause_recovery_rows = [
        (
            row["profile"],
            row["recovery_observed_rate_of_dead"] * 100.0,
            row["recovery_observed_ci95_low"] * 100.0,
            row["recovery_observed_ci95_high"] * 100.0,
        )
        for row in summaries
        if row["fault"] == "cpu-pause-target"
        and row["recovery_observed_rate_of_dead"] is not None
    ]
    interval_chart(
        charts / "target-pause-recovery-rate-ci.svg",
        "Target pause: DEAD 이후 ALIVE recovery rate와 Wilson 95% CI",
        target_pause_recovery_rows,
        "%",
    )
    heatmap(charts / "dead-decision-rate.svg", summaries)
    report(args.output / "benchmark-report.md", runs, summaries)
    print(f"analyzed {len(runs)} runs into {args.output}")


if __name__ == "__main__":
    main()
