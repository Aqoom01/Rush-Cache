#!/usr/bin/env python3
"""Validate real queue pressure on the detector's local NVMe instance store."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any


def numeric(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def integer(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--fio-exit-code", type=int, required=True)
    parser.add_argument("--state-path", type=Path, required=True)
    parser.add_argument("--fio-path", type=Path, required=True)
    parser.add_argument("--storage-device", type=Path, required=True)
    parser.add_argument("--mounted-device", type=Path, required=True)
    parser.add_argument("--storage-model", required=True)
    parser.add_argument("--expected-storage-model", required=True)
    parser.add_argument("--expected-engine", required=True)
    parser.add_argument("--expected-iodepth", type=int, required=True)
    parser.add_argument("--min-iops", type=float, required=True)
    parser.add_argument("--min-deep-queue-percent", type=float, required=True)
    parser.add_argument("--min-runtime-ms", type=int, required=True)
    parser.add_argument("--wall-duration-ms", type=int, required=True)
    parser.add_argument("--max-wall-duration-ms", type=int, required=True)
    args = parser.parse_args()

    errors: list[str] = []
    document: dict[str, Any] = {}
    try:
        document = json.loads(args.input.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        errors.append(f"fio output is unreadable: {error}")

    jobs = document.get("jobs", []) if isinstance(document, dict) else []
    job = jobs[0] if isinstance(jobs, list) and len(jobs) == 1 else {}
    if not job:
        errors.append("fio output must contain exactly one job")

    options = job.get("job options", {}) if isinstance(job, dict) else {}
    read = job.get("read", {}) if isinstance(job, dict) else {}
    write = job.get("write", {}) if isinstance(job, dict) else {}
    depth = job.get("iodepth_level", {}) if isinstance(job, dict) else {}

    engine = str(options.get("ioengine", ""))
    configured_iodepth = integer(options.get("iodepth"))
    direct = integer(options.get("direct"))
    total_iops = numeric(read.get("iops")) + numeric(write.get("iops"))
    deep_queue_percent = sum(numeric(depth.get(key)) for key in ("16", "32", ">=64"))
    runtime_ms = integer(job.get("job_runtime"))
    fio_job_error = integer(job.get("error"), default=1)

    same_filesystem = False
    try:
        same_filesystem = os.stat(args.state_path).st_dev == os.stat(args.fio_path).st_dev
    except OSError as error:
        errors.append(f"cannot compare state and fio filesystems: {error}")

    storage_device_matches_mount = (
        os.path.realpath(args.storage_device) == os.path.realpath(args.mounted_device)
    )
    storage_device_is_block = args.storage_device.exists() and args.storage_device.is_block_device()

    checks = {
        "fio_exit_zero": args.fio_exit_code == 0,
        "fio_job_error_zero": fio_job_error == 0,
        "single_job": bool(job),
        "async_engine": engine == args.expected_engine,
        "direct_io": direct == 1,
        "configured_iodepth": configured_iodepth == args.expected_iodepth,
        "deep_queue_achieved": deep_queue_percent >= args.min_deep_queue_percent,
        "minimum_iops_achieved": total_iops >= args.min_iops,
        "minimum_runtime_achieved": runtime_ms >= args.min_runtime_ms,
        "wall_duration_within_budget": args.wall_duration_ms
        <= args.max_wall_duration_ms,
        "detector_state_and_fio_share_filesystem": same_filesystem,
        "state_mount_uses_selected_storage_device": storage_device_matches_mount,
        "storage_device_is_block_device": storage_device_is_block,
        "storage_model_is_ec2_instance_store": args.expected_storage_model in args.storage_model,
    }
    result = {
        "status": "passed" if not errors and all(checks.values()) else "failed",
        "claim": "shared-local-nvme-instance-store-detector-state-contention",
        "checks": checks,
        "metrics": {
            "engine": engine,
            "configuredIodepth": configured_iodepth,
            "deepQueuePercent": deep_queue_percent,
            "totalIops": total_iops,
            "runtimeMs": runtime_ms,
            "wallDurationMs": args.wall_duration_ms,
            "fioJobError": fio_job_error,
            "storageDevice": str(args.storage_device),
            "mountedDevice": str(args.mounted_device),
            "storageModel": args.storage_model,
        },
        "thresholds": {
            "minIops": args.min_iops,
            "minDeepQueuePercent": args.min_deep_queue_percent,
            "minRuntimeMs": args.min_runtime_ms,
            "maxWallDurationMs": args.max_wall_duration_ms,
        },
        "statePath": str(args.state_path),
        "fioPath": str(args.fio_path),
        "errors": errors,
    }
    args.output.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result, indent=2))
    if result["status"] != "passed":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
