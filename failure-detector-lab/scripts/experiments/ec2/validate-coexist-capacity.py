#!/usr/bin/env python3
"""Validate that a Terraform EC2 plan fits the regional On-Demand Standard vCPU quota."""

from __future__ import annotations

import argparse
import json
import re
import subprocess
from collections import Counter
from pathlib import Path
from typing import Any


STANDARD_FAMILY = re.compile(r"^[acdhimrtz]\d", re.IGNORECASE)
STANDARD_ON_DEMAND_QUOTA = "L-1216C47A"


def command_json(command: list[str]) -> Any:
    completed = subprocess.run(command, check=True, capture_output=True, text=True)
    return json.loads(completed.stdout)


def aws_command(args: argparse.Namespace, *parts: str) -> list[str]:
    command = [args.aws_bin, "--region", args.region]
    if args.profile:
        command.extend(["--profile", args.profile])
    command.extend(parts)
    return command


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--plan", type=Path, required=True)
    parser.add_argument("--terraform-bin", required=True)
    parser.add_argument("--aws-bin", required=True)
    parser.add_argument("--region", required=True)
    parser.add_argument("--profile", default="")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--minimum-remaining-vcpus", type=float, default=1.0)
    args = parser.parse_args()

    plan = command_json([args.terraform_bin, "show", "-json", str(args.plan)])
    managed_changes = [
        change
        for change in plan.get("resource_changes", [])
        if change.get("mode") == "managed"
    ]
    destructive = [
        change.get("address", "unknown")
        for change in managed_changes
        if "delete" in change.get("change", {}).get("actions", [])
    ]
    updates = [
        change.get("address", "unknown")
        for change in managed_changes
        if "update" in change.get("change", {}).get("actions", [])
    ]
    planned_instances = [
        change
        for change in managed_changes
        if change.get("type") == "aws_instance"
        and change.get("change", {}).get("actions") == ["create"]
    ]
    planned_type_counts = Counter(
        str(change.get("change", {}).get("after", {}).get("instance_type", ""))
        for change in planned_instances
    )

    current = command_json(
        aws_command(
            args,
            "ec2",
            "describe-instances",
            "--filters",
            "Name=instance-state-name,Values=pending,running",
        )
    )
    current_instances = [
        instance
        for reservation in current.get("Reservations", [])
        for instance in reservation.get("Instances", [])
        if instance.get("InstanceLifecycle", "on-demand") == "on-demand"
        and STANDARD_FAMILY.match(str(instance.get("InstanceType", "")))
    ]
    current_type_counts = Counter(
        str(instance["InstanceType"]) for instance in current_instances
    )

    all_types = sorted(set(current_type_counts) | set(planned_type_counts))
    instance_types: dict[str, Any] = {}
    if all_types:
        described = command_json(
            aws_command(
                args,
                "ec2",
                "describe-instance-types",
                "--instance-types",
                *all_types,
            )
        )
        instance_types = {
            item["InstanceType"]: item for item in described.get("InstanceTypes", [])
        }

    quota_document = command_json(
        aws_command(
            args,
            "service-quotas",
            "get-service-quota",
            "--service-code",
            "ec2",
            "--quota-code",
            STANDARD_ON_DEMAND_QUOTA,
        )
    )
    quota_vcpus = float(quota_document["Quota"]["Value"])

    unknown_types = sorted(set(all_types) - set(instance_types))
    nonstandard_planned = sorted(
        instance_type
        for instance_type in planned_type_counts
        if not STANDARD_FAMILY.match(instance_type)
    )
    current_vcpus = sum(
        count * int(instance_types[instance_type]["VCpuInfo"]["DefaultVCpus"])
        for instance_type, count in current_type_counts.items()
        if instance_type in instance_types
    )
    planned_vcpus = sum(
        count * int(instance_types[instance_type]["VCpuInfo"]["DefaultVCpus"])
        for instance_type, count in planned_type_counts.items()
        if instance_type in instance_types
    )
    projected_vcpus = current_vcpus + planned_vcpus
    remaining_vcpus = quota_vcpus - projected_vcpus

    checks = {
        "plan_has_instances_to_create": len(planned_instances) > 0,
        "no_destructive_changes": not destructive,
        "no_in_place_updates": not updates,
        "planned_instances_use_standard_quota": not nonstandard_planned,
        "all_instance_types_resolved": not unknown_types,
        "minimum_vcpu_headroom_preserved": remaining_vcpus >= args.minimum_remaining_vcpus,
    }
    result = {
        "status": "passed" if all(checks.values()) else "failed",
        "checks": checks,
        "region": args.region,
        "quotaCode": STANDARD_ON_DEMAND_QUOTA,
        "quotaVcpus": quota_vcpus,
        "currentOnDemandStandard": {
            "instances": sum(current_type_counts.values()),
            "vcpus": current_vcpus,
            "types": dict(sorted(current_type_counts.items())),
        },
        "plannedOnDemandStandard": {
            "instances": len(planned_instances),
            "vcpus": planned_vcpus,
            "types": dict(sorted(planned_type_counts.items())),
        },
        "projectedVcpus": projected_vcpus,
        "remainingVcpus": remaining_vcpus,
        "minimumRemainingVcpus": args.minimum_remaining_vcpus,
        "destructiveChanges": destructive,
        "inPlaceUpdates": updates,
        "unknownInstanceTypes": unknown_types,
        "nonstandardPlannedTypes": nonstandard_planned,
    }
    args.output.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result, indent=2))
    if result["status"] != "passed":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
