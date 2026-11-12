#!/usr/bin/env python3
"""Reject structurally successful runs that lack real detector evidence."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


PROFILE_IDS = {
    "product-swim-lifeguard": "swim-lifeguard",
    "product-redis-cluster": "redis-cluster",
    "product-phi-gossip": "phi-gossip",
    "product-ignite-tcp-ring": "ignite-tcp-ring",
    "product-aerospike-mesh": "aerospike-mesh",
}
TCP_PEER_COUNTS = {
    "product-ignite-tcp-ring": 1,
    "product-aerospike-mesh": 4,
}
TRANSIENT_NETWORK_FAULTS = {"loss15", "loss45", "delay"}
TARGET = "node-1"


def event_records(path: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.startswith("EVENT_JSON="):
            records.append(json.loads(line.removeprefix("EVENT_JSON=")))
    return records


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--result", type=Path, required=True)
    parser.add_argument("--events", type=Path, required=True)
    parser.add_argument("--profile", choices=sorted(PROFILE_IDS), required=True)
    parser.add_argument("--fault", required=True)
    args = parser.parse_args()

    result = json.loads(args.result.read_text(encoding="utf-8"))
    events = event_records(args.events)
    fault_events = [event for event in events if event.get("kind") == "fault-start"]
    detector_events = [event for event in events if event.get("kind") == "detector"]
    target_events = [
        event for event in detector_events if event.get("target") == TARGET
    ]
    transport_events = [
        event for event in events if event.get("kind") == "transport-ready"
    ]

    fault_at = result.get("faultAtNanos")
    fault_end = result.get("faultEndAtNanos")

    def first_target_at_or_after(status: str, threshold: Any) -> Any:
        if threshold is None:
            return None
        return next((
            int(event["atNanos"])
            for event in target_events
            if event.get("newStatus") == status
            and int(event["atNanos"]) >= int(threshold)
        ), None)

    def latency(start: Any, end: Any) -> int | None:
        if start is None or end is None or int(end) < int(start):
            return None
        return int(end) - int(start)

    target_state = "ALIVE"
    transition_chain_valid = True
    for event in target_events:
        old_status = event.get("oldStatus")
        new_status = event.get("newStatus")
        if old_status != target_state or new_status == old_status:
            transition_chain_valid = False
            break
        target_state = new_status

    pre_fault_suspect = sum(
        event.get("newStatus") == "SUSPECT"
        and (fault_at is None or int(event["atNanos"]) < int(fault_at))
        for event in target_events
    )
    pre_fault_dead = sum(
        event.get("newStatus") == "DEAD"
        and (fault_at is None or int(event["atNanos"]) < int(fault_at))
        for event in target_events
    )
    suspect_at = first_target_at_or_after("SUSPECT", fault_at)
    dead_at = first_target_at_or_after("DEAD", fault_at)
    recovered_at = first_target_at_or_after("ALIVE", fault_end)

    checks: dict[str, bool] = {
        "profile_matches": result.get("profile") == PROFILE_IDS[args.profile],
        "observer_node": result.get("nodeId") == 0,
        "result_target_matches": result.get("target") == TARGET,
        "detector_events_attributed": all(
            isinstance(event.get("target"), str) and bool(event["target"])
            for event in detector_events
        ),
        "target_transition_chain": transition_chain_valid,
        "final_target_status_matches": result.get("finalTargetStatus") == target_state,
        "prefault_metrics_match": (
            result.get("preFaultSuspectEvents") == pre_fault_suspect
            and result.get("preFaultDeadEvents") == pre_fault_dead
        ),
        "suspect_metric_matches_target": (
            result.get("suspectLatencyNanos") == latency(fault_at, suspect_at)
        ),
        "dead_metric_matches_target": (
            result.get("deadLatencyNanos") == latency(fault_at, dead_at)
        ),
        "recovery_metric_matches_target": (
            result.get("recoveredAtNanos") == recovered_at
            and result.get("recoveryLatencyNanos") == latency(fault_end, recovered_at)
        ),
        "real_wire_source": result.get("wireSource") in {
            "real-udp-observed",
            "real-tcp-heartbeat-and-udp-control-observed",
        },
        "received_real_frames": int(result.get("receivedFrames", 0)) > 0,
        "fault_targets_expected_node": all(
            event.get("target") == TARGET for event in fault_events
        ),
        "fault_marker_contract": (
            result.get("faultAtNanos") is None and not fault_events
            if args.fault == "normal"
            else result.get("faultAtNanos") is not None and len(fault_events) == 1
        ),
        "fault_end_contract": (
            result.get("faultEndAtNanos") is None
            if args.fault in {"normal", "crash"}
            else result.get("faultEndAtNanos") is not None
        ),
    }

    outcomes: dict[str, bool] = {
        "no_prefault_suspect": result.get("preFaultSuspectEvents") == 0,
        "no_prefault_dead": result.get("preFaultDeadEvents") == 0,
        "crash_detected": args.fault != "crash"
        or (
            result.get("deadLatencyNanos") is not None
            and result.get("finalTargetStatus") == "DEAD"
        ),
        "transient_network_not_convicted": args.fault not in TRANSIENT_NETWORK_FAULTS
        or (
            result.get("deadLatencyNanos") is None
            and result.get("finalTargetStatus") == "ALIVE"
        ),
        "normal_remains_alive": args.fault != "normal"
        or result.get("finalTargetStatus") == "ALIVE",
    }

    if args.profile in TCP_PEER_COUNTS:
        expected_peers = TCP_PEER_COUNTS[args.profile]
        ready_before_fault = len(transport_events) == 1
        if ready_before_fault and fault_events:
            ready_before_fault = (
                int(transport_events[0]["atNanos"]) < int(fault_events[0]["atNanos"])
            )
        checks.update({
            "tcp_wire_source": result.get("wireSource")
            == "real-tcp-heartbeat-and-udp-control-observed",
            "tcp_transport_ready_once": len(transport_events) == 1,
            "tcp_expected_peer_count": len(transport_events) == 1
            and transport_events[0].get("peerCount") == expected_peers,
            "tcp_ready_before_fault": ready_before_fault,
            "tcp_bidirectional_frames": int(result.get("sentFrames", 0)) > 0
            and int(result.get("receivedFrames", 0)) > 0,
        })
    else:
        checks["no_unexpected_tcp_ready"] = not transport_events

    output = {
        "status": "passed" if all(checks.values()) else "failed",
        "profile": args.profile,
        "fault": args.fault,
        "checks": checks,
        "outcomes": outcomes,
    }
    print(json.dumps(output, indent=2))
    if output["status"] != "passed":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
