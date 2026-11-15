#!/usr/bin/env python3
"""A1/A2 실제 통합 실행: 입력 생성·8 JVM 로컬 리허설·전용 EC2 SSH 실행. 기본 EC2 명령은 dry-run이다."""
import argparse
import concurrent.futures
import csv
import hashlib
import itertools
import json
import os
import random
import re
import shlex
import shutil
import signal
import socket
import struct
import subprocess
import sys
import threading
import time
import urllib.request
import uuid
import zipfile
from pathlib import Path, PurePosixPath

PROTOCOLS = ("PB", "PB_FENCED", "RAFT", "VR", "CHAIN", "CRAQ", "ROSE", "O00", "V01", "V02", "V03", "V04", "V05", "V06", "V07")
REPLICA = "io.openkache.server.integration.IntegrationReplicaMain"
GATEWAY = "io.openkache.server.integration.IntegrationGatewayMain"
LOAD = "io.openkache.runner.IntegrationLoadMain"
A1_PROTOCOLS = ("PB", "PB_FENCED", "RAFT", "VR", "CHAIN", "CRAQ", "ROSE")
A2_PROTOCOLS = tuple(p for p in PROTOCOLS if p not in A1_PROTOCOLS)
DISTRIBUTIONS = ("RANGE", "SLOT", "RING", "RING_ALLOC", "TAG", "RESIZABLE", "ORDER")
PLACEMENTS = ("NONE", "GREEDY", "HYSTERESIS", "COST", "ESTORE", "P2C", "SLICER", "RENDEZVOUS", "FIXED_LPT")
SCALERS = ("fixed", "scads", "elastman", "estore", "accordion", "pstore", "tiramola", "pax", "aws-warmup")
SCHEDULERS = ("fifo", "mongodb", "tidb-token", "tidb-feedback", "iter-gwin", "single-mcs", "slamig", "vmbuddies", "fpta")
DRAINS = ("preflight-all", "incremental-drain")
LEGACY_FDS = ("FIXED_TIMEOUT", "SWIM", "SWIM_LIFEGUARD")
LAB_FDS = ("F0", "F1", "F2", "F3", "F4", "F5", "F6", "F7A", "F7B", "F7C", "REDIS_CLUSTER", "PHI_GOSSIP", "IGNITE_TCP_RING", "AEROSPIKE_MESH")
FAILURE_METHODS = LEGACY_FDS + LAB_FDS
FD_ALIASES = {"FIXED_DEADLINE":"F0", "SWIM_BASE":"F2", "LAB_SWIM_LIFEGUARD":"F3", "PRODUCT_SWIM_LIFEGUARD":"F3", "ADAPTIVE_DEADLINE":"F4", "FIXED_DEADLINE_RING":"F5", "FIXED_DEADLINE_RING_VERIFY":"F6", "SWIM_LHA_PROBE":"F7A", "SWIM_LHA_SUSPICION":"F7B", "SWIM_BUDDY":"F7C"}
READ_SELECTIONS = ("ADAPTIVE", "ROUND_ROBIN")
OPERATING_FAULTS = ("SLOW_REPLICA", "NETWORK_DEGRADED", "CRASH_LEADER")
CRASH_SCENARIOS = ("CRASH_MOVE", "CRASH_SCALE", "CRASH_LEADER")
A2_SETTINGS = {"read.logical.limit":128, "read.rpc.limit.per.replica":256, "read.rtt.initial.ms":5, "read.sample.ttl.ms":1000, "read.failure.cooldown.ms":200, "read.explore.every":16, "meta.batch.bytes":65536, "meta.pending.limit":1024, "meta.inflight.limit":32, "storage.shards":2}
FD_KEYS = {"fd.heartbeat.ms", "fd.deadline.ms", "fd.confirm.ms", "fd.probe.timeout.ms", "fd.phi.threshold", "fd.gossip.fanout", "fd.ring.successors", "fd.verify.failures", "fd.verify.helpers", "fd.verify.quorum", "fd.coordinator.index", "fd.native.watchdog.ms", "fd.repair.timeout.ms", "fd.report.validity.ms"}


def canonical_fd(value):
    value=value.upper().replace("-","_")
    value=FD_ALIASES.get(value,value)
    check(value in FAILURE_METHODS, "unknown detector: "+value)
    return value


def fd_defaults(method):
    if method in LEGACY_FDS: return {}
    interval=1000 if method in ("REDIS_CLUSTER","PHI_GOSSIP","IGNITE_TCP_RING") else 150 if method=="AEROSPIKE_MESH" else 200
    deadline={"REDIS_CLUSTER":15000,"PHI_GOSSIP":3000,"IGNITE_TCP_RING":5000,"AEROSPIKE_MESH":1500}.get(method,600)
    return {"fd.heartbeat.ms":interval,"fd.deadline.ms":deadline,"fd.confirm.ms":400,"fd.probe.timeout.ms":100,"fd.phi.threshold":8,
            "fd.gossip.fanout":1,"fd.ring.successors":2,"fd.verify.failures":2,"fd.verify.helpers":1,"fd.verify.quorum":1,"fd.coordinator.index":2,
            "fd.native.watchdog.ms":300000,"fd.repair.timeout.ms":60000,"fd.report.validity.ms":deadline*2}
PREFLIGHT = "io.openkache.server.integration.IntegrationConfig"
LOCAL_PROCESSES = {}


def check(condition, text):
    if not condition:
        raise ValueError(text)


def sha(path):
    h = hashlib.sha256()
    with Path(path).open("rb") as f:
        for b in iter(lambda: f.read(65536), b""):
            h.update(b)
    return h.hexdigest()


def save(path, value):
    Path(path).write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def props(path, values):
    Path(path).write_text("\n".join(f"{k}={v}" for k, v in values.items()) + "\n", encoding="utf-8")


def load_props(path):
    return dict(line.strip().split("=", 1) for line in Path(path).read_text(encoding="utf-8").splitlines() if line.strip() and not line.startswith("#"))


def verify_bundle(bundle):
    bundle = Path(bundle).resolve()
    seen = set()
    for line in (bundle / "BUNDLE_SHA256SUMS").read_text().splitlines():
        digest, name = line.split("  ", 1)
        check(name.startswith("lib/") and ".." not in Path(name).parts and name not in seen, "invalid bundle manifest")
        check(sha(bundle / name) == digest, f"bundle hash mismatch: {name}")
        seen.add(name)
    check({p.relative_to(bundle).as_posix() for p in (bundle / "lib").glob("*.jar")} == seen, "extra/missing JAR")
    check(any("experiment-runner" in name for name in seen), "Runner JAR missing")
    receipt = bundle / "TOOLS_SHA256SUMS"
    if receipt.exists():
        tools_seen = set()
        for line in receipt.read_text().splitlines():
            digest, name = line.split("  ", 1)
            check(name.startswith("tools/") and ".." not in Path(name).parts, "invalid tools manifest")
            check(sha(bundle / name) == digest, "tool hash mismatch: " + name)
            tools_seen.add(name)
        actual = {p.relative_to(bundle).as_posix() for p in (bundle / "tools").rglob("*") if p.is_file() and "__pycache__" not in p.parts}
        check(actual == tools_seen, "extra/missing bundle tools")
    return hashlib.sha256((bundle / "BUNDLE_SHA256SUMS").read_bytes() + (receipt.read_bytes() if receipt.exists() else b"")).hexdigest()


def endpoint(host, port, opcode, body=b"", timeout=10):
    check(len(body) < 2 * 1024 * 1024 - 28, "control frame too big")
    request = 1
    header = struct.pack(">HBBBBHQQ", 0x4F4B, 1, 3, opcode, 0, 0, request, 0)
    with socket.create_connection((host, port), timeout=timeout) as s:
        s.settimeout(timeout)
        s.sendall(struct.pack(">I", len(header) + len(body)) + header + body)
        def exact(n):
            out = bytearray()
            while len(out) < n:
                b = s.recv(n - len(out))
                if not b:
                    raise EOFError("control EOF")
                out.extend(b)
            return bytes(out)
        size = struct.unpack(">I", exact(4))[0]
        check(48 <= size <= 2 * 1024 * 1024, "control response length")
        data = exact(size)
    magic, version, kind, op, flags, status, response, routing = struct.unpack(">HBBBBHQQ", data[:24])
    check((magic, version, kind, op, flags, response) == (0x4F4B, 1, 2, opcode, 0, request), "control reply mismatch")
    pos = 40
    detail = struct.unpack_from(">I", data, pos)[0]
    pos += 4 + detail
    check(pos + 4 <= len(data), "control detail length")
    n = struct.unpack_from(">I", data, pos)[0]
    pos += 4
    check(pos + n == len(data), "control value length")
    check(status == 0, f"control status={status}")
    return json.loads(data[pos:pos+n])


def prepare(args):
    args.fd = canonical_fd(args.fd)
    rate_after = getattr(args, "rate_after", None)
    rate_change_at = getattr(args, "rate_change_at", None)
    check((rate_after is None and rate_change_at is None)
          or (rate_after is not None and rate_change_at is not None
              and 1 <= rate_after <= 100000 and 1 <= rate_change_at < args.requests),
          "rate transition requires --rate-after and --rate-change-at inside measurement")
    fault_seconds = getattr(args, "fault_seconds", 8)
    check(0 < fault_seconds <= 30, "fault duration must be in (0, 30] seconds")
    operating_case = getattr(args, "operating_case", None)
    strategy_number = getattr(args, "strategy_number", None)
    if operating_case:
        from operating_suite import SCENARIOS
        scenario, workload, groups = SCENARIOS[operating_case]
        check((args.scenario, args.workload, args.initial_groups) == (scenario, workload, groups)
              and (operating_case in ("scale-out", "scale-in")) == (rate_after is not None)
              and strategy_number and strategy_number > 0, "operating case does not match the workload or strategy")
    else:
        check(strategy_number is None, "strategy number requires an operating case")
    output = Path(args.output).resolve()
    output.mkdir(parents=True, exist_ok=False)
    topology = json.loads(Path(args.topology).read_text()) if args.topology else {
        "replica_hosts": [f"127.0.0.{i}" for i in range(11, 11 + 3 * (args.groups or 3))], "gateway_host": "127.0.0.1", "load_host": "127.0.0.1",
        "ssh_user": "ubuntu", "remote_root": "/home/ubuntu/openkache-integration", "environment": "local-shared-host"}
    hosts = topology["replica_hosts"]; groups = len(hosts) // 3
    check(len(hosts) % 3 == 0 and 2 <= groups <= 8 and len(set(hosts)) == len(hosts), "2..8 groups of three distinct replica hosts required")
    check(args.groups is None or args.groups == groups, "topology and --groups disagree")
    check(1 <= args.min_groups <= args.initial_groups <= groups, "invalid active/minimum/prepared group counts")
    for host in hosts + [topology["gateway_host"], topology["load_host"]]:
        check(re.fullmatch(r"[A-Za-z0-9._-]+", host) is not None, "IPv4/DNS hostname required")
    check(args.distribution in ("RANGE", "TAG", "ORDER") or args.split == "NONE", "individual split incompatible with this B1 scheme")
    check(not args.distribution.startswith("RING") or args.placement == "NONE" and args.scenario not in ("MOVE", "CRASH_MOVE"), "Ring allows membership moves only; use CRASH_SCALE/AUTO")
    check(args.placement!="FIXED_LPT" or args.split=="NONE" and args.distribution!="RESIZABLE", "fixed-unit LPT cannot combine with split or resizable slots")
    check(args.keys >= 8 and args.rate > 0 and args.requests > 0 and args.warmup >= 0, "invalid workload bounds")
    check(0 <= args.read <= 100 and 0 <= args.delete <= 100 and args.read + args.delete <= 100, "invalid operation mix")
    legacy = args.protocol
    a1, a2 = getattr(args, "a1", None) or "RAFT", getattr(args, "a2", None) or "O00"
    check(not legacy or not (getattr(args, "a1", None) or getattr(args, "a2", None)), "use --a1 and --a2 together; --protocol is isolated legacy mode")
    label = legacy or a1 + "+" + a2
    check(getattr(args,"read_selection","ADAPTIVE")=="ADAPTIVE" or (legacy or a2)=="V06", "ROUND_ROBIN read selection requires V06")
    check(not legacy or not legacy.startswith("PB") or args.delete == 0, "PB has no replicated DELETE; no silent command substitution")
    check((legacy or a1) != "PB" or args.scenario not in ("PAUSE_LEADER", "PAUSE_OBSERVER"), "plain PB is crash-only; use PB_FENCED for pause/fencing")
    check(args.scenario not in ("SCALE_CYCLE", "CRASH_SCALE") or groups > args.initial_groups, "scale-out needs a prepared reserve group")
    run_id = "i" + uuid.uuid4().hex
    crash = args.scenario in ("CRASH_MOVE", "CRASH_SCALE")
    cluster = {
        "run.id": run_id, "failure.detector": args.fd,
        "gateway.host": topology["gateway_host"], "gateway.port": args.gateway_port,
        "groups.count": groups, "groups.initial": args.initial_groups, "groups.min": args.min_groups, "replica.port": args.replica_port,
        "failure.timeout.ms": 1000, "failure.grace.ms": 3000, "failure.seed": args.seed,
        "distribution": args.distribution, "tablets.per.group": 4, "split": args.split, "placement": args.placement,
        "copy": args.copy, "migration.bytes.per.tick": 256 if crash else 65536, "migration.tick.ms": 20 if crash else 10,
        "migration.timeout.ms": 300000, "migration.max.records": 200000, "request.timeout.ms": 5000,
        "storage.workers": 2, "storage.io.workers": 2, "history.target.entries": 4096, "native.data.root": "",
        "prepare.max.entries": 16, "prepare.max.encoded.bytes": 65536, "prepare.max.delay.ms": 20,
        "apply.max.in.flight.keys": 16, "read.selection": "ADAPTIVE", "meta.batch.entries": 16, "meta.batch.delay.ms": 1,
        "vr.mode": args.vr_mode, "rose.limit": args.rose_limit, "replication.retry.ms": 50,
        "scaler": args.scaler, "scheduler": args.scheduler, "drain": args.drain,
        "control.interval.ms": args.control_ms if args.control_ms is not None else (1000 if args.scenario == "AUTO" else 0),
        "control.cooldown.windows": 2, "control.max.plans": 1000,
        "scheduler.concurrent.jobs": 4, "scheduler.jobs.per.node": 4, "scheduler.node.bytes.per.tick": 65536,
        "copy.batch.bytes": 65536, "copy.dirty.threshold.bytes": 16384, "copy.max.live.rounds": 10,
        "scaling.node.bytes": 67108864, "scaling.key.bytes": args.value_bytes,
        "scaling.read.service": .01, "scaling.write.service": .02, "scaling.slo": .12,
        "scaling.low": .35, "scaling.high": .75, "scaling.target": .60, "scaling.provision.windows": 2,
        "scaling.period": 48, "scaling.training.periods": 6, "scaling.forecast.horizon": 24,
        "scaling.transfer.bytes": 2048, "scaling.migration.penalty": .15, "scaling.write.ratio": (100-args.read)/100, "scaling.placement.search.limit": 2000000,
    }
    cluster.update(fd_defaults(args.fd)); cluster.update(A2_SETTINGS); cluster["read.selection"] = getattr(args,"read_selection","ADAPTIVE")
    if legacy: cluster["protocol"] = legacy
    else: cluster.update({"a1.method": a1, "a2.method": a2})
    for group in range(groups): cluster[f"group.{group}.hosts"] = ",".join(hosts[group*3:group*3+3])
    protected = {"run.id", "gateway.host", "gateway.port", "replica.port", "protocol", "a1.method", "a2.method", "failure.detector", "read.selection", "distribution", "split", "placement", "copy", "scaler", "scheduler", "drain", "groups.count", "groups.initial", "groups.min"}
    for item in args.override:
        key, value = item.split("=", 1)
        check((key in cluster or key in FD_KEYS) and key not in protected and not key.startswith("group."), "use named options for topology/policies")
        check("\n" not in value and "\r" not in value, "multiline override")
        cluster[key] = value
    if args.fd == "IGNITE_TCP_RING" and args.fault_index == int(cluster.get("fd.coordinator.index",2)) and args.scenario in ("CRASH_MOVE","CRASH_SCALE","PAUSE_LEADER","PAUSE_OBSERVER"):
        raise ValueError("UNSUPPORTED: the current Ignite prototype has no coordinator re-election; select a non-coordinator fault")
    schedule = ""
    if args.scenario in ("SCALE_CYCLE", "CRASH_SCALE"):
        schedule = f"{args.requests//4}:{groups}"
        if args.scenario == "SCALE_CYCLE": schedule += f",{args.requests*3//4}:{args.min_groups}"
    workload = {
        "target.host": topology["gateway_host"], "target.port": args.gateway_port,
        "study": "system-integration-full", "variant": label, "scenario": args.scenario, "seed": args.seed,
        "key.count": args.keys, "key.length": 24, "value.size": args.value_bytes,
        "mix.get": args.read, "mix.set": 100 - args.read - args.delete, "mix.delete": args.delete,
        "distribution": args.workload, "concurrency": args.concurrency,
        "warmup.requests": args.warmup, "measured.requests": args.requests, "request.timeout.ms": cluster["request.timeout.ms"],
        "repeat.count": 1, "run.timeout.ms": 3600000, "execution.timeout.ms": 3600000,
        "results.dir": "unused-integration-cli", "server.workers.hint": 2,
        "integration.run.id": run_id, "integration.rate": args.rate,
        "integration.balance.at": args.requests // 3 if args.scenario in ("MOVE", "BALANCE", "CRASH_MOVE") else -1,
        "integration.move.tablet": 0 if args.scenario in ("MOVE", "CRASH_MOVE") else -1,
        "integration.move.to": 1, "integration.audit.keys": min(args.keys, 1000), "integration.drain.ms": 300000,
        "integration.window.requests": max(1, args.requests // 2), "integration.scale.schedule": schedule,
        "integration.require.scaler.decision": "true" if args.scaler != "fixed" else "false",
        "integration.require.joint": "false" if legacy else "true",
        "integration.key.mode": args.key_mode, "integration.tag.count": min(8, args.keys),
    }
    if args.workload == "HOT_RANGE": workload.update({"hot.range.key.fraction": 0.125, "hot.range.request.fraction": 0.85, "hot.range.shift.per.window": args.hot_shift})
    if args.workload == "ZIPF": workload["zipf.exponent"] = 0.99
    if rate_after is not None:
        workload.update({"integration.rate.after": rate_after, "integration.rate.change.at": rate_change_at})
    auxiliary = {}
    for source, key, name in [(args.calibration, "scaling.calibration.file", "calibration.csv"), (args.history, "scaling.history.file", "history.txt")]:
        if source:
            shutil.copyfile(source, output/name); cluster[key] = name; auxiliary[name] = sha(output/name)
    props(output/"cluster.properties", cluster); props(output/"workload.properties", workload); save(output/"topology.json", topology)
    save(output/"plan.json", {"schema": 2, "run_id": run_id, "scenario": args.scenario, "protocol": label, "joint_cache": not bool(legacy), "a1": None if legacy else a1, "a2": None if legacy else a2, "fault_layer": getattr(args, "fault_layer", "A1"), "fd": args.fd, "read_selection":getattr(args,"read_selection","ADAPTIVE"),
        "copy": args.copy, "scaler": args.scaler, "scheduler": args.scheduler, "drain": args.drain, "topology": topology,
        "groups": groups, "initial_groups": args.initial_groups, "fault_index": args.fault_index,
        "keys": args.keys, "rate": args.rate, "requests": args.requests, "seed": args.seed,
        "rate_after": rate_after, "rate_change_at": rate_change_at, "fault_seconds": fault_seconds,
        "operating_case": operating_case, "strategy_number": strategy_number,
        "notes": ["single run-local gateway; native TCP/file KV; prepared replica groups", "logical service scale-out/in; no EC2 instance lifecycle", "A1 admission/routing authority + A2 SSD data replication in each replica JVM" if not legacy else "legacy isolated engine: not a full six-module cache"],
        "cluster_sha256": sha(output/"cluster.properties"), "workload_sha256": sha(output/"workload.properties"), "auxiliary_sha256": auxiliary})
    print(output)


def process_identity(pid):
    base = Path(f"/proc/{pid}")
    stat = (base / "stat").read_text()
    fields = stat[stat.rfind(")") + 2:].split()
    return {"start_ticks": fields[19], "state": fields[0], "command": (base / "cmdline").read_bytes().split(b"\0")[:-1]}


def owned(state):
    try:
        info = process_identity(state["pid"])
        return info["state"] != "Z" and info["start_ticks"] == state["start_ticks"] and [x.decode() for x in info["command"]] == state["command"]
    except (FileNotFoundError, ProcessLookupError):
        return False


def role_command(bundle, root, role):
    config = root / "input/cluster.properties"
    if role == "gateway":
        return GATEWAY, [str(config), str(root / "gateway")]
    if role == "load":
        return LOAD, [str(root / "input/workload.properties"), str(root / "load-result")]
    match = re.fullmatch(r"replica-g([0-7])-n([012])", role)
    check(match is not None, "unknown role")
    return REPLICA, [str(config), match[1], match[2], str(root / role)]


def agent(args):
    bundle, root = Path(args.bundle).resolve(), Path(args.root).resolve()
    main_class, role_args = role_command(bundle, root, args.role)
    state_path = root / "state" / (args.role + ".json")
    root.mkdir(parents=True, exist_ok=True)
    (root / "state").mkdir(exist_ok=True)
    (root / "logs").mkdir(exist_ok=True)
    if args.action == "start":
        check(not state_path.exists(), "role already has a start record; use a new run")
        main, argv = role_command(bundle, root, args.role)
        command = [args.java, "-Xms64m", "-Xmx512m", "-XX:MaxDirectMemorySize=128m", "-Dfile.encoding=UTF-8",
                   f"-Xlog:gc*:file={root / 'logs' / (args.role + '-gc.log')}", "-cp", str(bundle / "lib/*"), main] + argv
        with (root / "logs" / (args.role + ".log")).open("xb") as log:
            child = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
        LOCAL_PROCESSES[child.pid] = child
        time.sleep(.02)
        info = process_identity(child.pid)
        check(child.poll() is None and [x.decode() for x in info["command"]] == command, "spawned process identity differs or process exited")
        state = {"pid": child.pid, "start_ticks": info["start_ticks"], "command": [x.decode() for x in info["command"]], "role": args.role,
                 "started_utc_ns": time.time_ns(), "started_monotonic_ns": time.monotonic_ns()}
        save(state_path, state)
        print(json.dumps(state))
        return
    state = json.loads(state_path.read_text())
    check(state.get("role") == args.role and main_class in state["command"] and state["command"][-len(role_args):] == role_args
          and str(bundle / "lib/*") in state["command"], "state is not owned by this run/role/bundle")
    active = owned(state)
    if args.action in ("slow-replica", "network-degraded"):
        from operating_network import impair
        check(active and args.role.startswith("replica-"), "network fault requires an active, run-owned replica")
        cluster = load_props(root / "input/cluster.properties")
        group, index = map(int, re.fullmatch(r"replica-g([0-7])-n([012])", args.role).groups())
        hosts = cluster[f"group.{group}.hosts"].split(",")
        peers = [host for i, host in enumerate(hosts) if i != index]
        helper = Path("/usr/local/libexec/openkache-operating-network")
        if helper.is_file():
            check(cluster["replica.port"] == "27101", "EC2 network helper requires base replica port 27101")
            command = ["sudo", "-n", str(helper), args.action, "--peers", *peers,
                       "--gateway", cluster["gateway.host"], "--duration", str(args.fault_seconds)]
            print(subprocess.check_output(command, text=True, timeout=110).strip())
        else:
            print(json.dumps(impair(args.action, peers, cluster["gateway.host"], int(cluster["replica.port"]), args.fault_seconds)))
        return
    if args.action == "inspect":
        details = {"active": active, "state": state, "epoch_ns": time.time_ns(), "monotonic_ns": time.monotonic_ns()}
        if active:
            base = Path(f"/proc/{state['pid']}")
            for name in ("io", "status", "stat"):
                try:
                    details[name] = (base / name).read_text()
                except OSError as e:
                    details[name] = {"missing_reason": str(e)}
        print(json.dumps(details))
        return
    if active:
        sig = {"kill": signal.SIGKILL, "pause": signal.SIGSTOP, "resume": signal.SIGCONT, "stop": signal.SIGTERM}[args.action]
        os.kill(state["pid"], sig)
        if args.action in ("stop", "kill"):
            until = time.monotonic() + 15
            while owned(state) and time.monotonic() < until:
                time.sleep(.05)
            if owned(state):
                os.kill(state["pid"], signal.SIGKILL)
        child = LOCAL_PROCESSES.get(state["pid"])
        if child is not None and args.action in ("stop", "kill"):
            child.wait(timeout=10)
    if not active and args.action in ("stop", "kill") and state["pid"] in LOCAL_PROCESSES:
        LOCAL_PROCESSES[state["pid"]].wait(timeout=10)
    print(json.dumps({"action": args.action, "was_active": active, "epoch_ns": time.time_ns(), "monotonic_ns": time.monotonic_ns()}))


class Controller:
    def __init__(self, args, remote):
        self.args, self.remote = args, remote
        self.plan_dir = Path(args.plan).resolve()
        self.plan = json.loads((self.plan_dir / "plan.json").read_text())
        self.topology = self.plan["topology"]
        self.replica_count = len(self.topology["replica_hosts"]); self.group_count = self.replica_count // 3
        check(self.replica_count % 3 == 0 and 2 <= self.group_count <= 8, "invalid replica group inventory")
        for name, digest in self.plan.get("auxiliary_sha256", {}).items():
            check(Path(name).name == name and sha(self.plan_dir/name) == digest, "auxiliary input hash mismatch")
        self.bundle = Path(args.bundle).resolve()
        self.bundle_hash = verify_bundle(self.bundle)
        self.run_id = self.plan["run_id"]
        check(sha(self.plan_dir / "cluster.properties") == self.plan["cluster_sha256"] and sha(self.plan_dir / "workload.properties") == self.plan["workload_sha256"], "plan settings changed")
        self.output = Path(args.output).resolve()
        self.output.mkdir(parents=True, exist_ok=False)
        self.started = []
        self.start_attempts = []
        self.paused = []
        self.fault = None
        self.network_fault_pending = False
        self.closed = False
        self.metrics = []
        if remote:
            check(len(set(self.topology["replica_hosts"] + [self.topology["gateway_host"], self.topology["load_host"]])) == self.replica_count + 2, "EC2 needs distinct replica/gateway/load hosts")
            check(args.identity and args.known_hosts, "EC2 requires identity and verified known_hosts")
            root = self.topology["remote_root"]
            check(root.startswith("/") and ".." not in PurePosixPath(root).parts and re.fullmatch(r"/[A-Za-z0-9_./-]+", root), "remote_root must be an absolute literal path")
            user = self.topology["ssh_user"]
            check(re.fullmatch(r"[A-Za-z0-9_-]+", user), "invalid SSH user")
            self.root = PurePosixPath(root) / self.run_id
            self.remote_bundle = PurePosixPath(root) / ("bundle-" + self.bundle_hash[:16])
        else:
            self.root = self.output / "runtime"
            self.root.mkdir()
            shutil.copytree(self.plan_dir, self.root / "input")
            self.remote_bundle = self.bundle
        self.cluster = load_props(self.plan_dir / "cluster.properties")
        self.gateway_host = self.topology["gateway_host"]
        self.gateway_port = int(self.cluster["gateway.port"])
        self.roles = [(f"replica-g{g}-n{n}", self.topology["replica_hosts"][g*3+n]) for g in range(self.group_count) for n in range(3)] + [("gateway", self.gateway_host), ("load", self.topology["load_host"])]

    def ssh_options(self):
        return ["-i", str(Path(self.args.identity).resolve()), "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=yes", "-o", "UserKnownHostsFile=" + str(Path(self.args.known_hosts).resolve()), "-o", "ConnectTimeout=10"]

    def ssh(self, host, command, timeout=60):
        result = subprocess.run(["ssh", *self.ssh_options(), self.topology["ssh_user"] + "@" + host, shlex.join([str(x) for x in command])], capture_output=True, text=True, timeout=timeout)
        check(result.returncode == 0, "remote command failed: " + result.stderr[-1500:])
        return result.stdout

    def agent(self, action, role, host):
        command = ["python3", str(self.remote_bundle / "tools/integration.py"), "agent", action,
                   "--bundle", str(self.remote_bundle), "--root", str(self.root), "--role", role, "--java", self.args.java]
        if self.remote:
            if action in ("slow-replica", "network-degraded"):
                command.extend(["--fault-seconds", str(self.plan["fault_seconds"])])
                return json.loads(self.ssh(host, command, timeout=120))
            return json.loads(self.ssh(host, command))
        ns = argparse.Namespace(action=action, bundle=str(self.bundle), root=str(self.root), role=role, java=self.args.java)
        import contextlib, io
        text = io.StringIO()
        with contextlib.redirect_stdout(text):
            agent(ns)
        return json.loads(text.getvalue())

    def validate(self):
        version = subprocess.run([self.args.java, "-version"], capture_output=True, text=True, check=True)
        check(re.search(r'version "21\.', version.stdout + version.stderr), "Java 21 required")
        validations = []
        for main, arguments in [(PREFLIGHT, [str(self.plan_dir / "cluster.properties")]), (LOAD, ["--validate", str(self.plan_dir / "workload.properties")])]:
            result = subprocess.run([self.args.java, "-cp", str(self.bundle / "lib/*"), main] + arguments, capture_output=True, text=True)
            check(result.returncode == 0, "config validation failed: " + result.stderr[-2000:])
            validations.append({"main": main, "output": result.stdout})
        result = subprocess.run([self.args.java, "-cp", str(self.bundle / "lib/*"), PREFLIGHT, "--catalog"], capture_output=True, text=True, check=True)
        catalog = json.loads(result.stdout)
        check(set(catalog["a1"]) == set(A1_PROTOCOLS) and set(catalog["a2"]) == set(A2_PROTOCOLS), "Python/Java engine catalog differs")
        check(set(catalog["distribution"]) == set(DISTRIBUTIONS) and set(catalog["scaler"]) == set(SCALERS) and set(catalog["scheduler"]) == set(SCHEDULERS) and set(catalog["drain"]) == set(DRAINS), "Python/Java policy catalog differs")
        check(set(catalog["failureDetector"]) == set(FAILURE_METHODS), "Python/Java A3 catalogue differs")
        check(set(catalog["readSelection"]) == set(READ_SELECTIONS), "Python/Java A2 read policy catalogue differs")
        save(self.output/"runtime-catalog.json", catalog)
        save(self.output / "preflight.json", {"java": version.stdout + version.stderr, "validation": validations})

    def stage(self):
        if not self.remote:
            return
        script = "import json,os,pathlib,platform,shutil,subprocess,sys,time;v=subprocess.run([sys.argv[1],'-version'],capture_output=True,text=True,check=True);print(json.dumps({'java':v.stdout+v.stderr,'python':sys.version,'platform':platform.platform(),'cpu_count':os.cpu_count(),'disk_free_bytes':shutil.disk_usage(pathlib.Path.home()).free,'proc_available':pathlib.Path('/proc/self/stat').exists(),'epoch_ns':time.time_ns()}))"
        environments = {}
        for host in dict.fromkeys(host for _, host in self.roles):
            info = json.loads(self.ssh(host, ["python3", "-c", script, self.args.java]))
            environments[host] = info
            save(self.output / "remote-environments.json", environments)
            check(re.search(r'version "21\.', info["java"]) and info["proc_available"], "remote Java 21/Linux required: " + host)
        archive = self.output / "bundle.zip"
        with zipfile.ZipFile(archive, "w", zipfile.ZIP_DEFLATED) as z:
            for p in self.bundle.rglob("*"):
                if p.is_file():
                    z.write(p, str(p.relative_to(self.bundle)))
        for host in dict.fromkeys(host for _, host in self.roles):
            self.ssh(host, ["mkdir", "-p", str(self.root.parent)])
            self.ssh(host, ["mkdir", str(self.root)])
            destination = self.topology["ssh_user"] + "@" + host + ":"
            inputs_zip = self.output / "inputs.zip"
            if not inputs_zip.exists():
                with zipfile.ZipFile(inputs_zip, "w", zipfile.ZIP_DEFLATED) as z:
                    for item in self.plan_dir.iterdir():
                        if item.is_file(): z.write(item, item.name)
            for source, name in [(archive, "bundle.zip"), (inputs_zip, "inputs.zip")]:
                subprocess.run(["scp", *self.ssh_options(), str(source), destination + str(self.root / name)], check=True, capture_output=True)
            script = "import pathlib,zipfile,sys;root=pathlib.Path(sys.argv[1]);bundle=pathlib.Path(sys.argv[2]);bundle.mkdir(exist_ok=True);zipfile.ZipFile(root/'bundle.zip').extractall(bundle);(root/'input').mkdir();zipfile.ZipFile(root/'inputs.zip').extractall(root/'input')"
            self.ssh(host, ["python3", "-c", script, str(self.root), str(self.remote_bundle)])
            self.ssh(host, ["python3", str(self.remote_bundle / "tools/integration.py"), "verify-bundle", "--bundle", str(self.remote_bundle)])

    def node_status(self, group, index, layer="A2"):
        host = self.topology["replica_hosts"][group*3+index]
        with urllib.request.urlopen(f"http://{host}:{int(self.cluster['replica.port'])+4+(8 if layer == 'A1' and self.plan.get('joint_cache') else 0)}/status", timeout=2) as response:
            return json.load(response)

    def sample(self):
        snapshot = {"collected_epoch_ns": time.time_ns(), "collected_monotonic_ns": time.monotonic_ns(), "nodes": [], "processes": {}}
        for g in range(self.group_count):
            for n in range(3):
                try:
                    state = self.node_status(g, n)
                    check(state["runId"] == self.run_id, "wrong replica run ID")
                    if self.plan.get("joint_cache"): state["a1_authority"] = self.node_status(g, n, "A1")
                    snapshot["nodes"].append(state)
                except Exception as e:
                    snapshot["nodes"].append({"group": g, "index": n, "missing_reason": str(e)})
        def inspect(role_host):
            role, host = role_host
            try:
                return role, self.agent("inspect", role, host)
            except Exception as e:
                return role, {"missing_reason": str(e)}
        if self.remote:
            with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
                snapshot["processes"].update(pool.map(inspect, self.started))
        else:
            snapshot["processes"].update(map(inspect, self.started))
        snapshot["completed_monotonic_ns"] = time.monotonic_ns()
        self.metrics.append(snapshot)
        # Preserve incremental evidence without rewriting every prior sample on
        # the single-vCPU gateway during each observation interval.
        with (self.output / "resource-samples.jsonl").open("a") as stream:
            stream.write(json.dumps(snapshot) + "\n")

    def leader(self, group, *, layer=None):
        votes = {}
        layer = layer or self.plan.get("fault_layer", "A1")
        ionia = layer == "A2" if self.plan.get("joint_cache") else self.plan["protocol"] in A2_PROTOCOLS
        for n in range(3):
            try:
                native = self.node_status(group, n, layer)["native"]
                key = native.get("view", native.get("epoch", 0)), native["leader"]
                votes[key] = votes.get(key, 0) + 1
            except Exception:
                pass
        candidates = [key for key, count in votes.items() if count >= 2 and key[1] >= 0]
        check(candidates, "no agreeing leader evidence")
        _, leader = max(candidates)
        return int(leader) - (1 if ionia else 0)

    def check_fault_supported(self,index):
        if self.cluster["failure.detector"]=="IGNITE_TCP_RING" and index==int(self.cluster.get("fd.coordinator.index",2)):
            raise ValueError("UNSUPPORTED: current Ignite coordinator re-election is absent; target a non-coordinator replica")

    def measurement_started(self):
        if self.remote:
            script = "import pathlib,sys; print((pathlib.Path(sys.argv[1])/'load-result/MEASURE_STARTED').exists())"
            return self.ssh(self.topology["load_host"], ["python3", "-c", script, str(self.root)]).strip() == "True"
        return (self.root / "load-result/MEASURE_STARTED").exists()

    def crash_replica(self, group, index):
        self.check_fault_supported(index)
        role, host = self.roles[group*3+index]
        requested = time.monotonic_ns()
        evidence = self.agent("kill", role, host)
        confirmed = time.monotonic_ns()
        check(evidence["was_active"] and not self.agent("inspect", role, host)["active"], "owned crash was not confirmed")
        evidence["repair_admission"] = endpoint(self.gateway_host, self.gateway_port, 104, struct.pack(">ii", group, index))
        return {"scenario": self.plan["scenario"], "role": role, "group": group, "index": index, "evidence": evidence,
                "controller_requested_monotonic_ns": requested, "controller_confirmed_monotonic_ns": confirmed}

    def fault_if_due(self):
        if self.fault is not None:
            return
        scenario = self.plan["scenario"]
        if scenario in ("CRASH_MOVE", "CRASH_SCALE"):
            state = endpoint(self.gateway_host, self.gateway_port, 96)
            active = [j for j in state["migrations"] if (not j.get("published") and j.get("state") in ("COPYING", "CATCHING_UP", "SWITCHING")) or (j.get("copy") == "post-copy" and j.get("state") == "PULLING")]
            if not active:
                return
            group = active[0]["from"]
            index = self.plan.get("fault_index")
            if index is None: index = self.leader(group)
            self.fault = self.crash_replica(group, index)
            self.fault["migration"] = active[0]
        elif scenario in OPERATING_FAULTS:
            if not self.measurement_started():
                return
            if scenario == "CRASH_LEADER":
                self.fault = self.crash_replica(0, self.leader(0))
                marker = self.root / "load-result/FAULT_CONFIRMED"
                if self.remote:
                    self.ssh(self.topology["load_host"], ["touch", marker])
                else:
                    marker.touch(exist_ok=False)
            else:
                index = (self.leader(0, layer="A2") + 1) % 3 if scenario == "SLOW_REPLICA" else self.leader(0)
                role, host = self.roles[index]
                action = "slow-replica" if scenario == "SLOW_REPLICA" else "network-degraded"
                self.network_fault_pending = True
                evidence = self.agent(action, role, host)
                check(evidence.get("released") is True, "network impairment cleanup not confirmed")
                self.network_fault_pending = False
                self.fault = {"scenario": scenario, "role": role, "group": 0, "index": index, "evidence": evidence}
        elif scenario in ("PAUSE_LEADER", "PAUSE_OBSERVER"):
            if not self.measurement_started():
                return
            index = self.leader(0)
            if scenario == "PAUSE_OBSERVER":
                index = (index + 1) % 3
            self.check_fault_supported(index)
            role, host = self.roles[index]
            evidence = self.agent("pause", role, host)
            self.paused.append((role, host))
            self.fault = {"scenario": scenario, "role": role, "group": 0, "index": index, "evidence": evidence}
            time.sleep(1)
            self.fault["resume"] = self.agent("resume", role, host)
            self.paused.remove((role, host))
        if self.fault is not None:
            save(self.output / "fault.json", self.fault)

    def fd_evidence(self):
        if self.cluster["failure.detector"]=="FIXED_TIMEOUT":return {"valid":True,"source":"native-engine-timeouts"}
        nodes=self.metrics[-1]["nodes"] if self.metrics else []
        failed=((self.fault or {}).get("group"), (self.fault or {}).get("index")) if self.plan["scenario"] in CRASH_SCENARIOS else (None, None)
        survivor_count=0; healthy=True; live_down=[]; observed={"A1":False,"A2":False}
        for node in nodes:
            if (node.get("group"),node.get("index"))==failed:continue
            for layer,status in (("A2",node),("A1",node.get("a1_authority"))):
                if status is None and not self.plan.get("joint_cache"):continue
                if not status or "missing_reason" in status:healthy=False;continue
                fd=status.get("fd");healthy=healthy and bool(fd) and fd.get("runtimeHealthy",True);survivor_count+=1
                if fd:
                    for member in fd.get("deadMembers",[]):
                        index=member-(1 if layer=="A2" and (self.plan.get("joint_cache") or self.plan["protocol"] in A2_PROTOCOLS) else 0)
                        if (node.get("group"),index)!=failed:
                            live_down.append({"observerGroup":node.get("group"),"observerIndex":node.get("index"),"layer":layer,"reportedIndex":index})
                if self.fault and node.get("group")==failed[0] and fd:
                    native_id=failed[1]+(1 if layer=="A2" and (self.plan.get("joint_cache") or self.plan["protocol"] in A2_PROTOCOLS) else 0)
                    if native_id in fd.get("deadMembers",[]):observed[layer]=True
        crash=self.plan["scenario"] in CRASH_SCENARIOS
        if crash:healthy=healthy and observed["A2"] and (observed["A1"] or not self.plan.get("joint_cache"))
        return {"valid":bool(healthy and survivor_count),"liveLayerSnapshots":survivor_count,"crashDeadObserved":observed,"liveProcessDownReports":live_down,"scope":"native-observer-signals; not promotion grants"}

    def collect(self):
        failures = []
        if self.remote:
            for role, host in self.roles:
                target = self.output / "collected" / role
                target.mkdir(parents=True, exist_ok=True)
                for name in ("logs", "state", "input") + (("load-result",) if role == "load" else ("gateway",) if role == "gateway" else (role,)):
                    # Replica data는 원격에 보존한다. 대용량 KV 파일을 자동 전송하지 않는다.
                    if name == role and role.startswith("replica-"):
                        for leaf in (("a1-authority/events.jsonl", "a1-authority/ready.json", "a2-data/events.jsonl", "a2-data/ready.json") if self.plan.get("joint_cache") else ("events.jsonl", "ready.json")):
                            command = ["scp", *self.ssh_options(), self.topology["ssh_user"] + "@" + host + ":" + str(self.root / role / leaf), str(target / leaf.replace("/", "-"))]
                            try:
                                result = subprocess.run(command, capture_output=True, text=True, timeout=60)
                                if result.returncode: failures.append({"role": role, "path": leaf, "error": result.stderr[-1500:]})
                            except subprocess.TimeoutExpired: failures.append({"role": role, "path": leaf, "error": "collection timeout"})
                    else:
                        try:
                            result = subprocess.run(["scp", "-r", *self.ssh_options(), self.topology["ssh_user"] + "@" + host + ":" + str(self.root / name), str(target)], capture_output=True, text=True, timeout=60)
                            if result.returncode: failures.append({"role": role, "path": name, "error": result.stderr[-1500:]})
                        except subprocess.TimeoutExpired: failures.append({"role": role, "path": name, "error": "collection timeout"})
        else:
            # runtime은 이 output 아래에 이미 보존되어 있다.
            pass
        save(self.output / "collection.json", {"complete": not failures, "failures": failures})
        return failures

    def run(self):
        if self.plan["scenario"] in ("SLOW_REPLICA", "NETWORK_DEGRADED"):
            check(self.remote, "UNSUPPORTED: network impairment requires dedicated remote Linux hosts")
        self.validate()
        self.stage()
        metadata = {"schema": 1, "run_id": self.run_id, "bundle_manifest_sha256": self.bundle_hash,
                    "plan": self.plan, "controller_epoch_ns": time.time_ns(), "controller_monotonic_ns": time.monotonic_ns(),
                    "origin": "ec2-distinct-hosts" if self.remote else "local-independent-jvms-shared-host"}
        save(self.output / "manifest.json", metadata)
        try:
            if self.remote:
                startup_errors = []
                self.start_attempts.extend(self.roles[:self.replica_count])
                with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
                    futures = {pool.submit(self.agent, "start", role, host): (role, host) for role, host in self.roles[:self.replica_count]}
                    for future in concurrent.futures.as_completed(futures):
                        role_host = futures[future]
                        try:
                            future.result(); self.started.append(role_host)
                        except Exception as e:
                            startup_errors.append({"role": role_host[0], "error": str(e)})
                check(not startup_errors, "replica startup failed: " + json.dumps(startup_errors))
            else:
                for role, host in self.roles[:self.replica_count]:
                    self.start_attempts.append((role, host))
                    self.agent("start", role, host)
                    self.started.append((role, host))
            until = time.monotonic() + 90
            while True:
                try:
                    statuses = [self.node_status(g, n, layer) for g in range(self.group_count) for n in range(3) for layer in (("A1", "A2") if self.plan.get("joint_cache") else ("A2",))]
                    check(all(s["runId"] == self.run_id for s in statuses), "replica run ID mismatch")
                    break
                except Exception:
                    if time.monotonic() >= until:
                        raise
                    time.sleep(.2)
            if self.cluster["failure.detector"] != "FIXED_TIMEOUT":
                # HTTP readiness는 FD startup grace가 끝났다는 뜻이 아니다.
                # pause 비교가 grace 안에서 일어나 FD가 작동하지 않는 것을 막는다.
                fd_after = time.monotonic() + int(self.cluster["failure.grace.ms"]) / 1000 + .4
                while time.monotonic() < fd_after:
                    time.sleep(max(0, min(1, fd_after - time.monotonic())))
                fd_states = [self.node_status(g, n, layer) for g in range(self.group_count) for n in range(3) for layer in (("A1", "A2") if self.plan.get("joint_cache") else ("A2",))]
                check(all(s["runId"] == self.run_id and s["fd"] is not None and s["fd"]["sentFrames"] > 0 and s["fd"].get("runtimeHealthy",True) for s in fd_states), "FD startup gate has no actual socket evidence")
                save(self.output / "fd-readiness-gate.json", {"controller_epoch_ns": time.time_ns(), "nodes": fd_states})
                until = time.monotonic() + 90
            self.start_attempts.append(self.roles[-2])
            self.agent("start", *self.roles[-2]); self.started.append(self.roles[-2])
            while True:
                try:
                    ready = endpoint(self.gateway_host, self.gateway_port, 101, timeout=15)
                    check(ready["runId"] == self.run_id, "gateway run ID mismatch")
                    break
                except Exception:
                    if time.monotonic() >= until:
                        raise
                    time.sleep(.2)
            self.start_attempts.append(self.roles[-1])
            self.agent("start", *self.roles[-1]); self.started.append(self.roles[-1])
            until = time.monotonic() + self.args.timeout
            sample_at = time.monotonic()
            while True:
                self.fault_if_due()
                if self.args.sample_seconds > 0 and time.monotonic() >= sample_at:
                    self.sample(); sample_at = time.monotonic() + self.args.sample_seconds
                if not self.agent("inspect", *self.roles[-1])["active"]:
                    break
                check(time.monotonic() < until, "run timeout")
                time.sleep(.02 if self.plan["scenario"] in ("CRASH_MOVE", "CRASH_SCALE") else .2)
            self.sample()
            self.fd_runtime_evidence = self.fd_evidence()
            save(self.output/"fd-runtime-evidence.json",self.fd_runtime_evidence)
            self.final_gateway = endpoint(self.gateway_host, self.gateway_port, 96)
            save(self.output / "gateway-final-controller.json", self.final_gateway)
            if self.plan["scenario"] in (*OPERATING_FAULTS, "CRASH_MOVE", "CRASH_SCALE", "PAUSE_LEADER", "PAUSE_OBSERVER"):
                check(self.fault is not None, "fault trigger window was not observed; not a valid fault run")
        finally:
            cleanup_errors = []
            if self.network_fault_pending:
                cleanup_errors.append({"action": "network", "error": "network impairment release was not confirmed"})
            for role, host in self.paused:
                try:
                    self.agent("resume", role, host)
                except Exception as e:
                    cleanup_errors.append({"role": role, "action": "resume", "error": str(e)})
            for role, host in reversed(self.start_attempts):
                try:
                    self.agent("stop", role, host)
                    check(not self.agent("inspect", role, host)["active"], "owned process remains active")
                except Exception as e:
                    cleanup_errors.append({"role": role, "action": "stop", "error": str(e)})
                    save(self.output / (role + "-cleanup-error.json"), {"error": str(e)})
            save(self.output / "cleanup.json", {"complete": not cleanup_errors, "failures": cleanup_errors})
            collection_errors = self.collect()
            save(self.output / "resource-samples.json", self.metrics)
        check(not cleanup_errors and not collection_errors, "cleanup or collection incomplete; inspect evidence")
        load_dir = self.output / "collected/load/load-result" if self.remote else self.root / "load-result"
        result = verify_result(load_dir)
        result["fdRuntimeValid"] = self.fd_runtime_evidence["valid"]
        result["coverageValid"] = result["coverageValid"] and result["fdRuntimeValid"]
        result["fdLiveProcessDownReports"] = len(self.fd_runtime_evidence.get("liveProcessDownReports",[]))
        save(self.output / "result.json", result)
        (self.output / "RUN_COMPLETE").write_text("collected; inspect correctnessValid/migrationValid, fault and clock limits\n")
        print(json.dumps({"output": str(self.output), "result": result}))


def verify_result(directory):
    directory = Path(directory)
    check((directory / "COMPLETE").exists() and not (directory / "FAILED").exists(), "load did not complete")
    result = json.loads((directory / "result.json").read_text())
    check(result["offered"] == result["ok"] + result["miss"] + result["failed"] + result["notAdmitted"], "terminal count mismatch")
    check(sha(directory / "config.properties") == result["configSha256"], "load config hash mismatch")
    measured = []
    with (directory / "requests.csv").open(newline="") as stream:
        for row in csv.DictReader(stream):
            if row["phase"] == "measure":
                measured.append(row)
    check(len(measured) == result["offered"], "raw operation rows missing")
    check(len({(r["client"], r["operation"]) for r in measured}) == len(measured), "duplicate measured identities")
    check(sum(int(r["status"]) == 0 for r in measured) == result["ok"], "raw OK mismatch")
    check(sum(int(r["status"]) == 1 for r in measured) == result["miss"], "raw MISS mismatch")
    check(sum(int(r["status"]) == -3 for r in measured) == result["notAdmitted"], "raw admission mismatch")
    wanted = sorted(int(r["latency_from_schedule_ns"]) for r in measured if int(r["status"]) in (0, 1))
    import math
    p99 = wanted[math.ceil(len(wanted)*.99)-1] if wanted else -1
    check(p99 == result["successP99FromScheduleNanos"], "raw percentile mismatch")
    if result.get("jointCache"):
        gateway = json.loads((directory / "gateway-final.json").read_text())
        components = gateway.get("components", [])
        valid = gateway.get("routingInitialized") is True and gateway.get("routingUncertain") is False
        valid = valid and len(components) == gateway.get("preparedGroups") and all(c and c.get("a1") in A1_PROTOCOLS and c.get("a2") in A2_PROTOCOLS for c in components)
        valid = valid and all(sum(c.get(field, 0) for c in components) > 0 for field in ("authorityAdmissions", "dataCalls", "routingPublications"))
        check(bool(valid) == result.get("jointComponentsValid"), "joint role evidence disagrees with Runner")
    return result


def valid_combination(row):
    return not ((row.get("placement")=="FIXED_LPT" and (row.get("split")!="NONE" or row.get("distribution")=="RESIZABLE")) or (row.get("read_selection","ADAPTIVE")=="ROUND_ROBIN" and row.get("a2",row.get("protocol"))!="V06") or (row["distribution"] not in ("RANGE", "TAG", "ORDER") and row["split"] != "NONE")
                or (row["distribution"].startswith("RING") and row["placement"] != "NONE"))


def covering_rows(protocols=None, a1s=None, a2s=None, fds=None, read_selections=None):
    """호환 가능한 모든 전략 쌍을 덮는 결정적 기능 입력. 전수 Cartesian 실측을 대신하지 않는다."""
    axes = {**({"protocol": protocols} if protocols is not None else {"a1": a1s or A1_PROTOCOLS, "a2": a2s or A2_PROTOCOLS}), "fd": fds or FAILURE_METHODS, "read_selection":read_selections or READ_SELECTIONS, "distribution": DISTRIBUTIONS,
            "split": ("NONE", "MIDPOINT", "HEAT", "ESTORE"),
            "placement": PLACEMENTS,
            "scaler": SCALERS, "scheduler": SCHEDULERS, "copy": ("STOP", "PRE", "POST"), "drain": DRAINS}
    names = list(axes); base = {name: values[0] for name, values in axes.items()}; candidates = set()
    for left, right in itertools.combinations(names, 2):
        for a, b in itertools.product(axes[left], axes[right]):
            row = dict(base); row[left] = a; row[right] = b
            if not valid_combination(row):
                # 부수 기본값만 조정한다. 시험할 쌍 자체가 충돌하면 후보가 없다.
                if "distribution" not in (left, right): row["distribution"] = "RANGE"
                if "split" not in (left, right): row["split"] = "NONE"
                if "placement" not in (left, right): row["placement"] = "NONE"
            if row.get("read_selection")=="ROUND_ROBIN" and row.get("a2",row.get("protocol"))!="V06":
                if protocols is None and "a2" not in (left,right) and "V06" in (a2s or A2_PROTOCOLS): row["a2"]="V06"
                elif protocols is not None and "protocol" not in (left,right) and "V06" in protocols: row["protocol"]="V06"
                elif "read_selection" not in (left,right): row["read_selection"]="ADAPTIVE"
            if valid_combination(row): candidates.add(tuple(row[n] for n in names))
    rng = random.Random(111)
    for _ in range(4000):
        row = {n: rng.choice(values) for n, values in axes.items()}
        if valid_combination(row): candidates.add(tuple(row[n] for n in names))
    def pairs(values): return frozenset((i, values[i], j, values[j]) for i, j in itertools.combinations(range(len(names)), 2))
    pool = {values: pairs(values) for values in sorted(candidates)}
    uncovered = set().union(*pool.values()); selected = []
    while uncovered:
        best = max(pool, key=lambda row: len(pool[row] & uncovered)); got = pool.pop(best)
        check(got & uncovered, "pairwise coverage stalled")
        selected.append(dict(zip(names, best))); uncovered.difference_update(got)
    return selected


def matrix(args):
    joint = not getattr(args, "protocols", None)
    a1s = (getattr(args, "a1s", None) or ",".join(A1_PROTOCOLS)).split(",")
    a2s = (getattr(args, "a2s", None) or ",".join(A2_PROTOCOLS)).split(",")
    check(set(a1s) <= set(A1_PROTOCOLS) and set(a2s) <= set(A2_PROTOCOLS), "unknown joint shortlist")
    protocols = [a+"+"+b for a,b in itertools.product(a1s,a2s)] if joint else args.protocols.split(",")
    check(protocols and (joint or set(protocols) <= set(PROTOCOLS)), "unknown shortlist")
    def expand(row):
        if joint and "protocol" in row:
            row = dict(row); row["a1"], row["a2"] = row.pop("protocol").split("+")
        return row
    seeds = [int(x) for x in getattr(args, "seeds", "1,2,3").split(",")]; check(seeds and len(set(seeds)) == len(seeds), "distinct seeds required")
    workloads = getattr(args, "workloads", "UNIFORM,HOT_RANGE").split(","); check(set(workloads) <= {"UNIFORM", "HOT_RANGE", "ZIPF"}, "unknown workload")
    fds=[canonical_fd(x) for x in (getattr(args,"fds",None) or ",".join(FAILURE_METHODS)).split(",")]
    read_selections=(getattr(args,"read_selections",None) or ",".join(READ_SELECTIONS)).split(",")
    check(set(read_selections)<=set(READ_SELECTIONS),"invalid read-selection shortlist")
    protocol_reads=[(p,r) for p in protocols for r in read_selections if p.split("+")[-1]=="V06" or r=="ADAPTIVE"]
    entries = []
    offset, limit = getattr(args, "offset", 0), getattr(args, "limit", None)
    check((offset == 0 and limit is None) or args.stage == "exhaustive", "matrix slicing is for the exhaustive stage")
    if args.stage == "exhaustive":
        distributions = (getattr(args, "distributions", None) or ",".join(DISTRIBUTIONS)).split(",")
        splits = (getattr(args, "splits", None) or "NONE,MIDPOINT,HEAT,ESTORE").split(",")
        placements = (getattr(args, "placements", None) or ",".join(PLACEMENTS)).split(",")
        check(set(distributions) <= set(DISTRIBUTIONS) and set(splits) <= {"NONE", "MIDPOINT", "HEAT", "ESTORE"} and set(placements) <= set(PLACEMENTS), "unknown B1/B2 policy")
        valid_b = [dict(distribution=d, split=s, placement=p) for d,s,p in itertools.product(distributions,splits,placements) if valid_combination(dict(distribution=d,split=s,placement=p))]
        count = len(protocol_reads)*len(fds)*len(valid_b)*len(SCALERS)*len(SCHEDULERS)*3*len(DRAINS)*len(workloads)*len(seeds)
        check(0 <= offset <= count and (limit is None or limit > 0), "invalid matrix slice")
        stop = count if limit is None else min(count, offset + limit)
        selected = stop - offset
        if getattr(args, "count_only", False): print(json.dumps({"stage":"exhaustive","runs":selected,"execute":False})); return
        check(getattr(args, "allow_large", False) or selected <= 100000, "large exhaustive matrix: use --limit for a slice or --allow-large to write all rows")
        check(args.output, "--output required to write matrix")
        names = (["a1","a2"] if joint else ["protocol"])+["read_selection","fd","distribution","split","placement","scaler","scheduler","copy","drain","scenario","workload","seed","key_mode"]
        combinations = itertools.product(protocol_reads, fds, valid_b, SCALERS, SCHEDULERS, ("STOP", "PRE", "POST"), DRAINS, workloads, seeds)
        with Path(args.output).open("x",newline="") as stream:
            writer=csv.DictWriter(stream,fieldnames=names); writer.writeheader()
            for (protocol,read_selection),fd,b,scaler,scheduler,copy,drain,workload,seed in itertools.islice(combinations, offset, stop):
                writer.writerow(expand(dict(protocol=protocol,read_selection=read_selection,fd=fd,**b,scaler=scaler,scheduler=scheduler,copy=copy,drain=drain,scenario="AUTO",workload=workload,seed=seed,key_mode=getattr(args,"key_mode","TAGGED"))))
        print(json.dumps({"stage":"exhaustive","runs":selected,"execute":False})); return
    if args.stage == "coverage":
        for row, workload, seed in itertools.product(covering_rows(None,a1s,a2s,fds,read_selections) if joint else covering_rows(protocols,fds=fds,read_selections=read_selections), workloads, seeds):
            entries.append(dict(row, scenario="AUTO", workload=workload, seed=seed, key_mode=getattr(args,"key_mode","TAGGED")))
    elif args.stage == "smoke":
        for (protocol, read_selection), copy in itertools.product(protocol_reads, ("STOP", "PRE", "POST")):
            entries.append(dict(protocol=protocol, read_selection=read_selection, copy=copy, fd="FIXED_TIMEOUT", distribution="RANGE", split="NONE", placement="NONE", scaler="fixed", scheduler="fifo", drain="preflight-all", scenario="SCALE_CYCLE", workload="UNIFORM", seed=1))
    elif args.stage == "replication":
        for (protocol, read_selection), workload, seed in itertools.product(protocol_reads, workloads, seeds):
            entries.append(dict(protocol=protocol, read_selection=read_selection, copy="PRE", fd="FIXED_TIMEOUT", distribution="RANGE", split="NONE", placement="NONE", scaler="fixed", scheduler="fifo", drain="preflight-all", scenario="NORMAL", workload=workload, seed=seed))
    elif args.stage in ("policies", "interaction"):
        distributions = (getattr(args, "distributions", None) or ",".join(DISTRIBUTIONS)).split(",")
        splits = (getattr(args, "splits", None) or "NONE,MIDPOINT,HEAT,ESTORE").split(",")
        placements = (getattr(args, "placements", None) or ",".join(PLACEMENTS)).split(",")
        check(set(distributions) <= set(DISTRIBUTIONS) and set(splits) <= {"NONE", "MIDPOINT", "HEAT", "ESTORE"} and set(placements) <= set(PLACEMENTS), "unknown B1/B2 policy")
        for (protocol, read_selection), distribution, split, placement, copy, workload, seed in itertools.product(protocol_reads, distributions, splits, placements, ("PRE",) if args.stage == "policies" else ("STOP", "PRE", "POST"), workloads, seeds):
            row = dict(protocol=protocol, read_selection=read_selection, copy=copy, fd="FIXED_TIMEOUT", distribution=distribution, split=split, placement=placement, scaler="fixed", scheduler="fifo", drain="preflight-all", scenario="AUTO", workload=workload, seed=seed)
            if valid_combination(row): entries.append(row)
    else:
        for (protocol, read_selection), fd, scenario, copy, seed in itertools.product(protocol_reads, fds, ("CRASH_SCALE", "PAUSE_LEADER", "PAUSE_OBSERVER"), ("STOP", "PRE", "POST"), seeds):
            if protocol.split("+")[0] == "PB" and scenario != "CRASH_SCALE": continue
            if scenario != "CRASH_SCALE" and copy != "PRE": continue
            entries.append(dict(protocol=protocol, read_selection=read_selection, copy=copy, fd=fd, distribution="RANGE", split="NONE", placement="NONE", scaler="fixed", scheduler="fifo", drain="preflight-all", scenario=scenario, workload="HOT_RANGE", seed=seed))
    entries = [expand(row) for row in entries]
    check(entries, "no supported matrix entries")
    if getattr(args,"count_only",False): print(json.dumps({"stage":args.stage,"runs":len(entries),"execute":False})); return
    check(args.output, "--output required to write matrix")
    with Path(args.output).open("x", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(entries[0])); writer.writeheader(); writer.writerows(entries)
    print(json.dumps({"stage": args.stage, "runs": len(entries), "execute": False, "coverage": "all compatible strategy pairs; not exhaustive Cartesian" if args.stage == "coverage" else "specified stage"}))


PLAN_COLUMNS = {"a1":"--a1","a2":"--a2","protocol":"--protocol","fd":"--fd","distribution":"--distribution",
    "split":"--split","placement":"--placement","scaler":"--scaler","scheduler":"--scheduler","copy":"--copy","drain":"--drain",
    "scenario":"--scenario","workload":"--workload","seed":"--seed","key_mode":"--key-mode","read_selection":"--read-selection",
    "fault_index":"--fault-index","fault_layer":"--fault-layer","vr_mode":"--vr-mode","rose_limit":"--rose-limit",
    "operating_case":"--operating-case","strategy_number":"--strategy-number"}


def row_plan_arguments(row, allow_legacy=False):
    check(set(row)<=set(PLAN_COLUMNS),"unknown matrix columns: "+str(set(row)-set(PLAN_COLUMNS)))
    check(allow_legacy or not row.get("protocol"),"isolated legacy row rejected; joint A1+A2 rows required")
    check(bool(row.get("protocol")) or row.get("a1") in A1_PROTOCOLS and row.get("a2") in A2_PROTOCOLS,"row needs both A1 and A2")
    effective={"distribution":"RANGE","split":"NONE","placement":"NONE","read_selection":"ADAPTIVE",**row}
    check(valid_combination(effective),"incompatible matrix row")
    if row.get("fd"):canonical_fd(row["fd"])
    for column,choices in (("distribution",DISTRIBUTIONS),("placement",PLACEMENTS),("scaler",SCALERS),("scheduler",SCHEDULERS),("drain",DRAINS),("copy",("STOP","PRE","POST")),("read_selection",READ_SELECTIONS)):
        check(not row.get(column) or row[column] in choices,"unknown matrix selector: "+column)
    return [part for key,value in row.items() if value not in (None,"") for part in (PLAN_COLUMNS[key],str(value))]


def batch(args):
    check(0<=args.start and 1<=args.limit<=100000,"batch start>=0; limit=1..100000")
    with Path(args.matrix).open(newline="") as stream:
        rows=list(itertools.islice(csv.DictReader(stream),args.start,args.start+args.limit))
    check(rows,"no selected matrix rows")
    for row in rows: row_plan_arguments(row,args.allow_legacy)
    protected={"--output",*PLAN_COLUMNS.values()}
    check(not any(value.split("=",1)[0] in protected for value in args.plan_arg),"common plan arguments cannot overwrite matrix selectors/output")
    if not args.execute:
        print(json.dumps({"dry_run":True,"selected":len(rows),"start":args.start,"mode":args.mode,"first_rows":rows[:3],"message":"No processes or SSH calls; add --execute to run selected rows."},ensure_ascii=False,indent=2));return
    if args.mode=="ec2":check(args.identity and args.known_hosts and args.topology,"EC2 batch requires existing topology, identity and known-hosts")
    output=Path(args.output).resolve();output.mkdir(parents=True,exist_ok=False)
    verify_bundle(args.bundle);tool=str(Path(__file__).resolve());results=[]
    for offset,row in enumerate(rows):
        number=args.start+offset;case=output/f"row-{number:06d}";plan=case/"plan";run=case/"run";case.mkdir()
        command=[sys.executable,tool,"plan","--output",str(plan),*row_plan_arguments(row,args.allow_legacy)]
        if args.topology:command.extend(["--topology",args.topology])
        command.extend(args.plan_arg)
        log=case/"console.log"
        with log.open("x") as stream:
            preparation=subprocess.run(command,stdout=stream,stderr=subprocess.STDOUT)
            code=preparation.returncode
            if code==0:
                launch=[sys.executable,tool,args.mode,"--bundle",str(Path(args.bundle).resolve()),"--plan",str(plan),"--output",str(run),"--java",args.java,"--timeout",str(args.timeout),"--sample-seconds",str(args.sample_seconds)]
                if args.mode=="ec2":launch.extend(["--execute","--identity",args.identity,"--known-hosts",args.known_hosts])
                code=subprocess.run(launch,stdout=stream,stderr=subprocess.STDOUT).returncode
        result=json.loads((run/"result.json").read_text()) if (run/"result.json").exists() else None
        cleanup=json.loads((run/"cleanup.json").read_text()) if (run/"cleanup.json").exists() else None
        detail=log.read_text()[-6000:]
        status="PASS" if code==0 and result and all(result.get(k) is True for k in ("correctnessValid","migrationValid","coverageValid","jointComponentsValid","fdRuntimeValid")) else "FAILED_VALIDATION" if code==0 else "UNSUPPORTED" if "UNSUPPORTED" in detail else "REJECTED_CONFIG" if preparation.returncode else "RUN_ERROR"
        if cleanup is not None and not cleanup.get("complete"):status="CLEANUP_INCOMPLETE"
        record={"row":number,"selectors":row,"status":status,"exitCode":code,"result":result,"cleanup":cleanup,"path":str(case)}
        if status == "PASS" and row.get("operating_case"):
            from operating_suite import DEFAULT_MIN_SUCCESS, assess_case
            load_dir = run / ("collected/load/load-result" if args.mode == "ec2" else "runtime/load-result")
            fault_path = run / "fault.json"
            fault = json.loads(fault_path.read_text()) if fault_path.is_file() else None
            checks = assess_case(row["operating_case"], result, json.loads((run / "gateway-final-controller.json").read_text()),
                                 fault, load_dir, DEFAULT_MIN_SUCCESS)
            record["operatingChecks"] = checks
            if not all(checks.values()):
                record["status"] = status = "FAILED_VALIDATION"
        if code:record["errorTail"]=detail
        results.append(record);save(output/"batch-results.json",results)
        with (output/"batch-summary.csv").open("w",newline="") as stream:
            fields=["row","status",*PLAN_COLUMNS,"offered","ok","miss","failed","notAdmitted","auditErrors","path"]
            writer=csv.DictWriter(stream,fieldnames=fields);writer.writeheader()
            for item in results:writer.writerow({"row":item["row"],"status":item["status"],**item["selectors"],**{key:(item.get("result") or {}).get(key,"") for key in ("offered","ok","miss","failed","notAdmitted","auditErrors")},"path":item["path"]})
        print(json.dumps({"row":number,"status":status,"path":str(case)}),flush=True)
        if status=="CLEANUP_INCOMPLETE" or args.fail_fast and status!="PASS":break
    save(output/"batch-complete.json",{"selected":len(rows),"collected":len(results),"pass":sum(r["status"]=="PASS" for r in results),"statuses":{name:sum(r["status"]==name for r in results) for name in sorted({r["status"] for r in results})}})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    p = sub.add_parser("plan")
    p.add_argument("--output", required=True); p.add_argument("--topology"); p.add_argument("--protocol", choices=PROTOCOLS, help="legacy isolated engine; omits one A role")
    p.add_argument("--a1", choices=A1_PROTOCOLS); p.add_argument("--a2", choices=A2_PROTOCOLS)
    p.add_argument("--fault-layer", choices=("A1", "A2"), default="A1", help="leader used to choose the crashed/paused co-located replica")
    p.add_argument("--fd", type=canonical_fd, choices=FAILURE_METHODS, default="FIXED_TIMEOUT")
    p.add_argument("--read-selection", type=lambda s:s.upper().replace("-","_"), choices=READ_SELECTIONS, default="ADAPTIVE")
    p.add_argument("--copy", choices=("STOP", "PRE", "POST"), default="PRE"); p.add_argument("--distribution", choices=DISTRIBUTIONS, default="RANGE")
    p.add_argument("--split", choices=("NONE", "MIDPOINT", "HEAT", "ESTORE"), default="NONE")
    p.add_argument("--placement", choices=PLACEMENTS, default="NONE")
    p.add_argument("--scenario", choices=("NORMAL", "MOVE", "BALANCE", "AUTO", "SCALE_CYCLE", "CRASH_SCALE", "CRASH_MOVE", "PAUSE_LEADER", "PAUSE_OBSERVER", *OPERATING_FAULTS), default="MOVE")
    p.add_argument("--workload", choices=("UNIFORM", "HOT_RANGE", "ZIPF"), default="UNIFORM")
    p.add_argument("--hot-shift", type=float, default=0); p.add_argument("--keys", type=int, default=32)
    p.add_argument("--value-bytes", type=int, default=256); p.add_argument("--read", type=int, default=80); p.add_argument("--delete", type=int, default=0)
    p.add_argument("--concurrency", type=int, default=16); p.add_argument("--warmup", type=int, default=32); p.add_argument("--requests", type=int, default=300)
    p.add_argument("--rate", type=int, default=100); p.add_argument("--seed", type=int, default=1)
    p.add_argument("--rate-change-at", type=int); p.add_argument("--rate-after", type=int)
    p.add_argument("--fault-seconds", type=float, default=8)
    p.add_argument("--operating-case", choices=("steady", "hotspot", "slow-replica", "scale-out", "scale-in", "network-degraded", "leader-crash"))
    p.add_argument("--strategy-number", type=int)
    p.add_argument("--gateway-port", type=int, default=27300); p.add_argument("--replica-port", type=int, default=27101)
    p.add_argument("--groups", type=int); p.add_argument("--initial-groups", type=int, default=2); p.add_argument("--min-groups", type=int, default=1)
    p.add_argument("--scaler", choices=SCALERS, default="fixed"); p.add_argument("--scheduler", choices=SCHEDULERS, default="fifo"); p.add_argument("--drain", choices=DRAINS, default="preflight-all")
    p.add_argument("--control-ms", type=int); p.add_argument("--calibration"); p.add_argument("--history")
    p.add_argument("--key-mode", choices=("DEFAULT", "TAGGED", "ORDERED"), default="DEFAULT")
    p.add_argument("--vr-mode", choices=("FULL_LOG", "INCREMENTAL", "INCREMENTAL_CATCHUP"), default="INCREMENTAL_CATCHUP")
    p.add_argument("--rose-limit", type=int, default=4); p.add_argument("--fault-index", type=int, choices=(0,1,2))
    p.add_argument("--override", action="append", default=[])
    for name in ("local", "ec2"):
        c = sub.add_parser(name); c.add_argument("--bundle", required=True); c.add_argument("--plan", required=True); c.add_argument("--output", required=True)
        c.add_argument("--java", default="java"); c.add_argument("--timeout", type=float, default=900); c.add_argument("--sample-seconds", type=float, default=1)
        c.add_argument("--identity"); c.add_argument("--known-hosts"); c.add_argument("--execute", action="store_true")
    b = sub.add_parser("batch"); b.add_argument("--matrix",required=True); b.add_argument("--bundle",required=True); b.add_argument("--output",required=True)
    b.add_argument("--mode",choices=("local","ec2"),default="local"); b.add_argument("--execute",action="store_true")
    b.add_argument("--start",type=int,default=0); b.add_argument("--limit",type=int,default=10); b.add_argument("--fail-fast",action="store_true"); b.add_argument("--allow-legacy",action="store_true")
    b.add_argument("--topology"); b.add_argument("--identity"); b.add_argument("--known-hosts"); b.add_argument("--java",default="java")
    b.add_argument("--timeout",type=float,default=900); b.add_argument("--sample-seconds",type=float,default=1); b.add_argument("--plan-arg",action="append",default=[])
    a = sub.add_parser("agent"); a.add_argument("action", choices=("start", "inspect", "stop", "kill", "pause", "resume", "slow-replica", "network-degraded"))
    a.add_argument("--bundle", required=True); a.add_argument("--root", required=True); a.add_argument("--role", required=True); a.add_argument("--java", default="java")
    a.add_argument("--fault-seconds", type=float, default=8)
    v = sub.add_parser("verify-bundle"); v.add_argument("--bundle", required=True)
    v = sub.add_parser("verify"); v.add_argument("--result", required=True)
    m = sub.add_parser("matrix"); m.add_argument("--stage", choices=("smoke", "replication", "policies", "interaction", "fault", "coverage", "exhaustive"), required=True)
    m.add_argument("--fds"); m.add_argument("--read-selections"); m.add_argument("--protocols", help="legacy isolated-engine shortlist"); m.add_argument("--a1s"); m.add_argument("--a2s"); m.add_argument("--output")
    m.add_argument("--count-only", action="store_true"); m.add_argument("--allow-large", action="store_true"); m.add_argument("--key-mode", choices=("DEFAULT","TAGGED","ORDERED"), default="TAGGED")
    m.add_argument("--seeds", default="1,2,3"); m.add_argument("--workloads", default="UNIFORM,HOT_RANGE")
    m.add_argument("--distributions"); m.add_argument("--splits"); m.add_argument("--placements")
    m.add_argument("--offset", type=int, default=0); m.add_argument("--limit", type=int)
    args = parser.parse_args()
    if args.command == "plan": prepare(args)
    elif args.command == "agent": agent(args)
    elif args.command == "verify-bundle": print(verify_bundle(args.bundle))
    elif args.command == "verify": print(json.dumps(verify_result(args.result)))
    elif args.command == "matrix": matrix(args)
    elif args.command == "batch": batch(args)
    else:
        if args.command == "ec2" and not args.execute:
            print(json.dumps({"dry_run": True, "plan": json.loads((Path(args.plan) / "plan.json").read_text()), "message": "No SSH/AWS calls. Add --execute for dedicated existing hosts."}, ensure_ascii=False, indent=2))
            return
        controller = Controller(args, remote=args.command == "ec2")
        try:
            controller.run()
        except BaseException as e:
            save(controller.output / "RUN_FAILED.json", {"type": type(e).__name__, "reason": str(e)})
            raise


if __name__ == "__main__":
    main()
