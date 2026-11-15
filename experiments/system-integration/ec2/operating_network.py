#!/usr/bin/python3 -I
"""Bounded egress impairment on a dedicated experiment replica."""

import argparse
from ipaddress import IPv4Address, IPv4Network
import json
import os
import signal
import socket
import subprocess
import time


def command(*args):
    return subprocess.check_output(args, text=True, timeout=10)


def impair(action, peers, gateway, port, duration):
    if not 0 < duration <= 30 or action not in ("slow-replica", "network-degraded"):
        raise ValueError("invalid network impairment")

    def tc(*args):
        prefix = ("tc",) if os.geteuid() == 0 else ("sudo", "-n", "tc")
        return command(*prefix, *args)

    def interrupted(*_):
        raise InterruptedError("network fault interrupted")

    slow = action == "slow-replica"
    targets = [socket.gethostbyname(host) for host in ([gateway] if slow else peers)]
    routes = [json.loads(command("ip", "-j", "route", "get", target))[0]["dev"] for target in targets]
    iface = routes[0]
    if iface == "lo" or any(route != iface for route in routes):
        raise ValueError("experiment traffic must use one non-loopback interface")
    root = "71:"
    netem = "73:"

    def qdiscs():
        return json.loads(tc("-j", "-s", "qdisc", "show", "dev", iface))

    if any(q.get("root") and (q.get("kind") not in ("mq", "fq", "fq_codel", "pfifo_fast", "noqueue")
                             or q.get("handle") != "0:") for q in qdiscs()):
        raise RuntimeError("refusing to replace a configured root qdisc")
    signals = [signal.SIGTERM, signal.SIGINT]
    if hasattr(signal, "SIGHUP"):
        signals.append(signal.SIGHUP)
    handlers = {sig: signal.signal(sig, interrupted) for sig in signals}
    installed = False
    try:
        tc("qdisc", "replace", "dev", iface, "root", "handle", root, "prio", "bands", "3", "priomap", *(["0"] * 16))
        installed = True
        settings = ("delay", "80ms") if slow else ("delay", "250ms", "loss", "30%")
        tc("qdisc", "add", "dev", iface, "parent", root + "3", "handle", netem, "netem", *settings)
        # Match only experiment ports. SSH and administrative HTTP are untouched.
        filters = [("6", "sport", port)] if slow else [
            ("17", "dport", port + 1), ("17", "dport", port + 10),
            *(("6", field, tcp_port) for field in ("sport", "dport")
              for tcp_port in (port, port + 1, port + 8, port + 9, port + 10)),
        ]
        for target in targets:
            for protocol, field, matched_port in filters:
                tc("filter", "add", "dev", iface, "protocol", "ip", "parent", root, "prio", "1", "u32",
                   "match", "ip", "dst", target + "/32", "match", "ip", "protocol", protocol, "0xff",
                   "match", "ip", field, str(matched_port), "0xffff", "flowid", root + "3")
        started = time.monotonic_ns()
        time.sleep(duration)
        ended = time.monotonic_ns()
        stats = next(q for q in qdiscs() if q.get("handle") == netem)
    finally:
        try:
            if installed and any(q.get("root") and q.get("handle") == root for q in qdiscs()):
                tc("qdisc", "del", "dev", iface, "root", "handle", root)
        finally:
            for sig, handler in handlers.items():
                signal.signal(sig, handler)
    return {"released": not any(q.get("root") and q.get("handle") == root for q in qdiscs()),
            "targets": targets, "filters": filters, "settings": settings, "stats": stats,
            "duration_ns": ended - started}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("slow-replica", "network-degraded"))
    parser.add_argument("--peers", nargs=2, type=IPv4Address, required=True)
    parser.add_argument("--gateway", type=IPv4Address, required=True)
    parser.add_argument("--duration", type=float, required=True)
    args = parser.parse_args()
    private = tuple(map(IPv4Network, ("10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16")))
    if len({*args.peers, args.gateway}) != 3 or any(not any(host in net for net in private) for host in (*args.peers, args.gateway)):
        parser.error("expected three distinct private experiment IPv4 addresses")
    print(json.dumps(impair(args.action, list(map(str, args.peers)), str(args.gateway), 27101, args.duration)))


if __name__ == "__main__":
    main()
