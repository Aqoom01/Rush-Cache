"""마무리 검증: bounded write windows, client-inflight crash, timed clean CRAQ reads.

No production protocol changes. One connection/client per worker. Retry latency
includes all attempts; raw attempt status counts are retained. Results are never
called steady-state write throughput: the engines retain unbounded logs.
"""
import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import signal
import struct
import threading
import time

from pb_bench import Client, quantile
from election_bench import discover
from chain_bench import discover as discover_chain
from craq_bench import read_follow
import pb_control


class WorkerClient(Client):
    def __init__(self, hosts, identity, on_sent=None):
        super().__init__(hosts, on_sent)
        self.identity = identity

    def set(self, node, operation, key, value, epoch=1, deadline=1000):
        body = struct.pack('!QQQII', self.identity, operation, epoch, deadline, len(key))
        return self.request(node, 2, body + key + struct.pack('!I', len(value)) + value)


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def payload(operation, size):
    return struct.pack('!Q', operation) + bytes((i * 17 + 3) % 256 for i in range(size - 8))


def write_budget(value):
    # Conservative budget for full view-change snapshots; includes warmup + retries.
    return 128 if value > 64 else 1024


def retry_set(client, route, epoch, operation, key, value, budget=15, excluded=()):
    started = time.perf_counter_ns()
    until = time.monotonic() + budget
    attempts = []
    while time.monotonic() < until:
        row = {'node': route, 'epoch': epoch, 'startNs': time.perf_counter_ns()}
        try:
            reply = client.set(route, operation, key, value, epoch)
            row.update(status=reply['status'], responseEpoch=reply['epoch'])
            epoch = max(epoch, reply['epoch'])
            if reply['status'] == 0:
                row['endNs'] = time.perf_counter_ns()
                attempts.append(row)
                return {'startNs': started, 'endNs': row['endNs'], 'attempts': attempts,
                        'node': route, 'epoch': epoch, 'operation': operation,
                        'latencyNs': row['endNs'] - started}
            require(reply['status'] in (4, 5, 6, 7, 8), f"unexpected SET status {reply['status']}")
            if reply['detail'].startswith('redirect-node='):
                route = int(reply['detail'].split('=')[1])
        except (OSError, EOFError) as error:
            row.update(status='CLIENT_FAILURE', error=type(error).__name__)
            route = (route + 1) % 3
        row['endNs'] = time.perf_counter_ns()
        attempts.append(row)
        while route in excluded:
            route = (route + 1) % 3
        time.sleep(.01)
    raise RuntimeError(f'SET retry budget exhausted: op={operation}, attempts={attempts[-4:]}')


def audit(client, nodes, records, size, budget=10):
    until = time.monotonic() + budget
    while True:
        missing = []
        for node in nodes:
            for record in records:
                key = record['key'].encode()
                response = client.local(node, key)
                if response['status'] != 0 or response['value'] != payload(record['operation'], size):
                    missing.append([node, record['key']])
        if not missing:
            return len(nodes) * len(records)
        require(time.monotonic() < until, f'acknowledged values missing/mismatched: {missing[:8]}')
        time.sleep(.02)


def summarize(records, elapsed):
    samples = [r['latencyNs'] for r in records]
    statuses = Counter(str(a['status']) for r in records for a in r['attempts'])
    return {'logicalOk': len(records), 'elapsedNs': elapsed,
            'okOpsPerSecond': len(records) * 1e9 / elapsed,
            'p50Ns': quantile(samples, .5), 'p95Ns': quantile(samples, .95),
            'p99Ns': quantile(samples, .99), 'attemptStatusCounts': dict(statuses),
            'attemptCount': sum(statuses.values()), 'samplesNs': samples}


def run_writes(args, probe, leader, epoch):
    total = args.count or write_budget(args.value)
    require(total <= write_budget(args.value), 'write count exceeds bounded snapshot budget')
    gate = threading.Barrier(args.clients + 1)

    def worker(index):
        client = WorkerClient(args.hosts.split(','), 100 + index)
        records = []
        try:
            gate.wait(timeout=15)
            for number in range(index, total, args.clients):
                operation = 10000 + number
                key = f'measure-{operation}'
                row = retry_set(client, leader, epoch, operation, key.encode(), payload(operation, args.value))
                require(row['node'] == leader and row['epoch'] == epoch, 'unexpected leadership change during normal load')
                row['key'] = key
                records.append(row)
            return records
        finally:
            client.close()

    before = [probe.status(i) for i in range(3)]
    with ThreadPoolExecutor(max_workers=args.clients) as pool:
        futures = [pool.submit(worker, i) for i in range(args.clients)]
        begin = time.perf_counter_ns()
        gate.wait(timeout=15)
        records = [row for future in futures for row in future.result()]
        end = time.perf_counter_ns()
    copies = audit(probe, range(3), records, args.value)
    after = [probe.status(i) for i in range(3)]
    return {'measurement': summarize(records, end - begin), 'records': records,
            'verifiedCopies': copies, 'statesBefore': before, 'statesAfter': after,
            'peerBytes': sum(s['sentBytes'] for s in after) - sum(s['sentBytes'] for s in before),
            'scope': 'fresh-cluster bounded write window; not continuous steady-state throughput'}


def run_crash(args, probe, leader, epoch, kill):
    records = []
    fault = {}
    count = 32
    client = WorkerClient(args.hosts.split(','), 200)
    survivors = [i for i in range(3) if i != leader]
    try:
        for number in range(count):
            operation = 10000 + number
            key = f'crash-{operation}'
            row = retry_set(client, leader, epoch, operation, key.encode(), payload(operation, args.value))
            row['key'] = key
            records.append(row)

        def sent(node, opcode, peer, sequence):
            if not peer and opcode == 2 and not fault:
                require(node == leader, 'crash candidate sent to wrong leader')
                fault['sentNs'] = time.perf_counter_ns()
                fault['callBeginNs'] = time.perf_counter_ns()
                kill(leader)
                fault['callReturnNs'] = time.perf_counter_ns()

        client.on_sent = sent
        candidate = 10000 + count
        for operation in range(candidate, candidate + 17):
            key = f'crash-{operation}'
            row = retry_set(client, leader, epoch, operation, key.encode(), payload(operation, args.value), excluded=(leader,))
            row['key'] = key
            records.append(row)
            if row['node'] in survivors and row['epoch'] > epoch:
                fault.setdefault('firstNewLeaderOkNs', row['endNs'])
                leader_route = row['node']
                # Keep original epoch/leader for fault classification; routing follows reply below.
                client.on_sent = None
            else:
                leader_route = leader
            # Remaining requests use the new leader after discovery; retry keeps logical identity.
            if 'firstNewLeaderOkNs' in fault:
                break
        require('firstNewLeaderOkNs' in fault, 'no newer-epoch success from survivor')
        new_epoch = records[-1]['epoch']
        for operation in range(records[-1]['operation'] + 1, candidate + 17):
            key = f'crash-{operation}'
            row = retry_set(client, leader_route, new_epoch, operation, key.encode(), payload(operation, args.value), excluded=(leader,))
            row['key'] = key
            records.append(row)
        copies = audit(probe, survivors, records, args.value)
        return {'records': records, 'fault': fault,
                'failoverNs': fault['firstNewLeaderOkNs'] - fault['callBeginNs'],
                'verifiedCopies': copies, 'preservedSuccesses': len(records),
                'statesAfter': [probe.status(i) for i in survivors],
                'scope': 'kill after client sendall before response read; internal commit boundary NOT forced'}
    finally:
        client.close()


def run_reads(args, probe):
    # One read-only dataset. No read log growth for Chain/CRAQ. Tail direct is a separate baseline.
    keys = [f'warm-{i}'.encode() for i in range(16)]
    def phase(seconds, save):
        gate = threading.Barrier(args.clients + 1)
        stop = threading.Event()
        def reader(index):
            client = WorkerClient(args.hosts.split(','), 300 + index)
            samples = []
            redirects = 0
            count = 0
            try:
                gate.wait(timeout=15)
                while not stop.is_set():
                    node = 2 if args.read_target == 'tail' else count % 3
                    key_index = (count + index) % 16
                    response, hops, elapsed = read_follow(client, node, keys[key_index], 1)
                    require(response['status'] == 0 and response['value'] == payload(key_index, args.value), 'clean read mismatch')
                    count += 1
                    redirects += hops - 1
                    if save:
                        samples.append(elapsed)
                return {'count': count, 'samplesNs': samples, 'redirects': redirects}
            finally:
                client.close()
        with ThreadPoolExecutor(max_workers=args.clients) as pool:
            futures = [pool.submit(reader, i) for i in range(args.clients)]
            begin = time.perf_counter_ns()
            try:
                gate.wait(timeout=15)
                stop.wait(seconds)
            finally:
                stop.set()
            output = [f.result() for f in futures]
            elapsed = time.perf_counter_ns() - begin
        return output, elapsed
    phase(args.warmup_seconds, False)
    before = [probe.status(i) for i in range(3)]
    rows, elapsed = phase(args.seconds, True)
    samples = [s for r in rows for s in r['samplesNs']]
    return {'measurement': {'logicalOk': len(samples), 'elapsedNs': elapsed,
            'okOpsPerSecond': len(samples) * 1e9 / elapsed,
            'p50Ns': quantile(samples, .5), 'p95Ns': quantile(samples, .95),
            'p99Ns': quantile(samples, .99), 'samplesNs': samples,
            'redirects': sum(r['redirects'] for r in rows)},
            'perClientCounts': [r['count'] for r in rows], 'statesBefore': before,
            'statesAfter': [probe.status(i) for i in range(3)],
            'scope': 'read-only uniform warm keys; single generator host; no mixed-load linearizability claim'}


def run(args, kill=None):
    result = {'schema': 'a1-final-validation-1', 'ok': False,
              'config': {k: v for k, v in vars(args).items() if k not in ('hosts', 'out')},
              'timeDomain': 'wall-clock', 'durationUnit': 'ns',
              'clientPlacement': 'original-leader-host' if args.scenario == 'crash' else 'node2-host (may share with leader); fixed across variants',
              'utcEpochMs': int(time.time() * 1000)}
    probe = WorkerClient(args.hosts.split(','), 1)
    warm = WorkerClient(args.hosts.split(','), 2)
    try:
        if args.protocol == 'CHAIN':
            states = discover_chain(probe, args.read_mode)
            leader, epoch = 0, 1
        else:
            leader, epoch, states = discover(probe, args.protocol)
        if args.protocol == 'VR':
            require(all(s['vrMode'] == args.vr_mode for s in states), 'VR mode mismatch')
        result['statesInitial'] = states
        result['originalLeader'] = leader
        for i in range(16):
            retry_set(warm, leader, epoch, i, f'warm-{i}'.encode(), payload(i, args.value))
        # Warmup reads/pings for a time budget without growing retained write logs.
        until = time.monotonic() + args.warmup_seconds
        while time.monotonic() < until:
            for i in range(3):
                probe.local(i, b'warm-0')
            time.sleep(.01)
        if args.scenario == 'write':
            result.update(run_writes(args, probe, leader, epoch))
        elif args.scenario == 'crash':
            require(kill is not None, 'crash requires an explicit scoped controller')
            result.update(run_crash(args, probe, leader, epoch, kill))
        else:
            result.update(run_reads(args, probe))
        result['ok'] = True
    except Exception as error:
        result['error'] = repr(error)
        raise
    finally:
        probe.close()
        warm.close()
        Path(args.out).write_text(json.dumps(result, indent=2) + '\n', encoding='utf-8')
    return result


def parser():
    p = argparse.ArgumentParser()
    p.add_argument('--hosts', required=True)
    p.add_argument('--out', required=True)
    p.add_argument('--scenario', choices=['write', 'crash', 'read'], required=True)
    p.add_argument('--protocol', choices=['PB', 'VR', 'RAFT', 'CHAIN'], required=True)
    p.add_argument('--vr-mode', default='INCREMENTAL')
    p.add_argument('--read-mode', default='TAIL_ONLY')
    p.add_argument('--read-target', choices=['tail', 'spread'], default='spread')
    p.add_argument('--value', type=int, choices=[64, 8192], default=64)
    p.add_argument('--clients', type=int, choices=[1, 4, 8], default=1)
    p.add_argument('--count', type=int, default=0)
    p.add_argument('--seconds', type=float, default=30)
    p.add_argument('--warmup-seconds', type=float, default=5)
    p.add_argument('--repeat', type=int, default=1)
    p.add_argument('--timeout', type=int, default=500)
    p.add_argument('--controller', type=int, default=2)
    return p


if __name__ == '__main__':
    args = parser().parse_args()
    def kill(node):
        require(node == args.controller, 'controller must run on crash target host')
        pid = int(pb_control.PID.read_text())
        require(pb_control.alive(pid), 'dedicated JVM identity check failed')
        os.kill(pid, signal.SIGKILL)
    result = run(args, kill)
    print(json.dumps({'ok': result['ok'], 'scenario': args.scenario,
                      'measurement': {k: v for k, v in result.get('measurement', {}).items() if k != 'samplesNs'},
                      'failoverNs': result.get('failoverNs')}))
