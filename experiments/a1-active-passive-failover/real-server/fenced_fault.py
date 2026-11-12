"""Bounded faults for the dedicated PB JVM; never targets a raw disk or another service."""
import argparse
import json
import os
from pathlib import Path
import signal
import subprocess
import time


def command(*args):
    return subprocess.check_output(args, text=True, timeout=10)


def apply(args):
    root = Path(args.root).resolve()
    pid = int((root / 'pb.pid').read_text())
    cmdline = Path(f'/proc/{pid}/cmdline').read_bytes()
    if (b'io.openkache.server.replication.PbServerMain' not in cmdline
            or str(root / 'node.properties').encode() not in cmdline):
        raise RuntimeError('PID is not the dedicated experiment JVM')
    events = root / 'fault.jsonl'
    def event(kind, **extra):
        with events.open('a') as out:
            out.write(json.dumps(dict(event=kind, scenario=args.scenario,
                                      nanoTime=time.monotonic_ns(), **extra)) + '\n')
    def interrupted(*_):
        raise InterruptedError('fault interrupted; restoring')
    signal.signal(signal.SIGTERM, interrupted)
    signal.signal(signal.SIGINT, interrupted)
    iface = None
    owned_qdisc = False
    paused = False
    try:
        event('requested')
        if args.scenario == 'crash':
            os.kill(pid, signal.SIGKILL)
        elif args.scenario == 'cpu-pause':
            os.kill(pid, signal.SIGSTOP)
            paused = True
        elif args.scenario in ('loss', 'delay', 'partition'):
            peers = args.peers.split(',')
            route = json.loads(command('ip', '-j', 'route', 'get', peers[1]))
            iface = route[0]['dev']
            qdiscs = json.loads(command('sudo', '-n', 'tc', '-j', 'qdisc', 'show', 'dev', iface))
            # Do not replace administrator- or experiment-owned traffic control.
            if any(q.get('root') and (q.get('kind') not in ('mq', 'fq_codel', 'noqueue')
                                     or q.get('handle') != '0:') for q in qdiscs):
                raise RuntimeError('existing root qdisc; refusing to replace it')
            command('sudo', '-n', 'tc', 'qdisc', 'replace', 'dev', iface,
                    'root', 'handle', '71:', 'prio', 'bands', '3',
                    'priomap', *(['0'] * 16))
            owned_qdisc = True
            impairment = (['delay', '250ms', '50ms'] if args.scenario == 'delay'
                          else ['loss', '100%' if args.scenario == 'partition' else '30%'])
            command('sudo', '-n', 'tc', 'qdisc', 'add', 'dev', iface, 'parent', '71:3',
                    'handle', '73:', 'netem', *impairment)
            priority = 1
            for peer in peers[1:]:
                ports = [(17, args.failure_port)]
                if args.scenario == 'partition':
                    ports.append((6, args.peer_port))
                for protocol, port in ports:
                    command('sudo', '-n', 'tc', 'filter', 'add', 'dev', iface,
                            'protocol', 'ip', 'parent', '71:', 'prio', str(priority), 'u32',
                            'match', 'ip', 'dst', peer + '/32', 'match', 'ip', 'protocol',
                            str(protocol), '0xff', 'match', 'ip', 'dport', str(port),
                            '0xffff', 'flowid', '71:3')
                    priority += 1
            event('netem', evidence=command('sudo', '-n', 'tc', '-s', 'qdisc', 'show', 'dev', iface))
        elif args.scenario != 'healthy':
            raise ValueError('unsupported fault')
        event('applied')
        if args.scenario != 'crash':
            time.sleep(args.duration)
    except Exception as error:
        event('error', detail=repr(error))
        raise
    finally:
        if paused:
            try:
                os.kill(pid, signal.SIGCONT)
            except ProcessLookupError:
                pass
        if owned_qdisc:
            # Only delete the qdisc that this worker installed.
            state = json.loads(command('sudo', '-n', 'tc', '-j', 'qdisc', 'show', 'dev', iface))
            if any(q.get('root') and q.get('handle') == '71:' for q in state):
                command('sudo', '-n', 'tc', 'qdisc', 'del', 'dev', iface, 'root', 'handle', '71:')
        event('released')


if __name__ == '__main__':
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--root', required=True)
    p.add_argument('--scenario', choices=['healthy', 'crash', 'loss', 'delay', 'cpu-pause', 'partition'], required=True)
    p.add_argument('--peers', required=True)
    p.add_argument('--failure-port', type=int, default=7002)
    p.add_argument('--peer-port', type=int, default=7001)
    p.add_argument('--duration', type=float, default=8)
    a = p.parse_args()
    if not 0 < a.duration <= 60:
        p.error('duration must be in (0, 60] seconds')
    apply(a)
