"""Measure fenced failover on node 0 using one controller monotonic clock."""
import argparse
import json
from pathlib import Path
import signal
import struct
import subprocess
import sys
import time

from pb_bench import Client, quantile
import pb_control


def get(client, node, epoch, key=b'pre-0'):
    return client.request(node, 1, struct.pack('!QQQII', 17, 900000, epoch, 1000, len(key)) + key)


def clock_bounds(sample, node_time):
    """Map a JVM time to controller time; never subtract different nanoTime origins."""
    return (sample['beginNs'] + node_time - sample['status']['nanoTime'],
            sample['endNs'] + node_time - sample['status']['nanoTime'])


def run(a):
    root = Path(__file__).resolve().parent
    client = Client(a.hosts.split(','), client_port=a.client_port, peer_port=a.peer_port)
    primary = fault = None
    result = dict(schema='a1-a3-fenced-1', protocol='PB_FENCED', scenario=a.scenario,
                  detector=a.detector, repeat=a.repeat, ok=False, samples=[], normalSamplesNs=[],
                  storage='memory', ssdStallCovered=False,
                  failureModel='settled acknowledged SETs; GET probes during fault; one promotion',
                  clock='controller monotonic; JVM times mapped with request-roundtrip bounds')
    try:
        primary = pb_control.start()
        deadline = time.monotonic() + 30
        while True:
            try:
                states = [client.status(n) for n in range(3)]
                if (states[0].get('fenceState') == 'SERVING'
                        and all(s['epoch'] == 1 and s['failureDetector'] == a.detector for s in states)):
                    break
            except (OSError, EOFError):
                pass
            if time.monotonic() > deadline:
                raise RuntimeError('startup lease/readiness failed')
            time.sleep(.1)
        # Let startup grace end before injecting any fault.
        time.sleep(a.startup_grace + 1)
        value = b'x' * a.value
        for i in range(16):
            reply = client.set(0, i + 1, f'pre-{i}'.encode(), value)
            if reply['status'] != 0:
                raise RuntimeError('warmup failed: ' + str(reply))
        begin_states = [client.status(n) for n in range(3)]
        for i in range(a.count):
            begin = time.monotonic_ns()
            reply = client.set(0, i + 1000, f'normal-{i}'.encode(), value)
            result['normalSamplesNs'].append(time.monotonic_ns() - begin)
            if reply['status'] != 0:
                raise RuntimeError('normal SET failed')
        end_states = [client.status(n) for n in range(3)]
        result.update(statesBefore=end_states,
                      normalP50Ns=quantile(result['normalSamplesNs'], .5),
                      normalP99Ns=quantile(result['normalSamplesNs'], .99),
                      normalReplicationBytes=sum(e['sentBytes'] - b['sentBytes'] for b, e in zip(begin_states, end_states)),
                      normalFdBytes=sum(e['fdSentBytes'] - b['fdSentBytes'] for b, e in zip(begin_states, end_states)))
        calibrations = {}
        for node in (1, 2):
            begin = time.monotonic_ns()
            status = client.status(node)
            calibrations[node] = dict(beginNs=begin, endNs=time.monotonic_ns(), status=status)
        fault = subprocess.Popen([sys.executable, str(root / 'fenced_fault.py'), '--root', str(root),
                                  '--scenario', a.scenario, '--peers', a.hosts,
                                  '--peer-port', str(a.peer_port), '--failure-port', str(a.failure_port),
                                  '--duration', str(a.duration)])
        fault_file = root / 'fault.jsonl'
        deadline = time.monotonic() + 10
        while True:
            events = [json.loads(s) for s in fault_file.read_text().splitlines()] if fault_file.exists() else []
            applied = next((e for e in events if e['event'] == 'applied'), None)
            if applied:
                break
            if fault.poll() is not None or time.monotonic() > deadline:
                raise RuntimeError('fault injection failed; see fault.jsonl')
            time.sleep(.01)
        result['faultAppliedNs'] = applied['nanoTime']
        first_success = first_new = None
        epochs = {0: 1, 1: 1}
        # Bounded observation includes fault release; GET avoids divergent uncommitted writes.
        deadline = time.monotonic() + a.duration + 15
        promoted = False
        dead_observed = set()
        last_states = {}
        while time.monotonic() < deadline:
            tick = dict(atNs=time.monotonic_ns(), nodes={}, reads={})
            for node in (1, 2):
                begin = time.monotonic_ns()
                try:
                    status = client.status(node)
                    tick['nodes'][str(node)] = dict(beginNs=begin, endNs=time.monotonic_ns(), status=status)
                    last_states[node] = status
                    epochs[1] = max(epochs[1], status['epoch'])
                    if status['fdDeadNs'] >= 0:
                        dead_observed.add(node)
                    promoted |= node == 1 and status.get('fenceState') == 'SERVING' and status['epoch'] == 2
                except (OSError, EOFError) as error:
                    tick['nodes'][str(node)] = dict(error=type(error).__name__)
            for node in ((1,) if a.scenario == 'crash' else (0, 1)):
                try:
                    reply = get(client, node, epochs[node])
                    tick['reads'][str(node)] = dict(status=reply['status'], epoch=reply['epoch'], atNs=time.monotonic_ns())
                    epochs[node] = max(epochs[node], reply['epoch'])
                    if reply['status'] == 0:
                        if reply['value'] != value:
                            raise AssertionError('acknowledged value lost')
                        first_success = first_success or tick['reads'][str(node)]['atNs']
                        if node == 1 and reply['epoch'] == 2:
                            first_new = first_new or tick['reads'][str(node)]['atNs']
                except (OSError, EOFError) as error:
                    tick['reads'][str(node)] = dict(error=type(error).__name__)
            result['samples'].append(tick)
            if fault.poll() is not None and (first_new or (a.scenario != 'crash' and first_success)):
                # Continue at least one second after release to detect lasting loss of authority.
                released = [json.loads(s) for s in fault_file.read_text().splitlines()]
                released_at = next((e['nanoTime'] for e in released if e['event'] == 'released'), None)
                if released_at and time.monotonic_ns() - released_at >= 1_000_000_000:
                    break
            time.sleep(.1)
        fault.wait(timeout=5)
        if fault.returncode != 0:
            raise RuntimeError('fault worker failed or restoration failed')
        events = [json.loads(s) for s in fault_file.read_text().splitlines()]
        result['faultEvents'] = events
        result['statesAfter'] = last_states
        result['promoted'] = promoted
        result['deadObserverCount'] = len(dead_observed)
        result['falsePositiveRun'] = a.scenario in ('healthy', 'loss', 'delay') and bool(dead_observed)
        result['falsePositiveDefinition'] = 'any standby marks still-running primary DEAD in healthy/loss/delay run'
        result['firstSuccessfulReadNs'] = first_success
        result['firstNewPrimaryReadNs'] = first_new
        result['trafficRecoveryNs'] = None if first_new is None else first_new - applied['nanoTime']
        status = last_states[1]
        if status['detectedNs'] >= 0:
            lower, upper = clock_bounds(calibrations[1], status['detectedNs'])
            result['detectionLatencyBoundsNs'] = [lower - applied['nanoTime'], upper - applied['nanoTime']]
            result['detectionClockAssumption'] = 'relative monotonic drift during short run negligible; RTT bounds only'
        if promoted:
            if status['promotionVotes'] != 2 or status['readyNs'] < status['detectedNs']:
                raise AssertionError('invalid quorum or ready timing')
            result['promotionNs'] = status['readyNs'] - status['detectedNs']
            target, epoch = 1, 2
        else:
            target, epoch = 0, 1
        for i in range(16):
            reply = get(client, target, epoch, f'pre-{i}'.encode())
            if reply['status'] != 0 or reply['value'] != value:
                raise AssertionError('preserved data unavailable')
        reply = client.set(target, 100000, b'post', value, epoch)
        if reply['status'] != 0:
            raise AssertionError('post-fault SET did not recover')
        result['firstPostFaultSetNs'] = time.monotonic_ns()
        if promoted:
            result['staleEpochStatus'] = get(client, 1, 1)['status']
            if result['staleEpochStatus'] != 5:
                raise AssertionError('stale epoch accepted')
            if a.scenario != 'crash':
                old = get(client, 0, 1)
                result['oldPrimaryStatus'] = old['status']
                if old['status'] == 0:
                    raise AssertionError('old primary still serving after promotion')
        result.update(ok=True, preserved=16)
    except Exception as error:
        result['error'] = repr(error)
        raise
    finally:
        if fault is not None and fault.poll() is None:
            fault.terminate()
            fault.wait(timeout=15)
        if primary is not None and primary.poll() is None:
            # Resume before stopping a paused JVM even after an aborted measurement.
            primary.send_signal(signal.SIGCONT)
            pb_control.stop()
        client.close()
        Path(a.out).write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps({k: result.get(k) for k in ('ok', 'scenario', 'detector', 'promoted', 'falsePositiveRun', 'trafficRecoveryNs', 'promotionNs')}))


if __name__ == '__main__':
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--hosts', required=True)
    p.add_argument('--out', required=True)
    p.add_argument('--detector', choices=['SWIM', 'SWIM_LIFEGUARD'], required=True)
    p.add_argument('--scenario', choices=['healthy', 'crash', 'loss', 'delay', 'cpu-pause', 'partition'], required=True)
    p.add_argument('--duration', type=float, default=8)
    p.add_argument('--startup-grace', type=float, default=3)
    p.add_argument('--repeat', type=int, default=1)
    p.add_argument('--value', type=int, default=64)
    p.add_argument('--count', type=int, default=64)
    p.add_argument('--client-port', type=int, default=7000)
    p.add_argument('--peer-port', type=int, default=7001)
    p.add_argument('--failure-port', type=int, default=7002)
    run(p.parse_args())
