"""Read-only stdlib checks of distributed pilot artifacts and canonical stream partitions."""
import argparse
from collections import Counter
import hashlib
import importlib.util
import json
from pathlib import Path
import struct
import sys

sys.dont_write_bytecode = True

_spec = importlib.util.spec_from_file_location('single_pilot_verify', Path(__file__).with_name('verify.py'))
_single = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_single)
sha, check, hashes = _single.sha, _single.check, _single.hashes
OPERATIONS = ('GET', 'SET', 'DELETE')
KINDS = ('SERVICE', 'SCHEDULED', 'DISPATCH_LAG')
BOUNDS = {0}
_bound = 1
while _bound <= 60_000_000_000:
    BOUNDS.add(_bound)
    _bound += max(1, (_bound + 99) // 100)


def partitions(path, tag, total):
    data = path.read_bytes()
    digests = [hashlib.sha256(b'IONIA-PARTITION-1' + struct.pack('>iiqi', tag, slot, total, 2)) for slot in range(2)]
    counts = [[0, 0, 0], [0, 0, 0]]
    offset = 76
    for expected_index in range(total):
        start = offset
        index, stream, client, sequence, operation = struct.unpack_from('>qiqqB', data, offset)
        check((index, stream, client, sequence) == (expected_index, expected_index % 2, tag * 2 + expected_index % 2, expected_index // 2), 'partition identity')
        offset += 29
        key_size, = struct.unpack_from('>i', data, offset)
        offset += 4 + key_size
        value_size, = struct.unpack_from('>i', data, offset)
        offset += 4 + max(0, value_size)
        counts[stream][operation] += 1
        digests[stream].update(data[start:offset])
    check(offset == len(data), 'partition trailing data')
    return [{'requests': sum(c), 'gets': c[0], 'sets': c[1], 'deletes': c[2], 'sha256': d.hexdigest()} for c, d in zip(counts, digests)]


def histogram(h, expected):
    bins = {b['upper_ns']: b['count'] for b in h['buckets']}
    check(len(bins) == len(h['buckets']) and all(k in BOUNDS and v > 0 for k, v in bins.items()), 'histogram bins')
    n = h['count']
    check(n == expected and n > 0 and sum(bins.values()) + h['overflow'] == n, 'histogram count')
    check(h['bin_version'] == 'ns-ceil-1pct-v1' and h['highest_finite_ns'] == max(BOUNDS), 'histogram version/range')
    check(0 <= h['minimum_ns'] <= h['maximum_ns'] and h['minimum_ns'] * n <= h['sum_ns'] <= h['maximum_ns'] * n, 'histogram moments')
    check(abs(h['mean_ns'] - h['sum_ns'] / n) < 1e-6, 'histogram mean')
    for name, numerator, denominator in [('p50', 50, 100), ('p95', 95, 100), ('p99', 99, 100), ('p999', 999, 1000)]:
        rank = (numerator * n + denominator - 1) // denominator
        seen = 0
        expected_bound = None
        for bound, count in sorted(bins.items()):
            seen += count
            if seen >= rank:
                expected_bound = bound
                break
        check(h[name]['upperBoundNanos'] == expected_bound, 'histogram quantile')
        check(h[name]['comparisonEligible'] == (expected_bound is not None and (name != 'p999' or n >= 100000)), 'quantile eligibility')
    return bins


def series(rows, op_counts):
    result = {}
    for h in rows:
        key = h['operation'], h['kind']
        check(key not in result and key[0] in OPERATIONS and key[1] in KINDS, 'histogram identity/duplicate')
        histogram(h, op_counts[OPERATIONS.index(key[0])])
        result[key] = h
    check(set(result) == {(op, kind) for i, op in enumerate(OPERATIONS) if op_counts[i] for kind in KINDS}, 'histogram coverage')
    return result


def verify(bundle, run, local_stopped=False):
    libraries = hashes(bundle, 'BUNDLE_SHA256SUMS')
    check(libraries == {'lib/' + p.name for p in (bundle / 'lib').iterdir()}, 'bundle coverage')
    hashes(run, 'INPUT_SHA256SUMS')
    check(hashes(run, 'DISTRIBUTED_SHA256SUMS') == {'distributed.properties', 'manifest.json', 'INPUT_SHA256SUMS'}, 'contract coverage')
    check((run / 'DISTRIBUTED_PREPARED').read_text(encoding='utf-8').strip() == sha(run / 'DISTRIBUTED_SHA256SUMS'), 'prepared marker')
    expected_files = {'distributed-summary.json', 'collected-load-0.json', 'collected-load-1.json'} | {'distributed-phase-' + p + '.json' for p in ('load', 'warmup', 'measure')}
    check(hashes(run, 'DISTRIBUTED_RESULT_SHA256SUMS') == expected_files, 'result coverage')
    check((run / 'DISTRIBUTED_COMPLETE').read_text(encoding='utf-8').strip() == sha(run / 'DISTRIBUTED_RESULT_SHA256SUMS'), 'completion marker')
    summary = json.loads((run / 'distributed-summary.json').read_text(encoding='utf-8'))
    check(summary['passed'] and summary['schema_version'] == 'ionia-distributed-pilot-v1', 'summary status/schema')
    check(summary['contract_sha256'] == sha(run / 'DISTRIBUTED_SHA256SUMS') and summary['input_sha256'] == sha(run / 'INPUT_SHA256SUMS') and summary['bundle_sha256'] == sha(bundle / 'BUNDLE_SHA256SUMS'), 'summary hashes')
    check([l['slot'] for l in summary['loads']] == [0, 1] and len({l['instance_id'] for l in summary['loads']}) == 2, 'load identities')
    check(all(l['closed_idle'] for l in summary['loads']), 'loads not closed')
    replicas = summary['replicas']
    check(len(replicas) in (3, 5) and {r['node_id'] for r in replicas} == set(range(1, len(replicas) + 1)), 'replica membership')
    config = json.loads((run / 'trace-metadata.json').read_text(encoding='utf-8'))
    check(config['stream_count'] == 2, 'stream contract')
    load_files = []
    for slot in range(2):
        path = run / f'collected-load-{slot}.json'
        load = json.loads(path.read_text(encoding='utf-8'))
        identity = summary['loads'][slot]
        check(sha(path) == identity['result_sha256'] and load['slot'] == slot and load['identity']['pid'] == identity['pid'] and load['identity']['instance_id'] == identity['instance_id'], 'load payload identity/hash')
        check(load['run_id'] == summary['run_id'] and load['contract_sha256'] == summary['contract_sha256'], 'load run/contract')
        check(load['closed_idle']['closed'] and load['closed_idle']['close_error'] == '' and all(load['closed_idle'][k] == 0 for k in ('pending', 'raw', 'active', 'occupied')), 'load cleanup')
        load_files.append(load)
    writes = requests = 0
    for tag, phase in enumerate(config['phases']):
        _single.trace(run / phase['file'], phase, config, tag)
        expected = partitions(run / phase['file'], tag, phase['count'])
        row = summary['phases'][tag]
        check(row['phase'] == phase['phase'] and row['requests'] == phase['count'], 'phase request count/order')
        check(row == json.loads((run / f"distributed-phase-{phase['phase'].lower()}.json").read_text(encoding='utf-8')), 'phase artifact differs')
        parts = []
        for slot in range(2):
            report = row['loads'][slot]
            check(report['slot'] == slot and report['phase'] == phase['phase'] and report['partition'] == expected[slot], 'partition digest/count')
            check(report == load_files[slot]['phases'][tag], 'load phase differs from controller')
            check(report['duration_ns'] > 0 and report['attempts'] >= expected[slot]['requests'] and 0 <= report['retried'] <= expected[slot]['requests'], 'attempt/duration accounting')
            parts.append(series(report['histograms'], [expected[slot]['gets'], expected[slot]['sets'], expected[slot]['deletes']]))
        merged = series(row['merged_histograms'], [phase['get'], phase['set'], phase['delete']])
        for key, h in merged.items():
            pieces = [part[key] for part in parts if key in part]
            bins = Counter()
            for piece in pieces:
                bins.update({b['upper_ns']: b['count'] for b in piece['buckets']})
            check(dict(bins) == {b['upper_ns']: b['count'] for b in h['buckets']} and h['sum_ns'] == sum(v['sum_ns'] for v in pieces) and h['overflow'] == sum(v['overflow'] for v in pieces), 'histogram merge differs')
            check(h['minimum_ns'] == min(v['minimum_ns'] for v in pieces) and h['maximum_ns'] == max(v['maximum_ns'] for v in pieces), 'merged extrema')
        check(row['controller_window_ns'] > 0 and abs(row['controller_completion_window_ops_per_second'] - phase['count'] * 1e9 / row['controller_window_ns']) < 1e-6, 'controller window rate')
        writes += phase['set'] + phase['delete']
        requests += phase['count']
        check(row['expected_writes'] == writes and row['drain']['status'] == 'READY', 'global drain')
        for replica in replicas:
            node = replica['node_id']
            check(row['drain']['evidence'][f'node_{node}_applied'] == writes and row['drain']['evidence'][f'node_{node}_view'] == 0, 'replica apply/view')
    check(len(summary['phases']) == 3 and summary['workload_writes'] == writes and summary['cleanup']['status'] == 'READY', 'final accounting')
    audit = summary['audit']
    check(audit['passed'] and audit['replica_value_checks'] == len(replicas) * 3 and audit['final_applied_frontier'] == writes + 4, 'audit coverage/frontier')
    check([s['status'] for s in audit['steps']] == ['MISS', 'OK', 'OK', 'OK', 'MISS', 'MISS', 'OK', 'OK'], 'audit statuses')
    check([s['applied_frontier'] for s in audit['steps']] == [writes, writes+1, writes+1, writes+2, writes+2, writes+3, writes+4, writes+4], 'audit apply sequence')
    if local_stopped:
        pids = [summary['controller_pid']] + [l['pid'] for l in summary['loads']] + [r['pid'] for r in replicas]
        check(len(set(pids)) == len(replicas) + 3, 'distinct local JVMs')
        for slot in range(2):
            check((run / f'LOAD_{slot}_COMPLETE').read_text(encoding='utf-8').strip() == sha(run / f'load-{slot}-result.json'), 'local load complete')
        for replica in replicas:
            check((run / f"state/node-{replica['node_id']}.stopped").read_text(encoding='utf-8').strip() == 'closed_after_stop_file', 'local node close')
    return {'passed': True, 'requests': requests, 'replicas': len(replicas), 'loads': 2, 'controllers': 1, 'writes': writes, 'audit_requests': 8, 'replica_value_checks': len(replicas) * 3, 'local_stopped_checked': local_stopped}


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('bundle', type=Path)
    parser.add_argument('run', type=Path)
    parser.add_argument('--local-stopped', action='store_true')
    args = parser.parse_args()
    print(json.dumps(verify(args.bundle, args.run, args.local_stopped), indent=2))
