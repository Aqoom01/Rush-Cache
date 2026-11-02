"""Independent stdlib artifact checks; never connects to a replica or changes input/results."""
import argparse
import csv
import hashlib
import json
from pathlib import Path
import struct


def sha(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def check(condition, reason):
    if not condition:
        raise ValueError(reason)


def hashes(root, name):
    result = set()
    for line in (root / name).read_text(encoding='utf-8').splitlines():
        digest, name = line.split('  ', 1)
        relative = Path(name)
        target = (root / relative).resolve()
        check(not relative.is_absolute() and root.resolve() in target.parents, 'hash path escapes root')
        check(name not in result and digest == sha(target), 'hash mismatch/duplicate: ' + name)
        result.add(name)
    check(bool(result), 'empty hashes')
    return result


def trace(path, metadata, config, tag):
    data = path.read_bytes()
    check(data[:8] == b'OKTRACE1', 'trace magic')
    header = struct.unpack_from('>iqqiiiiiiidiq', data, 8)
    check(header == (1, config['seed'], config['key_count'], config['key_bytes'], config['value_bytes'],
                     config['mix']['get'], config['mix']['set'], config['mix']['delete'],
                     config['stream_count'], 0 if config['distribution'] == 'UNIFORM' else 1,
                     config['hot_probability'], tag, metadata['count']), 'trace header')
    offset = 76
    counts = [0, 0, 0]
    for index in range(metadata['count']):
        global_index, stream, client, sequence, operation = struct.unpack_from('>qiq qB'.replace(' ', ''), data, offset)
        offset += 29
        streams = config['stream_count']
        check((global_index, stream, client, sequence) == (index, index % streams, tag * streams + index % streams, index // streams), 'trace namespace/index')
        check(operation in (0, 1, 2), 'unknown operation')
        key_length, = struct.unpack_from('>i', data, offset)
        offset += 4
        check(key_length == config['key_bytes'], 'key size')
        offset += key_length
        value_length, = struct.unpack_from('>i', data, offset)
        offset += 4
        check(value_length == (config['value_bytes'] if operation == 1 else -1), 'value size')
        offset += max(0, value_length)
        counts[operation] += 1
    check(offset == len(data) == metadata['canonical_bytes'], 'trace byte count/trailing data')
    check(counts == [metadata['get'], metadata['set'], metadata['delete']], 'trace operation counts')
    check(sha(path) == metadata['sha256'], 'trace metadata hash')
    return counts


def verify(bundle, run, local_stopped=False):
    libraries = hashes(bundle, 'BUNDLE_SHA256SUMS')
    check(libraries == {'lib/' + p.name for p in (bundle / 'lib').iterdir()}, 'library hash coverage')
    inputs = hashes(run, 'INPUT_SHA256SUMS')
    summary = json.loads((run / 'pilot-summary.json').read_text())
    check((run / 'PILOT_COMPLETE').read_text().strip() == sha(run / 'pilot-summary.json'), 'pilot completion hash')
    check(summary['passed'] is True and summary['input_sha256'] == sha(run / 'INPUT_SHA256SUMS'), 'pilot summary/input')
    config = json.loads((run / 'trace-metadata.json').read_text())
    phase_counts = {}
    for tag, metadata in enumerate(config['phases']):
        phase_counts[metadata['phase']] = trace(run / metadata['file'], metadata, config, tag)
    writer = run / 'result'
    integrity = json.loads((writer / 'integrity.json').read_text())
    check(integrity['complete'] and integrity['collector_reconciled'] and not integrity['invalid_reasons'], 'writer invalid')
    for entry in integrity['files']:
        check(sha(writer / entry['path']) == entry['sha256'], 'writer file hash: ' + entry['path'])
    check(sha(writer / 'integrity.json') in (writer / 'COMPLETE').read_text(), 'writer completion hash')
    rows = list(csv.DictReader((writer / 'counters.csv').open(newline='')))
    total = 0
    for row in rows:
        op = ['GET', 'SET', 'DELETE'].index(row['operation'])
        expected = phase_counts[row['phase']][op]
        check(all(int(row[k]) == expected for k in ('considered', 'admitted', 'success')), 'request conservation')
        check(all(int(row[k]) == 0 for k in ('error_response', 'no_response', 'not_admitted', 'unresolved')), 'request failure')
        total += expected
    check(len(rows) == 9 and total == sum(p['count'] for p in config['phases']), 'phase/operation coverage')
    writes = sum(v[1] + v[2] for v in phase_counts.values())
    check(summary['workload_expected_writes'] == writes, 'write frontier')
    execution = json.loads((writer / 'execution.json').read_text())
    check(execution['passed'] and execution['workersTerminated'], 'runner failure/leak')
    drained = execution['cleanupObservation']
    check(drained['status'] == 'READY', 'cleanup not ready')
    for node in summary['replicas']:
        check(drained['evidence'][f"node_{node['node_id']}_applied"] == writes, 'replica frontier')
    audit = summary['audit']
    check(audit['passed'] and audit['replica_value_checks'] == 3 * len(summary['replicas']), 'audit replica coverage')
    check([s['status'] for s in audit['steps']] == ['MISS', 'OK', 'OK', 'OK', 'MISS', 'MISS', 'OK', 'OK'], 'audit statuses')
    check([s['applied_frontier'] for s in audit['steps']] == [writes, writes+1, writes+1, writes+2, writes+2, writes+3, writes+4, writes+4], 'audit frontiers')
    check(audit['final_applied_frontier'] == writes + 4, 'audit final frontier')
    if local_stopped:
        pids = {summary['load_pid']} | {n['pid'] for n in summary['replicas']}
        check(len(pids) == len(summary['replicas']) + 1, 'processes were not separate')
        for node in summary['replicas']:
            check((run / f"state/node-{node['node_id']}.stopped").read_text().strip() == 'closed_after_stop_file', 'node close not confirmed')
    return {'passed': True, 'requests': total, 'workload_writes': writes, 'audit_requests': 8,
            'replica_value_checks': audit['replica_value_checks'], 'input_files': len(inputs),
            'bundle_libraries': len(libraries), 'local_stopped_checked': local_stopped}


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('bundle', type=Path)
    parser.add_argument('run', type=Path)
    parser.add_argument('--local-stopped', action='store_true')
    arguments = parser.parse_args()
    print(json.dumps(verify(arguments.bundle, arguments.run, arguments.local_stopped), indent=2))
