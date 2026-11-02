"""Independent stdlib verifier for P02 JVM sidecar bundles; no server/network calls.

COMPLETE certifies storage, not a healthy service. Invalid diagnostic bundles can be
structurally verified; valid bundles additionally require every semantic invariant.
Hashes detect accidental changes; they are not signatures or protection from an attacker.
"""
import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path

PERIOD = 1_000_000_000
OPS = {'GET', 'SET', 'DELETE'}
PHASES = {'LOAD', 'WARMUP', 'MEASURE'}
STAGES = ['PREFLIGHT', 'LOAD', 'LOAD_DRAIN', 'WARMUP', 'WARMUP_DRAIN', 'MEASURE', 'FINAL_DRAIN', 'CLEANUP']
CUMULATIVE = {'process_cpu_ns', 'gc_count', 'gc_time_ns'}
KEYS = CUMULATIVE | {'heap_used_bytes', 'heap_committed_bytes', 'heap_max_bytes',
    'non_heap_used_bytes', 'non_heap_committed_bytes', 'non_heap_max_bytes',
    'rss_bytes', 'allocation_bytes', 'gc_pause_ns'}
FILES = {'samples.jsonl', 'request-windows.jsonl', 'execution.json', 'jvm-result.json'}


def require(condition, message):
    if not condition:
        raise ValueError(message)


def load(path):
    return json.loads(path.read_text(encoding='utf-8'))


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def value(v, key):
    require(v['unit'] == ('count' if key == 'gc_count' else 'bytes' if key.endswith('_bytes') else 'ns'), 'value unit')
    if v['status'] == 'measured':
        require(type(v['value']) is int and v['value'] >= 0 and v['reason'] is None, 'measured value')
    else:
        require(v['status'] == 'unsupported' and v['value'] is None and isinstance(v['reason'], str) and v['reason'].strip(), 'unsupported null/reason')


def counts(c):
    require(set(c['byOperation']) == OPS, 'client operation coverage')
    total = Counter()
    for op, row in c['byOperation'].items():
        require(all(type(n) is int and n >= 0 for n in row.values()), 'client negative/noninteger')
        require(row['admitted'] == row['success'] + row['errorResponse'] + row['noResponse'] + row['pending'], 'client conservation')
        total.update(row)
    return total


def verify(directory):
    root = Path(directory)
    require((root / 'STARTED').is_file() and (root / 'COMPLETE').is_file(), 'incomplete bundle')
    require((root / 'COMPLETE').read_text(encoding='utf-8').strip() == 'integrity_sha256=' + digest(root / 'integrity.json'), 'integrity digest')
    integrity = load(root / 'integrity.json')
    require(integrity['schema_version'] == 'benchmark-jvm-integrity-v1' and integrity['complete'] is True, 'integrity schema')
    records = integrity['files']
    require(len(records) == 4 and {r['path'] for r in records} == FILES, 'file coverage/duplicate/path')
    for row in records:
        require(digest(root / row['path']) == row['sha256'], 'file digest: ' + row['path'])
    report, execution = load(root / 'jvm-result.json'), load(root / 'execution.json')
    samples = [json.loads(line) for line in (root / 'samples.jsonl').read_text(encoding='utf-8').splitlines()]
    windows = [json.loads(line) for line in (root / 'request-windows.jsonl').read_text(encoding='utf-8').splitlines()]
    require(report['schema_version'] == 'benchmark-jvm-observation-v1', 'report schema')
    require(report['observation_token'] == integrity['observation_token'], 'observation token')
    require(report['validity'] == integrity['validity'] and report['validity'] in {'valid', 'invalid'}, 'validity agreement')
    valid = report['validity'] == 'valid'
    require((not report['issues']) if valid else bool(report['issues']), 'issues/validity')
    require(report['sample_count'] == len(samples) and report['request_window_count'] == len(windows), 'record counts')
    require(report['nominal_period_ns'] == PERIOD and report['true_peak_pending'] is None and report['peak_method'] == 'sampled_lower_bound', 'period/peak semantics')
    require(report['resource_per_op_cost'] is None and report['standard_result_M18_M23'] == 'unsupported_until_declared_replica_coverage_and_profile_integration', 'scope overclaim')
    require(report['request_window_method'] == 'raw_terminal_ns_global_run_anchor_cohort_phase_separate', 'request method')
    require(report['resource_window_method'] == 'actual_snapshot_intervals_stage_work_drain_cleanup_separate', 'resource method')
    settings, result, plan = report['settings'], execution['result'], execution['plan']
    require(settings['runId'] == plan['runId'] == report['run_id'] and settings['loadSlot'] == plan['loadSlot'] == report['load_slot'], 'run identity')
    require(settings['clockId'] == report['clock_id'], 'clock identity')
    require(set(settings['requiredSources']) <= KEYS, 'required key names')
    require(len(samples) <= settings['limits']['maxSamples'] and len(windows) <= settings['limits']['maxOutcomeBuckets'], 'bounded storage')
    # Stored invalid runs retain diagnostics, including unavailable/reset sources.
    # Numeric value shape and labels are still enforced; qualification is never inferred.
    for sample in samples:
        for key, v in sample['jvm']['values'].items():
            require(key in KEYS, 'unknown key')
            value(v, key)
        for key, v in sample['resourceDeltas'].items():
            require(key in CUMULATIVE, 'unknown delta')
            value(v, key)
    if not valid:
        return {'directory': str(root), 'declared_validity': 'invalid', 'verified_storage': True,
                'qualified_observation': False, 'issues': report['issues']}
    require(report['sampler_terminated'] and report['late_callbacks'] == 0, 'sampler/callback shutdown')
    require(execution['mode'] in {'closed', 'open'}, 'execution mode')
    if execution['mode'] == 'closed':
        require(result['passed'] and result['workersTerminated'], 'execution not passed')
    else:
        require(all(result[k] for k in ['allArrivalsConsidered', 'collectionValid', 'scheduleQualified', 'serviceHealthy']) and result['outstandingCallbacks'] == 0, 'execution not passed')
    require(result['failure'] is None, 'execution failure')
    anchor, ended = report['anchor_ns'], report['finished_ns']
    require(result['startedNanos'] <= anchor <= ended <= result['finishedNanos'], 'run clock scope')
    require(samples and report['transitions'], 'no observations')
    identity = samples[0]['jvm']['processInstanceId'], samples[0]['jvm']['pid']
    require(report['source_processes'] == [{'process_instance_id': identity[0], 'pid': identity[1], 'role': settings['role']}], 'process deduplication/role')
    previous, peak = None, 0
    for sequence, sample in enumerate(samples, 1):
        jvm = sample['jvm']
        require(sample['sequence'] == sequence, 'sample sequence')
        require(sample['stage'] in STAGES, 'sample stage')
        require(anchor <= sample['collectionStartedNanos'] <= jvm['startedNanos'] <= jvm['endedNanos'] <= sample['collectionEndedNanos'] <= ended, 'sample clock bounds')
        require((jvm['processInstanceId'], jvm['pid']) == identity, 'process changed')
        require(set(jvm['values']) == KEYS and set(sample['resourceDeltas']) == CUMULATIVE, 'source coverage')
        require(all(jvm['values'][key]['status'] == 'measured' for key in settings['requiredSources']), 'required unavailable')
        require(all(jvm['values'][key]['status'] == 'unsupported' for key in ['rss_bytes', 'allocation_bytes', 'gc_pause_ns']), 'unsupported source overclaim')
        current = counts(sample['client'])
        peak = max(peak, current['pending'])
        require(sample['intervalEndNanos'] == jvm['endedNanos'], 'actual resource end')
        if previous is None:
            require(sample['baseline'] and sample['intervalStartNanos'] == jvm['endedNanos'] and not sample['clientDeltas'], 'baseline interval')
            require(current['admitted'] == 0, 'baseline not unused')
        else:
            require(not sample['baseline'] and sample['intervalStartNanos'] == previous['jvm']['endedNanos'] <= jvm['startedNanos'], 'resource interval continuity')
            require(set(sample['clientDeltas']) == OPS, 'client delta coverage')
            for op in OPS:
                before, after, delta = previous['client']['byOperation'][op], sample['client']['byOperation'][op], sample['clientDeltas'][op]
                for key in ['admitted', 'success', 'errorResponse', 'noResponse', 'attempts']:
                    require(after[key] >= before[key] and delta[key] == after[key] - before[key], 'client delta')
                require(delta['pendingAtEnd'] == after['pending'], 'pending gauge')
        for key in CUMULATIVE:
            delta = sample['resourceDeltas'][key]
            before = previous['jvm']['values'][key]['value'] if previous else None
            after = jvm['values'][key]['value']
            if before is not None and after is not None:
                require(after >= before and delta['status'] == 'measured' and delta['value'] == after - before, 'resource delta: ' + key)
            else:
                require(delta['status'] == 'unsupported' and delta['reason'] == ('baseline' if previous is None else 'counter_source_unavailable'), 'unavailable delta')
        previous = sample
    require(report['sampled_peak_pending'] == peak, 'sampled peak')
    transitions = report['transitions']
    require([t['stage'] for t in transitions] == STAGES, 'stage coverage/order')
    prior_time, prior_sequence = anchor, 0
    for index, t in enumerate(transitions):
        require(prior_time <= t['requestedNanos'] <= t['observedNanos'] <= ended and prior_sequence < t['sampleSequence'] <= len(samples), 'transition order')
        sample = samples[t['sampleSequence'] - 1]
        require(t['observedNanos'] == sample['collectionEndedNanos'] and sample['stage'] == STAGES[max(0, index - 1)], 'transition boundary')
        prior_time, prior_sequence = t['requestedNanos'], t['sampleSequence']
    observed, keys = Counter(), set()
    for w in windows:
        require(w['cohortPhase'] in PHASES and w['operation'] in OPS and type(w['index']) is int and w['index'] >= 0, 'request population')
        key = w['index'], w['cohortPhase'], w['operation']
        require(key not in keys, 'duplicate request window')
        keys.add(key)
        require(w['startNanos'] == anchor + w['index'] * PERIOD and w['startNanos'] <= ended and w['endNanos'] == min(anchor + (w['index'] + 1) * PERIOD, ended), 'nominal request window')
        for kind in ['success', 'errorResponse', 'noResponse']:
            require(type(w[kind]) is int and w[kind] >= 0, 'request count')
            observed[w['cohortPhase'], w['operation'], kind] += w[kind]
    for phase in PHASES:
        expected = execution['trace']['keyCount'] if phase == 'LOAD' else plan['warmupRequests'] if phase == 'WARMUP' else plan['measuredRequests']
        phase_total = 0
        for op in OPS:
            for kind in ['success', 'errorResponse', 'noResponse']:
                n = observed[phase, op, kind]
                require(n == result['phases'][phase]['byOperation'][op][kind], 'cohort outcome reconciliation')
                phase_total += n
        require(phase_total == expected, 'phase plan reconciliation')
    final = counts(result['finalClient'])
    require(final['pending'] == 0 and final['admitted'] == sum(observed.values()) == report['observed_outcomes'], 'final reconciliation')
    for op in OPS:
        require(samples[-1]['client']['byOperation'][op] == result['finalClient']['byOperation'][op], 'final client sample')
    return {'directory': str(root), 'declared_validity': 'valid', 'verified_storage': True,
            'qualified_observation': True, 'samples': len(samples), 'request_windows': len(windows),
            'outcomes': sum(observed.values()), 'processes': 1}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directories', nargs='+', type=Path)
    args = parser.parse_args()
    print(json.dumps([verify(p) for p in args.directories], indent=2, ensure_ascii=False))
