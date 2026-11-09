"""Fail-closed analysis: require complete cohort, matching configs, hashes and cleanup."""
import argparse
import csv
import hashlib
import json
from pathlib import Path
import statistics

from final_bench import require


def validate_result(case, result):
    require(result.get('schema') == 'a1-final-validation-1' and result.get('ok'), 'failed/unknown result')
    for key, value in case.items():
        if key not in ('caseId', 'resultSha256'):
            require(result['config'].get(key) == value, f'config mismatch: {key}')
    if case['scenario'] == 'crash':
        records = result['records']
        require(len(records) == 49, 'crash workload incomplete')
        require(result['verifiedCopies'] == 2 * len(records) == 2 * result['preservedSuccesses'], 'preservation audit incomplete')
        fault = result['fault']
        candidate = records[32]['attempts'][0]
        require(candidate['startNs'] <= fault['sentNs'] <= fault['callBeginNs'] <= fault['callReturnNs'] <= candidate['endNs'], 'fault outside in-flight request')
        original = result['originalLeader']
        initial_epoch = result['statesInitial'][original]['epoch']
        new = [r for r in records if r['node'] != original and r['epoch'] > initial_epoch and r['endNs'] >= fault['callBeginNs']]
        require(new and fault['firstNewLeaderOkNs'] == min(r['endNs'] for r in new), 'old leader OK counted as failover')
        require(result['failoverNs'] == fault['firstNewLeaderOkNs'] - fault['callBeginNs'] > 0, 'invalid failover interval')
    else:
        measure = result['measurement']
        require(measure['logicalOk'] > 0 and measure['elapsedNs'] > 0, 'empty measurement')
        require(measure['logicalOk'] == len(measure['samplesNs']), 'missing raw samples')
        require(all(s > 0 for s in measure['samplesNs']), 'invalid duration')
        if case['scenario'] == 'write':
            expected = case['count'] or (128 if case['value'] > 64 else 1024)
            require(measure['logicalOk'] == expected, 'write workload truncated')
            require(result['verifiedCopies'] == 3 * expected, 'write audit incomplete')
            records = result['records']
            require(len({r['key'] for r in records}) == expected, 'duplicate keys hide data loss')
            require(all(r['attempts'][-1]['status'] == 0 for r in records), 'unsuccessful logical write')
        else:
            require(measure['elapsedNs'] >= case['seconds'] * 1e9, 'timed read ended early')
            require(sum(result['perClientCounts']) == measure['logicalOk'] and all(result['perClientCounts']), 'reader did not participate')


def analyze(source, output):
    manifest = json.loads((source / 'manifest.json').read_text())
    require(manifest.get('executed') and manifest.get('complete'), 'not a completed remote cohort')
    require(len(manifest['cleanup']) == 3 and all(r['ok'] for r in manifest['cleanup']), 'cleanup incomplete')
    require(hashlib.sha256((source / 'executed-dist.tar').read_bytes()).hexdigest() == manifest['distSha256'], 'artifact hash mismatch')
    for name, expected in manifest['scriptsSha256'].items():
        require(hashlib.sha256((source / 'runtime-scripts' / name).read_bytes()).hexdigest() == expected, 'script hash mismatch')
    rows = []
    for case in manifest['cases']:
        path = source / (case['caseId'] + '.json')
        require(hashlib.sha256(path.read_bytes()).hexdigest() == case['resultSha256'], 'raw result hash mismatch')
        result = json.loads(path.read_text())
        validate_result(case, result)
        m = result.get('measurement', {})
        rows.append({'case': case['caseId'], 'scenario': case['scenario'], 'protocol': case['protocol'],
                     'mode': case.get('vr_mode', case.get('read_mode', '')),
                     'target': case.get('read_target', ''), 'value': case['value'],
                     'clients': case['clients'], 'repeat': case['repeat'],
                     'ok': m.get('logicalOk', result.get('preservedSuccesses')),
                     'opsPerSecond': m.get('okOpsPerSecond', ''),
                     'p99Ms': m['p99Ns'] / 1e6 if m else '',
                     'failoverMs': result['failoverNs'] / 1e6 if 'failoverNs' in result else '',
                     'attempts': m.get('attemptCount', ''), 'peerBytes': result.get('peerBytes', '')})
    output.mkdir(parents=True, exist_ok=False)
    with (output / 'runs.csv').open('w', newline='', encoding='utf-8') as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    groups = {}
    for row in rows:
        key = tuple(row[k] for k in ('scenario', 'protocol', 'mode', 'target', 'value', 'clients'))
        groups.setdefault(key, []).append(row)
    summary = []
    for key, runs in groups.items():
        row = dict(zip(('scenario', 'protocol', 'mode', 'target', 'value', 'clients'), key))
        row['repeats'] = len(runs)
        for metric in ('opsPerSecond', 'p99Ms', 'failoverMs', 'peerBytes'):
            values = [r[metric] for r in runs if r[metric] != '']
            for name, function in [('median', statistics.median), ('min', min), ('max', max)]:
                row[f'{metric}_{name}'] = function(values) if values else ''
        summary.append(row)
    with (output / 'summary.csv').open('w', newline='', encoding='utf-8') as stream:
        writer = csv.DictWriter(stream, fieldnames=list(summary[0]))
        writer.writeheader()
        writer.writerows(summary)
    (output / 'scope.md').write_text(
        '# 해석 범위\n\n'
        '- write는 매 실행 새 JVM/로그의 제한된 구간이다. 장시간 steady-state·최대 처리량이 아니다.\n'
        '- p99는 실행별 원본 표본에서 구한 값의 중앙값/min/max다. 합산 p99나 신뢰구간이 아니다.\n'
        '- crash는 sendall 후 응답 전 종료이며 서버 내부 commit 시점을 강제하지 않는다.\n'
        '- read는 clean read-only 부하이며 혼합 부하의 선형화가능성 검증이 아니다.\n'
        '- 부하 발생기를 노드와 함께 실행한다. 1 vCPU에서는 발생기 경합도 측정에 포함된다.\n'
        '- 반복으로 관측 범위를 늘렸을 뿐 운영 성능/모든 장애의 안전성을 증명하지 않는다.\n', encoding='utf-8')
    print(f'VALIDATED {len(rows)} runs -> {output}')


if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('--source', type=Path, required=True)
    p.add_argument('--out', type=Path, required=True)
    args = p.parse_args()
    analyze(args.source, args.out)
