"""공통 Runner가 시작하는 Python 알고리즘 backend. JSON에서 보고서를 재생성한다."""
from __future__ import annotations

from dataclasses import asdict, replace
from pathlib import Path
import csv
import hashlib
import io
import json
import platform
import sys
import statistics
import subprocess
import numpy as np
import scipy
import sklearn
from threadpoolctl import threadpool_limits

from cluster import Config, Cluster, METHODS, SCENARIOS, CONTROLS, trace, history, digest
from algorithms import FACTORIES, Models

SCHEMA = 'b3-literature-algorithms-json-1'
GENERATOR = 'literature-request-trace-1'
ALL = METHODS + CONTROLS
NAMES = dict(scads='SCADS', elastman='ElastMan', estore='E-Store', accordion='Accordion',
             pstore='P-Store', tiramola='TIRAMOLA', pax='PAX', slicer='Slicer',
             fixed_single='고정 4노드', fixed_replica='고정 복제 구성', fixed_slices='고정 slice 배치')


def group(method):
    return 'replicated-dormant' if method in ('pax', 'fixed_replica') else 'fixed-node-sharding' if method in ('slicer', 'fixed_slices') else 'initial-single-copy'


def configuration(profile):
    if profile == 'full':
        return Config(), range(1, 6)
    if profile == 'quick':
        return replace(Config(), windows=48, forecast_horizon=16), range(1, 3)
    raise ValueError('unknown profile')


def execute(method, scenario, seed, config, models, events=None, past=None):
    events = trace(scenario, seed, config) if events is None else events
    past = history(scenario, seed, config) if past is None else past
    cluster = Cluster(config, method)
    policy = FACTORIES[method](models, past) if method in FACTORIES else None
    decisions = []
    for t, requests in enumerate(events):
        cluster.advance(t)
        observation = cluster.observe(t, requests)
        if policy and hasattr(policy, 'observe'):
            policy.observe(len(requests))
        if policy and (t + 1) % config.control_windows == 0:
            if cluster.pending is not None:
                decisions.append(dict(window=t, reason='action-in-progress'))
            elif t - cluster.last_complete <= config.cooldown_windows:
                decisions.append(dict(window=t, reason='cooldown'))
            else:
                plan = policy.decide(observation)
                decisions.append(dict(window=t, reason=plan.reason, detail=plan.detail))
                cluster.accept(plan, t + 1)
    return dict(method=method, scenario=scenario, seed=seed, comparison_group=group(method),
                config=asdict(config), workload_hash=digest(events), history_hash=digest(past),
                metrics=cluster.metrics(), windows=cluster.rows, decisions=decisions, actions=cluster.actions)


def write_json(path, value):
    path.write_text(json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(',', ':'), allow_nan=False) + '\n', encoding='utf-8')


def parse_properties(path):
    result = {}
    for line in path.read_text(encoding='utf-8').splitlines():
        if not line or line.startswith('#'):
            continue
        key, value = line.split('=', 1)
        result[key] = value.replace('\\:', ':').replace('\\=', '=').replace('\\\\', '\\')
    return result


def run(folder, profile, repeat, repeats):
    config, seeds = configuration(profile)
    launch = parse_properties(folder / 'launch.properties')
    provenance = {k: launch[k] for k in ('sourceHash', 'backendHash', 'runnerHash', 'gitCommit', 'suiteExecutionId')}
    provenance['workingTreeDirty'] = bool(subprocess.check_output(['git', 'status', '--porcelain'], text=True).strip())
    provenance['repeat'] = repeat
    provenance['repeats'] = repeats
    models = Models(config)
    environment = dict(python=platform.python_version(), numpy=np.__version__, scipy=scipy.__version__, sklearn=sklearn.__version__)
    original = folder / 'results'
    original.mkdir()
    inputs = folder / 'inputs'
    inputs.mkdir()
    write_json(folder / 'calibration.json', dict(model=models.record(), queue_calibration=models.rows, reward_training=models.reward_data))
    for scenario in SCENARIOS:
        for seed in seeds:
            events = trace(scenario, seed, config)
            past = history(scenario, seed, config)
            write_json(inputs / f'{scenario}-{seed}.json', dict(generator=GENERATOR, events=events, history=past))
            for method in ALL:
                record = execute(method, scenario, seed, config, models, events, past)
                record.update(resultSchemaVersion=SCHEMA, profile=profile, generator=GENERATOR,
                              timeDomain='virtual', dataPlane='modeled-transfer-no-KV',
                              implementationScope='paper-derived-mechanism-prototype-not-original-system-reproduction',
                              provenance=provenance, environment=environment, calibration=models.record())
                record['evidence_hash'] = digest(record)
                write_json(original / f'{scenario}-{seed}-{method}.json', record)
            print(f'{scenario} seed={seed}: 8 methods + 3 controls complete', flush=True)
    rebuild(folder, profile)
    expected = len(ALL) * len(SCENARIOS) * len(seeds)
    (folder / 'completion.properties').write_text(f'status=complete\nresultSchemaVersion={SCHEMA}\ncases={expected}\nprofile={profile}\n', encoding='utf-8')


def validate_record(record, profile, inputs):
    config, seeds = configuration(profile)
    assert record['resultSchemaVersion'] == SCHEMA and record['timeDomain'] == 'virtual'
    assert record['implementationScope'] == 'paper-derived-mechanism-prototype-not-original-system-reproduction'
    assert record['dataPlane'] == 'modeled-transfer-no-KV'
    assert record['method'] in ALL and record['scenario'] in SCENARIOS and record['seed'] in seeds
    assert record['comparison_group'] == group(record['method'])
    assert record['config'] == asdict(config) and record['profile'] == profile
    evidence = dict(record)
    checksum = evidence.pop('evidence_hash')
    assert digest(evidence) == checksum, 'changed result evidence'
    source = inputs / f"{record['scenario']}-{record['seed']}.json"
    original = json.loads(source.read_text(encoding='utf-8'))
    assert original['generator'] == GENERATOR
    assert digest(original['events']) == record['workload_hash']
    assert digest(original['history']) == record['history_hash']
    assert record['workload_hash'] == digest(trace(record['scenario'], record['seed'], config))
    assert record['history_hash'] == digest(history(record['scenario'], record['seed'], config))
    rows = record['windows']
    assert len(rows) == config.windows
    for t, row in enumerate(rows):
        assert row['window'] == t
        assert row['requests'] == len(original['events'][t])
        assert config.min_nodes <= row['ready_nodes'] <= row['allocated_nodes'] <= config.max_nodes
        assert row['ready_nodes'] == len(set(row['active']))
        assert row['unit_count'] >= 1
    metric = record['metrics']
    assert metric['node_windows'] == sum(row['allocated_nodes'] for row in rows)
    assert metric['requests'] == sum(row['requests'] for row in rows)
    assert metric['completed'] == sum(row['completed'] for row in rows)
    assert metric['outstanding'] + metric['completed'] == metric['requests']
    assert abs(metric['deadline_miss_rate'] - sum(row['deadline_misses'] for row in rows) / metric['requests']) < 1e-12
    assert metric['model_transfer_bytes'] == rows[-1]['transferred_bytes']
    assert metric['scale_out'] == sum(a['action'] == 'out' for a in record['actions'])
    assert metric['scale_in'] == sum(a['action'] == 'in' for a in record['actions'])
    assert metric['completed_actions'] == sum(a['completed_window'] is not None for a in record['actions'])
    assert all(isinstance(value, (int, float)) and np.isfinite(value) and value >= 0 for value in metric.values())
    if record['method'] in ('slicer', 'fixed_slices'):
        assert all(row['ready_nodes'] == config.initial_nodes for row in rows)
        assert metric['scale_out'] == metric['scale_in'] == 0
    if record['method'] in CONTROLS:
        assert not record['actions']
    for action in record['actions']:
        keys = [key for u in action['assignments'] for key in u['keys']]
        assert sorted(keys) == list(range(config.keys))
        assert all(set(u['holders']) & set(action['new_nodes']) for u in action['assignments'])
        if action['completed_window'] is not None:
            assert action['completed_window'] >= action['start_window']
    return record


def summarize(records):
    rows = []
    for comparison in ('initial-single-copy', 'replicated-dormant', 'fixed-node-sharding'):
        for scenario in SCENARIOS:
            for method in ALL:
                selected = [r for r in records if r['comparison_group'] == comparison and r['scenario'] == scenario and r['method'] == method]
                if not selected:
                    continue
                row = dict(group=comparison, scenario=scenario, method=method, seeds=len(selected))
                for key in selected[0]['metrics']:
                    values = [r['metrics'][key] for r in selected]
                    row[key + '_mean'] = statistics.mean(values)
                    row[key + '_sd'] = statistics.stdev(values) if len(values) > 1 else 0.
                rows.append(row)
    return rows


def rebuild(folder, profile):
    _, seeds = configuration(profile)
    records = [validate_record(json.loads(p.read_text(encoding='utf-8')), profile, folder / 'inputs')
               for p in sorted((folder / 'results').glob('*.json'))]
    expected = {(method, scenario, seed) for method in ALL for scenario in SCENARIOS for seed in seeds}
    actual = [(r['method'], r['scenario'], r['seed']) for r in records]
    assert len(actual) == len(set(actual)) and set(actual) == expected, 'duplicate/missing results'
    provenance = {digest(r['provenance']) for r in records}
    assert len(provenance) == 1, 'mixed provenance'
    assert len({digest(r['calibration']) for r in records}) == 1, 'mixed calibration'
    rows = summarize(records)
    output = io.StringIO(newline='')
    writer = csv.DictWriter(output, fieldnames=list(rows[0]), lineterminator='\n')
    writer.writeheader()
    writer.writerows(rows)
    (folder / 'summary.csv').write_text(output.getvalue(), encoding='utf-8', newline='\n')
    config, _ = configuration(profile)
    md = ['# 선행 방법 8개: 알고리즘 수준 가상 실험', '',
          f'프로필 `{profile}` · {len(METHODS)}개 방법 + 3개 대조군 × {len(SCENARIOS)}조건 × {len(list(seeds))} seeds = **{len(records)}회**.', '',
          '**원 논문 시스템의 재현이나 실제 OpenKache 서버 측정이 아니다.** 논문 핵심 계산을 가상 요청·대기열·준비·전송 모형에 구현한 prototype이다. 모든 지연은 가상 시간 단위이며 ms로 환산하지 않는다.', '',
          f"실행 commit `{records[0]['provenance']['gitCommit']}` + dirty={records[0]['provenance']['workingTreeDirty']} · source `{records[0]['provenance']['sourceHash']}`.",
          f"모형 calibration {records[0]['calibration']['calibration_rows']}행 · 학습 관측 {records[0]['calibration']['reward_rows']}행. 학습/평가 분리. 원문 및 변경 범위는 실험 입구 README에 기록한다.", '',
          f'각 실행 {config.windows} window. 비율은 seed별 비율의 평균. ±는 seed 표준편차이며 반복 재실행은 독립 표본에 추가하지 않는다.', '',
          'deadline miss는 모형이 계산한 각 요청의 완료 예정 시각과 가상 지연 목표의 차이다. 관측 종료 시 미완료 요청은 outstanding으로 별도 기록한다. 정책의 p99 입력은 해당 시점까지 실제로 완료된 가상 읽기 요청만 사용한다.', '',
          '노드-window는 준비·drain 중 할당도 포함한다. 전송량은 모델 byte이며 실제 wire·KV 전송량이 아니다. 서로 다른 복제·노드 수 전제의 그룹끼리 순위를 매기지 않는다.', '']
    labels = {'initial-single-copy': 'A. 같은 초기 데이터의 탄력성 알고리즘 6개',
              'replicated-dormant': 'B. PAX: 복제 3개·CL=1·휴면 노드',
              'fixed-node-sharding': 'C. Slicer: 4노드 고정·배치 및 분할'}
    for comparison, label in labels.items():
        md += ['## ' + label, '', '| 조건 | 방법 | 목표 지연 초과 % | 노드-window | 전송 모델 KiB | 증설/축소/재배치 | 미완료 action |', '|---|---|---:|---:|---:|---|---:|']
        for row in rows:
            if row['group'] != comparison:
                continue
            md.append(f"| {row['scenario']} | {NAMES[row['method']]} | {100 * row['deadline_miss_rate_mean']:.2f} ± {100 * row['deadline_miss_rate_sd']:.2f} | {row['node_windows_mean']:.1f} | {row['model_transfer_bytes_mean'] / 1024:.2f} | {row['scale_out_mean']:.1f}/{row['scale_in_mean']:.1f}/{row['rebalance_mean']:.1f} | {row['pending_actions_mean']:.1f} |")
        md.append('')
    md += ['## 검증 범위와 제한', '',
           '- 원본 JSON의 전체 조합·입력 hash·형식·핵심 집계와 메타데이터 범위를 검증했다. 실제 KV 정합성·장애 복구 검증이 아니다.',
           '- SCADS는 동적 복제를 사용해 읽기를 나눌 수 있다. 다른 단일 owner 알고리즘보다 가능한 행동 범위가 넓다.',
           '- E-Store는 hot key 단위 재배치를 지원한다. 고정 bin 방법과 데이터 이동 입도가 다르다.',
           '- Accordion은 null-affinity·복제 1의 MILP를 사용한다. 최적해를 얻지 못한 경우 solver 상태를 남기고 실행하지 않는다.',
           '- ElastMan은 가상 대기열 calibration과 PI 이득을 사용한다. 실제 p99 성능 목표를 검증한 결과가 아니다.',
           '- P-Store는 SPAR와 동적 계획법을 사용하되 전송 시간·유효 용량 함수는 공통 모형으로 대체했다. 균등 배치 가정은 skew 조건에서 깨진다.',
           '- TIRAMOLA는 노드 수 상태·load-slice clustering·Q 식을 사용한다. 학습은 별도 모형 sweep이며 실제 VM 탐색 비용을 재현하지 않는다.',
           '- PAX는 기존 복제 배치의 휴면 노드를 켜고 hint byte를 동기화한다. 신규 빈 노드와 같은 비용이 아니다.',
           '- Slicer는 단일 assignment 설정의 핵심 탐욕 이동·cold merge·hot split만 사용한다. 서버 증감 결과로 해석하지 않는다.',
           '- 이 한 세트의 모형·설정에서 최종 우수 알고리즘이나 실환경 성능을 확정하지 않는다.', '']
    (folder / 'results.md').write_text('\n'.join(md), encoding='utf-8', newline='\n')
    return records


def main():
    if len(sys.argv) == 4 and sys.argv[1] == 'rebuild':
        rebuild(Path(sys.argv[2]).resolve(), sys.argv[3])
        return
    if len(sys.argv) != 6 or sys.argv[1] != 'run':
        raise SystemExit('run <folder> <full|quick> <repeat> <repeats> / rebuild <folder> <profile>')
    run(Path(sys.argv[2]).resolve(), sys.argv[3], int(sys.argv[4]), int(sys.argv[5]))


if __name__ == '__main__':
    with threadpool_limits(limits=1):
        main()
