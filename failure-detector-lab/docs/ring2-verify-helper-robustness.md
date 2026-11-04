# 실험 10 — Ring-2 Verify helper Partition·Loss 강건성

## 질문

Verify Suspect가 안전하려면 독립 helper가 실제로 대상에 도달해야 한다. Helper
경로도 Partition되거나 packet loss가 발생할 때 false DEAD가 다시 생기는지,
확인 실패 임계값을 늘리면 어느 정도 보완되는지 확인했다.

## 조건

- 5·8노드
- Helper 완전 도달/완전 차단: 모든 대상 위치 13개
- Helper packet loss 15%·45%: 노드 수별 30 seed, 프로필별 60회
- 결정적 `SplittableRandom(seed)` 사용
- Verify 실패 임계값 2·3·4회 비교
- 동일 정책으로 연속 3노드 Crash 13개 회귀

15%와 45%는 기존 OpenKache 실험 설정의 일반 loss와 burst loss 값을 재사용했다.

## Partition 안전성 결과

| 프로필 | 안전 실행 | false DEAD 실행 |
|---|---:|---:|
| Helper 도달, loss 0% | **13/13** | 0/13 |
| Helper 완전 차단 | **0/13** | 13/13 |
| Helper loss 15%, 실패 2회 | 50/60 | 10/60 |
| Helper loss 15%, 실패 3회 | 56/60 | 4/60 |
| Helper loss 15%, 실패 4회 | **60/60** | **0/60** |
| Helper loss 45%, 실패 2회 | 8/60 | 52/60 |
| Helper loss 45%, 실패 3회 | 34/60 | 26/60 |
| Helper loss 45%, 실패 4회 | 48/60 | 12/60 |

현재 기본값인 실패 2회 정책은 15% 독립 loss에서도 10/60 false DEAD를
발생시켰다. 실패 4회는 15%에서 60/60을 보호했지만 45%에서는 12/60을
보호하지 못했다. Helper가 완전히 차단된 topology에서는 임계값만 늘려도
결국 false DEAD를 피할 수 없다.

## Crash 지연 trade-off

| Verify 실패 임계값 | Crash 전체 탐지 | 마지막 DEAD | View 수렴 |
|---:|---:|---:|---:|
| 2회 | 13/13 | 7초 | 8초 |
| 3회 | 13/13 | 9초 | 10초 |
| 4회 | 13/13 | 11초 | 12초 |

모든 임계값에서 연속 3노드 Crash 39/39는 유지됐다. 확인 실패 임계값을 한
단계 늘릴 때 마지막 장애 확정과 View 수렴이 각각 2초씩 늦어졌다. 따라서
임계값 4회는 15% loss 안전성을 얻는 대신 임계값 2회보다 최종 DEAD가 4초
느리다.

## 결론

단일 helper와 고정 실패 임계값만으로는 높은 loss나 helper Partition을 안전하게
처리할 수 없다. 현재 결과로는 다음과 같이 판단한다.

- 15% loss 범위의 1차 UDP 후보: 실패 임계값 4회
- 45% burst loss 및 helper Partition: 단일 helper 정책으로 불합격
- 실제 UDP 30회 최종 비교 전에 다중 helper 또는 quorum 확인이 필요

다음 구현은 2개 이상의 독립 helper 중 하나라도 도달하면 refute하는 방식과,
DEAD에 필요한 실패 quorum을 분리해 비교한다. 그 뒤 Ring 후보와 SWIM +
Lifeguard를 실제 UDP에서 동일 fault·seed·traffic budget으로 반복한다.

## 측정 출처와 한계

- false DEAD, View 제거, Crash DEAD: `simulation-observed`
- Heartbeat·Verify·Membership 비용: `model-estimated`
- packet loss: seed 기반 독립 Bernoulli 모델
- 실제 burst 상관관계, UDP reorder·jitter, CPU/GC: 미검증

## 실행

```bash
./gradlew -p failure-detector-lab ringPartitionRobustnessExperiment \
  --args='--nodes 5,8 --output-dir results-ring-partition-robustness-20261028-v3'
```

- `runs.csv`
- `crash-runs.csv`
- `manifest.json`
- `summary.md`
