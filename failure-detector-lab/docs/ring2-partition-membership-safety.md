# 실험 9 — Ring-2 Partition Membership 안전성

## 질문

분산 Ring-2 재구성이 Crash를 복구하더라도, Partition을 DEAD로 오판해 살아
있는 노드를 전체 membership에서 제거한다면 안전한 후보가 될 수 없다. 이를
Verify Suspect가 막으면서 기존 연속 3노드 Crash 복구를 유지하는지 확인했다.

## 조건

- 5·8노드에서 Partition 대상 위치를 모두 회전한 13개 실행
- Partition 20~32초, 전체 관측 40초
- 대상은 기존 Ring 관측자 2개에서는 보이지 않지만 독립 helper에서는 도달 가능
- DEAD update 전파 지연 1초, 노드별 versioned membership view
- 비교: Ring-2 분산 재구성 / Ring-2 분산 재구성 + Verify Suspect
- Verify 비용: 요청·helper probe·응답 3개 메시지로 모델링
- Crash 대조: 연속 3노드 모든 시작 위치 13개

## Partition 결과

| 프로필 | 안전 실행 | false DEAD | 잘못 제거된 View |
|---|---:|---:|---:|
| Ring-2 no Verify | **0/13** | 26 | 89 |
| Ring-2 + Verify Suspect | **13/13** | **0** | **0** |

Verify가 없으면 각 Partition 대상의 두 Ring 관측자가 모두 DEAD를 발생시켰다.
DEAD update는 1초 뒤 다른 노드로 확산되어 장애 시작 5초 후 모든 View에서
살아 있는 대상이 제거됐다. 잘못 제거된 View 89개는 5노드 5회×5개 View와
8노드 8회×8개 View의 합이다.

Verify Suspect는 독립 helper가 대상에 도달한 사실로 SUSPECT를 ALIVE로
복구했다. 모든 위치에서 false DEAD, membership update, 잘못된 제거가 0이었다.

| 노드 | no Verify messages/run | Verify messages/run | Verify 구성 |
|---:|---:|---:|---|
| 5 | 58 | 106 | Heartbeat 82 + Verify 24 |
| 8 | 64 | 106 | Heartbeat 82 + Verify 24 |

Verify는 안전성을 얻는 대신 Partition 동안 실행당 24개의 확인 메시지를
추가했다. no Verify 비용이 더 작아 보이는 이유 중 하나는 false DEAD 이후
대상을 감시 구조에서 잘못 제거해 Heartbeat 자체가 중단되기 때문이다. 따라서
이를 정상적인 비용 절감으로 해석하면 안 된다.

## Crash 회귀

| 프로필 | 전체 탐지 실행 | 탐지 노드 | 마지막 DEAD | View 수렴 |
|---|---:|---:|---:|---:|
| Ring-2 + Verify Suspect | **13/13** | **39/39** | 7초 | 8초 |

Verify 경로는 SUSPECT 이후 두 번의 독립 확인 실패로 DEAD를 확정한다. 이
7초는 기존 고정 confirm 기반 9초와 확정 계약이 다르므로 “Verify가 더 빠르다”는
성능 결론으로 사용하지 않는다. 중요한 결과는 Verify를 추가해도 실제 Crash
39/39 복구가 유지됐다는 점이다.

## 측정 출처와 한계

- 상태 전이, false DEAD, View 제거·수렴: `simulation-observed`
- Heartbeat·Verify·Membership 메시지와 byte: `model-estimated`
- Partition은 Ring 관측 경로만 차단되고 helper 경로는 도달 가능한 결정적 모델
- helper까지 분리되는 Partition, loss·reorder·jitter, 실제 UDP: 미검증
- Replica 승격, quorum, epoch, fencing: 별도 Failover 계층 범위

## 결론과 다음 단계

분산 Ring-2는 Verify 없이 사용할 경우 Partition 오판을 전체 membership으로
확산시키는 명확한 안전성 문제가 있다. Verify Suspect는 이번 조건에서 잘못된
제거를 0으로 만들면서 연속 3노드 Crash 39/39 복구를 유지했다. 따라서 이후
Ring 후보는 **Ring-2 + Verify Suspect + 분산 Membership** 조합으로 좁힌다.

다음은 helper도 영향을 받는 다양한 Partition topology와 packet loss를 추가한
뒤 실제 UDP에서 Ring 후보와 SWIM + Lifeguard를 동일 seed·fault·traffic
budget으로 30회 반복 비교한다.

## 실행

```bash
./gradlew -p failure-detector-lab ringPartitionSafetyExperiment \
  --args='--nodes 5,8 --output-dir results-ring-partition-safety-20261028'
```

- `partition-runs.csv`
- `crash-runs.csv`
- `manifest.json`
- `summary.md`
