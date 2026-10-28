# 실험 8-2 — Ring-2 분산 Membership 재구성

## 목적

즉시 공유 View에서 확인한 39/39 복구가 노드별 View, DEAD 전파 지연,
중복·오래된 update와 제어 메시지 비용을 포함해도 유지되는지 확인했다.

## 조건

- 5·8노드, 연속 3노드 Crash의 모든 시작 위치 13개
- 각 정상 노드는 독립적인 membership view와 View version 보유
- 로컬 Detector의 DEAD 이벤트는 자신의 View에 즉시 반영
- DEAD update는 나머지 노드에 직접 broadcast, 전파 지연 1초
- `(Lamport counter, origin node)` 버전의 remove-only tombstone 사용
- 중복·오래된 update는 제거를 취소하거나 View를 되돌릴 수 없음
- 제어 메시지 payload는 기존 Heartbeat 모델과 같은 64 bytes로 추정
- Crash 정답은 전송 중단과 사후 평가에만 사용하고 제거 결정에는 사용하지 않음

## 결과

| 프로필 | 전체 탐지 | 탐지 노드 | 마지막 DEAD | View 수렴 |
|---|---:|---:|---:|---:|
| Ring-2 Static | 0/13 | 26/39 | 미완료 | 해당 없음 |
| Ring-2 즉시 공유 View | 13/13 | 39/39 | 9초 | 9초 |
| Ring-2 지연 분산 View | **13/13** | **39/39** | **9초** | **10초** |
| Ring-3 Static | 13/13 | 39/39 | **4초** | 해당 없음 |

로컬에서 처음 두 DEAD를 모두 확인한 관측자가 즉시 자신의 View를 줄이면서
숨겨진 세 번째 장애를 감시하기 시작했기 때문에 마지막 DEAD는 즉시 공유
View와 같은 9초를 유지했다. 다른 정상 노드가 해당 update를 받은 뒤 같은
View에 도달하는 데 전파 지연 1초가 추가되어 전체 수렴은 10초였다.

## 통신 비용

| 노드 | Static Ring-2 | 즉시 Ring-2 | 지연 Ring-2 Heartbeat | Membership | 지연 Ring-2 총합 | Ring-3 |
|---:|---:|---:|---:|---:|---:|---:|
| 5 | 284 | 262 | 300 | 16 | **316** | 426 |
| 8 | 530 | 530 | 568 | 28 | **596** | 795 |

분산 Ring-2의 Membership 제어 메시지를 포함한 총 모델 비용은 Ring-3보다
5노드 25.8%, 8노드 25.0% 적었다. 즉시 공유 View보다 Heartbeat가 늘어난
이유는 전파 지연 동안 노드별 View가 달라 이미 제거된 노드 방향의 기존 감시
구조가 일부 남기 때문이다.

실행당 중복·오래된 delivery는 5노드 2개, 8노드 5개였다. 이 update들은
remove-only tombstone 비교에서 무시되거나 tombstone 근거만 갱신했으며,
View version을 되돌리거나 제거된 노드를 다시 포함하지 않았다.

## 해석 범위

- DEAD·View 시각, 탐지 성공률, 중복 update 처리: `simulation-observed`
- Heartbeat·Membership 메시지와 byte: `model-estimated`
- 전파 지연: 고정 1초 모델
- 실제 UDP loss·reorder·jitter, CPU/GC, 재전송: 미검증
- Partition false DEAD와 잘못된 membership 제거: 미검증

## 결론과 다음 단계

1초 지연의 노드별 View에서도 Ring-2는 연속 3노드 장애를 모두 복구했고,
Membership 비용을 포함해도 Ring-3보다 약 25% 적은 모델 통신량을 보였다.
반면 마지막 장애 판정은 Ring-3보다 5초 느리다.

따라서 다음 우선순위는 성공 경로를 더 확장하는 것이 아니라 안전성 반례를
확인하는 것이다. **Partition에서 false DEAD가 발생해 살아 있는 노드를
membership에서 제거하는지**, Ring-2 + Verify Suspect가 이를 막으면서
Crash 39/39 복구를 유지하는지 비교한다. 이후 실제 UDP에서 loss·reorder와
30회 반복 통계를 측정한다.

## 실행

```bash
./gradlew -p failure-detector-lab distributedRingMembershipExperiment \
  --args='--nodes 5,8 --output-dir results-ring-distributed-membership-20261028'
```

- `runs.csv`
- `targets.csv`
- `detector-events.jsonl`
- `view-events.jsonl`
- `control-events.jsonl`
- `manifest.json`
- `summary.md`
