# 실험 8 — Ring-2 Membership 재구성을 통한 연속 3노드 장애 복구

## 질문

정적 Ring-2가 놓친 연속 세 번째 Crash를, 먼저 탐지한 두 노드의 DEAD 이벤트로
membership view를 갱신하고 감시 대상을 재배치해 최종적으로 발견할 수 있는지
확인했다.

## 1차 구현: 이상적 즉시 재구성

- 5·8노드에서 연속 3노드 Crash 시작 위치를 모두 회전한 13개 실행
- Heartbeat 1초, SUSPECT deadline 3초, DEAD confirm 2초
- Crash 20초, 전체 관측 40초
- Detector가 실제로 발생시킨 DEAD 이벤트만 재구성 입력으로 사용
- 시뮬레이터의 Crash 대상 목록은 패킷 전달 중단과 사후 정답 평가에만 사용
- 모든 정상 노드가 지연 없이 하나의 공유 membership view를 본다고 가정
- membership 제어 메시지와 전파 지연은 아직 모델링하지 않음

재구성으로 더 이상 맡지 않는 감시 대상은 Detector에서 등록 해제했다. 이를
하지 않으면 예전 감시 대상이 stale timeout으로 잘못 DEAD가 되는 문제가
발생하므로 별도 회귀 테스트로 확인했다.

## 결과

| 프로필 | 전체 탐지 실행 | 탐지 노드 | 마지막 장애 DEAD |
|---|---:|---:|---:|
| Ring-2 Static | 0/13 | 26/39 | 미완료 |
| Ring-2 + 즉시 재구성 | **13/13** | **39/39** | 9초 |
| Ring-3 Static | **13/13** | **39/39** | **4초** |

Ring-2 + 즉시 재구성의 시간 흐름은 모든 위치에서 같았다.

1. 장애 시점 +4초: 처음부터 관측 가능했던 두 노드를 DEAD로 판정
2. 같은 시각: 두 DEAD 이벤트로 공유 View를 갱신하고 Ring-2 감시 대상 재배치
3. 장애 시점 +4초: 숨겨졌던 세 번째 노드에 살아 있는 관측자 배정
4. 장애 시점 +7초: 세 번째 노드를 SUSPECT로 판정
5. 장애 시점 +9초: 세 번째 노드를 DEAD로 판정하고 membership에서 제거

Membership event 26개는 모두 `detector-dead-events` 근거를 포함한다. 첫 View
갱신에서 미검출 상태인 세 번째 노드는 제거되지 않았고, 재배치 후 해당 노드의
DEAD 이벤트가 발생한 뒤 두 번째 View 갱신에서 제거됐다. 따라서 실험이 주입한
Crash 정답을 재구성에 직접 사용하지 않았다.

## 통신 비용

| 프로필 | 5노드 fault messages/run | 8노드 fault messages/run |
|---|---:|---:|
| Ring-2 Static | 284 | 530 |
| Ring-2 + 즉시 재구성 | **262** | **530** |
| Ring-3 Static | 426 | 795 |

| 프로필 | 5노드 정상 40초 | 8노드 정상 40초 |
|---|---:|---:|
| Ring-2 / Ring-2 + 재구성 | 410 | 656 |
| Ring-3 Static | 615 | 984 |

즉시 재구성 Ring-2의 Heartbeat 모델 비용은 fault 실행에서 Ring-3보다 5노드
38.5%, 8노드 33.3% 적었다. 정상 조건에서는 이웃 수가 2개와 3개이므로
Ring-2가 33.3% 적다.

하지만 즉시 공유 View의 membership control 메시지는 0으로 기록했다. 따라서
이 차이는 **분산 재구성 제어 트래픽이 얼마까지 추가되어도 Ring-3보다 유리할
수 있는지를 보는 비용 상한선**이지, 실제 네트워크 비용 측정 결과가 아니다.

## 측정 출처

- 상태 전이, 탐지 성공률, 마지막 DEAD, View 갱신 시각: `simulation-observed`
- Heartbeat 메시지와 byte: `model-estimated`
- 즉시 공유 View consistency delay: 이상적 가정 0
- membership control 메시지: 미모델링, CSV에는 0으로 명시
- 실제 UDP, 전파 손실, 노드별 View 불일치, CPU/GC: 미검증

## 결론

1차 실험에서는 membership 재구성 자체가 Ring-2의 구조적 관측 공백을 해결할
수 있음을 확인했다. 대가는 마지막 장애 확정 시간이 Ring-3보다 5초 느리다는
점이다. Heartbeat 비용은 Ring-3보다 낮지만, 실제 분산 전파 비용을 포함하지
않았으므로 아직 최종 후보를 선택할 수 없다.

다음 단계는 **2차 분산 재구성**이다. DEAD 이벤트 전파 지연, 노드별 View
버전, 중복·오래된 이벤트, 일시적 View 불일치를 모델링하고 다음을 측정한다.

- 3/3 최종 탐지 성공률과 p95/p99 마지막 DEAD
- 모든 정상 노드의 View 수렴 시간
- 숨겨진 장애의 미관측 구간
- Heartbeat + membership 제어 메시지 총비용
- 재구성 중 중복 감시와 감시 공백
- Partition false DEAD가 잘못된 membership 제거로 이어지는지

Crash 복구가 유지되는 것을 먼저 확인한 뒤 Ring-2 + Verify Suspect를 결합한다.
Replica 승격, quorum, epoch, fencing은 이번 Detector 실험 범위 밖의 별도
Failover 계약으로 유지한다.

## 실행과 산출물

```bash
./gradlew -p failure-detector-lab ringMembershipReconfigurationExperiment \
  --args='--nodes 5,8 --output-dir results-ring-membership-reconfiguration-20261028'
```

- `results-ring-membership-reconfiguration-20261028/runs.csv`
- `results-ring-membership-reconfiguration-20261028/targets.csv`
- `results-ring-membership-reconfiguration-20261028/detector-events.jsonl`
- `results-ring-membership-reconfiguration-20261028/membership-events.jsonl`
- `results-ring-membership-reconfiguration-20261028/manifest.json`
- `results-ring-membership-reconfiguration-20261028/summary.md`
