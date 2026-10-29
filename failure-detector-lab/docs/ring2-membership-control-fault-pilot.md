# 실험 14 — Ring-2 Membership control fault pilot

## 질문

Ring-2 detector가 연속 3노드 Crash를 모두 찾아도, DEAD tombstone을 전달하는
Membership control 메시지가 손실·지연·중복·순서 변경을 겪을 때 생존 노드의
View가 실제로 수렴하는지 확인했다.

## 주입 방식

Heartbeat와 Verify UDP 메시지는 그대로 두고 `MEMBERSHIP_REMOVE`에만 다음
application-level fault를 결정적으로 적용했다.

- `delay250`: 모든 remove 전송을 250ms 지연
- `reorder500`: 첫 번째 장애 remove를 500ms 추가 지연해 후속 remove보다 늦게 전달
- `duplicate2`: remove를 수신자별 두 번 전송
- `loss15`, `loss45`: sender·receiver·target·logical time·copy·seed의 hash로 drop

메시지는 drop되지 않으면 실제 UDP socket으로 전송된다. 따라서 detector,
DEAD, 최종 View와 wire frame은 `real-udp-observed`지만, loss/delay/reorder는
kernel netem이 아니라 `application-injected`다.

## Smoke

| Control fault | seed | View 수렴 | injected drop | 마지막 DEAD | 전체 수렴 |
|---|---:|---:|---:|---:|---:|
| delay250 | 1 | 성공 | 0/28 | 4.298초 | 4.548초 |
| reorder500 | 1 | 성공 | 0/28 | 4.338초 | 4.339초 |
| duplicate2 | 1 | 성공 | 0/42 | 4.429초 | 4.435초 |
| loss15 | 1 | 성공 | 2/28 | 4.320초 | 4.323초 |
| loss45 | 1 | **실패** | 14/35 | 4.393초 | 미수렴 |

loss45 seed 1에서는 node-5와 node-6이 node-2 removal을 받지 못했다. 두 노드의
최종 View에는 죽은 node-2가 남았지만 detector 차원에서는 세 장애의 최초 DEAD가
모두 존재했다.

## Loss 10-seed pilot

| Control fault | 전체 View 수렴 | Wilson 95% CI | 평균 injected drop | 사전 false DEAD |
|---|---:|---:|---:|---:|
| loss15 | **4/10** | 16.8%~68.7% | 3.6 | 0 |
| loss45 | **1/10** | 1.8%~40.4% | 14.0 | 0 |

성공한 실행만의 latency는 다음과 같다. 실패 실행을 latency 통계에서 숨기지 않기
위해 성공률과 실패 seed를 별도로 보존했다.

| Control fault | 마지막 DEAD p50 / p95 | View 수렴 p50 / p95 | 실패 seed |
|---|---:|---:|---|
| loss15 | 4.305초 / 4.360초 | 4.314초 / 4.446초 | 2, 4, 5, 6, 8, 10 |
| loss45 | 4.311초 / 4.311초 | 4.324초 / 4.324초 | 1, 2, 4, 5, 6, 7, 8, 9, 10 |

loss45의 latency 분위수는 성공 실행이 1개뿐이므로 일반화할 수 없다.

## 결론

delay·reorder·duplicate는 remove-only VersionStamp와 tombstone으로 안전하게
흡수됐다. 반면 **one-shot 직접 broadcast는 15% loss에서도 10회 중 6회 전체
View 수렴에 실패했다.** 독립 관측자가 같은 DEAD를 중복 broadcast하면 일부
drop을 우연히 보완하지만, 모든 생존 노드가 특정 tombstone을 받을 보장은 없다.

따라서 현재 Ring 후보의 다음 필수 구성요소는 detector 변경이 아니라 Membership
전파 복구다. 다음 대조군을 같은 loss seed로 비교한다.

1. One-shot direct broadcast — 현재 기준선
2. Fixed retry 3회 — 작은 클러스터에서의 최소 보완
3. Periodic tombstone anti-entropy — 누락된 removal의 최종 수렴 보장 후보

평가 지표는 전체 View 수렴률·수렴 p95·추가 control frame·중복/오래된 delivery다.
ACK 기반 per-peer retry는 상태·타이머 비용이 더 크므로 fixed retry와 periodic
anti-entropy 결과 뒤에 검토한다.

## 재현

```bash
DOCKER_BIN='/mnt/c/Program Files/Docker/Docker/resources/bin/docker.exe' \
  failure-detector-lab/scripts/experiments/run-udp-distributed-membership-matrix.sh \
  failure-detector-lab/results-udp-membership-control-loss15 loss15 10

./gradlew -p failure-detector-lab udpDistributedMembershipMatrixReport \
  --args='results-udp-membership-control-loss15'
```
