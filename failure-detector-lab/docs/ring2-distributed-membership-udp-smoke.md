# 실험 13 — Ring-2 H3/Q3/R3 실제 UDP 분산 Membership 재구성

## 질문

시뮬레이션에서 확인한 Ring-2의 연속 3노드 장애 복구가 실제 UDP에서도
작동하는지 확인했다. 특히 runner가 알고 있는 장애 목록을 Membership에 직접
주지 않고, 실제 detector의 DEAD 이벤트만으로 숨겨진 세 번째 장애의 관측자를
재배정하는 것이 핵심이다.

## 조건

- Docker 컨테이너 8개, Ring-2
- Verify H3/Q3/R3
- 연속된 node-1·node-2·node-3 동시 process crash
- fault marker와 세 JVM의 `Runtime.halt(137)`를 한 UDP injection에서 연속 전송
- remove-only Membership View
- 로컬 DEAD는 `(logical clock, origin node)` stamp로 모든 초기 peer에 직접 전파
- 생존 노드 5개에 같은 finish control을 broadcast하여 종료 순서 오탐 방지
- fault 후 관측 12초

Crash 대상 목록은 fault 주입과 사후 성공 판정에만 사용했다. detector의 target
선택, DEAD 판정, View 제거, 다음 감시 대상 재배정에는 전달하지 않았다.

## 실제 UDP 결과

| 결과 | 값 |
|---|---:|
| 생존 노드 View 수렴 | **5/5 성공** |
| 사전 false DEAD | **0** |
| 추가 생존 노드 제거 | **0** |
| node-1 최초 DEAD | 0.840초 |
| node-2 최초 DEAD | 0.890초 |
| 숨겨진 node-3 최초 생존 관측자 배정 | 0.842초 |
| node-3 최초 DEAD | 4.290초 |
| 모든 생존 View 수렴 | 4.291초 |
| Membership control frame | 28 |
| 생존 노드 송신 frame / byte | 914 / 23,764 |

최종 생존 View는 모든 노드에서 동일했다.

`[node-0, node-4, node-5, node-6, node-7]`

node-0과 node-7이 일부 장애를 독립적으로 DEAD 판정해 총 4번의 로컬 removal
broadcast가 발생했다. remove-only tombstone이 중복 전달을 안전하게 흡수했고,
각 broadcast가 다른 7개 초기 peer에 전송되어 Membership control frame은
28개였다.

latency·frame·byte는 모두 `real-udp-observed`다. 생존 노드 송신량에는 강제
종료된 세 노드의 fault 이전 송신량이 없으므로 전체 클러스터 비용으로 사용하지
않는다.

## 해석

Ring-2는 처음에 node-1과 node-2만 관측한다. 두 노드의 DEAD가 View에
적용되면서 node-3이 node-0과 node-7의 감시 대상에 들어왔고, 이후 실제
Heartbeat 부재와 H3/Q3/R3 Verify 실패만으로 node-3까지 제거했다. 따라서
시뮬레이션에서 확인한 **DEAD → View 축소 → 관측자 재배정 → 숨겨진 장애
탐지** 경로가 실제 UDP에서도 한 번 완주했다.

node-3은 관측자 배정 뒤 약 3.45초 후 DEAD가 됐다. 새 target 등록 시 기존
Fixed Deadline 계약의 3초 warmup이 다시 적용되기 때문이다. 이는 안전한 초기화
정책이지만 장애 복구 지연의 대부분을 차지한다. 다음 반복 실험에서는 재배정
target의 warmup 유지·단축 정책을 별도 변수로 다뤄야 한다.

## 측정 오류 진단 기록

초기 runner는 노드별 `docker exec`로 fault/finish control을 순차 전송했다.
그 실행은 기준시각과 종료시각이 벌어져, 먼저 종료한 생존 노드를 추가 장애로
오인했다. 해당 결과 디렉터리는 삭제하지 않고 진단 증거로 보존했다.

최종 runner는 한 control process가 여러 host에 datagram을 broadcast하고,
fault marker 직후 crash 대상 JVM을 직접 halt한다. 최종 판단에는
`results-udp-ring2-distributed-membership-smoke-20261028-v3`만 사용한다.

## 현재 한계와 다음 단계

이번 결과는 기능 경로를 검증한 1회 smoke다. 아직 다음을 결론 내릴 수 없다.

- 반복 실행의 p50·p95·p99와 수렴 실패율
- Membership update loss·delay·reorder·duplicate 상황의 수렴성
- 같은 다중 장애에서 SWIM + Lifeguard membership 기준선과의 비용 비교
- churn과 rejoin, tombstone 정리
- 종료된 세 노드를 포함한 전체 클러스터 wire 비용

다음은 동일 crash 위치를 반복한 뒤 Membership control에 loss·delay를 주입하고,
마지막으로 같은 조건의 SWIM + Lifeguard 기준선을 연결한다.

## 실행

```bash
DOCKER_BIN='/mnt/c/Program Files/Docker/Docker/resources/bin/docker.exe' \
  failure-detector-lab/scripts/experiments/run-udp-distributed-membership.sh \
  failure-detector-lab/results-udp-ring2-distributed-membership-smoke

./gradlew -p failure-detector-lab udpDistributedMembershipReport \
  --args='results-udp-ring2-distributed-membership-smoke'
```
