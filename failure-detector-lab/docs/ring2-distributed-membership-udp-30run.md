# 실험 13 — Ring-2 H3/Q3/R3 실제 UDP 분산 Membership 30회 반복

## 질문

1회 smoke에서 확인한 `DEAD → View 축소 → 관측자 재배정 → 숨겨진 장애
탐지` 경로가 반복 실행에서도 안정적으로 수렴하는지 확인했다.

## 조건

- Docker 컨테이너 8개, Ring-2
- Verify H3/Q3/R3
- node-1·node-2·node-3 연속 3노드 동시 process crash
- fault marker와 세 JVM halt를 한 UDP injection에서 전송
- detector의 실제 DEAD 이벤트만 remove-only View에 적용
- 실행당 fault 전 5초, fault 후 12초 관측
- 총 30회 순차 실행

실험은 같은 fault 위치와 설정을 반복했다. 별도의 난수 seed나 netem을 사용하지
않았으므로 분포는 Docker·JVM·host scheduling 변동을 반영하며 다양한 네트워크
trace를 대표하지 않는다.

## 결과

| 측정치 | 실제 UDP 결과 |
|---|---:|
| 전체 View 수렴 | **30/30** |
| 성공률 Wilson 95% CI | 88.6%~100.0% |
| 사전 false DEAD | **0** |
| 정확한 최종 View를 가진 생존 node 결과 | **150/150** |
| 숨겨진 node-3 관측자 배정 p50 / p95 / p99 | 0.863초 / 0.921초 / 0.990초 |
| 마지막 장애 DEAD p50 / p95 / p99 | 4.312초 / 4.370초 / 4.438초 |
| 전체 View 수렴 p50 / p95 / p99 | 4.312초 / 4.371초 / 4.438초 |
| 평균 Membership control frame | 23.1 |
| 평균 생존 노드 송신 frame / byte | 950.0 / 24,699.1 |

모든 생존 노드의 최종 View는
`[node-0, node-4, node-5, node-6, node-7]`이었다. Crash 대상 목록은 fault
주입과 사후 평가에만 사용했고 detector target, DEAD 판정, View 제거와 재배정
로직에는 전달하지 않았다.

latency·frame·byte는 모두 `real-udp-observed`다. 송신량에는 crash된 세 노드의
fault 이전 송신량이 없으므로 전체 클러스터 비용으로 해석하지 않는다.

## 관찰

### 숨겨진 세 번째 장애를 반복해서 발견했다

node-3은 최초 Ring-2에서 두 관측자 node-1·node-2도 함께 죽기 때문에 관측자가
없다. 앞선 두 DEAD가 View에 적용된 뒤 평균 약 1초 이내에 생존 관측자가
재배정됐고, 모든 실행에서 node-3까지 DEAD가 됐다.

### 마지막 DEAD 뒤 View 전파는 빨랐다

마지막 장애 최초 DEAD에서 전체 생존 View 수렴까지 평균 0.809ms, 최소
0.214ms, 최대 5.963ms였다. 현재 Docker 내부망의 손실 없는 직접 broadcast
조건에서는 Membership 전파보다 숨겨진 target을 새로 감시하고 판정하는 시간이
지배적이었다.

### 재배정 warmup이 복구 지연의 대부분이다

node-3 관측자 배정 p95 0.921초에서 마지막 DEAD p95 4.370초까지 약 3.45초가
추가됐다. 재배정 target에 적용되는 3초 warmup과 이후 deadline·Verify 라운드가
그 원인이다. 즉 현재 다음 최적화 후보는 Ring 폭을 늘리는 것이 아니라
**기존 membership 구성원의 재배정 warmup 정책**이다.

### 중복 DEAD broadcast는 안전했지만 비용 변동을 만들었다

| Membership control frame | 실행 수 | 로컬 removal broadcast 수 |
|---:|---:|---:|
| 21 | 22 | 3 |
| 28 | 7 | 4 |
| 35 | 1 | 5 |

여러 관측자가 같은 장애를 거의 동시에 DEAD로 판정하면 각자 removal을
broadcast한다. remove-only tombstone이 중복을 안전하게 흡수해 최종 View에는
영향이 없었지만, control 비용은 21~35 frame으로 변했다.

## 결론

**Ring-2 + H3/Q3/R3 + 분산 Membership은 손실 없는 실제 UDP에서 연속 3노드
Crash를 30/30 복구했다.** 정적 Ring-2의 구조적 미검출은 DEAD 기반 재구성으로
반복해서 해소됐다.

다만 현재 결론은 Membership control transport가 정상인 조건에 한정된다.
다음 우선순위는 다음과 같다.

1. Membership remove에 loss·delay·reorder·duplicate 주입
2. 직접 broadcast만으로 수렴하지 못할 때 retry/anti-entropy 필요성 측정
3. 재배정 warmup 3초 유지와 단축 정책의 false DEAD·복구 지연 비교
4. 같은 다중 장애의 SWIM + Lifeguard membership 기준선 비교

## 재현

```bash
DOCKER_BIN='/mnt/c/Program Files/Docker/Docker/resources/bin/docker.exe' \
  failure-detector-lab/scripts/experiments/run-udp-distributed-membership-matrix.sh \
  failure-detector-lab/results-udp-ring2-distributed-membership-30run 30

./gradlew -p failure-detector-lab udpDistributedMembershipMatrixReport \
  --args='results-udp-ring2-distributed-membership-30run'
```
