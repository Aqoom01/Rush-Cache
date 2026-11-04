# 실험 11 — Ring-2 다중 Verify helper와 UDP smoke

## 질문

단일 Verify helper는 helper 경로가 함께 차단되거나 손실되면 false DEAD를
막지 못했다. 여러 독립 helper 중 몇 개가 실패해야 한 라운드를 실패로
볼지, 몇 개 실패 라운드 뒤 DEAD를 확정할지를 분리해 비교했다.

- `H`: Verify helper 수
- `Q`: 한 라운드를 실패로 기록하는 데 필요한 실패 helper 수
- `R`: DEAD 확정에 필요한 실패 라운드 수

예를 들어 H3/Q3/R3은 helper 3개가 모두 실패해야 한 라운드가 실패하며,
이 상태가 3라운드 연속일 때 DEAD가 된다. 정상 응답이 하나라도 있으면
대상이 살아 있다는 독립 증거로 사용한다.

## 결정적 시뮬레이션

5·8노드, 노드 수별 30 seed, Verify 경로 독립 손실 15%·45%를 사용했다.
판정과 지연은 `simulation-observed`, 메시지와 바이트 비용은
`model-estimated`다.

| 방법 | 15% loss 안전 실행 | 45% loss 안전 실행 | 연속 3노드 Crash | 마지막 DEAD | 평균 Crash Verify 메시지 |
|---|---:|---:|---:|---:|---:|
| H1/Q1/R2 | 50/60 | 8/60 | 13/13 | 7초 | 27 |
| H2/Q2/R2 | **60/60** | 50/60 | 13/13 | 7초 | 54 |
| H2/Q2/R3 | — | 58/60 | 13/13 | 9초 | 84 |
| H2/Q2/R4 | — | **60/60** | 13/13 | 11초 | 114 |
| H3/Q3/R2 | **60/60** | 56/60 | 13/13 | 7초 | 81 |
| H3/Q3/R3 | — | **60/60** | 13/13 | 9초 | 126 |

2-of-3 실패 방식인 H3/Q2/R2는 15%에서 58/60, 45%에서 28/60만
안전했다. 한 helper가 성공해도 나머지 두 helper의 실패로 라운드를 실패
처리하기 때문이다. 현재 Crash/Partition 모델에서는 `모든 helper 실패`를
요구하는 Q=H가 적합하다.

## Helper 경로 차단

| 방법 | 차단 조건 | 안전 실행 |
|---|---|---:|
| H2/Q2/R2 | helper 1개 차단 | **13/13** |
| H2/Q2/R2 | helper 2개 차단 | 0/13 |
| H3/Q2/R2 | helper 2개 차단 | 0/13 |
| H3/Q3/R2 | helper 2개 차단 | **13/13** |

H3/Q3는 두 helper 경로가 차단되어도 남은 한 helper의 응답으로 false DEAD를
막았다. 모든 helper가 target과 같은 Partition에 갇히는 경우에는 다중 helper도
안전성을 보장하지 못한다.

## 실제 UDP + Docker/netem smoke

5개 컨테이너에서 Ring-2 heartbeat와 실제 UDP Verify request/ACK를 사용했다.
Crash와 target egress 45% netem loss를 후보별 1회 실행했다. 이 표의 latency와
frame/byte는 모두 `real-udp-observed`다.

| 방법 | Fault | 사전 false DEAD | Crash DEAD | 송신 frame | 송신 byte |
|---|---|---:|---:|---:|---:|
| H2/Q2/R4 | Crash | 0 | 1.588초 | 160 | 4,160 |
| H2/Q2/R4 | 45% loss | 0 | 없음 | 154 | 4,004 |
| H3/Q3/R3 | Crash | 0 | 1.242초 | 161 | 4,186 |
| H3/Q3/R3 | 45% loss | 0 | 없음 | 155 | 4,030 |

Smoke에서는 H3/Q3/R3이 H2/Q2/R4보다 Crash를 약 0.35초 빨리 확정했고,
관측된 전체 송신량 차이는 1 frame이었다. 다만 각 조건 1회이므로 우열에
대한 통계적 결론은 내리지 않는다.

## 현재 판단

- 낮은 Verify 비용 후보: H2/Q2/R4
- helper 경로 차단 내성과 더 짧은 Crash 지연 후보: H3/Q3/R3
- 최종 우선 후보: **H3/Q3/R3**

H3/Q3/R3은 45% 모델 손실 60/60, helper 2개 차단 13/13, 연속 3노드 Crash
13/13을 만족했고 UDP smoke에서도 Crash 탐지와 loss 안전 동작을 확인했다.
다음에는 고정 netem seed로 H3/Q3/R3과 SWIM + Lifeguard를 동일 노드 수,
duration, fault 주입 시각에서 반복 비교한다.

## 한계

- UDP 결과는 프로필당 조건별 1회 smoke이며 통계 검정 대상이 아니다.
- UDP runner는 단일 target 관측을 검증한다. 분산 Membership 재구성과 연속
  3노드 Crash의 end-to-end UDP 동작은 아직 simulation-observed다.
- netem loss는 target egress에 적용했다. helper별 독립 경로 Partition과
  상관 burst loss는 아직 실제 UDP로 검증하지 않았다.
- Failure Detector의 DEAD는 replica 승격 권한이 아니다. 실제 승격에는 합의와
  fencing이 별도로 필요하다.

## 실행

```bash
./gradlew -p failure-detector-lab ringVerificationQuorumExperiment \
  --args='--nodes 5,8 --output-dir results-ring-verification-quorum-20261028-v2'

DOCKER_BIN='/mnt/c/Program Files/Docker/Docker/resources/bin/docker.exe' \
  failure-detector-lab/scripts/experiments/run-udp-netem.sh \
  ring-verify-h3-q3-r3 crash results-ring-h3-crash 5

DOCKER_BIN='/mnt/c/Program Files/Docker/Docker/resources/bin/docker.exe' \
  failure-detector-lab/scripts/experiments/run-udp-netem.sh \
  ring-verify-h3-q3-r3 loss45 results-ring-h3-loss45 5 11
```

고정 netem seed는 마지막 인자로 전달하며 `run.properties`에 기록된다.
