# 실험 12 Pilot — Ring-2 H3/Q3/R3 vs SWIM + Lifeguard 실제 UDP

## 목적

시뮬레이션에서 선정한 Ring-2 + H3/Q3/R3 Verify와 SWIM + Lifeguard를
동일한 실제 UDP 조건에서 반복할 수 있는지 검증했다. 이 단계는 seed 1~3의
pilot이며 최종 통계 결론이 아니다.

## 동일 조건

- 5개 Docker 컨테이너
- observer 실행 15초, peer 실행 20초
- host 기준 준비 5초 후 fault 주입
- Heartbeat/Probe 주기 200ms
- Crash, target egress loss 15%, target egress loss 45%
- loss별 동일 netem seed 1·2·3
- 병렬도 1: 다른 실험의 CPU·네트워크 경합 제거
- frame·byte·latency: `real-udp-observed`

## 결과

| 방법 | Fault | 성공 | 사전 false DEAD | DEAD p50 | DEAD p95 | 평균 송신 frame | 평균 송신 byte |
|---|---|---:|---:|---:|---:|---:|---:|
| Ring-2 H3/Q3/R3 | Crash | 3/3 | 0 | 1.288초 | 1.291초 | 161 | 4,186 |
| Ring-2 H3/Q3/R3 | Loss 15% | 3/3 | 0 | — | — | 152 | 3,952 |
| Ring-2 H3/Q3/R3 | Loss 45% | 3/3 | 0 | — | — | 158 | 4,108 |
| SWIM + Lifeguard | Crash | 3/3 | 0 | 6.228초 | 6.235초 | 198 | 5,148 |
| SWIM + Lifeguard | Loss 15% | 3/3 | 0 | — | — | 164 | 4,264 |
| SWIM + Lifeguard | Loss 45% | 3/3 | 0 | — | — | 175 | 4,550 |

현재 pilot에서 Ring 후보의 Crash DEAD p95는 SWIM + Lifeguard보다 약 4.8배
짧았다. 평균 송신 frame은 Crash 18.7%, loss 15% 7.3%, loss 45% 9.7%
적었다. 표본이 3개이므로 효과 크기의 신뢰구간이나 유의성은 계산하지 않는다.

## Runner 계약 검증

초기 pilot에서는 각 컨테이너가 자신의 시작 시점부터 15초 후 동시에
종료하도록 설정되어 있었다. 시작 순서가 다른 경우 target/helper가 observer보다
먼저 종료됐고, Ring은 이를 빠르게 DEAD로 판정했다. SWIM은 긴 suspicion
timeout 때문에 observer가 먼저 끝나 DEAD가 기록되지 않았다. 이는 알고리즘
차이가 아니라 실험 종료 순서 오류였다.

현재 runner는 observer 15초, peer 20초로 분리하고 다음을 결과에 기록한다.

- fault 전 DEAD 수
- Verify 성공/실패 라운드
- helper probe 성공/실패
- observer monotonic latency
- 실제 송수신 frame과 byte
- 적용한 netem seed

수정 뒤 seed 1~3에서 사전 false DEAD와 종료 직전 peer 소실은 모두 0이었다.

## 성공 정의

- Crash: fault 이후 DEAD가 존재하고 fault 이전 DEAD가 없음
- Loss: fault 이전과 이후 모두 DEAD가 없음
- SUSPECT 후 ALIVE 복귀는 안전 성공이며 별도 이벤트로 보존

## 다음 단계

동일 runner를 병렬도 1로 seed 1~30까지 실행한다. 최종 보고에는 다음을
포함한다.

- Crash DEAD p50·p95·p99와 bootstrap 신뢰구간
- Loss별 false DEAD 비율과 Wilson 신뢰구간
- SUSPECT 발생·복귀 비율
- 실제 송수신 frame·byte 평균과 분포
- Ring과 SWIM의 paired seed 차이

실제 UDP의 연속 3노드 Crash와 분산 Membership 재구성은 이 단일 target
비교와 별도 실험으로 유지한다.

## 실행

```bash
DOCKER_BIN='/mnt/c/Program Files/Docker/Docker/resources/bin/docker.exe' \
  failure-detector-lab/scripts/experiments/run-udp-comparison-matrix.sh \
  failure-detector-lab/results-udp-ring-vs-swim 3 1 5

./gradlew -p failure-detector-lab udpComparisonReport \
  --args='results-udp-ring-vs-swim'
```
