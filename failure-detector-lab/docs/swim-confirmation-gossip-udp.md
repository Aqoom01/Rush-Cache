# SWIM + Lifeguard suspicion confirmation Gossip 실제 UDP 구현

## 목적

기존 실제 UDP adapter는 observer 한 대의 실패 Probe만 `SUSPECT`로 기록했고,
다른 reporter의 confirmation을 전송하지 않았다. 따라서 Lifeguard suspicion
deadline이 최대값에서 줄어들지 않는 상태였다.

이번 구현은 `SUSPECT`와 `ALIVE` wire frame을 추가하고 다음 경로를 연결한다.

1. observer의 Direct·Indirect Probe가 모두 실패하면 현재 incarnation의
   `SUSPECT`를 모든 peer에 전파한다.
2. peer는 메시지를 받았다는 이유만으로 confirmation을 만들지 않는다. Target에
   Direct Probe를 보내고, 실패하면 별도 helper를 통한 Indirect Probe를 수행한다.
3. 두 Probe가 모두 실패한 peer만 자신의 Node ID를 reporter로 `SUSPECT`를
   전파한다.
4. observer는 서로 다른 reporter를 수신한 경우에만 Lifeguard confirmation으로
   기록하고 suspicion deadline을 단축한다.
5. 살아 있는 Target은 더 높은 incarnation의 `ALIVE`를 전파해 suspicion을
   반박한다. Probe에 성공한 peer도 실제 ACK 증거를 원래 reporter에게 전달한다.
6. `(target, incarnation)` 단위로 중복 Probe와 중복 report를 억제한다.

Docker 시작 직후 peer 준비 지연을 fault로 오인하지 않도록 SWIM protocol
traffic에는 3초 warmup을 적용한다. 5노드 Lifeguard는 Direct Probe마다 indirect
helper 1명을 사용하고, 최대 3개의 독립 confirmation에 따라 deadline을
점진적으로 줄인다.

`suspicionGossipFrames`, `aliveGossipFrames`, `independentConfirmations`,
`confirmationProbeSuccesses`, `confirmationProbeFailures`를 실제 UDP 결과에
추가했다. frame과 latency는 모두 `real-udp-observed`다.

## 실제 Docker UDP 3-seed pilot

5노드, seed 1~3, Crash와 target egress loss 15%·45% 조건에서 Ring H3/Q3/R3과
같은 runner로 총 18회를 실행했다.

| 방법 | 조건 | 안전 성공 | 사전 false DEAD | DEAD p50 | DEAD p95 | 평균 송신 frame |
|---|---|---:|---:|---:|---:|---:|
| Ring H3/Q3/R3 | Crash | 3/3 | 0 | 1.357초 | 1.376초 | 161.0 |
| SWIM + Lifeguard confirmation | Crash | 3/3 | 0 | 3.403초 | 3.450초 | 87.0 |
| Ring H3/Q3/R3 | loss 15% | 3/3 | 0 | — | — | 152.0 |
| SWIM + Lifeguard confirmation | loss 15% | 3/3 | 0 | — | — | 128.3 |
| Ring H3/Q3/R3 | loss 45% | 3/3 | 0 | — | — | 162.0 |
| SWIM + Lifeguard confirmation | loss 45% | 3/3 | 0 | — | — | 166.7 |

Crash 세 실행은 모두 독립 confirmation 3개를 기록했고 DEAD p95는 3.450초였다.
기존 confirmation 전 30-seed p95 6.408초보다 짧아지는 방향은 확인했지만,
현재 표본은 3개이므로 두 수치를 직접적인 통계 비교로 사용하지 않는다. loss
15%·45%에서는 사전·사후 false DEAD가 없었다.

이 pilot의 원시 결과는
`results-udp-ring-vs-swim-confirmation-gossip-pilot3-20261029-v4`에 보존한다.

## 재현 명령

```bash
./gradlew -p failure-detector-lab test jar

DOCKER_BIN='/mnt/c/Program Files/Docker/Docker/resources/bin/docker.exe' \
  failure-detector-lab/scripts/experiments/run-udp-comparison-matrix.sh \
  failure-detector-lab/results-udp-ring-vs-swim-confirmation-pilot 3 2 5

./gradlew -p failure-detector-lab udpComparisonReport \
  --args='results-udp-ring-vs-swim-confirmation-pilot'
```

runner는 Docker image를 만들기 전에 최신 lab JAR을 생성한다. 기존 결과
디렉터리는 덮어쓰지 않는다.

## 남은 검증

- 동일 seed 1~30의 Crash·loss15·loss45 반복
- Gossip frame 증가분과 DEAD p50/p95/p99 집계
- 전체 노드가 독립적으로 무작위 Probe하는 완전 분산 SWIM runner
- SUSPECT·ALIVE loss·delay·duplicate·reorder
- Membership View 전파와 결합한 최종 Ring 대 SWIM 비교

현재 구현은 실제 UDP에서 독립 Probe 증거를 전달하는 confirmation 경로를
완성했지만, 모든 노드가 자체 SWIM detector와 무작위 Probe scheduler를 갖는
완전한 제품용 SWIM membership 구현은 아니다.
