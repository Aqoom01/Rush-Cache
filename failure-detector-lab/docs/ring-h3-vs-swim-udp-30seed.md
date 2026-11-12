# 실험 12 — Ring-2 H3/Q3/R3 vs SWIM + Lifeguard 실제 UDP 30-seed 비교

## 질문

시뮬레이션에서 압축한 두 후보 중 어느 쪽이 OpenKache의 현재 요구에 더
적합한지 실제 UDP에서 확인했다.

- **Ring-2 + Verify H3/Q3/R3**: 두 선행 관측자, 독립 helper 3개, 실패 quorum
  3개, 연속 3라운드 실패 후 DEAD
- **SWIM + Lifeguard**: 무작위 probe와 suspicion timeout을 사용하는 일반적인
  분산 membership 기준선

## 고정한 조건

- 5개 Docker 컨테이너
- observer 15초, peer 20초
- host 준비 5초 뒤 fault 주입
- Heartbeat/Probe 주기 200ms
- Crash, target egress loss 15%, target egress loss 45%
- 조건별 seed 1~30, 두 방법에 같은 loss별 netem seed 적용
- 총 180회: 2개 방법 × 3개 fault × 30 seed
- 병렬도 2, 같은 노드 수·실행 시간·fault 주입 시각 사용

Crash 성공은 fault 뒤 DEAD가 있고 fault 전 DEAD가 없는 실행이다. Loss 성공은
fault 전후 DEAD가 없는 실행이다.

## 실제 UDP 결과

아래 frame, byte, latency와 이벤트는 모두 `real-udp-observed`다. 시뮬레이션
메시지 모델 추정치는 이 표에 섞지 않았다.

| 방법 | Fault | 안전 성공 | 사전 false DEAD | DEAD p50 | DEAD p95 | 평균 송신 frame | 평균 송신 byte |
|---|---|---:|---:|---:|---:|---:|---:|
| Ring-2 H3/Q3/R3 | Crash | 30/30 | 0 | 1.320초 | 1.425초 | 161.0 | 4,186.0 |
| Ring-2 H3/Q3/R3 | Loss 15% | 30/30 | 0 | — | — | 152.5 | 3,965.0 |
| Ring-2 H3/Q3/R3 | Loss 45% | 30/30 | 0 | — | — | 159.7 | 4,152.2 |
| SWIM + Lifeguard | Crash | 30/30 | 0 | 6.271초 | 6.408초 | 196.7 | 5,114.2 |
| SWIM + Lifeguard | Loss 15% | 30/30 | 0 | — | — | 156.5 | 4,069.0 |
| SWIM + Lifeguard | Loss 45% | 30/30 | 0 | — | — | 173.2 | 4,502.3 |

30/30 성공의 Wilson 95% 신뢰구간은 각 조건에서 88.6%~100.0%다. 표본에서
실패가 없었다는 뜻이지 실제 성공률이 100%로 증명됐다는 뜻은 아니다.

동일 seed의 paired 차이에서 Ring은 다음과 같았다.

- Crash DEAD 평균 **4.952초 단축**
- Crash DEAD p95 **4.50배 빠름**
- 평균 송신 frame: Crash **18.2%**, Loss 15% **2.6%**, Loss 45%
  **7.8% 감소**
- 평균 송신 byte: Crash 928.2B, Loss 15% 104.0B, Loss 45% 350.1B 감소

Loss 45%에서 Ring은 29/30회 SUSPECT를 발생시켰지만 모든 실행에서 Verify로
복귀했고 false DEAD는 0이었다. SWIM + Lifeguard도 30/30에서 false DEAD가
없었다. 따라서 이 범위에서는 Ring의 빠른 의심이 H3/Q3/R3 검증을 통해 안전한
결과로 이어졌다.

## 결론

현재 실제 UDP 범위에서는 **Ring-2 + Verify H3/Q3/R3이 OpenKache의 우선
후보**다. SWIM + Lifeguard와 같은 안전 결과를 유지하면서 Crash를 더 빨리
탐지했고 송신량도 더 적었다.

그러나 이는 최종 제품 채택 결론이 아니다. 현재 runner는 observer 한 대가
target 한 대를 판단하는 비교다. 시뮬레이션에서 검증한 연속 3노드 Crash,
DEAD 전파, 노드별 View 불일치, Membership 재구성을 실제 UDP로 연결하지
않았다. 또 병렬도 2의 paired seed 실행은 같은 부하 수준을 공유하지만 두
방법의 시작 시각을 완전히 동기화하지는 않는다.

## 다음 실험

다음은 **실제 UDP 연속 3노드 Crash + 분산 Membership 재구성**이다.

1. Ring-2 H3/Q3/R3이 실제 detector의 DEAD 이벤트만으로 View를 갱신한다.
2. DEAD 이벤트 전파 지연과 View version 불일치를 주입한다.
3. 세 장애의 최종 DEAD 시간, membership 수렴 시간, 미관측 구간, 제어
   메시지 비용을 측정한다.
4. 같은 조건의 SWIM + Lifeguard membership 수렴 결과와 비교한다.

이 단계까지 성공하면 Ring 후보를 OpenKache adapter에 붙이는 판단 근거가
생긴다. 실패하거나 재구성 복잡도·비용이 커지면 SWIM + Lifeguard를 선택한다.

## 재현 명령

```bash
DOCKER_BIN='/mnt/c/Program Files/Docker/Docker/resources/bin/docker.exe' \
  failure-detector-lab/scripts/experiments/run-udp-comparison-matrix.sh \
  failure-detector-lab/results-udp-ring-vs-swim-30seed-20261028 30 2 5

./gradlew -p failure-detector-lab udpComparisonReport \
  --args='results-udp-ring-vs-swim-30seed-20261028'
```

산출물은 `runs.csv`, `runs.jsonl`, `summary.md`, `manifest.json`이며 각 실행의
원시 event log, runner log, netem 적용 기록을 함께 보존한다.
