# SWIM+Lifeguard vs Phi+Gossip 상세 실험 보고서

실행일: 2026-10-24

## 실험 구성

- 기본 비교: 12개 시나리오 × 3개 노드 규모 × 50개 seed × 2개 알고리즘
  = 3,600 runs
- 파라미터 sweep: 12개 시나리오 × 3개 노드 규모 × 30개 seed × 7개
  운용점 = 7,560 runs
- 노드 규모: 5, 25, 100
- 동일 scenario, node count, seed 조합으로 비교
- 결과: `results-detailed/`, `results-sweep/`

시나리오는 normal, crash, crash-under-jitter, packet-loss,
packet-loss-burst, latency step, gradual drift, bursty latency, regime change,
observer pause, target pause, partition을 포함한다.

False Positive는 `ALIVE`에서 `SUSPECT` 또는 `DEAD`로 이탈한 사건이고,
False DEAD는 실제로 살아 있는 노드를 `DEAD`로 확정한 사건이다. SWIM의
일시적인 suspicion과 실제 failover 가능한 판정을 구분하기 위해 두 지표를
별도로 기록했다.

## 기본 설정 결과

### 정상 상태 통신 비용

| 알고리즘 | 노드당 초당 메시지 | 노드당 초당 바이트 |
| --- | ---: | ---: |
| SWIM+Lifeguard | 2.044 | 130.844 |
| Phi+Gossip | 3.067 | 294.400 |

현재 모델에서 SWIM은 Phi보다 메시지가 약 33%, 바이트가 약 56% 적었다.

### Crash 감지 지연 중앙값

| 노드 수 | SWIM SUSPECT | SWIM DEAD | Phi DEAD |
| ---: | ---: | ---: | ---: |
| 5 | 0 ms | 14,400 ms | 18,500 ms |
| 25 | 0 ms | 26,100 ms | 18,500 ms |
| 100 | 0 ms | 37,000 ms | 18,500 ms |

SWIM은 첫 실패 probe에서 즉시 `SUSPECT`가 되었지만, 최종 `DEAD` 확정은
클러스터 크기에 따른 suspicion timeout과 Lifeguard awareness multiplier의
영향으로 느려졌다. Phi 기본 threshold 8의 확정 시간은 이 시뮬레이션에서
노드 수와 무관하게 18.5초였다.

### 동적 지연 및 일시 장애

- gradual, bursty, regime-change 지연에서 두 방식 모두 False DEAD 0회였다.
- 45% burst packet loss에서 SWIM은 run당 평균 3.92회의 일시적인 suspicion을
  만들었지만 False DEAD는 0회였다.
- 같은 packet-loss-burst에서 Phi threshold 8은 상태 전이를 만들지 않았다.
- 15초 partition에서 SWIM 기본 설정은 5노드에서 평균 1회의 False DEAD와
  5.6초의 False DEAD 시간을 기록했다. 25·100노드에서는 긴 suspicion
  timeout 때문에 False DEAD가 없었다.
- Phi threshold 8은 같은 partition에서 False DEAD가 없었다.

## 파라미터 sweep 결과

### Phi threshold

| Threshold | Crash DEAD | 15초 Partition False DEAD 시간 |
| ---: | ---: | ---: |
| 4 | 8.8초 | 6.1초 |
| 6 | 13.7초 | 1.2초 |
| 8 | 18.5초 | 0초 |
| 10 | 23.3초 | 0초 |

Phi는 threshold를 낮추면 감지가 빨라지는 대신 일시 partition을 실제 장애로
확정했다. threshold 8 이상에서는 이번 15초 partition을 견뎠다.

### SWIM suspicion profile

| Profile | 5노드 Crash DEAD | 100노드 Crash DEAD | Partition 특징 |
| --- | ---: | ---: | --- |
| fast | 7.2초 | 18.5초 | 5·100노드 모두 False DEAD |
| balanced | 14.4초 | 37.0초 | 5노드만 False DEAD |
| conservative | 21.6초 | 55.4초 | 기본 partition에서 False DEAD 없음 |

conservative profile은 100노드 crash-under-jitter에서 실험 시간 안에 DEAD를
확정하지 못했다. 안전성을 높이는 설정이 탐지 완전성을 훼손할 수 있음을
보여준다.

## 현재 판단

SWIM+Lifeguard는 더 낮은 통신 비용과 즉각적인 `SUSPECT` 신호가 장점이다.
`SUSPECT` 단계에서 신규 요청만 우회하고, 다수 관측 확인 후에만 failover를
실행한다면 빠른 반응과 안전성을 분리할 수 있다.

Phi+Gossip은 기본 설정에서 더 많은 통신량을 사용했지만, 25·100노드에서
최종 DEAD 확정이 빨랐고 일시적인 packet loss와 partition에서 상태가 덜
흔들렸다. 다만 이 안정성은 threshold에 따른 긴 감지 시간과 교환된다.

따라서 시뮬레이션 결과만으로 한 방식을 최종 선정하지 않는다. 현재 가장
유력한 다음 검증은 다음 두 운용점을 실제 UDP 환경에서 비교하는 것이다.

1. SWIM fast/balanced + 다수 관측 Failover Gate
2. Phi threshold 6/8 + Gossip fanout 및 heartbeat 주기 최적화

## 한계

- 실제 UDP socket, kernel scheduling, GC pause를 사용하지 않은 이산시간
  시뮬레이션이다.
- 정확도는 한 observer-target 쌍을 중심으로 측정하고, background protocol
  비용은 전체 클러스터로 환산했다.
- Phi Gossip과 SWIM membership dissemination의 실제 packet 크기와
  piggyback 효율은 단순화했다.
- SSD I/O 및 실제 GET/SET 복구 시간은 아직 측정하지 않았다.
- 네트워크 partition을 Ground Truth상 `alive`로 정의했으므로 False DEAD는
  fail-stop 정확도 관점의 값이다. 서비스 가용성 관점에서는 별도 해석이
  필요하다.
