# Failure Detector v2 비교 실험 방법

## 공통 시간축과 재현성

시뮬레이션은 monotonic nanosecond 가상 시간과 deterministic event queue를 사용한다.
같은 시각에는 `fault injection → packet delivery → timeout → send → detector tick` 순으로
처리하며 같은 phase는 증가하는 event id로 고정한다. fault onset은 scenario 기본 시각에
seed 순환 offset `0/137/491/809ms`를 더한다.

프로필은 동일 scenario, seed, node count, fault onset을 공유한다. packet draw는
seed/time/leg와 관측 모듈 namespace로 계산한다. F0/F4, F5/F6처럼 같은 관측 모듈은 같은
trace를 공유하고, 서로 다른 프로토콜의 random 호출 수는 다른 trace를 밀지 않는다.

## 모듈별 통제 비교

- 판단 A: F0↔F1, F0↔F4. 판단 규칙 변화의 지연·오판 영향을 본다.
- 관측 B: F0↔F5. 같은 고정 deadline에서 all-to-all과 ring 비용을 비교한다.
- 보정 C: F5↔F6, F2↔F3/F7a/F7b/F7c. 같은 기반 detector에서 보정만 바꾼다.

Phi가 SWIM random probe를 직접 소비하거나 deadline heartbeat에 Lifeguard correction을
붙이는 등 의미 없는 조합은 capability validation에서 거부한다. F6은 원 관측자의 다른
패킷을 독립 증거로 세지 않고 helper 경로를 사용한다. F7c Buddy probe는 refutation 기회일
뿐 독립 DEAD 증거로 세지 않는다.

현재 `realistic-default`와 `matched-activity`는 target 주기가 모두 1초라 activity는 같다.
fanout, full mesh, indirect probe가 달라 실제 message/byte budget까지 같다는 뜻은 아니다.
wire-budget 정규화 비교는 아직 구현하지 않았으므로 결과에 그렇게 표기하지 않는다.

## 메트릭 정의

- crash suspect/dead latency: 최초 SUSPECT/DEAD 시각에서 실제 crash truth 시각을 뺀다.
  미검출은 0이 아니라 `null`이며 summary의 `missing_n`으로 센다.
- false suspect/dead: target process가 살아 있는 동안 failure state에 들어간 사건·시간이다.
- partition: fail-stop false DEAD와 `unreachable-but-alive` 시간을 별도 집계한다.
- 종료 시 해제되지 않은 failure state는 `right_censored=true`로 표시한다.
- 감지 시각·오판 횟수·지속 시간: `simulation-observed`.
- protocol frame/byte: 선언된 메시지 크기에 따른 `model-estimated`.
- 실제 UDP frame/byte와 observer-local 시간: `real-udp-observed`.

`summary.csv`의 95% CI는 표본 평균에 대한 normal approximation이다. n<2이면 비우며,
결측·중도절단이 있는 감지 지연은 분위수와 run 단위 값을 함께 해석한다.

## 실험 행렬

형식 simulation은 12 scenarios × 5/25/100 nodes × 30 seeds × 10 profiles ×
`realistic-default`로 10,800 runs다. profile별 판단/관측/보정은 manifest, runs,
summary에 기록하며 paired 변화는 `module-contributions.csv`에 남긴다.

## 실제 UDP 제약

Docker Compose harness는 3·5·8노드를 지원하고 fault injector가 observer에게 control
frame을 먼저 보내므로 fault와 detector event를 같은 monotonic clock으로 기록한다.
target `node1`의 container `eth0`에만 loss/delay qdisc를 적용하고 호스트 qdisc는 바꾸지
않는다. smoke 파라미터는 실행 시간을 줄인 값이므로 P2 simulation 수치와 직접 합치지 않는다.

F5/F6의 5·8노드 crash run은 동작 경로 확인을 위한 각 1회 smoke다. 통계적 순위 근거가
아니다. F7c 실제 UDP adapter, 제품 `cache-server`, GET/SET health, CPU/GC, 자동 failover는
미구현이다.
