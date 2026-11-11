# E4 이동 방식 비교 (stop-and-copy vs pre-copy, 실서버)

[통합 실험 설계](../design.md) §4 E4. Tablet 하나를 부하 중에 옮길 때 서비스가 어떻게 되는지, 쓰기율에 따라 방식 차이가 어떻게 바뀌는지 잰다.

## 구성

- 서버: `B1ServerMain` 2대(`deploy.server=b1`, `deploy.members=2`, 노드당 Tablet 2개 → Tablet 4개). node 0이 Controller.
- 이동: B3 `StopAndCopyProtocol`·`PreCopyProtocol`을 실제 노드 간 TCP로 실행한다(`docs/protocol-v1.md` §6-5). commit은 Controller MOVE CAS.
- Runner: `cluster.mode=b1`(`infra/README.md` §10). 표를 받아 직행하고 MOVED를 따라가며, BUSY는 1ms부터 두 배씩 늘려 20ms 간격까지 다시 보낸다.

| 항목 | 값 |
|---|---|
| key·value | 100만 개 × 1 KiB(측정 전 전부 SET), key 16 B |
| 부하 | open-loop 5,000 req/s(예정 시각 기준 지연), Zipf θ0.99, seed 42 |
| 쓰기율 | 높음 GET/SET/DELETE 40/30/30, 낮음 80/10/10 |
| 구간 | warm-up 4초, 측정 60초, 측정 시작 10초 뒤 이동 지시 |
| 이동 Tablet | Tablet 3(node 1 → node 0). seed 42에서 요청이 가장 많은 Tablet(약 31%), 약 25만 key·260 MB |
| 이동 대역 | 상한 없음(`migration.rate.bytes.per.sec=0`). batch 64 KiB, 한 번에 batch 1개(ACK 뒤 다음 batch) |
| pre-copy 설정 | 종료 임계 256 KiB, live 라운드 상한 10 (B3 기본값) |

| config | 방식 | 쓰기율 |
|---|---|---|
| `1-stop-and-copy-write-high` | stop-and-copy | 높음 |
| `2-pre-copy-write-high` | pre-copy | 높음 |
| `3-stop-and-copy-write-low` | stop-and-copy | 낮음 |
| `4-pre-copy-write-low` | pre-copy | 낮음 |
| `5-cutover-write-high` | 복사 없음(참고선, B3 방식 아님) | 높음 |

run.sh가 config마다 서버를 재시작하므로 조건마다 빈 저장소·초기 표에서 시작한다.

## 실행

```
NODES=2, CONFIG=experiments/cluster-integration/e4-migration   (GitLab CI 웹 실행)
```

config당 약 5분(미리 채우기 100만 SET, 측정 60초, 끝난 뒤 100만 GET 정합성 검사).

## 결과 파일과 읽는 법

결과 폴더(`s3://.../results/cluster-e4/<날짜>/<run>/`)마다:

- `cluster-<id>.json`
  - `migrations[0].server`: 서버가 잰 이동 기록. `writeBlockedMs`(freeze 시작 → source가 새 표 받음), `sentBytes`, pre-copy `rounds`·`stopReason`·`roundEvents`(라운드별 dirty)
  - `tabletLoad`: Tablet별 요청 수, `setBytesPerSec`(rho 분자), GET·쓰기 지연. 옮긴 Tablet 3의 `write`와 나머지를 나눠 본다
  - `consistency`: `lostSet`·`wrongValue`·`staleHit`(복사 방식은 모두 0이어야 한다), `samples`
  - `sendLag`: 예정 시각 → 실제 송신. 부하 생성기가 따라갔는지 확인한다
- `timeline-<id>.csv`: 예정 시각 기준 100ms 구간별 요청·결과·p50/p99/max. 이동 지시 시각은 `migrations[0].sentMs`(같은 기준)
- `gc-node-<NodeId>.log`: 서버 GC 정지(UTC). `measuredStartEpochMs`로 timeline과 맞춘다

rho = 옮긴 Tablet의 `setBytesPerSec` ÷ 이동 대역(서버 보고 `sentBytes` ÷ 전송 시간, 또는 iperf3 실측).

## 결과

EC2 실행(2026-11-11) 결과와 해석은 [results.md](results.md).

## 로컬 리허설 (2026-11-11, WSL 2노드, 같은 config)

EC2 실행 전 확인용이며 결과로 인용하지 않는다(로컬은 CPU·네트워크 조건이 다르다). 쓰기율 높음 조건.

| 방식 | 쓰기 막힘(서버) | Tablet 3 쓰기 p99 / max | 다른 Tablet 쓰기 p99 | 전송량 | 정합성 위반 |
|---|---|---|---|---|---|
| stop-and-copy | 7,178 ms | 6,948 / 7,273 ms | 1.8-24.8 ms | 259.3 MB | 0 |
| pre-copy | 21.6 ms(라운드 1, THRESHOLD) | 0.9 / 28 ms | 0.7-0.9 ms | 260.4 MB | 0 |

## 해석할 때 주의

- **closed-loop가 아니다.** 요청마다 가상 thread를 띄우고 노드별 연결 64개를 나눠 쓴다. freeze로 기다리는 요청이 수천 건이어도 다른 요청은 예정 시각에 나간다.
- **BUSY 재시도도 부하다.** stop-and-copy freeze 동안 기다리는 쓰기들이 재시도(최대 20ms 간격)로 source에 수만 req/s를 더 보낸다. 같은 노드의 다른 Tablet 지연(Tablet 2)과 freeze 길이에 이 영향이 들어 있다.
- **같은 key의 쓰기는 Runner 안에서 차례로 보낸다**(정합성 oracle). freeze된 key에 이어 온 쓰기는 앞 쓰기가 끝날 때까지 Runner에서 기다리고, 그 대기도 지연에 들어간다.
- **snapshot은 background에서 만든다.** pre-copy는 쓰기 추적을 먼저 켜고, stop-and-copy는 freeze를 먼저 건 뒤 만든다. commit 뒤 source 범위 삭제도 background다. 그래도 노드가 1 vCPU라 이 작업들이 요청 처리와 CPU를 나눈다.
- 이동 대역에 상한을 두지 않아 실제 NIC가 쓰기율보다 훨씬 빠르다. 그래서 rho가 작고 pre-copy가 라운드 1-2회로 수렴할 것으로 예상한다. 시뮬레이션의 rho 범위와 다르면 결과에 명시한다.
- value 1 KiB(시뮬레이션 128 B), 이동 Tablet 260 MB는 B3 시뮬레이션과 다르다(설계 §7).
