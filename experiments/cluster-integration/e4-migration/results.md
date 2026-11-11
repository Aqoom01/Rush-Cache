# E4 결과: stop-and-copy vs pre-copy (EC2, 2026-11-11)

조건은 [README](README.md). 요약하면 다음과 같다.

- **환경**: m6gd.medium 3대(B1 서버 2대 + 부하 생성 1대), 메모리 저장소
- **데이터**: key 100만 × 1 KiB
- **부하**: open-loop 5,000 req/s, Zipf 0.99
- **이동**: 측정 10초 시점에 Tablet 3(약 25만 key, 260 MB, 요청 약 31%)을 node 1 → node 0으로 옮김

config당 1회 실행이다. 원본은 `s3://openstd-s15p31d111-results/results/cluster-e4/2026-11-11/`에 있다. `results/<조건>/`에는 `cluster.json`, `timeline.csv`, `gc-node-*.log`, `runner.log`를 복사했다. 실행 commit은 E4 실행용 임시 커밋 `366f45b`(push하지 않음)이며, 내용은 이 브랜치의 작업 트리와 같다.

## 핵심 수치

| 조건 | 쓰기 차단(서버) | 이동 총시간 | 전송량 | Tablet 3 쓰기 p99 / max | pre-copy 라운드 | 정합성 위반 |
|---|---|---|---|---|---|---|
| stop-and-copy, 쓰기 높음 | **10,035 ms** | 10.46 s | 259.3 MB | 10,297 / 11,122 ms | - | 0 |
| pre-copy, 쓰기 높음 | **53 ms** | 8.58 s | 260.9 MB (+0.6%) | 58.5 / 373 ms | live 1 + final, THRESHOLD | 0 |
| stop-and-copy, 쓰기 낮음 | **10,000 ms** | 10.39 s | 260.8 MB | 9,653 / 10,397 ms | - | 0 |
| pre-copy, 쓰기 낮음 | **70 ms** | 8.68 s | 261.4 MB (+0.2%) | 36.0 / 167 ms | live 1 + final, THRESHOLD | 0 |
| cutover(복사 없음, 참고선) | 91 ms | 0.29 s | 0 | 16.9 / 332 ms | - | **lostSet 234,666** |

열 정의:

- **쓰기 차단**: 서버가 보고한 `writeBlockedMs`. freeze 시작부터 source가 새 표를 받을 때까지다.
- **이동 총시간**: 서버 `doneMs`. source cleanup까지 포함한다.
- **정합성 위반**: key 100만 개를 모두 GET해 oracle과 대조한 결과. 사라진 SET(`lostSet`), 다른 값(`wrongValue`), 삭제 후 다시 보인 값(`staleHit`)을 센다.

### pre-copy 라운드

| 쓰기율 | live 라운드 dirty | final 라운드 dirty |
|---|---|---|
| 높음 | 2,812 key, 1.59 MB | 97 key, 57.5 KB |
| 낮음 | 1,131 key, 0.65 MB | 26 key, 17.3 KB |

두 쓰기율 모두 두 번째(final) 라운드가 임계 256 KiB 아래로 내려가 THRESHOLD로 끝났다.

### 이동 대역과 rho

- **실효 이동 대역**: 전송량 ÷ (commit 시작 − snapshot 준비) = stop-and-copy 27 MB/s, pre-copy 32-33 MB/s
- **rho**: Tablet 3 SET bytes/s ÷ 실효 대역 = 높음 0.474 / 32.9 ≈ **0.014**, 낮음 0.161 / 32.4 ≈ **0.005**

## 해석

1. **쓰기 차단 시간에서 두 방식이 크게 다르다.**
   - stop-and-copy는 Tablet 전체(260 MB)를 보내는 동안 쓰기를 막는다. 차단 시간이 전송 시간과 같아서 약 10초였다.
   - pre-copy는 마지막 라운드(17-58 KB)와 commit 동안만 막는다. 차단 시간은 53-70 ms로, stop-and-copy의 약 1/150이다.
   - 옮긴 Tablet의 쓰기 p99도 10.3초 / 9.7초에서 58 ms / 36 ms로 줄었다.
   - "pre-copy의 쓰기 차단은 Tablet 크기와 무관하게 짧다"(설계 §7)는 가설과 같은 방향이다.
2. **쓰기율이 높을수록 pre-copy가 더 많이 보낸다.** 추가 전송량은 쓰기 낮음 +0.2%, 높음 +0.6%였다. 다만 실서버의 rho가 0.005-0.014로 작아서 라운드 1회 만에 수렴했다. 시뮬레이션처럼 rho가 커서 라운드가 늘어나는 구간은 이번 조건에 들어 있지 않다.
3. **stop-and-copy의 차단 시간은 쓰기율과 거의 무관했다**(10.0초 vs 10.0초). 전송량이 같기 때문이다. 반면 그동안 기다린 쓰기 수(BUSY를 받은 요청 3,441건 vs 1,388건)와 그 영향은 쓰기율에 비례했다.
4. **같은 source의 다른 Tablet도 영향을 받았다.**
   - stop-and-copy, 쓰기 높음: source(node 1)의 Tablet 2 쓰기 p99가 5.7초까지 올랐다.
   - 원인은 세 가지가 겹친 것이다.
     - 1 vCPU 노드가 전송, 일반 요청, BUSY 재시도(약 1만 req/s)를 함께 처리했다.
     - Runner가 정합성 oracle 때문에 같은 key의 쓰기를 차례로 보낸다. 그래서 뜨거운 key의 쓰기 대기가 줄줄이 늘어났다.
     - 이 기간에 Runner 송신 지연 p99가 6.3초로 커졌다. 그만큼 측정이 Runner 쪽 대기를 포함한다.
   - 이 조건에서 Tablet 2 쓰기 지연은 서버 단독 지표로 인용하지 않는다.
5. **복사 없는 cutover는 Tablet 3의 값 23만 개를 잃는다**(참고선). 복사 방식 두 개는 100만 key 중 위반이 0건이었다.
   - 쓰기 차단 중 BUSY 재시도, commit 직후 MOVED 추적(최대 3회), 옛 주인 cleanup이 모두 들어간 실행이다.
   - MOVED 3회를 넘겨 실패한 요청도 0건이었다.

## 같이 보이는 잡음

- **GC 정지**: 서버 GC가 약 150 ms씩 측정 구간에 4-5번(노드별) 있었다. 두 방식에서 거의 같은 시각(6-7초, 14초, 16-18초, 32초)에 생겼다.
  - 이동 전(6초)에도 있으므로 방식 차이의 원인이 아니라 공통 배경이다.
  - 이동하지 않는 Tablet의 쓰기 max가 150-300 ms인 것도 주로 이 정지 때문이다.
  - 이동 전 100 ms 구간 p99는 중앙값 0.54 ms, 최대 150-345 ms였다.
- **pre-copy 중 다른 Tablet**: 전송이 이어지는 약 8초 동안 다른 Tablet의 p99가 15-85 ms로 올랐다.
  - 노드가 1 vCPU라 전송과 요청이 CPU를 나눠 쓰기 때문이다. 쓰기 차단과는 별개다.
  - 대역 상한(`migration.rate.bytes.per.sec`)을 두면 이 영향과 이동 시간을 맞바꿀 수 있다. 이번에는 상한을 두지 않았다.

## 한계

- config당 1회 실행이다. 설계의 런 내 3회 반복은 하지 않았다.
- iperf3를 측정하지 않았다. rho의 분모는 iperf3 대역이 아니라 서버가 보고한 실효 이동 대역이다.
- 실효 대역(27-33 MB/s)은 NIC가 아니라 다음 두 가지가 정한다.
  - protocol이 batch를 한 번에 하나만 보낸다(64 KiB, ACK 뒤 다음 batch).
  - 노드가 1 vCPU다.
- 시뮬레이션과 조건이 다르다.
  - value: 1 KiB(시뮬레이션 128 B)
  - 이동 Tablet: 260 MB
  - 쓰기율 2단계: 40/30/30, 80/10/10
- Runner가 정합성 검사 때문에 같은 key의 쓰기를 차례로 보낸다. 과부하일 때 뜨거운 key의 쓰기 지연이 실제보다 커질 수 있다(해석 4).
