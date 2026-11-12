# 연속 쓰기와 느린 Backup 복구

한 건의 새 쓰기 비용을 확인한 [전송 개선 실험](../network-optimization/README.md)에 이어, 응답을 기다리지 않고 요청을 계속 보내는 open-loop 실험이다.
기본적인 처리 정확성과 관측 결과를 확인하되 동시 쓰기 폭·느린 노드 처리 정책·checkpoint 범위는 아직 연구 결정으로 남긴다.

## 조건

| 항목 | full |
|---|---|
| 방식 | PB / VR_INCREMENTAL |
| 시나리오 | NORMAL / SLOW_RECOVER |
| 제출 간격 | 1 / 5 / 20ms (1,000 / 200 / 50회/s) |
| value | 64 / 8,192B |
| key | 고유한 16B key |
| 방향별 링크 | 1 / 10MiB/s |
| 제출 구간 | 가상 0~30초, 30초 시각에는 제출하지 않음 |
| ACK 지연 구간 | SLOW_RECOVER에서 10초 이상~20초 미만에 생성된 노드 2 ACK에 +200ms |
| 정상화 | 20초부터 새 ACK는 추가 지연 없음 |
| 초기 heartbeat timeout / 주기 | 2,000 / 10ms |
| 요청 deadline / timer | 1,000 / 1ms |
| 샘플 간격 | 100ms |
| 부하 종료 후 관측 | 최대 추가 30초 |

full은 `2 × 2 × 3 × 2 × 2 = 48조건`이다. quick은 간격 1·20ms, 링크 1MiB/s의 16조건을 동일한 30초 길이로 두 번 실행한다.
난수는 없으며 같은 입력의 반복은 결정성 확인이다.

NORMAL도 같은 before(0~10), middle(10~20), after(20~30) 구간으로 나눈다. 실제 지연은 SLOW_RECOVER에만 주입한다.
기존 네트워크 모델의 방향별 FIFO 직렬화와 편도 전파 1ms를 유지했다. 추가 ACK 지연은 직렬화 뒤 전달 대기다.
20초에 이미 지연 중인 ACK를 강제로 앞당기지는 않는다. 정상화 뒤 새 ACK가 이전 ACK보다 먼저 도착할 수 있다.
이는 느린 응답 확인의 모델이며 실제 KV 적용·SSD·GC가 200ms 걸린다는 뜻은 아니다.

## 입력과 클라이언트 정책

매 제출 슬롯에 고유 ClientId/OperationId와 key로 한 번 요청한다. 응답을 기다리지 않고 다음 슬롯도 제출한다.
BUSY/실패가 발생해도 예정 요청 수를 줄이지 않는다. 고유 요청을 자동 재시도하지 않는다.
클라이언트는 응답의 최신 epoch·Redirect를 향후 요청에 반영하고 UNAVAILABLE이면 다음 알려진 노드를 시도한다. 늦은 과거 epoch 응답으로 경로를 되돌리지 않는다.
client↔server 전송 지연·소켓 접속 비용은 없다.

PB와 VR 모두 현재 새 쓰기 한 건씩을 처리한다. 따라서 높은 입력률에서는 BUSY가 정상적으로 관측될 수 있다.
이 제한을 고정한 조건의 비교이며 알고리즘의 최대 병렬 처리량을 측정한 것이 아니다.

## 지표의 기준

### 결과·지연·처리량

- `phase` 행은 **제출 시각** 기준이다. phase 0/1/2는 before/middle/after다. 부하 종료 후 완료된 요청도 원래 제출 구간에 속한다.
- OK/BUSY/TIMEOUT/UNAVAILABLE/STALE_EPOCH/NOT_PRIMARY/미완료를 모두 집계한다. 합은 제출 수와 같아야 한다.
- p50/p99는 해당 제출 구간의 **성공한 요청만**을 실제 callback까지 측정한 정확한 nearest-rank다. 샘플 수 n에 대해 ceil(p×n)의 값을 택하며 성공이 없으면 -1이다.
- `completedInWindow`는 **완료 시각**이 실제 해당 10초 구간 안에 들어온 OK 개수다.
- `okDuringLoad / 30`이 0~30초의 실제 완료 처리량이다. 제출 수를 처리량으로 부르지 않는다.
- TIMEOUT/UNAVAILABLE 쓰기는 일부 또는 전체가 나중에 적용될 수 있다. 취소·롤백으로 해석하지 않는다.

### 따라잡기와 전체 수렴

20초에 watermark를 고정한다. VR은 그때까지 알려진 최대 commit-number를 노드 2가 적용한 첫 샘플을 찾는다.
새로운 요청이 계속 들어와 목표가 계속 움직이지 않도록 복구 시점의 prefix를 고정한 것이다.
PB는 성공 전에 모든 Backup의 적용 ACK를 기다리므로, 그때까지 성공한 요청의 watermark 지연은 이미 0이다. 같은 0이라고 해서 PB의 처리량도 유지된다는 뜻은 아니다.
catchupNs는 100ms 간격의 첫 샘플이므로 최대 100ms의 관측 오차가 있다. 끝까지 확인하지 못하면 -1이다.

30초에 새 입력을 멈춘 뒤 다음을 확인하며 최대 30초 더 진행한다.

- 모든 제출 요청의 응답이 완료됨
- 데이터 frame의 대기·전송·전파가 끝남 (작은 제어 메시지는 남을 수 있음)
- 세 KV의 개수가 같음
- VR은 같은 view의 NORMAL 상태이며 세 로그 길이·commit·applied가 모두 맞음

마지막에 **모든 제출 key**를 실제 세 KV에서 읽어 값과 존재 여부를 검사한다. 성공한 요청은 최소 하나의 복사본에 반드시 있어야 한다.
성공한 key의 일부 복사본이 없으면 `missingOkCopies`, 존재 여부가 다른 key는 `divergentKeys`로 기록한다.
BUSY/STALE_EPOCH/NOT_PRIMARY로 거절한 고유 요청이 실제 적용되어 있으면 검증 실패다.
`converged=0`은 추가 관측 30초 안에 모든 복제본 수렴을 확인하지 못했다는 뜻이며 영구 복구 불능이나 성공한 쓰기 전체 유실의 판정이 아니다.

### 시계열

100ms마다 다음을 수집한다.

- 누적 제출·각 응답 수·응답 대기 수
- 모든 링크의 pending bytes와 그중 data bytes
- 노드 2의 `max(logLength)-logLength2`, `max(commit)-applied2`
- 노드별 KV count, 최대 view 증가

PB에는 VR 로그 위치가 없으므로 logLag/applyLag는 -1이다. 값을 임의로 0으로 채워 비교하지 않는다.
샘플의 최대 lag는 샘플링된 최대이며 모든 이벤트의 정확한 최고점이 아니다. network peakPendingBytes는 각 전송 이벤트에서 집계한 최대다.

## 원본과 CSV

고정 schema `a1-sustained-json-1`은 조건·네트워크 합계·세 구간 결과·전체 시계열을 JSON에 담는다.
CSV는 `kind`별 long 형식이다.

| kind | 기준 |
|---|---|
| case | 한 조건 전체 요약. 부하 종료 후 결과 포함 |
| phase | 해당 구간에 제출한 요청의 결과·성공 지연. completedInWindow만 완료 시각 기준 |
| sample | timeNs 시각의 누적 상태. p50/p99 칸은 해당 없음 |

없는 칸은 빈 값, 명시적 미완료/해당 없음 metric은 -1이다. 다른 kind의 offered/ok를 모두 합산하면 중복 계산이 된다.
원본에서 예정 제출 수, 응답 합, 샘플 간격, 바이트 수지, 성공 key 검증 결과, coverage를 확인한 뒤 CSV·보고서·완료 표식을 생성한다.
고정 JSON 형식만 지원하며 누락·중복·schema·불변식 위반은 완료 표식을 제거한다.

## 실행

저장소 루트, JDK 21:

```powershell
.\gradlew.bat build :experiment-runner:installDist
.\experiment-runner\build\install\openkache-experiment-runner\bin\openkache-experiment-runner.bat experiments/a1-active-passive-failover/sustained-load/comparison-quick.properties
.\experiment-runner\build\install\openkache-experiment-runner\bin\openkache-experiment-runner.bat experiments/a1-active-passive-failover/sustained-load/comparison.properties
```

원본 재생성:

```powershell
java -cp "cache-core/build/classes/java/main;cache-core/build/classes/java/test" io.openkache.core.replication.experiment.SustainedLoadResearchMain --rebuild "<repeat-001 폴더>"
```

공통 Runner가 설정·실행 명령·source/class hash·반복·실제 시간 제한을 기록한다.
JSON·manifest는 로컬 보관하고 summary.csv와 보고서는 Git 보관 대상이다.

## 범위와 남겨 둔 결정

가상 시계·즉시 KV·논리 bytes 모델이다. 실제 서버 처리량·CPU·SSD 비용은 측정하지 않는다.
전체 로그 이력과 client table은 계속 보관한다. checkpoint·동시 쓰기 pipeline·느린 복제본 제외 정책을 이번 실험에 추가하지 않았다.
부하 실행의 불필요한 CPU 소모를 줄이기 위해 요청 식별자 조회를 인덱스로 바꾸고, 전송 억제 중인 snapshot을 미리 복사하지 않게 했다. 합의 규칙과 가상 전송 비용은 바꾸지 않았다.

측정 결과를 근거로 [멘토 질문](mentoring-questions.md)의 연구 범위·정책을 결정한다.
