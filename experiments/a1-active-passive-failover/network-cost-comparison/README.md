# PB / VR 메시지 크기·대역폭 비용 비교

이 디렉터리는 개선 전 FULL_LOG 모드를 재현한다. 기본 VrEngine의 증분 전송 개선과 새 결과는 [network-optimization](../network-optimization/README.md)을 따른다.

## 목적

기존 [프로토콜 비교](../protocol-comparison/README.md)는 메시지마다 고정 지연만 주었다.
이번 실험은 메시지 크기, 링크 대역폭, FIFO 대기열을 반영해 현재 PB/VR 구현의 쓰기 응답과 crash 복구 비용을 확인한다.
실제 wire codec이나 SSD/TCP 측정이 아니라 명시된 네트워크 모델의 비교다.

프로토콜 알고리즘은 변경하지 않았다. 기존 ProtocolCluster는 기본 경로를 유지하고, 이 실험에서만 LinkNetwork를 활성화한다.

## 바이트 계산: logical-link-fifo-1

공통 frame은 32B(type 4 + from 8 + to 8 + view/epoch 8 + payload length 4)다.
ID와 op-number는 8B, 로그 개수는 4B, commit-number는 8B, 명령 tag는 1B, key/value 길이는 각각 4B로 정했다.
key/value는 실제 Bytes 길이를 센다. 압축·패킷 헤더·TLS·메모리 객체 크기는 포함하지 않는다.

이번 입력에서 key는 항상 16B다. value 크기를 V, 메시지 안 로그 entry 수를 L이라 하면:

| 메시지 | 전체 frame 크기 |
|---|---:|
| PB Replicate | 65 + V |
| PB ReplicateAck | 40 |
| PB heartbeat / INSTALL / READY | 36 |
| VR PREPARE / STARTVIEW | 44 + L × (41 + V) |
| VR DOVIEWCHANGE | 52 + L × (41 + V) |
| VR PREPAREOK | 40 |
| VR STARTVIEWCHANGE / GETSTATE | 32 |

VR의 주기 heartbeat도 현재 구현대로 전체 로그 PREPARE다. 빈 heartbeat나 증분 로그로 대체하지 않았다.
GET/DELETE/EXISTS/COUNT의 논리 크기 계산도 지원하지만 이번 workload는 SET만 사용한다.

## 전송 모델

각 방향 링크가 독립적인 FIFO 직렬화 큐를 갖는다. A→B와 B→A는 병렬이며, A→B와 A→C도 각자 대역폭을 가진다.

```text
전송 시작 = max(현재 시각, 같은 방향 링크의 앞선 frame 직렬화 완료)
직렬화 시간 = ceil(frame bytes × 1,000,000,000 / bytes_per_second) ns
도착 시각 = 전송 시작 + 직렬화 시간 + 편도 전파 지연 1ms
```

앞 frame의 전파 시간이 끝날 때까지 링크를 점유시키지는 않는다. 여러 frame이 전파 중일 수 있다.
큐의 길이 제한·backpressure·중복 frame 병합을 추가하지 않았다. 느린 링크에서 재전송 생성량이 처리량보다 크면 backlog가 쌓인다.
crash된 노드를 향한 전송 시도는 즉시 drop으로 센다. 예약된 frame의 노드나 링크가 도착 시점에 끊겼다면 도착 drop으로 센다.
NIC 공유 대역폭, TCP 혼잡제어, 패킷 분할, CPU·SSD 처리 지연은 포함하지 않는다.

## 조건과 측정 구간

| 항목 | full |
|---|---|
| 방식 | PB / VR |
| 단계 | NORMAL / CRASH |
| 준비된 성공 연산 수 | 1 / 16 / 64 |
| value 크기 | 64 / 1,024 / 8,192B |
| 방향별 링크 대역폭 | 1 / 10 / 100 MiB/s (1 MiB = 1,048,576B) |
| heartbeat timeout | 100 / 2,000ms |
| heartbeat / timer tick | 10 / 1ms |
| 전파 지연 | 편도 1ms, jitter 없음 |
| 주 요청 deadline | 2,000ms |
| crash 클라이언트 재시도 간격 | 5ms |
| 관측 상한 | 5,000ms |

full은 `2 × 2 × 3 × 3 × 3 × 2 = 216조건`이다.
quick은 연산 수 1·64, value 64·8192, 대역폭 1·100의 64조건을 두 번 반복한다.
난수가 없는 모델이므로 seed 반복을 통계 표본으로 만들지 않는다.

### 준비

각 조건은 새 3노드와 독립 KV로 시작한다. 동일한 고유 키 SET 이력을 전파 지연만 있는 무제한 대역폭으로 만든다.
모든 VR 노드의 로그 길이와 적용 위치가 일치하고, 네트워크가 빈 시점에 지정 대역폭을 활성화한다.
준비 단계 트래픽은 측정에서 제외한다. 이는 이미 만들어진 이력 크기의 영향을 분리하는 실험이며 장시간 부하 후 정상 상태를 측정하는 실험이 아니다.
PB도 같은 성공 이력을 만들지만 전체 이력을 메시지에 담지는 않는다.

### NORMAL

기존 이력에 새로운 고유 키 SET 한 건을 추가한다. PB의 전달은 한 연산이고, VR의 첫 PREPARE 로그 길이는 준비 이력 + 1이다.
첫 요청의 OK/TIMEOUT/UNAVAILABLE callback 또는 관측 상한까지 진행한다. 실패 후 정상 단계의 클라이언트 재시도는 하지 않는다.
실제 crash가 없으므로 view 증가가 있으면 네트워크 비용으로 인한 장애 의심의 영향이다.

### CRASH

대역폭 활성화 직후 Primary 0을 영구 종료한다. 클라이언트는 알려진 Backup 2를 조회하고 Redirect·epoch 응답에 따라 새 Primary를 찾는다.
접속 실패는 즉시 UNAVAILABLE이며 client↔server 통신 비용은 없다.
새 고유 키 SET의 첫 OK callback 또는 5초 상한까지 진행한다. 재시도는 같은 ClientId/OperationId를 사용한다.
관측 중 새 Primary 후보가 다시 timeout으로 view를 바꾸는 경우도 제한하지 않는다.

### 종료와 검증

callback 시각은 ns로 기록한다. 상태와 네트워크 통계는 callback을 확인한 1ms polling 종료 시점에서 수집하므로 최대 1ms 늦을 수 있다.
모든 기존 고유 키를 생존 노드에서 검사한다. NORMAL은 이력 수 × 3, CRASH는 이력 수 × 2 복사본이 같아야 한다.
OK인 새 SET은 최소한 성공 응답한 Primary에 적용되어야 한다. VR Backup 적용은 commit 통지가 도착한 뒤일 수 있어 복구 callback 시점에 전부 적용됐다고 가정하지 않는다.

## 지표

CSV와 JSON의 시간은 ns이며 Markdown만 ms로 변환한다.

| 지표 | 의미 |
|---|---|
| responseNs | NORMAL 요청 제출 → callback. 해당 없음/미완료는 -1 |
| recoveryNs | Primary crash → 첫 OK callback. 해당 없음/미완료는 -1 |
| observedNs | 대역폭 활성화 → 통계 수집 시점 |
| outcome | OK / TIMEOUT / UNAVAILABLE / CENSORED |
| attempts | 주 요청·재시도 호출 수 |
| preservedCopies | 기존 성공 이력의 일치한 복사본 수 |
| probeCopies | 새 요청 값이 실제 적용된 생존 복제본 수 |
| maxViewAdvance | 관측 종료의 최대 epoch/view에서 초기 값을 뺀 값 |
| attemptedBytes | drop 포함 모든 전송 시도의 논리 bytes |
| enqueuedBytes | 연결 가능한 링크에 넣은 bytes |
| deliveredBytes | 목적지 프로토콜에 전달한 bytes |
| droppedBytes | 전송 전 또는 도착 시 drop한 bytes |
| pendingBytes | 수집 시 아직 대기/전송/전파 중인 bytes |
| dataBytes / controlBytes | enqueued 중 복제/전체 로그 frame과 ACK·승격 제어 frame의 bytes |
| messages | 전송 시도 frame 수 |
| peakPendingBytes | 모든 링크의 대기/전송/전파 bytes 합의 최댓값 |
| maxQueueWaitNs | 관측 구간에 예약한 frame의 최대 대기시간. 아직 전송을 시작하지 않은 frame도 포함 |

`attempted = delivered + dropped + pending`, `enqueued = data + control`을 원본 검증에서 확인한다.
PB NORMAL은 독립 기대식 `2ms + serialize(65+V) + serialize(40)`과 callback 지연이 정확히 같은지도 확인한다.

CENSORED는 5초 안에 복구 성공을 관측하지 못했다는 뜻이다. 복구 시간을 5초로 대입해 평균내거나 영구 복구 불능으로 해석하지 않는다.
관측 길이와 전송 성공 비율이 조건별로 다르므로 총 bytes만으로 같은 성공 작업량의 전송 효율을 비교하지 않는다.

## 실행과 산출물

저장소 루트에서 JDK 21:

```powershell
.\gradlew.bat build :experiment-runner:installDist
.\experiment-runner\build\install\openkache-experiment-runner\bin\openkache-experiment-runner.bat experiments/a1-active-passive-failover/network-cost-comparison/comparison-quick.properties
.\experiment-runner\build\install\openkache-experiment-runner\bin\openkache-experiment-runner.bat experiments/a1-active-passive-failover/network-cost-comparison/comparison.properties
```

공통 Runner가 실행 설정·명령·source/class hash·반복·제한 시간을 기록한다.
backend는 원본 JSON에서 조건 coverage와 바이트·시각·보존 불변식을 검사하고 CSV·보고서·완료 표식을 생성한다.
PB의 기준 기대식 위반이나 기존 성공 이력 유실은 실험 실패다. VR의 의도치 않은 view 증가와 관측 상한 내 미완료는 실험 결과로 남긴다.

```powershell
java -cp "cache-core/build/classes/java/main;cache-core/build/classes/java/test" io.openkache.core.replication.experiment.NetworkCostResearchMain --rebuild "<repeat-001 폴더>"
```

backend 고정 JSON schema만 지원한다. 원본 누락·중복·schema 변경·지표 불변식 위반은 완료 표식을 제거하고 실패한다.
JSON·manifest는 로컬 보관하며 CSV와 보고서는 기존 Git 정책의 보관 대상이다.
현재 전체 로그 VR 구현의 비용을 측정하며 최적화된 VR의 일반적인 성능으로 확대 해석하지 않는다.

[실행 결과와 해석](results.md)

## 그림 재생성

`plot_results.py`는 위 결과 문서가 선택한 참조 CSV를 읽어 두 그림과 입력 hash를 다시 만든다. Java 실험을 재실행할 필요는 없다.
Python 환경의 Matplotlib·NumPy 버전은 `requirements-plot.txt`를 참고한다. 한글 폰트는 Windows의 맑은 고딕을 사용했다.
이번 작업에서는 그래프 의존성을 `cache-core/build/a1-plot-deps`에만 설치했다.

```powershell
# 프로젝트 build 폴더에 그래프 의존성을 둔 경우
$env:PYTHONPATH = "$PWD/cache-core/build/a1-plot-deps"
$env:MPLCONFIGDIR = "$PWD/cache-core/build/matplotlib"
python experiments/a1-active-passive-failover/network-cost-comparison/plot_results.py
```
