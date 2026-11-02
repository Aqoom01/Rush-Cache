# 연속 부하 중 Primary crash

기존 연속 부하 실행기에 Primary 종료를 결합했다. 동시 쓰기 폭·checkpoint·느린 노드 정책을 바꾸지 않고 현재 구현의 failover와 보존을 확인한다.

## 조건

| 항목 | 설정 |
|---|---|
| 방식 | PB / VR_INCREMENTAL |
| 시나리오 | CRASH / SLOW_CRASH |
| 제출 간격 | 1 / 5 / 20ms |
| value / key | 64 / 8,192B, 고유한 16B key |
| 방향별 링크 | 1 / 10MiB/s |
| 입력 | 0~30초 고정 간격 open-loop, 고유 요청당 한 번 |
| crash | 11초에 Primary 0 영구 종료 |
| SLOW_CRASH | 10~20초에 생성된 노드 2의 복제 ACK 전달에 +200ms |
| 초기 감지 timeout / heartbeat / timer | 2,000 / 10 / 1ms |
| 요청 deadline | 1,000ms |
| 샘플 / 추가 관측 상한 | 100ms / 부하 종료 후 30초 |

full은 `2 × 2 × 3 × 2 × 2 = 48조건`이다. quick은 간격 1·20ms, 링크 1MiB/s인 16조건을 같은 30초 길이로 두 번 실행한다.
네트워크는 기존 방향별 FIFO·논리 bytes·편도 1ms 모델을 쓴다. 실제 TCP·SSD·CPU 비용은 없다.

11초에는 아직 최초 Primary가 실제로 정상 Primary인지 검사하고 종료한다. ACK 지연 시작 1초 뒤라 초기 2초 감지 timeout에 의한 선행 전환과 혼동하지 않도록 했다.
같은 시각의 crash 이벤트는 미리 등록해 해당 시각 요청보다 먼저 처리한다. 실제 스레드 정지 경쟁을 측정한 것은 아니다.
crash 노드로 가거나 그 노드에서 나오는 미전달 메시지는 기존 장애 모델대로 도착 시 drop한다.

## 입력·재탐색

응답을 기다리지 않고 예정한 요청을 모두 보낸다. 고유 요청의 재전송은 없다.
클라이언트는 최신 epoch·Redirect를 향후 요청에 반영하고 UNAVAILABLE이면 다음 알려진 노드를 찾는다.
죽은 노드의 낮은 epoch UNAVAILABLE도 접속 실패로 처리한다. 과거 요청의 실패가 이미 이동한 경로를 되돌리지 않도록 요청을 보냈던 노드와 현재 경로를 확인한다.
접속 실패와 client↔server 호출은 즉시이며 소켓 timeout은 모델링하지 않는다.

## failover 끝점

`firstPostCrashOkNs`는 **crash 이후 제출된 새 쓰기**의 첫 OK callback이다.
`failoverNs = firstPostCrashOkNs - crashAtNs`다. 죽은 Primary의 진행 중 요청이 늦게 끝나는 것을 복구로 세지 않는다.
제출을 계속하는 동안 BUSY·UNAVAILABLE·STALE_EPOCH·NOT_PRIMARY·TIMEOUT을 모두 집계한다.

첫 OK를 못 보면 -1로 남긴다. 이를 0이나 관측 상한으로 대입하지 않는다.
새 입력은 30초에 끝나며 추가 관측 중 새 probe는 보내지 않는다. 따라서 -1은 **이번에 제출한 요청에서 재개를 확인하지 못했다**는 뜻이다. 60초 전체의 서비스 가능성을 연속 probe로 검사하거나 영구 복구 불능을 판정한 것은 아니다.

## 장애 전 성공 쓰기의 보존

crash 직전에 OK를 받은 고유 요청을 확정한다. 살아 있는 노드 1·2만 보존 근거로 사용하고 죽은 노드 0의 메모리는 사용하지 않는다.

| 방식 | 보존 evidence |
|---|---|
| PB | 전원 ACK 때 이미 적용된 생존 KV |
| VR | 과반수에 저장된 생존 로그. 아직 KV에 적용하지 않은 로그도 포함 |

VR의 마지막 OK는 Backup에 로그가 있어도 COMMIT 통지가 도착하기 전에 Primary가 죽을 수 있다.
이 경우 생존 KV에 아직 값이 없다는 이유만으로 로그 유실로 판정하지 않는다.
owner mailbox에서 불변 로그 사본을 읽고 request ID·key·value를 확인한다. 진단 사본 생성 시간은 가상 네트워크 비용에 포함하지 않는다.

- `preCrashOk`: crash 전 OK를 받은 요청 수
- `evidenceCopiesAtCrash`: 그 요청들의 생존 evidence 복사본 수. 방식별 evidence 종류가 다르다.
- `retainedPreCrashAtEnd`: 관측 종료에도 최소 한 생존 evidence에 남은 장애 전 성공 요청 수
- `retainedAllOkAtEnd`: 장애 전후의 모든 OK 요청 중 최소 한 생존 evidence에 남은 수
- `verifiedPreCrashKvCopies` / `verifiedAllOkKvCopies`: 실제 생존 KV에 적용된 복사본 수. 기대 복사본은 요청당 2개다.
- `preCrashPending`: crash 시점의 응답 미완료 요청 수
- `inflightRetainedAtEnd`: 당시 미완료 요청 중 최종 생존 evidence에 남은 수. 취소·실행 여부와 같은 뜻이 아니다.

모든 OK 요청은 생존 evidence에 있어야 하며, 값이 다르거나 모든 생존 evidence에서 사라지면 실험 검증이 실패한다.
TIMEOUT/UNAVAILABLE 요청의 로그나 값이 남는 것은 허용한다. BUSY/STALE_EPOCH/NOT_PRIMARY로 거절된 고유 요청이 KV에 적용됐다면 실패다.

## 시계열과 전체 수렴

CSV `kind=phase`는 제출 시각이 crash 전(phase 0: 0~11초)인지 후(phase 1: 11~30초)인지로 나눈다.
성공 p50/p99는 해당 제출 집합의 실제 callback 지연을 nearest-rank로 계산한다. 실패를 p99에 섞거나 결과에서 빼지 않고 따로 집계한다.
completedInWindow는 완료 시각이 각 구간 안인 OK 개수다.

100ms 시계열은 누적 요청 결과, 네트워크 대기, 생존 로그·적용 지연, KV count, view 증가를 담는다.
crash 뒤 `kv0=-1`이다. 노드 0을 정상 복제본처럼 읽지 않는다.
`pendingLiveDataBytes`는 생존 링크의 data frame만 센다. global pendingBytes에는 죽은 노드와 관련된 폐기 예정 frame도 포함될 수 있다.

전체 수렴은 응답 대기가 없고 생존 data 전송이 끝나며 노드 1·2 KV가 같아지는 것으로 검사한다.
VR은 두 노드가 같은 NORMAL view, 같은 로그 길이, 같은 commit/applied까지 도달해야 한다.
최종 key/값 비교로 재확인하며, 수렴 미확인은 영구 장애나 모든 성공 쓰기의 유실을 뜻하지 않는다.
첫 OK는 나와도 전체 수렴이 늦을 수 있고, 로그가 보존돼도 새 쓰기 재개는 늦을 수 있다.

## 실행·원본

저장소 루트, JDK 21:

```powershell
.\gradlew.bat build :experiment-runner:installDist
.\experiment-runner\build\install\openkache-experiment-runner\bin\openkache-experiment-runner.bat experiments/a1-active-passive-failover/load-crash/comparison-quick.properties
.\experiment-runner\build\install\openkache-experiment-runner\bin\openkache-experiment-runner.bat experiments/a1-active-passive-failover/load-crash/comparison.properties
```

고정 JSON schema `a1-load-crash-json-1`에 조건·보존 결과·두 구간 결과·시계열을 기록한다.
coverage·요청 수·응답 합·보존 evidence·적용 복사본·바이트 수지를 검증한 뒤 case/phase/sample long CSV와 보고서를 만든다.
누락·중복·schema·지표 불변식 위반은 완료 표식을 제거한다.

```powershell
java -cp "cache-core/build/classes/java/main;cache-core/build/classes/java/test" io.openkache.core.replication.experiment.LoadCrashResearchMain --rebuild "<repeat-001 폴더>"
```

공통 Runner가 실행 설정·명령·source/class hash·반복·시간 제한을 기록한다. JSON·manifest·로그는 로컬 보관하고 CSV·보고서는 Git 보관 대상이다.

## 유지한 연구 범위

동시 처리 1건, 메모리 로그 이력, 현재 재전송 정책, 고정 3노드와 단일 영구 crash를 유지했다.
checkpoint·부분 snapshot·동시 쓰기 pipeline·느린 노드 정책은 추가하지 않았다.
실제 서버 성능·restart recovery·두 번째 crash·membership 변경에 대한 결과가 아니다.
[기존 멘토 질문 세 가지](../sustained-load/mentoring-questions.md)는 그대로 두고 이 실험을 추가 근거로 사용한다.

[결과와 그래프](results.md)
