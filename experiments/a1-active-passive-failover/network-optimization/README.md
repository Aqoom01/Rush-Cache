# VR 전송 개선 및 동일 조건 재실험

기본 VrEngine은 INCREMENTAL 모드를 사용한다. FULL_LOG 모드는 개선 전 결과를 재현하기 위해 유지했다.
새로운 합의 알고리즘을 제안한 것이 아니라 기존 VR 구현의 전송·재전송을 보강한 것이다.
공통 CacheEngine/Transport 계약은 변경하지 않았다.

## 변경한 동작

| 항목 | FULL_LOG (개선 전) | INCREMENTAL (개선 후) |
|---|---|---|
| 정상 PREPARE | 전체 로그 | peer별 마지막 ACK 이후 suffix만 전송 |
| 확정 통지·heartbeat | 전체 로그 PREPARE | view와 commit-number만 있는 COMMIT |
| 앞 로그가 없는 Backup | 전체 로그 메시지를 기다림 | out-of-order 적용을 거절하고 GETSTATE 요청 |
| 전체 상태 전달 | GETSTATE마다 snapshot 응답 | 확인 대기 중인 snapshot은 중복 생성을 억제 |
| view change 재전송 | 10ms마다 전체 로그 재전송 | DOVIEWCHANGE 수신 확인 및 STARTVIEW 설치 확인으로 전송 상태 관리 |

여전히 과반수 PREPAREOK 뒤에만 commit하고, 읽기도 로그를 경유한다.
view change 로그 선택은 마지막 정상 view → 로그 길이이며, 알려진 commit prefix는 보존한다.
미확정 로그는 KV에 먼저 적용하지 않는다. ClientId/OperationId 중복 처리와 순차 적용을 유지했다.

### 증분·누락 로그 처리

DeltaPrepare는 1-based firstOp와 연산 목록을 담는다. 이미 가진 prefix와 겹치는 연산은 같아야 하며, 앞 로그가 없으면 뒤 연산을 적용하지 않는다.
Backup이 가진 로그가 부족하면 GETSTATE로 전체 상태를 요청한다. 정상 전송은 다음 연산만 보내지만 뒤처진 노드에는 누락 suffix 또는 snapshot이 필요하다.
주기 COMMIT은 작은 heartbeat로 동작하며 유실된 확정 통지를 복구한다.

### 재전송과 상태 전달 우선순위

Transport.send 완료는 원격 수신 확인이 아니다. peer별 확인 대기 슬롯 하나를 유지하고 원격 ACK를 기다린다.
재전송 대기는 1초에서 시작해 2초·4초로 늘리고 4초로 제한한다. 같은 frame이 네트워크에 절대로 하나만 있다는 보장은 아니며, ACK 유실이나 긴 전송에서는 재전송 frame이 겹칠 수 있다.

- DOVIEWCHANGE는 지정된 새 Primary의 ViewChangeAck로 확인한다. 이 ACK는 쓰기 확정 정족수로 세지 않는다.
- DeltaPrepare와 STARTVIEW는 같은 view의 PREPAREOK가 필요한 로그 길이를 확인해야 완료된다.
- GETSTATE는 진행 중인 증분 전송을 snapshot으로 승격할 수 있다. snapshot 설치 확인 전에는 timer가 증분 전송으로 되돌려 상태 전달을 굶기지 않는다.
- 새 view에서는 이전 전송 확인 상태를 버린다. 오래된 ACK는 새 view의 쓰기 확정에 쓰지 않는다.

초기 heartbeat 장애 감지 timeout은 각 실험의 100/2,000ms 그대로다.
INCREMENTAL의 view-change/INSTALLING 대기 예산은 `max(감지 timeout, 3초)`다. 감지 이후 큰 로그를 옮기는 시간까지 100ms로 제한해 반복 승격하는 일을 줄이기 위한 별도 정책이다.
FULL_LOG는 원래대로 전환 대기에도 감지 timeout을 사용한다.
따라서 개선 전후는 전송 방식·재전송·전환 대기 정책의 묶음 비교이며 각 요소의 기여를 독립적으로 분리한 실험은 아니다.

## 메시지 크기와 실험 조건

네트워크 모델은 [이전 비용 실험](../network-cost-comparison/README.md)과 같은 방향별 독립 FIFO 및 논리 인코딩이다.
추가한 메시지의 크기:

| 메시지 | 논리 frame bytes |
|---|---:|
| DeltaPrepare | 52 + 전송 entry 수 × (41 + value 크기), key 16B |
| COMMIT | 40 |
| ViewChangeAck | 32 |

로그가 충분히 복제된 상태에서는 DeltaPrepare가 새 entry 하나만 포함한다. STARTVIEW/DOVIEWCHANGE 전체 로그 크기는 기존과 같다.

조건은 PB/개선 VR × NORMAL/CRASH × 이력 1·16·64 × 값 64·1024·8192B × 링크 1·10·100MiB/s × 초기 감지 100·2000ms로 총 216개다.
heartbeat 10ms, timer 1ms, 전파 1ms, 주 요청 deadline 2초, crash 관측 상한 5초를 유지했다.
준비 단계 이력·빈 링크에서의 측정 시작·클라이언트 탐색·바이트 집계·KV 보존 검사는 동일하다.

원본의 variant `VR`은 FULL_LOG, `VR_INCREMENTAL`은 개선 모드다. PB는 대조군이다.
개선 전 실행도 같은 최종 빌드에서 다시 돌려 원래 CSV와 바이트 단위로 같음을 확인한다.

## 검증

기존 VR 안전성 테스트를 FULL_LOG와 INCREMENTAL 모두에 적용한다.
개선 모드는 재전송 간격이 길어져 유실 시 즉시 따라잡기를 가정하지 않으며, 동일 요청을 재시도하면서 제한된 시간 안에 성공·수렴하는지 검사한다.
반복 분단 테스트는 유실·중복·순서 역전을 계속 주입한다. 새 모드의 정상성 주장은 유한한 주입 조건의 검증이며 형식 증명이 아니다.

추가 검사는 idle heartbeat에 로그 payload가 없는지, 새 쓰기 비용이 준비 이력 길이에 비례하지 않는지, 누락 prefix를 건너뛰어 적용하지 않는지, ACK 없는 send 완료가 전송 슬롯을 비우지 않는지, 재전송 간격이 늘어나는지, 과거 8개 미완료 조건이 같은 5초 내 끝나는지를 확인한다.

## 실행

저장소 루트, JDK 21:

```powershell
.\gradlew.bat build :experiment-runner:installDist
.\experiment-runner\build\install\openkache-experiment-runner\bin\openkache-experiment-runner.bat experiments/a1-active-passive-failover/network-optimization/comparison-quick.properties
.\experiment-runner\build\install\openkache-experiment-runner\bin\openkache-experiment-runner.bat experiments/a1-active-passive-failover/network-optimization/comparison.properties
```

quick은 64조건을 두 번 반복하고, full은 216조건을 한 번 실행한다. JSON 원본에서 CSV·보고서를 재생성한다.
FULL_LOG/INCREMENTAL 원본이 한 run에 섞이면 coverage 검증에서 실패한다.

```powershell
java -cp "cache-core/build/classes/java/main;cache-core/build/classes/java/test" io.openkache.core.replication.experiment.NetworkCostResearchMain --rebuild "<repeat-001 폴더>"
```

## 범위

view change와 뒤처진 노드의 상태 전달은 여전히 전체 로그를 보낸다. 로그 크기에 따른 crash 비용까지 없어진 것은 아니다.
선택한 최대 프레임은 약 0.51MiB이며, 이 범위의 대역폭·deadline·재전송 설정으로 관측한 결과다. 이보다 큰 이력·장시간 부하에 대한 보장은 별도 검증이 필요하다.
유실 시 긴 재전송 간격 때문에 개별 요청은 TIMEOUT일 수 있다. 클라이언트는 같은 요청 식별자로 재시도한다.
실제 TCP/SSD·NIC 공유·restart recovery·checkpoint·membership 변경은 이번 작업에 포함하지 않았다.

[개선 전후 결과](results.md)
