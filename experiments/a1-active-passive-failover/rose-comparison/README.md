# Rosé 기반 bounded-lag 변형

[Rosé (CIDR 2026)](https://vldb.org/cidrdb/papers/2026/p8-zarkadas.pdf)의 bounded queue/backpressure를 단일 파티션에 적용한 연구 변형이다. **논문 전체 구현 또는 다중 파티션 coordinated apply의 재현이 아니다.** 코드·원본 variant도 `ROSE_ADAPTATION`으로 구분한다.

## 논문과 이번 구현의 경계

논문은 여러 파티션의 snapshot과 복제 진행을 조정한다. 이번 구현은 Primary 1대·Backup 2대, 파티션 하나, 명령 하나를 적용 경계로 둔다. 쓰기는 SET/DELETE이며 각 Backup은 Primary와 같은 순서의 완전한 명령 prefix만 적용한다. 실험 지연 단위는 논문의 epoch가 아닌 **op 개수**다.

다중 파티션 snapshot 서비스·coordinated apply·MVCC·트랜잭션·지역 복제·영속 WAL·자동 backpressure 해제는 포함하지 않았다. 따라서 이 결과로 논문의 전체 consistency, 지역 장애 시 유실 상한, failover 후 제품 처리량 유지 등을 검증했다고 주장하지 않는다.

## 상한과 응답

|설정|수락·응답|검사|
|---|---|---|
|L=0|비교용 전원 적용 ACK 동기 모드|성공 쓰기 유실 0|
|L>0|예약 로그 길이 - 최소 Backup 적용 ACK가 L 미만일 때 수락. 로컬 적용 후 OK|예약 lag≤L, 성공 쓰기 유실≤L|
|L=-1 (∞)|상한 없는 비동기 제어군. 로컬 적용 후 OK|유실 상한 없음|

양수 L은 **성공 쓰기뿐 아니라 수락했지만 아직 응답하지 않은 로그 예약까지 포함**해 검사한다. L=0은 미응답 쓰기 한 건이 복제 중일 수 있어 관측 peak lag가 1이어도 성공 쓰기 유실은 0이다. ACK는 실제 KV 적용 이후에만 보낸다.

가득 차면 아직 수락하지 않은 요청 한 건을 대기시킨다. 추가 다른 요청은 BUSY다. deadline이 지나면 그 대기 요청은 로그에 넣지 않는다. 반면 이미 수락·적용한 요청의 TIMEOUT은 취소가 아니다. deadline과 ACK가 같은 시각에 오더라도 만료된 대기 요청을 수락하지 않는 경계를 테스트했다.

Primary의 로컬 KV 작업은 한 번에 하나만 처리한다. **원격 미확인 로그를 최대 L건 허용하는 것은 이 변형의 핵심 설정**이며, 다른 네 방식에 generic pipeline을 추가한 것은 아니다. client/operation 중복 응답, 다른 명령의 ID 재사용 거부, 낮은 소유권 epoch 거절을 제공한다. Backup의 일반 CacheEngine 읽기는 Primary로 Redirect한다.

## 장애 후 재구성

외부 실험 master는 원래 Primary의 정확한 영구 crash를 200ms 뒤 통지받는다. 살아 있는 분단 노드는 제거하지 않는다.

1. 두 생존 Backup을 모두 freeze하고 이미 수락한 KV 적용을 끝낸다.
2. **가장 긴 적용 prefix**를 가진 Backup을 승격한다. 동률이면 NodeId가 작은 쪽이다.
3. 새 epoch에서 peer 전송으로 느린 Backup의 suffix를 따라잡힌다.
4. 생존자 로그·적용·새 Primary의 ACK 일치 후 입장을 연다. freeze/open 조정 각각 2ms를 모델링한다.

이는 PB 기준형의 NodeId 고정 승계와 다른 선택이다. 이미 적용한 KV를 rollback하거나 좋은 생존 로그를 버리지 않기 위해 정했다. master 합의·통신 bytes·감지 오판 비용은 포함하지 않는다. 두 Backup이 모두 메모리를 유지하는 **단일 Primary crash** 범위에서 검증하며 재시작·다중 장애·membership 추가는 제외한다.

## 실험 240조건

L `{0,1,10,100,1000,∞}` × 5시나리오 × 제출 간격 `{1,5}ms` × 값 `{64B,8KiB}` × 방향별 `{1,10}MiB/s`다. quick은 L `{0,10,∞}`, 간격 1ms·링크 1MiB/s의 30조건을 두 번 실행한다.

입력은 0~2초 open-loop이며 서로 다른 ID/key의 SET을 매번 새로 제출한다. 요청 deadline은 500ms, 같은 요청을 재시도하지 않고 결과의 epoch·Redirect로 **이후 새 요청**의 경로만 바꾼다. 최대 30초 더 drain한다.

|시나리오|주입|
|---|---|
|NORMAL|장애 없음|
|SLOW_BACKUP|0.5~1.5초 Backup 1의 Append 전달 +200ms|
|PARTITION_REPAIR|0.5~1.0초 Primary 격리 후 repair|
|PRIMARY_CRASH|1.2초 Primary 영구 종료|
|ISOLATED_CRASH|1.0초 Primary 격리, 1.2초 영구 종료|

네트워크는 기존 방향별 FIFO 직렬화+1ms 전파 지연이다. 데이터 전송 창은 Backup별 한 메시지, 재전송 간격 1초다. 논리 Append 크기는 `52B + command`, Applied ACK는 36B다. CPU·로컬 KV 지연이 0인 모델이므로 비동기 0ns는 실제 성능 숫자가 아니다.

## 지표

- OK/제출, BUSY·TIMEOUT·UNAVAILABLE·STALE_EPOCH·Redirect를 모두 기록한다. 성공 지연 p50/p99만으로 가용성이나 처리량을 판단하지 않는다.
- peakLag는 두 세대 Primary에서 측정한 최댓값이다. finite L의 예약 lag와 유실 상한을 매 조건 검사한다.
- lostPreCrash는 crash 전 OK를 받았으나 최종 새 Primary prefix에 없는 **쓰기 ID 수**다. retained 성공 ID·명령·값과 현재 Primary의 실제 KV도 검사한다.
- firstPostCrashOkNs는 crash 후 제출한 새 요청의 성공 callback이다. 2초 뒤 새 probe를 추가하지 않으므로 미관측을 영구 복구 불능으로 해석하지 않는다.
- converged는 생존자의 로그 길이·적용·ACK 및 live 데이터 전송 완료를 확인한 뒤 실제 prefix까지 비교한 값이다.
- 시계열은 10ms마다 성공 수·현재 Primary·lag·대기 수·각 Backup 적용 위치를 남긴다. 전환 중 lag=-1은 미정이며 0이 아니다.

기존 PB/VR/Raft/Chain 288조건은 closed-loop·동기/과반수 성격의 별도 실험이다. 이번 240조건과 합쳐 **다섯 방식의 같은 조건 성능 순위**를 만들지 않는다. 이 실험은 동일한 엔진에서 L을 바꾼 trade-off 비교다.

## 실행과 원본 재생성

```powershell
.\gradlew.bat build :experiment-runner:installDist
.\experiment-runner\build\install\openkache-experiment-runner\bin\openkache-experiment-runner.bat experiments/a1-active-passive-failover/rose-comparison/comparison-quick.properties
.\experiment-runner\build\install\openkache-experiment-runner\bin\openkache-experiment-runner.bat experiments/a1-active-passive-failover/rose-comparison/comparison.properties
```

공통 Runner가 source/backend hash와 설정·실행 명령을 기록한다. `results/*.json`이 schema `a1-rose-json-1`의 원본이다. 원본에서 `summary.csv`, `samples.csv`, `results.md`를 생성하며 누락·중복·schema·보존/상한/응답 count 위반 시 완료 표시를 제거한다.

```powershell
java -cp "cache-core/build/classes/java/main;cache-core/build/classes/java/test" io.openkache.core.replication.experiment.RoseResearchMain --rebuild "<repeat-001 폴더>"
```

generic pipeline·slow replica 전송 정책·checkpoint는 여전히 멘토 질문이다. 다중 파티션 coordinated apply까지 확장할지와 보장 수준이 다른 변형의 비교 묶음도 멘토에게 판단받을 사항으로 남긴다.
