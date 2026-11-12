# 공통 correctness 이력·ACK 감사 — 단계 2-A

기준일: 2026-11-02. O00 및 V01–V07의 공통 실행기에 전수 이력 decorator, 제한된 오프라인 선형성 검사, 별도 예약 키 ACK 원장·감사를 연결했다. 원본/파생 replication 알고리즘과 공통 core API는 변경하지 않았다. 실제 기능 검증 범위와 전체 회귀 결과는 `CORRECTNESS_VALIDATION.json`을 따른다.

## 제공하는 코드와 사용 순서

- `experiment-runner/src/benchmark/java/io/openkache/runner/benchmark/correctness/`: `HistoryRecorder`, `RecordingBenchmarkClient`, `HistoryChecker`, `AckLedger`.
- 같은 benchmark sourceSet의 `measurement/CorrectnessRunWriter`: payload/결과/검사 증거를 새 디렉터리에 저장.
- `cache-server/src/benchmarkTest/java/io/openkache/server/benchmark/variants/CorrectnessCluster`와 `CorrectnessBenchmarkTcpTest`: 기존 실제 Node·adapter·TCP fixture를 재사용한 통합 검증.

1. 고유 run/slot과 같은 monotonic clock 식별자로 recorder를 만든다. operation/submission/payload 용량을 명시한다.
2. 기존 adapter를 `RecordingBenchmarkClient`로 감싸 기존 closed/open-loop 실행기에 전달한다. drain probe는 실제 원본 adapter를 관찰한다. 생성기·프로토콜·네트워크 경계는 유지한다.
3. 실행기 종료·cleanup 뒤 `seal(System.nanoTime())`로 불변 snapshot을 만든다. close 자체는 snapshot을 봉인하지 않는다. 늦은 완료가 drain 중 기록될 수 있게 한다.
4. 새 격리 저장소의 빈 초기 상태는 `Map.of()`로 명시한다. 기존 값이 있으면 정확한 초기 Map을 제공한다. 초기 상태를 모르면 null을 전달해 INCONCLUSIVE로 둔다.
5. correctness writer에 snapshot·초기 상태·checker 한도와 선택적 ACK 증거를 전달한다. 기존 성능 결과의 `not_evaluated`를 자동 변경하지 않는다.

예시 결합은 `CorrectnessBenchmarkTcpTest`를 따른다. CLI/EC2 launcher를 새로 제공한 것은 아니다.

## 이력 수집 계약

논리 ID에 대한 모든 submit 시도와 실제 key/SET value/GET result의 Bytes를 보존한다. 재입장 거절 후 재시도와 active duplicate future는 같은 논리 작업의 여러 제출로 기록한다. 같은 ID에 다른 command나 서로 다른 terminal 결과가 붙으면 유효한 이력으로 인정하지 않는다.

용량 초과는 delegate 호출 전 거부하거나 응답 수집 실패로 드러낸다. 표본 추출/조용한 eviction으로 바꾸지 않는다. pending·예외·거절·미응답도 남긴다. 봉인 뒤 snapshot은 바뀌지 않으며 late callback 수는 recorder에서 별도 관찰할 수 있다. 봉인 당시 pending은 완료로 소급하지 않는다.

전수 payload 수집과 오프라인 탐색은 correctness 전용이다. 성능 mode와 관찰 비용이 같다고 가정하지 않는다. 로컬 base64 payload 파일은 Notion이나 외부 서비스에 업로드하지 않는다.

## 검사 의미와 한계

검사 모델은 단일 키 GET/SET/DELETE와 정확한 Bytes 상태다. GET은 현재 값 또는 MISS, SET은 OK, DELETE는 기존 값이 있을 때 OK/없을 때 MISS여야 한다. 여러 키의 operation도 한 전역 순서로 탐색하며 완료한 작업이 다음 호출보다 앞선 real-time 제약을 지킨다. 같게 관측된 ns timestamp는 동시로 취급한다.

검사 시각은 단일 adapter의 논리 invoke/terminal clock 영역이다. 서로 다른 JVM의 nanoTime을 그대로 합치지 않는다. client publication·원격 clock 정렬을 포함한 분산 수집은 후속이다.

- PASS: 알려진 초기 상태·수집 무결성·확정된 성공 결과에 대해 한도 내 합법 순서를 찾음. 모든 가능한 실행에 대한 프로토콜 증명은 아니다.
- VIOLATION: 허용한 미확정 완료 확장까지 탐색했지만 관측 결과를 설명할 순서가 없음.
- INCONCLUSIVE: 초기 상태 미확인, 수집 오류/미완료, 시간·상태·작업 수 한도, interrupt 또는 합법 순서는 있지만 결과가 불확실한 요청이 있음.

미응답/미완료 쓰기는 실행되지 않았거나 나중에 실행될 수 있다. logical timeout을 원격 abort로 가정하지 않고 optional effect로 다룬다. 불확실한 요청이 포함된 합법 확장은 PASS가 아니다. admission 거절만 받은 요청은 효과가 없는 것으로 제외한다.

최대 checker operation 24개, 탐색 상태 1천만, 탐색 시간 1분의 하드 상한이 있다. 테스트는 더 작은 24개/20만 상태/2초를 사용한다. 따라서 본시험의 천만 요청 전수를 이 탐색기로 처리할 수 있다고 주장하지 않는다. 대규모 correctness campaign의 분할 경계·독립성/확정 초기 상태와 별도 검사기 확장은 후속이다. TTL·transaction·동일 값을 물리적으로 중복 기록한 부작용까지 판별하는 모델은 아니다.

## 예약 키 ACK 원장

일반 혼합 workload의 덮어쓰기를 손실로 오인하지 않도록 audit 전용 배타적 namespace를 쓴다. 배타성은 호출자의 명시적 attestation이며 도구가 외부 접근 부재를 발견해 증명하는 것이 아니다.

ledger window 내 key당 mutation이 정확히 하나이고 성공 ACK를 받은 SET/DELETE만 포함한다. SET은 정확한 payload, DELETE OK/MISS는 부재를 기대한다. DELETE 초기 값은 window 전에 다른 ID namespace로 준비할 수 있다. 덮어쓰기·불확실한 추가 mutation·미확인 namespace·비정상 ACK는 제외 사유를 남기며 손실로 단정하지 않는다.

감사는 같은 clock의 ACK 이후 READY barrier와 그 뒤의 성공 GET을 요구한다. 읽기 누락/중복/실패·clock 불일치·제외 키가 있으면 coverage 불완전으로 표시한다. 확정된 예약 SET의 MISS/값 불일치 또는 DELETE 값 부활은 VIOLATION이다. writer는 ledger와 audit의 history token 및 실제 read 증거 재계산 결과를 대조해 무관한/임의 PASS 증거 결합을 거부한다.

이번 통합 감사는 모든 복제본 적용 후 follower 하나를 정상 close하고 같은 파일로 재기동하여 생존 다수로부터 복구한 뒤 수행한다. 복구 준비 gate와 정상 상태용 READY probe는 구분한다. 실제 서비스 GET의 ACK 값/삭제 상태를 확인하며, 모든 audit GET이 재기동한 replica에서 직접 처리되었다는 주장은 하지 않는다. V01은 현재 Leader를 읽는다.

이는 ACK 직후 apply 전 crash, leader kill, SIGKILL, 네트워크 분리, 다중 장애, 전원 손실 검증이 아니다. 독립 replica RAM durability log, stable View의 영속화, 선택적 KV fsync/force의 의미를 유지한다.

## 저장 파일과 검증

새 디렉터리에 다음을 저장하며 기존 경로는 덮어쓰지 않는다.

| 파일 | 내용 |
| --- | --- |
| history.jsonl | 모든 논리 요청·submit/callback·결과·attempts·ID/시각·원본 payload base64/length/SHA256 |
| correctness.json | 명시 초기 상태·수집 및 탐색 한도·검사 verdict/이유/witness·범위 |
| ack-ledger.json | 예약 mutation 기대 상태·ACK 시각·제외 사유 또는 not_run |
| ack-audit.json | READY/barrier·실제 GET·각 항목 일치 여부/verdict 또는 not_run |
| integrity.json | 위 4파일 SHA256, history token·verdict·수집 완전성 |
| STARTED / COMPLETE | 저장 시작 및 integrity hash가 연결된 저장 완료 표시 |

COMPLETE는 증거 저장 완료다. VIOLATION/INCONCLUSIVE 결과도 정상적으로 저장하며 COMPLETE를 correctness PASS로 해석하지 않는다. atomic move를 사용하지만 결과 파일의 fsync/전원 손실 보장까지 제공하지 않는다.

`validate_correctness.py`는 Python 3 표준 라이브러리만 사용해 marker/hash·payload·ID/clock·계수·성공 witness의 실제 Bytes replay와 real-time 순서·ACK 증거를 독립 대조한다. exit 0은 artifact 형식/증거 대조 통과이며 서비스 PASS와 다르다. VIOLATION을 새로 전역 탐색해서 증명하는 두 번째 checker는 아니다.

```powershell
$env:JAVA_HOME='C:\Users\SSAFY\.jdks\ms-21.0.11'
$env:PATH="$env:JAVA_HOME\bin;$env:PATH"
.\gradlew.bat :experiment-runner:benchmarkTest --tests '*correctness*' --offline --console=plain
.\gradlew.bat :cache-server:correctnessBenchmarkTest --offline --console=plain
python .\experiments\replication-benchmark-v1\validate_correctness.py <NEW_CORRECTNESS_RUN_DIRECTORY>
.\experiments\ionia-validation\Run-Validation.ps1 -Mode Full -Repeat 1 -PortWaitSeconds 600
```

새 correctness TCP task는 기존 variantBenchmarkTest 뒤에 실행한다. 기존 benchmark74·variant99의 검사 조건·병렬/timeout은 변경하지 않는다. Windows 새 그룹에도 기존10% 포트 회복 gate를 적용한다.

## 다음 단계

이번 구현은 공통 정확성의 로컬 부분이다. M28/M29의 분산/본시험 수집과 표준 성능 result 통합, host/JVM·protocol/storage 지표, 1초 창, clock/observer overhead, V06 cold/steady 학습·워밍업 정책, remote 관찰은 완료 처리하지 않는다. 이후 본시험용 공통 데이터/요청열 후보와 EC2 재개 시 inventory/pilot을 진행한다. EC2·정식 성능 비교·commit/push/merge는 이번에 실행하지 않는다.
