# V02a History/LTI 구현 및 기능 검증

2026-10-28. 기준 revision: `99d25a3805081d84296d284a1ce8e9b00cbc3205`.

[확정 설계 v1.0](https://app.notion.com/p/4074ed90243781458022c5ca311424ca) · [착수 전 근거 감사](https://app.notion.com/p/4074ed90243781e4b939e923f9b7f055) · [IONIA §4.2.2·4.4·5.6](https://www.usenix.org/system/files/fast24-xu.pdf).

## 변경과 보존

V02a는 정석 O00의 History 보존량만 조절하는 별도 구현이다. V01 Leader-only Read와 합치지 않는다. 기존 `ionia/`와 `ionia-second/`의 소스·테스트·서버·설정은 변경하지 않았다. 기존 파일 수정은 core Gradle 소스 경로 등록과 개발 기록 추가뿐이다. 기준 시점의 보호 대상 146개 파일 hash가 동일함을 확인했다.

- 코어 보관: `cache-core/src/main/java/io/openkache/core/replication/ionia-third/`, 그 아래 별도 `src/main/java`·`src/test/java` 소스 루트. Java package `io.openkache.core.replication.ionia_third`.
- 새 정책 `IoniaHistorySettings`·`IoniaBudgetHistory`와 엔진 `IoniaThirdEngine`을 분리했다. 엔진은 원본 final/private 구조 때문에 별도 사본이며 변경은 History 타입·설정·정리 호출·불변 진단으로 제한했다. 이후 원본 수정이 자동 전파되지는 않는다.
- 정석 `IoniaClient`, `IoniaConfig`, `IoniaLog`, `IoniaMessage`, `IoniaRecovery`, `Progress/ViewInfo/Status`, 저장·TCP·wire 어댑터와 공통 API를 그대로 재사용한다.
- server의 `ionia_third`에는 새 엔진과 profile을 연결하는 `IoniaThirdNode/Main/ClientMain`을 둔다. 공통 API·외부 의존성·Workload Generator는 변경하지 않았다.

## 정책과 설정

`history.target.entries`는 시작 시 반드시 지정하는 0 이상 int다. 실행 중 변경하지 않는다. N은 서로 다른 key의 최신 수정 index 항목 수이며 heap/RSS·로그·KV 데이터의 강제 상한이 아니다.

- N=0: 원본 safe prefix trim 대조군. 정리할 항목이 없어도 안전 상한 S가 LTI보다 크면 LTI를 S로 전진시킨다.
- N>0, entries≤N: 보존하고 LTI도 유지한다.
- N>0, entries>N: active Follower와 Leader의 연속 적용 prefix로 제한한 S 이하에서, 목표에 필요한 최소 prefix만 정리한다. 같은 cutoff의 항목은 함께 제거한다.
- 안전하게 지울 수 없으면 N 초과를 허용한다. 메모리 예산 때문에 active 판정·ACK/quorum·로그/dedup·KV 데이터를 변경하지 않는다.
- 원본과 같은 1초 owner tick에서 판단한다. pending SET/DELETE·DATA 응답·Follower 선캡처·Client의 INDEX 비교·View fencing·재구성 순서를 유지한다.

profile은 UTF-8 `v02a|targetEntries=<N>|trimIntervalNanos=1000000000`의 SHA-256 소문자 hex 64자리다. 서버·클라이언트·안정 View 저장은 기존 configuration 뒤에 `|algorithm=V02a|profile=<hash>`를 결합한다. 다른 N 또는 O00와의 wire 혼용은 거절하며, 기존 디렉터리의 profile이 다르면 KV 파일을 열기 전에 거절한다. 자동 삭제·초기화하지 않는다.

진단 `historyDiagnostics()`는 불변 snapshot이다. 현재 View/generation과 마지막 정리 판단의 View/generation을 구분한다. N·entries·overshoot·LTI·정리 호출/삭제 누계·판단 당시 S/시각·cutoff·사유를 제공한다. eligible 미계산은 0이 아닌 빈 값이다. rebuild 시 이전 보고·판단을 초기화한다. N=0의 overshoot는 목표 위반 개념이 없어 0이며 실제 entries를 함께 본다. 실제 heap, Client 경로 비율, Prometheus 및 공통 비교 계측은 후속이다.

## 실행

JDK 21을 사용한다. 저장소 루트에서:

```powershell
.\gradlew.bat build :cache-server:installDist --rerun-tasks --console=plain
```

서로 다른 세 터미널에서 실행한다.

```powershell
.\experiments\ionia-third\start-node.ps1 -Node 1
.\experiments\ionia-third\start-node.ps1 -Node 2
.\experiments\ionia-third\start-node.ps1 -Node 3
```

다른 터미널에서:

```powershell
.\experiments\ionia-third\client.ps1 -Operation set -Key hello -Value world
.\experiments\ionia-third\client.ps1 -Operation get -Key hello
.\experiments\ionia-third\client.ps1 -Operation delete -Key hello
```

예제는 포트 17301–17303, N=1024, `build/ionia-third/target-1024/node-*`를 사용한다. 1024는 실행 예시이며 적정값 측정 결과가 아니다. N을 바꿀 때 세 설정과 데이터 경로를 함께 바꾸고 기존 데이터는 보존한다. 개별 재시작은 같은 profile·디렉터리를 유지하며 Survivor majority에서 복구한다.

Linux/직접 진입점 예시:

```bash
java -cp 'cache-server/build/install/openkache-server/lib/*' io.openkache.server.ionia_third.IoniaThirdMain experiments/ionia-third/node-1.properties
java -cp 'cache-server/build/install/openkache-server/lib/*' io.openkache.server.ionia_third.IoniaThirdClientMain experiments/ionia-third/node-1.properties get hello
```

CLI는 정석 Follower+Meta 및 majority View 확인을 사용한다. 초기 Follower를 고정하므로 View 교체 후 해당 대상이 Leader가 될 수 있으며 균등 분산 정책은 아니다. 10초 총 deadline 안에서 discovery·UNAVAILABLE/NOT_PRIMARY를 재시도한다. 라이브러리 조립에서는 타이머 호출과 View 갱신을 호출자가 담당한다. TIMEOUT은 쓰기 미적용을 확정하지 않는다. 재시도는 같은 논리 ID와 payload를 유지한다.

## 실제 검증

전체 build와 installDist를 `--rerun-tasks`로 실행해 **257개 통과**, 실패·오류·skip 0을 확인했다. core 195 / server 46 / runner 16. V02a 소스에서 62개가 추가됐다(기존 O00 fixture를 새 엔진에 연결한 회귀 32개 포함). production jar의 새 진입점 포함 및 테스트 클래스 미포함도 확인했다. [집계 JSON](IONIA_V02A_VALIDATION.json).

| 설계 인수 항목 | 구현한 검사 |
| --- | --- |
| V02-01 | N=0을 원본 History와 64 seed×200단계 대조: rewrite/report/active subset/trim/rebuild |
| V02-02–08 | 0/1/2/MAX·설정 거절, 목표 아래/동일/초과, S/LTI 경계, unknown active=0, 부족한 prefix, 동률·재수정·S 회귀 |
| V02-09–10 | 과거 VALUE/MISS 선캡처 뒤 apply/trim, Client 재조회, pending SET/DELETE DATA, DELETE OK/MISS |
| V02-11–13 | 적용 gap·키별 FIFO, View rebuild·옛 보고 차단, 이전 apply 종료 뒤 reset, 3/5노드 ACK·fallback·DAG·dedup·재합류 |
| V02-14 | N=0/1/2/MAX 각각 64 seed×10개 완료 CRUD 이력, 오류 검출 대조군, 결과 불명확 SET의 복구·동일 ID 재시도 |
| V02-15 | 실제 Follower+Meta 호출 계수, Leader-only와 혼합하지 않음 |
| V02-16 | TCP 32 hot-key 동시 SET 수렴, 실제 1초 trim, profile 혼용/디렉터리 변경 거절, 독립 JVM 3개 강제 Leader 종료·재시작·CLI CRUD |

추가 Java 상태 불변식 검사는 N=0/1/2/MAX ×64 seed×80단계에서 누락 key index 과소평가와 LTI 감소를 검사한다. 유한 이력 checker는 로그용으로 앞부분만 표시하는 `Bytes.toString()` 대신 원래 Bytes map의 내용 동등성으로 탐색 상태를 구분한다. 32바이트 이후만 다른 값의 대조군도 포함했다.

미완료 논리 호출은 결과 불명확 상태로 기록하고, 일부를 완료 확장하거나 생략하는 유한 탐색을 수행한다. 재시도 attempt를 별개 업무 연산으로 중복 삽입하지 않는다. 이는 일반 장애 이력 수집 프레임워크나 전체 분산 실행 증명이 아니다.

한 차례 전체 실행에서 Replica 관측용 직접 GET의 연결이 거절됐다. 이 관측 요청만 같은 요청 ID·총 5초 한도에서 ConnectException을 재시도하도록 보완했다. 매 시도 시 엔진 failure/normal 상태를 검사하고 지속 오류·다른 예외·응답 불일치는 실패한다. 서버/클라이언트 알고리즘에 성공을 꾸미는 재시도는 추가하지 않았다. 최종 전체 실행은 통과했다.

## 남은 범위

기능·장애 검증 단계다. 공통 데이터셋·비교 driver·throughput·p50/p99·network amplification·실측 heap/RSS·GC·SSD I/O·다중 물리 머신·power-loss 검증은 실행하지 않았다. History 삭제가 HashMap capacity나 전체 RAM을 즉시 줄인다는 보장도 없다.

내구성은 독립 Replica의 RAM 로그 복제이며 안정 View 메타데이터 저장과 선택적 KV force를 구분한다. per-request WAL+fsync D-PERSIST는 미구현이다. 전체 Replica RAM 동시 손실, 전체 로그 replay 비용, 64 MiB 상태 전송 한도 등 정석의 제한을 유지한다. Git commit/push는 사용자가 진행한다.

## 공통 benchmark adapter 연결 후속 (2026-10-30)

현재 표시 ID V02의 실제 알고리즘을 server benchmark/ionia_third adapter에 연결했다. 위 과거 문서명·원래 profile 표기/최초 알고리즘 검사 기록은 보존한다. 기존 알고리즘/core API/wire는 변경하지 않고 공통 ID/deadline/retry·trace·closed/open-loop·전수 지연·로컬 표준 export를 재사용한다.

V02의 실제3/5노드·두 실행 방식·읽기/혼합8조건과 별도 CRUD 값·DELETE OK/MISS·재삽입을 검증하고 결과8개를 schema/hash·histogram/설정으로 대조했다. 여섯 방식 합계 추가99/전체760검사 통과이며 개별 방식의99검사라는 뜻은 아니다. ../experiments/replication-benchmark-v1/ADAPTERS_V02_V07.md 및 REMAINING_ADAPTERS_VALIDATION.json/REMAINING_ADAPTERS_SOURCE_PRESERVATION.json/REMAINING_ADAPTERS_ARTIFACT_CHECK.json을 따른다.

V06 학습의 phase 분리·V07 coalescer의 client별 범위, ready manifest/서버 inventory·공통 정확성/필수 계측·clock/overhead/remote 관찰을 정식 성능 전 보강해야 한다. 본시험 데이터/요청열 후보·pilot과 EC2 접속/진단·성능 비교는 후속이며 commit/push/merge는 하지 않았다. RAM log·stable View/선택적 KV force와 RPC별 socket 정책을 유지한다.
