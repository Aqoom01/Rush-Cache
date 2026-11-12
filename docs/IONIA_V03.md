# V03 Prepare 배치 제어 — 구현 및 기능 검증

2026-10-28. 기준 revision: `36920e82fcb55e69e418723a97bee1d381e83267`.

[설계 v1.1](IONIA_V03_DESIGN.md) · [문헌 검토와 미확인 범위](IONIA_V03_RESEARCH.md) · [실행 결과 JSON](IONIA_V03_VALIDATION.json) · [노션 V03](https://app.notion.com/p/4074ed90243781c39428e82a04e0ecff).

## 구현과 보존

`replication/ionia-fourth/`에 별도 main/test 소스 루트를 두고 Java package `io.openkache.core.replication.ionia_fourth`를 사용한다. V01 Leader-only Read·V02a History 예산 정책을 합치지 않는다. 기존 보호 대상 168개 파일의 SHA-256이 기준과 같았다. 기존 파일 변경은 core Gradle의 새 소스 경로 등록과 개발 기록·V03 설계의 현재 상태 갱신이다. 공통 API·WorkloadGenerator·외부 의존성·기존 방식의 소스/테스트/설정은 보존했다.

- `IoniaBatchSettings`: 필수 세 설정의 검증, SHA-256 profile, wire/stable configuration.
- `IoniaPrepareSizer`: 외부 4-byte 길이를 포함한 기존 wire v1의 packet 크기, modified UTF-8, 기록 ACK 전 단일 쓰기 크기 검사.
- `IoniaBatchPlanner`: owner 전용 연속 prefix 선택·첫 ordering 시각·논리 flight·재전송·urgent barrier·누적 시도 통계. 전체 suffix 복사 없이 기존 log.entry(index)를 사용한다.
- `IoniaFourthEngine`: O00 final/private 경계 때문에 분리한 사본이다. 배치·admission·동기 barrier·ACK 후 전진·예약 작업 및 callback fencing·진단을 연결했다. 원본 수정이 자동 전파되지 않으므로 이후 수동 대조가 필요하다.
- `server.ionia_fourth`의 Node/Main/ClientMain은 새 정책만 조립한다. 원본 Client/Config/Log/History/Recovery/Message/Progress/ViewInfo/Status, TCP/wire·파일 KV·stable View를 재사용한다.

## 설정과 동작

| 설정 | 범위·의미 |
| --- | --- |
| prepare.max.entries | 1–1,000,000; Prepare의 연속 항목 수 상한 |
| prepare.max.encoded.bytes | 1–67,108,868; 외부 길이를 포함한 목표 packet bytes |
| prepare.max.delay.ms | 0 또는 20–60,000 정수 ms; 첫 ordering 이후 추가 모으기 대기 목표 |

모든 값은 시작 시 필수이며 실행 중 바꾸지 않는다. 예제 E128/B65536/D20ms는 최적 설정이 아니다. 각 Follower에 논리 미확인 구간 Wf=1, retry 20ms, node tick 20ms를 사용한다. 논문의 global 미결정 합의 인스턴스 WND와 다르다. TCP는 원본대로 메시지마다 새 연결을 만들며, 재전송 socket/가상 스레드가 겹칠 수 있다.

선택은 `matched+1`부터 count/bytes로 자른다. 꽉 찬 prefix, D=0, `now-firstOrderedAt >= D`, urgent 또는 retry가 발송 조건이다. 첫 ordering 시각은 신규 도착·같은 ID 재시도·재전송으로 초기화하지 않는다. 빈 batch를 발송하지 않는다. 목표 B보다 큰 유효 단일 항목은 쪼개지 않고 단독 발송하며 초과를 센다.

전체 길이는 `52 + modifiedUTF(configuration) + Σ(entryBytes)`이고 SET은 `33+key+value`, DELETE는 `29+key`다. hard wire body 64MiB에는 외부 4바이트가 포함되지 않는다. 파일 KV는 `17+key+value <= 32MiB` payload를 요구한다. 모든 Replica는 단일 쓰기 크기를 log.record/기록 ACK **전에** 검사해 초과를 CAPACITY_EXCEEDED로 거절한다. 이는 O00와의 명시적인 입력 admission 차이이며 실제 남은 disk/RAM 공간의 예약 보장은 아니다. 원본 Client의 fast 경로는 이 상태를 직접 종결하지 않으므로 timer 기반 slow fallback에서 최종 상태를 받을 수 있다.

로컬 send 완료는 matched를 올리지 않는다. 현재 View·membership·leader log 상한 검사를 통과한 ACK만 반영하며, 전진한 ACK 뒤 다음 chunk를 즉시 판단한다. 중복 ACK는 반복 발송 loop를 만들지 않는다. 부분 ACK·긴 복구 prefix의 누적 ACK를 허용하며, 재전송은 현재 matched+1부터 다시 고른다. 첫 전송 시각은 peer/index별로 보존해 부분 ACK와 확장된 재전송이 겹쳐도 잘못 상속하지 않는다.

Fast SET의 기록 ACK와 합의·KV 적용은 계속 분리된다. Slow SET·DELETE·pending Leader GET/Meta DATA는 필요한 prefix를 urgent로 만들고, ACK마다 다음 chunk까지 전진한다. 적용 완료 GET이 무관한 미래 쓰기를 모두 force하지 않는다. urgent는 모으기 대기만 우회하므로 기존 flight/ACK·quorum·commit·apply 대기는 남는다. D와 일정 RTT 내 전체 응답 완료를 동일시하지 않는다.

View/recovery fencing은 flight/예약 flush/local stage 관측을 비우고 별도 send epoch를 갱신한다. 같은 View에서 startRecovery가 발생해도 이전 stage callback이 새 카운터를 차감하지 못한다. KV apply의 generation과 구별한다. 복구는 원본의 old apply drain → reset → authoritative replay → normal 순서를 유지하고, 복구된 tail은 urgent로 보낸다. wire ACK에 로컬 epoch/generation 필드를 추가하지 않았다.

## 관측 의미

`batchDiagnostics()`는 owner에서 발행한 불변 snapshot이다. View/generation·Leader/status·monotonic sampledAt·profile·설정, Prepare 시도 수/항목/encoded bytes/재시도/단독 초과, peer별 matched/flight/oldest ordering age, ordered−committed·committed−applied를 제공한다. flight는 범위·해당 첫 항목 최초 전송·마지막 전송 시각을 구분한다.

발송 원인은 `RETRY > URGENT > FULL > ZERO_DELAY > AGE` 순서로 하나만 센다. 시도 통계와 원인 누계는 engine 수명 동안 유지하며 flight/시각은 installation 범위다. outstandingSendStages는 현재 send epoch의 Prepare transport Future 수다. maxOutstanding는 engine 수명 내 최대 관측값이고 locallyFailedStages는 현재 epoch일 때 완료가 관측된 실패의 누적이다. 과거 epoch callback은 무시한다. 실제 NIC bytes·socket 수·물리적 내구성 지표가 아니다. Follower의 peer matched 값은 Leader 진행 판단에 쓰지 않는다.

요청 도착/record부터의 종단 지연, commit/apply 시간 분포, 실제 batch 분포·연결 생성 비용·heap/RSS·NIC 실측·Prometheus/공통 비교 driver는 후속이다. 현재 raw 시도 통계만으로 성능 개선을 주장하지 않는다.

## 재현 명령

JDK 21, 저장소 루트에서:

```powershell
.\gradlew.bat build :cache-server:installDist --rerun-tasks --console=plain
```

최초 실행 시 각 노드를 별도 터미널/프로세스로 실행한다. 예제는 포트 17401–17403과 `build/ionia-fourth/entries-128-bytes-65536-delay-20/node-*`를 사용한다.

```powershell
$v03Classpath = 'cache-server/build/install/openkache-server/lib/*'
java -cp $v03Classpath io.openkache.server.ionia_fourth.IoniaFourthMain experiments/ionia-fourth/node-1.properties
# 다른 터미널에서 node-2.properties, node-3.properties로 같은 Main 실행
```

```powershell
$v03Classpath = 'cache-server/build/install/openkache-server/lib/*'
java -cp $v03Classpath io.openkache.server.ionia_fourth.IoniaFourthClientMain experiments/ionia-fourth/node-1.properties set hello world
java -cp $v03Classpath io.openkache.server.ionia_fourth.IoniaFourthClientMain experiments/ionia-fourth/node-1.properties get hello
java -cp $v03Classpath io.openkache.server.ionia_fourth.IoniaFourthClientMain experiments/ionia-fourth/node-1.properties delete hello
```

원본 Follower+Meta 읽기를 사용한다. 같은 실행의 서버·client·stable View는 동일 E/B/D profile을 사용해야 한다. 다른 profile/방식의 기존 데이터 디렉터리는 초기화·삭제하지 않고 거절한다. 모든 Replica를 동시에 종료해 RAM 로그를 잃은 기존 디렉터리를 재실행하면 자동 복구/서비스 재개를 보장하지 않는다.

## 실제 검증

Java 21.0.11에서 전체 build와 installDist를 --rerun-tasks로 성공했다. **329개 통과: core 261 / server 52 / runner 16, 실패·오류·skip 0.** V03에 연결된 테스트는 72개(core 66/server 6)이며 원본 프로토콜 회귀 32개, 공통 worker 및 기존 이력 검사 재사용 등을 포함한다. 새 알고리즘 72개를 검증했다는 뜻이 아니다. production JAR의 새 클래스 포함·test 클래스 제외도 확인했다.

| 인수 항목 | 실제 증거 |
| --- | --- |
| V03-01–02 | entry/byte 경계, oversize·hard limit·admission, 기존 encoder와 320가지 frame 길이/round trip 대조 |
| V03-03·13 | 가상 시계의 저부하 tail·D40ms, 지속 도착·중복 기록에도 첫 age 유지 |
| V03-04·08 | Prepare/ACK 유실·중복, 미래/긴 prefix ACK, 로컬 throw/실패 Future, 느린 Follower 분리·회복 |
| V03-05·07 | 3/5노드 배치 중간 crash, 완료 쓰기 7개 보존, 옛 View ACK·예약 flush·늦은 stage callback·원본 reset/replay 회귀 |
| V03-06·15 | 여러 chunk 뒤 pending Meta/GET, DELETE·slow fallback, 기존 flight/ACK 유실 뒤 barrier drain, 무관한 쓰기 force 금지 |
| V03-09–10 | E1/2/3/128 네 profile×64 seeds×10 CRUD의 유한 이력, 양수 D40/B256 조합, timeout 쓰기의 completion 확장·View 복구·같은 ID 재시도, 잘못된 이력 대조군 |
| V03-11 | 실제 TCP 32 동시 hot-key 쓰기, 세 disk Replica의 applied prefix·버전·값 수렴, 실제 Follower+Meta |
| V03-12 | 독립 JVM 3개, Leader 강제 종료, 새 View·majority fallback·재시작 replay/rejoin·CLI CRUD, E/B/D별 wire/디렉터리 혼용 거절 |
| V03-14·16–18 | 부분 ACK/확장 retry의 최초 전송 시각, 연결 역순·gap 거절, 논리 flight와 겹친 Future 구분·fencing, 불변 snapshot·배타적 원인 통계 |

인수 항목은 선택한 유한 fixture로 확인했다. 모든 interleaving·무제한 장애에서의 정확성/진행성 증명은 아니다. 시험 중 발견한 테스트 import/반복 staging 빌드 등록 오류를 수정했고, 최종 진단 검토에서는 부분 ACK 뒤 최초 전송 시각을 peer/index별로 보존하도록 보완했다. 원본을 수정해 테스트를 맞추지 않았다.

## 남은 범위

공통 데이터셋·IONIA 비교 driver·처리량/지연/heap/SSD/NIC 성능은 미측정이다. Wf1/20ms·예제 설정은 최적값이나 원논문 재현 설정이 아니다. O00 대 V03은 전송 창·TCP 연결 패턴·admission까지 다른 구성 비교이며 순수 batch 크기 효과로 단정하지 않는다.

독립 Replica 복제 RAM 로그, stable View 저장, 선택적 KV force는 별개다. per-request WAL/fsync(D-PERSIST)·전원 손실 내구성·SplinterDB 성능 재현은 미구현/미검증이다. 전체 RAM 로그 손실은 fail-closed이고, StartView/RecoveryResponse 전체 전송의 64MiB·파일 KV 단일 32MiB 한도는 남는다. 2013 TCS 최종 원문 상세 분석도 미완료다. commit/push/merge는 사용자가 수행한다.

## 공통 benchmark adapter 연결 후속 (2026-10-30)

현재 표시 ID V03의 실제 알고리즘을 server benchmark/ionia_fourth adapter에 연결했다. 위 과거 문서명·원래 profile 표기/최초 알고리즘 검사 기록은 보존한다. 기존 알고리즘/core API/wire는 변경하지 않고 공통 ID/deadline/retry·trace·closed/open-loop·전수 지연·로컬 표준 export를 재사용한다.

V03의 실제3/5노드·두 실행 방식·읽기/혼합8조건과 별도 CRUD 값·DELETE OK/MISS·재삽입을 검증하고 결과8개를 schema/hash·histogram/설정으로 대조했다. 여섯 방식 합계 추가99/전체760검사 통과이며 개별 방식의99검사라는 뜻은 아니다. ../experiments/replication-benchmark-v1/ADAPTERS_V02_V07.md 및 REMAINING_ADAPTERS_VALIDATION.json/REMAINING_ADAPTERS_SOURCE_PRESERVATION.json/REMAINING_ADAPTERS_ARTIFACT_CHECK.json을 따른다.

V06 학습의 phase 분리·V07 coalescer의 client별 범위, ready manifest/서버 inventory·공통 정확성/필수 계측·clock/overhead/remote 관찰을 정식 성능 전 보강해야 한다. 본시험 데이터/요청열 후보·pilot과 EC2 접속/진단·성능 비교는 후속이며 commit/push/merge는 하지 않았다. RAM log·stable View/선택적 KV force와 RPC별 socket 정책을 유지한다.
