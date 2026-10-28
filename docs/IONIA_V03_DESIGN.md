# V03 기능 대조군 설계 v1.1 — Prepare 배치 경계·대기·ACK 기반 전진

2026-10-28 · 실제 읽은 기준 revision: `9abfcb68cf493c74b7d7281155aa5f3336e6bc4d`(feat/ionia). V02a까지의 검증 산출물 23개 파일은 직전 hash와 동일했다. 커밋·push는 사용자가 담당하며 이번 작업에서는 Git 변경 작업을 하지 않았다.

**현재 판정: v1.1 기능 대조군을 ionia-fourth에 구현하고 기능·장애·실제 TCP/JVM 및 전체 329개 회귀 테스트를 통과했다. Wf=1·20ms는 기능 fixture이며 최적 설정·성능 측정은 미완료다.** 실제 구현/명령/한계는 [V03 구현 기록](IONIA_V03.md), 수치는 [검증 JSON](IONIA_V03_VALIDATION.json)을 따른다. 아래 미구현·미실행 표현은 설계 당시 기록이며 이번 실행 증거는 구현 기록의 인수 항목 대조표로 구분한다. 변경 근거와 판본·접근 한계는 [추가 문헌 검토](IONIA_V03_RESEARCH.md)를 따른다. 아래 본문은 기능 대조군의 정책이며 §12의 시간·전송·비교 제한을 함께 적용한다.

### 1. R: 원문에서 유지할 내용과 D/U 구분

IONIA §4.1/Fig.5는 배경 Prepare 배치와 ACK 후 합의/적용을 설명한다. Skyros §4.3은 배경 batching을 사용하며 §4.4–4.5는 읽기/결과 노출 연산의 동기 진행을 구별한다. VR §6.2는 저부하 대기와 고부하 비용 상각을 구별한다. [IONIA](https://www.usenix.org/system/files/fast24-xu.pdf) · [Skyros](https://research.cs.wisc.edu/adsl/Publications/nilext.pdf) · [VR](https://pages.cs.wisc.edu/~remzi/Classes/739/Papers/vr-revisited.pdf)

아래 maxEntries/bytes/delay, Follower당 한 개의 확인 대기 구간, profile, 계측·자료구조는 D(프로젝트 설계)다. 원논문이 이 Java 정책을 명세하거나 증명했다고 표시하지 않는다. 배경 SET ACK 경로와 GET/DELETE/fallback의 체감 지연은 구별한다.

### 2. 기준선과 보존 범위

V03은 O00에서 분리하며 V01 Leader-only Read나 V02a History 목표 정책을 합치지 않는다. 기존 세 방식의 소스·테스트·설정을 보존한다.

현재 O00의 order는 후속 owner flush를 합치고, flush는 각 matched+1 이후 **전체 suffix**를 발송한다. 따라서 O00를 batch size=1로 부르거나, 아래 정책의 큰 상한/0 대기가 O00와 완전히 동일하다고 주장하지 않는다.

보존: Fast SET의 Leader 포함 정족수, majority fallback, 동기 DELETE 결과, pending 우선 DATA, Follower 선캡처와 Client INDEX/View 검사, 키별 FIFO·연속 applied prefix, History/LTI 원본 정책, View/DAG/nonce 복구, dedup과 반환 의미. 독립 Replica RAM 로그의 복제 내구성·안정 View 저장·선택적 KV force·미구현 D-PERSIST도 그대로 구분한다.

### 3. 기능 대조군 설정 D

| 설정 | 허용 범위·정확한 의미 |
| --- | --- |
| prepare.max.entries | 1–1,000,000. Prepare 하나의 연속 로그 항목 수 |
| prepare.max.encoded.bytes | 1–67,108,868. 외부 4-byte 길이를 포함한 wire packet의 목표 상한 |
| prepare.max.delay.ms | 0 또는 20–60,000. 첫 미확인 항목의 ordering 시각 이후 목표 대기. 0은 지연 없음 |
| retry interval | 기존 20ms tick에 맞춰 20,000,000ns 고정 |
| Follower당 논리 확인 대기 Wf | 기능 대조군은 한 개의 연속 구간. 논문의 global WND·socket 수 상한과 다름 |

세 값 모두 시작 시 필수이며 음수·범위 초과·누락·잘못된 숫자는 거절한다. 같은 실행 중 값을 바꾸지 않는다. 예제는 E=128, B=65536, D=20ms로 하되 최적값이나 논문 재현 설정으로 주장하지 않는다. 기능 fixture는 E=1/2/3과 작은 bytes 및 D=0/20/40ms를 사용한다.

실제 node timer는 20ms 주기다. 양수 D를 더 작은 값으로 받거나 정확한 실행시각을 약속하지 않는다. 첫 due tick까지의 양자화와 owner 지연이 추가될 수 있다. 새 고해상도 timer·별도 flush thread는 추가하지 않는다. 재전송과 Commit heartbeat가 배치 대기에 의해 멈춰서는 안 된다.

### 4. wire byte 정의와 단일 큰 쓰기

기존 IoniaWire v1의 실제 인코딩과 대조한 전체 길이 W는 다음과 같다.

```txt
U = configuration을 DataOutputStream.writeUTF로 표현할 때의
    modified UTF-8 payload 길이 (2-byte UTF 길이 자체는 제외)

W(Prepare) = 52 + U + sum(entryBytes)
SET entryBytes    = 33 + keyBytes + valueBytes
DELETE entryBytes = 29 + keyBytes
```

W에는 **외부 4-byte frame length**가 포함된다. IoniaWire.MAX_FRAME=67,108,864는 그 4바이트를 제외한 body 한도이므로 W의 hard limit은 67,108,868이다. 일반 UTF-8 byte 수로 writeUTF를 추정하지 않는다. ASCII 구성 ID만을 가정하지 않는 코덱 대조 테스트를 둔다.

prefix는 첫 항목부터 순서대로 모은다. 다음 항목을 넣으면 E 또는 B를 넘는다면 현재 prefix를 닫는다. 단일 항목이 B를 넘지만 wire와 저장 계층에서 유효하면 **단독 발송·목표 초과 계수**를 사용한다. B는 목표 상한이며 hard wire 한도와 다르다. 연산을 임의의 여러 논리 쓰기로 분할하지 않는다.

현재 파일 KV의 payload 한도는 **17+keyBytes+valueBytes≤32MiB**이며 DELETE는 valueBytes=0이다. V03의 조립에서는 wire에 인코딩 가능하고 KV에 적용 가능한 쓰기인지 **모든 Replica에서 기록 ACK 이전**에 검사한다. 초과를 CAPACITY_EXCEEDED 등 확정된 검증 결과로 반환하고 기록·순서 부여를 하지 않는다. 이미 ACK한 쓰기를 batch 선택 시 버리지 않는다.

이 admission 검사는 기존 O00의 Fast ACK 뒤 저장소에서 크기 오류가 드러날 수 있는 경계를 보완하는 **V03의 명시적 차이**다. 원본 소스를 조용히 수정하지 않으며, 이후 공정 비교 입력은 모든 방식이 수용하는 공통 크기 범위로 제한한다. 입력 크기 제한 밖까지 동일 동작이라고 주장하지 않는다.

전체 StartView/RecoveryResponse 전송은 Prepare 배치 상한 대상이 아니다. 64MiB 전체 상태 전송 한도는 별도로 남으며 상태 분할·checkpoint·compaction은 이번 범위에 넣지 않는다.

### 5. owner 상태와 발송 정책

| 상태 | 의미 |
| --- | --- |
| matched[f] | 같은 View에서 Follower가 실제로 수락했다고 확인한 연속 index |
| flight[f] | 확인을 기다리는 연속 구간의 start/end, 마지막 발송 시각 |
| orderedAt[i] | 현재 View/generation에서 index i가 순서 로그에 들어간 시각. 재시도·재발송으로 갱신하지 않음 |
| urgentThrough | GET/Meta pending/DELETE/Slow fallback 때문에 조기 진행해야 하는 prefix 끝 |
| view/generation | 예약 owner 작업과 전송 상태를 구분하는 fencing 값 |

이동식 speculative send cursor를 ACK 위치로 취급하지 않는다. 첫 구현에서는 Follower당 하나의 확인 대기 구간으로 제한한다. matched는 로컬 send 성공·Future 완료만으로 증가하지 않는다.

```txt
각 Follower f:
  미확인 suffix가 없으면 발송하지 않음
  확인 대기 구간이 있고 retry 시각 전이면 새 구간을 보내지 않음
  start = matched[f] + 1
  start부터 E/B로 자른 연속 prefix C를 선택

  다음 중 하나면 발송:
    - 개수/bytes가 차서 더 모을 수 없음
    - delay=0
    - 현재 시각 - orderedAt[start] >= maxDelay
    - start <= urgentThrough
    - 확인 대기 구간의 재전송 시각 도달

  flight 기록 후 전송
  matched는 그대로 유지
```

모든 배경·timer·동기·재전송 경로는 같은 chunk 선택 함수를 거친다. 중간에 전체 suffix를 발송하는 우회 경로를 두지 않는다. owner 작업 하나에서 각 Follower에 한 조각까지만 발송하고, ACK 또는 후속 timer/owner 작업에서 다음 조각으로 전진한다. 실패한 Follower 때문에 정상 Follower의 구간 전진을 막지 않는다.

큰 로그에서 매번 entriesFrom으로 전체 suffix 복사 후 자르지 않는다. entry(index)를 이용해 선택할 prefix만 순회한다. 최초 구현은 기존 로그의 O(1) index 조회를 재사용하고 새 로그/compaction 구조를 추가하지 않는다.

### 6. ACK·중복·유실 처리와 진행성

- 송신자 membership·현재 View·normal 상태를 확인한 ACK만 수락한다.
- leader lastLogIndex를 넘는 ACK는 거절한다. matched는 max로만 증가한다.
- ACK가 해당 flight 끝 이상이면 확인 대기를 해제하고 **즉시 다음 chunk의 준비 여부를 판단**한다. 중복 ACK만으로 반복 발송 loop를 만들지 않는다.
- 합의 commit index는 기존 majority matched 계산을 유지한다. chunk 경계가 commit을 강제로 E의 배수로 제한하지 않는다.
- ACK의 lastLogIndex가 방금 보낸 chunk 끝보다 크다는 이유만으로 거절하지 않는다. 같은 View에 이미 긴 prefix를 설치/수락한 Follower의 누적 ACK일 수 있다. 이 검증은 원본의 일관된 로그 설치와 non-Byzantine 전제에 의존하며 별도 상한 leader.lastLogIndex는 유지한다.
- Prepare/ACK 유실은 20ms 이후 현재 matched+1부터 다시 선택한다. 재전송도 E/B와 단일 큰 항목 정책을 지킨다.
- ACK 유실 후 같은 prefix의 중복 수락은 기존 IoniaLog.accept의 index/payload 검사로 처리한다. dedup·applied 결과를 유지한다.

진행성 주장은 무조건적인 시간 보장이 아니다. owner와 timer가 계속 실행되고 필요한 quorum과 네트워크가 결국 응답한다는 조건에서 미확인 tail이 최종 진행해야 한다. ACK가 없는데 무한 전진하거나, 마지막 한 요청이 개수 임계값만 기다리는 동작은 금지한다.

Follower당 한 구간 제한 자체가 O00와 다른 전송 창이다. V03 내부 E/B/D 비교에서는 이를 고정한다. O00 대 V03의 성능 차이를 오직 ‘배치 크기 효과’로 분해했다고 주장하지 않는다. 더 큰 pipeline 창은 별도 후속 연구다.

### 7. 동기 요청의 대기 우회

Fast recordWrite와 동기 execute를 구별한다.

- Fast SET 기록은 기존 기록 ACK를 반환하고 배경 발송을 요청한다. 단순 배치 지연 때문에 Fast ACK를 합의 대기로 바꾸지 않는다.
- Slow SET fallback·DELETE는 해당 ordering index까지 urgentThrough를 올린다.
- Leader GET과 Meta pending DATA는 그 key의 미적용 barrier까지 urgentThrough를 올린다. pending 검사를 History보다 먼저 한다.
- 이후 ACK마다 barrier 뒤의 조각까지 계속 전진한다. 첫 chunk 하나만 보내고 끝내지 않는다.
- 이미 적용된 key의 GET만으로 모든 미래 쓰기를 무조건 urgent로 만들지 않는다.
- 같은 ID 재시도는 기존 로그 index/orderedAt를 재사용한다. 재시도로 age를 리셋해 영구 지연시키지 않는다.
- urgent는 배치 **대기만** 우회한다. count/bytes/hard wire·quorum·commit/apply·View 검증은 우회하지 않는다.

응답을 못 받은 쓰기는 결과 불명확일 수 있다. timeout으로 기록을 지우거나 미적용으로 확정하지 않는다. V02a에서 보강한 completion-extension 이력 검사 원칙을 V03 테스트에도 적용한다.

### 8. View 설치·진단·복구

새 View/generation 설치 시 flight·예약 flush·orderedAt·urgent 상태를 초기화한다. 기존 in-flight apply 종료 → 저장소 reset → authoritative replay → normal 순서를 유지한다. 복구된 미확정 suffix는 새 installation 시각을 기록하고 urgent로 재전송을 시작해 미확정 tail을 방치하지 않는다.

예약된 owner flush와 비동기 callback은 예약 당시 View/generation을 캡처하고 불일치하면 종료한다. 기존 wire ACK에는 generation 필드가 없으므로 외부 ACK를 로컬 generation만으로 판별한다고 주장하지 않는다. ACK는 현재 View·membership·leader log 상한으로 검사한다. 같은 View의 이전 누적 ACK는 authoritative prefix와 일치한다는 원본 전제 아래 max로 처리한다. 옛 View ACK나 옛 예약 작업으로 matched/진단을 잘못 갱신하지 않는다. 원본 nonce·최고 normal view·DAG n1+n2·dedup 계약을 변경하지 않는다.

불변 진단 snapshot 예정 항목:
- View/generation·고정 profile·E/B/D·window=1.
- Prepare 발송 시도/항목 수/전체 encoded bytes, 목표 초과 단독 발송 수, 재전송 시도.
- 개수/bytes/age/urgent/retry의 발송 원인. 여러 조건이 겹치면 원인 우선순위와 중복 집계 여부를 문서화한다.
- Follower별 matched/flight end/마지막 전송 시각, 판단 시각의 첫 미확인 항목 age.
- recorded/ordered/committed/applied 및 ordered−committed·committed−applied backlog. Fast 기록 수와 고유 논리 쓰기를 혼동하지 않는다.

여기서 ‘발송’ 수·bytes는 transport에 넘긴 인코딩 크기의 **시도량**이다. 실제 NIC bytes/수신량으로 표시하지 않는다. owner에서 파일·heap dump·blocking network I/O를 수행하지 않는다. 종단 성능 계측과 Client 경로 비율은 공통 driver 단계다.

### 9. 분리 위치·재사용·profile

코어 보관은 `cache-core/src/main/java/io/openkache/core/replication/ionia-fourth/`, 내부 별도 main/test root, Java package `io.openkache.core.replication.ionia_fourth`로 정한다. server는 `server.ionia_fourth`, 예제 포트 17401–17403, 설정은 `experiments/ionia-fourth/`를 사용할 예정이다. 이번 명세 작업에서 이 디렉터리를 실제 생성하지는 않았다.

| 구성 | 결정 |
| --- | --- |
| IoniaClient·Config·Log·Message·Recovery·History·Storage 계약 | O00 그대로 재사용 |
| 공개 Progress/ViewInfo/Status·wire/TCP·파일 KV/안정 View | 그대로 재사용 |
| IoniaBatchSettings·PrepareSizer·BatchPlanner | V03 전용으로 추가, 공통 API에 넣지 않음 |
| IoniaFourthEngine | O00에서 별도 분리. 변경은 admission·배치 전송/동기 barrier·설정·진단 |
| 노드/Main/CLI | profile과 새 엔진을 연결하는 조립부 분리 |

기존 IoniaWire v1의 크기 계산은 V03 전용 sizer에 두고 실제 기존 encoder와 대조하는 server 테스트를 둔다. core→server 의존성을 만들거나 공통 wire를 수정하지 않는다.

profile 원문을 고정하고 UTF-8 SHA-256 소문자 64 hex로 구분한다.

```txt
v03|maxEntries=<E>|maxEncodedBytes=<B>|maxDelayNanos=<D>|retryIntervalNanos=20000000|inFlightPerFollower=1|oversize=singleton|sizeConvention=wireV1IncludingLength

wireConfig = 기존 configurationId + |algorithm=V03|profile= + profileId
```

서버·클라이언트·안정 configuration은 같은 profile을 사용한다. writeUTF로 표현 불가능한 구성 ID는 시작 시 거절한다. 다른 profile의 기존 디렉터리는 자동 삭제하지 않는다. 예제 데이터는 `build/ionia-fourth/entries-128-bytes-65536-delay-20/node-*`처럼 별도로 둔다.

### 10. 기능 인수 계획 — 아직 미실행

| ID | 검증 조건 |
| --- | --- |
| V03-01 | E−1/E/E+1, prefix 순서, 모든 발송 경로의 entry 상한 |
| V03-02 | 실제 codec의 총 bytes, Unicode/NUL/writeUTF, 길이 prefix 포함, 단독 초과와 hard 한도, 쓰기 크기 사전 거절 |
| V03-03 | 저부하 tail·0/20/40ms 대기·첫 due tick·deadline·재시도 age 보존 |
| V03-04 | Prepare/ACK 유실·중복·역순, ACK 없이 matched 전진 없음, 복구된 긴 prefix의 ACK |
| V03-05 | chunk 중간 3/5노드 crash·View 교체·완료 쓰기·finalized 위치 보존 |
| V03-06 | 뒤쪽 key의 GET/Meta DATA/DELETE/fallback이 여러 chunk를 지나 완료, 적용 완료 GET은 불필요한 전체 force 금지 |
| V03-07 | 옛 View ACK·메시지 차단, 예약 작업/비동기 callback의 View+generation fencing, 같은 View 누적 ACK의 멱등 처리 |
| V03-08 | 유실 후 재개·마지막 tail drain·고유 요청/commit/apply 정합성과 backlog 진단 |
| V03-09 | pending SET/DELETE·stale VALUE/MISS·연속 prefix·키별 FIFO·dedup·전체 RAM 손실 거절 회귀 |
| V03-10 | E/B/D 여러 profile의 동시 CRUD·결과 불명확 쓰기·동일 ID 재시도·잘못된 이력 대조군 |
| V03-11 | 같은 서버에서 실제 Follower+Meta, TCP 동시 쓰기·진짜 timer로 tail 진행 |
| V03-12 | 독립 JVM 3개·Leader 강제 종료·재시작·CLI CRUD·profile 혼용/디렉터리 거절 |

먼저 선택기/크기/가상 시계 → owner·ACK·동기 barrier → View·유한 이력 → 실제 TCP/JVM → 기존 전체 회귀 build 순으로 구현·검증한다. 기능 fixture에서 사용한 작은 부하를 공통 성능 데이터셋으로 부르지 않는다.

### 11. 이번에 실제 실행한 검토와 한계

저장소의 production/test 파일을 변경하지 않고 작업용 도구를 별도 workspace의 `tmp/v03-audit/`에 두었다.

- Java 21로 기존 빌드의 IoniaWire를 직접 호출해 **320가지 Prepare frame**의 실제 encoded 길이, round trip, trailing bytes를 확인했다. key/value 길이·항목 수·Unicode/NUL/보충 문자를 포함한 configuration을 바꿨다. 위 W 계산식과 모두 일치했다.
- 단일 Follower의 작은 정책 모형 **6,912개 조합**에서 count/bytes/singleton·delay·강제 진행·유한 Prepare/ACK 유실·View 교체 뒤 긴 prefix 확인을 검사했다. 모형 내부 발송 22,839회, 상태 전이 42,222회였다.
- 잘못된 로컬 send→matched 전진의 gap, 첫 강제 chunk 뒤 중단, 4-byte 길이 누락의 반례 3개를 확인했다.
- 이 모형은 단일 Follower이고 authoritative 복구 prefix의 일관성을 가정한다. 실제 quorum·RPC·Java 엔진·KV·전체 View interleaving·진행성·linearizability 증명을 수행하지 않았다.
- 기존 257개 테스트는 V02a까지의 이전 성공 기록이다. 이번에 V03 Java 테스트나 전체 저장소 build를 다시 실행한 숫자로 표시하지 않는다.

위 v1.0 사전 검토 숫자는 당시 수행 기록이다. v1.1은 문헌 재검토 결과이며 실제 V03 구현·인수 테스트 통과·G7 성능 비교는 별도 완료 조건으로 남긴다.


### 12. 추가 문헌 검토 반영 v1.1 — 기존 해석의 제한과 증분

2026-10-28. [IONIA_V03_RESEARCH.md](IONIA_V03_RESEARCH.md)의 R/C/D/U 및 판본 범위를 함께 적용한다. 2013 TCS 최종 원문은 미확보이며, 저자 공개 선행 원고·JPaxos·적응 배치의 확인한 절을 최종판 전체 정독으로 대체하지 않는다.

- Wf=1은 Follower별 **논리 확인 대기** 제어다. 합의 미결정 인스턴스 수 WND와 다르다. 재전송 socket/가상 스레드가 겹칠 수 있어 physical in-flight·메모리·전송량 상한도 아니다.
- 원본 TCP는 call마다 새 Socket을 만든다. 연결 간 도착 순서는 보장되지 않으며 IoniaLog는 gap을 거절한다. 미래 pipeline은 slot 수만 늘려 구현할 수 없다. 기존 transport를 이번 연구에서 수정하지 않는다.
- `now - firstOrderedAt >= maxDelay`를 발송 due의 의미로 고정한다. 신규 요청·중복·재시도가 첫 시각을 바꾸면 안 된다. 미확인 suffix/빈 batch의 구분과 유효한 단일 oversize 처리도 명시한다. 원문 Algorithm 2의 부등호를 기계적으로 복사하지 않는다.
- D는 추가 모으기 대기의 목표다. window wait·owner lateness·network·ACK·commit·apply의 hard deadline이 아니다. 이미 flight가 있으면 urgent여도 해당 ACK 또는 기존 retry 시각을 기다린다. 일정 RTT 내 읽기 완료를 보장하지 않는다.
- recorded/ordered timestamp, 전송 가능한 시각, first send, last retry, ACK/commit/apply를 구분한다. slot wait와 batch wait가 겹치는 경우 중복 합산하지 않으며 공통 clock 기준/정의를 먼저 문서화한다.
- 20ms timer/retry 및 E128/B65536/D20은 기능 조립 예제다. 최적/논문 재현 설정이 아니다. O00 대 V03은 연결 생성/창/admission까지 다른 구성 비교다. 순수 배치 효과는 별도 통제 비교 없이는 주장하지 않는다.
- 기존 V03-01–12에 문헌 검토 보고서의 V03-13–18(지속 도착, slot 지연, 기존 flight의 urgent, 연결 역순, 겹친 시도, 계측 해석)을 추가한다. 모두 아직 Java에서 미실행이다.
- per-request WAL/fsync·전원 손실 내구성은 추가하지 않는다. 독립 Replica 복제 로그·stable View·선택적 KV force를 계속 구분한다.

Wf를 키우거나 적응형 배치/지속 연결/고해상도 timer를 도입하는 결정은 이번에 확정하지 않았다. 먼저 기능 대조군을 검증하고, 공통 성능 driver와 남은 최종판 근거를 확보한 후 통제된 실험으로 판단한다. commit/push는 사용자가 수행한다.
