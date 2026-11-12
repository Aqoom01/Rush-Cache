# V05 — 클라이언트 관측 기반 Follower 선택 구현·설계

## 현재 구현·검증 상태 — V05

2026-10-28. **V05 기능 구현과 선택한 인수 fixture 검증을 완료했다.** 시작 HEAD `74e2599582751c646c425815cf876a1b0a42c89d`는 clean이었다. 기존 구현/테스트/설정은 보존했고 commit/push/merge는 실행하지 않았다.

- `replication/ionia-seventh/`의 독립 main/test roots, package `ionia_seventh`에 Settings/ReplicaSelector/IoniaAdaptiveReadClient를 구현했다. 관측 Transport는 client의 private inner class다. 새 Engine/Node 사본은 없다.
- 원본 Client.readAtFollower·Engine·Node·Log/History/Recovery·wire/TCP·파일 KV/stable View를 직접 재사용한다. O00 서버와 client 정책 hash를 분리한다. server CLI만 새 client Main을 제공한다.
- RR/ADAPTIVE·EWMA1/8·TTL/탐색/cooldown·L/P·실제 raw completion·same-ID/timeout/cancel·generation/dispatch seq fencing·불변 진단을 구현했다. private delegate/public future 분리와 deferred publication으로 사용자 callback을 제어 lock 밖에서 완료한다.
- CLI는 같은 logical ID/10초 총 deadline으로 발견/재시도를 수행하며 UNAVAILABLE/NOT_PRIMARY/BUSY를 그 안에서만 재시도한다. 라이브러리 내부 read retry/hedging은 추가하지 않았다. 단발 CLI는 학습 성능 측정용이 아니다.
- diagnostics.outcomes는 거절 또는 private delegate의 완료 결과를 집계한다. public cancel 뒤 원본 요청이 OK로 끝날 수도 있으므로 publicCancels와 구분하고, 이를 사용자에게 전달된 성공 수나 goodput으로 바로 해석하지 않는다. 모드/설정은 불변 Settings와 실행 metadata에 함께 기록한다.
- Java21.0.11 최종 전체 build/installDist 성공. **457개(core358/server83/runner16), 실패·오류·skip0**, 신규 V05 연결 **41개(core37/server4)**다. 기존 checker/fixture 재사용을 포함하며 새 정리41개를 증명한 숫자가 아니다.
- 기존 보호 **237개 파일 SHA-256 동일**(공유 core Gradle sourceSet·개발 기록·이 문서 상태 갱신 제외). 새 production JAR 포함/test 제외·core 외부 production 의존성 없음도 확인했다.

### 실제 인수 증거

| ID | 실행한 fixture |
| --- | --- |
| V05-01 | settings/모든 leader 회전·3/5 구성·실제 View failover |
| V05-02 | RR/ADAPTIVE 결정적 tie/초기점·실제 TCP 두 모드 |
| V05-03 | Meta DATA가 raw sample이 아님·raw failure와 delayed TCP READ 관측 |
| V05-04 | 설정·TTL 경계·EWMA·0/지연 sample clamp·saturating score |
| V05-05 | virtual-thread 32 caller L/P·동일 ID token·BUSY시 새 RPC 없음 |
| V05-06 | 탐색 E1/2/16·cooldown 만료/회복·실제 delayed reply 제거 후 재탐색 |
| V05-07 | throw/null stage/exception/cancel/BUSY/UNAVAILABLE/TIMEOUT·잘못된 node/view/client/op |
| V05-08 | public cancel·delegate timeout·Meta DATA 완료 뒤 raw P 보존/회수 |
| V05-09 | 빠른 stale value/MISS·실제 엔진의 오래된 응답과 나중 trimmed Meta |
| V05-10 | a<i/a=i/a>i, pending DATA·DELETE MISS·응답 역순·forged Meta |
| V05-11 | old generation callback·동일 View 학습 보존·old refresh rollback 거절·refresh 중 close |
| V05-12 | 같은 View 역순 sample·sequence exhaustion fail-closed |
| V05-13 | Meta/probe 인증 실패·3/5 원본 엔진 isolated old leader |
| V05-14 | 원본 fast SET/DELETE/시간초과 View retry·timer/close·public callback lock 경계/동기 재진입 |
| V05-15 | 3/5 replicas×RR/ADAPTIVE×64 seeds×10 CRUD=2560 유한 완료 이력·timeout completion 확장·기존 checker 대조군 |
| V05-16 | 실제 TCP 두 모드 각각32 hot-key/S2·80ms controlled response delay 회피/회복 탐색 |
| V05-17 | O00 JVM3개 leader crash/restart 및 follower 단독 crash/restart/rejoin·CLI CRUD |
| V05-18 | 전체 Java build/installDist·core 의존성 검사·JAR/기존 파일 hash·실제 policy profile/진단 |

3/5×두 모드×64 seeds×10=2560 완료 CRUD의 유한 이력과 결과 불명확 write의 completion 확장을 검사했다. 실제 TCP는 두 모드 각각32 hot-key와 별도80ms 응답 전달 지연 fixture를 사용했다. 후자는 SSD 지연/성능 측정이 아니다. 독립 JVM3개의 leader 및 follower 각각의 crash/restart/rejoin, 새 CLI CRUD를 확인했다.

검증 요약·클래스별 수·소스/산출물/보호 hash는 `IONIA_V05_VALIDATION.json`, 실행 방법은 `../experiments/ionia-seventh/README.md`에 있다. 아래1–9는 설계 당시 기록을 보존한 것이다. 그중 '이번 단계 Java 미실행'과 인수 '계획'은 당시 상태이며 현재 실행 증거는 이 절/VALIDATION을 따른다.

직전 전체 실행의 기존 V03 IoniaFourthProcessTest에서 leader crash 직후 CLI GET이1회 TIMEOUT을 반환했다. V05 검사는 그 실행에서도 모두 통과했다. 기존 소스를 변경하지 않은 분리 재실행과 최종 전체 재실행은 통과했으며 원인은 확정하지 않았다. V03의 시간 관련 안정성을 해결한 것으로 표시하지 않는다.

성능/최적값·실시간 lag/서버 계측 후속·V06은 미완료다. 모든 후보가 cooldown/full이면 정상 leader가 있어도 BUSY를 반환한다. L/P는 raw READ stage/논리 GET만 제어하며 전체 socket/쓰기/Meta/probe·RAM 상한이 아니다. 기존 메시지별 TCP 연결·64MiB 상태 전송/32MiB KV payload 제약을 유지한다. Replica RAM Durability Log·stable View·선택적 KV force를 구별하며 per-request stable WAL/fsync를 추가하지 않았다. 이번은 유한 기능·장애 fixture 검증이며 모든 interleaving이나 성능의 증명이 아니다.

---

2026-10-28. 분류 **D(자체 가설)**. 이번 산출물은 문헌 근거·상세 설계·유한 사전모형이다. **Java V05 구현·통합 테스트·성능 검증은 아직 실행하지 않았다.** 직전 Java 기준은 TCP 감사 후 416개×2회 통과이며 이번 모형 수와 합산하지 않는다. 시작 HEAD `3998af206d7b2363f9d130b0c185b5f99b0c4d3e` 및 미커밋 V04b/TCP 감사 변경을 보존했다. commit/push/merge 미실행.

노션 설계: [03-7 V05](https://app.notion.com/p/4074ed90243781a18384daafe8eb8fa8). 문헌 DB 전체 조회는 고유81편+DUP3행=84행, 핵심 절12/서지·초록69/전체 상세 분석·재현 완료0이다. 이번에 P080/P081을 추가했다.

## 1. 원문 근거 R와 채택 범위 D

| 근거 | 확인 범위 | 이번 판단 |
| --- | --- | --- |
| [P001 IONIA](https://www.usenix.org/system/files/fast24-xu.pdf) | §4.2.1–4.2.2와 현재 Client/Engine 대조 | 선택한 복제본의 실제 응답에 대한 Meta/index·View 검증을 유지 |
| [P080 C3](https://www.usenix.org/system/files/conference/nsdi15/nsdi15-paper-suresh.pdf) | §3.1–3.3/§4, PDF6/8 수식·Algorithm1–2 시각 확인 | 서버 피드백을 쓰는 ranking과 rate/backpressure의 결합이다. client RTT 하나로 전체 C3를 재현했다고 할 수 없음 |
| [P081 Tars v1](https://arxiv.org/pdf/1702.08172v1) | §III/§IV.A–B/§VI, PDF5–6 식4–6·Algorithm1–2 시각 확인 | 피드백 age·재탐색을 고려하되 원문 설정값·시뮬레이션 성과를 이 환경에 승계하지 않음 |

C3 공식 PDF16쪽(표지 포함)·Tars 저자 v1 PDF11쪽을 확보했다. 두 문헌은 **핵심 절 검토** 단계이며 전체 상세 분석/성능 재현 완료가 아니다. Tars 초록은 ICDCS2017 **투고**를 표시하고 comments10pages/실제 PDF11쪽이 다르다. 최종 게재판으로 표시하지 않는다. [저자 기록](https://arxiv.org/abs/1702.08172)

### 범위 결정

V05a는 **client에서 보이는 raw Follower READ RTT와 이 client의 미완료 READ stage**만 사용한다. 서버 queue/service/SSD IOPS나 실시간 replication lag는 현재 인터페이스에 없다. appliedIndex는 이미 지난 해당 응답의 값일 뿐 현재 lag가 아니다. 이를 쓰는 V05b·서버 계측/wire 확장은 후속으로 둔다. C3/Tars 이름·전체 스케줄러·rate-control 증명/성능을 가져오지 않는다.

## 2. 기존 코드와 구현 경계

| 실제 경계 | 재사용/새로 필요한 것 |
| --- | --- |
| O00 IoniaClient.readAtFollower(node,get,context) | 지정 Follower의 실제 읽기·Meta·fallback·View 인증을 직접 재사용 |
| IoniaClient의 private endpoint | 밖에서 임의로 읽을 수 없음. 새 wrapper가 자신의 Endpoint/세대를 소유하고 delegate.updateView와 한 제어 lock 안에서 함께 갱신 |
| IoniaClient.refreshView | delegate가 먼저 endpoint를 바꾸는 비동기 틈을 피하기 위해 wrapper refresh가 같은 probe/정적 leader 규칙으로 발견하고 두 endpoint를 함께 갱신. caller는 private delegate를 직접 조작하지 않음 |
| IoniaClientTransport | 기존 계약 위 시험 가능한 ObservingTransport wrapper. raw followerRead의 시작/terminal 응답을 관측; 다른 DTO/wire 변경 없음 |
| IoniaNode/Engine/FileStorage/TCP/ViewStore | O00 직접 사용. 새 Engine·Node 사본을 만들지 않음 |

예정 위치는 `cache-core/.../replication/ionia-seventh/src/{main,test}/java/.../ionia_seventh/`의 IoniaReadSelectionSettings/ReplicaSelector/IoniaAdaptiveReadClient/ObservedReadTransport와 별도 test roots다. 서버에는 `server.ionia_seventh.IoniaAdaptiveReadClientMain`만 추가할 계획이다. **이번 단계에는 이 Java 디렉터리나 Gradle sourceSet을 만들지 않는다.**

샘플 서버는 O00 Main을 별도17701–17703/`build/ionia-seventh/.../node-N`으로 실행한다. server node.properties는 O00 설정만, client.properties는 membership과 read 정책만 담도록 분리한다. O00 서버의 허용 설정에 read.*를 섞지 않는다. V01/V02a/V03/V04a/V04b 정책을 누적하지 않는다.

## 3. 안전성 경계

1. 선택은 목적지만 정한다. GET마다 원본의 fresh Meta와 현재 View 인증을 수행한다. 오래된 appliedIndex/lag/RTT/이전 Meta로 값을 승인하지 않는다.
2. Follower가 KV 읽기 **전에** 캡처한 a와 해당 Meta i를 비교한다. OK와 MISS 모두 같은 검증을 거친다. a<i면 원본 leader fallback이다.
3. Meta DATA가 있으면 원본 pending 처리 결과를 사용한다. 빠른 Follower 응답만 먼저 반환하거나 hedged 응답 경쟁을 만들지 않는다.
4. 후보 변경이 fast-write quorum·DELETE/SET 순서·History/LTI·View/recovery·ACK 내구성 의미를 바꾸지 않는다.
5. 선택 metadata가 완전히 틀려도 느림/BUSY/UNAVAILABLE/fallback 비용만 생겨야 한다. 잘못된 값이 OK가 되는 경로는 허용하지 않는다. 이것은 설계 의무이며 새 Java 구현으로 검증해야 한다.

## 4. V05a 선택 규칙 D

정적 멤버 집합에서 **현재 leader 제외**. node별 미완료 raw READ stage O가 P 미만이고 cooldown이 끝난 후보만 eligible이다. 별도 health probe로 후보를 승인하지 않는다. 실제 상태/응답의 검증은 원본이 수행한다. 후보가 없거나 논리 GET L 한도면 **RPC 없이 BUSY**, 대기 큐나 leader 우회는 추가하지 않는다. 선택 뒤 원본이 수행하는 leader fallback은 유지한다.

이 local admission은 모두 cooldown/full인 순간 정상 leader가 있어도 BUSY를 줄 수 있다. 따라서 O00와 동일한 읽기 가용성을 무조건 보존한다고 주장하지 않는다. cooldown 만료·raw completion 후 재진입을 검증하고, 비교 결과에 BUSY/실패율·완료율을 포함해야 한다.

두 모드는 같은 관측·L/P·cooldown·쓰기·인증 조건을 쓴다.

- `ROUND_ROBIN`: eligible 집합을 membership 순서로 순환한다. RTT는 관측만 하고 순위에 사용하지 않는다.
- `ADAPTIVE`: score(s)=R(s)×(1+O(s))의 최소 후보. R은 raw READ EWMA, cold/expired면 R0. 동점은 순환 순서로 푼다. 매 E번째 **admit된** GET은 점수를 무시하고 eligible 집합을 별도 cursor로 순환 탐색한다. 추가 probe/중복 GET은 보내지 않는다.
- cursor 시작점은 clientId에서 결정적으로 유도한다. 동일 입력/응답열/시계는 동일 선택을 만들지만 여러 client의 herd/불균형 제거를 보장하지 않는다.
- TTL age는 로컬 monotonic 수신 시각 기준이며 0≤age≤TTL일 때 fresh다. 성공 sample의 R은 `R + (sample-R)/8`, 최초 성공은 clamp된 sample로 초기화한다. RTT는 최소1ns·최대30s로 clamp하고 score 곱은 saturating long으로 계산한다.
- 시간 만료는 비교 시 확인한다. timer가 늦어도 만료 sample을 fresh로 보지 않는다. 더 높은 dispatch seq의 feedback만 반영하며, 늦은 옛 성공이 새 실패 cooldown을 지우지 않는다. 긴 옛 요청을 EWMA에서 제외하는 편향은 별도 outcome/latency 집계에 남긴다.

이 식은 서버의 실제 대기열/처리율 추정이 아니다. RTT에 대기가 포함된 상태에서 O를 다시 곱하는 영향, 빠르지만 lagging인 Follower 선호와 leader 부하 증가도 반례/측정 대상이다.

### 기능 fixture와 설정 범위

| 필수 설정 | 초기 fixture | 허용 범위/의미 |
| --- | --- | --- |
| read.selection | adaptive | adaptive 또는 round-robin |
| read.logical.limit | 64 | 1..65536, delegate가 아직 끝나지 않은 논리 GET |
| read.rpc.limit.per.replica | 32 | 1..65536, node별 raw READ terminal stage 한도 |
| read.rtt.initial.ms | 1 | 1..60000, cold/expired 기본 R0 |
| read.sample.ttl.ms | 1000 | 1..60000, 로컬 피드백 age |
| read.failure.cooldown.ms | 250 | 1..60000, raw 실패 후 재선택 대기 |
| read.explore.every | 16 | 1..65536, admit ordinal 기준 순환 탐색 |

EWMA1/8, sample max30s, 원본 write fallback50ms는 정책 상수로 명시한다. 기존 기준선도 같은 fallback을 명시해야 한다. 모든 숫자는 **최적값이 아닌 기능 fixture**다. 정수 parse·overflow·알 수 없는 키를 시작 전에 검증한다.

## 5. 실제 completion과 수명 관리

- 선택/한도 확인/예약/초기 delegate dispatch를 하나의 짧은 lock에서 처리한다. transport API는 비차단이어야 하며 socket wait/join/사용자 callback을 lock 안에서 실행하지 않는다. delegate future는 private이며 public future로 직접 노출하지 않는다.
- wrapper는 GET만 delegate.readAtFollower로 보낸다. Observer.followerRead가 그 동기 호출 구간 안에서 raw token `(generation,node,dispatchSeq)`과 O를 등록한 뒤 borrowed transport를 호출한다. 요청 ID별 단일 map에 의존하지 않으므로 같은 logical ID의 동시 재호출이 token을 덮지 않는다. Observer도 P를 다시 확인한다.
- public cancel은 delegate/borrowed stage를 cancel하거나 L/P를 조기 반환하지 않는다. L은 private delegate terminal에서 회수한다. delegate timeout 또는 Meta DATA로 논리 GET이 끝나도 raw READ가 남으면 P는 유지한다. 이미 완료된 GET 뒤 오는 raw 응답도 현재 세대/seq 조건이면 sample을 갱신할 수 있다.
- P는 raw stage terminal에서 **한 번만** 회수한다. sync throw/null stage/exception/cancel/malformed identity/실패 status를 관측하며 성공 RTT로 학습하지 않는다. terminal callback token의 중복 처리는 idempotent하게 막는다. OK/MISS raw 응답도 사용자의 최종 읽기 성공을 뜻하지 않는다.
- View가 증가할 때 EWMA/cooldown/선택 cursor의 세대를 교체하되 **raw O와 기존 delegate 논리 요청은 지우지 않는다**. 옛 token은 P만 회수하고 새 학습/쿨다운은 못 바꾼다. 동일 View의 정상 refresh는 학습을 초기화하지 않는다. View 감소/같은 View의 다른 leader·정적 leaderFor 규칙 불일치는 거절한다.
- close는 lock 안에서 새 dispatch를 닫고, 밖에서 delegate.close를 호출한다. borrowed transport는 소유자가 닫는다. raw count는 거짓0으로 초기화하지 않는다. 영원히 끝나지 않는 외부 stage의 즉시 회수/물리 socket 종료는 보장하지 않는다.
- onTimer는 기존 delegate의 deadline/쓰기 fallback을 계속 구동한다. 선택 때문에 재시도 시 deadline이나 operationId를 새로 만들지 않는다. wrapper 내부의 자동 읽기 재시도/hedging은 추가하지 않는다.

L/P는 이 client의 논리 GET/raw READ stage 한도다. 전체 TCP socket·Meta/probe/쓰기·서버 backlog·총 RAM에 대한 hard 상한이 아니며, 이전 TCP 포트 문제를 해결하는 전역 admission이라고 부르지 않는다. `logicalStarts=logicalTerminal+activeLogical`, `rawStarts=rawTerminal+rawPending`을 구분해 계수한다.

## 6. policy fingerprint·진단·공정 비교

정책 설정/상수/순환 규칙을 고정 순서 UTF-8 문자열로 canonicalize하고 SHA-256을 기록한다. membership config ID와 함께 실행 결과에 남긴다. **client 정책 hash는 wire configuration이나 서버 stable View에 섞지 않는다.** client마다 다른 안전한 선택 정책이 공존할 수 있으며 O00 서버/로그 재사용에는 서버 합의 설정 변경이 필요 없다. 실제 실험 데이터 경로·포트는 분리한다.

진단은 endpoint/generation·mode/profile, admit/reject/selection reason, node별 READ starts/terminal/pending·EWMA/age/lastSeq/cooldown·ignored feedback, 최종 outcome, 실제 leader EXECUTE GET calls·Meta DATA 응답·probe/Meta/READ 개수를 기록한다. delegate의 최종 OK를 Follower 성공으로 학습하지 않는다. raw RTT는 network·queue·I/O·JVM 비용을 섞은 관측값이며 SSD service time이 아니다. leader fallback 호출 수만으로 stale/transport 원인을 단정하지 않는다.

RR와 ADAPTIVE는 같은 L/P·시드·요청열·timeout·storage shard/worker·History·batch·ACK·force·CPU·호스트 조건으로 비교한다. O00 fixed-Follower도 같은 외부 admission 예산으로 별도 기준선에 둔다. 균등/단일 hot key/hot set·느린 follower·빠르지만 lagging follower·자주 바뀌는 성능·여러 독립 client를 나눠 본다. 탐색/인증/Meta DATA/fallback 비용을 전체 지연과 트래픽에 포함한다. CLI 한 요청마다 새 client를 만드는 실행은 학습 정책의 성능 측정에 사용할 수 없다. 공통 driver는 장수 client와 관측 이력을 유지해야 한다.

## 7. 구현 후 반드시 실행할 인수 조건

아래는 **계획**이며 이번에 실행한 Java 테스트가 아니다.

| ID | 조건 | 필요한 실행 증거 |
| --- | --- | --- |
| V05-01 | 현재 leader 제외, 정적 구성원만 후보 | 3/5 replica·View 회전·잘못된 node/fixed membership |
| V05-02 | RR·ADAPTIVE 통제 조건과 결정 재현 | 같은 client/seed/clock/응답열이면 같은 선택; 서로 다른 client 시작점 |
| V05-03 | 실제 READ RTT만 점수화 | Meta DATA·leader fallback이 빨라도 느린 follower를 빠르다고 학습하지 않음 |
| V05-04 | EWMA/TTL/정수 상한 | cold/expired·동점·0/음수/최대 지연·곱 overflow·시간 경계 |
| V05-05 | 선택과 예약 원자성 | 동시 GET에서 L·노드별 P 초과 금지; capacity 없으면 RPC 없이 BUSY |
| V05-06 | 순환 탐색과 cooldown | 고정 eligible 집합에서 E×F 내 각 후보 탐색; cooldown/full 건너뜀·회복 재진입 |
| V05-07 | 오류/잘못된 응답 | throw/null stage/exception/cancel/BUSY/UNAVAILABLE/TIMEOUT/identity 불일치가 성공 sample 아님 |
| V05-08 | 취소·timeout·Meta DATA의 비대칭 완료 | public cancel은 L/P 조기 해제 금지; delegate timeout/DATA 완료 뒤 남은 raw READ의 P 보존 |
| V05-09 | 빠른 stale Follower 반례 | 저지연 old/MISS도 fresh Meta 기준으로 거절하고 원본 leader fallback; routing hint가 인증을 대체하지 않음 |
| V05-10 | pending·History/LTI·응답 순서 | pending Meta DATA, a<i/a=i/a>i·trimmed key·GET/DELETE MISS·역순 응답 |
| V05-11 | View/세대 fencing | old callback은 P만 회수, 새 점수/cooldown 불변; 동일 View refresh는 학습 유지 |
| V05-12 | sample 역순 완료 | 더 새 dispatch 실패 뒤 옛 성공이 cooldown을 지우지 않음; 양쪽 순서·seq overflow fail-closed |
| V05-13 | late/forged 인증과 old leader | 잘못된 view/client/op/node·과반 probe 부족·isolated old leader가 OK를 만들지 않음 |
| V05-14 | writes·lifecycle 재사용 | SET fast ACK/fallback·DELETE·같은 ID 재시도·단일 총 deadline·close/timer 보존 |
| V05-15 | 3/5 replica 실제 Java 이력 | 기존 checker 연결·완료 CRUD/timeout 확장·View/retry·오류 대조군; 단순 선택 모형과 구분 |
| V05-16 | 실제 TCP 선택 이동 | 지연 follower 회피/재탐색·hot/cold·고정 shard파일·연결 실패를 상태/이벤트와 함께 기록 |
| V05-17 | 독립 JVM 장애·재합류 | 3 JVM leader/follower crash/restart·View refresh·CLI CRUD·원본 durability 경계 |
| V05-18 | 동일 조건 회귀·비교 준비 | 전체 Java build/installDist·원본 보존 hash·policy/profile·노드별 READ/Meta/probe/fallback 집계 |

## 8. 이번에 실제 확인한 것

`docs/models/v05_selector_model.py`는 3/5 node의 leader 위치와 fresh-fast/fresh-slow/cold/expired/loaded/full/cooldown 스냅샷을 조합해 **36456개 단일 선택 상태**를 확인했다. RR/ADAPTIVE/탐색 경로의 eligibility·점수 조건, 고정 eligible 집합의 탐색6조건, 취소/raw 한도·옛 세대·역순 callback의 예시 경계6개를 확인했다. leader 포함·취소 조기 반환·옛 세대 학습·옛 성공의 cooldown 삭제·cached hint 인증의 **설명용 잘못된 경우5개**도 구별했다. tick은 임의 모형 단위이며 위 fixture ms가 아니다.

이것은 실제 비동기 Java 구현·네트워크·OS·원본 consensus를 포함하지 않는 유한 사전모형이다. 모든 interleaving·분산 선형성·교착 없음·성능의 증명이 아니다. 물리 socket 수 제어도 검증하지 않았다. L 제어·공통 API 통합 등은 위 인수 계획에 남아 있다. 결과는 `IONIA_V05_REVIEW.json`에서 원문 hash/검토 범위·보호 hash와 함께 분리한다.

```powershell
python .\docs\models\v05_selector_model.py
```

이번 변경은 문서·사전모형·개발 기록뿐이다. 기존 241개 파일의 SHA-256이 시작 상태와 같다(개발 기록 추가 제외). Java/Gradle/기존 설정은 변경하지 않았으므로 기존416개를 이번에 다시 실행했다고 주장하지 않는다.

## 9. 구현 순서와 미완료

1. **I1 — 새 client 디렉터리/Settings/Selector**: 순수 정책·정수 경계·모드 통제/diagnostics와 V05-01–06. 기존 코드는 보존.
2. **I2 — ObservingTransport·원본 Client wrapper**: lifecycle/endpoint 원자성·동일ID·raw completion·인증 연결, V05-07–14. 실패 시 prototype pool/새 프로토콜로 우회하지 않음.
3. **I3 — 실제 Java 이력/TCP/JVM/CLI**: V05-15–17와 잘못된 구현 대조군. 포트 사전점검 도구를 사용하되 오류를 pass로 바꾸지 않음.
4. **I4 — 전체 build/installDist·보호 hash·명세 대조**: V05-18. 이 단계 완료 뒤 V05a 기능 검증 완료로 표시.

성능·최적 파라미터·multi-host·SSD·power-loss·per-request stable WAL(D-PERSIST)·V05b lag/서버 queue 계측·V06 MetaQuery batch는 남아 있다. 독립 Replica RAM Durability Log, stable View 및 선택적 KV force를 유지/구별한다. 현재64MiB 상태 전송/32MiB KV payload 한도도 그대로다.

## 공통 benchmark adapter 연결 후속 (2026-10-30)

현재 표시 ID V06의 실제 알고리즘을 server benchmark/ionia_seventh adapter에 연결했다. 위 과거 문서명·원래 profile 표기/최초 알고리즘 검사 기록은 보존한다. 기존 알고리즘/core API/wire는 변경하지 않고 공통 ID/deadline/retry·trace·closed/open-loop·전수 지연·로컬 표준 export를 재사용한다.

V06의 실제3/5노드·두 실행 방식·읽기/혼합8조건과 별도 CRUD 값·DELETE OK/MISS·재삽입을 검증하고 결과8개를 schema/hash·histogram/설정으로 대조했다. 여섯 방식 합계 추가99/전체760검사 통과이며 개별 방식의99검사라는 뜻은 아니다. ../experiments/replication-benchmark-v1/ADAPTERS_V02_V07.md 및 REMAINING_ADAPTERS_VALIDATION.json/REMAINING_ADAPTERS_SOURCE_PRESERVATION.json/REMAINING_ADAPTERS_ARTIFACT_CHECK.json을 따른다.

V06 학습의 phase 분리·V07 coalescer의 client별 범위, ready manifest/서버 inventory·공통 정확성/필수 계측·clock/overhead/remote 관찰을 정식 성능 전 보강해야 한다. 본시험 데이터/요청열 후보·pilot과 EC2 접속/진단·성능 비교는 후속이며 commit/push/merge는 하지 않았다. RAM log·stable View/선택적 KV force와 RPC별 socket 정책을 유지한다.
