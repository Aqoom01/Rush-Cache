# 공통 시나리오 명세 v1

기준일 2026-10-30. **27개 논리 시나리오의 절차·판정 정의. 전부 미실행.** EC2 접속·환경 사전진단은 사용자 요청으로 보류. 아래 수치는 설계 시작값이며 하드웨어 적합성 확정이 아니다.

## 1. 비교군·API 계약

- O00=`ionia`, V01=`ionia-second`, V02=`ionia-third`, V03=`ionia-fourth`, V04=`ionia-fifth`, V05=`ionia-sixth`, V06=`ionia-seventh`, V07=`ionia-eighth`. 각 방식은 O00와 독립 비교한다. 표시 ID와 과거 문서명은 따로 보존한다.
- Hermes는 향후 같은 공통 API/측정점에 맞춘 adapter가 구현·검증돼야 참여한다. 현재 구현 또는 SSD 적용이 완료됐다고 가정하지 않는다.
- v1 실행 준비 schema는 O00/V01–V07만 허용한다. `HERMES`/`UNREPLICATED` ID는 초안 예약값이며, 별도 protocol·node count·persistence profile을 정의하기 전에는 ready로 승격할 수 없다.
- GET은 value 또는 MISS. 정상 MISS도 semantic completion이지만 known-existing key의 예상 밖 MISS는 correctness 검사의 대상이다.
- SET은 이전 값/실행 결과를 반환하지 않는 blind overwrite/insert ACK. ACK가 각 replica의 KV apply 또는 fsync 완료를 뜻하지 않는다.
- DELETE는 삭제 결과 OK/MISS를 반환한다. 현재 IONIA Client에서 leader의 ordered/executed 경로를 사용한다. SET fast-ACK 지연과 별도 집계한다.
- SCAN/atomic RMW/TTL/eviction은 공통 API 시험 범위 밖이다. 미지원 항목을 GET/SET으로 조용히 치환해 YCSB 전체 재현이라고 부르지 않는다.
- replica RAM durability log·stable-view force·선택적 KV force를 별도 설정으로 고정한다. 전체 동시 전원 손실/SSD WAL 내구성은 이번 보장에 포함하지 않는다.

근거: [O00 명세](https://app.notion.com/p/4064ed9024378122a673e1b4a7467ca1), [복구 명세](https://app.notion.com/p/4064ed9024378169a3eac3697244c9c3). 기존 Java `IoniaClient.execute`의 DELETE 분기와 `IoniaFileStorage`를 읽기 전용으로 대조했다.

## 2. EC2 배치·공정성

| 구성 | replicas | load generators | controller/수집 | spare |
| --- | --- | --- | --- | --- |
| 3노드 | 3 | 2 | 1 | 2 |
| 5노드 | 5 | 2 | 1 | 0 |

각 slot은 별도 EC2 한 대에 해당한다. 실제 ID/주소·AZ·storage·자원은 미확인. 비교 run에 동일 instance set, server core/heap/engine/transport/force 설정을 사용한다. leader placement를 block별 회전하고 O00/변형 실행 순서를 회전·기록한다. 재시도·멤버십과 API 보장을 바꾸어 동일 조건처럼 비교하지 않는다. 공동 사용자 작업/용량/권한 확인 전 실제 실험을 시작하지 않는다.

3노드는 IONIA qFast=3/qConsensus=2, 5노드는 qFast=4/qConsensus=3이다. 1 follower failure의 fast-path 가용성을 구분한다. majority가 살아 있어도 fallback/view가 구현·설정 조건에서 진행해야 하며 시간 상한을 임의 보장하지 않는다. Hermes의 active membership ACK 조건 및 membership 변경은 해당 adapter의 계약으로 검증한다.

## 3. 공통 데이터/요청열

1. 후보 live key 수1M/10M, key24B, value100B/1024B. 큰 데이터셋은 실제 cache budget과 접근 I/O를 확인한 후 정한다.
2. key 인코딩은 injective 고정 길이여야 한다. 잘라서 같은 key가 되는 generator는 부적합. value는 seed/client/sequence와 checksum으로 생성하고 인코딩 버전을 기록한다.
3. load trace와 measured trace를 분리해 hash한다. warm-up은 별도 seed stream을 사용한다. 기존 Runner의 같은 seed 재생 관행을 그대로 새 benchmark 계약으로 쓰지 않는다.
4. 모든 replica의 load/apply가 target frontier까지 끝나고 미완료 audit가 없는지 확인한 뒤 warm-up한다. stable point에서 시작한다.
5. paired seed 101/202/303/404/505를 각 방식에 동일하게 사용한다. 시드만 같다고 부하 host 수/각 stream 분할/PRNG 버전 차이를 무시하지 않는다. manifest에 stream mapping을 고정한다.
6. uniform 기본. Zipf theta0.75/0.99, fixed-hot p0.9와 moving-hot은 sensitivity 계열이다. 분포의 key permutation·hot ID·변경 시각과 관측 빈도를 기록한다.
7. read-latest/insert는 별도 trace semantics가 필요하다. 첫 v1 핵심 혼합 시험의 SET은 fixed existing key overwrite다.
8. DELETE 혼합은 live key/known-MISS 비중을 함께 보고한다. 임의 병렬 요청에 대해 단일 최종 map으로 매 GET의 정답을 판정하지 않는다.
9. 초기 데이터, live key bytes, 실제 append file bytes는 다르다. 현재 `IoniaFileStorage`는 JDK append adapter로, SET/DELETE가 파일을 늘리며 LSM compaction/GC가 아니다. record 증가 시작 추정은 SET당25+key+value byte, DELETE당25+key byte이지만 metadata·recovery·FS 비용은 별도다. 매 run 독립 data directory와 공간 budget이 필요하다. 기존 데이터 삭제 자동화는 이번 범위에 없다.

## 4. 부하·시간·반복

- 사전진단용1M requests, 본비교 후보10M requests×5 runs. warm-up 최소120초·본측정 최소300초를 시작값으로 설정한다. pilot 이후 paired 전체 비교에 같은 값을 고정한다.
- fixed-N은 총 logical requests가 load host/stream에 중복 없이 나뉘며 replica 수만큼 다시 세지 않는다. 이른 종료로 최소 측정 시간을 채우지 못하면 INSUFFICIENT_DURATION으로 남기고 모든 비교군의 공통 N을 재설정한다. 빠른 방식만 임의 추가 요청을 보내지 않는다.
- fixed-duration은 별도 프로파일이다. LONG-01 후보1800초 동안 같은 offered rate/trace generator를 쓴다. 서로 다른 achieved request 수는 정상적으로 기록한다.
- closed-loop 총concurrency 후보1/8/32/128은 두 load host를 합친 값이다. 한 logical op 완료 후 다음 op를 보낸다.
- open-loop는 공통 offered rate와 예정 발행 시각을 고정하고 bounded admission을 사용한다. 발행 지연/누락/drop/거절도 계수한다. client가 밀린 요청을 버려 낮은 latency를 만드는 결과를 허용하지 않는다.
- user retry는 기본 same-ID bounded 정책으로 명시하고 총 deadline을 재시도마다 늘리지 않는다. protocol 내부 메시지와 user-level attempt를 구분한다. retry 없는 대조는 별도 config다.
- run phase: PLAN_VALIDATE → LOAD → LOAD_DRAIN → WARMUP → MEASURE → DRAIN → AUDIT → EXPORT. 지표에 phase를 붙이고 warm-up/loading/drain을 정상 측정 분모에 섞지 않는다.
- 저/중/고 부하, 같은 절대 offered rate 비교와 각자의 최대 throughput 비교를 분리한다. SLO값이 없으면 SLO 내 최대 goodput을 보고하지 않는다.
- node 간 wall-clock 차이로 μs급 지연을 만들지 않는다. client 단조시각과 controller 관찰을 우선 사용한다. 동기화 불확실성 이상 정밀도를 주장하지 않는다.

## 5. 판정 체계

| 판정 | 의미 |
| --- | --- |
| NOT_READY / NOT_RUN | 설정·기능·환경 미확정 / 미실행. template의 현재 상태 |
| COMPLETED | 계획한 실행·export가 끝남. 정확성 PASS라는 뜻이 아님 |
| VALID / INVALID | 수집/입력/clock/장애주입/필수 artifact 조건의 충족 여부 |
| PASS / FAIL | 해당 safety·API·liveness 조건을 실제 증거로 만족/위반 |
| INCONCLUSIVE | checker timeout·불충분 observation·원인 미확정 등 |

정상 부하에서 오류0은 동작 인수 기준이다. 오류 발생 run도 원시값을 보존하며 성공 run만 골라 재실행하지 않는다. 과부하·장애 시험의 오류는 예정한 조건과 비교해 평가한다. 실패한 실행이 반드시 수집 INVALID인 것은 아니고, 반대로 수집 VALID라도 protocol safety FAIL일 수 있다.

liveness 관찰 시간은 deadline·failure detector timeout·관측기간을 manifest에 명시한 뒤 판정한다. 원문/구현이 제공하지 않는 보편적인 “몇 ms 안에 반드시 복구”를 발명하지 않는다. 권한/제약으로 주입하지 못한 fault를 PASS로 치환하지 않는다.

## 6. 시나리오 카탈로그

각 카드의 절차는 위 공통 phase 뒤에 적용한다. fixture 기반 correctness와 실제 EC2 실험은 evidence type으로 구분한다. 나열된 parameter 후보는 한 번에 모두 곱하는 필수 대규모 matrix가 아니다. 단계별 gate 통과 후 확장한다.

### ENV-01 — 환경·동일조건 검증

- 범주/대상: gate · all · 3/5노드.
- 사전조건: EC2 8대 중 replica 3/5·load 2·control 1 배치와 storage/CPU/clock/권한/독점 사용 확인
- 실행 절차: EC2 접속·수집은 사용자 요청으로 보류. inventory_id와 role mapping을 미확정으로 둔다.
- 판정: 환경 식별값 누락이면 NOT_READY. 원격 조작·비용 변경 없음.
- 계측: M36; 성능 run 공통 분석 M35/M36.
- 상태: **NOT_RUN**.

### FUN-01 — 순차 CRUD 의미

- 범주/대상: correctness · all · 3/5노드.
- 사전조건: 독립 저장 경로·빈 keyspace·고정 값·삭제 존재 여부 계약
- 실행 절차: GET 없는키→MISS; SET A→OK; GET→A; SET B→OK; GET→B; DELETE→OK; GET→MISS; DELETE→MISS; SET C→OK; GET→C
- 판정: 정확히 기대한 상태/값. DELETE는 실행결과 반환 경로. 인증된 client 응답만 판정.
- 계측: M01, M02, M03, M05, M07, M28; 성능 run 공통 분석 M35/M36.
- 상태: **NOT_RUN**.

### COR-01 — 동시 history 선형성

- 범주/대상: correctness · all · 3/5노드.
- 사전조건: 1–8키·2–4client·64 seed, 검사기 부정 대조군
- 실행 절차: 짧은 20–30 logical operation 이력을 반복. SET값에 client/sequence 넣기. 단일 client 단조시간으로 invocation/response 기록, 노드간 순서는 causal barrier로 보강.
- 판정: VIOLATION=0, checker positive/negative controls 통과. checker timeout/미확정은 INCONCLUSIVE. 동시 GET은 단일 최종값 oracle로 검사하지 않음.
- 계측: M01, M02, M03, M07, M28, M29; 성능 run 공통 분석 M35/M36.
- 상태: **NOT_RUN**.

### COR-02 — stale follower·History trim

- 범주/대상: correctness · ionia · 3/5노드.
- 사전조건: 제어 가능한 지연 fixture, IONIA adapter
- 실행 절차: SET ACK 뒤 follower apply를 지연시키고 GET. leader pending DATA 및 History trim/LTI 경계 각각 실행.
- 판정: 오래된 값/MISS를 최종 정상응답으로 반환하지 않음. fallback·rejection은 별도 계수.
- 계측: M13, M14, M25, M28; 성능 run 공통 분석 M35/M36.
- 상태: **NOT_RUN**.

### COR-03 — 재시도·중복·늦은 응답

- 범주/대상: correctness · all · 3/5노드.
- 사전조건: 동일 logical ID와 deadline, 중복 주입 fixture
- 실행 절차: 응답 지연·유실·동일요청 중복·view 전환 중 재시도. clientId는 전체 load host에서 유일하게 생성.
- 판정: 논리적 완료 1회. 서로 다른 값으로 같은 ID 재사용 금지. timeout 이후 실제 반영 가능성은 이력의 pending completion으로 처리.
- 계측: M01, M02, M04, M07, M08, M09, M28, M29; 성능 run 공통 분석 M35/M36.
- 상태: **NOT_RUN**.

### COR-04 — ACK 직후 미적용 쓰기 보존

- 범주/대상: correctness · all · 3/5노드.
- 사전조건: 별도 ACK ledger, apply/consensus 지연 fixture
- 실행 절차: SET ACK 이후 apply 전에 leader/follower를 종료하고 허용 장애 수 내에서 복구. overwrite 없는 audit keys 사용.
- 판정: ACK된 각 audit value/삭제가 후속 합법 update가 없을 때 보존. 최종 map 같음만으로 충분하지 않음.
- 계측: M13, M14, M28, M29, M30; 성능 run 공통 분석 M35/M36.
- 상태: **NOT_RUN**.

### COR-05 — 복구 중 노드·old view fencing

- 범주/대상: correctness · all · 3/5노드.
- 사전조건: 복구 중 replica 및 partition fixture
- 실행 절차: 재시작/상태이전 중 read와 격리된 old leader 접근, 늦은 이전 view 응답을 주입.
- 판정: 복구 완료 전 stale 정상응답 없음·이전 view 증명 수락 없음·중복 로그 효과 없음.
- 계측: M25, M27, M28, M30; 성능 run 공통 분석 M35/M36.
- 상태: **NOT_RUN**.

### PERF-01 — GET 100%

- 범주/대상: performance · all · 3/5노드.
- 사전조건: 초기 적재된 고정 existing keys·apply drain·동일 trace
- 실행 절차: GET100, uniform 기본. 3/5 replica와 concurrency 1/8/32/128을 pilot로 검증 후 고정.
- 판정: 정상 오류율 0을 인수 기준으로 사용. throughput 우위는 합격 기준으로 강제하지 않음. 미포화·부하기 병목은 표시.
- 계측: M01, M02, M03, M05, M07, M10, M11, M12, M15, M16, M17, M18, M19, M20, M21, M22, M23, M24; 성능 run 공통 분석 M35/M36.
- 상태: **NOT_RUN**.

### PERF-02 — SET 100% 덮어쓰기

- 범주/대상: performance · all · 3/5노드.
- 사전조건: 동일 preloaded keyspace, 신규 적재 시험과 분리
- 실행 절차: SET100 overwrite, 측정 중 apply/backlog 및 종료 뒤 drain 관찰. 100B/1024B 값을 별도 실행.
- 판정: 완료·오류·late 처리 집계 보존. backlog 발산/미해소를 숨기지 않음. 빠른 ACK만으로 지속 성능 합격 처리하지 않음.
- 계측: M01, M02, M03, M05, M07, M10, M11, M12, M13, M14, M15, M16, M17, M18, M19, M20, M21, M22, M23, M24; 성능 run 공통 분석 M35/M36.
- 상태: **NOT_RUN**.

### PERF-03 — GET95/SET5

- 범주/대상: performance · all · 3/5노드.
- 사전조건: 동일 existing keyspace·uniform 기본
- 실행 절차: GET95 SET5 overwrite; paired seeds 및 입력 sequence 공통. GET/SET histogram 분리.
- 판정: PERF-01/02 공통 기준. 혼합 percentile로 읽기/쓰기 지연을 대체하지 않음.
- 계측: M01, M02, M03, M05, M07, M10, M11, M12, M13, M14, M15, M16, M17, M18, M19, M20, M21, M22, M23, M24; 성능 run 공통 분석 M35/M36.
- 상태: **NOT_RUN**.

### PERF-04 — GET50/SET50

- 범주/대상: performance · all · 3/5노드.
- 사전조건: 동일 existing keyspace·uniform 기본
- 실행 절차: GET50 SET50 overwrite; 노드별 역할과 read/meta/apply 부하를 관찰.
- 판정: 공통 정상 기준과 backlog 판정. primary bottleneck과 storage 병목을 구분.
- 계측: M01, M02, M03, M05, M07, M10, M11, M12, M13, M14, M15, M16, M17, M18, M19, M20, M21, M22, M23, M24; 성능 run 공통 분석 M35/M36.
- 상태: **NOT_RUN**.

### PERF-05 — GET80/SET15/DELETE5

- 범주/대상: performance · all · 3/5노드.
- 사전조건: 회전 keyspace·초기 occupancy·membership-changing operation trace 고정
- 실행 절차: GET80 SET15 DELETE5. 순차/단일 owner trace에서 expected-hit case를 만들고 일반 동시 경합에서는 actual HIT/MISS 비율을 기록.
- 판정: MISS를 시스템 오류로 세지 않음. 존재 여부 의미는 FUN-01/COR-01에서 검사. live key 수 변화 및 append 파일 증가 기록.
- 계측: M01, M02, M03, M05, M07, M10, M11, M12, M14, M18, M19, M20, M23, M24, M28; 성능 run 공통 분석 M35/M36.
- 상태: **NOT_RUN**.

### PERF-06 — 공통 도착률–지연/과부하 곡선

- 범주/대상: performance · all · 3/5노드.
- 사전조건: open-loop scheduler·bounded queue·pilot peak·client 능력 검증
- 실행 절차: 최저 pilot peak의 10/30/50/70/90% 공통 offered rate, 110%는 overload 별도. client queue·drop·거절 포함.
- 판정: 예정 발행 누락을 제외하지 않음. 드롭은 성공 throughput 아님. SLO 임계치는 manifest 값 없으면 SLO 비교 불가.
- 계측: M01, M02, M03, M04, M05, M06, M07, M08, M09, M10, M11, M12; 성능 run 공통 분석 M35/M36.
- 상태: **NOT_RUN**.

### SENS-01 — 데이터/캐시와 value 크기

- 범주/대상: sensitivity · all · 3/5노드.
- 사전조건: memory_budget_bytes 및 실제 storage 확인 필요
- 실행 절차: small/large working-set, 100B/1024B values. 초기1M/10M키는 후보; large는 cache 밖이라는 증거 필요.
- 판정: 캐시 상주/SSD-bound 미확인은 UNKNOWN, 크기만으로 SSD 접근을 주장하지 않음.
- 계측: M10, M14, M18, M19, M20, M21, M22, M23, M24; 성능 run 공통 분석 M35/M36.
- 상태: **NOT_RUN**.

### SENS-02 — 키 쏠림·동적 hotspot

- 범주/대상: sensitivity · all · 3/5노드.
- 사전조건: Zipf generator 검증·명시적 mapping 및 hash
- 실행 절차: uniform 대조, Zipf theta0.75/0.99, hot-key 확률0.9·중간 hotspot 이동. 분포별 개별 trace.
- 판정: 관측 분포/충돌률 보존. Zipf와 fixed-hot을 같은 분포로 표기하지 않음.
- 계측: M05, M10, M11, M12, M14, M25, M26; 성능 run 공통 분석 M35/M36.
- 상태: **NOT_RUN**.

### FAULT-01 — follower 1대 process crash

- 범주/대상: fault · all · 3/5노드.
- 사전조건: 허용된 대상 process만 종료·persistent data 보존·control timestamp
- 실행 절차: 측정180초 후 follower1 crash, 120초 관찰 후 재시작·재합류. 상세 시각은 pilot 후 manifest 고정.
- 판정: 3노드 qFast부족과5노드 qFast4 유지 차이를 계측. 성공 ACK 안전성 검사. fallback/membership 지연을 분리.
- 계측: M01, M03, M05, M07, M10, M14, M27, M28, M29, M30; 성능 run 공통 분석 M35/M36.
- 상태: **NOT_RUN**.

### FAULT-02 — leader/coordinator process crash

- 범주/대상: fault · all · 3/5노드.
- 사전조건: 현재 leader/명시한 coordinator의 ID를 event에 기록
- 실행 절차: 같은 시각 프로파일로 leader 또는 Hermes coordinator crash, 신규 view·재시작·audit read.
- 판정: 어떤 역할이 중단됐는지 명시. 고정 recovery ms 보장 없이 실제 중단·복귀 측정 및 safety 판정.
- 계측: M05, M07, M10, M14, M26, M27, M28, M29, M30; 성능 run 공통 분석 M35/M36.
- 상태: **NOT_RUN**.

### FAULT-03 — 느린 replica

- 범주/대상: fault · all · 3/5노드.
- 사전조건: delay target과 request 방향·duration 고정, 별도 fault driver 필요
- 실행 절차: replica 한 대에 20/80ms 지연을 제어 주입(설계 값). 정상·delay·해제 구간 분리.
- 판정: 지연과 crash를 구분. 주입 실패는 INVALID_RUN. membership 제외에 따른 RF 변화를 보고.
- 계측: M05, M07, M10, M11, M12, M14, M25, M26, M27; 성능 run 공통 분석 M35/M36.
- 상태: **NOT_RUN**.

### FAULT-04 — partition/정족수 부족

- 범주/대상: fault · all · 3/5노드.
- 사전조건: 대상 주소·방향·heal 계획이 manifest에 명시돼야 실행 가능
- 실행 절차: 3노드2+1·5노드3+2, 별도 no-majority 분할. 격리된 old leader 읽기·heal·재합류.
- 판정: 불안전한 정상응답 0. liveness는 해당 quorum/membership 가정이 회복된 구간에서만 검사.
- 계측: M05, M07, M10, M14, M25, M26, M27, M28, M29, M30; 성능 run 공통 분석 M35/M36.
- 상태: **NOT_RUN**.

### LONG-01 — 30분 지속/회복

- 범주/대상: longrun · all · 3/5노드.
- 사전조건: 고정 trace seed·append 저장량과 disk budget·hard stop
- 실행 절차: 30분 동안 혼합부하, burst/hotspot change 후 원상복귀. phase별1초 창·종료 후drain.
- 판정: 메모리/backlog/공간 증가율·정상화 여부 보고. 현재 append-only file adapter에서 LSM compaction을 측정했다고 하지 않음.
- 계측: M05, M07, M10, M11, M12, M14, M18, M19, M20, M21, M22, M23, M24; 성능 run 공통 분석 M35/M36.
- 상태: **NOT_RUN**.

### VAR-01 — V01 leader-only reads

- 범주/대상: variant · V01 · 3/5노드.
- 사전조건: V01과O00, 동일 read placement 이외 설정
- 실행 절차: PERF-01/03/04 paired 비교, 동일 node/CPU/storage.
- 판정: leader CPU/I/O·read 분포와 처리량 차이를 함께 설명.
- 계측: M05, M10, M17, M18, M25; 성능 run 공통 분석 M35/M36.
- 상태: **NOT_RUN**.

### VAR-02 — V02 History soft budget

- 범주/대상: variant · V02 · 3/5노드.
- 사전조건: history budget 후보·동일 bounded workload
- 실행 절차: budget2개 이상 + O00 대조, trim 경계 포함.
- 판정: 실제 history/초과량·fallback·정확성. soft budget을 hard cap으로 판정하지 않음.
- 계측: M10, M23, M25, M28; 성능 run 공통 분석 M35/M36.
- 상태: **NOT_RUN**.

### VAR-03 — V03 Prepare batch

- 범주/대상: variant · V03 · 3/5노드.
- 사전조건: count/byte/delay 후보와 long-lived client
- 실행 절차: batch1/기본/확대, 저·중·고부하. parameter 한 축씩.
- 판정: 실효batch·대기·in-flight·CPU/bytes·write지연. input count와실제batch분리.
- 계측: M05, M10, M14, M15, M16, M31; 성능 run 공통 분석 M35/M36.
- 상태: **NOT_RUN**.

### VAR-04 — V04 applying-key K

- 범주/대상: variant · V04 · 3/5노드.
- 사전조건: 실행 worker·stripe 수 고정
- 실행 절차: K1/2/4 후보·uniform/hotkeys. 기존 source와 분리.
- 판정: 실제 applying keys/동일키 순서·backlog. 속도이득 강제 없음.
- 계측: M05, M10, M14, M24, M28, M32; 성능 run 공통 분석 M35/M36.
- 상태: **NOT_RUN**.

### VAR-05 — V05 actual I/O workers W

- 범주/대상: variant · V05 · 3/5노드.
- 사전조건: stripe S 고정·W만변화
- 실행 절차: W1/2/4 후보(cores/valid config 범위), 동일 S·key/trace.
- 판정: worker busy·queue·CPU/disk QD·동일키순서. S 변화 효과와 분리.
- 계측: M05, M10, M18, M19, M24, M28, M32; 성능 run 공통 분석 M35/M36.
- 상태: **NOT_RUN**.

### VAR-06 — V06 replica selection

- 범주/대상: variant · V06 · 3/5노드.
- 사전조건: 동일 RTT·outstanding 기준·탐색 설정 보존
- 실행 절차: 고정 선택/현재 selector, 느린 노드·hotspot·delay 변화.
- 판정: 선택분포·관측치 age·적응시간·tail·fallback. C3/Tars 전체재현 아님.
- 계측: M05, M10, M11, M25, M33; 성능 run 공통 분석 M35/M36.
- 상태: **NOT_RUN**.

### VAR-07 — V07 MetaQuery batching

- 범주/대상: variant · V07 · 3/5노드.
- 사전조건: count/byte/delay/pending/inflight 명시
- 실행 절차: batch1/기본/확대, 한frame 지연·개별deadline혼합·저부하.
- 판정: 메타batch occupancy·frame HOL/공동실패·연결비용. READ/probe/write batching으로표기금지.
- 계측: M05, M10, M15, M16, M25, M31, M34; 성능 run 공통 분석 M35/M36.
- 상태: **NOT_RUN**.

## 7. 실행 gate와 우선순위

1. 명세/schema 참조 검사(이번 범위). EC2는 보류.
2. logical generator/adapter/telemetry 구현. FUN-01·COR-01 부정 대조 포함.
3. 나머지 correctness·recovery fixture. 위험한 fault는 대상/복원/관측 기능이 준비될 때까지 NOT_READY.
4. 환경 조사·pilot 허용 후 LOAD/초기화/clock/load generator/관측 비용/공간 사전진단.
5. uniform·기본 value·하나의 qualified dataset에서 PERF-01–05, 3/5노드·O00부터 진행.
6. latency-load 곡선·skew/cache/value·변형별 ablation·fault/longrun으로 확장.
7. Hermes가 구현·검증되면 같은 trace·계측 경계·보장 조건으로 편입. 원논문의 RAM/RDMA 숫자와 현재 Java/TCP/파일 adapter를 직접 순위화하지 않는다.

최소 산출물과 metric 정의는 METRICS.md, 현재 미구현 차이는 IMPLEMENTATION_GAP.md를 따른다.

