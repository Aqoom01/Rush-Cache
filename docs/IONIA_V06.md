# V06 — MetaQuery 배치 전송 구현·기능 검증

2026-10-29. V06을 독립 replication/ionia-eighth(Java package ionia_eighth)에 구현했다. 시작 HEAD c077217ff2625cdf7fc94d5d71afaf62f363d09b·clean 확인. commit/push/merge 미실행.

## 원문 근거·자체 설계·재사용

**R:** [IONIA FAST2024 §4.2.1–4.2.2, §6](https://www.usenix.org/system/files/fast24-xu.pdf). §6은 client-side MetaQuery batching을 향후 최적화로 제안한다. 세부 batch 정책·안전성·성능이 논문에서 이미 검증됐다는 뜻이 아니다. 관련 절을 재검토했으며 새 논문 전체 정독/성능 재현으로 집계하지 않는다.

**D:** E/B/D/P/W 상한·호출별 token·개별 fresh Meta 실행·전체 응답 검증·opcode7·입출력 한도는 자체 설계다. Meta 결과 캐시나 같은 key/operation의 Meta 합치기는 없다.

**재사용:** 원본 IoniaClient의 captured applied-prefix/Meta INDEX·DATA/fallback/majority View 인증, IoniaEngine, Log/History/LTI·복구, IoniaWire, 일반 IoniaTcpTransport, 파일 KV/stable View를 직접 사용한다. 새 Engine/Client 사본 없음. final/private Node 조립부만 IoniaEighthNode로 분리해 batch 수신 branch를 추가했다. V01–V05 정책을 누적하지 않았고 공통 API/의존성 변경 없음.

## 상태·전송·완료 규칙

1. 원본 client가 개별 GET의 READ·View probes를 즉시 보내고 Meta만 큐에 넣는다. initial follower 고정이며 그 노드가 새 leader가 되면 원본 leader-read 경로를 쓴다. V05 adaptive 선택을 추가하지 않았다.
2. 큐는 (leader,view)별 FIFO다. count E, 정확한 body byte B(4-byte count + item당61+key bytes), delay D 또는 가까운 개별 deadline으로 전송한다. 같은 logical ID/key 호출도 서로 다른 양수 token을 갖는다.
3. relative timeout에서 큐·TCP executor/연결 대기시간을 차감한다. 서버는 자기 시계로 요청별 deadline을 만들고 최종 원본 client deadline 검사도 유지한다. 절대 nanoTime을 wire에 보내지 않는다.
4. 서버는 전체 입력을 검증하고 모든 item의 engine.metaQuery를 먼저 제출한다. 각 item마다 fresh History/LTI 또는 pending DATA를 얻는다. 배치 전체의 원자적 snapshot은 보장하지 않는다.
5. 응답은 한 frame이다. 전체 token 집합·개수·node/view/client/op을 검증한 뒤 개별 future를 완료한다. 역순은 허용, duplicate/missing/extra/identity 오류는 batch 전체 UNAVAILABLE이다. 성공 prefix를 먼저 내보내지 않는다.
6. Meta DATA는 값·MISS·오류를 포함한다. INDEX와 stale READ fallback은 원본에 맡긴다. View advance는 old queued/visible Meta를 fence하고 동일 View refresh는 보존한다.
7. public GET 취소는 private original work를 취소하지 않는다. P는 queued+raw in-flight item, W는 raw batch stage 수다. in-flight timeout/cancel/View/close 후에도 raw terminal까지 permit 유지. queued expiry/close는 즉시 회수한다. borrowed transport는 client가 닫지 않는다.
8. transport dispatch·future completion은 제어 lock 밖에서 실행한다. thread-local trampoline으로 동기 completion 재귀를 막는다. 별도 사용자 callback executor나 모든 blocking callback의 진행 보장은 없다.

## 설정·호환성

| 설정 | 범위 | 예제 |
| --- | --- | --- |
| meta.batch.entries E | 1–64 | 8 |
| meta.batch.bytes B | 65–1048576 body bytes | 65536 |
| meta.batch.delay.ms D | 0–1000 | 2 |
| meta.pending.limit P | E–65536 | 256 |
| meta.inflight.limit W | 1–256 | 4 |

E1/D0은 즉시 전송 대조군이다. 예제 및 TCP 검증 D1초/E8은 기능 fixture이고 최적값이 아니다. 라이브러리는 onTimer 호출이 필요하다. CLI timer10ms이므로 D2ms는 정확한2ms wall-clock 상한이 아니다. W 포화·스케줄·GC·callback으로 더 기다리면 개별 deadline에서 TIMEOUT이다.

wire 입력64item/1MiB, 응답 body60MiB/기존 frame64MiB 한도. DATA 합계 초과 item은 식별자를 유지한 UNAVAILABLE이며 INDEX/MISS/성공으로 위장하지 않는다. membership ID + ionia-meta-batch-v1 protocol을 wire/stable View에 기록해 O00와 구별한다. client E/B/D/P/W hash는 별도 metadata이며 여러 설정이 같은 V06 cluster에서 실행 가능하다. 기존 data directory를 자동 전환/삭제하지 않는다.

## 실제 검증·실패 이력

Java21.0.11 전체 build/installDist --rerun-tasks 통과. 기본 병렬·Windows 포트 사전진단 유지. **전체498개(core392/server90/runner16), 실패·오류·skip0, V06 연결41개(core34/server7)**. checker/fixture 재사용을 포함한 테스트 개수이며 증명 개수가 아니다.

- 3/5 replicas×E1/E4×64seeds×10CRUD=2560 유한 완료 operation 이력, 결과 불명확 write completion 확장·bad-history controls.
- 실제 엔진 stale value/MISS·나중 History trim·pending DATA·DELETE, a<i/a=i/a>i·잘못된 READ·Meta/개별 View 인증.
- 실제 TCP32hot-key write와32GET/E8→4 Meta RPC·S2, 만료/정상 item 혼합, 잘못된 envelope/configuration/stable metadata 거절.
- 독립 JVM3개 leader/follower 각각 강제 종료·restart/rejoin·majority fallback·CLI CRUD. 하나의 Windows loopback 호스트 조건이다.
- 기존 보호262파일 SHA-256 동일(core Gradle·개발기록 제외). Gradle 새main/test roots4줄. production JAR class포함/test제외·installDist·외부 production 의존성 없음 확인.

최종 전체 증거: build/ionia-validation/20261029-091346-00a8dc25. 첫 전체 실행 V06 TCP32GET fixture는 OK 대신 UNAVAILABLE1회였다. 당시 RPC 상세 로그가 없어 원인을 소급 확정하지 않는다. 진단을 추가한 최종 전체와 동일 TCP 시나리오 추가5회(각2개 테스트)는 통과했다. 개별 반복 결과는 VALIDATION의 tcpRepeats에 보존하며 반복을 새 테스트 개수로 합산하지 않는다. V06의 모든 TCP 타이밍 문제가 해결됐다고 주장하지 않는다. 기존 V03의 직전 단계 단발 TIMEOUT도 이번 변경으로 원인을 해결했다고 주장하지 않는다.

| 인수 | 확인 항목 | 테스트/증거 |
| --- | --- | --- |
| V06-01 | 설정 범위/missing/profile/protocol 분리 | IoniaMetaBatchTest.policyValidationAndStableProtocolAreIndependentOfClientTuning |
| V06-02 | E/B/D, exact byte boundary/near deadline | IoniaMetaBatchTest.countAndTokensKeepDuplicateLogicalIdsSeparate;byteBoundaryFlushesPrefixAndRejectsOversize;timerAndNearDeadlineFlushWithoutExtendingBudget;shortDeadlineBehindLongRequestFlushesBothWithoutWaitingForHeadAge |
| V06-03 | P/W·32 callers·cancel/timeout raw permit | IoniaMetaBatchTest.inFlightAndPendingLimitsHoldAcrossCancelAndExpiry;rawTimeoutKeepsPermitUntilActualTerminal;concurrentCallersNeverExceedAdmissionAndPhysicalCaps |
| V06-04 | token/identity/reorder/duplicate/missing/unknown/null | IoniaMetaBatchTest.reorderedAnswersMapByOccurrenceToken;malformedEnvelopeCannotPartiallyPublishValidPrefix;leaderGroupsAndOutOfOrderPhysicalBatchesStayIndependent;tokenExhaustionFailsClosedWithoutWrappingOrOverwriting |
| V06-05 | throw/null/exception/cancel raw stage·close | IoniaMetaBatchTest.throwingNullExceptionalAndCancelledStagesReleaseExactlyOnce;closePublishesUnavailableButRetainsRawPermits |
| V06-06 | callback lock 밖·5000 synchronous stage trampoline | IoniaMetaBatchTest.callbacksAndSynchronousReentryRunOutsideControlLock;synchronousBorrowedCompletionDrainsThousandsOfQueuedBatchesWithoutRecursion |
| V06-07 | 매 호출 fresh Meta·캐시 없음 | IoniaMetaBatchTest.successiveIdenticalLogicalRequestsNeverReuseACompletedMetaAnswer |
| V06-08 | View/leader별 큐·old batch fence·same View 유지 | IoniaMetaBatchTest.viewAdvanceFencesQueuedAndLateInFlightWithoutMixingGroups;IoniaMetaBatchReadBoundaryTest.oldViewCertificateAndEndpointAdvanceCannotAcceptLateBatch |
| V06-09 | a<i/a=i/a>i·forged READ·DATA/MISS·cancel/deadline | IoniaMetaBatchReadBoundaryTest |
| V06-10 | 실제 엔진 stale value/MISS와 나중 History trim·pending DATA·DELETE | IoniaMetaBatchProtocolTest |
| V06-11 | 3/5 isolated old leader·timeout write View retry | IoniaMetaBatchProtocolTest.V06_13_isolatedOldLeaderCannotCertifyAReadAfterNewView;IoniaMetaBatchHistoryTest.V06_15_timedOutWriteSurvivesViewChangeAndRetryRetainsOneLogicalId |
| V06-12 | 3/5×E1/E4×64seeds×10CRUD=2560 finite ops·bad controls | IoniaMetaBatchHistoryTest |
| V06-13 | wire malformed/truncated/count/token/oversize/trailing·INDEX/DATA | IoniaMetaBatchCodecTest |
| V06-14 | 60MiB reply budget 초과 item UNAVAILABLE | IoniaMetaBatchCodecTest.responseBudgetPreservesIdentityAndReportsOverflowAsFailure |
| V06-15 | TCP32 hot writes·32GET/E8→4Meta·혼합deadline·S2·config | IoniaMetaBatchTcpTest |
| V06-16 | JVM3 leader/follower crash/restart/rejoin·CLI CRUD | IoniaMetaBatchProcessTest |
| V06-17 | 전체 build/installDist·dependency/JAR/source hash | manifest·SHA-256·JAR inspection |

## 미완료·해석 한계

P/W는 이 client의 Meta item/raw stage만 제어한다. 전체 READ/probe/쓰기 socket·서버 잔류 작업·heap의 전역 상한이 아니다. BUSY 반환 전에 원본 READ/probes가 발생할 수 있다. 영구 미완료 borrowed stage면 permit도 남는다. 한 느린 pending DATA가 같은 frame을 지연시키고 네트워크 실패는 batch 전체에 영향을 준다. 이 가용성·지연 비용을 후속 성능 비교에서 기록해야 한다.

공통 GET/SET/DELETE 데이터셋·장기 client driver·처리량/p95/p99/heap/CPU/GC/NIC/SSD, 최적 E/B/D/P/W, multi-host·전원 장애·LSM/SplinterDB 재현은 미완료다. 독립 Replica RAM Durability Log·stable View·선택적 KV force를 구별하며 per-request stable WAL/fsync(D-PERSIST)를 추가하지 않았다. 기존64MiB state/frame·32MiB KV 한도와 메시지별 TCP 연결 제약도 유지한다. 이번 유한 fixture를 모든 interleaving의 증명이나 성능 향상으로 해석하지 않는다.

## 공통 benchmark adapter 연결 후속 (2026-10-30)

현재 표시 ID V07의 실제 알고리즘을 server benchmark/ionia_eighth adapter에 연결했다. 위 과거 문서명·원래 profile 표기/최초 알고리즘 검사 기록은 보존한다. 기존 알고리즘/core API/wire는 변경하지 않고 공통 ID/deadline/retry·trace·closed/open-loop·전수 지연·로컬 표준 export를 재사용한다.

V07의 실제3/5노드·두 실행 방식·읽기/혼합8조건과 별도 CRUD 값·DELETE OK/MISS·재삽입을 검증하고 결과8개를 schema/hash·histogram/설정으로 대조했다. 여섯 방식 합계 추가99/전체760검사 통과이며 개별 방식의99검사라는 뜻은 아니다. ../experiments/replication-benchmark-v1/ADAPTERS_V02_V07.md 및 REMAINING_ADAPTERS_VALIDATION.json/REMAINING_ADAPTERS_SOURCE_PRESERVATION.json/REMAINING_ADAPTERS_ARTIFACT_CHECK.json을 따른다.

V06 학습의 phase 분리·V07 coalescer의 client별 범위, ready manifest/서버 inventory·공통 정확성/필수 계측·clock/overhead/remote 관찰을 정식 성능 전 보강해야 한다. 본시험 데이터/요청열 후보·pilot과 EC2 접속/진단·성능 비교는 후속이며 commit/push/merge는 하지 않았다. RAM log·stable View/선택적 KV force와 RPC별 socket 정책을 유지한다.
