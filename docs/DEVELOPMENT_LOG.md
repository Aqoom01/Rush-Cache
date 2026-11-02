# 개발 진행 기록

## 2026-10-27 | IONIA O00 기능 구현

- 기준: `feat/ionia`, `4ed4dcb24857b1ce007140204fc932371c3c30d7`. 시작 시 tracked 변경 없음.
- G0: 실제 코드/API/기존 테스트와 Notion O00·Recovery v0.2 대조. JDK 21.0.11에서 기존 전체 build --rerun-tasks 통과.
- G1: 3·5노드 정족수, 동일 요청 dedup, 지연·실패·늦은 ACK와 동기 fallback fixture 추가.
- G2: 키별 Pending, Follower 선캡처, Meta DATA/INDEX, History/LTI, active follower lazy trim, View fencing 읽기 검증.
- G3: 별도 버전·길이·설정·요청 대응을 검사하는 TCP codec/transport와 독립 JVM 실행부 추가.
- G4: JDK 파일 기반 KV 어댑터, 안정 View 파일, reset+Consensus 전체 replay를 통한 상태 교체 구현. 디스크 재열기 및 파일 경계 테스트 추가.
- G5: VR View Change, highest-normal-view/최대 op-number, Skyros n1+n2 DAG, nonce/최고 View Leader 기반 재합류 구현.
- G6: 동기 fallback, 배경 Prepare batch, 키별 FIFO·비충돌 병렬 적용 및 연속 applied prefix, 유한 이력 linearizability checker, 실제 TCP 장애·재시작 테스트 구현.
- 공통 API/외부 의존성/Workload Generator/기존 단일 노드 서버 구현은 변경하지 않음. 정석은 replication/ionia에 유지하며 변형 디렉터리를 생성하지 않음.
- 기존 Fast 실패 테스트는 로컬 전송을 성공으로 세지 않는 원칙을 유지하면서 동기 fallback 응답을 제공하도록 보완. 기존 클라이언트 테스트 전송부에 View probe 추가.
- 내구성: O00 Replica RAM 로그 복제와 필수 View 메타데이터 영속화를 사용. D-PERSIST는 미활성. KV force 옵션을 Fast ACK 영속화로 표현하지 않음.
- 실행 안내/명세/테스트 범위: [IONIA_O00.md](IONIA_O00.md).
- 한계: 물리 SSD 속도·power loss·다중 머신 및 전체 상태 형식 검증 미실행. G7 비교 실험 미실행. 64 MiB 상태 전송 한도 및 전체 로그 replay 방식.
- commit/push/merge는 실행하지 않음.

## 최종 실행 검증

- 실제 저장소에서 ".\gradlew.bat build :cache-server:installDist --rerun-tasks --console=plain" 실행 성공.
- JDK 21.0.11, Windows. 169개 테스트: core 114 / server 39 / runner 16, 실패·오류·skip 모두 0.
- 독립 JVM 3개 강제 종료/재시작, TCP hot-key 32개 동시 쓰기, CLI SET/GET/DELETE, seed 0~63 이력 검사 통과.
- 원래 덮어쓴 13개 파일은 작업 채팅의 o00-baseline 폴더에 별도 보존. Git 기준 커밋도 유지.

## 2026-10-28 | V01 IONIA-LR 기능 구현·검증

- 기준 revision: 08d66c43c488445e6fc0616a895931e0606775c8. 정석 ionia 소스·테스트·서버·저장 어댑터는 보존.
- replication/ionia-second 아래 별도 source roots와 ionia_second package로 Leader 전용 읽기 클라이언트 추가. cache-core Gradle sourceSet만 확장하며 공통 API·외부 의존성·WorkloadGenerator는 유지.
- GET은 매 요청의 현재 Leader로 전송. Follower 데이터 GET·MetaQuery는 사용하지 않고 majority View 확인은 유지. SET/DELETE/ID/deadline/fallback 계약은 O00와 동일.
- 별도 V01 CLI·포트 17201~17203·build/ionia-second 데이터 디렉터리 제공. CLI는 총 10초 deadline 안에서 discovery 실패·Leader 전환을 재시도.
- 실제 저장소에서 JDK 21.0.11로 .\gradlew.bat build :cache-server:installDist --rerun-tasks --console=plain 성공.
- 전체 195 테스트 통과 (cache-core 138 / cache-server 41 / experiment-runner 16). V01 신규 26; 실패 0 / 오류 0 / skip 0.
- LR01~LR10 기능 fixture: 데이터 읽기 대상·probe 구분, pending apply, stale old Leader 차단, 늦은 응답, 3/5노드 복구·fallback·dedup, seed 0~63 이력 검사 및 잘못된 이력 대조군.
- 실제 TCP 32 hot-key 쓰기, 독립 JVM 3개 Leader 강제 종료 직후 V01 CLI 재탐색, 재시작·재합류·CLI CRUD 확인. 같은 서버의 O00 Follower+Meta 대조군도 확인.
- production jar에 V01 클라이언트 포함·테스트 클래스 미포함 확인. 명세·실행·한계: docs/IONIA_V01.md.
- 성능용 공통 데이터셋·비교 driver·G7 성능/SSD/다중 머신·power-loss 실험은 미진행. D-PERSIST는 미구현.
- Git 변경 작업(commit/push/merge)은 실행하지 않음. 기존 변경 파일은 작업 채팅 v01-baseline에 보존.
## 2026-10-28 | V02a History/LTI 기능 구현·검증

- 기준 revision: 99d25a3805081d84296d284a1ce8e9b00cbc3205. 작업 시작 시 tracked/untracked 변경 없음.
- replication/ionia-third 아래 별도 main/test roots와 ionia_third package에 정책·별도 엔진 구현. 기존 ionia·ionia-second 및 공통 API/외부 의존성은 보존.
- N=0 원본 prefix trim 대조, 양수 N의 안전 prefix 최소 정리·초과 허용, 1초 owner tick, View/generation을 구분한 불변 진단 구현.
- 정석 IoniaClient/Log/Recovery/Config/Message/DTO 및 기존 디스크·stable View·wire/TCP를 재사용. 엔진은 final/private 경계로 인해 분리한 사본이므로 이후 원본 수정 시 수동 대조 필요.
- 고정 길이 SHA-256 profile을 서버/클라이언트/stable configuration에 적용. 다른 N·O00 혼용 거절, profile 불일치 시 기존 KV 파일 열기 전 거절.
- 실제 저장소에서 Java 21.0.11로 전체 build와 installDist --rerun-tasks 성공. 257 테스트(core 195 / server 46 / runner 16), 실패·오류·skip 0. V02 신규 62(새 엔진 연결 회귀 32 포함).
- N=0 64×200 History 대조, 여러 N의 64×80 상태 불변식, N=0/1/2/MAX 각각 64×10 완료 CRUD 이력, pending/timeout 쓰기의 View 복구·같은 ID 재시도 및 오류 대조군 확인.
- 실제 TCP 32 동시 쓰기·1초 trim, 독립 JVM 3개 강제 종료/재시작/CLI CRUD, profile/디렉터리 불일치 거절 확인. 관측 GET의 단일 연결 거절은 총 5초·동일 ID의 한정 재시도로 시험 보완. 지속 엔진/응답 오류는 실패 처리.
- 보호 대상 146개 기존 파일 hash 동일, production jar 새 클래스 포함·test 클래스 제외 확인. 상세: docs/IONIA_V02A.md, docs/IONIA_V02A_VALIDATION.json.
- 포트 17301~17303, 예제 N=1024, 데이터 build/ionia-third/target-1024/node-*. 1024를 최적값으로 주장하지 않음.
- 공통 데이터셋·성능·실측 heap/SSD·다중 머신·power-loss·D-PERSIST는 미진행. commit/push/merge는 실행하지 않음.

## 2026-10-28 | V03 Prepare 배치 명세 v1.0·사전 검토

- 기준 revision: 9abfcb68cf493c74b7d7281155aa5f3336e6bc4d. V02a 검증 산출물 23개 hash 일치 확인. 원본·V01·V02a Java 코드는 변경하지 않음.
- IONIA/Skyros 배경 배치와 VR 저부하 대기를 현재 flush/matched/ACK/읽기/복구 코드에 대조하고 docs/IONIA_V03_DESIGN.md에 구현 기준·인수 항목 12개 고정.
- 개수·전체 encoded bytes·목표 대기, Follower별 한 개 확인 대기 구간, ACK 후 다음 chunk, 유실 재전송·동기 barrier·View/generation 경계를 명시. 이 전송 창은 O00와 다른 설계이므로 순수 batch 크기 효과로 단정하지 않음.
- 기존 코덱으로 320개 실제 Prepare frame의 바이트 계산·round trip 확인. 외부 길이 prefix 및 writeUTF의 modified UTF-8 포함. 큰 단일 요청은 목표 초과와 hard limit을 분리하고 wire/KV 수용 여부를 기록 ACK 전에 검사하도록 명세.
- 작업용 단일 Follower 정책 모형 6,912조합(발송 22,839회/전이 42,222회)·반례 3개 확인. authoritative 복구 prefix 일관성을 가정하며 quorum/Java 엔진/RPC/분산 정확성 증명 아님.
- 후속 소스 예정 위치 replication/ionia-fourth, package ionia_fourth, 포트 17401~17403 및 별도 데이터 경로 확정. 이번 단계에서는 생성하지 않음.
- V03 Java 구현·통합/장애 시험·성능 측정 미진행. 이전 257개 통과는 V02a까지의 기록이며 이번 전체 build를 재실행한 숫자가 아님.
- 노션 V03 명세·진행 상태 동기화. commit/push/merge는 실행하지 않음.

## 2026-10-28 | V03 구현 전 추가 문헌 검토·기능 대조군 설계 v1.1

- 이전 v1.0의 ‘고정’은 당시 계획 기록이다. 성능 설계 확정 표현을 철회하고 docs/IONIA_V03_DESIGN.md를 기능 대조군 v1.1로 수정; 근거·판본·접근 제한은 docs/IONIA_V03_RESEARCH.md에 기록.
- IONIA/Skyros/VR 관련 절 재대조. Tuning Paxos 저자 공개 선행 원고의 모델·정책·실험 관련 절, JPaxos §6/9, Adaptive Batching §1–4/6 검토. 2013 Optimizing Paxos는 출판사 서지/초록만 확인; 최종 원문 미확보를 상세 분석으로 올리지 않음.
- JPaxos Algorithm 2 PDF p.24를 렌더 확인. 원문 부등호와 본문 의미의 차이, 빈 batch/초과 항목 처리는 자체 due 명세로 구분.
- 실제 TCP는 메시지별 새 연결이며 원문 지속 연결과 다름. Wf=1은 논리 flight이며 논문 WND·physical socket 상한 아님. timer/retry20ms는 기능 fixture, 최적값 아님.
- 첫 시각 보존·slot/owner 대기·urgent barrier의 ACK 대기·TCP 역순·겹친 retry·공정 비교 조건 보완. 인수 계획 V03-13–18 추가; Java에서 아직 미실행.
- 작업용 작은 반례 시나리오 5개 확인. 실제 quorum/엔진/TCP 테스트·선형성 증명·벤치마크 결과 아님. 이전 320 frame/6,912모형/257회귀와 합산하지 않음.
- 기존 소스·테스트·설정·빌드 파일을 보존. V03 Java·ionia-fourth 디렉터리 생성·전체 build·성능/SSD/fsync 재현 미실행. commit/push/merge 하지 않음.


## 2026-10-28 | V03 Prepare 배치 제어 구현·기능 검증

- 기준 revision: 36920e82fcb55e69e418723a97bee1d381e83267. 시작 시 미커밋 변경 없음. 설계 v1.1의 기능 대조군을 replication/ionia-fourth, package ionia_fourth에 분리 구현.
- IoniaBatchSettings/PrepareSizer/BatchPlanner 및 O00에서 분리한 FourthEngine, 별도 Node/Main/CLI/포트 17401–17403/데이터 경로 추가. 기존 Client/Config/Log/History/Recovery/DTO/wire/TCP/파일 KV·stable View 재사용.
- E/B/D 필수 검증·SHA-256 profile, matched+1 연속 chunk·logical Wf1·20ms retry, ACK 기반 전진·동기 barrier·부분/누적 ACK·View/설치/send epoch fencing, 입력 ACK 전 wire/KV 크기 검증·단독 목표 초과 집계 구현.
- 최초 ordering·peer/index별 최초 전송·마지막 재전송을 구분. 기존 flight 중 urgent도 ACK/retry를 기다리며 D를 전체 완료 상한으로 주장하지 않음. 전송 시도/Future 통계는 NIC/socket 실측이 아님.
- 실제 Java 21.0.11 전체 build + cache-server:installDist --rerun-tasks 성공. 329 테스트(core 261/server 52/runner 16), 실패·오류·skip 0. V03 연결 72개(원본 회귀 32·기존 이력 검사·공통 worker 연결 포함).
- 기존 encoder 320 frame, 4 profile×64 seeds×10 완료 CRUD의 유한 이력·timeout completion 확장·오류 대조군, 3/5노드 배치 중간 crash·완료 쓰기 보존, 실제 TCP 32 hot-key, 독립 JVM 3개 crash/restart/CLI, E/B/D 혼용 거절 확인.
- 기존 보호 168개 파일 hash 동일. 공통 API/외부 의존성/기존 방식의 소스·테스트·설정 변경 없음. 기존 파일 변경은 core Gradle sourceSet과 개발 기록·V03 설계 현재 상태. production JAR 새 클래스 포함/test 제외 확인.
- 상세: docs/IONIA_V03.md, docs/IONIA_V03_VALIDATION.json. 문헌/설계 당시 기록은 보존하고 현재 진행 상태만 갱신.
- 공통 데이터셋·성능·실측 heap/SSD/NIC·다중 머신·power-loss·D-PERSIST 및 2013 TCS 최종판 상세 분석은 미완료. commit/push/merge 하지 않음.


## 2026-10-28 | V04a 고정 저장 배치의 key 적용 동시성 구현·검증

- 시작 HEAD 36920e82fcb55e69e418723a97bee1d381e83267, 미커밋 V03 변경 존재. v04-baseline 사본·hash로 보존하고 commit/push 없이 진행.
- 노션 V04의 초기 범위 V04a를 replication/ionia-fifth, package ionia_fifth에 분리. storage shard/파일/worker 수 S를 고정하고 outstanding write key 허가 K만 제어. 실제 worker 수 V04b는 미구현.
- IoniaApplySettings/ApplyGate와 O00에서 분리한 FifthEngine, 별도 Node/Main/CLI·포트 17501–17503·데이터 keys-4-shards-2 추가. Client/Log/History/Recovery/공통 DTO·wire/TCP·파일 KV/stable View 재사용, V01/V02a/V03 정책과 합치지 않음.
- 동일 key FIFO, ready key 공정 회전, 실제 저장 completion 뒤 허가 반환, timeout/cancel·close·KV 실패·old apply drain/reset/replay·generation fencing과 불변 진단 구현. K와 실제 thread/I/O concurrency 구분.
- K/S를 SHA-256 profile에 포함하고 다른 K/S의 기존 디렉터리·wire 혼용, 조립 실제 S 불일치를 KV 열기 전 거절. 원본 어댑터의 key hash→shard 매핑은 유지.
- IONIA §4.1, Early Scheduling arXiv v1 §4.3–4.4/Appendix A, resource 분석 §V-A–D·hybrid 저자본 §1/5 관련 부분 재대조. 원문 스케줄러·최종판 전체/성능 재현은 아님.
- 실제 Java 21.0.11 전체 build/installDist --rerun-tasks 성공. 392 테스트(core 318/server 58/runner 16), 실패·오류·skip 0. V04a 연결 63개(원본 회귀 32·기존 이력 검사·공통 worker 포함).
- K1/2/4/65536×64 seeds×10 CRUD 유한 이력·오류 대조군, 허가/순서/예외/취소/reset 경계, 실제 TCP K1/K4 각각 32 hot-key·S2 파일 배치, 독립 JVM 3개 crash/restart/rejoin/CLI·profile 보호 확인.
- 기존 보호 194개 파일과 직전 V03 산출물 22개(공유 Gradle/개발 기록 제외) hash 동일. 기존 파일 변경은 core Gradle sourceSet·개발 기록 추가뿐. production JAR 새 클래스 포함/test 제외 확인.
- 상세: docs/IONIA_V04A.md, docs/IONIA_V04A_VALIDATION.json. V04b·공통 성능 데이터/driver·성능/SSD·power-loss/D-PERSIST·원문 최종판 전체 재현은 미완료. commit/push/merge 미실행.


## 2026-10-28 | V04b 고정 파일 배치와 실제 I/O worker 분리

- 시작 HEAD 3998af206d7b2363f9d130b0c185b5f99b0c4d3e, 작업 트리 clean. commit/push/merge 미실행.
- replication/ionia-sixth(package ionia_sixth)에 Settings/main/test roots를 분리하고 server.ionia_sixth의 파일 어댑터·Node/Main/CLI·예제 17601–17603을 추가. 이전 모든 방식 보존.
- 원본 IoniaEngine/Client/Log/History/Recovery/DTO/wire/TCP/stable View 직접 재사용. final/private 파일 어댑터는 별도 사본으로 분리. 공통 API·의존성 변경 없음, V01/V02a/V03/V04a 정책을 누적하지 않음.
- S=storage.shards와 W=storage.io.workers 1..256을 독립 설정. key hash→S 파일 배치 고정, shard%W 고정 owner FIFO, W 실제 전용 스레드. W<S의 공유 worker 대기와 W>S의 유휴 thread를 명시.
- S/W·정책 SHA-256 profile/wire/stable View 보호, exclusive layout manifest로 잘못된 S·legacy/손상/extra shard 거절. Node의 W 변경 기존 디렉터리도 KV 열기 전 거절. 자동 migration/삭제 없음.
- reset이 W 아닌 모든 S 파일을 처리하며 old apply drain 후 reset/replay. immutable 비원자 작업 카운터 제공. diagnostics는 SSD/처리량 지표 아님.
- Java 21.0.11 최종 전체 build/installDist --rerun-tasks --no-parallel --max-workers=1 성공. 413개(core 321/server 76/runner 16), 실패·오류·skip 0; 신규 V04b 21개(core 3/server 18).
- 직전 병렬 전체 실행의 기존 V02a TCP GET 버전 32/0 실패 1회는 소스 변경 없이 분리 실행 및 최종 전체 직렬 실행 통과. 원인 미확정, 병렬 안정성 해결로 표시하지 않음. V04b는 해당 실행도 통과.
- S4/W1,2,4,8 실제 thread overlap·FIFO/동일 hash·cancel/BUSY/예외/close/reset, 4096 실제 파일 CRUD 참조 비교, 원본 엔진 old apply/View 복구, TCP W1/W4 각각32 hot-key, 독립 JVM3개 crash/restart/rejoin/CLI 확인.
- 기존 보호 213개 파일 hash 동일. 기존 파일 수정은 core Gradle sourceSet·개발 기록 추가뿐. production JAR 새 클래스 포함/test 제외·installDist 확인.
- 상세 docs/IONIA_V04B.md 및 IONIA_V04B_VALIDATION.json. 공통 성능 비교·최적W/SSD/NIC/multi-host/power-loss/D-PERSIST·V05/V06은 미완료. Replica RAM 로그·stable View·초기 layout force·선택적 KV force 구별.


## 2026-10-28 | V02a TCP 읽기 간헐 실패 재현·회귀 실행 사전진단

- 시작 HEAD3998af206d7b2363f9d130b0c185b5f99b0c4d3e 및 직전 미커밋 V04b 변경을 사본/hash로 보호. commit/push/merge 미실행.
- 읽기 status/RPC 관측을 추가했다. 분리30회 통과 후 전체+추가30회에서22번째 GET의 UNAVAILABLE/version0와 READ/META/probe BindException을 재현. OS 동적 포트16384개, TIME_WAIT19730연결/고유로컬포트15064개 관측. 추가 반복9회 실패를 일반 실패확률로 주장하지 않음.
- 이전 32/0 한 건은 당시 RPC 로그가 없어 소급 원인 확정 불가. 이번 동일 형태의 실패는 포트 할당 실패였으며 성공 값 손상으로 재현되지 않음. 엔진 NORMAL/failureReason 없음·각 replica32기록 확인.
- 기존 V02a TCP 테스트에 OK 상태·RPC/엔진 진단을 보강하고 새 오류 회귀3개 추가. 시험용 Windows 포트 사전진단/반복 실행 도구·증거 저장 추가. 첫 도구 검증의 전체1회 통과 후 다음 전체 실행에서 V01 관측용 GET ConnectException 실패1회를 별도 확인하고, 정상 View0 엔진만 같은ID·총5초 안에서 접속 거절 재시도하도록 관측 경계를 보완. OS/알고리즘/Client/Transport/wire/저장소/실험 profile 변경 없음.
- Java21.0.11 대상4개×3회, 기존 병렬 전체 build/installDist 각416개(core321/server79/runner16)×2회 통과. 각 실패·오류·skip0. 반복을 다른 테스트832개로 합산하지 않음. 두 번째 실행 전 포트 회복 대기 실제 확인.
- 시작 보호 226개 파일 hash 동일. 기존 파일 변경은 V02a TCP 진단·V01 관측 경계·개발 기록뿐. V04b 포함 이전 production 구현/문서 보존. 임시30회 진단 클래스는 hash 확인 뒤 제거.
- docs/IONIA_TCP_READ_AUDIT.md, IONIA_TCP_READ_VALIDATION.json, experiments/ionia-validation/README.md 참조. 메시지별 새 TCP 연결 제약/운영 포트 고갈 가능성은 남으며 성능·SSD·multi-host·power-loss 검증 아님. 다음 V05 문헌·안전 조건 검토는 후속.


## 2026-10-28 | V05a 문헌 근거·Follower 선택 상세 설계 v1.0

- 시작 HEAD3998af206d7b2363f9d130b0c185b5f99b0c4d3e 및 미커밋 V04b/TCP 감사 변경을 사본/hash로 보존. commit/push/merge 미실행.
- IONIA §4.2.1–4.2.2와 현재 readAtFollower/Meta/View 인증·실제 DTO를 대조. C3(P080) 공식16쪽 §3.1–3.3/4, Tars(P081) arXivv1 11쪽 §III/IV.A–B/VI 핵심 절을 검토하고 수식/Algorithm 원본 페이지를 렌더 확인. 전체 정독/성능 재현 완료로 표시하지 않음.
- V05a는 client raw READ RTT×(1+local outstanding)의 자체 가설로 축소. 서버 queue/service/현재lag/SSD IOPS는 미관측이며 V05b·C3/Tars 전체 재현과 구분. RR 대조군·TTL/탐색/cooldown·L/P·원본 인증·raw completion/취소/View/seq 경계를 명세.
- 원본 Client의 readAtFollower/Engine/Node/Transport/wire/저장소 재사용 계획. wrapper endpoint와 delegate를 함께 갱신, client policy hash는 서버 wire/stable View에 넣지 않음. 다음 코드 위치 ionia-seventh를 계획했지만 이번에 Java/Gradle/sourceSet은 생성/변경하지 않음.
- 유한 모형 36456개 단일 선택 스냅샷·탐색6조건·lifecycle 예시6개·설명용 잘못된 경우5개 확인. Java/RPC/분산 선형성/전체 interleaving 증명 아님. Java 인수 조건18개 및 I1–I4 구현 순서는 계획 상태.
- 기존 보호 241개 파일 hash 동일(개발 기록 추가 제외). Java/Gradle/기존 설정 변경 없음. 직전416개×2회 회귀 결과와 이번 모형을 합산하거나 재실행으로 표시하지 않음.
- docs/IONIA_V05.md, IONIA_V05_REVIEW.json, docs/models/v05_selector_model.py. Java V05 구현·공통 성능 비교·lag/서버 계측·V06·최적값/실측SSD/multi-host/power-loss/D-PERSIST는 미완료.
- 노션03-7 상세 설계와 문헌 P080/P081 추가. DB 전체84행=고유81편+DUP3, ID충돌0·핵심절12/서지초록69/전체상세분석·재현0 직접 확인. local admission은 정상 leader가 있어도 모든 후보 cooldown/full이면 BUSY이므로 동일 읽기 가용성 보장으로 표시하지 않음.


## 2026-10-28 | V05 Follower 선택 Java 구현·기능 검증

- 시작 HEAD74e2599582751c646c425815cf876a1b0a42c89d, 작업 트리 clean 확인. commit/push/merge 미실행.
- replication/ionia-seventh(package ionia_seventh)의 독립 main/test roots에 Settings/Selector/AdaptiveReadClient·private 관측 Transport 구현. server.ionia_seventh에는 client CLI만 추가. 원본 Client.readAtFollower/Engine/Node/Log/History/Recovery/wire/TCP/파일KV/ViewStore를 직접 재사용, 새 Engine/Node 없음.
- RR/ADAPTIVE·실제 raw RTT와 local outstanding·TTL/EWMA/탐색/cooldown·L/P·timeout/cancel/MetaDATA의 비대칭 completion·View/seq fencing·불변 진단 구현. 정책 hash는 client metadata이며 O00 wire/stable View와 분리.
- 제어 lock 안에서 비차단 dispatch만 진행, private delegate 결과를 deferred publication해 사용자 callback은 lock 밖에서 완료. 동일ID token 덮어쓰기와 raw 허가 조기 회수를 방지. 전체 socket/쓰기/Meta/probe/서버·RAM 상한으로 주장하지 않음.
- 새 CLI의 초기3회 조기 종료를 같은ID/10초 총deadline 발견·재시도로 보완. UNAVAILABLE/NOT_PRIMARY/BUSY를 한도 안에서만 재시도. 단발 CLI는 학습 성능 비교용 아님.
- Java21.0.11 최종 전체 build/installDist 성공. 457개(core358/server83/runner16), 실패·오류·skip0. V05 연결41개(core37/server4), 기존 checker/fixture 재사용 포함.
- 3/5×두 모드×64seeds×10CRUD=2560 유한 완료 이력/timeout completion 확장·잘못된 이력 대조군, 실제 엔진 stale value/MISS+나중trim Meta·pendingDATA·isolated oldleader, 실제TCP 두모드 각각32hotkey/S2 및80ms 전달 지연 회피/탐색, 독립JVM3개 leader/follower 각각crash/restart/rejoin·CLI 확인.
- 기존 보호 237개 파일 hash 동일. 기존 파일 수정은 core Gradle 새sourceSet·개발기록·V05현재상태뿐. 공통API/기존방식 소스·테스트·설정·의존성 보존. production JAR 새class포함/test제외 확인.
- 직전 전체 실행의 기존 V03 ProcessTest leader crash후 CLI GET TIMEOUT1회는 기존 코드 변경 없이 분리 재실행·최종 전체 재실행 통과. 원인 미확정으로 기록하며 V03 안정성 해결로 표시하지 않음. V05 자체 검사는 해당 실행도 통과.
- docs/IONIA_V05.md, IONIA_V05_VALIDATION.json, experiments/ionia-seventh/README.md. V05b 서버/실시간lag 계측·V06·공통 데이터셋/성능·최적값/SSD/멀티호스트/power-loss/D-PERSIST는 미완료. C3/Tars 전체 재현·전체 interleaving 증명 아님.


## 2026-10-29 | V06 MetaQuery 배치 전송 구현·기능 검증

- 시작 HEADc077217ff2625cdf7fc94d5d71afaf62f363d09b·clean. commit/push/merge 미실행.
- replication/ionia-eighth(package ionia_eighth)에 Settings/BatchTransport/Coalescer/Client wrapper, server.ionia_eighth에 codec/TCP·Node 조립부·Main/CLI,17801–17803 예제 분리.
- IONIA §4.2/§6 관련 절 대조. batching은 원문 future work, E/B/D/P/W·wire는 자체 설계. 원본 Client/Engine·Log/History/Recovery/wire/일반TCP/파일KV/ViewStore 직접 재사용. 공통API/의존성/이전방식 변경 없음.
- 개별 fresh Meta·occurrence token·View/leader FIFO·개별deadline·상한·raw terminal permit·cancel/timeout/close·전체 응답 검증·no cache. 동기 completion trampoline·callback lock 밖.
- wire64item/1MiB 입력·60MiB reply budget·초과 item UNAVAILABLE·별도 protocol/stable View ID와 client tuning hash 분리. RAM durability/stable View/선택적 KV force 구별.
- Java21 전체 build/installDist 498(core392/server90/runner16)·V06 연결41(core34/server7), 실패·오류·skip0. 기본병렬·포트 사전진단 유지. build/ionia-validation/20261029-091346-00a8dc25.
- 3/5×E1/E4×64seeds×10=2560 finite CRUD history·timeout/View retry·bad controls; actual TCP32hot writes/32GET→4Meta·혼합deadline·S2·wire/config; JVM3 leader/follower crash/restart/rejoin·CLI CRUD.
- 기존 보호262파일 hash동일(기존파일 수정은 core Gradle4줄·개발기록). JAR 새class포함/test제외·dependency 검사 통과.
- 첫 전체 V06 TCP UNAVAILABLE1회·후속 전체 통과·추가 반복은 docs/IONIA_V06.md/IONIA_V06_VALIDATION.json에 보존. 당시 RPC 부재로 소급 원인 확정 없음. 기존 V03 단발timeout도 해결 주장 없음.
- 현재범위 O00+파생7개 기능fixture 구현/검증. 공통데이터셋/장기client·동일조건 성능·최적값/SSD/NIC/multi-host/power-loss·V05서버lag 미완료. batch HOL/공동 실패·메시지별TCP 제약 유지.

## 2026-10-30 | 공통 벤치마크 시나리오·계측 명세 v1

- experiments/replication-benchmark-v1에 독립 문서·JSON 카탈로그·Draft-07 manifest/result schema·실행 불가 draft template·NOT_RUN 결과 형식 예시를 추가. 27개 시나리오/36개 지표군, 3/5노드, 동일 trace/paired seed, 정상·정확성·장애·장기·변형별 민감도 절차와 판정 정의.
- goodput/전체 completion, logical op/attempt/message, client 종단/예정 지연, 전수 histogram·표본수, phase/cohort/clock·ACK audit·복구 시각·null 사유를 구분. 요청수/시간/데이터 크기는 pilot 이전 후보값.
- 기존 단일 대상 Runner의 실패 포함 throughput·8192 reservoir·연결 이후 지연·warm-up seed 재생 등과의 차이 및 후속 B1–B8 구현 순서를 기록. 분산 driver/collector/checker의 새 계약 구현은 후속이며 Hermes/B01 비교도 별도 adapter 계약 필요.
- PowerShell7 로컬 명세 검사31개 통과. 합성 schema fixture는 .invalid 주소로 메모리에서만 검사; benchmark/correctness/Java 테스트 실행0. source preservation 결과는 신규 디렉터리 source-preservation.json 참조.
- 사용자 요청에 따라 EC2 접속·사전진단·원격 실행은 보류. 실제 성능/SSD/멀티호스트 수치 없음. 기존 생산 코드·공통 API·테스트·설정·Gradle 변경 없음. commit/push/merge 미실행.


## 2026-10-30 | 공통 벤치마크 단계1 — 논리 데이터·요청열 생성기

- experiment-runner의 별도 benchmark/benchmarkTest sourceSet에 논리 trace 생성기를 추가. 기존 Command/Bytes 재사용, 기존 WorkloadGenerator/Main/공통 API/알고리즘/테스트/의존성 보존. Gradle은 새 sourceSet·검사·생성 task 연결만 추가하며 application JAR에 benchmark 클래스가 들어가지 않음 확인.
- LOAD/WARMUP/MEASURE 별도 namespace, indexed PRNG·UNIFORM/FIXED_HOT, 전역 index 재생성·고정 stream 분할·runtime request ID 계약, injective key/seed+client+sequence+key+CRC32C value, canonical binary SHA256/hash-only/export 구현. 확률적 mix이며 작은 trace exact quota 아님. Zipf/moving-hot과 실제 attempt/outcome은 후속.
- Java21.0.11 최종 전체 build/installDist 529개(core392/server90/기존runner16/새benchmark31) 통과, 실패·오류·skip0. 로컬 포트 사전진단·기존 병렬/인수 조건 유지. 최종 증거 build/ionia-validation/20261030-121153-013f5350.
- 최초 전체 회귀는 기존 V07 IoniaMetaBatchTcpTest에서 UNAVAILABLE/leader PROBE·BATCH Connection refused1건. 분리2검사와 최종 전체는 통과. 기존 코드·timeout 변경 없이 첫 실패 XML/log를 보존하며 원인/안정성 해결은 주장하지 않음. 원본 증거 build/ionia-validation/20261030-120748-ba60ddda, 분리 build/benchmark-b1-target-df50cbc5. 즉시 분리 시도는 포트 예산 초과로 실행되지 않았고, 뒤의 정상 포트 조회·gate 이후 실행함.
- 100만 key round-trip·stream 전역 순번 중복/누락·동시 재생성·비율·초기 적재·phase ID 분리·손상/설정 오류·overwrite 방지·binary/버전 hash/IO 실패 검증. key64/워밍업256/측정10000 작은 생성 예제를 -Xmx64m에서 hash-only/파일 두 번 실행해 SHA256/byte/count 일치. Gradle CLI도 같은 hash, 실행시 Task.project 경고 제거 후 확인.
- 기존 src214개 SHA256 동일, 새 Java13파일. experiments/replication-benchmark-v1/TRACE_GENERATOR.md, trace-small.properties, B1_VALIDATION.json, B1_SOURCE_PRESERVATION.json 참조. 원래31개 명세 형식 검사와 새31개 Java 생성기 검사는 별개임.
- 새 생성기를 통한 서버 부하 전송·공통 성능 측정·EC2 접속/사전진단·commit/push/merge 미실행. 기존 회귀에는 로컬 TCP/JVM 기능 검사가 포함됨. 단계1(logical generator) 완료이며 전체 B1의 attempt/outcome 및 B2 이후 분산 driver·계측·checker 완료 아님. 다음은 O00 장기 client adapter와 실제 결과 계수.


## 2026-10-30 | 공통 벤치마크 단계2 — O00 장기 client·실제 결과 집계

- experiment-runner benchmark 영역에 BenchmarkClient/ClientOutcome/ClientAccounting, cache-server 별도 benchmark 영역에 O00BenchmarkClient/ObservedRpc/O00TcpClients와 검사 추가. 원본 IoniaClient/인증/fast ACK/DELETE executed 경로/TCP/파일KV 그대로 재사용. common benchmark JAR만 server 실험 영역에 연결하며 기존 production 의존성·API·wire·application JAR 보존.
- stream/phase별 장기 client/View, runtime ID와 별도 wire ClientId base/OperationId 매핑, 같은 command·ID·총deadline의 bounded retry 구현. 초기 발견/재발견과 실제 client execute attempt·연산별 성공/오류/미응답·재시도 logical 수를 분리. 수명 전체 계수이며 phase별 성능 cohort/export는 후속.
- 전체 deadline은 submit 진입부터 발견·lock/client 준비·retry·publication 대기까지 포함. 관찰/전달 시각 구분·늦은 전달의 성공 금지·취소/close·외부 future copy 보호·중복/역순/overflow 거절. 완료ID 전수보관 없이 client별 high-water와 bounded active/publication/raw-stage 상태 유지.
- raw RPC permit은 실제 delegate stage terminal 때만 반환. logical 취소/timeout/close로 원격 abort·socket 종료·전체 RAM 상한을 주장하지 않음. 기존 RPC별 새 socket 정책을 유지하며 이번 장기 client가 TCP pooling 구현이라는 뜻 아님. wireClientBase의 run/slot 간 배타 예약은 후속 coordinator 책임.
- Java21.0.11 전체 build/installDist562개(core392/server90/기존runner16/runner benchmark33/server benchmark31)·추가33개 통과, 실패·오류·skip0. 증거 build/ionia-validation/20261030-124619-249372b2. 새 server TCP fixture만 기존 회귀 뒤에 배치하고 기존 회귀 병렬/timeout/포트 gate 유지.
- 제어 응답에서 View 변경 retry·영구/일시 오류·발견/실행 deadline·late completion·close·backpressure·publication 지연 음성 대조·cancel/response16회 유한 경합. 로컬TCP3/5노드 각각29logical·장기client6개·생성 적재/쓰기·인증 follower GET 값 대조·DELETE OK/MISS·재삽입. 무응답loopback에서 raw socket timeout5초보다 짧은 logical300ms deadline 관찰. 성능/정시 보장·전체 선형성 증명 아님.
- 기존 src227파일 SHA256 동일, 새 Java11파일. 기존 Runner/Server application JAR의 benchmark class0 확인. 과거 B1 V07 TCP 간헐 Connection refused는 해결 주장 없이 보존하며 이번 전체 회귀에서는 실패 없음.
- experiments/replication-benchmark-v1/ADAPTER_O00.md, B2_VALIDATION.json, B2_SOURCE_PRESERVATION.json 및 README/구현차이 문서 반영. 실제 EC2·공통 성능·commit/push/merge 미실행, template draft/disabled 유지. 다음은 phase coordinator·apply/drain·부하 scheduler·histogram/result export. 변형/Hermes adapter와 분산 checker/장애 controller는 후속.


## 2026-10-30 | 공통 벤치마크 단계3-A — fixed-N closed-loop·phase 전환

- 기존 benchmark 영역에 ClosedLoopPlan/Runner/Result·DrainProbe와 O00 LocalO00DrainProbe 추가. 고정 logical stream S와 concurrency C를 분리하고 worker별 한 요청의 결과를 기다리는 streaming closed-loop 구현. 기존 Java238파일·Gradle·공통 API·production 의존성·원본/변형 구현 보존, 새 Java8파일.
- PREFLIGHT→LOAD→LOAD_DRAIN→WARMUP→WARMUP_DRAIN→MEASURE→FINAL_DRAIN→CLEANUP. phase/GET·SET·DELETE별 scheduled/성공/오류/미응답/미수락/unresolved·시도/재시도 집계와 backend lifetime delta 대조. 임시 admission 거절만 같은 요청으로 bounded 재제출하며, 이미 수락된 미응답/오류를 새 요청으로 반복하지 않음.
- fixed-N 최소시간 미달은 요청/대기 추가 없이 실패. 잘못된 ID/결과·불일치 ledger·작업/완료/적용 한도·caller interrupt·종료 미완료 처리와 알려지지 않은 결과 보존. work budget과 cleanup grace를 구분하고 비협력 backend를 realtime 강제 종료하는 것으로 주장하지 않음.
- 로컬 probe는 기존 공개 engine progress/view/failure와 client raw/publication 상태만 읽는다. 빈 격리 그룹·고정 정상 View에서 모든 replica의 recorded/ordered/commit/apply=성공 unique SET+DELETE 누계 및 client 미완료0을 요구. 예상 초과 쓰기/replica누락/View변경은 실패. 원격 View-only probe를 remote apply 증거로 재해석하지 않으며 fsync/전원손실 보장 변경 없음.
- Java21.0.11 전체 build/installDist585개(core392/server90/기존runner16/runner benchmark47/server benchmark40), 추가23개(coordinator14/probe5/TCP4), 실패·오류·skip0. 증거 build/ionia-validation/20261030-132639-bd2f843e. 기본 병렬·기존 timeout·로컬 포트 gate 유지.
- 실제 로컬TCP3/5노드 × 읽기100%/GET80·SET15·DELETE5, streams4/concurrency2, LOAD8/WARMUP7/MEASURE19 각각34logical로 단계·cohort 분리. 모든 replica 적용 frontier는 읽기8/혼합15. 읽기 전용은 독립 audit client로8개 초기 값도 확인. 작은 기능 fixture이며 성능 비교·혼합 이력 선형성 증명 아님.
- 기존 src238개 SHA256 동일·새8개. 기존 Server/Runner application JAR에 benchmark class0. sourceSet 자동 포함으로 이번 Gradle 수정 없음. 과거 V07 간헐 연결 거부는 해결 주장 없이 기록 유지하며 이번 전체 회귀 실패 없음.
- experiments/replication-benchmark-v1/CLOSED_LOOP.md, B3_VALIDATION.json, B3_SOURCE_PRESERVATION.json·README/차이/이전 단계 후속 설명 갱신. Java API/로컬 fixture 제공이며 EC2 launcher·open-loop·histogram·표준 result export·변형 adapter·분산 checker/원격 관찰은 후속. EC2 접속/사전진단·공통 성능 비교·commit/push/merge 미실행. template draft/disabled 유지.


## 2026-10-30 | 전수 지연 histogram·로컬 표준 결과 저장

- 선행 open-loop 구현/추가22·전체607검사의 증거는 experiments/replication-benchmark-v1/B4_VALIDATION.json과 OPEN_LOOP.md를 따른다. 이번에는 기존 실행기에 전수 latency와 결과 저장을 연결했다.
- benchmark measurement에 LatencyHistogram/LatencyCollector/BenchmarkRunWriter/JDK JSON 인코더4개와 검사 보조/단위/TCP5개, 새Java9개를 추가. 기존 src253개 중252개 hash 동일; ClosedLoopRunner1개에 선택적 outcome Observer와 호환 기본 생성자만 추가했다. 원본·파생 알고리즘·공통 API·Gradle·production 의존성·wire·TCP는 변경하지 않았다.
- phase/op/outcome/reason별 전수 service/scheduled/dispatch-lag histogram, ns 1% bin·정확 sum/min/max/mean·nearest-rank upper bound·overflow/null·p99.9 n100000 정책을 구현했다. timeout은 censored 관찰로 성공과 분리하며 logical latency와 재시도 attempts를 구분한다.
- 종료 후 local manifest/환경/execution/전수 histogram/counters/events/표준 result/integrity/완료 marker를 새 디렉터리에 저장한다. 계수·필수 지표·ID/clock 불일치는 INVALID, 저장 오류·기존 경로 충돌은 예외이며 덮어쓰지 않는다. schema·hash·histogram 검사 Validate-Run.ps1 제공. 완전 저장된 INVALID 증거와 성능/서비스 합격은 구분한다.
- Java21.0.11 전체 `.\experiments\ionia-validation\Run-Validation.ps1 -Mode Full -Repeat 1 -PortWaitSeconds 60` build/installDist635개(core392/server90/기존runner16/runner benchmark89/server benchmark48), 추가28개, 실패·오류·skip0. 증거 build/ionia-validation/20261030-144730-ff8dc3df. 기본 병렬/기존 timeout/포트 gate 유지. 기존 application JAR의 benchmark class0.
- 실제 같은 JVM 로컬TCP3/5노드 × closed/open-loop에서 LOAD4/WARMUP3/MEASURE30·mix40/30/30의 모든 CRUD population과 replica 적용22 확인. open rates20/20/50·상한8, closed 동시성2/streams4. 작은 기능 fixture이며 성능 비교·처리 한계·전체 선형성 증명이 아니다.
- 산출물13개(로컬 subset valid9/의도적 invalid4)의 표준 JSON schema·hash·population/percentile/계수를 Python/jsonschema와 PowerShell 검증기로 대조했다. 파일 변조·COMPLETE 누락·hash를 다시 맞춘 잘못된 histogram count/percentile4개 음성 대조 모두 거부. 이17개 artifact 검사는 JUnit635개와 별도다.
- 설명/증거: experiments/replication-benchmark-v1/LATENCY_EXPORT.md, MEASUREMENT_VALIDATION.json, MEASUREMENT_SOURCE_PRESERVATION.json, MEASUREMENT_ARTIFACT_CHECK.json. 기존 README/계측/차이/closed/open 후속 설명과 Notion 시나리오·계측·보고용 요약의 진척을 갱신했다.
- 한계: 단일 JVM Java API의 로컬 결과 export이며 ready 분산 manifest는 아니다. trace/source tree hash·서버환경은 null. 1초 창·자원/프로토콜 지표·완전 history/ACK audit·remote/분산 launcher·변형 adapter·observer overhead pilot/paired 반복 분석은 후속이다. 과거 V07 간헐 연결 거부는 해결 주장 없이 유지한다.
- 다음 작업: V01–V07 adapter 연결과 공통 정확성/자원 관찰 검증. EC2 접속·사전진단·정식 성능 실측·commit/push/merge는 하지 않았으며 template draft/execution_enabled=false를 유지한다. 독립 replica RAM log·stable View/선택적 KV force 구분을 보존한다.

## 2026-10-30 | V01 Leader-read 공통 benchmark adapter

- 기존 IoniaLeaderReadClient를 server benchmark/ionia_second의 V01BenchmarkClient/V01TcpClients/LocalV01DrainProbe로 연결했다. 원본/파생 알고리즘·공통 core API·Gradle·production 의존성·wire/TCP는 보존했다. 기존 src262개 중261개 hash 동일, 실험 O00BenchmarkClient에 선택적 IoniaBenchmarkSession.Factory만 추가해 공통 수명주기를 공유했고 새Java7파일을 추가했다. 기본 생성자의 기존 IoniaClient/Options는 유지한다.
- 장기 세션·동일 ID/command·총deadline·View 재탐색/retry·logical/raw 상한·취소/late completion/publication/close를 재사용한다. V01의 데이터 GET은 실제 client의 현재 Leader를 따르며 View probe는 유지한다. 원본 readReplica를 최초 Leader로 고정하는 대체 구현이 아니다. LocalV01DrainProbe는 기존 O00 엔진 공개 progress를 재사용한다.
- 추가26검사(단위18/TCP8), 최종 전체661개(core392/server90/기존runner16/runner benchmark89/server benchmark74), 실패·오류·skip0. Java21.0.11에서 기존 helper로 build/installDist를 실행하고 포트 gate·기존 병렬/client timeout·부하/합격 조건을 유지했다. 최종 경로는 V01_ADAPTER_VALIDATION.json을 따른다.
- 실제 로컬TCP3/5노드 × closed/open-loop × GET100/GET40·SET30·DELETE30의8조건에서 각 LOAD4/WARMUP3/MEASURE30, replica 적용4/22를 확인했다. streams4, open rates20/20/50·상한8, closed 동시성2. V01 READ/META0·probe 존재 및 별도 O00 READ/META1 대조, 별도7logical CRUD 값·DELETE OK/MISS·재삽입을 확인했다. 보조 감사/대조는37요청 benchmark cohort와 다른 client/ID다. V01 export8개를 표준 schema·hash·계수/histogram/percentile로 검증했다.
- 초기 새 테스트의 Bytes API 오타를 수정했다. 첫60초 전체 gate는 회복 대기 초과로 테스트 미실행했다. 이후 첫 전체 회귀는 새 V01 TCP7조건 실패를 보존했고, 직후 OS 관찰에서 동적 포트 약15107/16384·TIME_WAIT21107을 확인했다. 개별 socket 예외 원문이 없어 소급 원인을 확정하지 않는다.
- 새 V01 TCP 그룹 BeforeAll에만 experiments/ionia-validation/Wait-LocalTcpBudget.ps1의10% 회복 gate를 연결했다. 구형 PowerShell 출력 파싱/인코딩으로 초기화 실패한 시도도 보존하고 PowerShell7/UTF-8로 통일했다. 30초 보조 확인은 미회복 상태를 정상 거부했다. 최종 그룹 gate는 최대600초 환경 회복을 준비 단계에서 기다리며 실패하면 skip/PASS하지 않는다. 개별 테스트 본문은 기존60초, client/작업 budget과 실행 부하는 변경하지 않았다. 원본 TCP 연결 정책이나 과거 V07 간헐 실패를 해결한 변경은 아니다.
- 원래 알고리즘 기능/장애 테스트와 공통 adapter 연결을 구분한다. docs/IONIA_V01.md의 최초195개 기록은 보존한다. 실제 코드/사용법·증거: experiments/replication-benchmark-v1/ADAPTER_V01.md, V01_ADAPTER_VALIDATION.json, V01_ADAPTER_SOURCE_PRESERVATION.json, V01_ADAPTER_ARTIFACT_CHECK.json. 기존 application JAR에 benchmark class0 확인.
- 현재 단일 JVM/로컬 결과 export이며 adapter Settings/socketTimeout/membership 전체의 자동 manifest 캡처·원격 progress·전체 history/ACK audit·자원/프로토콜 계측·1초 창·clock/observer overhead/pilot은 후속이다. 소규모 기능 검사를 실제 성능 비교/SSD/다중 머신 검증으로 보고하지 않는다.
- 다음은 V02 adapter와 순차적인 V03–V07 연결, 공통 정확성·자원 관찰 검증이다. EC2 접속/진단·정식 성능 비교·commit/push/merge는 하지 않았고 template draft/execution_enabled=false 및 독립 replica RAM log·stable View/선택적 KV force 구분을 유지했다. Notion V01/시나리오/보고용 요약을 실제 완료 범위로 갱신했다.

## 2026-10-30 | V02–V07 공통 benchmark 연결·로컬 검증

- 남은6개 adapter를 cache-server의 별도 benchmark/ionia_third–ionia_eighth에 연결했다. V02 History·V03 Prepare·V04 적용 key K·V05 실제 worker W는 기존 typed 정책 서버와 원본 client, V06/V07은 실제 AdaptiveRead/MetaBatch client를 사용한다. 이전 방식을 누적하지 않고 기존 알고리즘 디렉터리·core API·wire·production 의존성·기존 Java 테스트는 보존한다.
- 공통 O00 runtime의 ID/deadline/retry·admission/관찰/취소/close, trace·두 실행기·전수 histogram/result writer를 재사용한다. LocalVariantDrainProbe는 client 전체 membership·고정 정상 View·정확 apply frontier와 선택한 내부/raw 대기를 확인한다. V07 raw batch frame은 별도 cap/실제 terminal 기준으로 관찰해 timeout/close 뒤 미완료를 유지한다. 일반 RPC cap과 batch cap은 각각 runtime.maxOutstandingRpc이며 합산 단일 cap이 아니다.
- 기존269src 중268개 SHA256 동일, 기존 Java는 BenchmarkRunWriter.Metadata의 선택적 불변 adapterConfiguration1개만 수정·새Java20개. 기존7인자 생성자를 유지하고 새8인자로 정책/profile·초기 membership·client 제한/socket timeout을 저장한다. server Gradle은 새 variants 검사만 variantBenchmarkTest task로 원래 benchmark 뒤에 배치하며 check/build에 포함한다. 기존 server benchmark74개/병렬/timeout·부하/판정은 유지했다.
- Java21.0.11에서 `.\experiments\ionia-validation\Run-Validation.ps1 -Mode Full -Repeat 1 -PortWaitSeconds 600`으로 최종 build/installDist760검사(core392/server90/기존runner16/runner benchmark89/server benchmark74/새variants99), 실패·오류·skip0. 전체 증거 build/ionia-validation/20261030-165133-f3d7d545. 새 두 TCP 그룹도 기존10% Windows 포트 gate를 적용했고 각각24샘플 후 통과했다. 원본 TCP 연결 정책을 바꾸거나 과거 간헐 실패를 해결한 것으로 표시하지 않는다. application JAR의 benchmark class0.
- 추가99개 구성: 모델44, 실제 TCP matrix48, 잘못된 wire/profile 거절5, 실제 V07 batch burst2. 각 방식3/5노드 × closed/open-loop × GET100/GET40·SET30·DELETE30, LOAD4/WARMUP3/MEASURE12·streams4, open rates20/20/50·상한8 또는 closed 동시성2. 매 run19logical의 결과/복제 적용·histogram을 검증했다. 이전37요청 fixture와 성능을 비교하지 않는다.
- 각 방식 한 조건의 별도 namespace에서 CRUD 값·DELETE OK/MISS·재삽입7logical씩 총42개를 감사했다. 이 요청은 본 run cohort에 합치지 않는다. TCP3/5노드 V07에서는 logical4/fresh Meta4→raw frame1을 확인했다. client별 coalescer이므로 모든 부하에서 E개 batch가 채워질 것을 주장하지 않는다.
- 결과48개를 Python/jsonschema와 PowerShell Validate-Run으로 표준 envelope·SHA256·완료 marker·cohort·histogram/percentile 및 adapter 설정 필드에 대조했다. 각 방식8개 조합 모두 확인했다. 첫 모델에서 delay1초와 총deadline1초가 같아 원래 긴급 flush로 frame4개가 나왔던1실패는 build/remaining-adapter-initial-model에 보존했다. fixture delay만100ms로 고쳐4→1 인수 조건을 유지했고 알고리즘을 수정하지 않았다.
- 사용법/한계·증거: experiments/replication-benchmark-v1/ADAPTERS_V02_V07.md, REMAINING_ADAPTERS_VALIDATION.json, REMAINING_ADAPTERS_SOURCE_PRESERVATION.json, REMAINING_ADAPTERS_ARTIFACT_CHECK.json. README/구현 차이/지연 export·V01 후속과6개 기존 알고리즘 문서, Notion6개 방식·허브/시나리오/보고용 요약/계측 진척을 실제 완료 범위로 갱신했다.
- 현재 client ID와 세션은 phase별이므로 V06 WARMUP RTT 학습이 MEASURE 새 client로 자동 이전되지 않는다. JIT/서버 cache 워밍업과 구분해 cold/steady 학습/워밍업 정책을 성능 비교 전에 확정해야 한다. V07 queue도 phase/stream client별이다. local metadata는 전체 서버 inventory/ready manifest가 아니며 O00/V01의 기존 metadata 호출도 추가 보완해야 한다.
- 다음은 공통 정확성/full history/ACK audit·필수 자원/통신/저장소·1초 창/내부 지표·clock/overhead/remote 관찰 및 학습 워밍업 정책 보강이다. 이후 본시험 데이터/요청열 후보를 만들고 EC2 재개 시 inventory/pilot으로 규모·기간·rate를 확정한다. EC2 접속/진단·정식 성능 비교·commit/push/merge는 하지 않았다. template draft/execution_enabled=false, 독립 replica RAM log·stable View/선택적 KV force 구분을 유지했다.


## 2026-11-02 | 공통 correctness 이력·예약 ACK 감사

- 10월30일 중단 당시 새 로컬 TCP48검사 통과 이후, 미반영 writer 검증 보강/음성 대조와 독립 결과 검증기를 반영하고 전체 검증·진척 정리를 마쳤다. 새 benchmark 파일만으로 이력 수집 decorator·제한된 전역 GET/SET/DELETE 선형성 검사·예약 ACK 원장/감사·별도 correctness writer를 구현했다. 기존 보호 src289개 SHA256 모두 동일·새Java12개이며 core API/알고리즘/기존 실행기와 writer·wire/생산 의존성은 보존했다.
- run/slot/단일 monotonic clock과 모든 submit/callback·실제 Bytes·결과/attempts·pending/거절/미응답을 기록한다. 수집 용량 초과/ID·clock 오류·불확실 결과·checker 한도를 PASS로 바꾸지 않는다. 명시 초기 상태에서 최대24operation의 제한된 탐색이며 본시험 천만 요청 전수를 처리했다고 주장하지 않는다. full payload correctness mode의 성능 overhead는 미측정이다.
- ACK 감사는 별도 배타적·덮어쓰기 없는 예약 키에 대해 SET 값과 DELETE 부재를 확인한다. namespace/초기 상태 attestation과 READY 이후 실제 GET을 요구한다. 모든 replica 적용 후 follower1개 정상 close/재기동·생존 다수 복구 뒤 서비스 상태를 재검사했다. ACK 직후 apply 전 crash·leader kill·SIGKILL·전원 손실·특정 replica 직접 읽기 보장은 이번 검증 범위가 아니다.
- 새 단위32개, TCP48개 추가. Java21.0.11 전체 build/installDist840검사(core392/server90/기존runner16/runner benchmark121/server benchmark74/variant99/correctness48), 실패·오류·skip0. 기존 검사·병렬/timeout·10% 포트 gate는 유지하고 새 correctness task만 variant 뒤에 분리했다. 최종 증거: build/ionia-validation/20261102-091004-35e8466c.
- 실제 TCP32조건은 전체8방식×3/5노드×closed/open-loop, 각 LOAD4/WARMUP3/MEASURE12·혼합40/30/30·19logical을 정확한 Bytes/전역 순서로 검사했다. 별도 ACK16조건은 key2개씩, 적용 후/재합류 후 각각 감사했다. 작은 기능시험이며 성능 비교·대규모/분산 correctness 완료가 아니다.
- 최종 TCP 산출물64개·의미 대조4개(정상PASS/오래된읽기VIOLATION/pendingINCONCLUSIVE/ACK손실VIOLATION)를 독립 Python 검증기로 marker/hash·payload·ID/clock·계수·성공 witness replay·ACK 증거에 대조했다. 완료 marker 누락·파일 변조·hash를 다시 맞춘 잘못된 payload/witness·거짓 pending PASS/숨긴 ACK손실 등 음성 대조6개를 모두 거부했다. 이74개 artifact 검사는 JUnit840개와 별도다.
- 첫 새 TCP16실패는 복구 중 RECOVERING을 정상 상태용 probe로 즉시 평가한 fixture 순서 오류였다. build/correctness-initial-recovery-gate에 XML을 보존하고, bounded 복구 준비 gate를 추가한 뒤 기존 정상 READY 판정을 그대로 적용했다. 원본/파생 알고리즘은 변경하지 않았다. 과거 V01/V07 간헐 TCP 실패도 이번 변경으로 해결했다고 주장하지 않는다.
- correctness 전용 history/ledger/audit/report·integrity/완료 marker를 새 디렉터리에 저장한다. COMPLETE는 저장 완료이며 PASS가 아니다. raw payload는 로컬에만 보존한다. 기존 성능 result의 M28/M29 not_evaluated는 유지하며 별도 token/run/clock/hash 증거를 제공한다. application JAR의 benchmark class0 및 production distribution의 benchmark JAR0을 확인했다.
- 설명/증거: experiments/replication-benchmark-v1/CORRECTNESS_HISTORY.md, CORRECTNESS_VALIDATION.json, CORRECTNESS_SOURCE_PRESERVATION.json, CORRECTNESS_ARTIFACT_CHECK.json, validate_correctness.py. README/구현 차이/계측과 Notion 시나리오·보고용 요약·계측 진척을 실제 로컬 범위로 갱신했다.
- 다음은 필수 host/JVM·통신/저장소·1초 창/내부 지표, clock/observer overhead·remote 관찰 및 V06 cold/steady 학습·워밍업 정책 보강이다. 대규모/분산 correctness와 표준 성능 result M28/M29 통합도 남아 있다. 이후 본시험 데이터/요청열 후보·사용자 EC2 재개 시 inventory/pilot으로 진행한다. EC2 접속/진단·정식 성능 비교·commit/push/merge는 하지 않았다. template draft/disabled, 독립 replica RAM log·stable View/선택적 KV force 구분을 유지했다.

## 2026-11-02 | Jira 후속 40 SP 등록·설계 계약 완료

- 사용자 요청으로 S15P31D111 보드16646의 기존 A2 에픽 S15P31D111-20과 Sprint4(11/2~6)를 확인했다. 기존 완료6스토리를 유지하고 후속9스토리 S15P31D111-163~171을 총40SP(3/5/5/8/5/5/3/3/3), 담당자 박승진·레이블 ionia-benchmark-20261102로 등록했다. 목적·완료 조건·선행 실제 이슈 키·검증/원본 보호·EC2/Git 제외 범위를 각 설명에 기록했다.
- 첫 설계 스토리163은 진행 중→완료로 갱신했다. Notion 계측 명세(4094ed902437816ca22aed3febe6eb5a) 12번에 22개 지표의 catalog name/unit·출처·부분/미지원 경계·clock/phase/window·counter delta·중복 집계·null/coverage 판정·schema 호환/sidecar·후속 의존성을 정리하고 완료 조건별 근거를 Jira 댓글에 남겼다.
- 검증: 9개 실제 키/총40SP·순환 없는 의존성·22개 정확한 지표 범위·기존 catalog 이름/단위와 result 상태 enum/version 일치의 설계 일관성 검사가 통과했다. 이 완료는 Jira/Notion 설계 산출물에 한정하며 collector 구현이나 기존840검사의 신규 SP 재계상이 아니다. Java/production 코드·스키마 파일은 변경하지 않았다.
- 다음164(JVM/GC/1초 창,5SP)는 진행 중으로 전환했다. 두 실행기의 기존 outcome observer·LatencyCollector·writer 환경 snapshot과 로컬 Java21 MXBean API를 읽어 phase 경계 lifecycle/snapshot 보완 필요성을 기록했다. 구현·새 테스트·자원 실측은 미완료다. 최종 신규 묶음은 완료3SP/진행5SP/대기32SP이며 Sprint 화면으로 대조했다.
- 작업 중 상위 에픽의 하위 행에서 상태를 바꾸는 동작이 대상 모호성 때문에 자동 검토로 거절됐다. 에픽 상태는 변경하지 않고 각 실제 스토리 상세 URL·제목·키를 확인한 뒤 해당 스토리만 전환했다.
- 작업 mirror의 증거: JIRA_PLAN_20261102.json, IONIA_OBSERVATION_CONTRACT_20261102.md/.json, JIRA_P01_CONTRACT_VALIDATION.json, JIRA_P02_START.md, JIRA_40SP_STATUS_20261102.png. 이후 구현은 실제 착수/검증 근거로 Jira를 갱신하고 필요한 MR/병합 확인 전 완료로 올리지 않는다. commit/push/merge는 사용자 수행이며 EC2/정식 성능/원격 장애/Hermes 구현은 이번40SP 범위에서 제외했다.


## 2026-11-02 | P02 JVM/GC·1초 관찰 구현·로컬 검증

- Jira S15P31D111-164(5SP)의 현재 JVM process CPU·heap/non-heap·GC 누계 source, bounded 자원/요청 창 collector, 독립7파일 writer·Python reader를 benchmark sourceSet에 추가했다. 기존 closed/open 실행기2개는 선택적 lifecycle overload와 stage/finish callback만 추가하고 기존 생성자를 유지했다. 원본/파생 알고리즘·core API·wire/TCP·RAM replica log·stable View/선택적 KV force 의미를 변경하지 않았다.
- 실제 collection/source start/end와 phase 요청/관찰 경계를 저장한다. 1초는 목표 주기이며 실제 interval을 보존한다. 요청은 run anchor 기준 terminal timestamp 창과 LOAD/WARMUP/MEASURE cohort를 분리해 원래 실행 계수와 대조한다. 같은 JVM은 process_instance_id 하나로 관측하며 replica별 CPU/heap을 합산하지 않는다. RSS/allocation/개별 GC pause는 null+reason, GC시간은 누계, pending peak는 관측 endpoint 하한이다. 표준 M18/M23과 기존 성능 bundle은 전체 coverage/clock/분모 통합 전 unsupported 계약을 유지한다.
- 정상/미지원·reset·clock/identity·bounded cap·late callback·동시2000 callback·실제1초 ticker·수집/서비스 실패 cleanup·누락 observer·조작 CPU delta 거절을 검사했다. 최종 Java21 전체899검사 통과: core392/server90/기존 runner16/runner benchmark148/server benchmark74/variant99/correctness48/JVM TCP32. 추가59개(단위·모델27/TCP32), 실패·오류·skip0이다. TCP는 전체8방식×3/5노드×두 실행 방식, 적재4/워밍업3/측정12의 작은 기능시험이며 성능 비교가 아니다.
- 최종 JVM bundle34개·명시 invalid 대조2개를 독립 reader로 검사하고 완료누락·byte 변조·hash를 다시 맞춘 CPU delta/요청 계수/프로세스 중복/거짓 valid 등6개를 거부했다. artifact42건은 JUnit899건과 별도다. source baseline301개 중299동일·실험 실행기2개 수정·새Java9개이며 생산 JAR의 benchmark class0/production 배포 benchmark JAR0을 확인했다. 세부 경로/명령/무결성/보존은 experiments/replication-benchmark-v1/JVM_OBSERVATION_{VALIDATION,SOURCE_PRESERVATION,ARTIFACT_CHECK}.json과 JVM_OBSERVATIONS.md에 기록했다.
- 초기 새TCP32중6실패(V06 n5와 V07)를 보존했다. 새 검사에 기존10% 동적 포트 준비 gate를 시나리오마다 적용했고 회복 대기 중2400/16384 이상 포트·TIME_WAIT3000 이상을 관측했다. 원 socket 예외를 수집하지 않았으므로 소급 원인 확정/과거 socket 문제 해결을 주장하지 않는다. 준비 gate를 BeforeEach로 바꾸는 중 static 메서드로 인한 JUnit discovery 오류1건은 인스턴스 메서드로 바로잡았다. 기존 회귀 대상·timeout·정상 기준은 완화하지 않았다.
- 마지막 검토에서 ConcurrentSkipListMap의 mapping 함수가 원자적 단일 실행이 아니라 capacity reservation을 중복 계상할 수 있음을 로컬 Java21 src.zip 문서와 대조했다. ConcurrentHashMap의 per-key 원자적 생성과 종료 시 정렬로 바꾸고, cap1·동시4생산자·2000결과 대조를 통과시켰다. 전체 명령은 JVM 그룹 도중 이 수정을 위해 자체 client/worker만 식별해 중단했고 failed manifest를 그대로 보존했다. 소스가 같은6그룹719개 XML을 보존하고, 최종 변경이 새 collector/test2개뿐임을 hash로 대조한 뒤 영향2그룹 runner148/JVM_TCP32와 생산 installDist를 재검증했다. 899는 이 증거의 합이며 단일 전체 명령 통과 주장이 아니다.
- 최종 staged 폴더 일괄 복사는 사용자 변경 덮어쓰기 위험으로 자동 승인 검토에서 거절됐다. 실제 diff를 확인해 이번 자체 작성 파일6개만 대상으로 제한하고, 기존4개 SHA256 일치·새2개 부재를 사전 확인한 뒤 반영하는 안전한 명령은 승인됐다. 사용자 변경을 덮어쓰거나 Git을 조작하지 않았다.
- 상태는 구현·로컬 검증 완료/사용자 MR·병합 대기다. Jira/Notion에 확인된 근거만 기록하며 병합 확인 전164를 완료로 올리지 않는다(신규40SP 중 완료3/진행5/대기32 유지). 다음165 통신,166 저장소,167 내부 진단,168 V06 학습/워밍업,169 correctness 통합,170 공통 dataset/trace 후보,171 통합/overhead 순서다. TraceTool 생성은 서버 실행이 아니며 같은 설정/seed/index·canonical hash·stream mapping으로 입력을 고정해야 한다. EC2/SSH·정식 성능·원격 장애·Hermes·commit/push/merge는 하지 않았다.


## 2026-11-02 | TCP 통신 관측 구현·로컬 검증

- 사용자 요청에 따라 이후 Jira 조회·댓글·상태 변경을 중단한다. 이번 진행은 로컬 코드/문서와 채팅에만 기록했으며 Jira/Notion/EC2·commit/push/merge는 실행하지 않았다.
- 새 TcpTraffic은 process/configuration 단위 기본 비활성 counter다. 기존 IoniaTcpTransport·IoniaMetaBatchTcpTransport와 IoniaWire.write의3곳에만 관찰 hook을 추가했다. 원본/파생 알고리즘·core API·공개 생성자·wire bytes·timeout·새 socket/RPC 정책을 유지했다. 송신 frame과 initiator exchange·logical outcome을 분리하며 Node응답도 공통 wire에서 포착한다. 추가 의존성·sampler thread·프로토콜 callback의 파일 I/O·payload/ID metric label은 없다.
- TrafficRun은 기존 RunLifecycle/LatencyCollector를 재사용하고 TrafficWriter는 별도7파일을 새 경로에만 저장한다. bounded cap·부분 write 불확실성·실패/거절·local socket 종료·phase 경계·cohort와 계수를 대조한다. counter/API scope와 실제 TCP 전달/OS 전체 상태를 구분하며 source/frame admission 경계가 다를 수 있음을 명시했다. 표준 result M15/M16/M17의 전체 coverage·분모 통합은 후속으로 유지한다.
- 최종 Java21 전체 빌드949검사 통과: core392/server103/기존 runner16/runner benchmark148/server benchmark74/variant99/correctness48/JVM_TCP32/network_TCP37. 추가50(단위13/TCP37), 실패·오류·skip0. 실제 8방식×3/5노드×두 실행 방식32조건과 누락/구성2, Meta4→실제batch1의2, 알려진 연결 실패와 정상19요청의 진단1조건을 검사했다. 실제 wire/확인된 byte·연결/실패를 관찰하지만 작은 기능시험이며 정식 성능 비교가 아니다.
- 정상33·명시 invalid2·burst2 결과와 파일/완료 marker 누락·hash 재계산 후 client byte/수신 중복/논리 cohort/거짓 valid 등6개를 독립 Python reader로 대조했다. 총43건은 JUnit949건과 별도다. 최종 full 명령은 Run-Validation.ps1 -Mode Full -Repeat1 -PortWaitSeconds600, 증거는 build/ionia-validation/20261102-144644-a3a346a1이다. 기존310source 중307동일·공통 I/O3개 수정·새Java6개/최종316source가 전체 실행 중 불변이며 생산 benchmark class/JAR 혼입0을 확인했다.
- 초기 신규 테스트는 strict try-lint가 의도적인 명시 close 검사를 거부해 테스트에만 이유를 적은 억제를 적용했다. 전역 -Werror는 유지했다. 첫 export의 record-key Map은 strict JSON에서 거부돼 자체 예비 작업만 중단하고 typed cohort 목록으로 고쳤다. 완료 전 부분 파일은 COMPLETE 없이 남겼다. 별도 최종 파일 검증의 module-relative burst 경로는 소속 모듈을 기준으로 정규화하고 허용 폴더 내부임을 확인했다. 테스트/정상 기준을 완화한 것은 아니다.
- 근거: experiments/replication-benchmark-v1/NETWORK_OBSERVATIONS.md, NETWORK_OBSERVATION_VALIDATION.json, NETWORK_OBSERVATION_SOURCE_PRESERVATION.json, NETWORK_OBSERVATION_ARTIFACT_CHECK.json, validate_network.py. 다음은 저장소 file I/O·stable View/선택적 KV force 구분 관찰이다. 독립 replica RAM 복제 log 의미와 template draft/disabled는 유지한다. 공통 dataset/trace/hash 후보는 나머지 계측·워밍업 정리 후 진행한다.



## 2026-11-02 | O00 원격 기능 pilot 직전 패키지 — 빠른 필수 검증

- 사용자 요청에 따라 O00 3노드 기능 pilot 준비를 우선했다. Jira는 읽기/쓰기/브라우저 모두 접근하지 않았고 EC2/SSH·commit/push/merge도 수행하지 않았다. 저장소 file I/O/force 계측과 전체 성능 비교를 완료했다고 표시하지 않는다.
- 새 server benchmark/pilot의 PilotMain/PilotFiles/RemoteO00DrainProbe와 새 단위 검사2개, experiments/ionia-pilot의 프로필·기동/종료 wrapper·독립 결과 검사기·실행 안내를 추가했다. 기존 IoniaNode/TCP/O00 client/TraceGenerator/ClosedLoopRunner/BenchmarkRunWriter를 재사용한다. 기존316개 source는 모두 SHA256 동일하며 새Java5개만 추가했다. 기존 Gradle에는 opt-in pilotJar/pilotZip/pilotHashes만 추가하고 생산 application JAR에는 benchmark class가 없음을 확인했다.
- bundle의24개 JAR 해시, run의 입력11개 해시, 동일 run/code/input의 replica ready 메타데이터를 확인한다. 공개 View/followerRead RPC로 모든 replica의 view0/NORMAL/연속 applied frontier를 확인하며 client pending/raw/active/occupied도 대조한다. probe는 비차단·한 배치 상한이며 적용 인덱스를 recorded/commit/대기열/SSD force 증거로 사용하지 않는다. 매 run 새 data와 고유ID를 사용하고 중복 client 실행·과거 node data·입력 덮어쓰기를 거절한다.
- Java21.0.11에서 신규 선택 단위11개 통과(실패/오류/skip0). 사용자 요청에 따라 이전 전체949개/약40분 회귀는 재실행하지 않았다. 기존 알고리즘·공통 API·wire를 변경하지 않은 이번 도구 추가에는 입력/원격준비 모델 검사와 실제 별도 JVM 기동을 필수 검사로 선택했다.
- 최종 별도 replica JVM3개+load JVM1개에서 LOAD16/WARMUP8/MEASURE48=72logical 모두 성공(오류/무응답/미접수/미해결0), 기대 쓰기31에 모든 replica 적용, client 종료 후 drain을 확인했다. 본 workload와 별도 예약키 순차 CRUD8개 및 replica별 값9개 감사를 통과해 최종 적용35를 확인했다. 세 node 모두 stop 파일 기반 정상 종료와 stopped marker를 남겼다.
- 재확인 중 ready 파일 생성 직후 복사되어 일부만 읽힐 수 있는 기동 경쟁을 발견했다. node가 ready 내용을 임시 파일에 끝까지 쓴 뒤 atomic rename으로 게시하도록 수정했고 수정된 ZIP의 필수 검사를 재실행해 통과했다. 첫 실패 실행은 원본 작업 증거의 smoke-final에 보존했다. 기준을 완화하지 않았다.
- 독립 Python reader가 canonical binary trace의 헤더/namespace/개수/바이트/해시, writer 파일 무결성, 요청 계수, drain 인덱스, 감사 순서/값 검사 수, 서로 다른4PID와 정상 종료 표시를 대조했다. CLI 음성 대조5개(요청열 변조, replica code 불일치, workload 재실행, node data 재사용, dataset 덮어쓰기)도 모두 거절했다. code 불일치는 CLIENT_STARTED 생성 이전에 거절했다.
- 첫 EC2용 고정 dataset도 생성/독립 검증했다: seed101/key24B/value100B/UNIFORM/streams2/concurrency2, LOAD1000+WARMUP200+MEASURE2000=3200logical. LOAD는 SET100%, WARMUP/MEASURE 설정 혼합80/15/5. 실제 MEASURE는 GET1619/SET281/DELETE100이다. 이3200요청은 아직 실행하지 않았다. 본시험용 대용량 데이터와 동일시하지 않는다.
- 실행 ZIP: cache-server/build/distributions/ionia-pilot.zip. 안내: experiments/ionia-pilot/README.md. 최종 검증: docs/IONIA_PILOT_VALIDATION.json. 증거/준비 dataset: build/ionia-pilot-validation/20261102-161844. 원본 작업 증거는 C:/Users/SSAFY/.codex/.chatgpt-projects/g-p-6ac58d22cee48191ba9860f9176cf956/pilot-evidence-20261102-161844에도 남긴다.
- 다음은 사용자가 EC2 접속 재개를 지시한 뒤 실제3개 replica private IP/별도 load 호스트를 확정하고, Java21·포트·디스크 환경 확인 → 같은 ZIP/run 입력 배포 → ready 메타데이터 수집 → wait/run → 전 node stop/증거 수집이다. Linux wrapper/실제 EC2 호스트 실행은 미검증이다. 이 기능 pilot은 전체8방식 성능 비교·host 자원/전체 통신/저장소 force 계측·전수 선형화·전원 장애 복구를 대체하지 않는다. 독립 Replica RAM Durability Log와 stable View/선택적 KV force(off)를 구분한다.
