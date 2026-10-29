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
