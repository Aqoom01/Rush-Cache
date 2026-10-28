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
