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
