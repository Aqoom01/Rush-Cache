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
