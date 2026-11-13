> 이 문서는 과거 A1 **또는** A2 단일 엔진 구성의 검증 기록이다. A1+A2 동시 통합의 증거는 [JOINT_VALIDATION.md](JOINT_VALIDATION.md)를 따른다.

# 전체 통합 구현 검증 결과

2026-11-12. **EC2 접속·업로드·성능 실험은 수행하지 않았다.** 아래는 공통 runtime의 실제 TCP/파일 KV 기능 검사와 한 물리 호스트에서 독립 JVM을 띄운 로컬 검사다. 이전 부분 통합의37회는 `partial-VALIDATION.md`, `partial-local-validation.csv`, 기존 runs에 보존하며 이번 수치와 합치지 않는다.

## 빌드와 기능 검사

- `./gradlew build :cache-server:integrationZip --offline --console=plain`: **BUILD SUCCESSFUL**.
- Java XML 결과 총**2,090개**, 실패/오류0, 기존 조건부 skip12, 실행 통과2,078. JDK21.0.12.1, `-Xlint:all -Werror`, core 외부 production 의존성 검사 통과. [세부 집계](build-validation.json).
- server 통합 검사**78개** 모두 통과: `IntegratedCacheTest`20, `FullIntegratedCacheTest`52, `FailureBridgeTest`6.
- 이 중 native 복제15종의 Pre/Post 이동·중복·반환 이동, PB/PB_FENCED/Raft/VR/Chain/CRAQ/Rosé/O00/V07의 실제3그룹 scale-out/in·drain은 **실제 TCP와 파일 KV**를 사용한다. 단일 JVM에 여러 native node를 띄운 검사이므로 아래 독립 프로세스 검사와 구분한다.
- B1 7종×copy3, scheduler9종×copy3·B2 배치·drain, scaler8종 입력/결정, 반복 제어, 실제 resize double/half, Post의 지연 pull 중 SET/DELETE·source 장애 후 재개는 **정책/상태 기계 binding 검사**이며 Fake backend를 쓴다. 이를 TCP 성능 측정으로 보고하지 않는다.
- O00/V03/VR × SWIM/Lifeguard의 실제 UDP DEAD→view-change·값 보존6개를 검사했다. VR 외부 신호는 view와 함께 보존되어 mailbox 압력에도 tick에서 처리한다.
- Python 배포 도구 검사**9개** 통과. JAR/tool 변조, role/PID 소유권, control EOF·fault window, 전체 B1/B2 목록, covering array의 모든 method 및 scheduler×copy/protocol×scaler 쌍을 검사한다.

## 독립 JVM 실행

`full-r1`1회, `full-r2`5회, `full-r3`7회, `full-r4`3회, 총**16회/측정 요청7,300개**를 수집했다. 매 실행은 native replica9개(3그룹)+gateway+load의 **독립 JVM11개**다. 사용한 번들 hash와 입력 hash, 요청 원본·최종 감사·routing/FD/scheduler/repair/drain 이벤트를 보존했다. [전체 요약 CSV](local-validation.csv)는 실패한 초기 실행도 포함한다.

| 실행 | 실제로 확인한 내용 | 판정 |
|---|---|---|
| full-r1 Post scale | O00+TAG/TAGGED+Heat/Greedy+TiDB-feedback+Post+incremental-drain, 부하 중2→3→1 | 500요청 성공·최종 값 일치 |
| full-r2 VR scale | VR+Lifeguard+ORDER/ORDERED+Cost+SLAMIG+Pre,2→3→1 | 400요청 성공·최종 값 일치 |
| full-r2 A2 AUTO | O00+Lifeguard+TAG+Heat/Greedy+EStore 수 판단+VMbuddies+Post | 실제 CPU로4회 수 판단, active1그룹·값 일치 |
| full-r3 V07 scale | V07+Lifeguard+ORDER+EStore split/Hysteresis+FPTA+Post+incremental-drain | 500요청 성공,2→3→1·값 일치 |
| full-r3 관찰/history | 실제 O00 요청으로100ms 관찰 창40개 수집 | 원본 event에서 rate를 추출·hash 보존 |
| full-r3 P-Store AUTO | 위 실제 history+period4/horizon3, P-Store+Cost+SLAMIG+Post | 60회 수 판단, active1그룹·400요청/최종 값 일치. model은 reference로 명시 |
| full-r3 Chain head/tail crash | 진행 중 이동의 source JVM 종료 확인→A3 DEAD→FREEZE/REPAIR/prefix·apply·ACK/OPEN | 두 실행 모두 repair DONE·이동 완료·최종 값 일치 |
| full-r2/3 Rosé L=0 crash, CRAQ crash | 같은 실제 master 경로와 quorum/file 상태 복구 | repair/이동 완료·최종 값 일치 |
| full-r4 PB_FENCED fixed,1초 pause | old primary FENCED, backup2표 승격·epoch2 | 요청 실패/미발행은 남기고 최종 값 감사 통과 |
| full-r3/4 PB_FENCED+Lifeguard, lease1초·pause1초 | old primary FENCED, FD는 SUSPECT→ALIVE이며 DEAD 없음, backup 승격 없음 | **응답 불가로 최종 감사35개 실패**, correctnessValid=false. 성공으로 집계하지 않음 |
| full-r4 같은 Lifeguard, lease/timeout2초·pause1초 | old primary SERVING·epoch1 유지 | 발행 요청 실패0, 미발행24, 최종 값 감사 통과 |

중단 구간에는 실패·미발행이 있을 수 있다. correctnessValid=true가 무중단 가용성 통과를 뜻하지 않는다. 16회 중13회에서 최종 값 감사가 통과했고,3회는 false다. false3회는 아래 초기 Chain1회와 lease1초/Lifeguard2회다. auditErrors에는 응답 불가도 포함되므로 이를 곧바로 값 유실로 해석하지 않는다.

### 검출·수정한 연결 문제

초기 `full-r2/chain-crash`에서 head를 먼저 열어 cleanup 쓰기가 시작되면 아직 FROZEN인 peer의 길이가 달라져 마지막 OPEN이 거절됐다. 그 실행은 repair FAILED·최종 감사50개 실패로 보존했다. **backup/tail을 먼저, head/primary를 마지막에 열도록 수정**하고 `full-r3` head/tail 영구 종료로 다시 검증했다.

Resize 완료 계수가 metadata merge보다 먼저0이 되는 경합을 발견해, 최종 metadata 변경 후에만 pendingJobs=0을 게시하도록 고쳤다. double/half·값 보존 검사와 전체 native 회귀를 통과했다.

PB_FENCED backup timer가 plain PB의 첫 heartbeat 조건에 묶이는 연결 문제도 수정했다. fenced timer는 항상 실행하며 fixed 모드의 lease peer 접촉을 감지 입력으로 연결했다. 이 수정 이후 fixed pause는 승격/감사를 통과했다.

### 확인한 통합 상호작용

현재 PB_FENCED는 원래 primary의 lease가 만료되면 다시 쓰기를 열지 않는 안전 장치를 가진다. Lifeguard가 짧은 pause 후 ALIVE를 확인해 DEAD를 만들지 않으면 backup 승격이 시작되지 않는다. 계측에서 old `FENCED`, backup `BACKUP`, votes=0, deadAt=-1을 확인했다. 반면 fixed 조합에서는 votes=2/epoch2 승격을 확인했다. 같은 Lifeguard에서 lease를2초로 바꾸면 원래 primary가 SERVING을 유지하고 감사가 통과했다.

이는 **이 설정에서의 lease/FD 결합 효과**이며 Lifeguard 전체의 우열이나 EC2 성능 결론이 아니다. 기존 방법을 임의로 바꿔 통과시키지 않고 실패·설정·이유를 실험 결과에 남겼다.

## 재현과 해석의 경계

- 원본 요청 CSV의 계수·ID·입력 hash·p99를 `verify`로 재계산했다. 역할별11개 PID identity·cleanup 완료를 확인했다. 실제 호스트 `/proc`에서도 이 실행들이 시작한 JVM이 남지 않았는지 대조했다.
- 각 cohort의 실제 실행 bundle hash를 CSV에 보존한다. 마지막 문서/요약 패키징으로 tool receipt가 바뀔 수 있으므로 최종 ZIP의 실행용 JAR/Python이 마지막 검증 버전과 같은지 따로 대조한다.
- [coverage.csv](matrices/coverage.csv)는183개 전략 조합×2workload×3seed=**1,098행**이다. 모든 후보와 모든 호환 전략 쌍을 포함하는 **실행 입력**이며1,098회 실행 결과가 아니다. [smoke.csv](matrices/smoke.csv)45행도 계획이다. 전수14,959,080행은 수만 계산했고 생성/실행하지 않았다.
- `coverageValid`는 scaler 실행 여부를 별도로 판정한다. P-Store 이력 대기를 통과로 숨기지 않는다. 이번 P-Store는 실제40개 history를 사용했지만 capacity/reward model은 기존 reference calibration이다. 모델의 숫자를 실측값으로 설명하지 않는다.
- 서비스 scale-out/in은 미리 실행한 replica 그룹의 참여/제거다. EC2 인스턴스를 생성/종료하거나 idle 예약 자원의 비용을 줄인 결과가 아니다.
- 최종 감사의 큰 실행 기본값은 최대1,000 key 표본이며 이번 작은 pilot은 전수다. per-key oracle이 전체 이력의 선형화 가능성 증명은 아니다.
- gateway metadata/ledger의 HA·재시작, native replica 재가입, 전원 손실 내구성·SSD fsync, 물리 network rate limiter는 이 검사의 범위가 아니다. 파일 KV는 fsync=false이고 scheduler byte 예산은 logical protocol data다.

[전체 범위](../FULL_SYSTEM_SCOPE.md), [실행 방법](README.md), [EC2 실험 계획](EXPERIMENT_PLAN.md)을 따른다. 전체 후보를 연결한 구현과 전체 Cartesian의 안전성/성능 검증 완료를 구별한다.
