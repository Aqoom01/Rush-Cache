# V04a 적용 키 동시성 제어 — 고정 저장 배치의 구현·검증

2026-10-28. 기준 HEAD `36920e82fcb55e69e418723a97bee1d381e83267`에 미커밋 V03가 있는 상태에서 착수했다. 이를 workspace의 v04-baseline에 복사하고 해시를 기록했다. commit/push/merge는 수행하지 않았다.

[노션 V04 설계](https://app.notion.com/p/4074ed902437817093baf44698fa56ae) · [실행 결과 JSON](IONIA_V04A_VALIDATION.json).

## 범위와 원문 근거

V04a는 **저장 shard·파일·실제 storage worker 수를 고정하고, 동시에 KV 적용을 기다리는 독립 key 수 K만 조절**한다. 실제 Worker 수를 저장 배치와 분리해 바꾸는 V04b는 미구현이다. V01 읽기 제한·V02a History 예산·V03 배치/admission 정책을 합치지 않는다. O00에서 독립적으로 분리했다.

R(원문): IONIA §4.1은 충돌 연산을 consensus 순서로 실행하고 독립 연산의 병렬 실행을 허용한다. 공개 applied index는 연속 적용 prefix다. [IONIA §4.1](https://www.usenix.org/system/files/fast24-xu.pdf)

R(보충): Early Scheduling의 class 매핑 규칙·FIFO/barrier·Appendix A는 특정 실행 모델의 논증이다. 이번에 확보한 arXiv:1805.05152v1(2018-05-14, 15쪽)의 §4.3–4.4와 Appendix A를 재대조했다. SHA256 `200af25d65e5181aeeca69a5f130e7651af6aa96639c2e7e67441e290982aa49`. SoCC 최종본 전체 대조/성능 재현으로 표시하지 않는다. [저자 v1](https://arxiv.org/pdf/1805.05152v1)

R(보충): 자원 이용 분석은 §V의 환경·application·계측 정의 부분을 확인했다. 동기화 대기와 큐 유휴를 구별하며 BFT-SMaRt/JVM11·연결 리스트 환경은 현재 Java21 파일 KV와 다르다. 공개 PDF 본문은 웹으로 읽었고 직접 파일 다운로드는 403이었다. [Resource Utilization Analysis §V-A–D](https://www.inf.usi.ch/phd/batista/papers/badpLADC2019.pdf)

R(보충): Hybrid Scheduling의 §1, §5/§5.2는 per-shard DAG와 cross-shard 의존성을 사용한다. 그 스케줄러/증명을 K 허가 gate와 동일시하지 않는다. 저자 공개 본문을 웹으로 대조했으며 PDF 파일 직접 다운로드는 403이었다. [저자본 §5](https://www.inf.usi.ch/faculty/pedone/Paper/2022/2022TPDS.pdf)

D(자체 설계): 아래 FIFO key gate·완료 기반 허가 반환·profile·진단은 OpenKache의 단일-key GET/SET/DELETE에 대한 실험 정책이다. 위 논문의 스케줄러를 구현하거나 그 성능·증명을 재현했다고 주장하지 않는다. U(남은 범위): 다중 key/transaction·동적 remapping·V04b·최적 K·원논문 최종판 전체 대조와 성능 재현.

## 변경과 보존

- core 보관: `cache-core/src/main/java/io/openkache/core/replication/ionia-fifth/`, 내부 별도 main/test root. Java package는 `io.openkache.core.replication.ionia_fifth`다.
- `IoniaApplySettings`, `IoniaApplyGate`, `IoniaFifthEngine`을 추가했다. 엔진은 원본의 final/private 경계 때문에 별도 사본이며 적용 선택/완료·reset/진단만 변경했다. 향후 원본 변경은 수동 대조가 필요하다.
- 기존 Client/Config/Log/History/Recovery/Message/DTO와 wire/TCP·파일 KV·stable View를 재사용한다. 공통 API·외부 의존성·WorkloadGenerator·기존 네 방식의 소스/테스트/설정을 바꾸지 않았다.
- server의 `ionia_fifth`에는 Node/Main/ClientMain 조립을 추가했다. 원본 `IoniaFileStorage`의 key hash→shard→파일/worker 매핑은 그대로다.
- 기존 보호 194개 파일 SHA-256 동일. 직전 V03 산출물 중 공유 Gradle/개발 기록을 제외한 22개도 동일하다. 기존 파일 변경은 core Gradle의 새 소스 경로 등록과 개발 기록 추가다. V03 기록/소스를 이전 버전으로 되돌리지 않았다.

## 정확한 정책

| 설정 | 허용·의미 |
| --- | --- |
| apply.max.in.flight.keys | 필수 정수 1–65,536. 노드당 KV 쓰기 CompletionStage를 기다리는 서로 다른 key의 허가 한도 K |
| storage.workers | 필수 정수 1–256. 현재 파일 어댑터의 고정 shard/파일/worker 수 S. K 비교 동안 동일하게 유지 |

시작 후 정책을 바꾸지 않는다. 예제 K4/S2는 기능 fixture이며 최적값이 아니다. K는 thread 수·실제 동시 I/O·네트워크·GET 수를 뜻하지 않는다. 같은 shard의 독립 key들이 허가를 동시에 가져도 파일 I/O는 기존 shard worker에서 직렬화될 수 있다.

1. 기존 commit prefix를 key별 FIFO에 넣는다. 새로 준비된 key는 중복 없는 FIFO ready set에 한 번만 등록한다.
2. active key가 K 미만이면 가장 오래 기다린 ready key의 head 쓰기를 시작한다. 같은 key에서 두 작업을 동시에 시작하지 않는다.
3. 저장소 작업이 끝나고 owner가 완료를 처리할 때 허가를 반환한다. 같은 key에 다음 항목이 있으면 ready의 **뒤**에 다시 넣고 다른 대기 key에도 기회를 준다.
4. 서로 다른 key의 완료 순서는 달라도 기존 completed 집합으로 연속 applied prefix만 공개한다. keyApplied·History/LTI·Follower 선캡처·Client의 View/INDEX 검사·pending 읽기 의미를 유지한다.
5. 사용자 Future의 timeout/cancel은 물리 저장 작업의 취소/완료가 아니다. permit를 일찍 반환하거나 ACK한 로그를 지우지 않는다. KV BUSY/예외/cancel/잘못된 결과는 성공으로 바꾸지 않고 fail-closed한다.
6. View 변경/recovery 중 새 old-generation 작업 시작은 멈춘다. old in-flight drain → 저장소 reset → gate/queue 초기화 → authoritative replay → normal 순서다. replay에도 같은 K를 적용한다. old callback은 generation이 다르면 새 queue/허가를 변경하지 않는다. reset 실패 때는 drain된 old 허가를 정리하고 fail-closed한다.
7. close 후에는 다음 key를 시작하지 않는다. 이미 시작한 저장 작업이 완료되고 owner가 살아 있으면 해당 허가를 회수한다. 물리 작업/owner 자체가 영구 정지한 상황의 강제 종료를 증명하지 않는다.

**한도는 active key에만 적용한다.** ready/consensus backlog의 hard RAM 상한을 추가하지 않았다. owner와 저장 worker의 기존 bounded mailbox가 포화된 경우도 별도 실패 경로로 검증했다. 저장소 자체 자원 부족·기존 O00 입력 크기 제한·전체 상태 전송 한도는 그대로이며 V03의 admission 검사를 가져오지 않았다.

## profile·디렉터리 보호

정책 문자열은 UTF-8 SHA-256 소문자 64 hex다.

```text
v04a|maxInFlightKeys=<K>|storageShards=<S>|ready=fifoKeys|sameKey=fifo|release=physicalCompletion
wireConfig = originalConfiguration + |algorithm=V04a|profile=<hash>
```

서버/client/stable View 모두 K/S를 포함한 같은 profile을 사용한다. K 또는 파일 배치 S가 바뀐 기존 디렉터리는 KV 파일을 열기 전에 거절한다. Node의 실제 storageWorkers와 설정 S의 불일치도 파일 생성 전에 거절한다. 기존 디렉터리의 삭제·자동 초기화·resharding은 수행하지 않는다.

## 진단

`applyDiagnostics()`는 owner가 발행한 불변 snapshot이다. View/generation·Leader/status·installing/replaying·monotonic sampledAt·profile·K/S, gate active/ready/available/peak·starts/released/resetDiscarded/resets, scheduled/commit/applied와 prefix 뒤 완료 개수를 제공한다.

카운터 관계는 `starts = released + resetDiscarded + activeKeys`다. resetDiscarded는 물리 drain 이후 새 installation에서 버린 옛 논리 허가 수다. drain 중 old-generation의 완료 callback이 무시되면 reset 전까지 옛 논리 active가 남을 수 있다. active는 owner에서 아직 해제하지 않은 허가이며 실제 I/O thread 계측이 아니다. 누적 starts/peak는 engine 수명 기준이고, ready/active는 현재 gate 상태다. 클라이언트 종단 지연·큐 대기 시간 분포·CPU/GC/heap/SSD/NIC/실제 worker별 사용률은 아직 계측·비교하지 않았다.

## 실행

JDK 21, 저장소 루트에서:

```powershell
.\gradlew.bat build :cache-server:installDist --rerun-tasks --console=plain
$v04Classpath = 'cache-server/build/install/openkache-server/lib/*'
java -cp $v04Classpath io.openkache.server.ionia_fifth.IoniaFifthMain experiments/ionia-fifth/node-1.properties
# 다른 터미널에서 node-2.properties, node-3.properties로 동일 Main 실행
```

예제 포트 17501–17503, 데이터 `build/ionia-fifth/keys-4-shards-2/node-*`. 별도 터미널에서:

```powershell
$v04Classpath = 'cache-server/build/install/openkache-server/lib/*'
java -cp $v04Classpath io.openkache.server.ionia_fifth.IoniaFifthClientMain experiments/ionia-fifth/node-1.properties set hello world
java -cp $v04Classpath io.openkache.server.ionia_fifth.IoniaFifthClientMain experiments/ionia-fifth/node-1.properties get hello
java -cp $v04Classpath io.openkache.server.ionia_fifth.IoniaFifthClientMain experiments/ionia-fifth/node-1.properties delete hello
```

기존 Client의 Follower+Meta 읽기/majority View 확인을 유지한다. 모든 Replica RAM 로그를 동시에 잃은 기존 디렉터리에서 자동 서비스 재개를 보장하지 않는다.

## 실제 검증과 인수 대조

Java 21.0.11에서 전체 build/installDist --rerun-tasks 성공. **392개 통과(core 318/server 58/runner 16), 실패·오류·skip 0**. V04a 연결 테스트는 63개(core 57/server 6)이며 원본 프로토콜 회귀 32개, 기존 이력 checker/공통 worker 연결 재사용을 포함한다. 새로운 알고리즘 63개를 증명한 숫자가 아니다. 기존 모든 방식이 함께 통과했고 production JAR에 새 클래스 포함·test 클래스 제외를 확인했다.

| 인수 | 실제 확인 |
| --- | --- |
| V04-01 | 동일 key SET/DELETE 순서·동일 ID 재시도·나중 값 보존, hot key 중복 실행 금지 |
| V04-02 | K1/2/4/65536에서 실제 미완료 저장 stage overlap·cap, ready key 회전으로 cold key 진행 |
| V04-03 | index1 보류 중 독립 index2 완료, applied prefix는 0 유지 후 2로 진행 |
| V04-04 | owner mailbox 거절·KV BUSY/throw/exception/cancel, 호출 timeout/cancel·close, 다른 key 실패 중 물리 허가 조기 반환 금지 |
| V04-05 | old apply 보류 중 reset 금지, old drain 뒤 reset/replay, reset 실패 fail-closed 및 허가 카운터 관계 |
| V04-06 | K1/2/4/65536×64 seeds×10 CRUD의 유한 완료 이력, timeout 쓰기의 completion 확장·View 변경·retry, 잘못된 이력 대조군 |
| V04-07 | 다른 Bytes지만 동일 hash인 Aa/BB의 구별·logical overlap, 실제 파일 hash→shard는 고정 |
| V04-08 | 독립 JVM 3개 Leader 강제 종료·새 View·restart/rejoin·CLI CRUD, K/S별 wire·기존 디렉터리 혼용 거절, 실제 TCP K1/K4 각각 hot-key 32개와 S2 파일 수·배치 확인 |

가상 시계·메모리 지연 fixture, 실제 OwnerWorker/공통 KvEngine, 실제 TCP·disk 어댑터·독립 JVM을 구분한다. 모두 단일 호스트의 기능 시험이며 전체 프로토콜·무제한 장애·모든 interleaving에 대한 증명은 아니다. 원문 성능 수치를 이 결과로 대체하지 않는다.

## 남은 범위

V04b(실제 worker 수 실험), 다중 key/transaction, 동적 remapping, 공통 데이터셋/비교 driver·최적 K·성능/SSD/NIC 실측은 미완료다. 원논문 스케줄러·SoCC 최종판 전체 재현도 미완료다. 각 K의 별도 데이터 디렉터리에서 같은 S·논리 데이터/seed·읽기 경로·ACK·batch·History·KV force·CPU 조건을 통제해야 한다. 파일 배치가 같다는 것은 같은 key→shard 규칙이며 파일 내용의 바이트 순서나 warm cache가 항상 동일하다는 뜻이 아니다.

독립 Replica 복제 RAM 로그, stable View 저장, 선택적 KV force를 계속 구분한다. per-request WAL/fsync(D-PERSIST)·power-loss 내구성·SplinterDB 성능을 구현/측정하지 않았다. 기본 64MiB 전체 상태 전송·32MiB KV payload 한도와 전체 RAM 로그 손실 fail-closed를 유지한다.
