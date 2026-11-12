# V04b — 고정 파일 배치에서 실제 I/O worker 수 분리

2026-10-28. 시작 HEAD `3998af206d7b2363f9d130b0c185b5f99b0c4d3e`, 작업 트리 clean 확인 후 구현했다. commit/push/merge는 실행하지 않았다.

V04a의 K(미완료 쓰기 key 허가 수)와 구별해, V04b는 파일 shard S를 고정하고 실제 전용 I/O thread W를 조절한다. 원본 및 이전 변형은 보존했다. 기능 구현·선택한 장애 fixture 검증을 완료했으며 최적 W와 성능 비교는 미실행이다.

## 원문 근거와 자체 선택

- R: [IONIA §4.1](https://www.usenix.org/system/files/fast24-xu.pdf)의 충돌 순서·실행 큐·연속 applied prefix를 유지한다. 원본 O00 IoniaEngine을 직접 사용한다.
- R: [Early Scheduling arXiv v1 §4.3–4.4/Appendix A](https://arxiv.org/pdf/1805.05152v1)의 고정 큐/FIFO 전제를 V04a 단계에서 대조했다. 그 논문의 class/barrier 스케줄러를 복제하거나 증명을 가져온 것이 아니다. 상세 문헌 확인 범위는 IONIA_V04A.md에 보존했다.
- D: key→고정 shard→고정 worker를 두 단계로 매핑한다. `floorMod(key.hashCode(), S)`로 기존 파일 배치를 유지하고 `shard % W`로 한 파일·RAM index에 한 worker만 접근한다.
- U: 동적 remapping/work stealing·다중 key/transaction, 최적 worker 수, 논문 전체 성능 재현은 포함하지 않는다.

## 모듈과 재사용

| 위치 | 역할 |
| --- | --- |
| cache-core/.../replication/ionia-sixth/ | 독립 main/test roots. package `io.openkache.core.replication.ionia_sixth`. 불변 IoniaWorkerSettings와 테스트 |
| cache-server/.../server/ionia_sixth/ | IoniaWorkerFileStorage, IoniaSixthNode/Main/ClientMain, 실제 파일/TCP/JVM 테스트 |
| experiments/ionia-sixth/ | node-1..3.properties, 별도 포트 17601–17603·데이터 경로 |

원본 IoniaEngine/Client/Config/Log/History/Recovery/Message 및 DTO·wire/TCP·stable View를 직접 재사용한다. SixthEngine은 만들지 않았다. 원본 파일 어댑터는 final/private이며 worker 수와 파일 수가 묶여 있어, 서버에 별도 어댑터 사본을 두고 파일 레코드/CRC/scan/append·lock·선택적 KV force 로직을 유지했다. 공통 SPI/API 또는 외부 의존성을 추가하지 않았다. 이 사본은 원본 어댑터 수정 시 수동 대조해야 한다.

V01 Leader 전용 읽기, V02a History 예산, V03 Prepare 배치, V04a K gate를 누적하지 않는다. 읽기는 O00 Follower+Meta 방식이고 쓰기 fast ACK/합의/복구도 O00다. core는 라이브러리, server는 그 라이브러리를 포함하는 프로세스 조립·I/O 계층이다.

## 설정과 파일 접근

`storage.shards` S와 `storage.io.workers` W는 둘 다 필수, 1..256 정수다. 서버 Main은 예전 `storage.workers`나 모르는 설정을 거절한다. production worker mailbox 용량은 각 65536으로 고정했다. S4/W2는 기능 예제이며 권장 최적값이 아니다.

따라서 총 mailbox slot은 W×65536이며 W에 따라 달라진다. 엔진/consensus backlog 전체에 새 hard RAM 상한을 주지 않았다. 후속 성능 비교는 포화 이하 동일 backlog 조건 또는 별도 동일 총 admission 예산을 명시해야 한다.

S개 파일/offset index와 W개의 실제 OwnerWorker 전용 스레드를 만든다. 한 shard의 SET/GET/DELETE/reset은 같은 FIFO worker에서 처리한다. 서로 다른 shard가 같은 worker에 매핑되면 앞 작업에 함께 막힐 수 있다. W>S는 여분 thread가 유휴가 된다. W는 저장 작업의 실행 스레드 수이며 CPU affinity·물리 디스크 I/O 병렬도나 논리 key 수와 같지 않다.

reset은 W가 아닌 S개 파일 전체를 담당 worker에 제출한다. 원본 엔진의 old apply drain→storage reset→authoritative replay→normal 경계를 유지한다. 미래값의 cancel/timeout이 이미 실행 중인 파일 작업을 취소하거나 같은 shard의 후속 작업을 추월시키지 않는다. 어댑터 BUSY/exception/close는 원래 실패 의미로 반환하며 성공 ACK를 만들지 않는다. 무한 정지한 물리 I/O의 즉시 종료를 보장하지 않는다.

## profile·기존 데이터 보호

S/W·정적 배정 정책·mailbox 용량·KV force=false·레코드 포맷을 canonical 문자열의 SHA-256 profile로 묶는다. wire와 stable View의 configuration에 V04b/profile을 포함한다. Node는 다른 S/W 또는 O00의 기존 데이터 디렉터리를 KV 열기 전에 거절한다. 실제 실험에서는 profile별 새 데이터 경로를 사용한다.

어댑터는 exclusive `storage.lock`을 획득한 뒤 `io-layout-v1.txt`의 S/매핑/포맷을 먼저 검증한다. layout이 없는데 KV 파일이 있거나, manifest가 손상되거나, 범위 밖 shard 파일이 있으면 자동 migration/resharding 없이 거절한다. manifest는 W를 저장하지 않으므로 단독 어댑터의 같은 S/W 변경 재열기는 지원하지만 Node profile 변경을 허용하는 의미는 아니다. 원본 레코드 포맷 호환 읽기는 단독 어댑터 시험이며, 기존 Node 데이터의 자동 이전 절차는 제공하지 않는다.

초기 layout metadata는 force(true)로 기록한다. 이 1회 초기화 metadata force, stable View 저장, 선택적 KV force와 독립 Replica의 RAM Durability Log를 구별한다. Node는 KV force=false를 사용한다. per-request stable WAL/fsync(D-PERSIST)는 없으며, 모든 복제 RAM 로그가 사라지면 기존 fail-closed 복구를 따른다.

## 관측

`IoniaSixthNode.storageDiagnostics()`는 S/W, 현재/최대 작업 수, closed, worker별 submitted/started/finished/rejected/queuedEstimate/lastThreadId를 불변 snapshot으로 제공한다. 실행한 worker의 실제 thread ID를 관측한다. snapshot은 여러 원자 카운터를 순서대로 읽으므로 원자적 전체 상태가 아니다.

활성·대기 수는 command/reset task 개수이며 IOPS·worker 사용률·disk busy time·처리량이 아니다. queuedEstimate는 `max(0,submitted-started-rejected)`다. 정상적으로 drain한 idle 상태에서 수렴하지만, close가 버린 큐 항목은 추정치에 남을 수 있다. W>S의 미사용 thread는 lastThreadId=0일 수 있다. 무거운 hook은 package-private 시험 전용이며 production은 no-op이다.

## 실행

프로젝트 최상위 폴더에서 Java 21을 사용한다. `installDist`는 기본 application을 바꾸지 않으므로 아래처럼 새 Main class를 명시한다. 세 터미널에서 node 번호를 각각 1/2/3으로 바꾼다.

```powershell
.\gradlew.bat build :cache-server:installDist --rerun-tasks --no-parallel --max-workers=1 --console=plain
java -cp 'cache-server/build/install/openkache-server/lib/*' io.openkache.server.ionia_sixth.IoniaSixthMain experiments/ionia-sixth/node-1.properties
```

세 노드가 실행된 뒤 별도 터미널에서:

```powershell
java -cp 'cache-server/build/install/openkache-server/lib/*' io.openkache.server.ionia_sixth.IoniaSixthClientMain experiments/ionia-sixth/node-1.properties set example value
java -cp 'cache-server/build/install/openkache-server/lib/*' io.openkache.server.ionia_sixth.IoniaSixthClientMain experiments/ionia-sixth/node-1.properties get example
java -cp 'cache-server/build/install/openkache-server/lib/*' io.openkache.server.ionia_sixth.IoniaSixthClientMain experiments/ionia-sixth/node-1.properties delete example
```

W를 바꾸는 비교에는 세 노드/클라이언트의 S/W를 동일하게 맞추고 새 data.directory/포트를 사용한다. S와 키/입력 순서·CPU/저장 장치·읽기/배치/ACK/force 설정을 고정한다. 기존 디렉터리나 데이터를 삭제해 설정 보호를 우회하지 않는다.

## 실제 검증

Java 21.0.11의 최종 전체 build/installDist 직렬 실행이 성공했다. **413개 통과(core 321/server 76/experiment-runner 16), 실패·오류·skip 0.** 신규 V04b 연결 테스트는 **21개(core 3/server 18)**다. 기존 원본 엔진 회귀는 전체 회귀에서 그대로 실행했으며 신규 21개에 중복 합산하지 않았다.

직전 병렬 전체 실행에서 기존 V02a `IoniaThirdTcpConcurrencyTest.java:39`의 GET valueVersion이 기대 32/실제 0으로 한 번 실패했다. 기존 소스 변경 없이 해당 테스트 분리 재실행은 통과했고, 이후 전체 직렬 실행도 통과했다. 원인을 확정하지 않았으므로 병렬 실행 안정성이 해결됐다고 주장하지 않는다. V04b 테스트는 그 병렬 실행에서도 모두 통과했다.

| 인수 | 실행 증거 |
| --- | --- |
| V04B-01 | S/W 필수 정수 범위 1..256, 두 차원을 구별하는 SHA-256 configuration profile |
| V04B-02 | 고정 shard%W 매핑, S1/4/7/256 × W1/2/4/8/256, Main 입력 거절 |
| V04B-03 | S4/W1,2,4,8의 실제 전용 thread ID·동시 작업 상한·동일 4개 파일 |
| V04B-04 | 동일 shard Aa/BB FIFO, caller cancel 뒤 실제 작업 유지, 다른 worker 진행 |
| V04B-05 | W1/S4 전체 파일 FIFO reset; original engine View 설치가 old apply를 drain한 뒤 S4 reset/replay |
| V04B-06 | 실제 OwnerWorker mailbox(capacity=2 시험 전용) BUSY, I/O hook IOException, close/reopen, 오류를 성공으로 변환하지 않음 |
| V04B-07 | 단독 어댑터의 같은 S에서 W 변경 재열기, 원본 KV 포맷 호환 읽기; Node는 별도 profile 변경 금지 |
| V04B-08 | S/W directory 및 wire 혼용 거절, legacy/corrupt layout/extra shard, lock·trailing repair |
| V04B-09 | S4/W1,2,4,8 ×16 seeds ×64 CRUD =4096 실제 파일 요청, 단일 제출 순서의 참조 상태와 비교 |
| TCP | W1/W4 각각 32 hot-key 동시 쓰기·원본 Follower+Meta 읽기·모든 replica의 appliedIndex/valueVersion 및 고정 S4 파일 매핑 |
| JVM | 독립 JVM 3개 S4/W2, leader crash·View 변경·restart/rejoin·CLI set/get/delete |

S4×W1/2/4/8×16 seeds×64 = 4096 CRUD는 실제 파일 어댑터에 단일 제출 thread로 넣고 key/shard FIFO 결과를 순차 참조 상태와 비교했다. 분산 이력의 전수 선형성 증명은 아니다. worker 병렬성은 latch와 thread ID로 확인했으며 SSD 성능 수치로 해석하지 않는다. 실제 queue 포화는 같은 OwnerWorker 구현의 시험 전용 용량 2에서 검증했다. production 용량은 65536이다.

기존 보호 파일 **213개**의 SHA-256이 시작 시점과 같다. 기존 파일 변경은 core Gradle의 새 sourceSet 연결과 개발 기록 추가뿐이다. 새 production 클래스의 JAR 포함·test 클래스 제외, installDist 생성도 확인했다. 상세 클래스별 테스트 수·보호 hash·새 구현 hash·산출물 hash는 `IONIA_V04B_VALIDATION.json`에 있다.

## 남은 범위

공통 데이터셋/부하 driver와 처리량·p95/p99·실측 heap/CPU/GC/SSD/NIC, multi-host·power-loss·최적 K/W는 미검증이다. W 증가의 성능 향상을 주장하지 않는다. 기존 64MiB 상태 전송/32MiB KV payload 한도를 유지한다. V05(부하/lag 기반 읽기 대상 선택)와 V06(MetaQuery batching)은 후속 연구 후보이며 이번에 구현하지 않았다. 선택한 유한 기능·장애 fixture 검증이며 전체 프로토콜/모든 interleaving의 증명은 아니다.

## 공통 benchmark adapter 연결 후속 (2026-10-30)

현재 표시 ID V05의 실제 알고리즘을 server benchmark/ionia_sixth adapter에 연결했다. 위 과거 문서명·원래 profile 표기/최초 알고리즘 검사 기록은 보존한다. 기존 알고리즘/core API/wire는 변경하지 않고 공통 ID/deadline/retry·trace·closed/open-loop·전수 지연·로컬 표준 export를 재사용한다.

V05의 실제3/5노드·두 실행 방식·읽기/혼합8조건과 별도 CRUD 값·DELETE OK/MISS·재삽입을 검증하고 결과8개를 schema/hash·histogram/설정으로 대조했다. 여섯 방식 합계 추가99/전체760검사 통과이며 개별 방식의99검사라는 뜻은 아니다. ../experiments/replication-benchmark-v1/ADAPTERS_V02_V07.md 및 REMAINING_ADAPTERS_VALIDATION.json/REMAINING_ADAPTERS_SOURCE_PRESERVATION.json/REMAINING_ADAPTERS_ARTIFACT_CHECK.json을 따른다.

V06 학습의 phase 분리·V07 coalescer의 client별 범위, ready manifest/서버 inventory·공통 정확성/필수 계측·clock/overhead/remote 관찰을 정식 성능 전 보강해야 한다. 본시험 데이터/요청열 후보·pilot과 EC2 접속/진단·성능 비교는 후속이며 commit/push/merge는 하지 않았다. RAM log·stable View/선택적 KV force와 RPC별 socket 정책을 유지한다.
