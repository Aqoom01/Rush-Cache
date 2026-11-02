# V01 IONIA-LR 구현 및 기능 검증

2026-10-28. 정석 기준 revision: `08d66c43c488445e6fc0616a895931e0606775c8`.

명세: [V01 상세 설계](https://app.notion.com/p/4074ed90243781819932d9b6449b55c3).
문헌: [IONIA §4.2, §5.2–5.3, §5.7](https://www.usenix.org/system/files/fast24-xu.pdf).

## 변경 범위

V01은 GET 데이터를 현재 Leader에서만 읽는 비교군이다. SET·DELETE·합의·복구·History/LTI·디스크 KV는 정석 계약을 유지한다. 읽기 View 확인도 그대로 유지하므로 Follower에 대한 제어 probe가 발생한다. Follower 데이터 읽기와 MetaQuery를 사용하지 않는다는 의미이며, 모든 통신이 Leader 한 곳에만 발생한다는 의미는 아니다.

`IoniaLeaderReadClient`는 정석 `IoniaClient`의 위 revision에서 분리한 클라이언트다. GET 분기를 현재 endpoint의 Leader 실행으로 제한하고 Follower 읽기 API를 제공하지 않는다. View 확인을 끄는 옵션도 제공하지 않는다. SET fanout·정족수·fallback·ID 검증·deadline·종료 계약은 유지한다. 이후 정석의 변경이 파생 클라이언트에 자동 전파되지는 않으므로 수정 시 양쪽 회귀 검증이 필요하다.

소스 보관 위치는 `cache-core/src/main/java/io/openkache/core/replication/ionia-second/`이다. 하이픈을 포함하는 보관 폴더와 유효한 Java package를 구분하려고 이 폴더 아래의 `src/main/java`·`src/test/java`를 별도 소스 루트로 등록했다. package는 `io.openkache.core.replication.ionia_second`다. 외부 의존성·Gradle 모듈·공통 API를 추가하지 않았다.

`cache-server`에는 별도 V01 CLI만 추가하며 노드는 기존 `io.openkache.server.ionia.IoniaMain`으로 실행한다. 서버의 배너가 O00인 것은 서버 재사용에 따른 것이다. 배포본에는 두 클라이언트가 함께 들어가고 진입점과 실행 설정으로 구분한다.

## 실행

JDK 21을 사용해 저장소 루트에서 실행한다.

```powershell
.\gradlew.bat build :cache-server:installDist --rerun-tasks --console=plain
```

서로 다른 세 터미널에서 노드를 시작한다.

```powershell
.\experiments\ionia-second\start-node.ps1 -Node 1
.\experiments\ionia-second\start-node.ps1 -Node 2
.\experiments\ionia-second\start-node.ps1 -Node 3
```

클라이언트 터미널에서:

```powershell
.\experiments\ionia-second\client.ps1 -Operation set -Key hello -Value world
.\experiments\ionia-second\client.ps1 -Operation get -Key hello
.\experiments\ionia-second\client.ps1 -Operation delete -Key hello
```

V01은 포트 `17201~17203`, 데이터 `build/ionia-second/node-*`를 사용한다. O00 예제의 `17101~17103`, `build/ionia-o00/node-*`와 구분한다. 기존 데이터를 삭제하거나 파생 버전끼리 데이터 디렉터리를 공유하지 않는다. 개별 노드 재시작은 같은 설정·디렉터리를 사용한다. 모든 Replica의 RAM 로그를 잃으면 이 내구성 모델로 정상 복구할 수 없다.

Linux의 클라이언트 진입점은 다음과 같다.

```bash
java -cp 'cache-server/build/install/openkache-server/lib/*' io.openkache.server.ionia_second.IoniaClientMain experiments/ionia-second/node-1.properties get hello
```

라이브러리의 `execute`는 자동 View 재탐색·재시도를 숨겨 수행하지 않는다. 호출자가 주기적으로 `onTimer`를 호출하고, `refreshView`/`updateView` 후 동일 쓰기 ID·payload·총 deadline으로 재시도한다. CLI는 이 조립을 제공한다. CLI는 10초의 총 deadline 안에서 일시적인 discovery 실패와 UNAVAILABLE/NOT_PRIMARY를 재시도하며, 각 대기의 상한에도 남은 deadline을 적용한다. TIMEOUT은 쓰기가 적용되지 않았다는 확정이 아니라 결과 불명일 수 있다.

## 기능 검증 구성

| 명세 | 검증 내용 |
| --- | --- |
| LR01·LR07 | GET HIT/MISS, 현재 Leader 대상, Follower 데이터/Meta 요청 0, View probe 유지 |
| LR02 | Fast ACK 후 Leader apply 지연 중 GET이 과거 VALUE/MISS를 노출하지 않음 |
| LR03 | SET/DELETE/GET의 같은 key 대기와 DELETE 결과·재시도 의미 |
| LR04 | 세 Leader로의 명시적 View 전환, 실제 crash 후 새 Leader 읽기 |
| LR05 | 분할된 옛 Leader의 stale 읽기 거절 |
| LR06 | node/view/client/operation 응답 검증, 만료 뒤 늦은 응답·요청 중 endpoint 교체 |
| LR08 | 3/5노드 Fast 임계값, Leader 포함, majority fallback, dedup·payload 충돌, ACK 뒤 미확정 로그 복구 |
| LR09 | seed 0~63의 10개 동시 CRUD 이력과 오류 이력 검출 대조군, 실제 TCP 32 hot-key SET 수렴 |
| LR10 | 독립 JVM 3개, TCP, 디스크, Leader 강제 종료·재시작·재합류, 실제 V01 CLI CRUD |

프로세스 시험에서는 별도의 O00 클라이언트가 같은 서버에 Follower+Meta 읽기를 실제로 수행하는 대조군도 확인한다. V01 client traffic과 재합류 상태를 확인하기 위한 직접 Replica 조회는 구분해서 집계한다.

테스트 보고서: `cache-core/build/reports/tests/test/index.html`, `cache-server/build/reports/tests/test/index.html`. 유한 이력 검사와 단일 호스트 프로세스 시험은 전체 형식적 증명·다중 물리 장애 도메인 검증을 대체하지 않는다.

## 비교 실험과 제한

이번 단계는 기능·장애 검증이다. throughput·p50/p95/p99·network amplification·SSD 성능·복구 시간의 비교 결과는 측정하지 않는다. 공통 데이터셋과 IONIA용 experiment-runner driver·성능 계측은 후속 작업이다.

O00 기본 생성자를 사용하면 최초 Leader를 읽을 수 있으므로, 비교 시 O00의 Follower 읽기 Options 또는 `readAtFollower`를 명시한다. 기존 O00 CLI는 초기 Follower를 지정하지만 고정 대상 정책이며 전체 Replica 균등 분산과 다르다. View 교체로 해당 대상이 Leader가 될 수도 있다. 대조 실험의 실제 라우팅을 함께 기록해야 한다.

내구성은 정석과 같은 독립 Replica RAM 로그 복제다. per-request SSD WAL/fsync인 D-PERSIST는 구현하지 않았다. KV force와 안정 View 메타데이터 영속화를 Fast ACK 내구성과 혼동하지 않는다. 정석의 전체 로그 replay·64 MiB 상태 전송 제한도 그대로 남는다.

## 공통 benchmark 연결 후속 (2026-10-30)

위195개 검사는 최초 알고리즘 구현 시점 기록이다. 후속으로 실제 IoniaLeaderReadClient를 cache-server의 별도 benchmark/ionia_second adapter에서 공통 trace·closed/open-loop·전수 지연·로컬 표준 결과 저장에 연결했다. 기존 알고리즘 디렉터리와 core/server 생산 코드는 그대로 유지하며 실험 O00 adapter의 수명주기를 작은 session factory 경계로 공유한다. O00의 readReplica를 최초 Leader로 고정하는 대체 구현이 아니다.

추가26/최종전체661검사와 3/5노드 두 실행 방식·읽기/혼합8조건, 별도 CRUD audit/O00 Follower+Meta 대조·V01 export8개를 검증했다. 설정/사용법/실제 증거/한계는 ../experiments/replication-benchmark-v1/ADAPTER_V01.md 및 V01_ADAPTER_VALIDATION.json을 따른다. 현재 local manifest/같은 JVM 적용 관찰이며 정식 분산 성능 비교·ready manifest·SSD/power-loss 검증 완료를 뜻하지 않는다. EC2 접속/진단·commit/push/merge는 하지 않았다.
