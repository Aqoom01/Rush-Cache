# TCP 통신 관찰

2026-11-02. O00/V01–V07의 실제 TCP 송신 프레임·JDK stream bytes·연결 수명주기를 관찰한다. 최종 결과는 NETWORK_OBSERVATION_VALIDATION.json, NETWORK_OBSERVATION_SOURCE_PRESERVATION.json, NETWORK_OBSERVATION_ARTIFACT_CHECK.json에 기록한다. 로컬 기능 검증과 정식 성능 적합성은 구분한다.

## 세 계층의 의미

| 계층 | 출처 | 기록 | 합산 규칙 |
| --- | --- | --- | --- |
| Logical | 기존 실행기 outcome/LatencyCollector | LOAD/WARMUP/MEASURE의 CRUD·성공/오류/미응답·attempt/retry | 논리 완료1회. RPC 수로 대체하지 않는다. |
| Initiator exchange | IoniaTcpTransport와 IoniaMetaBatchTcpTransport | 시작/성공/실패/거절, socket 생성·connect·local closed, 요청 write·응답 read, 확인된 byte 수, 경과시간 | 구독 중 시작한 호출의 socket 종료까지 기록. 연결 관찰의 분모다. |
| Sender frame | 공통 IoniaWire.write | 직렬화 후 실제 write 시도·완료/실패·확인된 송신 bytes | client 요청·server 응답·peer 송신을 각1회만 합한다. exchange/receive 복사본을 더하지 않는다. |

연산1개가 여러 RPC/peer frame을 만들 수 있고 V07의 논리 Meta 질의4개는 wire batch1개가 될 수 있다. 이 차이를 별도 숫자로 보존한다. Opcode는 PEER1/RECORD2/EXECUTE3/READ4/META5/PROBE6/META_BATCH7이다. from0은 부하 측 요청, to0은 서버 응답, 두 endpoint가 replica이면 peer 송신이다. PEER exchange에는 실제 message class의 제한된 tag도 남긴다. frame tag를 추정하거나 payload/요청 ID를 metrics label로 저장하지 않는다.

송신 bytes에는4B 길이 prefix와 실제 protocol header/configuration/body가 포함된다. payload 크기×replica 수로 추정하지 않는다. JDK write가 정상 반환한 byte 수이며 TCP 전달·ACK·IP/TLS/packet bytes나 물리 링크 사용량이 아니다. IOException/unchecked exception으로 write가 실패하면 내부에서 일부가 진행됐을 수 있으므로 확인된 byte 하한과 WRITE_UNCERTAIN을 보존하고 exact bytes는 미확정이다. 거짓0으로 정상 수치를 만들지 않는다. 수신 역시 성공한 read 반환분을 센다.

## 적용 범위와 수명주기

- 새 production 유틸리티 `cache-server/src/main/java/io/openkache/server/observation/TcpTraffic.java`는 기본 비활성이다. `open(configuration,members,maxRows)`로 현재 process의 정확한 wire configuration 하나를 구독한다. 동일 configuration의 동시 구독은 거절한다. 일치하는 다른 트래픽도 관찰되므로 같은 process/configuration을 사용하는 실험은 배타적으로 실행해야 한다.
- 기존 transport2개와 공통 wire 쓰기1곳에만 counter hook을 추가했다. 코어 API·복제 알고리즘·client/engine/node 구현·wire bytes·생성자·timeout·RPC별 socket 정책은 유지한다. 새 의존성은 없다. observer 비용의 성능 적합성은 후속 paired 검사 대상이다.
- Scope는 payload/event 전수 목록 대신 bounded aggregate row를 저장한다. TrafficRun의 한도는50,000 row, Scope API 상한은100,000이다. endpoint/tag는 선언 집합으로 제한하며 cap 초과·범위 누락·counter overflow·불확실 write·종료 미완료는 명시 invalid로 남긴다. eviction으로 숨기지 않는다.
- callback·I/O 경로에서는 counter만 갱신한다. collector는 파일 I/O, 사용자 callback, 추가 sampler thread를 실행하지 않는다. 종료 시 최장3초 동안 기존 I/O의 관찰 완료를 기다리고 구독을 닫는다. 늦은 완료는 예전 Scope에 남고 불변 snapshot을 소급 변경하지 않는다.
- replica·부하가 함께 있는 로컬 fixture는 process_instance_id/pid 하나의 관찰이다. from/to로 노드 활동은 분리하지만 process를 replica 수로 복제해 합산하지 않는다. 다른 JVM의 시간 차를 직접 계산하지 않는다. 원격 수집/통합 agent는 이 단계에 없다.

## 창과 경계

행은 Scope 시작 기준1초 terminal 창, admission stage, terminal stage, from/to, kind, peer tag로 집계한다. Scope의 실제 시작/종료,8개 lifecycle의 요청/관찰 시각, elapsed 합계와 cross-stage 수를 보존한다. 창은 ticker snapshot이나 개별 byte 발생 시각이 아니라 **교환/frame 완료 시점에 귀속한 aggregate**다. 1초 미만의 마지막 구간과 늦은 완료도 실제 창으로 남긴다. phase를 걸친 비용을 시간 비례로 나누거나 서로 다른 cohort를 임의 분모로 쓰지 않는다.

Frame 관찰은 구독 중 시작한 wire write, exchange 관찰은 구독 중 admission된 호출이다. 구독 전에 시작한 peer RPC가 구독 후 frame을 쓸 수 있으므로 두 계층의 경계는 다를 수 있다. 기존 연결 수는 null이며 tracked active/peak는 observer event 처리 순서에 따른 gauge다. OS 전체 연결·진짜 동시 peak·TIME_WAIT로 표시하지 않는다. 각 실험은 시작·종료 drain과 배타적 process/configuration을 준비해야 한다.

client 송신 frame↔exchange write를 대조한다. 전송 실패가 없는 구간에서는 server reply frame↔client read도 정확히 대조한다. 실패가 있으면 송신과 수신이 달라질 수 있으므로 실패를 숨기거나 전달 성공으로 보정하지 않고 진단 수치로 남긴다. 알려진 연결 거부1건과 정상 서비스19요청이 공존하는 대조도 검사한다. 이 결과를 per-op 성능 수치로 정규화하지 않는다.

## 실행기 연결과 저장

`cache-server/src/benchmark/java/io/openkache/server/benchmark/network/TrafficRun.java`는 기존 RunLifecycle과 두 outcome observer를 구현한다. 기존 LatencyCollector를 재사용하고 새 sampler를 만들지 않는다. 정확한 configuration은 파생 adapter의 configuration() 또는 원본 membership.configurationId()에서 받는다.

```java
try (var observation = new TrafficRun(runId, slot, clockId, configuration, members)) {
    var result = new ClosedLoopRunner(generator, plan, client, probe,
        observation, observation).run();
    var exported = TrafficWriter.write(newDirectory, generator.config(), plan,
        result, observation);
}
```

open-loop도 같은 형태다. 기존 자원 관찰을 함께 쓸 때는 lifecycle과 outcome을 각각 두 observer에 전달해야 한다. 기존 생성자/default 성능 writer는 그대로다. 논리 observer를 누락하거나 잘못된 configuration을 구독한 결과는 healthy service여도 이 sidecar의 정상 근거가 될 수 없다.

TrafficWriter는 새 디렉터리에 STARTED, network.json, logical.json, execution.json, summary.json, integrity.json, COMPLETE의7파일을 쓴다. token/run/slot/clock·실제 cohort·누계·연결 종료·stage·반환 상태를 대조한다. integrity는4개 data file의 SHA256, COMPLETE는 integrity hash다. 완료 marker는 저장 완료이며 서비스/관찰/성능 PASS가 아니다. 원자적 rename을 사용하지만 fsync/power-loss 보장은 주장하지 않는다.

표준 성능 bundle/schema와 M15/M16/M17 상태는 변경하지 않았다. process coverage·clock·정확한 phase/분모·프로필 통합 전에는 unsupported를 유지한다. receiver를 sender에 더하는 증폭, logical=RPC=frame으로 단순화한 수치, 같은 process의 복제 합계는 생성하지 않는다. 독립 replica RAM log·stable View force·선택적 KV force의 의미도 그대로다.

## 검증과 후속

```powershell
$env:JAVA_HOME='C:\Users\SSAFY\.jdks\ms-21.0.11'
$env:PATH="$env:JAVA_HOME\bin;$env:PATH"
.\gradlew.bat :cache-server:test --tests '*TcpTrafficTest' :cache-server:networkBenchmarkTest --offline --console=plain
.\experiments\ionia-validation\Run-Validation.ps1 -Mode Full -Repeat 1 -PortWaitSeconds 600
python .\experiments\replication-benchmark-v1\validate_network.py <bundle-directory>
```

새 network 그룹만 기존 JVM 그룹 뒤에 실행하며 시나리오 준비 때 기존 Windows10% 포트 예산을 확인한다. 기존 test population·timeout·정상 판정은 완화하지 않는다. 독립 reader는 hash/token·cohort·sender 단일 집계·byte cross-check·연결 보존식과 불확실/실패 상태를 대조한다. 파일과 hash를 다시 맞춘 의미 변조도 대조한다. hash는 서명이나 임의 위조 전체에 대한 보증이 아니다.

현재 provider는 IONIA TCP용이다. Hermes는 해당 전송 경로를 같은 측정 의미로 별도 연결해야 한다. 다음은 저장소 file I/O·force 관찰이며, 내부 진단·워밍업과 통합 후 공통 dataset/trace/hash 후보로 진행한다. EC2/원격 장애·정식 SSD 성능 비교는 이 문서의 로컬 기능 검증과 별개다. 커밋·푸시는 사용자 수행이다. 외부 작업 관리 서비스는 접근하지 않는다.

## P03 TCP 관측 검증 (2026-11-02)

통신 관찰 연결과 Java21 전체 빌드949검사(추가50: 단위13/TCP37)가 통과했다. 실패·오류·skip0이다. O00/V01–V07×3/5노드×closed/open32조건, 누락/잘못된 구성2조건, 실제 V07 질의4→batch frame1의2조건, 실패 preflight 연결을 보존하는 정상 서비스1조건을 포함한다. 정상33/명시 invalid2/burst2 산출물과 의미 변조6개, 총43건을 독립 검증했다. artifact43건과 JUnit949건은 별도다.

기존 source310개 중307개 hash 동일·공통 TCP/송신 wire3개에 기본 비활성 counter hook·새Java6개다. 원본/파생 알고리즘·core API·공개 생성자·wire bytes·timeout·RPC별 socket 정책은 유지했다. 생산 JAR의 benchmark class0·배포 benchmark JAR0도 확인했다. source307/3 구분은 이번 작업 시작점을 기준으로 한다.

상세는 NETWORK_OBSERVATIONS.md와 NETWORK_OBSERVATION_{VALIDATION,SOURCE_PRESERVATION,ARTIFACT_CHECK}.json, validate_network.py를 따른다. 표준 M15/M16/M17의 process coverage·phase/분모 통합과 observer 비용 적합성은 후속이다. 현재 값은 단일 process/configuration의 raw 관찰이며 정식 SSD 성능 비교가 아니다. 다음은 저장소 file I/O·force 관찰이다. 외부 작업 관리 서비스는 접근하지 않았다.

