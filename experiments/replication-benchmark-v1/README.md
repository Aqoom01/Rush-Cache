# 공통 복제 벤치마크 명세 v1

기준일: 2026-11-02. **명세 v1·논리 trace·O00/V01–V07 client·closed/open-loop·전수 지연 histogram·로컬 결과 export·제한된 correctness 이력/ACK 감사 구현. 분산 관찰·정식 성능 실측은 미완료.**

사용자 요청에 따라 EC2 접속·사전진단은 보류한다. EC2 원격 실행·장애 주입 도구는 제공하지 않는다. closed/open-loop는 Java API와 작은 로컬 fixture로 검증했다. `execution_enabled=false`인 template을 기존 Runner에 넣어 실행하지 않는다.

## 산출물

- [CORRECTNESS_HISTORY.md](CORRECTNESS_HISTORY.md): 전수 payload 이력·제한된 전역 선형성 검사·예약 ACK 키 감사 및 정상 follower 재기동 범위.
- [CORRECTNESS_VALIDATION.json](CORRECTNESS_VALIDATION.json), [CORRECTNESS_SOURCE_PRESERVATION.json](CORRECTNESS_SOURCE_PRESERVATION.json), [CORRECTNESS_ARTIFACT_CHECK.json](CORRECTNESS_ARTIFACT_CHECK.json): 이번 전체 회귀·기존 소스 보존·저장 결과/음성 대조의 실제 검증 기록.
- [validate_correctness.py](validate_correctness.py): 별도 correctness 산출물의 hash·payload·ID/계수·성공 witness replay·ACK 증거 검증. 검증기 exit 0은 서비스 PASS와 다르다.
- 다음은 필수 자원/통신/저장소·1초 창 등 계측과 V06 cold/steady 학습·워밍업 정책 보강이다. 대규모/분산 correctness·표준 성능 result의 M28/M29 통합도 남아 있다.

- [ADAPTERS_V02_V07.md](ADAPTERS_V02_V07.md): 남은6방식의 실제 client/server/profile·raw batch/drain·metadata·검사·학습 워밍업 한계.
- [REMAINING_ADAPTERS_VALIDATION.json](REMAINING_ADAPTERS_VALIDATION.json), [REMAINING_ADAPTERS_SOURCE_PRESERVATION.json](REMAINING_ADAPTERS_SOURCE_PRESERVATION.json), [REMAINING_ADAPTERS_ARTIFACT_CHECK.json](REMAINING_ADAPTERS_ARTIFACT_CHECK.json): 추가99/전체760검사·결과48개·기존269src 중268동일/선택적 Metadata1수정·새Java20개.
- 위 adapter 단계 이후 로컬 correctness 이력/ACK 감사를 추가했다. 필수 계측·V06 cold/steady 학습 워밍업 보강 후 본시험 데이터/요청열 후보·pilot으로 진행한다. adapter 연결이 정식 성능 비교 준비 완료를 뜻하지 않는다.

- [ADAPTER_V01.md](ADAPTER_V01.md): 실제 Leader-read client·공유 수명주기·3/5노드 두 실행 방식·O00 대조군 연결.
- [V01_ADAPTER_VALIDATION.json](V01_ADAPTER_VALIDATION.json), [V01_ADAPTER_SOURCE_PRESERVATION.json](V01_ADAPTER_SOURCE_PRESERVATION.json), [V01_ADAPTER_ARTIFACT_CHECK.json](V01_ADAPTER_ARTIFACT_CHECK.json): 추가26검사·전체661검사·V01 산출물8개·기존262개 중261동일/실험 factory1수정·새Java7개. V02–V07 adapter는 후속.

- [LATENCY_EXPORT.md](LATENCY_EXPORT.md): phase/op/outcome별 전수 지연·percentile/표본 정책·로컬 결과 저장·한계.
- [MEASUREMENT_VALIDATION.json](MEASUREMENT_VALIDATION.json), [MEASUREMENT_SOURCE_PRESERVATION.json](MEASUREMENT_SOURCE_PRESERVATION.json), [MEASUREMENT_ARTIFACT_CHECK.json](MEASUREMENT_ARTIFACT_CHECK.json): 추가28/전체635검사·13개 산출물/4개 음성 대조·기존253소스 중252동일/실험 observer1수정·새Java9개.
- [Validate-Run.ps1](Validate-Run.ps1): PowerShell7 로컬 결과의 schema/hash/계수/histogram 검증. INVALID 증거와 성능 적합성을 구분.

- [OPEN_LOOP.md](OPEN_LOOP.md): fixed-rate open-loop·bounded admission·발행 지연/손실·미발행 계수 계약.
- [B4_VALIDATION.json](B4_VALIDATION.json), [B4_SOURCE_PRESERVATION.json](B4_SOURCE_PRESERVATION.json): 추가22검사·전체607검사 및 기존 src246개 보존 기록. B4 파일명은 이번 검증 기록의 차수이며 IMPLEMENTATION_GAP의 자원 계측 B4 완료를 의미하지 않는다.

- [CLOSED_LOOP.md](CLOSED_LOOP.md): 단계3-A closed-loop·phase/연산 집계·적용 barrier·종료 계약.
- [B3_VALIDATION.json](B3_VALIDATION.json), [B3_SOURCE_PRESERVATION.json](B3_SOURCE_PRESERVATION.json): 추가23검사·전체585검사 및 기존 src238개 보존 기록.

- [ADAPTER_O00.md](ADAPTER_O00.md): 단계2 O00 client·retry/deadline·결과/시도/RPC 집계·로컬 연결.
- [B2_VALIDATION.json](B2_VALIDATION.json), [B2_SOURCE_PRESERVATION.json](B2_SOURCE_PRESERVATION.json): 단계2 검사·기존 코드 보존 증거.

- [TRACE_GENERATOR.md](TRACE_GENERATOR.md): 단계1 생성기·ID/분할·binary/hash 계약·로컬 실행.
- [trace-small.properties](trace-small.properties): 작은 생성기 인수 예제. 최종 성능 workload가 아님.
- [B1_VALIDATION.json](B1_VALIDATION.json): 생성기 검사·CLI hash 대조·기존 회귀와 보호 파일 확인 기록.

- [SCENARIOS.md](SCENARIOS.md): 공통 절차, API·3/5노드·데이터·부하·27개 시나리오와 판정.
- [METRICS.md](METRICS.md): 36개 지표군, 시간/분모/단위·histogram·반복 통계·결과 파일 계약.
- [IMPLEMENTATION_GAP.md](IMPLEMENTATION_GAP.md): 현재 Java 코드와 계측 명세의 차이·후속 구현 순서.
- [scenario-catalog.json](scenario-catalog.json), [metric-catalog.json](metric-catalog.json): machine-readable 목록.
- [manifest.schema.json](manifest.schema.json), [plan.template.json](plan.template.json): Draft-07 run 설정 schema와 **실행 불가 초안**.
- [result.schema.json](result.schema.json), [result-shape.example.json](result-shape.example.json): 결과 envelope schema와 **측정값 없는 형식 예시**.
- [Validate-Spec.ps1](Validate-Spec.ps1): PowerShell 7 `Test-Json`으로 로컬 문서/JSON 일관성만 검사. AWS/네트워크/Java 서버를 호출하지 않는다.
- `validation.json`: 마지막 명세 형식·교차 참조 검사 결과이며, benchmark/correctness 실행 결과가 아니다.
- `source-preservation.json`: 이번 작업 전후 보호 대상 src 파일214개의 목록·SHA-256 동일성 확인. 별도 디렉터리의 모든 파일을 전수 hash한 결과는 아니다.

## 확정된 계약 / 실제 실행 전에 정할 것

| 명세로 고정 | 실행 전 결정·검증 |
| --- | --- |
| 각 EC2에 replica1개, 기본3/5복제본; 기존 O00/V01–V07 독립 비교 | 실제 instance/주소/AZ/storage·용량·메모리·배타적 사용 |
| GET value/MISS, SET blind ACK, DELETE 결과 반환 경로 구분 | 정확한 서버·client adapter와 capability 증거 |
| 성공/오류/미응답·logical op/attempt/message 구분 | 워밍업·요청수·시간·동시성·deadline·disk budget pilot 적합성 |
| client 종단 지연·노드별 비용·복구·정확성·변형 내부 지표 | clock 품질·계측 오버헤드·history checker·장애 주입 구현 |
| null+사유, 원시집계·manifest hash·paired 반복 | SLO 값과 재현 가능한 데이터/요청열 생성 |

100만/1000만키, 100B/1024B값, 1000만요청×5seed, warm-up 최소120초·측정 최소300초는 **계획용 시작값**이다. pilot 없이 optimal/ready로 확정하지 않는다. 3/5노드에서 같은 logical dataset을 각 replica가 보유하므로 메모리·디스크 예산은 복제·로그·과거 append까지 고려한다.

## 보존

기존 `cache-core`, `cache-server`, `experiment-runner` 소스·API·의존성·테스트·설정은 보존한다. 단계1에서 `experiment-runner`에 독립 `benchmark`/`benchmarkTest` 영역을, 단계2에서 공통 client 계수와 `cache-server`의 별도 O00 실험 adapter를 추가했다. 기존 application JAR/배포 의존성과 서버 wire는 보존한다. 이 명세가 기존 모든 변형이 완성됐거나 Hermes가 구현됐다는 증거가 아니다. 과거 V04a/b·V05·V06 파일명은 보존하고 현재 표시 ID V01–V07과 매핑한다.

## 검증 명령

```powershell
pwsh -NoProfile -File ./experiments/replication-benchmark-v1/Validate-Spec.ps1
```

명세 검사에 통과해도 `plan.template.json`은 `draft`이다. 이 검사는 실제 EC2 환경·driver 동작·논문 안전성·성능을 검증하지 않는다.

## 근거

문헌 조사/원문 확인 범위는 Notion 04-2(81건 대조·2건 추가확인)를 따른다.

- https://app.notion.com/p/4084ed9024378144bf1ce5eca63972e4
- O00 계약: https://app.notion.com/p/4064ed9024378122a673e1b4a7467ca1
- 복구 계약: https://app.notion.com/p/4064ed9024378169a3eac3697244c9c3
- IONIA §4.5/§5: https://www.usenix.org/system/files/fast24-xu.pdf
- Hermes §5–6/§8: https://users.cs.utah.edu/~vijay/papers/asplos20a.pdf
- YCSB §3–5: https://cgi.di.uoa.gr/~ad/M155/Papers/p143-cooper.pdf

## P02 JVM 자원·1초 관찰 연결 (2026-11-02)

[JVM_OBSERVATIONS.md](JVM_OBSERVATIONS.md)에 JDK process CPU·heap/non-heap·GC 누계, 실제 자원 interval과 별도 terminal 요청 창, 두 실행기의 선택적 lifecycle, bounded 종료와7파일 sidecar를 설명했다. 표준 결과 M18/M23의 전체 coverage 승격·RSS/개별 pause·원격 관찰·정식 성능 적합성은 후속이다. 현재 JVM을 한 번만 집계하며 로컬3/5 replica+load의 shared-fixture 비용을 노드별 비용으로 표시하지 않는다.

최종 전체899검사(추가59)·정상 JVM bundle34개·명시 invalid 대조2개·변조 거부6개를 확인했다. artifact42건은 JUnit899건과 별도다. 실제 실행·보존·한계는 JVM_OBSERVATION_VALIDATION.json, JVM_OBSERVATION_SOURCE_PRESERVATION.json, JVM_OBSERVATION_ARTIFACT_CHECK.json에 있다. 기존 src301개 중299개 동일·실험 실행기2개 lifecycle 보완·새Java9개이며 원본/파생 알고리즘과 생산 배포는 보존한다. Jira164는 구현·로컬 검증 완료/사용자 MR·병합 대기이며 진행 중으로 유지한다. EC2·성능 비교·Git 작업은 하지 않았다.

검증 집계는 소스가 그대로인6그룹719개와 최종 영향2그룹180개(148+32)다. 전체 명령은 JVM 창의 동시 생성 한도를 수정하기 위해 중단했고 원래 failed manifest를 보존했다. 수정 전후 변경은 새 collector/test2개뿐임을 hash로 확인했다. 최종 영향 재검증과 installDist는 별도 명령/manifest에 기록하며 단일 전체 명령이 통과했다고 표시하지 않는다.

다음은 통신/저장소/내부 진단·V06 워밍업·correctness 결과 통합 후 공통 dataset/trace 후보(P08), 통합·observer 비용 검사(P09), EC2 재개 시 inventory/pilot이다. 기존 TraceTool은 데이터·요청열 생성과 hash를 담당하며 서버 요청을 실행하지 않는다. 실행기는 같은 설정/seed/index로 요청을 생성하므로 재현 manifest·canonical trace hash·stream mapping을 함께 고정해야 한다. 현재 trace-small.properties는 key64/측정10,000의 생성기 예제이며 본시험 dataset이 아니다.


## P03 TCP 관측 검증 (2026-11-02)

통신 관찰 연결과 Java21 전체 빌드949검사(추가50: 단위13/TCP37)가 통과했다. 실패·오류·skip0이다. O00/V01–V07×3/5노드×closed/open32조건, 누락/잘못된 구성2조건, 실제 V07 질의4→batch frame1의2조건, 실패 preflight 연결을 보존하는 정상 서비스1조건을 포함한다. 정상33/명시 invalid2/burst2 산출물과 의미 변조6개, 총43건을 독립 검증했다. artifact43건과 JUnit949건은 별도다.

기존 source310개 중307개 hash 동일·공통 TCP/송신 wire3개에 기본 비활성 counter hook·새Java6개다. 원본/파생 알고리즘·core API·공개 생성자·wire bytes·timeout·RPC별 socket 정책은 유지했다. 생산 JAR의 benchmark class0·배포 benchmark JAR0도 확인했다. source307/3 구분은 이번 작업 시작점을 기준으로 한다.

상세는 NETWORK_OBSERVATIONS.md와 NETWORK_OBSERVATION_{VALIDATION,SOURCE_PRESERVATION,ARTIFACT_CHECK}.json, validate_network.py를 따른다. 표준 M15/M16/M17의 process coverage·phase/분모 통합과 observer 비용 적합성은 후속이다. 현재 값은 단일 process/configuration의 raw 관찰이며 정식 SSD 성능 비교가 아니다. 다음은 저장소 file I/O·force 관찰이다. 외부 작업 관리 서비스는 접근하지 않았다.

