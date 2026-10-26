# migration-comparison

단일 Tablet 이동의 복사 방식(variant)을 같은 조건으로 검증·비교하는 실험 폴더다.
B1~B3는 연구 담당 영역이고 SC·S는 B3 안의 실험 시나리오 ID다.

- S1~S5 참조 run과 핵심 수치는 [결과](results.md)에 있다. 2026-10-23 generator v2의 full 240회를 사용한다.
- **runs/&lt;execution_id&gt;/**: 실행 한 번의 원본과 자동 생성 보고서. 실행마다 새 폴더를 만든다.
- B3 통합 결과 문서는 공식 run을 선정한 뒤 작성한다. 비교·확장 실험의 공통 계약은 [design.md §6.6](../design.md)에 있다.

## 범위와 상태

| 시나리오 | 내용 | 상태 |
|---|---|---|
| SC1~SC6 | stop-and-copy 단독 Baseline 검증(§6.1) | 실행 코드와 기존 130회 Baseline 결과 보관 |
| S1~S5 | 같은 입력의 stop-and-copy / pre-copy / post-copy 비교 | 비교 backend·finite Zipf·JSON 원본/요약 생성 구현 |
| S6·S3 민감도 | 중단 경계·임계/라운드 설정 비교 | 프로토콜 경계 테스트는 있으나 비교 suite는 후속 |

pre-copy·post-copy의 구현과 별도 정합성 테스트는 [프로토콜 구현 안내](../protocols.md)에 있다. Baseline config는 stop-and-copy만 실행하고 아래 comparison config로 세 방식을 비교한다.

## 세 방식 비교 실행

~~~powershell
.\gradlew.bat build :experiment-runner:installDist
.\experiment-runner\build\install\openkache-experiment-runner\bin\openkache-experiment-runner.bat experiments/b3-live-migration/migration-comparison/comparison.properties
~~~

[comparison.properties](comparison.properties)는 S1~S5의 16조건 × seed 1~5 × 세 방식, 총 240회를 실행한다. N=1만/10만/100만, B=64 KiB/tick, rho=0~1.2, SET/DELETE 비율, Zipf theta=0/0.99/1.2를 바꾼다. 공통 Runner는 새 runs 폴더와 config·source/class hash·실행 로그·원본 JSON을 보존한다.

[comparison-quick.properties](comparison-quick.properties)는 N=100/1,000/10,000, B=4 KiB/tick, pre-copy 종료 임계 1 KiB로 줄여 같은 240회 묶음을 두 번 실행한다. 이는 반복·재현 확인용이며 full 수치와 섞지 않는다. 결과는 cache-core/build/b3-comparison-quick 아래에 생성한다.

비교 원본 schema는 b3-comparison-json-1이다. 기존 Baseline v1 또는 계획된 b3-research-v2 전체 schema와 다르다. 같은 case/seed에 초기 데이터와 tick별 요청 시도 열을 재생한다. 완료 시점이 달라 요청열 소비 길이와 성공 쓰기 수가 다르므로 각 방식은 자신의 성공 요청 oracle과 exact compare한다.

inputHash는 generator 규격·설정·seed의 SHA-256이고 materialized trace 전체 hash가 아니다. 원본 phaseEvents는 상태/라운드 경계와 전송량을 기록하며 dirty generation별 byte 상세를 포함하는 v2 rounds 원본은 아직 없다. finite Zipf는 이 비교 backend에서만 구현했고 공통 Workload Generator의 지원 범위를 바꾸지 않는다.

쓰기 차단은 데이터에 영향을 주지 않는 누락 key DELETE 제어 요청의 BUSY tick 수다. post-copy의 미도착 GET BUSY는 별도 비율로 집계한다. snapshot/delta/post-copy 응답 framing은 각각 실제 모델 크기로 계산하고 ACK/pull 요청 제어 bytes는 제외한다. 원본 JSON을 다시 읽어 누락·중복·입력/소스 출처·정합성을 확인한 뒤 summary.csv와 results.md를 생성한다.

~~~powershell
java -cp "cache-core/build/classes/java/main;cache-core/build/classes/java/test" io.openkache.core.migration.experiment.ComparisonResearchMain --rebuild "<반복 결과 폴더>"
~~~

S6 취소/장애 비교, S3 임계·라운드 민감도, E/M scaling과 실제 KV/peer 실험은 이 suite에 포함하지 않는다.

기존 SC Baseline과 새 비교 결과는 별도 run/schema로 보관한다. Baseline config 실행은 공통 Runner의 연구 backend를 사용하며 JSON 원본 형식 `b3-baseline-json-1`을 생성한다. S1~S6 비교용 `b3-research-v2`와 별도 형식이다. 공식 run은 검증을 마친 뒤 입구 문서에서 명시적으로 선택한다.

## 공통 Runner로 실행

JDK 21과 저장소 checkout이 필요하다. 연구 backend는 core test 소스이므로 먼저 전체 빌드와 Runner 배포본 생성을 수행한다. 기본 TCP config는 기존 방식 그대로 동작한다.

```powershell
.\gradlew.bat build :experiment-runner:installDist
.\experiment-runner\build\install\openkache-experiment-runner\bin\openkache-experiment-runner.bat experiments/b3-live-migration/migration-comparison/baseline.properties
```

Linux/WSL:

```bash
./gradlew build :experiment-runner:installDist
experiment-runner/build/install/openkache-experiment-runner/bin/openkache-experiment-runner \
  experiments/b3-live-migration/migration-comparison/baseline.properties
```

[baseline.properties](baseline.properties)는 전체 26조건 × seed 5개를 1회 실행한다. [baseline-quick.properties](baseline-quick.properties)를 전달하면 key 수를 줄인 25조건 × seed 5개 묶음을 두 번 실행하고 build 아래에 저장한다. `repeat.count`는 seed 수가 아니라 **조건·seed 전체 묶음의 반복 수**다. N·rho·fault 목록과 seed 1~5는 현재 Baseline suite의 고정 조건이며 임의 설정 변경은 아직 지원하지 않는다.

| config | 의미 |
|---|---|
| execution.mode=research | 공통 Runner의 연구 모드. 생략 또는 tcp는 기존 TCP 모드 |
| study / experiment | b3-live-migration / b3-stop-and-copy 식별자 |
| backend.main / backend.classpath | BaselineResearchMain과 core main/test 클래스 경로 |
| backend.arg.1~4 | 출력 폴더·profile·반복 번호·전체 반복 수를 순서대로 전달 |
| backend.result.schema / backend.expected.cases | b3-baseline-json-1과 전체 130 / quick 125개 원본 |
| backend.source.paths / backend.jvm.arg.1 | hash에 포함할 B3 소스와 -Xmx2g JVM 옵션 |
| backend.build.receipt | build 아래의 소스·실행 클래스 hash 기록. 전체 빌드와 installDist에서 생성 |
| repository.root | config 파일 폴더 기준 저장소 경로 |
| backend.option.profile=full 또는 quick | 전체/축소 데이터 크기 |
| repeat.count | 1~100회 반복 |
| run.timeout.ms / execution.timeout.ms | backend 한 번 / 반복 전체의 실제 시간 제한 |
| results.dir | 저장소 기준 출력 상위 폴더. 매번 고유 suite 폴더 생성 |

Runner는 별도 JVM의 성공·실패·실제 시간 초과·미실행을 수집한다. B3 모델의 tick timeout 취소와 프로세스 시간 초과는 별도다. 의도된 migration 취소도 정합성을 만족하면 실험 실행은 성공이다. backend가 없으면 전체 빌드를 요구하는 오류를 내고 기존 run을 덮어쓰지 않는다.

Git commit은 manifest와 원본 JSON에 같은 값을 사용한다. source hash는 텍스트 줄바꿈을 LF로 정규화한다. 빌드 기록과 현재 소스·backend classpath·설치된 Runner 클래스 hash가 다르면 재빌드 전에는 실행하지 않는다. classpath의 byte hash는 source hash와 별도로 manifest와 JSON에 기록한다. 원본 JSON의 Git ignore 정책은 유지한다.

공통 Runner에는 B3 클래스·조건 수·profile 분기가 없다. 이 값은 위 config와 B3 backend가 소유한다. 다른 연구 연결과 완료 파일의 공통 계약은 [연구 backend 안내](../../research-backends.md)를 따른다. 새 run ID는 연구와 무관한 `run-<시각>-<UUID 일부>` 형식이며 기존 run 경로는 보존한다.

## 현재 생성하는 결과

```text
runs/<suite_execution_id>/
├── config.properties                재실행 가능한 해석된 설정
├── manifest.json                    설정·source hash·예상 수·산출물 hash·수집 상태
├── run-summary.json                 반복별 SUCCESS/FAILED/TIMEOUT/SKIPPED
├── summary.csv, results.md          반복별 상태와 상세 결과 링크
└── repeat-001/                      repeat.count에 따라 추가
    ├── launch.properties, backend.log
    ├── results/<run_id>.json        조건·seed별 실제 원본
    ├── summary.csv                  조건별 요약: 조건당 1행(26행), seed 간 값이 다르면 최솟값~최댓값 (Git 보관)
    ├── metrics.csv                  실행·지표별 long CSV(약 1,300행). 분석용 로컬 파일
    ├── results.md                   출처 한 줄 + SC1~SC6 표 (Git 보관)
    └── completion.properties        정상 수집 완료 표시
```

개별 JSON은 전체 정밀도의 metric·unit, 조건·seed·실제 결과·정합성, 시작 시각, suite/개별 실행 ID, 반복 번호, 전체 config, 명령, Git·JVM/OS·source hash를 기록한다. `manifest.json`의 artifact hash로 파일을 확인할 수 있다. 원본 JSON은 Git에서 제외되며 현재 실행 폴더에 보관된다. Git에는 각 반복의 `summary.csv`·`results.md`만 보관한다. manifest가 Git에 없으므로 `results.md` 첫머리에 실행 ID·반복·설정·commit·작업 트리 상태·source hash를 한 줄로 남긴다.

`summary.csv`의 열은 조건 열, `seed 수`, `seed별 값 동일`(1/0), 시간 기준, 결과, 지표 열 순서다. 해당 조건의 표시 대상이 아니고 모든 seed에서 0인 지표는 `–`다. seed별 값은 `metrics.csv`와 원본 JSON에 있다.

`BaselineResearchMain`은 JSON을 case별로 먼저 저장한 뒤 다시 읽어서 요약을 만든다. JSON 개수·중복·형식·정합성 위반이 있으면 완료 표시를 생성하지 않는다. 부분 실행은 complete로 표시하지 않는다. Runner의 실제 제한 시간이 끝나면 backend 프로세스를 종료한다.

JSON에서 보고서만 다시 생성할 때는 빌드된 backend의 다음 명령을 사용할 수 있다. 아래 경로는 JSON과 completion 파일이 있는 실제 반복 폴더로 바꾼다.

```powershell
java -cp "cache-core/build/classes/java/main;cache-core/build/classes/java/test" io.openkache.core.migration.experiment.BaselineResearchMain --rebuild <반복_결과_폴더>
```

기존 `StopAndCopyBaseline <CSV> [--quick]` 직접 실행은 과거 v1 재현용으로 유지한다. 새 실험은 공통 Runner config를 사용한다. 공식 run을 바꾸면 입구 `results.md`와 B3 통합 결과를 함께 갱신한다. JUnit은 대표 경로·JSON 왕복·원본 기반 재생성·설정·프로세스 시간 제한을 검증하며 전체 main 실험을 자동 실행하지 않는다.

## SC Baseline: 모델과 구현 경계

이 절부터 「SC Baseline: 파일 형식」까지는 현재 JSON backend의 모델·지표 정의다. CSV 열은 호환 가능한 파생 요약으로 유지한다.

실제 서버·네트워크·KvEngine을 사용하지 않는다. 초 단위 downtime·p99 latency·실제 네트워크 성능을 측정하는 실험은 아니다.

`StopAndCopyProtocol`이 freeze·snapshot 전송·ACK·abort·cleanup을 수행한다.
`MigrationFixture`는 최소 저장소·전송 adapter·CAS coordinator이고, 외부 `TickRunner`가 준비 조건과 timeout을 확인해 commit을 요청한다.
`StopAndCopyBaseline`이 실행 값을 수집하고 `BaselineResults`가 표를 만든다. 보고서 생성 코드는 test의 experiment 패키지에 있으며 production 프로토콜 구조는 유지한다.

| 항목 | 값 |
|---|---|
| seed | 1~5 |
| key/value | 16/128 bytes. key는 seed/index 각 8 bytes |
| record | 길이 필드 8 + key/value 144 = 152 bytes |
| batch | 목표 64 KiB, 헤더 29 bytes, record 경계 유지 |
| 기본 전송 예산 | 64 KiB/tick |
| 시간 순서 | tick 0 begin·freeze → 전송 → 별도 tick commit → 다음 tick부터 cleanup |
| timeout | commit 전 예산 `ceil(Tablet 직렬화 bytes / budget) + 30` ticks를 다 쓴 다음 호출에서 취소 |
| workload | 균등 key 선택, SET/DELETE=70/30, rho로 누적한 소수 credit으로 요청 수 결정 |

`rho = (152 bytes × 쓰기 시도 수/tick) / budget`이다. freeze 중 이동 Tablet의 SET·DELETE는 BUSY로 거절한다.
GET은 source에서 계속 처리한다. commit 전 destination은 MOVED(source), commit 뒤 source는 GET·SET·DELETE 모두 MOVED(destination)를 반환한다.

## SC Baseline: 시나리오

| ID | 바꾸는 조건 |
|---|---|
| SC1 | N=1만/10만/100만, write 없음. N=10만에서 예산 16/64/256 KiB/tick 비교(`SC1-rate`는 예산 비교 행) |
| SC2 | N=10만, rho=0/0.1/0.5 |
| SC3 | N=10만, freeze 전 DELETE=0/30%, rho=0.1 |
| SC4 | N=1만, 부분 복사·마지막 ACK 전·SWITCHING에서 취소, commit 직후 취소, 부분 cleanup 실패 2회 후 재시도 |
| SC5 | N=1만, checksum 손상·항목 수 불일치·데이터 크기 불일치·batch 중복/누락·마지막 ACK 지연·잘못된 ACK·ACK 누락 |
| SC6 | 이동 Tablet N=10만 + 별도 Tablet, 다른 Tablet 쓰기 0/1/16회/tick, rho=0.1 |

SC3의 삭제는 freeze 전에 성공한 DELETE다. 삭제 0%는 SC2의 rho=0.1과 같은 대조군이다.
SC6의 N은 이동 Tablet 안의 항목 수다. 별도 Tablet 항목은 제외한다.

손상은 수신 시 한 번 검출하고 적용·ACK하지 않는다. batch/ACK 누락과 잘못된 ACK도 commit하지 않고 timeout으로 취소하며 자동 재전송은 없다.
ACK 지연은 마지막 batch 적용 뒤 ACK 조회 3회를 숨긴다. 조회는 데이터를 다시 보내지 않는다.
중복은 수신 adapter가 같은 batch를 두 번 전달하는 이벤트이며 source의 추가 byte 예산을 소비하지 않는다. 중복 네트워크 패킷의 대역폭 비용을 측정하는 모델은 아니다.

## SC Baseline: 결과와 지표 정의

결과 열은 아래 네 값 중 하나다. 예상한 시나리오 이름이 아니라 실제 종료 상태·취소 요청·TickRunner의 timeout 경로로 판정한다.

| 결과 | 의미 |
|---|---|
| 완료 | 이동이 DONE으로 종료 |
| 취소(timeout) | commit 전 실행 예산 소진으로 ABORTED, source에서 쓰기 재개 |
| 취소(요청) | commit 전 명시적 취소로 ABORTED, source에서 쓰기 재개 |
| 취소 거부 | commit 뒤 취소 요청 거부, 이동은 DONE으로 종료 |

| 지표 | 정의 |
|---|---|
| 쓰기 차단 지속 시간(tick) | tick 0 최초 BUSY부터 현재 owner가 제어 SET을 처리한 시점까지. 성공은 destination, 취소는 source에서 재개를 관측 |
| 전체 이동 시간(tick) | begin부터 DONE 또는 ABORTED까지. cleanup과 재시도 포함 |
| 이동 대상 항목 수·데이터 크기 | freeze 전 삭제가 반영된 snapshot. 데이터 크기는 152 bytes/record |
| 보낸 데이터 크기(바이트) | source가 전송한 데이터. batch 헤더 포함, ACK·실험 adapter 호출 비용 제외 |
| 전송량 비율 | 보낸 데이터 크기 ÷ 이동 대상 데이터 크기 |
| 거절한 쓰기 요청 수 | 이동 Tablet workload의 BUSY 수 |
| 다른 Tablet에서 거절한 쓰기 요청 수 | 별도 Tablet workload의 거절 수 |
| 전송 검증 실패 수 | 손상·잘못된 ACK 검출 수. 검증용 stale epoch probe는 제외 |
| 중복 전달 수 | source 전송 뒤 수신 adapter가 추가로 전달한 batch 수 |
| 이전 노드·새 노드 데이터 일치 여부 | 각 노드 전체 map의 기대값 비교. 1=일치, 0=불일치 |
| 잘못된 소유권 전환 수 | 준비 조건 미충족 상태에서 routing이 바뀐 횟수 |
| 정합성 규칙 위반 수 | 단일 owner·데이터 보존·routing/epoch·원복·격리 등의 검사 실패 합계 |

제어 SET은 같은 값을 쓰며 workload 요청·거절 수에는 포함하지 않는다. 정상 쓰기 차단은 `ceil(보낸 바이트 / 예산) + 1` tick이고,
기본 cleanup은 1 tick이다. ACK 지연·cleanup 재시도는 별도로 더해진다. 취소 비용도 실제 쓰기 차단 시간으로 기록하므로 -1로 버리지 않는다.
abort의 전체 데이터 기대값은 source 원본과 빈 destination이다. 성공 시에는 정리된 source와 freeze 전 성공한 SET·DELETE가 반영된 destination을 비교한다.

## SC Baseline: 파일 형식

현재 backend의 원본은 JSON(UTF-8·LF)이고 CSV·Markdown은 파생 요약이다. 과거 v1의 원본은 CSV다. CSV는 UTF-8 BOM·LF, Markdown은 UTF-8·LF로 저장한다. `.gitattributes`도 생성 파일의 LF를 유지한다.
방법론·코드 용어는 `stop-and-copy`, `Tablet`, `batch`, `ACK`, `tick`을 쓰고 설명·상태·지표명은 한국어로 쓴다.
정수는 정수, 입력 rho는 소수 2자리, 측정 전송량 비율은 소수 6자리로 반올림하며 지수 표기는 쓰지 않는다. 아주 작은 비율은 `0.000000`이 될 수 있으므로 정확한 비교에는 원본 byte 정수 둘을 사용한다.

long 형식은 유지하되 결과를 조합하던 세 여부 지표와 중복된 -1 downtime 지표는 제거했다. **실행당 지표 수는 고정이 아니다.**

- 공통: 대상 항목·bytes, 보낸 bytes, 쓰기 차단·전체 시간, 전송량 비율, source/destination 데이터 일치, 정합성 위반.
- SC2·SC3·SC6: 이동 Tablet 쓰기 거절. SC6: 다른 Tablet 쓰기 거절.
- SC4·SC5: 잘못된 소유권 전환. SC5: 전송 검증 실패, 중복 조건의 중복 전달 수.
- 그 밖의 조건에서도 예상하지 않은 nonzero 값은 생략하지 않는다.

분석할 때는 조건·seed·결과로 실행을 묶고 `측정 항목`으로 값을 찾는다. 생략된 지표는 해당 조건의 표시 대상이 아니며 미측정 장애로 해석하지 않는다.
run 폴더의 `results.md`는 seed별 결과가 같으면 조건당 한 행으로 모으고, 다르면 최솟값~최댓값과 관측 결과를 표시한다. CSV 요약과 JSON 원본에는 모든 seed를 보존한다.

## S1~S6 비교 (계획)

**상태: S1~S5 비교 backend·finite Zipf·JSON/요약 생성 구현.** 실행은 위 comparison config를 따른다. S6·민감도·v2 상세 rounds 형식은 후속이다. 상세 조건은 [design.md §6.2](../design.md), 전체 v2 계획은 §6.6을 따른다.

같은 조건의 `stop-and-copy`와 `pre-copy`를 비교한다. S 비교에서는 stop-and-copy도 같은 입력으로 새로 실행하며, SC run의 수치를 S 비교 측정값으로 대신 쓰지 않는다.

| 시나리오 | 비교 질문 |
|---|---|
| S1 | 쓰기 없을 때 크기별 쓰기 차단·전송량 |
| S2 | 쓰기가 있을 때 Tablet 크기별 차단·전체 시간 |
| S3 | rho 증가에 따른 수렴·차단·전송량, 라운드 상한·종료 임계 민감도 |
| S4 | 균등·Zipf에서 dirty 집합과 전송량 변화 |
| S5 | 성공한 DELETE의 보존·tombstone 적용 |
| S6 | 공통 취소 경계와 pre-copy 전용 CATCHING_UP 복구 |

### 실행 계약

조건·seed마다 초기 데이터와 tick별 요청 시도를 동일하게 재생하고 fixture는 새로 만든다. 성공·BUSY에 따라 다음 요청열을 바꾸지 않는다. 각 variant의 성공한 SET·DELETE로 자기 정합성 oracle을 갱신하며 두 variant의 최종 map이 같아야 한다고 가정하지 않는다.

쓰기 차단의 시작·재개를 모두 관측한다. commit gate·dirty 관찰·진행도는 test 전용 adapter가 연결한다. 기존 `StopAndCopyBaseline`과 `TickRunner`를 그대로 variant 비교기로 사용하지 않는다. 실험 진입점은 config를 읽는 공통 `experiment-runner`로 하고, test의 `experiment/comparison/MigrationComparison`을 연구 backend로 호출할 계획이다. 현재 Runner는 TCP와 B3 Baseline 연구 backend를 지원한다. S 비교 backend는 아직 없으며, 같은 config 선택·별도 JVM·원본 수집 경로에 추가할 계획이다.

S6의 CATCHING_UP은 stop-and-copy에서 미지원으로 표시한다. post-copy를 추가할 때는 소유권 선전환·미도착 key·실패 계약을 별도로 검증한 뒤 공통 입력·출력을 재사용한다.

### 생성할 결과

suite 실행마다 이 폴더의 새 `runs/<execution_id>/`에 저장한다. 다른 run 폴더에는 덮어쓰기·append하지 않는다. 원본은 JSON이고 CSV·Markdown은 원본에서 재생성하는 요약이다.

| 파일 | 내용 |
|---|---|
| manifest.json | suite·입력·설정 식별, 예상 실행 목록과 수집 상태 |
| results/<run_id>.json | 공통 Runner metadata와 B3 설정·정합성·종료 사유·숫자 지표·라운드의 원본 |
| run-summary-<suite_id>.json | 반복 실행의 성공·실패·실제 시간 초과·미실행과 원본 경로 |
| cases.csv / runs.csv | JSON 조건·실행 상태의 파생 요약 |
| summary.csv | 조건별 요약(조건당 1행). Git 보관 |
| metrics.csv | JSON 숫자 지표의 실행·지표별 long 형식. 로컬 전용 |
| rounds.csv | JSON 라운드 관측의 dirty·전송 bytes·tick·전환 사유 요약 |
| results.md | 공통 조건별 stop-and-copy / pre-copy / 차이 표와 해석 |

원본 JSON은 공통 결과 정책대로 Git에서 제외한다. Git에는 각 반복의 `summary.csv`·`results.md`만 보관하고, config snapshot·manifest·실행 로그는 실행한 PC에 남는다. 원본 별도 보관 위치와 hash는 manifest에 남긴다. 현재 SC v1은 그대로 보존하고 새 실행부터 JSON 원본을 만든다. manifest만 JSON이고 지표는 CSV에만 있는 상태를 JSON 원본 형식으로 부르지 않는다.

차이는 같은 case·seed·반복의 `pre-copy - stop-and-copy`로 계산한 뒤 seed별로 요약한다. 쓰기 차단·전체 시간·전송량을 함께 보고, 전송량 비율의 분모는 공통 초기 데이터 bytes로 고정한다. stop phase 전환 이유는 실제 종료 이유와 별도 열로 둔다. 한쪽 누락·미지원·정합성 실패는 차이 대신 상태와 이유를 표시한다.

### 구현 완료 조건

공통 요청열 재현, Baseline 회귀, pre-copy 정합성·종료 조건, S1~S6와 민감도, JSON 라운드 원본·자동 비교 보고서, 누락·중복·중단된 실행 처리까지 확인해야 전체 비교 완료로 표시한다. 빈 결과 파일이나 예상 수치를 미리 만들지 않는다.
