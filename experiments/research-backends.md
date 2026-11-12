# 공통 Runner의 연구 backend 연결

연구 실험은 `openkache-experiment-runner <config.properties>`로 실행한다. 공통 Runner는 연구 이름으로 분기하지 않는다. config가 JVM main 클래스, classpath, 인자, 출력 schema를 선택하고 Runner는 반복·실제 시간 제한·프로세스 종료·로그·파일 수집을 맡는다. 알고리즘과 연구별 결과 검증·표 생성은 연구 backend에 둔다. 기존 TCP 부하 실험은 기본 또는 `execution.mode=tcp`를 유지한다.

## 설정

아래는 연결 형식을 보여 주는 템플릿이다. `example.research.ComparisonMain`은 실제 구현이 아니며 담당 연구의 main으로 바꿔야 한다. 실행 가능한 예시는 [B3 전체 설정](b3-live-migration/migration-comparison/baseline.properties)과 [빠른 설정](b3-live-migration/migration-comparison/baseline-quick.properties)이다.

```properties
execution.mode=research
study=example-study
experiment=policy-comparison
repository.root=../..
results.dir=experiments/example-study/runs
repeat.count=3
run.timeout.ms=600000
execution.timeout.ms=3600000

backend.main=example.research.ComparisonMain
backend.classpath=cache-core/build/classes/java/main,cache-core/build/classes/java/test
backend.source.paths=cache-core/src/main,cache-core/src/test/java/example/research
backend.build.receipt=cache-core/build/research-build/example.properties
backend.result.schema=example-json-1
backend.expected.cases=10
backend.jvm.arg.1=-Xmx2g
backend.option.policy=baseline
backend.arg.1={output.dir}
backend.arg.2={config.backend.option.policy}
backend.arg.3={repeat}
backend.arg.4={repeat.count}
backend.arg.5={config.file}
```

| 설정 | 의미 |
|---|---|
| study / experiment | 연구·실험 식별자. 코드 선택은 `backend.main`이 담당한다 |
| repository.root | config 파일 폴더 기준 저장소 루트 |
| results.dir | 저장소 기준 결과 상위 경로 |
| backend.main | 실행할 Java main 클래스. 사전 등록이나 공통 코드 수정이 필요 없다 |
| backend.classpath | 저장소 기준 디렉터리 또는 JAR 목록. 쉼표로 구분하며 공백은 경로의 일부다 |
| backend.source.paths | 재현 hash에 포함할 소스 파일·폴더 목록. 의존하는 연구 코드도 포함한다 |
| backend.build.receipt | 빌드 완료 시 생성할 소스·backend 클래스·Runner 클래스 hash 기록 경로. 저장소 기준이며 build 아래에 둔다 |
| backend.result.schema | backend JSON 원본의 schema 식별자 |
| backend.expected.cases | 반복당 예상 JSON 수. 생략 시 backend가 선언한 양수 개수와 실제 개수만 비교한다 |
| backend.jvm.arg.1, 2, … | 선택 JVM 옵션. `-X`/`-XX`/`-D` 옵션을 각각 한 항목으로 지정한다 |
| backend.arg.1, 2, … | main 인자. 1부터 연속 번호로 지정하며 공백을 포함한 값도 인자 하나로 전달한다 |
| backend.option.* | 연구별 설정. snapshot과 launch 문맥에 그대로 저장한다 |
| repeat.count | 1~100회 반복. 연구 내부의 case·seed 묶음과 구별한다 |
| run.timeout.ms / execution.timeout.ms | 반복당 / 전체 실제 시간 제한(ms). 기본 600000 / 3600000, 최대 86400000 |

인자는 `{output.dir}`, `{repeat}`, `{repeat.count}`, `{config.file}`, `{config.<설정 key>}`를 지원한다. 예를 들어 `{config.backend.option.policy}`는 연구별 정책 값을 전달한다. `{config.file}`은 전달받은 config의 절대 경로이며 backend가 직접 읽을 수 있다. 기존 B3의 `backend.option.profile`은 backend 인자로 전달하는 연구 설정일 뿐, 공통 Runner는 full/quick의 의미를 해석하지 않는다. 연구별 설정은 `backend.option.*`에 추가하면 된다.

절대 경로도 사용할 수 있다. Windows의 properties 경로는 `/`를 쓰거나 `\\`로 escape한다. shell command·Gradle task를 문자열로 조합하지 않고 지정한 JVM main을 직접 실행한다. config는 코드 실행 대상을 지정하는 저장소의 실행 설정이다.

## backend가 지킬 결과 계약

반복마다 Runner가 빈 폴더를 만들고 `launch.properties`를 먼저 저장한다. 이 파일에는 설정, suite ID, source hash, config 경로, Runner JVM 옵션, 실제 backend 실행 명령이 있다. backend는 전달받은 출력 경로 아래에 다음을 생성한다.

```text
repeat-001/
├── launch.properties              Runner의 실행 문맥
├── backend.log                    Runner가 수집한 표준 출력·오류
├── results/*.json                 연구가 검증한 JSON 원본
├── summary.csv                    원본에서 파생한 요약
├── results.md                     사람이 읽는 연구 결과
└── completion.properties          검증을 모두 마친 뒤 마지막에 생성
```

```properties
status=complete
resultSchemaVersion=example-json-1
cases=10
```

backend는 원본의 중복·형식·연구별 불변식과 종료 사유를 검증하고 원본에서 CSV·보고서를 생성한 뒤 완료 파일을 쓴다. 공통 Runner는 정상 종료 코드, 파일 존재, 완료 상태, schema 일치, JSON 개수를 확인한다. 연구별 JSON 본문의 의미를 해석하거나 B3의 정합성 지표를 다른 연구에 강제하지 않는다. 실패 시 0이 아닌 종료 코드를 반환하고 부분 원본을 남긴다. 의도된 프로토콜 취소도 연구 조건을 만족했다면 backend 실행 자체는 성공일 수 있다.

Runner는 매번 새 `runs/run-<시각>-<UUID 일부>/`를 만들며 config snapshot, manifest, 반복별 상태 요약, 상세 결과 링크를 저장한다. manifest는 연구 소스·공통 Runner 소스·Gradle 설정 hash와 반복 산출물 SHA-256을 기록한다. 실행 실패·시간 초과·미실행은 `FAILED`/`TIMEOUT`/`SKIPPED`로 남고 전체는 `partial`, 모두 성공하면 `complete`다. backend 시간 초과 시 자식 프로세스도 종료한다. Git에는 각 반복의 `repeat-*/summary.csv`·`repeat-*/results.md`만 올라간다. config snapshot·manifest·run-summary·suite 요약·launch·backend.log·completion과 원본 JSON은 `.gitignore`로 제외돼 실행한 PC에만 남는다.

Git commit은 Runner가 수집해 manifest와 backend의 launch 문맥에 기록한다. 실행 코드의 재현 기준은 소스·backend 클래스·Runner 클래스 hash와 빌드 기록이다.

source hash의 `sha256-lf-text-v1`은 상대 경로를 `/`로 통일하고 Java·Gradle·properties·TOML 등 UTF-8 텍스트의 CRLF/CR을 LF로 정규화한다. 바이너리 입력은 원래 byte를 사용한다. backend classpath는 별도 `backendHash`, 실제 Runner 클래스는 `runnerHash`로 식별한다. JDK와 컴파일 환경에 따라 클래스 hash는 달라질 수 있다.

`gradlew build :experiment-runner:installDist`의 `prepareResearchBackends`가 모든 모듈의 `testClasses` 이후 연구 config마다 빌드 기록을 생성한다. `experiments/**/*.properties` 중 research config가 대상이며 runs/results 아래의 실행 snapshot은 제외한다. Runner는 실행 전에 현재 소스·classpath·자기 클래스 hash를 빌드 기록과 대조하고, 기록이 없거나 값이 다르면 출력 폴더 생성 전에 재빌드를 요구한다. 소스만 바꾸고 이전 클래스나 이전 설치 배포본으로 실행한 경우도 성공 결과로 기록하지 않는다. 이 기록은 컴파일 입력과 산출물을 묶는 개발용 검증이며 테스트 통과 증명은 아니다. 실행 중에는 소스·classpath를 수정하거나 동시에 빌드하지 않는다.

## 다른 연구 연결 순서

1. 기존 연구 main이 결과 계약을 만족하면 그대로 사용한다. 다르면 해당 연구 패키지에 얇은 backend main을 두어 기존 실험을 호출하고 JSON·파생 결과·완료 파일을 만든다.
2. 소스와 backend를 빌드한다. 현재 B3처럼 core test에 두는 경우 `gradlew build :experiment-runner:installDist` 후 checkout의 main/test classpath를 사용한다. JAR 및 외부 라이브러리가 필요하면 classpath에 명시한다.
3. 연구 폴더의 config에서 main·classpath·인자·schema·출력 경로를 지정하고 공통 Runner에 전달한다. 공통 Runner에 연구별 분기를 추가하지 않는다.

B3 Baseline은 이 계약으로 연결돼 있다. B1의 기존 `DistributionComparison`은 아직 출력 계약을 연결하지 않았고, B2는 현재 정책 단위 테스트만 있으며 독립 실험 main이 없다. 이 문서는 해당 연구의 실험 구현이나 결과 생성이 완료됐다는 뜻은 아니다. 서로 다른 두 JVM backend의 config 선택·반복·결과 수집과 완료 계약 불일치는 공통 Runner 테스트에서 검증한다.
