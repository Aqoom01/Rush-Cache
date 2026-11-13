> 전체 방법론 source 감사와 누락 보완: [감사 결과](system-integration/ec2/METHOD_AUDIT.md), [현재 검증](system-integration/ec2/AUDIT_VALIDATION.md).

> 현재 A1+A2+A3+B1+B2+B3 동시 통합: [실행 안내](system-integration/ec2/README.md), [역할과 범위](system-integration/FULL_SYSTEM_SCOPE.md). 과거 단일 protocol 결과는 동시 통합 검증과 구분한다.

# experiments

연구별 실험 시나리오, 실행 스크립트, 결과 요약을 두는 디렉터리입니다.

**A1~B3 공통 통합 구현·전체 후보와 호환성**은 [전체 통합 기준](system-integration/FULL_SYSTEM_SCOPE.md), 실제 TCP/파일 KV 실행은 [system-integration/ec2](system-integration/ec2/README.md)를 따른다. native 복제 그룹과 반복 B2/B3 제어·세 복사·스케줄러·증감/drain을 연결한다. 기능 검증과 EC2 성능 실험을 구분하며 EC2 실험은 아직 수행하지 않았다. [이전 가상 fixture](system-integration/README.md)와 `partial-*`는 과거 부분 통합 기록이다.

- **디렉터리 구성:** 연구 ID별로 나눕니다(예: `experiments/b2-load-balancing/`). `baseline-single-node/`는
  단일 노드 KV baseline 예시입니다.
- **결과 파일:** 원본 결과(`<executionId>.json`)는 Git에 올리지 않습니다(`.gitignore`). 비교용 요약
  (`summary.csv`)과 설정 파일(`config.properties`)만 커밋합니다. 연구 모드의 `runs/`는 각 반복 폴더의 `summary.csv`·`results.md`만
  커밋하고, 실행 snapshot·manifest·로그·완료 표시·원본 JSON은 ignore합니다.
- **실험 코드:** 실험이 사용하는 알고리즘 코드는 `cache-core`의 연구 패키지에 두고, 이 디렉터리에는 실험 설정과 실행
  절차만 둡니다. 설정 선택·실행 조율·반복·실제 제한 시간·결과 수집의 진입점은 `experiment-runner`입니다. 연구별 backend와 최소 fixture는 해당 연구의 core test 패키지에 둘 수 있으며, 공통 Runner가 config에 지정한 JVM main을 호출합니다. 공통 Workload Generator는 `experiment-runner`에 있습니다.

## Experiment Runner 실행

```bash
./gradlew :cache-server:installDist :experiment-runner:installDist
cache-server/build/install/openkache-server/bin/openkache-server &
experiment-runner/build/install/openkache-experiment-runner/bin/openkache-experiment-runner \
    experiments/baseline-single-node/config.properties
```

- 설정 항목은 `experiment-runner/.../RunnerConfig.java`의 Javadoc을 따릅니다.
- 같은 설정 파일과 `seed`로 다시 실행하면 같은 요청열이 재현됩니다. `concurrency`를 바꾸면 thread 간 끼어듦 순서까지는
  재현을 보장하지 않습니다(각 thread가 만드는 요청열 자체는 재현됩니다).
- warm-up과 측정 구간은 같은 per-thread seed로 시작합니다(측정 구간에 이어지는 새 요청열이 아니라, JIT·연결 초기화
  영향을 줄이기 위해 같은 요청열을 먼저 한 번 돌리는 의도된 설계입니다).
- closed-loop 부하 생성입니다. thread마다 하나의 연결로 요청-응답을 순서대로 반복하므로 `concurrency`가 동시성
  상한이고, 고정 요청률(open-loop)은 지원하지 않습니다.
- 지금은 단일 노드(`cache-server` 1개)만 지원합니다. 3-node나 장애 주입(Fault Scenario)은 아직 없습니다.
- 결과 JSON의 `storageLayer`는 항상 인메모리이고, `faultScenario`는 항상 `"none"`입니다. SSD IOPS·byte, 분산
  failover 시간은 이 Runner가 다루지 않으므로 결과에 그런 필드 자체가 없습니다 — 측정하지 않은 것을 0으로 보고하지
  않습니다.

## 연구용 backend 실행

TCP 결과의 Git 출처는 commit으로 기록합니다. 작업 트리 변경 여부는 수집하지 않습니다. 이전 열 구성을 가진 누적 CSV에는 새 형식의 행을 섞어 쓰지 않으므로 새 `results.dir`로 실행합니다.

공통 Runner는 기본 TCP 모드와 `execution.mode=research`의 config 지정 JVM backend를 지원합니다. 연구 모드는 `backend.main`·classpath·인자로 실행 대상을 선택하며 연구 이름으로 분기하지 않습니다. 서버 host/port·실측 p99 설정은 요구하지 않습니다. 공통 설정과 다른 연구 연결 방법은 [연구 backend 계약](research-backends.md)을 따릅니다.

```powershell
.\gradlew.bat build :experiment-runner:installDist
.\experiment-runner\build\install\openkache-experiment-runner\bin\openkache-experiment-runner.bat experiments/b3-live-migration/migration-comparison/baseline.properties
```

전체 설정은 [B3 baseline.properties](b3-live-migration/migration-comparison/baseline.properties), 빠른 2회 반복은 [baseline-quick.properties](b3-live-migration/migration-comparison/baseline-quick.properties)를 사용합니다. [B3 실행·결과 안내](b3-live-migration/migration-comparison/README.md)를 따릅니다.

연구 backend는 별도 JVM에서 config의 classpath로 실행됩니다. 현재 B3는 빌드된 checkout의 core main/test를 사용합니다. 다른 연구도 결과 계약을 만족하는 main과 config를 준비하면 공통 Runner 코드 수정 없이 같은 경로를 사용합니다. B1의 기존 main은 결과 계약 연결이 필요하고 B2 시나리오 실험 main은 아직 없습니다.

JSON이 실행 원본이고 CSV·Markdown은 파생 요약입니다. B3의 원본은 각 반복 폴더의 `results/*.json`이며 Git에 올리지 않습니다. `runs/`에서 Git에 올라가는 것은 각 반복의 `summary.csv`·`results.md`뿐입니다. 실제 프로세스 시간 제한과 연구 모델의 virtual tick timeout은 별도로 기록합니다.
