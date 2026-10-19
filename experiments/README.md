# experiments

연구별 실험 시나리오, 실행 스크립트, 결과 요약을 두는 디렉터리입니다.

- **디렉터리 구성:** 연구 ID별로 나눕니다(예: `experiments/b2-load-balancing/`). `baseline-single-node/`는
  단일 노드 KV baseline 예시입니다.
- **결과 파일:** 원본 결과(`<executionId>.json`)는 Git에 올리지 않습니다(`.gitignore`). 비교용 요약
  (`summary.csv`)과 설정 파일(`config.properties`)만 커밋합니다.
- **실험 코드:** 실험이 사용하는 알고리즘 코드는 `cache-core`의 연구 패키지에 두고, 이 디렉터리에는 실험 설정과 실행
  절차만 둡니다. Runner·Workload Generator 코드 자체는 `experiment-runner` 모듈에 있습니다.

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
