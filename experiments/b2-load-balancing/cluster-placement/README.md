# cluster-placement

배치 정책([../policy-benchmark.md](../policy-benchmark.md) Benchmark B)의 결정을 **실제 cache-server 노드의 라우팅에 적용해** EC2에서 잰다. in-process 벤치마크가 계산값(이용률·이동 MB)이었다면, 여기서는 노드별 처리량·p99·CPU와 **Tablet을 옮긴 뒤 새 노드에서 생기는 cache miss**를 실측한다.

## 방식: 오프라인 계획 재생

1. `ClusterPlacementPlanner`(experiment-runner test, B2 도구)가 Runner와 똑같은 요청열을 재현해 window별 Tablet 부하를 세고, window `w`의 부하로 정책을 돌려 window `w+1`의 주인을 정한다. 결과를 `*.schedule`(window별 라우팅)과 `*.properties`로 쓴다.
2. EC2의 부하 생성 노드는 Runner의 window 실행(`schedule.file`)으로 계획을 재생만 한다. 정책은 실행 중에 돌지 않는다.
3. **이동 = 라우팅 변경.** 데이터를 옮기지 않고(B3 영역), 새 주인은 빈 상태에서 시작한다. GET이 MISS면 클라이언트가 같은 key를 바로 SET해 다시 채운다(`refill.on.miss`, cache-aside). 그래서 이동 비용은 miss 수와 다시 채운 SET 수로 나타난다(Slicer가 캐시에서 쓰는 방식과 같다).

계획은 seed로 정해지므로, 실제 서버가 받을 요청열을 보고 결정한 것과 같다. 다만 실제 관측 부하(실패·지연)를 보고 결정하는 온라인 controller는 아니다.

## 환경

| 항목 | 값 |
|---|---|
| cache-server | 4대(모든 묶음. 묶음 3은 3대로 시작해 window 10부터 4번째 노드 사용) |
| 부하 생성 | 1대(`concurrency=32`) |
| 인스턴스 합계 | 5대(`NODES=4`). **팀원 수동 인스턴스와 합쳐 9대를 넘지 않는지 `infra/scripts/status.sh`로 먼저 확인** |
| 저장소 | `STORAGE=nvme` 권장(SSD 기반). memory로도 실행 가능 |
| 데이터 | key 50만 개 × 1KiB = 약 0.5GB(노드당 약 125MB), GET 90 / SET 10, 측정 전에 모든 key를 SET(`prefill`) |
| Tablet | 노드당 16개 균등 구간에서 시작, 경계 고정(묶음 1·2는 64개, 묶음 3은 48개) |
| window | 20개, window당 요청 2만 건(burst window는 4배). Tablet 64개면 window마다 Tablet당 약 300건 |
| 계획의 노드 용량 | 상대값(평균 이용률 0.7 기준). 실제 용량과 무관하게 정책 입력으로만 쓴다 |

## 시나리오와 묶음

| 묶음 | 폴더 | 시나리오 | 정책 | 실행 수 | NODES | 예상 시간 |
|---|---|---|---|---|---|---|
| 1 | `bundle1/` | HOT_RANGE, ZIPF | greedy(Baseline), slicer, estore, cost-aware, hysteresis, no-rebalance | 2 × 6 × seed 2 = 24 | 4 | 약 27분 |
| 2 | `bundle2/` | MOVING_HOTSPOT(window마다 5% 이동), BURSTY(5 window마다 4배) | greedy, slicer, rendezvous, hysteresis | 2 × 4 × 2 = 16 | 4 | 약 18분 |
| 3 | `bundle3/` | SCALE_OUT(Zipf, window 10에 3→4대) | greedy, slicer, rendezvous, cost-aware | 1 × 4 × 2 = 8 | 4 | 약 9분 |

모든 묶음이 `NODES=4`라 **파이프라인 한 번**(`CONFIG`에 `cluster-placement` 폴더)으로 48회를 연달아 실행한다. 실행 하나를 약 1.1분(미리 채우기 50만 건 + 요청 약 42만 건 + 서버 4대 동시 재시작)으로 잡으면 실험 약 52분, 클러스터 생성·삭제 약 13분을 더해 **전체 약 65분**이다. EC2 처리량을 아직 모르는 추정이다. 실행 하나의 제한은 4분이고, 자동 정지 상한(180분) 안에서 끝나지 않는 config는 `run.sh`가 시작하지 않는다(그 config는 실패로 남고 job도 실패로 표시된다).

## 실행

1. **브랜치 준비:** 이 브랜치를 push하고 Settings → Repository → Protected branches에 추가한다.
2. **대수 확인:** 로컬에서 `infra/scripts/status.sh`를 실행한다. 기존 인스턴스 + 5대가 9대 이하여야 한다.
3. **Run pipeline** (한 번)

   | 입력 | 값 |
   |---|---|
   | `NODES` | 4 |
   | `STORAGE` | nvme |
   | `CONFIG` | `experiments/b2-load-balancing/cluster-placement` (묶음 1-3 전부) |

   묶음 하나만 돌리려면 `CONFIG`에 `.../cluster-placement/bundle1`처럼 그 폴더를 넣는다.

   `CONFIG`에 폴더를 넣으면 CI가 그 안의 config를 이름순으로 모두 실행한다. 실행마다 cache-server를 재시작해 빈 상태에서 시작하고, 로그에 window별 한 줄 요약(`window N moved=.. max_share=.. miss=.. refills=.. max_cpu=.. worst_p99us=..`)이 나온다.

4. **결과 받기와 요약**

   ```bash
   aws s3 cp --recursive --profile openstd \
       s3://openstd-s15p31d111-results/results/b2-cluster-placement/<KST 날짜>/ ./cp-results/
   ./gradlew :experiment-runner:testClasses
   java -cp experiment-runner/build/classes/java/main:experiment-runner/build/classes/java/test:cache-core/build/classes/java/main \
       io.openkache.runner.ClusterPlacementReport ./cp-results experiments/b2-load-balancing/cluster-placement/results.md
   ```

   요약은 (시나리오, 정책)마다 처리량, 평균 최대/평균 요청, 평균 최대 노드 CPU, 평균 최악 노드 p99, GET miss 비율, 이동 Tablet, 다시 채운 SET을 seed 평균으로 쓰고, greedy 대비 개선율을 함께 적는다.

## 계획 다시 만들기

정책·시나리오·기준값을 바꾸면 계획을 다시 만들고 커밋한다(seed로 정해지므로 같은 입력이면 같은 파일이 나온다).

```bash
./gradlew :experiment-runner:testClasses
java -cp experiment-runner/build/classes/java/main:experiment-runner/build/classes/java/test:cache-core/build/classes/java/main \
    io.openkache.runner.ClusterPlacementPlanner experiments/b2-load-balancing/cluster-placement
```

각 묶음의 `plan.md`는 계획 단계의 예상값(이동 Tablet 합, 예상 평균 최대/평균)이다. 실측과 비교해 계산과 실제의 차이를 본다.

## 한계

- 부하 생성 노드가 1 vCPU라 서버 4대보다 먼저 포화될 수 있다. 첫 실행에서 `max_cpu`가 서버마다 낮게(예: 0.5 미만) 나오면 서버가 아니라 부하 생성기가 병목이므로, 처리량 비교로 결론을 내지 않는다.
- CPU는 `/metrics`의 프로세스 CPU 시간이고, 같은 포트(9091)를 쓰는 EC2에서만 모든 노드가 읽힌다(로컬 다중 서버는 하나만).
- 실제 데이터 이동(B3), 이동 중 쓰기 보호, 분할(Benchmark C)은 포함하지 않는다.
