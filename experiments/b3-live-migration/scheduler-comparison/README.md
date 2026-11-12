# 고정 이동 계획의 Scheduler 9개 비교

2026-11-05. **목적지는 입력으로 고정하고 실행 순서·경합 회피·byte 배분만 바꾼다.** CAMIG와 candidate-first/candidate-balanced 목적지 선택 비교는 B2 범위여서 코드·조건·이전 결과에서 제거했다. M1~M5의 FIFO 조건 변화 실험과 별도다.

| 실행 정책 | 적용한 원칙·제한 |
|---|---|
| FIFO | 오래된 실행 가능 job 우선·회전 예산 |
| MongoDB식 제한 | source/destination 합쳐 노드당 이동 1개 |
| TiDB token 변형 | 노드별 token으로 시작률 제한 |
| TiDB 착안 AIMD | pending에 따라 시작률 감소/증가, PD v2 재현 아님 |
| iter-GWIN | 경합 graph에서 가중치/degree 기반 독립 집합 |
| single-MCs | clique cover 기반 동시 실행 그룹 |
| SLAMIG | 비용·기한 기반 그룹 순서, 네트워크 영향 γ=0 |
| VMbuddies | dirty/남은 데이터로 byte 배분, VM 공동 cutover 생략 |
| FPTA | 단일 경로·4slot flow 근사 배분 |

선행 근거: [iter-GWIN/single-MCs](https://clouds.cis.unimelb.edu.au/papers/MigPlan-SDCEdge-TMC.pdf), [SLAMIG](https://arxiv.org/html/2101.09716v1), [VMbuddies](https://www.comp.nus.edu.sg/~hebs/pub/VMbuddies-tpds.pdf), [FPTA](https://arxiv.org/html/1412.4980v1), [MongoDB](https://www.mongodb.com/docs/manual/core/sharding-balancer-administration/#range-migration-procedure), [TiDB scheduling](https://www.pingcap.com/blog/tidb-internal-scheduling/).

VM/컨테이너 원 논문의 기본 copy는 Pre-copy다. B3는 실제 Java 세 protocol을 같은 Scheduler에 연결해 Stop/Post-copy까지 확장 비교한다. 원 시스템이 세 copy를 모두 비교했다는 뜻이 아니다.

## 조건과 지표

heterogeneous, conflict, shared-source, shared-destination, dirty-skew, deadline의 6조건이다. 각 조건의 Tablet·source·destination·쓰기·기한·입력 hash는 실행 정책 사이에 같다. 정책은 목적지를 바꿀 수 없다.

- Full: 6조건 × 9정책 × 3복사 × 5seed = 810회/반복, 두 반복 1,620회.
- Quick: 같은 조건·2seed = 324회/반복, 두 반복 648회.
- 실행당 12 jobs. Full 기본 512 keys(크기 8/4/2/1배), node 4,096 bytes/tick·cluster 16,384. Quick 기본 64 keys, node 1,024·cluster 4,096. global 동시 4.
- key16/value128/record152 bytes, 읽기 2회/job/tick, SET70%·DELETE30%. 같은 생성 규격·seed이며 정책에 미래 oracle을 주지 않는다.
- makespan·개별 완료·대기 p99·전송 증폭·BUSY·기한 초과, 매 tick byte/동시 상한·단일 owner·guard·최종 데이터 exact match를 검사한다. 실제 ms·p99가 아닌 논리 tick이다.
- VMbuddies는 초기 dirty 사전 관측·성공 write EWMA(α=.125)를 사용한다. 실패 요청 수와 종료 시점이 달라 성공 요청 수가 같은 실험으로 해석하지 않는다.

원본 schema=b3-scheduler-fixed-plan-json-3. summary/results/jobs/budgets/rounds/paired/dirty 자료는 결정적 파생 결과, timings.csv는 실제 계산 시간 참고값이다. [결과](results.md)와 [실행 감사](../java/results.md)를 따른다.

    .\gradlew.bat :cache-core:b3Research "-Pb3.args=scheduler --profile full --repeat 2 --output experiments/b3-live-migration/scheduler-comparison/runs"

이전 CAMIG 포함 schema를 새 결과에 섞지 않는다. 공통 Runner 연계 없이 독립 Java 실행기를 쓸 수 있다. 실제 노드 N/U/R 시나리오는 실행하지 않았다.
