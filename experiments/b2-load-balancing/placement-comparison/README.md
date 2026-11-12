# placement-comparison

Placement 정책 7개(+ 참고용 no-rebalance)를 같은 Tablet 64개·노드 용량·초기 배치·요청 분포에서 20 window 동안 돌려, 노드 부하 균형과 이동량을 비교한다. Baseline은 `greedy-load-placement`. 서버·네트워크·실제 데이터 이동이 없는 계획 계산 실험이다. 설계는 [../design.md](../design.md) §9, 해석은 [../policy-benchmark.md](../policy-benchmark.md).

## 실행

```bash
./gradlew :cache-core:testClasses
java -cp cache-core/build/classes/java/main:cache-core/build/classes/java/test \
    io.openkache.core.balance.experiment.BalanceBenchmark experiments/b2-load-balancing B
```

30초 안팎이 걸린다(B·C 합계). 같은 seed면 결정 시간(`decision_micros`) 외의 값은 매번 같다.

## 파일

| 파일 | 내용 |
|---|---|
| `benchmark-b.csv` | 원본. `benchmark,scenario,variant,seed,metric,value` long 형식 |
| `benchmark-b.json` | (시나리오, variant, 지표)별 평균·최소·최대·표준편차와 Baseline 대비 개선율·95% 신뢰구간 |
| `benchmark-b.md` | 위 JSON을 시나리오별 표로 만든 자동 요약 |
