# end-to-end

Split 정책과 Placement 정책의 조합을 처음 Tablet 8개에서 20 window 동안 돌려, 분할과 배치를 함께 썼을 때의 노드 부하 균형·이동량·분할 수를 비교한다. Baseline은 `midpoint-split + greedy-load-placement`. 서버·네트워크·실제 데이터 이동이 없는 계획 계산 실험이다. 설계는 [../design.md](../design.md) §9, 해석은 [../policy-benchmark.md](../policy-benchmark.md).

## 실행

```bash
./gradlew :cache-core:testClasses
java -cp cache-core/build/classes/java/main:cache-core/build/classes/java/test \
    io.openkache.core.balance.experiment.BalanceBenchmark experiments/b2-load-balancing C
```

30초 안팎이 걸린다(B·C 합계). 같은 seed면 결정 시간(`decision_micros`) 외의 값은 매번 같다.

## 파일

| 파일 | 내용 |
|---|---|
| `benchmark-c.csv` | 원본. `benchmark,scenario,variant,seed,metric,value` long 형식 |
| `benchmark-c.json` | (시나리오, variant, 지표)별 평균·최소·최대·표준편차와 Baseline 대비 개선율·95% 신뢰구간 |
| `benchmark-c.md` | 위 JSON을 시나리오별 표로 만든 자동 요약 |
