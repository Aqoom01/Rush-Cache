# Target tracking 운영 사례 참고

이 문서는 HPA·EC2·ElastiCache의 target tracking을 설명한다. OpenKache의 노드 증감 비교 방법으로 확정한 내용은 아니다. 실제 논문별 알고리즘은 [선행 시스템별 설명](elastic-scaling-algorithms.md), 관련 논문과 운영 사례는 [전체 조사](tablet-aware-elasticity-survey.md)를 따른다.

## Kubernetes HPA

관측 지표와 목표의 비율로 원하는 replica 수를 계산한다.

```text
desiredReplicas = ceil(currentReplicas × currentMetric / targetMetric)
```

여러 지표는 각각 계산한 replica 수 중 큰 값을 사용한다. tolerance, 준비되지 않은 Pod·누락 지표 처리, 축소 stabilization과 증감 속도 제한을 함께 적용한다. 제어 대상은 Pod이며 저장소 노드 증감·Tablet 이동 자체를 구현하지 않는다.

출처: [공식 문서 Algorithm details·Configurable scaling behavior](https://kubernetes.io/docs/concepts/workloads/autoscaling/horizontal-pod-autoscale/).

## Amazon EC2 Auto Scaling

목표 부하에 맞춰 instance 용량을 추적한다. 지표는 instance 수가 늘면 instance당 부하가 줄어드는 등 용량 변화에 적합한 특성을 가져야 한다. 일반적인 지연값을 단순 비율식에 대입하는 방식으로 설명하지 않는다.

여러 target tracking 정책에서는 하나라도 증설을 요구하면 증설할 수 있고, 축소는 축소가 켜진 모든 정책이 허용할 때 수행한다. warm-up은 준비 중 지표와 조기 축소를 다루는 장치다. 정확한 동작은 instance·지표·정책 설정에 따른다.

출처: [공식 문서 Choose metrics·Multiple target tracking scaling policies·Instance warmup](https://docs.aws.amazon.com/autoscaling/ec2/userguide/as-scaling-target-tracking.html).

## Amazon ElastiCache

shard·replica 단위 target tracking, cooldown, scale-in 비활성화 옵션을 제공한다. shard·replica 수와 물리 서버 수는 같은 제어 변수가 아니다.

출처: [공식 Auto Scaling policies](https://docs.aws.amazon.com/AmazonElastiCache/latest/dg/AutoScaling-Policies.html).

## Tablet 저장소에 적용할 때의 한계

평균 비율식은 Tablet 배치 가능성, 단일 hot key, 데이터 크기, 이동 중 유효 용량을 직접 판정하지 않는다. 이 문서에서 확인한 target tracking 규칙만으로 데이터 이동 비용을 고려하는 저장소 알고리즘을 재현했다고 볼 수 없다.

실제 적용 여부와 안정화 설정은 선행 방법의 재현 범위·B1/B2 계획·migration 실행을 확인한 후 결정한다. 이 문서의 target tracking을 별도 정책으로 실행하지 않았다. 선행 메커니즘 8개의 가상 prototype 결과와 변경점은 [실험 안내](../scaling-decision/README.md)를 따른다.
