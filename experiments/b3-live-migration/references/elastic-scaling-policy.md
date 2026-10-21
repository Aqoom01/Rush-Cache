# Elastic Scaling Policy

## 출처

### 공식 문서

[Kubernetes Horizontal Pod Autoscaling](https://kubernetes.io/docs/concepts/workloads/autoscaling/horizontal-pod-autoscale/)

- Kubernetes 공식 문서. 관측 metric과 target의 비율로 desired replica 수를 계산하는 controller.
- 읽을 부분: **Algorithm details**(비율식, 준비되지 않은 Pod 처리, 여러 metric), **Tolerance**, **Stabilization window**
- 문장 인용은 같은 문서의 소스([kubernetes/website](https://github.com/kubernetes/website/blob/main/content/en/docs/concepts/workloads/autoscaling/horizontal-pod-autoscale.md))에서 확인했다.

[Amazon EC2 Auto Scaling — Target tracking scaling policies](https://docs.aws.amazon.com/autoscaling/ec2/userguide/as-scaling-target-tracking.html)

- 읽을 부분: **Choose metrics**, **Multiple target tracking scaling policies**, **Define instance warmup time**, **Considerations**
- scale-out과 scale-in의 비대칭, 새 instance warm-up 처리를 원문 문장으로 확인한다.

### 같은 계열의 시스템

[Amazon ElastiCache — Auto Scaling policies](https://docs.aws.amazon.com/AmazonElastiCache/latest/dg/AutoScaling-Policies.html)

- 캐시(Valkey·Redis OSS)에서 target tracking으로 shard·replica를 늘리고 줄이는 사례. cooldown과 scale-in 비활성화를 확인한다.

## 핵심 아이디어

**target tracking:** 노드 수에 따라 바뀌는 metric 하나와 target 값을 두고, metric이 target 근처에 오도록 capacity를 조절한다. 필요한 개수는 현재 값과 target의 비율로 계산한다.

```text
desired = ceil(current × currentMetric / targetMetric)

예) 노드 4대, 노드당 사용률 90%, target 60%  →  ceil(4 × 90 / 60) = 6대
```

이 비율식만 쓰면 metric이 조금만 흔들려도 capacity가 오르내린다. 그래서 세 시스템 모두 **진동을 막는 장치**를 함께 둔다.

| 장치 | 내용 | 확인한 곳 |
|---|---|---|
| tolerance | target과의 차이가 작으면 scaling 생략 | Kubernetes HPA |
| stabilization window | 일정 기간의 추천값을 보고 축소. 짧은 부하 하락에 바로 줄이지 않음 | Kubernetes HPA |
| cooldown | 한 action 뒤 일정 시간 다음 action 차단 | ElastiCache |
| warm-up | 새 instance가 준비되는 동안 metric 집계에서 제외, 그동안 scale-in 차단 | EC2 Auto Scaling |
| 비대칭 | scale-out은 하나라도 필요하면, scale-in은 모두 허용할 때만. 축소는 점진적으로 | EC2 Auto Scaling |

## 효과와 한계

### 기대 효과

- 노드 수를 사람이 정하지 않고 metric에서 바로 계산한다. 공식이 단순해 결정을 재현하고 검증하기 쉽다.
- 진동 방지 장치로 짧은 spike나 target 주변 흔들림에 반응하지 않는다.
- 축소를 보수적으로 해서 가용성을 지킨다.

### 한계

- **metric이 노드 수에 비례해야 한다:** 노드를 늘려도 줄지 않는 metric(예: 전체 요청 수)이나 비례하지 않는 metric(예: 지연)은 비율식에 맞지 않는다.
- **반응이 늦다:** 연속 관측, stabilization, warm-up을 거치므로 갑작스러운 부하에는 늦게 따라간다. 예측 기반 scaling은 다루지 않는다.
- **기본값이 환경에 묶여 있다:** tolerance, window, cooldown의 기본값은 각 제품 환경의 값이다.
- **이동 비용을 모른다:** 세 시스템 모두 capacity를 늘리고 줄이는 데 드는 데이터 이동 비용을 결정에 넣지 않는다.

## 읽으면서 답할 질문

답변에는 근거가 되는 자료와 절을 함께 적는다. 자료에 없는 내용은 **"자료에 없음"**으로 표시한다.

### 1. 필요한 노드 수는 어떻게 계산하고, 어떤 metric을 써야 하나?

**답**

`desired = ceil(current × currentMetric / targetMetric)`로 계산한다. metric이 여러 개면 metric마다 계산해 가장 큰 값을 쓴다.

metric은 capacity에 비례해 늘고 줄어야 한다. 노드당 평균 사용률이나 처리량처럼 노드를 늘리면 노드당 값이 줄어드는 metric이 맞다. 전체 요청 수는 노드 수와 무관해서, 지연은 비례하지 않아서 맞지 않는다.

**근거**

- 비율식과 여러 metric 중 최댓값: [Kubernetes HPA](https://kubernetes.io/docs/concepts/workloads/autoscaling/horizontal-pod-autoscale/) **Algorithm details**: controller는 "operates on the ratio between desired metric value and current metric value"로 replica 수를 계산하고, metric이 여러 개면 "the largest of the desired replica counts is chosen".
- metric 조건: [EC2 target tracking](https://docs.aws.amazon.com/autoscaling/ec2/userguide/as-scaling-target-tracking.html) **Choose metrics**: "The metric value must increase or decrease proportionally to the number of instances in the Auto Scaling group." 맞지 않는 예로 load balancer가 받은 요청 수("doesn't change based on the utilization of the Auto Scaling group")와 요청 지연("doesn't necessarily change proportionally")을 든다.

### 2. capacity가 빠르게 오르내리는 진동(flapping)을 어떻게 막나?

**답**

- **tolerance:** 현재 값과 target의 비율이 1에 충분히 가까우면 아무것도 하지 않는다.
- **stabilization window:** 축소할 때 최근 일정 기간의 추천값 중 가장 큰 값을 써서, 짧은 부하 하락에 바로 줄이지 않는다.
- **cooldown:** 한 action 뒤 일정 시간 다음 scale-in·scale-out 요청을 막는다.

**근거**

- tolerance: [Kubernetes HPA](https://kubernetes.io/docs/concepts/workloads/autoscaling/horizontal-pod-autoscale/) **Tolerance**: "the amount of variation around the desired metric value under which no scaling will occur". 따로 정하지 않으면 "the default cluster-wide tolerance of 10%"를 쓴다.
- stabilization window: 같은 문서 **Stabilization window**: 축소할 때 지난 추천값 중 "uses the highest value from the specified interval". 기본 window는 5분이다(`--horizontal-pod-autoscaler-downscale-stabilization`, **Algorithm details**).
- cooldown: [ElastiCache](https://docs.aws.amazon.com/AmazonElastiCache/latest/dg/AutoScaling-Policies.html): "A cooldown period blocks subsequent scale-in or scale-out requests until the period expires." 지정하지 않으면 "the default for scale-out is 600 seconds and for scale-in 900 seconds". 같은 문서는 target tracking이 "minimizes rapid fluctuations in the number of available shards/replicas"라고 설명한다.

### 3. scale-out과 scale-in을 왜 다르게 다루고, 새 노드는 어떻게 다루나?

**답**

가용성을 우선하기 때문이다. 늘리는 것은 하나라도 필요하면 하고, 줄이는 것은 모두가 허용할 때만 천천히 한다. 줄였다가 부족해지면 서비스가 나빠지지만, 늘렸다가 남으면 비용만 든다.

새 노드는 warm-up이 끝날 때까지 metric 집계에서 빼고, 그동안 scale-in을 막는다. 방금 추가한 노드의 불완전한 metric 때문에 다시 줄이는 일을 막기 위해서다.

**근거**

[EC2 target tracking](https://docs.aws.amazon.com/autoscaling/ec2/userguide/as-scaling-target-tracking.html)

- **Multiple target tracking scaling policies:** "It will scale out the Auto Scaling group if any of the target tracking policies are ready to scale out. It will scale in only if all of the target tracking policies (with the scale in portion enabled) are ready to scale in."
- **Considerations:** "A target tracking scaling policy prioritizes availability during periods of fluctuating traffic levels by scaling in more gradually when traffic is decreasing."
- **Define instance warmup time:** "Until its specified warmup time has expired, an instance is not counted toward the aggregated EC2 instance metrics", "While the scale-out activity is in progress, all scale in activities initiated by scaling policies are blocked until the instances finish warming up."

ElastiCache도 scale-out은 항상 켜져 있고 scale-in만 끌 수 있다("Scale-out activities are always enabled", "Disabling scale-in activities prevents the scaling policy from deleting shards/replicas").

### 추가 확인 항목

| 항목 | 현재 확인한 내용 | 추가로 확인할 내용 |
|---|---|---|
| 준비되지 않은 노드의 metric | EC2는 warm-up 중 instance를 집계에서 제외. HPA는 scale-up 때 not-ready Pod를 "consuming 0% of the desired metric"으로, metric이 없는 Pod는 scale-down 때 100%로 가정한다(**Algorithm details**) | OpenKache의 `JOINING` 노드 metric을 어떻게 볼지 |
| 축소 단위 | EC2는 점진적 축소, ElastiCache는 traffic이 0이면 최소 크기까지 축소 | 한 번에 몇 개까지 줄이는지의 제품별 규칙 |
| 이동 비용 | 세 문서 모두 데이터 이동 비용을 결정에 넣지 않음 | **자료에 없음**. OpenKache는 migration 진행 중 결정을 보류하는 규칙으로 대신한다 |

## 프로젝트 적용

B3는 노드 증감 요청을 외부에서 고정 입력으로 받지 않는다. `ScalingPolicy`가 cluster metric을 받아 결정하고, 이 정책은 1단계 `stop-and-copy` 이후에 구현한다.

### 적용할 내용

- **출력:** `KEEP`, `SCALE_OUT(desiredNodes)`, `SCALE_IN(desiredNodes, candidateNode)` 중 하나.
- **초기 baseline:** `desiredNodes = ceil(currentNodes × currentMetric / targetMetric)`. metric은 노드 수가 늘면 감소하는 노드당 평균 처리량 또는 사용률을 쓰고, 결과는 `minNodes`와 `maxNodes` 사이로 제한한다.
- **안정화 규칙:**
  1. target 주변의 작은 차이는 tolerance로 무시한다.
  2. scale-out은 상한 초과가 연속 관측되면 필요한 노드 수를 계산한다.
  3. scale-in은 모든 사용 metric이 하한 아래이고 stabilization window를 통과했을 때만 한 번에 한 노드씩 허용한다.
  4. 새 노드가 READY가 되고 초기 migration이 끝날 때까지 scale-in하지 않는다.
  5. scale action 또는 migration이 진행 중이면 중복 결정을 보류한다.
- **책임 경계:**

| 단계 | 책임 |
|---|---|
| B3 `ScalingPolicy` | metric에서 desired node count와 증감 방향을 결정 |
| 외부 provisioner·membership | 노드를 실제 생성·종료하고 `JOINING`·`READY`·`DRAINING`·`REMOVED` 상태를 제공 |
| B1 | membership 변화에 맞는 Tablet `Change` 목록 생성 |
| B3 `MigrationScheduler` | `Change`를 실행하고 완료·실패를 scaling controller에 보고 |
| B2 | 필요할 때 load-aware placement·rebalance 품질 개선 |

### 적용하지 않는 내용

- Kubernetes Pod·Deployment API와 control-plane 구현
- CloudWatch alarm, AWS API, service-linked role와 quota
- ElastiCache의 shard·replica 단위와 고정 threshold를 OpenKache node 정책에 그대로 복사하는 것
- 특정 공식 기본값을 OpenKache의 성능 목표로 사용하는 것

### 직접 설계하고 기록할 내용

- tolerance, 연속 관측 횟수, stabilization window, cooldown 값. 공식 제품의 기본값을 복사하지 않고 실험 변수로 둔다 (B3 design.md §6.3).
- scale-in candidate. 첫 baseline은 가장 낮은 부하의 eligible node이고, 실제 drain 가능 여부와 Tablet 이동 계획은 B1 계약으로 확인한다. 모든 migration이 끝난 뒤에만 노드를 종료한다.
- 실제 scaling metric의 이름·단위·집계 window (B3 design.md §8).
