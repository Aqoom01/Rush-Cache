# DynamoDB Split-for-Heat

## 출처

### 입문 자료

[Scaling DynamoDB: How partitions, hot keys, and split for heat impact performance (Part 2: Querying)](https://aws.amazon.com/blogs/database/part-2-scaling-dynamodb-how-partitions-hot-keys-and-split-for-heat-impact-performance/)

- AWS Database Blog, 2023-01-30, Jason Hunter·Vivek Natarajan
- 읽을 부분: **Querying test: On-demand table, one partition key value**
- 그래프를 통해 부하 집중과 분할 이후 처리량 변화를 확인한다.

### 관련 논문

[Amazon DynamoDB: A Scalable, Predictably Performant, and Fully Managed NoSQL Database Service](https://www.usenix.org/system/files/atc22-elhemali.pdf)

- USENIX ATC 2022
- 핵심 절: **§4.4 Splitting for consumption**
- 관련 절: **§4.3 Balancing consumed capacity**
- 관측한 접근 분포에 따른 분할과 저장 노드 간 재배치의 배경을 확인한다.

## 핵심 아이디어

요청이 특정 partition에 집중되면, 해당 partition을 나누어 부하를 분산한다.

분할 지점은 **키 범위의 단순한 중간보다 관측한 접근 분포를 고려해 선택**한다. 분할 이후에도 한쪽으로 요청이 몰린다면 효과가 제한되므로, 이득이 없는 접근 패턴에서는 분할을 보류한다.

```text
부하 관측
    ↓
분할 필요성 판단
    ↓
분할 후 양쪽으로 부하가 나뉠지 확인
    ↓
분할 지점 선택 또는 분할 보류
```

## 효과와 한계

### 기대 효과

같은 partition의 여러 항목에 요청이 집중될 때, 항목들을 나누어 서로 다른 처리 자원에 배치할 수 있다.

```text
분할 전: 1,000 QPS

분할·재배치 후 예시:
A1 → 600 QPS
A2 → 400 QPS
```

수치는 설명용 예시이며 성능 향상을 보장하지 않는다.

### 한계

- **단일 항목 집중:** 항목 하나는 분할 후에도 한쪽에 남으므로 해당 항목의 요청을 나누기 어렵다.
- **순차 접근:** 새 요청이 계속 증가하는 키의 끝부분에 몰리면 분할 후에도 최신 구간만 바빠질 수 있다.
- **LSI 제약:** LSI가 있으면 같은 Partition Key의 항목 묶음 내부를 여러 partition으로 나눌 수 없다. [AWS 설명](https://aws.amazon.com/blogs/database/part-2-scaling-dynamodb-how-partitions-hot-keys-and-split-for-heat-impact-performance/) 이 프로젝트의 KV에는 보조 인덱스가 없으므로 해당 없음.
- **공개 범위:** 정확한 임계값, 관측 기간, 분할점 계산 절차까지 모두 공개되어 있지는 않다.

## 읽으면서 답할 질문

답변에는 근거가 되는 자료와 절·그림을 함께 적는다. 자료에 없는 내용은 **"자료에 없음"**으로 표시한다.

### 1. 크기 기반 분할과 부하 기반 분할은 무엇이 다른가?

**답**

크기 기반 분할은 **저장된 데이터가 너무 커졌을 때 용량을 나누는 것**이고, 부하 기반 분할은 **요청이 집중되어 처리 부담이 커졌을 때 부하를 나누는 것**이다.

| 구분 | 크기 기반 분할 | 부하 기반 분할 |
| --- | --- | --- |
| 판단 기준 | 데이터 크기 | 요청량·소비 처리량 |
| 목적 | 저장 용량 분산 | 처리 부하 분산 |
| 분할점에서 고려할 것 | 자식 구간의 데이터 크기 | 자식 구간에 들어올 요청 부하 |

예를 들어 데이터가 절반씩 나뉘어도 요청의 90%가 한쪽에 몰리면 부하는 균등하지 않다. 부하 기반 분할은 이러한 접근 분포를 고려한다.

분할점 선택에서 논문 §4.4가 비교 대상으로 드는 것은 **key 범위의 가운데**다. 이 프로젝트의 Baseline `midpoint-split`은 이 방식을 따른다.

**근거**

[DynamoDB 논문](https://www.usenix.org/system/files/atc22-elhemali.pdf)의 **§4 도입부와 §4.4 Splitting for consumption**.

- §4 도입부: 크기 기반 분할은 "애플리케이션이 key에 균등하게 접근하고, 크기로 나누면 성능도 균등하게 나뉜다"는 가정에 기댄다. 실제 접근은 시간과 key 범위에 따라 불균등해서, 분할 후 뜨거운 부분이 분할 전보다 적은 성능을 받을 수 있다(throughput dilution).
- §4.4: 분할 지점은 partition이 관측한 key 분포로 정하며, 이 방식이 key 범위를 가운데에서 나누는 것보다 효과적이라고 기술한다.

### 2. 분할 후 부하가 양쪽으로 나뉘었는지 무엇으로 확인하는가?

**답**

우리 실험에서는 **분할 이후 관측 구간에서 두 자식 Tablet의 요청량과 자원 사용량**을 같은 구간 기준으로 비교한다. 분할 결정에 쓴 구간이 아니라 그다음 구간을 본다([design.md](../design.md) §6).

```text
분할 전: 1,000 QPS

분할 후 예시:
A1 500 QPS / A2 500 QPS → 고르게 분산
A1 950 QPS / A2  50 QPS → 한쪽에 계속 집중
```

요청 비용이 다를 수 있으므로 이후에는 CPU·SSD IOPS·queue delay도 함께 확인한다. 처리 완료 건수만 보면 거절되거나 대기 중인 요청을 놓칠 수 있어, 수신 요청량과 성공·실패도 구분한다.

전체 처리량 증가와 p99 감소는 성능 개선의 증거지만, 그것만으로 양쪽 부하가 균등해졌다고 판단하지 않는다.

**근거**

[AWS Part 2](https://aws.amazon.com/blogs/database/part-2-scaling-dynamodb-how-partitions-hot-keys-and-split-for-heat-impact-performance/)의 **Querying test: On-demand table, one partition key value**, 특히 **Figure 2**에서 분할 이후 전체 처리량이 두 배로 증가한 사례를 보여준다.

다만 해당 그래프는 자식 partition별 부하를 직접 보여주지는 않는다. 위의 Tablet별 측정 방법은 **우리 프로젝트의 검증안**이다.

### 3. 분할 효과가 없는 접근 패턴은 무엇인가?

**답**

대표적으로 다음 두 패턴이 있다.

**① 단일 항목에 요청이 집중되는 경우**

```text
user:1 → 900 QPS
나머지 → 100 QPS
```

분할해도 `user:1`은 한쪽에 남는다. 주변 항목을 분리할 수는 있지만, 해당 항목 자체의 요청량은 나뉘지 않는다.

**② 요청이 순차적으로 이동하거나 증가하는 키의 끝부분에 집중되는 경우**

```text
분할 후:
과거 구간 → 새 쓰기 적음
최신 구간 → 새 쓰기 계속 집중
```

분할 이후에도 한쪽이 계속 뜨거워져 효과가 제한된다.

DynamoDB는 이런 접근 패턴을 감지해 분할을 피한다고 설명한다. 다만 구체적인 감지 임계값과 판단 절차는 자료만으로 확정할 수 없다.

**근거**

[DynamoDB 논문](https://www.usenix.org/system/files/atc22-elhemali.pdf)의 **§4.4 Splitting for consumption**에서 단일 항목 집중과 순차 접근을 예로 든다.

[AWS Part 2](https://aws.amazon.com/blogs/database/part-2-scaling-dynamodb-how-partitions-hot-keys-and-split-for-heat-impact-performance/)의 두 곳에서 설명한다.

- **Querying test: On-demand table, one partition key value:** 데이터를 적재할 때 계속 증가하는 sort key로 쓰면, split for heat가 이 "steadily increasing line of heat"를 감지해 분할하지 않는다. 분할해도 새 쓰기가 모두 두 번째 partition으로 가기 때문이다.
- **Query test: Summary:** 계속 증가하는 sort key에 쓰는 경우처럼 분할이 이득이 없다고 판단되면 분할하지 않는다고 정리한다.

### 추가 확인 항목

| 항목 | 현재 확인한 내용 | 추가로 확인할 내용 |
| --- | --- | --- |
| 분할 조건 | 소비 처리량이 일정 임계값을 넘으면 분할 | 정확한 임계값과 지속 시간 |
| 관측 정보 | 관측한 key 분포를 접근 패턴의 대리 지표로 사용 | 집계·샘플링 방식과 관측 기간 |
| 분할점 선택 | 관측 분포를 고려해 선택 | 정확한 계산식과 후보 선택 절차 |
| 분할 보류 | 단일 항목 집중·순차 접근 등의 패턴 감지 | 구체적인 감지 규칙 |

## 프로젝트 적용

공개된 아이디어를 참고한 실험용 variant를 `split-for-heat`로 구현한다.

### 적용할 내용

- 관측한 부하 분포를 이용한 분할점 선택
- 분할 효과가 작은 패턴의 감지와 분할 보류

### 직접 설계하고 기록할 내용

- 초기 부하 지표로 사용할 QPS의 측정 기준
- 관측 시간과 과부하 임계값
- 분할 후보와 예상 부하 계산 방식
- 분할 보류 조건

예를 들어 **누적 요청량이 절반에 가까워지는 위치**를 분할 후보로 삼을 수 있다. 이는 프로젝트의 구현안이며 AWS 내부 알고리즘으로 단정하지 않는다.

비교 실험과 불변식은 프로젝트의 `design.md` §5·§6을 따른다. key 범위 가운데 분할(Baseline)과 비교하고, 여러 key 집중과 단일 key 집중을 구분해 검증한다.
