# E-Store — hot 데이터 식별과 두 단계 분할

## 출처

[E-Store: Fine-Grained Elastic Partitioning for Distributed Transaction Processing Systems](https://www.vldb.org/pvldb/vol8/p245-taft.pdf)

- PVLDB 8(3), 2014 (VLDB 2015 발표), Taft et al.
- 읽을 부분: 시스템 구성(E-Monitor, E-Planner), 두 단계(two-tiered) 분할, 재배치 계획 알고리즘

## 논문에서 가져온 것

- **hot 데이터를 따로 다룬다.** 접근이 많은 소수의 tuple(hot)을 찾아 개별로 배치하고, 나머지(cold)는 큰 블록 단위로 다룬다(두 단계 분할).
- **과부하일 때 hot을 먼저 옮긴다.** 재배치 계획은 hot tuple을 먼저 정하고, 그다음 cold 블록으로 균형을 맞춘다.

세부 기준(hot을 몇 개로 볼지, 감시 주기, 계획 알고리즘별 차이)은 이 문서에 옮기지 않았다. 구현이 그 세부를 따르지 않기 때문이다. 논문의 정확한 기준값을 인용해야 하면 원문을 다시 확인한다.

## 이 프로젝트에서 자체 결정한 것

| 항목 | 이 구현 |
|---|---|
| 단위 | tuple이 아니라 key 해시(분할)와 Tablet(배치) |
| hot 판정 | 요청 수가 평균의 2배 이상(분할: key 평균, 배치: Tablet 평균) |
| `estore-inspired` 분할 | 해시 순서로 이어진 hot key 묶음 중 가장 뜨거운 것의 경계에서 한 번 자른다. hot이 없으면 `NO_HOT_DATA`로 보류 |
| `estore-inspired` 배치 | 과부하 노드(평균 이용률 × 1.05 초과)에서 hot Tablet을 먼저, 그래도 넘치면 cold Tablet을 가장 한가한 노드로 옮긴다 |

`SplitPolicy`는 한 번에 한 번만 자르므로 hot key 하나를 떼어 내려면 두 번 잘라야 한다. 그래서 E-Store의 "hot을 개별로" 효과는 반복 분할(Benchmark C)에서만 나타난다.

## 결과에서 본 것 ([../policy-benchmark.md](../policy-benchmark.md))

- 단일 hot key(30%)에서 분할과 배치를 함께 쓴 조합이 가장 균형이 좋았다(Benchmark C, Baseline 대비 +3.9%).
- key당 요청이 적은 균등 부하에서는 우연히 평균의 2배를 넘은 key 묶음을 hot으로 잘못 잡아 엉뚱한 곳을 자른다(Benchmark A S1, −45%). hot 판정 배수 하나로는 잡음과 실제 hot을 구분하지 못한다.
