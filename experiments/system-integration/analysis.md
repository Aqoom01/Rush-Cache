> 이 문서는 A2 병합 전 가상 fixture의 범위·기록이다. 최신 A2 포함 실제 TCP 통합 준비는 [EC2 실행 안내](ec2/README.md)를 따른다. 아래 과거 결과를 새 실제 서버 검증 결과로 사용하지 않는다.

# 통합 실험 결과와 해석 — 2026-11-12

**통합 상태에서 방법론의 효과가 달라지는 것을 확인했다. 현재 실행 범위의 우선 후보는 hotspot에서 PB + HashRange + Heat split + Hysteresis다. 지연 변동을 고려하면 A3는 Lifeguard가 필요했고, B3 copy 방식은 아직 확정할 근거가 약하다.** A2 미구현 및 미연결 방법론이 있어 여섯 모듈 전체의 최선 조합을 확정한 결과는 아니다.

## 실행한 범위

| 실행 | 전체 조합 탐색 | 선택·이웃 독립 seed 검증 | 노드 KV 서비스 가정 |
|---|---:|---:|---:|
| [full-20261112-02](results/full-20261112-02/results.md) | 120조합 × 4시나리오 × 30 seed = 14,400회 | 1,020회, 조합당 30개 새 seed | 연산당 4ms |
| [fast-node-20261112-02](results/fast-node-20261112-02/results.md) | 120조합 × 4시나리오 × 3 seed = 1,440회 | 170회, 조합당 5개 새 seed | 연산당 1ms |
| 합계 | 15,840회 | 1,190회 | 총 **17,030회** |

시간은 virtual, byte는 model-estimated다. 실제 core 알고리즘을 실행했지만 TCP/SSD/CPU 실측은 아니다. 정상·hotspot·이동 중 리더 crash·지연 변동 네 조건, 고정 2개 복제 그룹, 읽기 65%/쓰기 35%, 약 256B 값에서 얻었다. 구현·모형·미지원 범위는 [README](README.md)를 따른다.

아래 후보는 모든 탐색 seed에서 검사한 정합성·오판·이동 완료 조건을 통과한 조합 중 goodput 평균으로 선택했다. 독립 검증에서 선택 조합과 한 옵션만 다른 이웃을 비교했고, 네 후보 모두 30/30회 조건을 통과했다. 이웃에서 평균 goodput이 더 큰 조합은 관측되지 않았다. 통계적으로 모든 이웃보다 우수하다는 뜻은 아니다.

## 시나리오별 후보

| 조건 | 복제 | 감지 | 분배 | 분할 | 배치 | copy 대표값 | 독립 검증 goodput 평균 ± 95% CI |
|---|---|---|---|---|---|---|---:|
| 균등 부하 | PB | SWIM | FixedSlot | 없음 | Hysteresis | Stop | 286.53 ± 0.77 virtual req/s |
| Hotspot | PB | SWIM | HashRange | Heat | Hysteresis | Stop | 263.62 ± 1.07 |
| Hotspot+이동 중 crash | PB | SWIM | HashRange | Heat | Hysteresis | Stop | 259.18 ± 1.07 |
| Hotspot+지연 변동 | PB | Lifeguard | HashRange | Heat | CostAware | Pre | 259.07 ± 1.14 |

SWIM/Lifeguard가 정상 조건에서 같은 성능을 낸 조합도 있다. 동률의 대표 ID 선택은 우열 근거가 아니다. copy 대표값도 평균 기반 선택일 뿐, 아래 짝비교에서는 우열을 확정하지 못했다.

## 방법론별 판단과 이유

### A1: 현재 구현·부하에서는 PB 우세

독립 seed에서 다른 옵션을 고정한 PB→Raft 변경은 goodput을 균등 부하에서 **120.24 ± 0.72**, hotspot에서 **104.02 ± 1.04 virtual req/s** 낮췄다.

이 저장소의 PB는 primary에서 로컬 읽기를 처리하고, Raft는 읽기도 로그에 기록하며 선행 로그 적용 중에는 새 요청을 BUSY로 돌려준다. 모든 로컬 KV 연산에 같은 서비스 시간을 부여했을 때 이 구현 차이가 크게 나타난다. Raft의 분단 안전성·선출 보장과 PB의 crash-only 전제는 다르다. 이 결과를 Raft 일반의 성능 열세나 PB의 운영 적합성 확정으로 해석하지 않는다.

### A2: 선정 불가

현재 체크아웃의 `IoniaEngine.execute`는 `NOT_IMPLEMENTED`를 반환하고 peer/timer/log 주요 경로도 미구현이다. 완성 구현의 위치를 확인하고 adapter를 연결해야 한다. A1/A2를 동시에 켠 여섯 축의 독립 곱으로 계산하지 않았다.

### A3: 지연 변동을 포함하면 Lifeguard 우선

full 탐색의 지연 변동 조건에서:

| 감지 | 실행 수 | 검사 조건 통과 | 살아 있는 노드의 DEAD 오판이 있었던 실행 |
|---|---:|---:|---:|
| SWIM | 1,800 | 0 | 1,800 |
| Lifeguard | 1,800 | 1,800 | 0 |

단일 crash에서는 보정의 대가도 있다. hotspot 후보의 SWIM→Lifeguard 변경은 독립 seed에서 goodput을 **3.75 ± 0.52 virtual req/s** 낮췄다. 따라서 감지 속도 하나로 선택하면 지연 변동에서의 오판을 놓친다. 정상·crash·지연 변동을 모두 견딜 기본 후보는 Lifeguard지만, 다른 FD Lab 방법론과 비교한 결론은 아니다.

### B1: 균등 부하와 hotspot의 후보가 다르다

균등 부하에서는 FixedSlot+분할 없음이 대표 후보였다. hotspot에서는 HashRange의 split 지원을 B2와 함께 사용하는 조합이 유리했다. FixedSlot+Heat는 지원되지 않으므로 B1의 분할 가능성과 B2의 정책을 독립적으로 선택할 수 없다.

두 분배 방식만 비교했고 routing CPU·metadata 전파 비용은 실측하지 않았다. ring/tag/resizable/key-order까지 포함한 B1 최종 선정은 남아 있다.

### B2: 분할과 이동 비용을 함께 고려해야 한다

hotspot에서 PB/SWIM/HashRange/Hysteresis/Stop을 고정하고 NONE→Heat만 바꿨을 때 독립 seed의 변화는 다음과 같다.

- goodput **+12.38 ± 0.68 virtual req/s**
- p99 평균 **−1.67ms**
- 이동 byte 평균 **−9,841B**
- freeze 평균 **−383.07ms**

분할로 hot 부하를 나눌 수 있었고, 실제로 옮기는 데이터와 목적지 재복제 비용도 줄었다. 단순 분할 계산 시간만으로 판단하면 이 이득을 볼 수 없다.

배치 정책의 차이는 통합의 목적을 더 직접적으로 보여준다. 아래는 다른 옵션을 고정한 독립 seed 30회 평균이다.

| 조건 | 배치 | 완료 이동 수 | freeze 합계 | goodput |
|---|---|---:|---:|---:|
| 균등/PB/SWIM/Slot/None/Stop | Greedy | 2.80 | 419.67ms | 282.03 |
| 같은 조건 | Hysteresis | 0.00 | 0.00ms | 286.53 |
| Hotspot/PB/SWIM/Range/Heat/Stop | Greedy | 4.50 | 631.20ms | 260.58 |
| 같은 조건 | Hysteresis | 1.00 | 67.10ms | 263.62 |

Hysteresis의 goodput 개선은 균등에서 **+4.50 ± 1.23**, hotspot에서 **+3.03 ± 0.53**이었다. 부하를 적극적으로 다시 배치하는 정책이 이동·복제 비용을 포함한 시스템에서 항상 유리하지는 않다는 근거다. 지연 변동에서는 CostAware가 대표 후보가 됐다.

EStore는 tablet 내부 평균 대비 hot key를 찾는다. 현재처럼 작은 한 hash 구간에 여러 key가 비슷한 강도로 집중되는 부하와, 단일 key/Zipf/이동 hotspot은 다르다. 이번 결과만으로 EStore 자체를 배제하지 않는다.

### B3 copy: 현재는 보류

다른 옵션을 고정한 독립 seed의 Stop→Pre 변경:

| 조건 | goodput 차이 ± 95% CI | freeze 차이 |
|---|---:|---:|
| hotspot 후보 | −0.0278 ± 0.0368 | +3.63ms |
| 지연 변동 후보 | +0.0722 ± 0.0795 | −1.90ms |

두 CI 모두 0을 포함한다. 이번 데이터 크기에서는 copy 방식 자체의 작은 차이보다 목적지에서 순차 SET을 복제·확정하는 구간의 영향이 크다는 모형상의 설명과 일치한다. 1ms 노드 민감도에서는 지연 변동의 평균 최고 copy가 Pre→Stop으로 바뀌었다. 현재 copy 선정은 안정적인 결정 근거가 부족하다.

큰 tablet·큰 값·높은 dirty rate·느린 네트워크에서의 반복, 실제 목적지 bootstrap, post-copy의 pull·source 실패 경로까지 비교해야 한다. 증감 판단·병렬 scheduler·drain은 이번 실행에 연결하지 않았다.

## 검증과 한계

- 최종 source 기준 core Java와 전체 core test source를 JDK 21의 `-Xlint:all -Werror`로 컴파일했다.
- 신규 통합 테스트 **7/7 통과**: 지원/거부 조합, 실제 두 copy 이동과 목적지 replica 값 보존, crash 중 이동 복구, 같은 요청열·동일 결과 재현, oracle의 유실/불확정 쓰기 구분, crash 시 queued KV 중단, CI 계산.
- JUnit Platform으로 core 전체 **956개 발견, 944개 성공, 조건부 중단 12개, 실패 0개**. classpath에 기존 test resources를 포함했다. 전체 Gradle build 성공을 뜻하지 않는다.
- Gradle build는 이 환경의 `Could not determine a usable wildcard IP for this machine`으로 시작하지 못했다. 초기 기본 캐시 접근 확대 요청도 자동 승인 검토 도구 오류로 실행되지 않았다. 쓰기 가능한 임시 캐시와 JDK 직접 실행 경로를 사용했다.
- `verify_results.py`로 두 최종 cohort의 JSON↔CSV, 요청 합계, safe 판정, aggregate 평균, 한 축만 바꾼 paired delta, 요청열, source/config hash를 독립 대조해 통과했다.
- 최종 본 실험 14,400회에서 관측한 유실과 stale read는 0이었다. 지연 변동의 SWIM 오판과 그에 따른 이동 미완료를 통과 결과에 섞지 않았다. 오판은 곧 데이터 유실이라는 뜻도 아니다.
- 다중 비교 보정 없는 탐색이며 SLO/자원 비용의 최종 목적함수도 아직 없다. 선택 조합의 독립 seed 이웃 검증과 실서버 최적성은 다른 주장이다.
- 내부 초기 모형과 crash queue 보정 전 결과는 `runs/`의 Git 제외 경로에 보존했다. 이 문서는 `*-20261112-02` 최종 source 결과만 사용한다.

현재 다음 단계는 **A2 완성 구현 확보 → 미연결 방법론 adapter 확대 → 동일 보장/SLO를 만족하는 후보 추림 → 실제 TCP/SSD에서 최종 후보 검증**이다. 이 단계가 끝나기 전에는 “a1~b3 전체에서 각자 가장 좋은 방법론”을 확정하지 않는다.
