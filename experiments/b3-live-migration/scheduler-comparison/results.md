# 선행 Scheduler 비교 결과

2026-11-04. **정식 855회·10,260개 job을 모두 완료했다.** 데이터 exact match·소유권/epoch·source 정리·guard 보존·매 tick 예산·동시 상한 검증에서 위반은 0건이다. 기존 M1~M5의 FIFO 조건 변화 실험과 별도다.

공식 run: `run-2026-11-04T04-25-42.840856800Z-edbb9e21`.

- [171조건 전체 보고서](runs/run-2026-11-04T04-25-42.840856800Z-edbb9e21/repeat-001/results.md)
- [조건별 CSV](runs/run-2026-11-04T04-25-42.840856800Z-edbb9e21/repeat-001/summary.csv)
- [방법·원문·원문 대비 변경 범위](README.md)

고정 계획의 **9개 정책 × 6조건 × 세 복사 방식 × seed 5개 = 810회**, CAMIG의 별도 목적지 선택 비교 **3개 계획 × 세 방식 × seed 5개 = 45회**다. 실제 Java 이동 프로토콜을 호출하는 endpoint-only 인메모리·가상 시간 연구 구현이며 원 논문 시스템·MongoDB·TiDB 제품 자체의 성능 재현은 아니다.

원 논문 5편의 기본 모델은 Pre-copy다. 이번 855회는 **Pre-copy에서 선행 핵심을 이식한 비교 + Stop/Post-copy로 확장한 교차 비교**이며, 각 copy를 **285회씩** 실행했다. 원문의 copy와 DB의 실제 동기화 절차, 방법별 변형은 [실험 범위](README.md)를 따른다.

## 세 copy 방식의 같은 조건 비교

교차 경합 조건에서 seed 5개의 평균 전체 완료 시간이다. 단위는 논리 tick이다.

| Scheduler | Stop-copy | Pre-copy | Post-copy |
|---|---:|---:|---:|
| FIFO | 384.0 | 446.4 | 403.2 |
| MongoDB식 노드당 1개 | 379.8 | 414.8 | 413.8 |
| TiDB token 변형 | 382.2 | 452.2 | 403.0 |
| TiDB AIMD 피드백 변형 | 382.2 | 452.8 | 403.4 |
| iter-GWIN | 383.0 | 420.0 | 417.2 |
| single-MCs | 378.2 | 414.4 | 412.4 |
| SLAMIG γ=0 변형 | 381.6 | 417.8 | 415.8 |
| VMbuddies 배분 변형 | 543.0 | 645.8 | 588.8 |
| FPTA 단일 경로 배분 | 399.8 | 468.4 | 423.2 |

완료 시간만으로 세 copy 방식의 우열을 정하지 않는다. Stop-copy의 쓰기 차단·거절, Pre-copy의 재전송, Post-copy의 미도착 read BUSY도 함께 비교한다. 전체 171조건 보고서는 세 방식의 요청 영향과 전송량을 모두 포함한다.

## Pre-copy에서 보인 차이

아래는 각 조건의 seed 5개 평균이다. 전체 완료 시간 단위는 tick, 기한 초과는 12 job 중 평균 건수다. 세 열은 서로 다른 조건이며 조건을 섞어 하나의 종합 순위를 만들지 않았다.

| 방법 | 교차 경합: 전체 완료 tick | source 공유: 전체 완료 tick | deadline: 기한 초과 jobs |
|---|---:|---:|---:|
| FIFO 기준선 | 446.4 | 3,625.4 | 3.8 |
| MongoDB식 노드당 1개 | 414.8 | 1,114.8 | 3.0 |
| TiDB token 변형 | 452.2 | 3,779.0 | 3.8 |
| TiDB에서 착안한 AIMD 피드백 | 452.8 | 3,639.6 | 3.8 |
| iter-GWIN | 420.0 | 1,114.8 | 3.0 |
| single-MCs | 414.4 | 1,114.8 | 3.0 |
| SLAMIG γ=0 변형 | 417.8 | 1,076.8 | 3.0 |
| VMbuddies 배분 변형 | 645.8 | 1,935.0 | 7.6 |
| FPTA 단일 경로 배분 | 468.4 | 1,916.0 | 3.0 |

**공유 노드에서는 동시 이동을 무조건 늘리는 것이 불리했다.** source 공유·rho=0.5의 FIFO 동시 4개는 byte 예산을 나누면서 초기 데이터의 4.1815배를 전송했다. 같은 입력에서 MongoDB식 동시 1개는 1.2194배, 전체 완료는 약 69.3% 감소했다. write BUSY도 평균 19,127.2→323.8회였다. GWIN/MCs가 같은 결과를 낸 것은 이 조건에서 모두 source 하나 때문에 직렬 실행되고 비긴급 가중치가 같아 순서도 같기 때문이다.

**교차 경합에서는 single-MCs가 전체 완료 시간을 FIFO 대비 약 7.2% 줄였다.** 반면 평균 job 완료 시간은 SLAMIG γ=0 변형이 243.33→164.02 tick, iter-GWIN이 183.85 tick이었다. 전체 완료 시간과 평균 job 응답 시간을 구별해 판단해야 한다.

**deadline 조건에서도 모든 기한을 만족한 방법은 없었다.** FIFO 평균 3.8건에 비해 GWIN·MCs·MongoDB식·SLAMIG 변형·FPTA는 3.0건이었다. VMbuddies 변형은 7.6건으로 악화됐다. 이번 배분 변형의 예측 horizon·EWMA·남은 데이터 추정 및 비동기 Tablet cutover가 포함된 결과로, 원 논문의 VM 동기화 시스템 전체에 대한 결론은 아니다.

**원 논문 이름만으로 성능 향상이 보장되지는 않았다.** TiDB 변형은 이 조건에서 완료 시간을 개선하지 못했다. 실제 TiKV의 snapshot pending·I/O 비용 대신 job 수를 피드백으로 사용한 모델이므로 실제 서버 보호 효과를 이 수치로 판정하지 않는다. FPTA도 source 공유에서는 개선됐지만 교차 경합에서는 FIFO보다 느렸다. 단일 경로·FIFO 4 slot의 할당 stage 비교이며 원 논문의 전체 계획·경로 최적화를 평가한 것이 아니다.

## CAMIG — 별도 목적지 선택 비교

같은 12 Tablet과 같은 두 목적지 후보 집합에서 계획을 고른 뒤 모두 같은 MongoDB식 실행기로 실행했다. 아래는 Pre-copy의 seed 평균이다. `candidate-balanced`는 계획된 destination bytes가 작은 후보를 고르는 기준선이다.

| 계획 선택 | 전체 완료 tick | 평균 job 완료 tick | 초기 대비 전송 비율 |
|---|---:|---:|---:|
| 첫 후보 선택 | 1,114.8 | 696.53 | 1.2194 |
| destination bytes 균등 선택 | 631.8 | 261.45 | 1.3375 |
| CAMIG MIGC 기반 선택 | 631.8 | 231.20 | 1.3393 |

CAMIG는 첫 후보만 고르는 기준선보다 빨랐지만, **데이터량 균등 기준선과 전체 완료 시간 평균은 같았다.** 평균 job 완료 시간은 약 11.6% 줄었고 전송 비율은 소폭 증가했다. 같은 목적함수 개선을 주는 후보만 있다는 제한된 설정이며 CPU 부하 균형·에너지 절감·다른 B1/B2 배치 정책보다 우수하다는 결과는 아니다. 고정 `Change` 목록의 Scheduler 비교 순위에 이 수치를 섞지 않는다.

## 검증·재현

- 전체 빌드·테스트 633개, 실패/오류/건너뜀 0. JDK-only production 의존성 검사 통과.
- 정식 855개 JSON과 10,260 jobs 모두 검증. 누락 job을 제외해 완료율을 계산하지 않았다.
- quick 공식 확인 run은 `run-2026-11-04T04-30-51.430495500Z-cf843b8a`이며 `cache-core/build/b3-scheduler-quick/`에 보관한다. 342회×2반복=684회·8,208 jobs를 완료했다. 두 반복의 summary/results/jobs/budgets/rounds/paired 6개 파일이 byte 단위로 같다. 실제 planner 시간은 반복 간 달라 timings.csv를 반복 일치 비교에서 제외했다.
- full/quick 세 반복 모두 JSON에서 7개 파생 파일을 다시 생성해 기존 SHA-256과 일치했다. timings.csv도 같은 원본에서의 재생성은 일치했다.
- 요청 시도 생성 규격과 inputHash는 scenario/seed마다 모든 방법·복사 방식에 공통 적용했다. 성공 요청 수·BUSY·종료 시각은 달라 각자의 성공 요청 oracle로 비교했다.
- JSON 재검증은 lifecycle·실전송 총합·매 tick 전체/노드 예산·동시 상한·TiDB token 사용·선택 계획·phase/round·집계 일치를 확인한다. checksum을 다시 계산해도 집계 위조·예산 초과·변경 계획은 거절한다.
- planner 실제 nanos는 `timings.csv`에 분리한다. 같은 JVM의 참고 관측이며 JIT/CPU가 통제된 benchmark로 해석하거나 논리 시간과 합치지 않는다.

| 실행 코드 기록 | SHA-256 |
|---|---|
| source (`sha256-lf-text-v1`) | `93c89a9014fb015adb7f306679d497c9df0c19fe7cbea83107062be16d6d0db1` |
| backend classes | `aabb6c462430b46e17cd23c7773055c0faac2d2ab66b3832438c93bddb76d63d` |
| runner classes | `7b916db90bf405baba2b65076de5e4a505d03b667418f1f6c835b341540f6c66` |
| full summary.csv | `746d69f778d8a0ac5db83e20d30e8ab2899683675ff81376b34f6e2a2e610d46` |
| full results.md | `5b28d336084ea1e481a739dc2817e484237e10e42aff6e66c8b60322dd4e09b1` |
| quick summary.csv (두 반복) | `23f1d86d9b1394838b78d323b513ae3639bf90c2817883cdd51c024a164a0f7b` |
| quick results.md (두 반복) | `d5441b08e10675b803e13daed4a34bb25a7b6ab89bf3b27a4393bc526b11f0d6` |

HEAD `4a9509f` 위의 미커밋 구현으로 실행했다. HEAD만으로 실행 코드를 식별하지 말고 위 source/backend hash와 함께 판단한다. JSON·manifest·로그·상세 CSV는 실행 PC에 남고 Git에는 full 반복의 summary.csv/results.md를 보관한다. 전체 조건과 Stop/Post-copy 결과는 위 공식 보고서에 있다.

실험 이후 해당 구현을 **`4b5e4ba`**에 커밋했다. 이 커밋이 위 Scheduler 실험의 코드 스냅샷이며, 이후 새 원격 작업 브랜치의 최신 변경(`7ec7efe`)을 병합해도 실험 당시 source/backend hash와 원본 실행 HEAD는 그대로 보존한다.

원격 변경 통합 뒤에도 전체 빌드를 확인했다. 테스트 672개 중 669개 통과·실패/오류 0이며, 3개는 B2의 분할 보류 시 불변식 검사가 해당되지 않아 조건부로 건너뛴 항목이다. B3 이동 코드와 공식 실행 요약은 통합 과정에서 변경하지 않았다.
