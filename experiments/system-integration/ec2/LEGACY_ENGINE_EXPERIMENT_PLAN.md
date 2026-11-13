> 과거 A1 **또는** A2 엔진 실험용 문서. 현재 동시 통합 경로는 README.md를 따른다.

# 전체 통합 runtime의 실험 계획

2026-11-12. 현재 구현과 기능 검증은 [전체 범위](../FULL_SYSTEM_SCOPE.md), [VALIDATION](VALIDATION.md)을 따른다. **EC2 실행 전 계획이다.** 방법론별 독립 순위와 통합 순위가 달라지는지, 그 이유가 복제·FD·분배·균형·증감·복사 비용 중 어디에 있는지 확인한다.

## 실험 모집단

복제15(A1 7+A2 8) × FD3 × B1/B2 유효114 상태 × node-count9 × scheduler9 × copy3 × drain2 = **2,493,180개 전략 조합**이다. B1/B2는 RANGE/TAG/ORDER 각각4 split×8 placement, SLOT/RESIZABLE 각각NONE split×8 placement, Ring2종은NONE split/placement다. 하위 VR 모드·Rosé lag·budget과 workload·seed는 별도 축이다. 기준 workload2개·seed3개까지 펼치면 **14,959,080개 실행 입력**이다.

모든 입력을 실행했다는 의미가 아니다. 등록된 어떤 호환 조합도 `plan`으로 실행할 수 있다. `matrix --stage exhaustive --count-only`로 규모를 확인하고, `--allow-large --output`을 명시하면 전수 CSV를 stream으로 생성한다. CSV 생성은 EC2 접속·기동을 하지 않는다.

## 실행 순서와 통과 조건

1. **입력/호환성 검사:** runtime catalog와 Python 선택 목록 일치, topology/parameter/input/JAR/tool hash를 확인한다. 미연결/학습 대기를 원천적 조합 불가로 분류하지 않는다. 실제 제약은 이유를 기록한다.
2. **전략별 기능 coverage:** `smoke`는15 native engine×3 copy의 scale-out/in 입력45개다. `coverage`는 모든 호환 전략 쌍을 덮는 결정적 covering array다. 각 method가 실행되는지와 실제 값·이동·증감/drain을 검사한다. pairwise가 전수 고차 상호작용의 검증을 대신하지 않는다.
3. **EC2 계측/calibration pilot:** 작은 부하로 정확한 process identity·resource·raw 요청·FD·cleanup·clock 관측을 확인한다. 엔진별·활성 그룹 수별·같은 값 크기에서 부하를 올려 calibration을 수집하고, 필요한 과거 trace를 확보한다. queue reference model은 기능 pilot에만 쓰고 실측 calibration으로 명명하지 않는다.
4. **전체 후보의 통합 비교:** 초기부터 A·B를 함께 켠다. AUTO에서 모든 엔진/FD/B1/B2/scaler/scheduler/copy/drain 후보를 포함하는 covering cohort를 동일 seed·부하·key mode로 실행한다. 실험 예산을 줄이면 선택한 입력과 누락 조합을 공개한다. 독립 실험의1등만으로 후보를 제거하지 않는다. 예산이 허용되면 전수 전략 목록을 실행한다.
5. **상호작용 재대조:** 유력 조합과 한 축만 다른 이웃을 반복 측정한다. B2 후보를 고정한 뒤 복제15종 전체를 다시 대조해 초기 선별에 누락된 승자를 확인한다. 스케줄러와 copy는 실제 다중 이동이 일어난 실행에서 비교한다. 결정이 KEEP인 실행과 이동한 실행을 구분한다.
6. **장애/부하 변화:** hotspot 이동·burst·지속 쓰기 중 재배치·scale-out/in, source leader crash와 destination failure, pause를 비교한다. 기본 도구는 source crash/leader·observer pause를 주입한다. destination/network-only/SSD-only injector가 없는 경우 미구현으로 기록하고 수동 개입을 자동 시나리오로 꾸미지 않는다. Post 실패/복구는 foreground 요청과 완료 쓰기 보존을 함께 감사한다.
7. **미사용 seed 검증:** 탐색1/2/3과 별도로101~110, UNIFORM/HOT_RANGE/ZIPF, read/write 비율·값 크기·key 수를 바꾼다. 최적 조합의 적용 범위와 trade-off를 보고한다.

`correctnessValid`, `migrationValid`, `coverageValid`가 구분된다. PSTORE가 TRAINING에 머문 실행, 실제 이동이 없는 scheduler/copy 실행, source crash를 주입하지 못한 실행을 해당 방법의 검증/성능 표본으로 집계하지 않는다. 완료 표시만으로 통과 처리하지 않는다.

## 동일 조건

- 기존 전용 Linux/JDK21 호스트, 같은 AZ·동일 replica 자원·동일 파일 KV/shards/heap으로 시작한다. gateway/load도 같은 자원을 유지한다. 기본은 prepared3그룹×3 replica +gateway+load=11호스트, initial active2그룹이다. idle 예비 그룹의 비용도 수집한다. service scale과 실제 EC2 lifecycle/cost를 분리한다.
- 같은 input/request tape seed·key mode·값·offered rate·concurrency·측정 길이를 사용한다. TAGGED와 ORDERED 입력 cohort를 따로 두어 B1 해시 태그/순서 효과를 실제로 자극한다. 같은 seed라도 동시 실행 순서는 달라질 수 있다.
- 기능 pilot은64 key·256B·500요청/50req/s 등 작은 전수 감사다. 성능 결론을 내리지 않는다. 계측 pilot은1,000 key·1KiB·warmup2,000·측정12,000/200req/s 등으로 시작해 실제 안정 구간·gateway 병목을 확인한다.
- 정상 측정은 warmup30초 이상/측정120초 이상·최소3seed로 확장하고 rate를 단계적으로 올린다. 성공 goodput이 offered rate를 따라가지 못하거나 p99·실패·미발행이 증가하는 포화점을 확인한다. gateway가 포화되면 native 엔진의 고유 성능 우열로 일반화하지 않는다.
- control interval/cooldown·동시 job/endpoint/cluster 예산·copy threshold/round·capacity를 고정한 기준선과 민감도를 나눈다. `scaling.provision.windows`는 기존 정책의 예측/warm-up 모델 입력이며 EC2 생성 지연 실측값이 아니다. 현재 예비 그룹 readiness 비용과 구분한다.
- 장애 pilot은256 logical B/tick·20ms로 이동 창을 늘릴 수 있다. 정상 성능의65,536B/tick·10ms와 섞지 않는다. 실제 copy/source 의존 상태를 관측한 뒤 fault를 주입한다. SIGSTOP은 프로세스 전체 정지다.

## 수집/판정

| 영역 | 주요 근거 |
|---|---|
| 값/요청 | 요청 CSV의 논리 ID·status·예정/완료·retry·hash, 최종 oracle 감사, uncertain·held/quarantine |
| 성능 | 성공/MISS goodput·p50/p95/p99, terminal p99·status별 실패·notAdmitted. 성공만으로 분모를 줄이지 않음 |
| 제어 | 실제 window load/p99/CPU, scaler 결정/학습 상태·이유, B2 split/계획, active/prepared/목표 그룹 수 |
| 이동/축소 | scheduler 시작·배분/논리 소모, freeze·validation·게시·cleanup·실제 drain guard, 미해결/복구 수 |
| 장애 | 소유 PID 종료 확인, FD DEAD/awareness, native view/leader·prefix/apply/ACK, repair 결과와 원본 값 |
| 자원 | 모든 JVM CPU/RSS/heap/GC·I/O·UDP·TCP, gateway 대기/실패, 원천 누락/범위 |
| 재현 | JAR/tool/input/auxiliary hash, Java/OS와 process identity, 각 실행의 request tape |

최종 감사 기본은 최대1,000 key 표본이며 큰 실험은 전수 감사/추가 이력 검사로 보강한다. per-key oracle과 최종 감사가 전체 이력의 선형화 가능성 증명은 아니다. 단일 load clock 지연을 사용하고 호스트 간 nanoTime은 차감하지 않는다. fault→복구 정밀 지연은 clock 동기화 오차를 확인한 뒤 보고한다.

A2 RAM quorum ACK, A1 과반수/전원 ACK, Rosé bounded lag의 보장을 표시한다. Rosé의 허용 손실을 숨기지 않고 strong 보장이 필요한 후보와 같은 안전성 조건의 우승으로 섞지 않는다. 모델 예산을 물리 wire/SSD force bytes로 설명하지 않는다. Node-count의 논리 node-window를 실제 EC2 요금으로 환산하지 않는다.

최종 보고서는 `workload / 전체 조합·하위 설정 / 보장 / goodput·p99·오류 / 이동·복구·사용 그룹 / 자원 / 한 축 변경 대비 차이 / 선택 이유·한계`를 함께 제시한다. 현재 최적 조합을 미리 확정하지 않는다.
