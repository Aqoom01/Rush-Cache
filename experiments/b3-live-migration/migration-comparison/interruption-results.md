# S6 취소·장애 비교 결과

2026-10-28. 데이터를 옮기다가 취소하거나 원래 노드에 접근하지 못해도 성공한 쓰기·삭제와 소유권 규칙이 유지되는지 확인했다. 이번 보강에서는 변경분을 일부 적용·ACK한 뒤 같은 라운드 도중 취소하는 조건을 추가했다.

공식 run은 [전체 결과표](runs/run-2026-10-28T08-37-04.980049Z-13962040/repeat-001/results.md)이며 [조건별 CSV](runs/run-2026-10-28T08-37-04.980049Z-13962040/repeat-001/summary.csv), [실행 설정](interruption.properties), [실행·재생성 안내](README.md)를 함께 사용한다.

## 무엇을 확인했나

| 상황 | 검증 횟수 | 실제 결과 |
|---|---:|---|
| 소유권을 넘기기 전 취소 | 45회 | 취소 완료(ABORTED). 원래 노드의 성공한 쓰기·삭제를 보존하고 임시 복사본을 폐기했다 |
| 소유권을 넘긴 직후 취소 시도 | 15회 | 취소 거부. 새 노드 데이터를 유지하며 이동 완료(DONE) |
| 원래 노드의 데이터 정리가 일부 실패한 뒤 취소 시도 | 15회 | 취소 거부. 두 번의 정리 실패 뒤 세 번째 정리에서 이동 완료(DONE) |
| post-copy의 source 접근 불가·pull timeout 후 복구 | 10회 | 장애 중 새 노드의 쓰기·삭제 보존. 같은 데이터·fence 복구 후 이동 완료(DONE) |
| post-copy의 source를 복구하지 않음 | 5회 | FAILED 유지. 미도착 데이터를 다 가져온 것처럼 완료하지 않았고 source·성공 쓰기·소유권을 보존했다 |

실행한 90개 시나리오에서 정합성 위반은 0건이다. 취소된 45회는 source 전체, 완료한 40회는 destination 전체가 각 실행의 성공 요청 oracle과 일치했다. 미복구 5회는 source 전체 보존과 destination의 가용 데이터 전체 일치를 검증했다. 이 5회를 전체 데이터 이동 성공으로 세지 않는다.

## 추가한 변경분 복사 도중 취소

기존 CATCHING_UP_ABORT는 delta 라운드에 들어가자마자 취소한다. 새 CATCHING_UP_PARTIAL_ABORT는 마지막이 아닌 live delta batch가 destination에 적용·ACK된 뒤, 해당 라운드가 끝나기 전에 취소한다.

| 확인 항목 | 정식 seed 5개 결과 |
|---|---|
| 취소 직전 상태 | 모두 CATCHING_UP |
| snapshot 이후 전송한 delta | 모두 65,536 bytes |
| 적용·ACK된 delta record | 515~550개 |
| 소유권 전환 | 모두 0회 |
| 취소 후 source 전체 데이터 비교 | 5/5 일치 |
| 임시 staging 폐기와 source 쓰기 재개 | 모두 통과 |

부분 전송 상태를 확실히 관측하도록 이 조건의 batch 목표만 full 32 KiB·quick 2 KiB로 설정한다. 원본 condition/modelConfig와 경계 inputHash에 기록하며 다른 조건은 기존 64 KiB를 유지한다. 위 bytes는 delta framing과 진행 중 전송을 포함한다. ACK record 수로 실제 적용된 변경분이 있음을 별도로 확인한다. 같은 라운드의 마지막 batch 전이라는 증거와 delta 시작·주입 이벤트가 없으면 보고서 생성을 거부한다.

## 실험 조건과 구분

- 데이터 10,000개, key 16 bytes·value 128 bytes, 전송 예산 64 KiB/tick
- 균등 요청, rho=0.5, 쓰기 중 DELETE 30%, pre-copy 임계 256 KiB·상한 10회
- extended-full은 10경계 × seed 5개 × 세 방식 = 150개 JSON 행: 실제 실행 90개·미지원 60개
- extended-quick은 데이터 1,000개·예산 4 KiB/tick·임계 1 KiB로 줄인 재현 검증용이며 150행 묶음을 두 번 실행한다

미지원은 실패가 아니다. CATCHING_UP은 pre-copy 전용이고 post-copy에는 소유권 전환 전 전체 복사의 final ACK 대기가 없다. 적용되지 않는 행에는 이유를 기록하고 숫자·차이값을 채우지 않는다. post-copy 장애 중 미도착 GET은 UNAVAILABLE이며 정상 MISS로 바꾸지 않는다.

세부 표의 시간·전송량은 seed 범위, 취소 거부·UNAVAILABLE는 seed 합계다. 취소 후 0 tick은 같은 controller 작업에서 취소·재개한 모델 결과이며 실제 서버 0 ms 복구를 뜻하지 않는다.

## 재현·검증

- 전체 빌드·테스트 334개 통과. 부분 전송 후 취소를 full/quick의 seed 1~5에서 검사하고 ACK 증거·전송량 변조를 거부한다.
- 정식 실행 90회와 미지원 60행을 검증했다. 축소 두 반복의 실제 실행 180회와 미지원 120행도 검증했고 두 summary.csv hash가 일치했다.
- quick 실행 ID: run-2026-10-28T08-37-04.981051Z-cb3a28a7. 두 summary.csv SHA-256: AEF9B9CFFEAC9C7F98BE990F92528B87F14F94F042F8E534CD8E0F565BADD2CA
- 정식 JSON에서 CSV·Markdown을 재생성해 파일 hash가 이전과 같음을 확인했다.
- full summary.csv SHA-256: 995E4E0BD3F29FC79E4C379B70A1F93262A3157D233658921CE098C00857B80A
- full results.md SHA-256: 074C6C1A0034D14E9FC73FD18266596F82AD869ADAA27EB63D3B6AABBA28F680
- 실행 기준 commit은 b5b29ec이며 미커밋 중간 취소 보강을 포함한다. source hash는 ed9c3cce841a8603d6e833cd3a282c6550c467ef3a8664b8f8ff7e005154489e, backend class hash는 38a9e4a9393a2a98a26229887bf6abe34c9c2b2c376b5ca0bbe707a3cd676076이다.
- 원본 150개와 manifest·실행 로그는 공식 run 폴더에 로컬 보관한다. Git에는 반복별 summary.csv·results.md를 보관한다. quick 원본은 cache-core/build/b3-interruption-quick 아래에 있다.
- 기존 9경계의 full/quick profile과 원본은 보존한다. 이전 [135행 run](runs/run-2026-10-28T01-48-52.869484600Z-bc8ad164/repeat-001/results.md)의 JSON을 별도 검증 폴더에서 재생성해 summary.csv가 이전 SHA-256 A1217CE8262BC6ED213AAD14F4A156D60830BA3A3E148F7B026BD54CAA719530과 같음을 확인했다. 과거 run에 새 조건을 덧붙이지 않는다.

## 해석 범위

같은 seed에서 모든 경계·방식은 동일 초기 데이터와 tick별 요청 시도를 재생하지만 주입 시점·완료 시점·성공 요청 수는 다르다. workloadHash와 inputHash는 생성·주입 규격 hash이며 실제 trace 전체 hash가 아니다. 부분 전송 조건의 batch 설정은 경계 hash에 포함하며 성능 비교 조건으로 섞지 않는다.

이 실험은 실제 protocol을 호출하는 단일 controller·인메모리·가상 시간 모델이다. source 장애는 데이터를 보존한 접근 불가이며 물리적 영구 손실의 자동 복구·실제 네트워크 지연을 측정한 실험은 아니다. S6 전용 b3-s6-json-1과 별도 실행 profile로 기존 결과를 구분한다. dirty generation별 상세 v2, E/M scaling, 실제 KV/peer 통합은 후속이다.
