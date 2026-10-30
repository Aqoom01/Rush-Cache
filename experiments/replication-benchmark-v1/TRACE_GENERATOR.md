# 단계 1 — 공통 데이터·요청열 생성기

기준일: 2026-10-30. Java 21 기반의 **논리 요청 생성·hash/export** 기능이다. 실제 client/서버 전송·성능 계측·분산 실행은 다음 단계다. EC2 접속·사전진단은 보류 상태를 유지한다.

## 배치와 재사용

- 구현: experiment-runner/src/benchmark/java/io/openkache/runner/benchmark/
- 검사: experiment-runner/src/benchmarkTest/java/io/openkache/runner/benchmark/
- 기존 Command.Get/Set/Delete와 불변 Bytes를 재사용한다. TCP·Transport·IONIA 상태나 외부 production 라이브러리를 추가하지 않는다.
- 별도 benchmark/benchmarkTest sourceSet과 benchmarkTrace task를 experiment-runner Gradle에 추가했다. check/build는 새 검사도 수행하지만 기존 Main·WorkloadGenerator·application 배포 JAR에는 새 sourceSet을 섞지 않는다.
- 공용 API 확정/타 연구자 인수는 아직 아니다. 독립 생성기 계약을 먼저 검증한 단계다.

## 입력 계약

| 항목 | 계약 |
| --- | --- |
| seed | signed 64-bit 정수. 101/202/303/404/505는 본비교 후보 |
| key.count | 1..Long.MAX_VALUE. 초기 LOAD는 각 key에 한 번씩 SET |
| key.bytes | 16..1,048,576 byte; 현 명세 후보24B |
| value.bytes | 44..1,048,576 byte; 현 명세 후보100B/1024B |
| mix.get/set/delete | 각각0..100 정수, 합계100. LOAD에는 적용하지 않음 |
| stream.count | 양의 int. 실제 thread/host 개수와 별개인 고정 logical stream 수 |
| distribution | UNIFORM 또는 FIXED_HOT만 지원. Zipf/moving-hot/read-latest/insert 확장은 미구현 |
| hot.probability | finite0..1. UNIFORM은0만 허용 |
| warmup.requests/measured.requests | 각 phase 전체의 logical 수, 음수 금지. 0은 로컬 형식·경계 검사에 사용 가능 |

1MiB 상한은 v1 생성기의 실수 입력 방어 한도이며 실제 wire/서버 지원 한도나 성능 권장값이 아니다. 값44B 미만은 식별 header·checksum을 담을 수 없어 거절한다.

연산 비율은 **확률 선택의 기대 비율**이다. 모든 작은 trace의 개수를 정확한 quota로 강제하지 않는다. 같은 설정과 seed에서는 실제 선택 개수까지 동일하며 metadata에 관측 개수를 보존한다.

UNIFORM은 모든 key를 같은 확률로 선택한다. FIXED_HOT은 확률p로 key0, 나머지 확률로 전체 keyspace에서 uniform 선택한다. 따라서 key0의 기대 확률은 p+(1-p)/key.count이다. SET은 기존 key 범위의 overwrite다. DELETE 혼합의 HIT/MISS와 최종 상태는 실제 실행 순서에 따라 달라질 수 있다.

## 재현성과 ID·분할

- phase는 LOAD=0, WARMUP=1, MEASURE=2의 별도 namespace/salt를 쓴다. warm-up에서 측정 stream을 처음부터 재생하지 않는다.
- indexed-splitmix64-v1은 long modulo2^64 산술과 별도 op/key/hot/value channel을 사용한다. bounded 선택은 rejection으로 modulo bias를 제거한다. PRNG·channel·인코딩 변경은 버전 변경 및 golden vector 갱신 대상이다.
- at(phase, globalIndex)는 상태 없이 요청 하나를 재생성한다. 호출 순서·thread 스케줄과 무관하다. generator는 공유 가능하며 stream Iterator는 한 thread가 소유한다.
- streamId=globalIndex%stream.count, sequence=globalIndex/stream.count. 각 stream은 자기 globalIndex만 생성하므로 나머지 요청도 중복 없이 배분된다.
- clientId=phaseTag×stream.count+streamId. runtime ID=(runId, loadSlot, clientId, sequence). 재시도는 최초 ID/command를 그대로 보존해야 하며 이 단계는 실제 재시도를 실행하지 않는다.
- B2 분산 adapter는 각 logical stream을 한 loadSlot에만 배정하고 mapping을 고정해야 한다. 두 host가 같은 stream 전체를 각각 생성하면 중복이므로 허용하지 않는다.
- stream.count 변경은 값의 client identity와 canonical trace hash도 바꾼다. physical thread 수를 바꿀 때 logical stream mapping까지 무심코 바꾸지 않는다.
- runtime runId/loadSlot은 canonical hash에서 제외한다. 같은 논리 입력을 O00/각 변형에 비교하기 위한 것으로 실제 전송의 동시 interleaving까지 동일하다는 보장은 아니다.

## key/value 인코딩

모든 정수 header는 big-endian이다.

| 종류 | binary v1 |
| --- | --- |
| key | 8B ASCII OKKEYV01 + 8B nonnegative key index + zero padding |
| value | 4B ASCII OKV1 + phase int32 + seed int64 + clientId int64 + sequence int64 + keyIndex int64 + deterministic body + 마지막 CRC32C uint32 |

key는 숫자를 문자열로 잘라 넣지 않는다. index의8B를 항상 보존하고 decoder도 padding을 검사한다. 따라서 같은 길이의 유효한 key에서 서로 다른 index가 같은 byte열이 될 수 없다. 테스트는100만 key round-trip과 큰 index 경계를 확인한다.

body는 indexed PRNG의 각64-bit word를 낮은 byte부터 채운다. CRC32C는 마지막4B 이전의 header/body 전체를 대상으로 한다. checksum 검사는 손상 검출이며 **최신 값·선형성·내구성을 증명하지 않는다**. 그런 판정은 이후 history checker/ACK audit의 책임이다.

## canonical trace와 metadata

TraceWriter는 전체 요청을 메모리에 저장하지 않고 globalIndex 순서로 출력한다. 추가 메모리는 한 요청의 key/value와 고정64KiB 출력 buffer 수준이다. debug CLI가 큰 데이터 전체를 HashMap/리스트에 적재하지 않는다.

76B header:
- ASCII OKTRACE1(8B), format version int32=1
- seed int64, keyCount int64, keyBytes/valueBytes int32
- GET/SET/DELETE percent int32, streamCount int32
- distribution int32(UNIFORM0/FIXED_HOT1), hotProbability IEEE754 binary64 bits
- phase int32, global count int64

각 record:
- globalIndex int64, streamId int32, clientId/sequence int64
- op uint8(GET0/SET1/DELETE2)
- key length int32 + key bytes
- value length int32(비SET=-1) + SET value bytes

SHA-256은 header와 모든 record byte를 포함한다. phase별 global 순서 hash이므로 파일 생성과 hash-only가 일치한다. 빈 phase도 header hash를 가진다. frozen v1 vector는 seed101/key17/key24B/value100B/mix80:15:5/stream4와 LOAD17/WARMUP37/MEASURE101에 대해 고정해 두었다.

trace-metadata.json은 생성 설정·인코딩 버전·phase별 실제 연산 개수·canonical byte 수·SHA-256·파일 유무를 기록한다. status=generated_only, request_execution=not_run이다. 이것은 result.schema.json의 실제 benchmark 결과가 아니다.

- 기본은 hash-only: hash할 byte를 생성하지만 trace 파일은 저장하지 않는다. file=null.
- --emit-traces는 load.trace/warmup.trace/measure.trace도 저장한다. 큰 설정은 디스크 사용량을 먼저 계산해야 한다.
- CLI는 UTF-8 properties의 필수 field 전체를 요구하고 알 수 없는 field/분포/flag와 잘못된 숫자를 거절한다. benchmark run manifest를 읽어 자동 실행하지 않는다.
- 출력 디렉터리는 새 경로여야 한다. 기존 디렉터리/파일을 덮어쓰지 않으며 실패 중간 산출물을 자동 삭제하지 않는다. 완료 metadata와 실제 파일 hash를 대조해야 한다.

## 로컬 실행

프로젝트 최상위에서 JAVA_HOME을 Java21로 설정한다. 아래 output parent인 build는 Gradle이 만든다. 이미 있는 출력 경로이면 새 이름을 사용한다.

```powershell
.\gradlew.bat :experiment-runner:benchmarkTest --offline --console=plain
.\gradlew.bat :experiment-runner:benchmarkTrace --offline --console=plain "-PtraceConfig=experiments/replication-benchmark-v1/trace-small.properties" "-PtraceOutput=build/trace-small-new"
# 파일 byte까지 저장하려면 새 경로와 -PemitTraces=true 사용
```

trace-small.properties는 key64/워밍업256/측정10,000의 **생성기 인수 예제**다. 최종 데이터셋·성능 부하·운영 최적값이 아니다.

## 검증 범위와 다음 단계

검증 수·명령·실행 환경·source 보존은 B1_VALIDATION.json에 기록한다. 기존 전체 회귀와 새 benchmarkTest를 구분한다. 생성 성공을 protocol correctness/SSD/multi-host 성능 검증으로 보고하지 않는다.

이번 단계는 B1 중 **logical request·trace/key/value/ID 계약** 부분이다. 이후 단계2에서 O00 장기 client adapter와 실제 client attempt·최종 outcome 계수를 구현·검증했다(ADAPTER_O00.md 참조). 이는 원본 TCP의 지속 socket 구현이나 모든 변형 연결을 뜻하지 않는다. scheduler/open-loop·histogram·node collector·분산 history/ACK audit도 후속이다. Zipf/moving-hot 지원 전 해당 시나리오는 미지원 상태를 유지한다.
