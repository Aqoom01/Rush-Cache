# NVMe KV 저장소 설계 (제안)

상태: **구현**(2026-11-04, S15P31D111-184). 구현 B2 담당, A1(KV Store 담당) 리뷰 전이다. §6·§7의 결정은 리뷰에서 바뀔 수 있다.

`core.kv`의 저장소를 NVMe SSD에 둘 수 있게 한다. 지금 `KvStore`는 worker별 `HashMap`이라 실험이 CPU·네트워크만 재고, 프로젝트 주제인 SSD 지연·IOPS는 측정되지 않는다.

## 1. 범위

| 한다 | 하지 않는다 |
|---|---|
| worker별 로그 구조 파일 + 메모리 색인 | 재시작 복구(빈 상태로 시작) |
| `O_DIRECT`로 page cache 우회 | DRAM 앞단 캐시(2계층) — 후속 |
| 용량이 차면 오래된 segment부터 통째로 축출(FIFO) | compaction, TTL |
| 서버 설정으로 `memory`/`nvme` 선택 | wire·`CacheEngine`·`Command`·`Result` 변경 |
| SSD 읽기·쓰기 횟수·바이트 지표 | 복제·라우팅(연구 패키지 책임) |

기존 인메모리 저장소는 그대로 남기고 기본값으로 둔다. 같은 실험을 두 저장소로 비교할 수 있다.

## 2. 구조

```
KvEngine (변경 없음: key 해시 → owner worker → mailbox)
  └─ worker i 의 저장소 (KvStore 자리)
       ├─ 메모리 색인  HashMap<Bytes, Loc(segment, offset, length, version)>
       ├─ 쓰기 버퍼    정렬된 direct ByteBuffer (기본 64KiB)
       └─ 파일         <storage.path>/worker-<i>/seg-<n>.log  (고정 크기, 기본 64MiB)
```

- `KvStore`를 패키지 내부 interface로 바꾸고, 지금 구현은 `MemoryKvStore`, 새 구현은 `NvmeKvStore`로 둔다. 둘 다 `core.kv` 밖에 공개하지 않는다.
- worker마다 자기 파일만 쓰므로 lock이 없다. 저장소는 기존 규칙대로 owner worker thread에서만 접근한다.
- 디스크 I/O는 worker thread에서 동기로 한다. Netty EventLoop는 지금처럼 mailbox에 넘기기만 하므로 block되지 않는다.

## 3. 기록 형식

segment 안에 기록을 빈틈없이 이어 붙인다. 정렬은 버퍼를 디스크에 쓸 때만 맞춘다(§5).

```
| keyLength u16 | valueLength u32 | version u64 | key | value |
```

- 재시작 복구가 없으므로 tombstone, CRC, segment header를 두지 않는다. 색인이 유일한 진실이다.
- 읽은 기록의 key가 요청 key와 다르면 색인 버그로 보고 예외가 아닌 실패 상태로 돌려준다(§7).
- 기록 하나는 segment 하나를 넘지 않는다. 기록 크기가 segment보다 크면 SET은 `CAPACITY_EXCEEDED`다. wire 최대 frame이 2MiB이므로 기본값(64MiB)에서는 생기지 않는다.

## 4. 연산

| 연산 | 동작 |
|---|---|
| SET | 쓰기 버퍼에 기록을 붙이고 색인을 새 위치로 바꾼다. 버퍼가 차면 디스크에 쓴다. version은 지금처럼 기존 값 + 1(없으면 1) |
| GET | 색인에서 위치를 찾는다. 아직 버퍼에 있으면 메모리에서, 아니면 디스크에서 그 기록을 덮는 블록만 읽는다(보통 4KiB 1-2개) |
| DELETE | 색인에서 지운다. 디스크는 건드리지 않는다 |
| EXISTS, COUNT | 색인만 본다 |
| SCAN, RANGE | 색인을 돌며 조건에 맞는 key의 값을 디스크에서 읽는다. 지금과 결과가 같고 느리다 |

**축출.** worker 용량(`storage.capacity.bytes / workers`)만큼 segment를 원형으로 쓴다. 새 segment가 필요한데 자리가 없으면 가장 오래된 segment를 버린다. segment마다 그 안에 쓴 key 목록을 메모리에 들고 있다가, 색인이 아직 그 segment를 가리키는 key만 지운다(덮어쓴 key는 새 위치를 가리키므로 남는다).

- 이 방식은 **쓰기 순서 기준 FIFO**다. 자주 읽히지만 다시 쓰이지 않는 key도 오래되면 축출된다. 읽힌 key를 다시 쓰는 reinsertion(CacheLib 방식)은 후속이다.
- 덮어쓴 이전 기록은 죽은 공간으로 남다가 그 segment가 축출될 때 회수된다.

## 5. O_DIRECT

- 파일을 `ExtendedOpenOption.DIRECT`(`com.sun.nio.file`, `jdk.unsupported` 모듈)로 연다. 외부 라이브러리가 아니라 모든 JDK에 포함된 API다. cache-core 의존성 규칙(JDK만)에 맞는다고 보고, 리뷰에서 확인받는다.
- 이 이름을 코드에 직접 쓰면 javac가 끌 수 없는 "internal proprietary API" 경고를 내 `-Werror`에 걸린다. 그래서 실행 시점에 reflection으로 값을 얻는다(`NvmeKvStore.directOption`).
- direct I/O는 버퍼 주소·파일 offset·길이가 모두 블록 크기의 배수여야 한다. 블록 크기는 `FileStore.getBlockSize()`로 읽고, 버퍼는 `ByteBuffer.allocateDirect(n + align).alignedSlice(align)`로 만든다.
- 쓰기 버퍼는 항상 블록 경계에서 시작해 블록 배수로 쓴다. 기록은 블록 경계를 넘어 이어질 수 있고, 읽을 때는 기록을 덮는 블록들을 읽은 뒤 잘라 낸다.
- `storage.direct.io=false`이면 일반 I/O로 연다. direct I/O를 지원하지 않는 파일 시스템(tmpfs 등)의 로컬 테스트용이다. 실험 결과에는 이 값을 함께 기록한다.

## 6. 의미 변화 (A1 확인 필요)

`memory` 저장소와 결과가 달라지는 점이다.

1. **축출로 MISS가 생긴다.** 용량이 차면 SET했던 key도 GET이 `MISS`가 된다. 지금은 eviction이 없어 이런 MISS가 없다. `MISS`는 이미 정상 응답이라 상태값은 그대로다.
2. **SET 성공은 내구성을 뜻하지 않는다.** 쓰기 버퍼에 들어가면 `OK`다. 캐시이고 복구를 하지 않으므로 fsync하지 않는다.
3. **재시작하면 빈 상태다.** 시작할 때 `worker-<i>/` 아래 파일을 지운다. m6gd의 instance store도 정지하면 지워진다.
4. **version은 지금과 같다.** DELETE 후 SET은 1부터 다시 시작한다(색인에서 지우므로).

## 7. 오류 처리

예상 가능한 실패는 지금처럼 `Result` 상태로 낸다.

| 상황 | 상태 |
|---|---|
| 기록이 segment보다 큼 | `CAPACITY_EXCEEDED` |
| 디스크 I/O 오류, 읽은 key 불일치 | `UNAVAILABLE` + 오류 지표 증가 (제안. A1 확인 필요) |
| 설정 값 오류(필수 값 없음, worker당 segment 2개 미만 등) | 서버 기동 전 거절(종료 코드 2) |
| 시작 시 디렉터리를 만들 수 없음·direct I/O 불가 | 서버 기동 실패(종료 코드 1) |

## 8. 설정 (cache-server)

| key | 기본값 | 뜻 |
|---|---|---|
| `storage.engine` | `memory` | `memory` 또는 `nvme` |
| `storage.path` | (nvme면 필수) | segment 파일 디렉터리 |
| `storage.capacity.bytes` | (nvme면 필수) | 노드 전체 용량. worker 수로 나눈다 |
| `storage.segment.bytes` | 67108864 (64MiB) | segment 크기. 블록 크기 배수 |
| `storage.write.buffer.bytes` | 65536 (64KiB) | 쓰기 버퍼 크기. 블록 크기 배수 |
| `storage.direct.io` | `true` | `false`면 page cache 사용 |

**메모리 예산.** 색인 항목은 16바이트 key 기준 약 130바이트로 추정한다(측정 전). EC2 노드는 저장소와 관계없이 heap을 2GiB로 고정한다(`JAVA_OPTS=-Xms2g -Xmx2g`). 이때 약 1,000만 key, value 1KiB면 SSD에 약 10GB가 쌓인다(RAM보다 크다).

## 9. 지표 (Metrics)

`MetricsSink`로 worker 합계를 낸다. 이름은 상수이고 label은 server adapter가 붙인다.

| 이름 | 종류 | 뜻 |
|---|---|---|
| `storage_read_ops` / `storage_read_bytes` | counter | 디스크 읽기 횟수·바이트 |
| `storage_write_ops` / `storage_write_bytes` | counter | 버퍼 flush 횟수·바이트 |
| `storage_buffer_hits` | counter | 디스크 대신 쓰기 버퍼에서 읽은 GET |
| `storage_evicted_segments` / `storage_evicted_keys` | counter | 축출 |
| `storage_read_nanos` | duration | 디스크 읽기 한 건 소요 시간 |
| `storage_errors` | counter | §7의 I/O 오류·key 불일치 |

B2 입력의 IOPS는 `storage_read_ops`·`storage_write_ops`에서 얻는다.

## 10. 검증

1. **같은 계약 테스트를 두 저장소에.** 기존 `KvEngine` 테스트를 `memory`·`nvme`(임시 디렉터리, `direct.io=false`) 양쪽으로 돌린다.
2. **nvme 전용 단위 테스트.** 버퍼에 있는 기록·디스크에 있는 기록 GET, 블록 경계를 넘는 기록, 덮어쓰기 후 축출, 축출 후 MISS, `CAPACITY_EXCEEDED`, 시작 시 파일 정리.
3. **direct I/O.** 지원하는 파일 시스템에서만 도는 테스트(지원 여부를 확인하고 아니면 건너뛴다)로 정렬 경로를 검증한다.
4. **EC2.** baseline config를 `memory`·`nvme`로 각각 돌려 p50/p99, `storage_read_ops`로 실제 디스크 읽기가 일어났는지 확인한다.

## 11. 인프라

- CI 입력 `STORAGE`(memory/nvme) → Terraform `storage_engine`.
- nvme면 노드 `user_data`가 instance store(장치 모델명 `Amazon EC2 NVMe Instance Storage`로 찾는다)를 xfs로 포맷해 `/mnt/nvme`에 마운트하고, 컨테이너에 `/data`로 넘긴다. 용량은 40GiB(`storage.capacity.bytes`), segment·버퍼는 기본값이다.
- `deploy/docker/server.properties`의 기본값은 `memory`이고, nvme 설정은 `user_data`가 덧붙인다.
- Runner는 서버 저장소를 관측하지 않으므로 `run.sh`가 `server.storage.note`를 넣어 결과의 `storageLayer`에 남긴다.

## 12. 정해지지 않은 것

- I/O 오류 상태값(§7)
- reinsertion, DRAM 앞단 캐시
- worker 수와 NVMe 큐 깊이의 관계(지금은 worker당 동기 I/O 1개)
