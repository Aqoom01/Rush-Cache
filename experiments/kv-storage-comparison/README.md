# kv-storage-comparison

같은 부하를 memory 저장소와 NVMe 저장소(`docs/storage-nvme.md`)에서 돌려, SSD 읽기가 지연·처리량에 주는 영향을 잰다.

## 실행

GitLab Run pipeline에서 아래 설정으로 두 번 실행한다. 1회 약 25분(기동 + 3회 반복)이다.

| 입력 | 값 |
|---|---|
| `NODES` | `1` |
| `CONFIG` | `experiments/kv-storage-comparison/config.properties` |
| `STORAGE` | `memory`, 그다음 `nvme` |

## 조건

- 데이터 약 200MB(key 20만 개 x 1KiB)는 RAM보다 작다. NVMe 저장소는 O_DIRECT라 page cache를 쓰지 않으므로, 쓰기 버퍼(64KiB)에 아직 남은 기록이 아니면 GET은 SSD에서 읽는다. memory 저장소도 같은 데이터를 heap(2GiB)에 담을 수 있어 같은 조건으로 비교된다.
- 용량(40GiB)보다 훨씬 작아 NVMe 축출은 일어나지 않는다.
- 부하 생성 노드는 closed-loop(`concurrency=8`)다. 지연이 늘면 처리량도 같이 준다.

## 볼 것

| 파일 | 항목 | 기대 |
|---|---|---|
| `*/*.json`, `summary.csv` | p50, p99, 처리량 | 반복 3회의 범위로 비교 |
| `*/*.json` | `storageLayer` | `in-memory` / `nvme (...)` |
| `metrics-node-1.txt` | `openkache_storage_read_ops_total` | memory는 없음, nvme는 HIT 수에 가까움 |
| `metrics-node-1.txt` | `openkache_storage_read_seconds_sum / _count` | 디스크 읽기 한 건 평균 |
| `metrics-node-1.txt` | `openkache_storage_buffer_hits_total` | 작음(최근 쓴 기록만) |

## 한계

- 인메모리 색인 + SSD value 구조 하나만 본다. DRAM 앞단 캐시, 축출이 일어나는 크기는 다루지 않는다.
- 노드 1대, 단일 부하 생성기다. 부하 생성기가 먼저 포화되면 두 저장소의 처리량 차이가 작게 나온다.
