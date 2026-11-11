# B1·B2·B3 통합 실험 실행

## GitLab CI 한 번으로 실행

GitLab의 **Build → Pipelines → Run pipeline**에서 설정이 포함된 보호 브랜치를 선택한다. kache와 B1을 메모리 저장소에서 비교하도록 `STORAGE=memory`를 사용한다.

```text
NODES=8
CONFIG=experiments/cluster-integration/e1-routing-overhead experiments/cluster-integration/e2-moved experiments/cluster-integration/e3-balance experiments/cluster-integration/e5-e6-drain
```

CI는 폴더별 `*.properties`를 파일명 순으로 펼쳐 실행한다. 각 config 시작 전에 서버를 재시작하고, `deploy.members`로 선택된 노드만 켠다. 서버 8대와 부하 생성기 1대를 띄우며, 목록의 17개 config는 각각 한 번 실행한다. E4는 별도 실행이며 기존 설정은 [E4 README](e4-migration/README.md)를 따른다.

## 담당과 결과 위치

S3의 공통 접두사는 `s3://openstd-s15p31d111-results/results/<study>/`다. 실제 실행 경로는 job 로그의 `결과:` 줄에서 확인한다.

| 실험 | 담당 | S3 study | 명시된 멤버 수 |
|---|---|---|---|
| [E1 라우팅 오버헤드](e1-routing-overhead/README.md) | B1 | `cluster-e1-routing` | 1 / 4 / 8 |
| [E2 MOVED와 표 전파](e2-moved/README.md) | B1 | `cluster-e2-moved` | 8 |
| [E3 균형 조정](e3-balance/README.md) | B2 | `cluster-e3-balance` | 2 |
| [E4 이동 방식](e4-migration/README.md) | B3 | `cluster-e4` | 2 |
| [E5·E6 다중 이동과 drain](e5-e6-drain/README.md) | B3 | `cluster-e5-e6` | 4 |

## 예상 소요 시간

E4의 60초 측정 config 약 3.5분, 120초 측정 config 약 5분을 기준으로 잡았다. E1은 30만 closed-loop 요청으로 약 3분을 추정했다. 달성 2,000 req/s라면 warm-up과 측정에 약 2.5분이 들며 여기에 재시작·업로드가 붙는다. E2는 8대 재시작과 100만 key 사전 채움·검사 여유를 포함해 약 4.5분으로 추정했다. E2의 추정은 20건의 pre-copy가 측정 구간 안에 끝난다는 가정이다. 실제 소요 시간은 부하 생성기 처리량과 이동 완료 시점에 따라 달라진다.

| 설정 | 개수 | 예상/개 | 예상 소계 | `execution.timeout.ms` / 개 |
|---|---:|---:|---:|---:|
| E1 `01–02` kache 1대, 연결 8·64 | 2 | 3분 | 6분 | 240000 (4분) |
| E1 `03–04` B1 1대, 연결 8·64 | 2 | 3분 | 6분 | 240000 (4분) |
| E1 `05–06` B1 4대, 연결 8·64 | 2 | 3분 | 6분 | 240000 (4분) |
| E1 `07–08` B1 8대, 연결 8·64 | 2 | 3분 | 6분 | 240000 (4분) |
| E2 `01-precopy-20` | 1 | 4.5분 | 4.5분 | 480000 (8분) |
| E3 `1-no-balance`, `2-split-for-heat`, `3-midpoint-split`, `4-estore-split` | 4 | 5분 | 20분 | 1500000 (25분) |
| E5·E6 `0-sequential`, `1-fifo`, `2-mongodb`, `3-slamig` | 4 | 5분 | 20분 | 1200000 (20분) |
| **합계** | **17** | | **68.5분** | |

기동·실행 변동 여유 30분을 더한 예상은 **98.5분**이다. CI는 실제 예상 대신 각 설정의 `execution.timeout.ms`를 합산한다. 기존 E3·E5·E6 상한만 180분이며, E1·E2까지 더하면 220분이다. CI 계산은 `min(220 + 30, 180) = 180분`으로 제한된다. 각 config 시작 직전에 그 설정의 상한과 5분 여유가 남았는지 확인하므로, 앞 실행들이 상한에 가깝게 지연되면 뒤 설정의 시작이 거절될 수 있다. 기존 E3·E5·E6의 타임아웃 설정은 유지했다.
