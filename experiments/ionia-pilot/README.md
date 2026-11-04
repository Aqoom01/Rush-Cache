# O00 3노드 기능 pilot

2개 부하 서버·별도 제어 서버의 분산 기능 pilot은 [DISTRIBUTED.md](DISTRIBUTED.md)를 따른다. 고정3,200요청 dataset은 datasets/pilot-v1-seed101/에 build/clean과 독립적으로 보관한다. 아래 내용은 기존 단일 load 실행 방법이다.

Java 21 기준. 기존 IoniaNode, TCP 전송, O00 장기 client, TraceGenerator와 ClosedLoopRunner를 재사용한다. 복제 JVM 3개와 부하 JVM 1개를 분리한다. 이 도구는 원본/파생 알고리즘이나 공통 API를 변경하지 않는다.

## 빠른 시작 — Windows 로컬

저장소 루트에서:

```powershell
.\gradlew.bat :cache-server:benchmarkTest --tests '*PilotInputsTest' --tests '*RemoteO00DrainProbeTest' :cache-server:pilotZip --offline --console=plain
# 항상 존재하지 않는 새 출력 경로를 지정한다.
Expand-Archive cache-server/build/distributions/ionia-pilot.zip C:/pilot/bundle-NEW
& C:/pilot/bundle-NEW/tools/rehearse.ps1 -Bundle C:/pilot/bundle-NEW -NewOutput C:/pilot/run-NEW -Java C:/jdk21/bin/java.exe
```

검사 11개와 한 번의 별도 JVM 기동 예행연습이 이 변경의 필수 검사다. 기존 전체 949개 회귀는 앞 단계 증거를 유지하며 이 도구 추가만을 이유로 재실행하지 않는다. 실행 프로세스·로그·데이터를 보존하며 자동으로 기존 결과를 삭제하지 않는다.

## 고정 데이터

| 설정 | LOAD | WARMUP | MEASURE | 합계 | 용도 |
|---|---:|---:|---:|---:|---|
| smoke.properties | 16 | 8 | 48 | 72 | 기동·연결 검증 |
| pilot.properties | 1,000 | 200 | 2,000 | 3,200 | 첫 원격 기능 pilot |

seed=101, key=24B, value=100B, 요청 혼합 GET/SET/DELETE=80/15/5, UNIFORM, stream=2, concurrency=2. LOAD는 전부 SET이고 혼합 비율은 WARMUP/MEASURE에만 적용된다. 짧은 요청열의 실제 비율은 난수 표본이므로 설정 비율과 다를 수 있다.

각 데이터셋에는 3개 binary trace, 메타데이터, 원본 설정, SHA-256과 완료 마커가 있다. 실행기는 고정 seed/config/index로 요청을 다시 생성하고, 실행 전 파일 해시와 재생성한 canonical trace 해시가 일치해야 진행한다. binary reader를 이용해 trace 파일을 직접 재생하는 방식은 아니다. 다른 방식에도 같은 canonical dataset을 사용하되 실제 실행 연결은 각 adapter에서 별도로 검증한다.

## Linux EC2 실행 순서 — 아직 원격 실행하지 않음

전제: Linux, Java 21, 노드 간 TCP 도달 가능, 포트 미사용, 충분한 새 디스크 공간. 실제 EC2 OS/CPU/메모리/SSD/네트워크 상태는 미확인이다. SSH·보안그룹·시스템 설정은 이 도구에서 변경하지 않는다.

1. 동일 ZIP을 3개 replica 호스트와 별도 load 호스트에 풀어 둔다. `topology-ec2.properties.example`을 복사하여 replica의 실제 private IP로 채운다. 첫 실행은 3개 replica만 사용한다.
2. load 호스트에서 데이터셋과 공통 run 입력을 **한 번만** 만든다. 이미 생성된 동일 데이터셋을 전송해 써도 된다.

```bash
B=/absolute/path/to/bundle
BASE=/absolute/path/to/new-pilot-parent
mkdir "$BASE"                       # 기존 경로 재사용 금지
bash "$B/tools/pilot.sh" dataset "$B/tools/pilot.properties" "$BASE/dataset"
bash "$B/tools/pilot.sh" prepare /absolute/topology.properties "$BASE/dataset" "$BASE/run"
bash "$B/tools/pilot.sh" verify "$B" "$BASE/run"
```

3. **기동 전에** `run` 디렉터리 전체를 3개 replica에 복사한다. 하나의 prepare 결과를 공유해야 run ID/input hash가 같다. `data/`, `state/`, `ready/`는 이때 비어 있어야 한다. 각 호스트의 절대 경로는 달라도 된다. 노드마다 prepare를 다시 실행하지 않는다.
4. 각 replica에서 자기 번호 하나만 실행한다. 아래는 node 1이며 다른 호스트에서는 2, 3으로 바꾼다.

```bash
B=/absolute/path/to/bundle
R=/absolute/path/to/run
bash "$B/tools/node.sh" start "$R" 1
# state/node-1.ready.properties와 stderr를 확인한다.
```

5. 각 호스트의 `state/node-N.ready.properties` 파일을 load 호스트의 `run/ready/node-N.ready.properties`로 복사한다. LISTENING은 소켓 기동 표시이며 전체 준비 완료가 아니다. load 명령이 같은 code/input/run ID와 모든 노드 view=0/NORMAL/applied=0을 추가 확인한다.
6. load 호스트에서 다음 두 명령을 실행한다.

```bash
bash "$B/tools/pilot.sh" wait "$B" "$BASE/run"
bash "$B/tools/pilot.sh" run  "$B" "$BASE/run"
```

7. 성공·실패 모두 각 replica에서 `bash "$B/tools/node.sh" stop "$R" N`을 실행한다. 로그, ready 파일, `state/node-N.stopped`, 결과 디렉터리를 수집한다. stop은 소유한 run의 stop 파일을 만들고 종료를 기다린다. 임의의 java/PID를 일괄 종료하지 않는다. 장애 시 자동 삭제/재시작하지 말고 로그를 보존한다.

Linux wrapper는 Java 명령을 전달할 뿐이다. 이 변경에서 실제 검증한 호스트 OS는 Windows이며 Linux/EC2 기동은 첫 원격 pilot에서 확인한다. 5노드 입력 형식도 수용하지만 이번 로컬 검증은 3노드다.

## 통과 조건과 결과

- 고정 입력과 bundle `lib/*.jar`의 해시 및 선언 범위 일치. 기존 run/client/data 디렉터리 재사용 거절.
- 소켓만 열린 상태를 넘어 모든 replica의 공개 view/followerRead RPC로 연속 적용 인덱스 확인. 미도달/적용 지연은 대기 후 timeout, 예상보다 큰 인덱스·다른 view·잘못된 응답 식별자는 실패.
- LOAD → drain → WARMUP → drain → MEASURE → drain → client 종료 후 확인. client pending/raw/active/occupied=0과 모든 replica applied=기대 쓰기 수를 확인한다. 관찰자는 비차단이며 동시에 한 배치만 전송한다.
- workload 뒤 예약 키에 MISS → SET A → GET A → DELETE → MISS → DELETE MISS → SET B → GET B, 8개 순차 감사를 수행한다. A/삭제/B 반영 뒤 각 replica의 값까지 총 9번 확인한다.
- `result/`에는 기존 writer의 계수, 지연 histogram, 이벤트, 환경, manifest, integrity 해시를 기록한다. `pilot-summary.json`은 workload와 별도의 CRUD 감사 결과를 연결한다. 모든 검증을 마친 뒤에만 `PILOT_COMPLETE`를 기록한다. 실패한 workload/audit는 nonzero 종료와 `PILOT_FAILED.json`을 남긴다. 시작 전 입력 실패는 완료 표시 없이 nonzero 종료한다.
- 실행 이후 독립 검사: `python "$B/tools/verify.py" "$B" /absolute/path/to/run`. 로컬 예행연습 결과에는 `--local-stopped`를 붙여 별도 PID와 정상 종료 표시도 확인한다.

M01/M03/M07/M10/M11의 client 측 요청 계수·처리량·재시도·지연 등 기존 writer 증거만 사용한다. remote host CPU/디스크/fsync, 전체 peer network drain, 완전한 전수 이력 선형화, 전원 장애 복구는 검증하지 않는다. 공개 applied prefix를 commit/recorded/대기열 카운터나 SSD force 횟수로 해석하지 않는다.

이것은 성능 비교용 최종 데이터셋이나 SSD 벤치마크가 아니다. 최소 측정 시간은 0인 fixed-N 기능 시험이며 반복 실행·장시간 부하·포화 곡선·장애 시나리오는 후속 단계다. 기존 파일 저장 어댑터를 사용하고 독립 Replica RAM Durability Log, stable View, 선택적 KV force(여기서는 off)를 구분한다. 동기식 probe의 부가 I/O가 있으므로 이 수치를 논문급 알고리즘 성능으로 비교하지 않는다.

`BUNDLE_SHA256SUMS`는 실행 JAR 목록, `INPUT_SHA256SUMS`는 입력 파일 목록의 일치 확인용이다. 서명·원격 attestation을 제공하지 않는다. 도구/문서는 ZIP SHA-256으로 배포본을 구분하고, 동일 ZIP을 사용한다.
