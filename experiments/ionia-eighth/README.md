# V06 실행

Java21. 기존 방식과 data directory를 공유하지 않는다. 프로젝트 최상위 PowerShell:

~~~powershell
$env:JAVA_HOME='C:\Users\SSAFY\.jdks\ms-21.0.11'
$env:Path=$env:JAVA_HOME+'\bin;'+$env:Path
& .\experiments\ionia-validation\Run-Validation.ps1 -Mode Full -Repeat 1
~~~

터미널3개에서 각각 노드1/2/3을 실행한다.

~~~powershell
java -cp "cache-server/build/install/openkache-server/lib/*" io.openkache.server.ionia_eighth.IoniaEighthMain experiments/ionia-eighth/node-1.properties
java -cp "cache-server/build/install/openkache-server/lib/*" io.openkache.server.ionia_eighth.IoniaEighthMain experiments/ionia-eighth/node-2.properties
java -cp "cache-server/build/install/openkache-server/lib/*" io.openkache.server.ionia_eighth.IoniaEighthMain experiments/ionia-eighth/node-3.properties
~~~

~~~powershell
java -cp "cache-server/build/install/openkache-server/lib/*" io.openkache.server.ionia_eighth.IoniaMetaBatchClientMain experiments/ionia-eighth/client.properties set hello world
java -cp "cache-server/build/install/openkache-server/lib/*" io.openkache.server.ionia_eighth.IoniaMetaBatchClientMain experiments/ionia-eighth/client.properties get hello
java -cp "cache-server/build/install/openkache-server/lib/*" io.openkache.server.ionia_eighth.IoniaMetaBatchClientMain experiments/ionia-eighth/client.properties delete hello
~~~

CLI는 호출마다 새client/요청1개라 batch 성능 비교용이 아니다. 같은ID/총10초 deadline 안에서 발견·UNAVAILABLE/NOT_PRIMARY/BUSY 재시도를 제공한다. 라이브러리 자동 read retry는 없다.

library는 장기 IoniaMetaBatchClient를 재사용하고 onTimer를 호출한다. E8/B65536/D2ms/P256/W4는 fixture다. E1 또는 D0 대조군과 같은 server/storage 조건에서 비교한다. CLI timer10ms·W 포화·스케줄 때문에 D2ms는 wall-clock2ms 상한이 아니다.

~~~powershell
.\gradlew.bat :cache-core:test :cache-server:test --tests '*ionia_eighth*' --rerun-tasks --console=plain
~~~

Meta만 묶고 READ/개별View probes/쓰기/peer는 원본 경로다. BUSY 이전에도 READ/probes가 발생할 수 있다. P/W는 전역 socket/서버/heap 상한이 아니다. timeout/cancel 이후 raw stage terminal까지 permit 유지. 한 frame의 HOL/공동 실패 비용, 기존 메시지별 TCP 제약이 남고 처리량/p99는 미측정이다. 상세: ../../docs/IONIA_V06.md 및 IONIA_V06_VALIDATION.json.
