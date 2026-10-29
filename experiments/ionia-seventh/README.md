# V05 — 클라이언트 관측 기반 Follower 선택

Java21, 프로젝트 최상위 폴더에서 실행한다. 이 서버들은 원본 O00이며 새로운 client만 Follower 선택을 바꾼다. 세 노드를 서로 다른 터미널에서 node 번호1/2/3으로 바꿔 실행한다.

```powershell
.\gradlew.bat :cache-server:installDist
java -cp 'cache-server/build/install/openkache-server/lib/*' io.openkache.server.ionia.IoniaMain experiments/ionia-seventh/node-1.properties
```

모든 노드가 실행된 뒤 별도 터미널에서:

```powershell
java -cp 'cache-server/build/install/openkache-server/lib/*' io.openkache.server.ionia_seventh.IoniaAdaptiveReadClientMain experiments/ionia-seventh/client.properties set example value
java -cp 'cache-server/build/install/openkache-server/lib/*' io.openkache.server.ionia_seventh.IoniaAdaptiveReadClientMain experiments/ionia-seventh/client.properties get example
java -cp 'cache-server/build/install/openkache-server/lib/*' io.openkache.server.ionia_seventh.IoniaAdaptiveReadClientMain experiments/ionia-seventh/client.properties delete example
```

server node.properties에 read.*를 넣지 않는다. client 파일에는 replicas/leader.id와 필수 read.*만 둔다. read.selection은 adaptive/round-robin이다. client마다 정책이 달라도 O00 wire/stable View configuration을 공유한다. 정책 hash는 IoniaAdaptiveReadClient.diagnostics().profile()로 관측하며 서버 설정에 섞지 않는다.

기능 예제는 L64/P32/R0=1ms/TTL1000ms/cooldown250ms/E16이다. 최적값이 아니다. 원본 쓰기 fallback50ms·READ Meta/View 검증을 유지한다. 모든 후보가 cooldown/full이면 BUSY다. CLI는 같은ID/총10초 안에서 UNAVAILABLE/NOT_PRIMARY/BUSY를 재시도하며 단발 client이므로 학습 성능 비교에 사용하지 않는다. 비교 driver는 장수 client와 같은 admission/부하·저장 배치·CPU·timeout을 유지해야 한다.

diagnostics.outcomes는 거절/private delegate 완료 결과다. public cancel 뒤에 OK 완료가 집계될 수 있으므로 publicCancels와 구분한다. 사용자에게 전달된 성공 수·goodput을 이 map만으로 계산하지 않는다. 모드·설정·client ID는 실행 metadata에도 기록해야 한다.

Windows 전체 회귀(서버는 종료한 상태, JAVA_HOME은Java21):

```powershell
& .\experiments\ionia-validation\Run-Validation.ps1 -Mode Full -Repeat 1
```

사전진단은 OS 포트 설정을 바꾸거나 실패를 skip/pass로 만들지 않는다. 선택한 기능 fixture의 결과는 docs/IONIA_V05_VALIDATION.json, 원문 근거/설계/제약은 docs/IONIA_V05.md에 있다. 성능·실시간 lag/서버 queue·SSD IOPS·multi-host·power-loss 검증은 후속이다.
