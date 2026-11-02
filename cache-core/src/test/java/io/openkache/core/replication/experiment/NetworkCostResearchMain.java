package io.openkache.core.replication.experiment;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.regex.*;
import static io.openkache.core.replication.experiment.ReplicationExperiment.require;

/** 바이트·대역폭 비용 실험의 JSON 검증 및 공통 Runner backend. */
public final class NetworkCostResearchMain {
    private static final String SCHEMA="a1-network-cost-json-1";
    private static final String HEADER="variant,scenario,history,valueBytes,mibps,timeoutMs,outcome,responseNs,recoveryNs,observedNs,attempts,preservedCopies,probeCopies,maxViewAdvance,attemptedBytes,enqueuedBytes,deliveredBytes,droppedBytes,pendingBytes,dataBytes,controlBytes,messages,peakPendingBytes,maxQueueWaitNs";
    private NetworkCostResearchMain() {}
    public static void main(String[] args) throws IOException {
        if(args.length==2 && args[0].equals("--rebuild")) { regenerate(Path.of(args[1])); return; }
        if((args.length!=2 && args.length!=3) || !List.of("quick","full").contains(args[1])
                || (args.length==3 && !args[2].equals("optimized"))) throw new IllegalArgumentException("<output.dir> <quick|full> [optimized] or --rebuild <output.dir>");
        Path folder=Path.of(args[0]); require(Files.isRegularFile(folder.resolve("launch.properties")),"run through common Runner");
        Files.createDirectory(folder.resolve("results"));
        for(var condition:NetworkCostExperiment.conditions(args[1].equals("quick"),args.length==3)) {
            var measurement=NetworkCostExperiment.run(condition);
            Files.writeString(folder.resolve("results/"+condition.id()+".json"),encode(measurement));
        }
        regenerate(folder);
    }
    private static String row(NetworkCostExperiment.Measurement m) {
        var c=m.condition(); var n=m.network();
        return c.variant()+","+c.scenario()+","+c.history()+","+c.valueBytes()+","+c.mibps()+","+c.timeoutMs()+","+m.outcome()+","+m.responseNs()+","+m.recoveryNs()+","+m.observedNs()+","+m.attempts()+","+m.preservedCopies()+","+m.probeCopies()+","+m.maxViewAdvance()+","+n.attemptedBytes()+","+n.enqueuedBytes()+","+n.deliveredBytes()+","+n.droppedBytes()+","+n.pendingBytes()+","+n.dataBytes()+","+n.controlBytes()+","+n.messages()+","+n.peakPendingBytes()+","+n.maxQueueWaitNs();
    }
    static String encode(NetworkCostExperiment.Measurement m) {
        String[] names=HEADER.split(","),values=row(m).split(",");
        StringBuilder b=new StringBuilder("{\n  \"resultSchemaVersion\": \""+SCHEMA+"\",\n  \"modelVersion\": \"logical-link-fifo-1\",\n  \"timeDomain\": \"virtual\",\n  \"timeUnit\": \"ns\",\n  \"byteUnit\": \"logicalBytes\"");
        for(int i=0;i<names.length;i++) b.append(",\n  \"").append(names[i]).append("\": ").append(i==0 || i==1 || i==6?"\""+values[i]+"\"":values[i]);
        return b.append("\n}\n").toString();
    }
    static NetworkCostExperiment.Measurement decode(String json) {
        Map<String,String> f=new HashMap<>();
        Matcher strings=Pattern.compile("\"([A-Za-z]+)\": \"([^\"\\\\\r\n]*)\"").matcher(json);
        while(strings.find()) require(f.put(strings.group(1),strings.group(2))==null,"duplicate string field");
        Matcher numbers=Pattern.compile("\"([A-Za-z]+)\": (-?[0-9]+)").matcher(json);
        while(numbers.find()) require(f.put(numbers.group(1),numbers.group(2))==null,"duplicate numeric field");
        for(String key:HEADER.split(",")) require(f.containsKey(key),"missing "+key);
        var c=new NetworkCostExperiment.Condition(f.get("variant"),f.get("scenario"),integer(f,"history"),integer(f,"valueBytes"),integer(f,"mibps"),integer(f,"timeoutMs"));
        var n=new LinkNetwork.Stats(number(f,"attemptedBytes"),number(f,"enqueuedBytes"),number(f,"deliveredBytes"),number(f,"droppedBytes"),number(f,"pendingBytes"),number(f,"dataBytes"),number(f,"controlBytes"),number(f,"messages"),number(f,"peakPendingBytes"),number(f,"maxQueueWaitNs"));
        var m=new NetworkCostExperiment.Measurement(c,f.get("outcome"),number(f,"responseNs"),number(f,"recoveryNs"),number(f,"observedNs"),integer(f,"attempts"),integer(f,"preservedCopies"),integer(f,"probeCopies"),integer(f,"maxViewAdvance"),n);
        require(encode(m).equals(json.replace("\r\n","\n")),"noncanonical or incompatible JSON"); return m;
    }
    private static long number(Map<String,String> f,String key) { return Long.parseLong(f.get(key)); }
    private static int integer(Map<String,String> f,String key) { return Math.toIntExact(number(f,key)); }
    static void validate(NetworkCostExperiment.Measurement m) {
        var c=m.condition(); var n=m.network();
        require(NetworkCostExperiment.conditions(false,c.variant().equals("VR_INCREMENTAL")).contains(c),"unknown condition");
        require(List.of("OK","TIMEOUT","UNAVAILABLE","CENSORED").contains(m.outcome()),"unexpected outcome");
        require(m.observedNs()>0 && m.observedNs()<=ProtocolCluster.ms(NetworkCostExperiment.LIMIT_MS) && m.attempts()>0,"invalid observation window");
        boolean normal=c.scenario().equals("NORMAL"),ok=m.outcome().equals("OK"),censored=m.outcome().equals("CENSORED");
        require(m.preservedCopies()==c.history()*(normal?3:2) && m.probeCopies()>=0 && m.probeCopies()<=(normal?3:2)
                && (!ok || m.probeCopies()>0) && m.maxViewAdvance()>=0,"invalid data audit");
        if(normal) require(m.recoveryNs()==-1 && (censored?m.responseNs()==-1:m.responseNs()>=0 && m.responseNs()<=m.observedNs()),"invalid response timing");
        else require(m.responseNs()==-1 && (ok?m.recoveryNs()>=0 && m.recoveryNs()<=m.observedNs():censored && m.recoveryNs()==-1),"invalid recovery timing");
        require(!censored || m.observedNs()==ProtocolCluster.ms(NetworkCostExperiment.LIMIT_MS),"censored before limit");
        require(n.attemptedBytes()>=0 && n.enqueuedBytes()>=0 && n.deliveredBytes()>=0 && n.droppedBytes()>=0 && n.pendingBytes()>=0
                && n.dataBytes()>=0 && n.controlBytes()>=0 && n.messages()>0 && n.maxQueueWaitNs()>=0,"negative network statistic");
        require(n.attemptedBytes()==Math.addExact(Math.addExact(n.deliveredBytes(),n.droppedBytes()),n.pendingBytes()),"byte accounting mismatch");
        require(n.enqueuedBytes()==Math.addExact(n.dataBytes(),n.controlBytes()) && n.enqueuedBytes()<=n.attemptedBytes()
                && n.enqueuedBytes()>=n.deliveredBytes()+n.pendingBytes(),"enqueue accounting mismatch");
        require(n.peakPendingBytes()>=n.pendingBytes() && n.peakPendingBytes()<=n.enqueuedBytes(),"invalid peak backlog");
        if(c.variant().equals("PB")) {
            require(ok,"PB should finish within this model's bounds");
            if(normal) {
                long expected=ProtocolCluster.ms(2)+LinkNetwork.serializationNs(65L+c.valueBytes(),c.mibps()*NetworkCostExperiment.MIB)
                        +LinkNetwork.serializationNs(40,c.mibps()*NetworkCostExperiment.MIB);
                require(m.responseNs()==expected && m.maxViewAdvance()==0,"PB normal RTT differs from independent formula");
            }
        }
    }
    public static void regenerate(Path folder) throws IOException {
        Files.deleteIfExists(folder.resolve("completion.properties")); List<Path> files;
        try(var stream=Files.list(folder.resolve("results"))) { files=stream.filter(p->p.toString().endsWith(".json")).sorted().toList(); }
        require(files.size()==64 || files.size()==216,"expected quick 64 or full 216 cases");
        boolean optimized=files.stream().anyMatch(p->p.getFileName().toString().startsWith("VR_INCREMENTAL-"));
        var expected=new HashSet<>(NetworkCostExperiment.conditions(files.size()==64,optimized)); Set<NetworkCostExperiment.Condition> seen=new HashSet<>();
        List<NetworkCostExperiment.Measurement> rows=new ArrayList<>();
        for(Path file:files) {
            var m=decode(Files.readString(file)); validate(m);
            require(expected.contains(m.condition()) && seen.add(m.condition()) && file.getFileName().toString().equals(m.condition().id()+".json"),"duplicate/misnamed/mixed condition"); rows.add(m);
        }
        require(seen.equals(expected),"condition coverage incomplete");
        StringBuilder csv=new StringBuilder(HEADER+"\n"),md=new StringBuilder("# PB / VR 메시지 크기·대역폭 비용\n\n논리 frame 바이트, 방향별 독립 FIFO, 편도 전파 1ms. 시간은 가상 시간이다.\n\n");
        Properties launch=new Properties(); try(var reader=Files.newBufferedReader(folder.resolve("launch.properties"))) { launch.load(reader); }
        for(String key:List.of("suiteExecutionId","gitCommit","sourceHash","backendHash")) md.append("- ").append(key).append(": `").append(launch.getProperty(key,"unknown")).append("`\n");
        md.append("\n|방식|단계|기존 연산|값 B|링크 MiB/s|감지 ms|결과|응답 ms|복구 ms|전송 시도 MiB|도착 MiB|대기·전송 중 MiB|최대 view 증가|\n|---|---|---:|---:|---:|---:|---|---:|---:|---:|---:|---:|---:|\n");
        for(var m:rows) {
            var c=m.condition(); var n=m.network(); csv.append(row(m)).append('\n');
            md.append('|').append(c.variant()).append('|').append(c.scenario()).append('|').append(c.history()).append('|').append(c.valueBytes()).append('|').append(c.mibps()).append('|').append(c.timeoutMs()).append('|').append(m.outcome()).append('|')
                    .append(time(m.responseNs())).append('|').append(time(m.recoveryNs())).append('|').append(mib(n.attemptedBytes())).append('|').append(mib(n.deliveredBytes())).append('|').append(mib(n.pendingBytes())).append('|').append(m.maxViewAdvance()).append("|\n");
        }
        md.append("\n## 해석 범위\n\n"
                + "- 준비 단계는 전파 지연만 적용하고 동일한 성공 이력을 만든다. 링크가 빈 시점에 대역폭을 활성화하며 준비 트래픽은 제외한다.\n"
                + "- 주 SET deadline 2000ms, crash 관측 상한 5000ms. CENSORED는 상한까지 성공을 관측하지 못했다는 뜻이며 복구 시간 5000ms로 계산하지 않는다.\n"
                + "- response/recovery=-1은 해당 없음 또는 미완료다. callback은 ns로 기록하고 통계는 최대 1ms 뒤 polling 종료 시점에서 수집한다.\n"
                + "- attempted = delivered + dropped + pending, enqueued = data + control을 검증한다. 전송량은 실제 TCP bytes가 아닌 선언된 논리 인코딩이다.\n"
                + "- maxQueueWaitNs는 관측 구간에 예약된 프레임의 최대 대기시간이며 아직 전송을 시작하지 않은 프레임도 포함한다.\n"
                + "- VR은 개선 전 전체 로그 모드다. VR_INCREMENTAL은 증분 PREPARE·작은 COMMIT·ACK 기반 재전송 제어를 사용한다.\n"
                + "- VR_INCREMENTAL의 재전송 대기는 1초부터 4초까지 늘며, view-change 대기 예산은 최소 3초다. 초기 감지 timeout은 조건값 그대로다.\n"
                + "- NORMAL의 view 증가를 장애 없는 오판으로 관측한다. CRASH의 추가 view 증가에는 실패한 승격 재시도도 포함한다.\n"
                + "- 각 링크 방향은 독립 대역폭이며 NIC 공유, 패킷 분할, TCP 혼잡제어, CPU·SSD·클라이언트 전송은 모델링하지 않는다.\n"
                + "- 모든 기존 고유 키를 생존 저장소에서 확인한다. 새 OK의 모든 Backup 적용은 commit 통지 도착 이후일 수 있다.\n"
                + "- 현재 전체 로그 VR 구현의 비용이며 최적화된 VR 전반의 성능을 일반화하지 않는다.\n");
        Files.writeString(folder.resolve("summary.csv"),csv); Files.writeString(folder.resolve("results.md"),md);
        Files.writeString(folder.resolve("completion.properties"),"status=complete\nresultSchemaVersion="+SCHEMA+"\ncases="+rows.size()+"\n");
    }
    private static String time(long ns) { return ns<0?"—":String.format(Locale.ROOT,"%.6f",ns/1_000_000.0); }
    private static String mib(long bytes) { return String.format(Locale.ROOT,"%.6f",bytes/(double)NetworkCostExperiment.MIB); }
}
