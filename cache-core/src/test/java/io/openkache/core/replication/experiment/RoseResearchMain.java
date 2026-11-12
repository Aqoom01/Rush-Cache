package io.openkache.core.replication.experiment;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.regex.*;
import static io.openkache.core.replication.experiment.ReplicationExperiment.require;

/** bounded-lag 단일 파티션 변형의 원본·요약·시계열을 공통 Runner에서 생성한다. */
public final class RoseResearchMain {
    private static final String SCHEMA="a1-rose-json-1";
    private static final String SAMPLE="timeNs,ok,primary,lag,pending,backup1Applied,backup2Applied";
    private RoseResearchMain() {}
    public static void main(String[] args) throws IOException {
        if(args.length==2 && args[0].equals("--rebuild")) { regenerate(Path.of(args[1])); return; }
        if(args.length!=2 || !List.of("quick","full").contains(args[1])) throw new IllegalArgumentException("<output.dir> <quick|full> or --rebuild <output.dir>");
        Path folder=Path.of(args[0]); require(Files.isRegularFile(folder.resolve("launch.properties")),"use common Runner"); Files.createDirectory(folder.resolve("results"));
        for(var c:RoseExperiment.conditions(args[1].equals("quick"))) Files.writeString(folder.resolve("results/"+c.id()+".json"),encode(RoseExperiment.run(c)));
        regenerate(folder);
    }
    static String sample(RoseExperiment.Sample s) { return s.timeNs()+","+s.ok()+","+s.primary()+","+s.lag()+","+s.pending()+","+s.backup1Applied()+","+s.backup2Applied(); }
    static String encode(RoseExperiment.Measurement m) {
        var c=m.condition(); StringBuilder b=new StringBuilder("{\n  \"resultSchemaVersion\": \""+SCHEMA+"\",\n  \"modelVersion\": \"single-partition-op-bound-1\",\n  \"timeDomain\": \"virtual\",\n  \"timeUnit\": \"ns\",\n  \"variant\": \"ROSE_ADAPTATION\",\n  \"scenario\": \""+c.scenario()+"\",\n  \"limit\": "+c.limit()+",\n  \"intervalMs\": "+c.intervalMs()+",\n  \"valueBytes\": "+c.valueBytes()+",\n  \"mibps\": "+c.mibps());
        for(String n:RoseExperiment.FIELDS) b.append(",\n  \"").append(n).append("\": ").append(m.values().get(n));
        b.append(",\n  \"sampleColumns\": \"").append(SAMPLE).append("\",\n  \"samples\": [\n");
        return b.append(String.join(",\n",m.samples().stream().map(s->"    ["+sample(s)+"]").toList())).append("\n  ]\n}\n").toString();
    }
    static RoseExperiment.Measurement decode(String original) {
        String json=original.replace("\r\n","\n"); Map<String,Long> f=new LinkedHashMap<>();
        Matcher numbers=Pattern.compile("\"([A-Za-z][A-Za-z0-9]*)\": (-?[0-9]+)").matcher(json);
        while(numbers.find()) require(f.put(numbers.group(1),Long.parseLong(numbers.group(2)))==null,"duplicate numeric field");
        Matcher scenario=Pattern.compile("\"scenario\": \"([A-Z_]+)\"").matcher(json); require(scenario.find(),"missing scenario");
        var c=new RoseExperiment.Condition(Math.toIntExact(f.remove("limit")),RoseExperiment.Scenario.valueOf(scenario.group(1)),Math.toIntExact(f.remove("intervalMs")),
                Math.toIntExact(f.remove("valueBytes")),Math.toIntExact(f.remove("mibps")));
        List<RoseExperiment.Sample> samples=new ArrayList<>(); Matcher rows=Pattern.compile("    \\[(-?[0-9]+(?:,-?[0-9]+){6})\\]").matcher(json);
        while(rows.find()) { long[] n=Arrays.stream(rows.group(1).split(",")).mapToLong(Long::parseLong).toArray(); samples.add(new RoseExperiment.Sample(n[0],Math.toIntExact(n[1]),Math.toIntExact(n[2]),Math.toIntExact(n[3]),Math.toIntExact(n[4]),Math.toIntExact(n[5]),Math.toIntExact(n[6]))); }
        var m=new RoseExperiment.Measurement(c,f,samples); require(encode(m).equals(json),"unsupported schema or malformed original"); validate(m); return m;
    }
    static void validate(RoseExperiment.Measurement m) {
        var c=m.condition(); var f=m.values(); require(f.keySet().equals(new HashSet<>(RoseExperiment.FIELDS)),"numeric fields differ");
        for(String n:RoseExperiment.FIELDS) require(f.get(n)>=-1,"invalid negative metric");
        require(f.get("offered")==2000/c.intervalMs(),"offered differs");
        long outcomes=0; for(String n:List.of("ok","busy","timeout","unavailable","stale","redirect","pending")) { require(f.get(n)>=0,"negative count"); outcomes+=f.get(n); }
        require(outcomes==f.get("offered") && f.get("pending")==0,"outcome accounting differs");
        require(f.get("retainedOk")+f.get("lostPreCrash")==f.get("ok") && f.get("lostPreCrash")>=0 && f.get("lostPreCrash")<=f.get("preCrashOk") && f.get("preCrashOk")<=f.get("ok"),"loss accounting differs");
        if(c.limit()>=0) require(f.get("peakLag")<=Math.max(1,c.limit()) && f.get("lostPreCrash")<=c.limit(),"lag/loss bound violated");
        if(!c.crash()) require(f.get("lostPreCrash")==0 && f.get("preCrashOk")==0 && f.get("crashAtNs")==-1 && f.get("firstPostCrashOkNs")==-1 && f.get("readyAtNs")==-1 && f.get("primary")==0,"non-crash metadata differs");
        else require(f.get("crashAtNs")==ProtocolCluster.ms(1200) && f.get("readyAtNs")>=ProtocolCluster.ms(1400) && f.get("primary")>=1 && f.get("primary")<=2
                && (f.get("firstPostCrashOkNs")==-1 || f.get("firstPostCrashOkNs")>=f.get("readyAtNs")),"crash timing differs");
        require(f.get("drainNs")>=0 && f.get("drainNs")<=ProtocolCluster.ms(30000) && f.get("converged")>=0 && f.get("converged")<=1,"invalid drain/convergence");
        require(f.get("attemptedBytes")==f.get("deliveredBytes")+f.get("droppedBytes")+f.get("pendingBytes") && f.get("enqueuedBytes")>=f.get("deliveredBytes")+f.get("pendingBytes"),"network accounting differs");
        require(f.get("ok")==0?f.get("p50Ns")==-1 && f.get("p99Ns")==-1:f.get("p50Ns")>=0 && f.get("p99Ns")>=f.get("p50Ns"),"percentiles differ");
        var samples=m.samples(); require(samples.size()>=201 && samples.getFirst().timeNs()==0,"sample coverage differs");
        long oldOk=-1; for(int i=0;i<samples.size();i++) {var s=samples.get(i);
            require(s.timeNs()==ProtocolCluster.ms(i*10L) && s.ok()>=oldOk && s.ok()<=f.get("ok") && s.pending()>=0 && s.primary()>=0 && s.primary()<=2 && s.lag()>=-1 && s.backup1Applied()>=0 && s.backup2Applied()>=0,"invalid sample");
            if(c.limit()>=0) require(s.lag()<=Math.max(1,c.limit()),"sample exceeds window"); oldOk=s.ok();
        }
        require(samples.getLast().ok()==f.get("ok") && samples.getLast().timeNs()==ProtocolCluster.ms(2000)+f.get("drainNs"),"final sample differs");
    }
    public static void regenerate(Path folder) throws IOException {
        Files.deleteIfExists(folder.resolve("completion.properties")); List<Path> files;
        try(var paths=Files.list(folder.resolve("results"))) { files=paths.filter(p->p.toString().endsWith(".json")).sorted().toList(); }
        require(files.size()==30 || files.size()==240,"expected quick 30 or full 240 conditions");
        var expected=new HashSet<>(RoseExperiment.conditions(files.size()==30)); Set<RoseExperiment.Condition> seen=new HashSet<>();
        String prefix="limit,scenario,intervalMs,valueBytes,mibps";
        StringBuilder csv=new StringBuilder(prefix+","+String.join(",",RoseExperiment.FIELDS)+",failoverNs\n"),series=new StringBuilder(prefix+","+SAMPLE+"\n");
        StringBuilder md=new StringBuilder("# Rosé 기반 단일 파티션 bounded-lag 변형\n\n0~2초 open-loop, Primary crash는 1.2초, 감지 oracle 200ms. 논문 전체 재현이 아니다.\n\n");
        Properties launch=new Properties(); try(var reader=Files.newBufferedReader(folder.resolve("launch.properties"))) { launch.load(reader); }
        for(String n:List.of("suiteExecutionId","gitCommit","sourceHash","backendHash")) md.append("- ").append(n).append(": `").append(launch.getProperty(n,"unknown")).append("`\n");
        md.append("\n|L|시나리오|간격 ms|값 B|MiB/s|OK / 제출|BUSY|TIMEOUT|peak lag|장애 전 OK 유실|첫 post-crash OK ms|수렴|\n|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n");
        for(Path path:files) {
            var m=decode(Files.readString(path)); var c=m.condition(); var f=m.values(); require(expected.contains(c) && seen.add(c) && path.getFileName().toString().equals(c.id()+".json"),"coverage differs");
            String p=c.limit()+","+c.scenario()+","+c.intervalMs()+","+c.valueBytes()+","+c.mibps(); long failover=f.get("firstPostCrashOkNs")<0?-1:f.get("firstPostCrashOkNs")-f.get("crashAtNs");
            csv.append(p); for(String n:RoseExperiment.FIELDS) csv.append(',').append(f.get(n)); csv.append(',').append(failover).append('\n');
            for(var s:m.samples()) series.append(p).append(',').append(sample(s)).append('\n');
            md.append('|').append(c.limit()<0?"∞":c.limit()).append('|').append(c.scenario()).append('|').append(c.intervalMs()).append('|').append(c.valueBytes()).append('|').append(c.mibps())
                    .append('|').append(f.get("ok")).append('/').append(f.get("offered")).append('|').append(f.get("busy")).append('|').append(f.get("timeout")).append('|').append(f.get("peakLag"))
                    .append('|').append(f.get("lostPreCrash")).append('|').append(failover<0?"—":String.format(Locale.ROOT,"%.3f",failover/1e6)).append('|').append(f.get("converged")).append("|\n");
        }
        require(seen.equals(expected),"incomplete condition coverage");
        md.append("\n- L=0은 비교용 전원 적용 ACK 동기 모드, 양수는 op 수 상한, -1은 무제한 비동기다. 로컬 KV 비용이 0인 모델이므로 비동기 0ns는 실제 시스템의 성능이 아니다.\n"
                +"- finite L>0의 예약 로그 lag≤L, 성공 응답 유실≤L를 검사한다. L=0은 미응답 1건이 있어 peakLag 1이어도 성공 쓰기 유실은 0이다.\n"
                +"- SLOW: 0.5~1.5초 Backup 1 Append 전달 +200ms. PARTITION_REPAIR: 0.5~1.0초 Primary 격리. ISOLATED_CRASH: 1.0초 격리, 1.2초 종료.\n"
                +"- 단일 파티션·명령 하나를 적용 경계로 쓰며 다중 파티션 coordinated apply와 영속 WAL을 재현하지 않는다. 외부 master는 최장 생존 prefix를 선택해 peer로 나머지를 복구한다.\n"
                +"- 모든 제출은 서로 다른 key/operation이다. deadline 500ms, 실제 재시도 없이 이후 새 요청의 경로만 갱신한다. snapshot/읽기 강한 일관성 및 generic pipeline을 추가하지 않았다.\n"
                +"- 첫 post-crash OK는 crash 후 제출한 요청만 센다. 2초 뒤에는 새 probe를 보내지 않고 최대 30초 더 drain하므로 —를 영구 복구 불능으로 해석하지 않는다.\n"
                +"- 성공 지연만 보면 BUSY가 가려진다. OK/제출·상태별 count·lag 시계열·유실을 함께 해석한다. 실패는 취소가 아니다.\n");
        Files.writeString(folder.resolve("summary.csv"),csv); Files.writeString(folder.resolve("samples.csv"),series); Files.writeString(folder.resolve("results.md"),md);
        Files.writeString(folder.resolve("completion.properties"),"status=complete\nresultSchemaVersion="+SCHEMA+"\ncases="+files.size()+"\n");
    }
}
