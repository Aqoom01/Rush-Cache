package io.openkache.core.replication.experiment;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.regex.*;
import static io.openkache.core.replication.experiment.ReplicationExperiment.require;

/** 공통 Runner용 PB/VR/Raft 선출 비교 backend. 원본에서만 CSV/보고서를 재생성한다. */
public final class ElectionResearchMain {
    private static final String SCHEMA="a1-election-json-1";
    private static final String NUMBERS="initialLeader,nextLeader,startupNs,faultAtNs,firstOkNs,attempts,preOk,retainedPre,postOk,retainedPost,timedOut,busy,unavailable,stale,redirects,minorityOk,inflightRetained,converged,elections,enqueuedBytes,messages,successP50Ns,successP99Ns";
    private static final List<String> NAMES=List.of(NUMBERS.split(","));
    record Row(ElectionComparison.Condition condition,Map<String,Long> values) {}
    private ElectionResearchMain() {}
    public static void main(String[] args) throws IOException {
        if(args.length==2 && args[0].equals("--rebuild")) { regenerate(Path.of(args[1])); return; }
        if(args.length!=2 || !List.of("quick","full").contains(args[1])) throw new IllegalArgumentException("<output.dir> <quick|full> or --rebuild <output.dir>");
        Path folder=Path.of(args[0]); require(Files.isRegularFile(folder.resolve("launch.properties")),"use common Runner");
        Files.createDirectory(folder.resolve("results"));
        for(var c:ElectionComparison.conditions(args[1].equals("quick"))) {
            var m=ElectionComparison.run(c); Files.writeString(folder.resolve("results/"+c.id()+".json"),encode(row(m)));
        }
        regenerate(folder);
    }
    static Row row(ElectionComparison.Measurement m) {
        long[] numbers={m.initialLeader(),m.nextLeader(),m.startupNs(),m.faultAtNs(),m.firstOkNs(),m.attempts(),m.preOk(),m.retainedPre(),
                m.postOk(),m.retainedPost(),m.timedOut(),m.busy(),m.unavailable(),m.stale(),m.redirects(),m.minorityOk(),m.inflightRetained(),
                m.converged(),m.elections(),m.enqueuedBytes(),m.messages(),m.successP50Ns(),m.successP99Ns()};
        Map<String,Long> values=new LinkedHashMap<>(); for(int i=0;i<numbers.length;i++) values.put(NAMES.get(i),numbers[i]);
        return new Row(m.condition(),values);
    }
    static String encode(Row row) {
        var c=row.condition(); StringBuilder b=new StringBuilder("{\n  \"resultSchemaVersion\": \""+SCHEMA+"\",\n  \"modelVersion\": \"fixed-three-election-1\",\n  \"timeDomain\": \"virtual\",\n  \"timeUnit\": \"ns\",\n  \"variant\": \""+c.variant()+"\",\n  \"scenario\": \""+c.scenario()+"\",\n  \"seed\": "+c.seed()+",\n  \"valueBytes\": "+c.valueBytes()+",\n  \"mibps\": "+c.mibps());
        for(String name:NAMES) b.append(",\n  \"").append(name).append("\": ").append(row.values().get(name));
        return b.append("\n}\n").toString();
    }
    static Row decode(String original) {
        String json=original.replace("\r\n","\n"); Map<String,String> strings=new HashMap<>(); Map<String,Long> numbers=new LinkedHashMap<>();
        Matcher text=Pattern.compile("\"([A-Za-z]+)\": \"([^\"\\\\\r\n]*)\"").matcher(json);
        while(text.find()) require(strings.put(text.group(1),text.group(2))==null,"duplicate string");
        Matcher value=Pattern.compile("\"([A-Za-z][A-Za-z0-9]*)\": (-?[0-9]+)").matcher(json);
        while(value.find()) require(numbers.put(value.group(1),Long.parseLong(value.group(2)))==null,"duplicate number");
        var c=new ElectionComparison.Condition(strings.get("variant"),ElectionComparison.Scenario.valueOf(strings.get("scenario")),
                Math.toIntExact(numbers.remove("seed")),Math.toIntExact(numbers.remove("valueBytes")),Math.toIntExact(numbers.remove("mibps")));
        Row row=new Row(c,numbers); require(encode(row).equals(json),"unsupported schema or malformed original"); validate(row); return row;
    }
    static void validate(Row row) {
        var v=row.values(); require(v.keySet().equals(new HashSet<>(NAMES)),"fields differ");
        for(String name:NAMES) require(v.get(name)>=-1,"invalid negative metric");
        require(v.get("initialLeader")>=0 && v.get("initialLeader")<3 && v.get("nextLeader")>=0 && v.get("nextLeader")<3,"invalid leader");
        require(v.get("preOk")==64 && v.get("retainedPre").equals(v.get("preOk")),"pre OK lost");
        require(v.get("postOk")>=0 && v.get("postOk")<=16 && v.get("retainedPost").equals(v.get("postOk")),"post OK lost");
        long attempts=v.get("postOk"); for(String n:List.of("timedOut","busy","unavailable","stale","redirects")) { require(v.get(n)>=0,"negative count"); attempts+=v.get(n); }
        require(v.get("attempts")==attempts,"attempts/outcomes differ");
        require(v.get("startupNs")>=0 && v.get("faultAtNs")>=v.get("startupNs"),"time order differs");
        require(v.get("postOk")==0?v.get("firstOkNs")==-1:v.get("firstOkNs")>=v.get("faultAtNs"),"first OK differs");
        require(v.get("converged")>=0 && v.get("converged")<=1 && v.get("minorityOk")>=0 && v.get("minorityOk")<=1
                && v.get("inflightRetained")>=0 && v.get("inflightRetained")<=1,"invalid flag");
        if(!row.condition().variant().equals("PB")) require(v.get("minorityOk")==0,"minority committed");
        require(v.get("enqueuedBytes")>0 && v.get("messages")>0,"missing network metrics");
        require(row.condition().variant().equals("RAFT")?v.get("elections")>0:v.get("elections")==-1,"election counter differs");
        require(v.get("postOk")==0?v.get("successP50Ns")==-1 && v.get("successP99Ns")==-1:
                v.get("successP50Ns")>=0 && v.get("successP99Ns")>=v.get("successP50Ns"),"percentiles differ");
    }
    public static void regenerate(Path folder) throws IOException {
        regenerate(folder,false);
    }
    static void regenerate(Path folder,boolean includeChain) throws IOException {
        Files.deleteIfExists(folder.resolve("completion.properties")); List<Path> files;
        try(var paths=Files.list(folder.resolve("results"))) { files=paths.filter(p->p.toString().endsWith(".json")).sorted().toList(); }
        require(includeChain?files.size()==48 || files.size()==288:files.size()==36 || files.size()==216,"condition count differs");
        var expected=new HashSet<>(includeChain?ChainResearchMain.conditions(files.size()==48):ElectionComparison.conditions(files.size()==36)); Set<ElectionComparison.Condition> seen=new HashSet<>();
        StringBuilder csv=new StringBuilder("variant,scenario,seed,valueBytes,mibps,"+NUMBERS+",firstOkDelayNs\n");
        StringBuilder md=new StringBuilder(includeChain?"# PB / VR / Raft / Chain 비교\n\n가상 시간, 초기 3노드, 1ms 전파 지연, 방향별 FIFO 링크.\n\n":
                "# PB / VR / Raft 선출 비교\n\n가상 시간, 고정 3노드, 1ms 전파 지연, 방향별 FIFO 링크.\n\n");
        Properties launch=new Properties(); try(var reader=Files.newBufferedReader(folder.resolve("launch.properties"))) { launch.load(reader); }
        for(String n:List.of("suiteExecutionId","gitCommit","sourceHash","backendHash")) md.append("- ").append(n).append(": `").append(launch.getProperty(n,"unknown")).append("`\n");
        md.append("\n|방식|시나리오|seed|값 B|MiB/s|첫 OK ms|성공 / 16|장애 전 보존|장애 후 보존|KV 일치|\n|---|---|---:|---:|---:|---:|---:|---:|---:|---:|\n");
        for(Path file:files) {
            Row row=decode(Files.readString(file)); var c=row.condition(); var v=row.values();
            require(expected.contains(c) && seen.add(c) && file.getFileName().toString().equals(c.id()+".json"),"duplicate/misnamed condition");
            long delay=v.get("firstOkNs")<0?-1:v.get("firstOkNs")-v.get("faultAtNs");
            csv.append(c.variant()).append(',').append(c.scenario()).append(',').append(c.seed()).append(',').append(c.valueBytes()).append(',').append(c.mibps());
            for(String n:NAMES) csv.append(',').append(v.get(n)); csv.append(',').append(delay).append('\n');
            md.append('|').append(c.variant()).append('|').append(c.scenario()).append('|').append(c.seed()).append('|').append(c.valueBytes()).append('|').append(c.mibps())
                    .append('|').append(delay<0?"—":String.format(Locale.ROOT,"%.3f",delay/1_000_000.0)).append('|').append(v.get("postOk")).append('|').append(v.get("retainedPre"))
                    .append('|').append(v.get("retainedPost")).append('|').append(v.get("converged")).append("|\n");
        }
        require(seen.equals(expected),"coverage incomplete");
        md.append("\n- 각 조건은 정상 쓰기 64건 이후 새 쓰기 16건. 실패 응답도 attempts 및 상태별 count에 포함한다.\n"
                +"- 클라이언트는 같은 요청을 재시도하며 응답 epoch·Redirect와 round-robin으로 경로를 찾는다. 재시도 10ms, 시도 deadline 최대 1000ms, 논리 쓰기당 5초 예산.\n"
                +"- 첫 OK는 faultAt 이후 첫 새 쓰기의 성공 callback 시각. NORMAL/SLOW/REJOIN 값은 장애 failover가 아니다.\n"
                +"- p50/p99는 성공한 마지막 시도의 RTT만이며 재시도 포함 논리 지연과 다르다.\n"
                +"- PB/VR timeout 200ms, Raft election timeout [200,400)ms. 동일한 감지 분포가 아니므로 프로토콜 자체 속도 순위를 주장하지 않는다.\n"
                +"- PB 생존 KV, VR/Raft 생존 로그에서 성공 ID·명령·값을 검사한다. KV 일치는 별도이며 미완료 요청은 남을 수 있다.\n"
                +"- SLOW는 Backup 한 대의 ACK +200ms. INFLIGHT는 ACK를 차단하고 쓰기 제출 40ms 뒤 리더 종료. PARTITION은 옛 리더 격리 및 소수 쓰기 확인 후 repair.\n"
                +"- REJOIN은 follower 격리 후 4개 논리 쓰기를 시도하고 repair하며 2초 대기한다. PB는 전원 ACK 때문에 이 구간의 쓰기를 진행하지 못할 수 있다.\n"
                +"- 결과 확인 전 모든 조건에서 지연을 제거하고 6초 더 관측한다. enqueuedBytes/messages는 startup·warmup·최종 관측을 포함한다.\n"
                +"- Raft term/vote/log는 메모리만 유지. 프로세스 재시작·디스크·TCP·snapshot·동시 쓰기 확대는 범위 밖이다.\n");
        if(includeChain) md.append("\n## Chain의 별도 전제\n\n- Chain의 외부 master는 정확한 영구 crash를 200ms 뒤 통지받는 실험 모델이며 살아 있는 분단 노드를 제거하지 않는다.\n"
                +"- 전체 생존자 freeze/적용 완료 → peer suffix 복구 → tail Ack 역전파 확인 → 전 생존자 open 순서다. master 조정은 freeze/open 각각 2ms이며 데이터 링크 통계와 분리한다.\n"
                +"- tail 확정 결과는 CacheEngine future를 가진 head로 한 번 중계한다. 역방향 Ack와 별개이고 실제 tail→client 연결은 없다.\n"
                +"- SLOW는 tail의 Ack 및 완료 중계에 +200ms. 읽기는 tail만 제공하고 CRAQ는 포함하지 않는다. 성공 보존은 생존 로그로 확인한다.\n"
                +"- PARTITION에서 쓰기가 멈추는 것은 안전한 master 가정에 따른 결과다. 이 실험에 분단 노드 강제 제외를 추가하지 않는다.\n"
                +"- [head/middle/tail 별도 crash 원본 요약](position-results.md) · [위치별 CSV](position-summary.csv)\n");
        Files.writeString(folder.resolve("summary.csv"),csv); Files.writeString(folder.resolve("results.md"),md);
        Files.writeString(folder.resolve("completion.properties"),"status=complete\nresultSchemaVersion="+SCHEMA+"\ncases="+files.size()+"\n");
    }
}
