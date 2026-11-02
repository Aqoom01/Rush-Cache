package io.openkache.core.replication.experiment;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.regex.*;
import static io.openkache.core.replication.experiment.ReplicationExperiment.require;

/** PB/VR 연구 결과를 공통 Runner 계약으로 기록한다. 고정 schema 원본에서만 표를 재생성한다. */
public final class ProtocolResearchMain {
    private static final String SCHEMA="a1-protocol-json-1";
    private static final String MODEL="a1-vr-full-log-1";
    private static final String HEADER="variant,scenario,delay,seed,responseMs,recoveryMs,ok,timeout,divergent,preserved,messages,logEntries";
    private ProtocolResearchMain() {}
    public static void main(String[] args) throws IOException {
        if(args.length==2 && args[0].equals("--rebuild")) { regenerate(Path.of(args[1])); return; }
        if(args.length!=2 || !List.of("quick","full").contains(args[1])) throw new IllegalArgumentException("<output.dir> <quick|full> or --rebuild <output.dir>");
        Path folder=Path.of(args[0]); require(Files.isRegularFile(folder.resolve("launch.properties")),"run through common Runner");
        Files.createDirectory(folder.resolve("results"));
        int seeds=args[1].equals("quick")?1:5;
        for(String variant:List.of("PB","VR")) for(var scenario:ProtocolComparison.Scenario.values())
            for(int delay:new int[]{1,5}) for(int seed=1;seed<=seeds;seed++) {
                var m=ProtocolComparison.run(variant,scenario,delay,seed);
                Files.writeString(folder.resolve("results/"+id(m)+".json"),encode(m));
            }
        regenerate(folder);
    }
    private static String id(ProtocolComparison.Measurement m) { return m.variant()+"-"+m.scenario()+"-d"+m.delay()+"-s"+m.seed(); }
    private static String row(ProtocolComparison.Measurement m) {
        return m.variant()+","+m.scenario()+","+m.delay()+","+m.seed()+","+m.responseMs()+","+m.recoveryMs()+","+m.ok()+","+m.timeout()+","+m.divergent()+","+m.preserved()+","+m.messages()+","+m.logEntries();
    }
    static String encode(ProtocolComparison.Measurement m) {
        require(!m.trace().contains("\"") && !m.trace().contains("\\") && !m.trace().contains("\n") && !m.trace().contains("\r"),"unsupported trace character");
        String[] values=row(m).split(","),names=HEADER.split(",");
        StringBuilder b=new StringBuilder("{\n  \"resultSchemaVersion\": \""+SCHEMA+"\",\n  \"modelVersion\": \""+MODEL+"\",\n  \"timeDomain\": \"virtual\",\n  \"timeUnit\": \"ms\"");
        for(int i=0;i<names.length;i++) b.append(",\n  \"").append(names[i]).append("\": ").append(i<2?"\""+values[i]+"\"":values[i]);
        return b.append(",\n  \"trace\": \"").append(m.trace()).append("\"\n}\n").toString();
    }
    static ProtocolComparison.Measurement decode(String json) {
        Map<String,String> fields=new HashMap<>();
        Matcher strings=Pattern.compile("\"([A-Za-z]+)\": \"([^\"\\\\\r\n]*)\"").matcher(json);
        while(strings.find()) require(fields.put(strings.group(1),strings.group(2))==null,"duplicate string field");
        Matcher numbers=Pattern.compile("\"([A-Za-z]+)\": (-?[0-9]+)").matcher(json);
        while(numbers.find()) require(fields.put(numbers.group(1),numbers.group(2))==null,"duplicate number field");
        for(String name:HEADER.split(",")) require(fields.containsKey(name),"missing "+name);
        require(fields.containsKey("trace"),"missing trace");
        var m=new ProtocolComparison.Measurement(fields.get("variant"),fields.get("scenario"),integer(fields,"delay"),integer(fields,"seed"),
                number(fields,"responseMs"),number(fields,"recoveryMs"),integer(fields,"ok"),integer(fields,"timeout"),integer(fields,"divergent"),
                integer(fields,"preserved"),integer(fields,"messages"),integer(fields,"logEntries"),fields.get("trace"));
        require(encode(m).equals(json.replace("\r\n","\n")),"noncanonical or incompatible JSON"); return m;
    }
    private static long number(Map<String,String> f,String key) { return Long.parseLong(f.get(key)); }
    private static int integer(Map<String,String> f,String key) { return Math.toIntExact(number(f,key)); }
    static void validate(ProtocolComparison.Measurement m) {
        require(List.of("PB","VR").contains(m.variant()) && List.of(1,5).contains(m.delay()) && m.seed()>=1 && m.seed()<=5,"invalid condition");
        var scenario=ProtocolComparison.Scenario.valueOf(m.scenario());
        boolean direct=scenario==ProtocolComparison.Scenario.NORMAL || scenario==ProtocolComparison.Scenario.SLOW_BACKUP || scenario==ProtocolComparison.Scenario.BACKUP_PARTITION;
        boolean blocked=m.variant().equals("PB") && (scenario==ProtocolComparison.Scenario.SLOW_BACKUP || scenario==ProtocolComparison.Scenario.BACKUP_PARTITION);
        boolean divergent=m.variant().equals("PB") && (blocked || scenario==ProtocolComparison.Scenario.PARTIAL_CRASH || scenario==ProtocolComparison.Scenario.PARTIAL_PARTITION);
        require(m.ok()==(blocked?0:1) && m.timeout()==(blocked || scenario==ProtocolComparison.Scenario.PARTIAL_PARTITION?1:0),"invalid availability result");
        require(m.divergent()==(divergent?1:0) && m.preserved()==(direct && scenario!=ProtocolComparison.Scenario.BACKUP_PARTITION?6:4),"invalid data audit");
        require((direct && m.responseMs()>=2L*m.delay() && m.recoveryMs()==-1)
                || (!direct && m.responseMs()==-1 && m.recoveryMs()>=0),"invalid timings");
        require(!blocked || m.responseMs()==60,"timeout boundary changed");
        require(m.messages()>0 && m.logEntries()>=(m.variant().equals("VR")?1:0)
                && (!m.variant().equals("PB") || m.logEntries()==0) && m.trace().contains("audit node="),"invalid evidence");
    }
    public static void regenerate(Path folder) throws IOException {
        Files.deleteIfExists(folder.resolve("completion.properties"));
        List<Path> files;
        try(var stream=Files.list(folder.resolve("results"))) { files=stream.filter(p->p.toString().endsWith(".json")).sorted().toList(); }
        require(files.size()==32 || files.size()==160,"expected quick 32 or full 160 cases");
        int seeds=files.size()==32?1:5; Set<String> ids=new HashSet<>(); List<ProtocolComparison.Measurement> rows=new ArrayList<>();
        for(Path file:files) {
            var m=decode(Files.readString(file)); validate(m);
            require(m.seed()<=seeds && ids.add(id(m)) && file.getFileName().toString().equals(id(m)+".json"),"duplicate/misnamed/mixed condition"); rows.add(m);
        }
        StringBuilder csv=new StringBuilder(HEADER+"\n"),md=new StringBuilder("# PB / VR 비교 실험\n\n가상 ms. heartbeat 10ms, 감지 timeout 100ms, jitter 0~3ms, 클라이언트 순차 재시도 5ms.\n\n");
        Properties launch=new Properties(); try(var reader=Files.newBufferedReader(folder.resolve("launch.properties"))) { launch.load(reader); }
        for(String key:List.of("suiteExecutionId","gitCommit","sourceHash","backendHash")) md.append("- ").append(key).append(": `").append(launch.getProperty(key,"unknown")).append("`\n");
        md.append("\n|방식|시나리오|편도 지연|seed|응답|복구|OK|TIMEOUT|불일치 조건|보존된 marker 복사본|메시지|전체 로그 전송 entry 합|\n|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n");
        for(var m:rows) { csv.append(row(m)).append('\n'); md.append('|').append(row(m).replace(',', '|')).append("|\n"); }
        md.append("\nresponseMs/recoveryMs=-1은 해당 없음이다. PB의 의도된 TIMEOUT·미완료 쓰기 불일치는 기준형의 한계 관측이며 실험 실행 실패가 아니다.\n"
                + "\n분단 안전성·미완료 로그 복구는 VR에서 구현·검증했다. PB는 crash-only 기준형을 유지한다. 읽기는 VR 로그에 포함한다.\n"
                + "VR은 전체 로그 전송을 사용하므로 메시지 개수와 실제 bytes/SSD/TCP 성능은 동등하지 않다. 메시지는 전송 시도 수다.\n"
                + "seed마다 실제 지연을 생성하지만 프로토콜별 메시지 수가 달라 동일 seed가 동일한 packet trace를 뜻하지 않는다.\n"
                + "marker 보존은 일부 고유 키 확인이며 전체 선형화 가능성의 증명이 아니다. 별도의 작은 읽기/쓰기 이력 검사와 반복 분단 테스트를 수행한다.\n");
        Files.writeString(folder.resolve("summary.csv"),csv); Files.writeString(folder.resolve("results.md"),md);
        Files.writeString(folder.resolve("completion.properties"),"status=complete\nresultSchemaVersion="+SCHEMA+"\ncases="+files.size()+"\n");
    }
}
