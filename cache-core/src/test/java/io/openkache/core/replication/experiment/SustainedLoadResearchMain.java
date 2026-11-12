package io.openkache.core.replication.experiment;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.regex.*;
import static io.openkache.core.replication.experiment.ReplicationExperiment.require;

/** 30초 연속 부하 연구 backend. 원본으로부터 case/phase/sample long CSV를 만든다. */
public final class SustainedLoadResearchMain {
    private static final String SCHEMA="a1-sustained-json-1";
    static final String PHASE="phase,offered,ok,busy,timeout,unavailable,stale,redirect,other,pending,p50Ns,p99Ns,completedInWindow";
    static final String SAMPLE="timeNs,offered,ok,busy,timeout,unavailable,stale,redirect,other,pending,pendingBytes,pendingDataBytes,logLag,applyLag,kv0,kv1,kv2,viewAdvance";
    private static final String CSV="kind,variant,scenario,intervalMs,valueBytes,mibps,"+PHASE+",timeNs,pendingBytes,pendingDataBytes,logLag,applyLag,kv0,kv1,kv2,viewAdvance,loadMs,drainNs,converged,pendingAtLoadEnd,okDuringLoad,watermarkOk,watermarkCommit,catchupNs,verifiedOkCopies,missingOkCopies,divergentKeys,attemptedBytes,enqueuedBytes,deliveredBytes,droppedBytes,dataBytes,controlBytes,messages,peakPendingBytes,maxQueueWaitNs";
    private SustainedLoadResearchMain() {}
    public static void main(String[] args) throws IOException {
        if(args.length==2 && args[0].equals("--rebuild")) { regenerate(Path.of(args[1])); return; }
        if(args.length!=2 || !List.of("quick","full").contains(args[1])) throw new IllegalArgumentException("<output.dir> <quick|full> or --rebuild <output.dir>");
        Path folder=Path.of(args[0]); require(Files.isRegularFile(folder.resolve("launch.properties")),"run through common Runner");
        Files.createDirectory(folder.resolve("results"));
        for(var c:SustainedLoadExperiment.conditions(args[1].equals("quick"))) Files.writeString(folder.resolve("results/"+c.id()+".json"),encode(SustainedLoadExperiment.run(c)));
        regenerate(folder);
    }
    static String phaseRow(SustainedLoadExperiment.Phase p) {
        return p.phase()+","+p.offered()+","+p.ok()+","+p.busy()+","+p.timeout()+","+p.unavailable()+","+p.stale()+","+p.redirect()+","+p.other()+","+p.pending()+","+p.p50Ns()+","+p.p99Ns()+","+p.completedInWindow();
    }
    static String sampleRow(SustainedLoadExperiment.Sample s) {
        return s.timeNs()+","+s.offered()+","+s.ok()+","+s.busy()+","+s.timeout()+","+s.unavailable()+","+s.stale()+","+s.redirect()+","+s.other()+","+s.pending()+","+s.pendingBytes()+","+s.pendingDataBytes()+","+s.logLag()+","+s.applyLag()+","+s.kv0()+","+s.kv1()+","+s.kv2()+","+s.viewAdvance();
    }
    private static Map<String,Long> numeric(SustainedLoadExperiment.Measurement m) {
        var c=m.condition(); var n=m.network(); Map<String,Long> f=new LinkedHashMap<>();
        f.put("intervalMs",(long)c.intervalMs()); f.put("valueBytes",(long)c.valueBytes()); f.put("mibps",(long)c.mibps());
        f.put("loadMs",(long)m.loadMs()); f.put("drainNs",m.drainNs()); f.put("converged",m.converged()?1L:0L);
        f.put("pendingAtLoadEnd",(long)m.pendingAtLoadEnd()); f.put("okDuringLoad",(long)m.okDuringLoad());
        f.put("watermarkOk",(long)m.watermarkOk()); f.put("watermarkCommit",(long)m.watermarkCommit()); f.put("catchupNs",m.catchupNs());
        f.put("verifiedOkCopies",(long)m.verifiedOkCopies()); f.put("missingOkCopies",(long)m.missingOkCopies()); f.put("divergentKeys",(long)m.divergentKeys());
        f.put("attemptedBytes",n.attemptedBytes()); f.put("enqueuedBytes",n.enqueuedBytes()); f.put("deliveredBytes",n.deliveredBytes()); f.put("droppedBytes",n.droppedBytes()); f.put("pendingBytes",n.pendingBytes());
        f.put("dataBytes",n.dataBytes()); f.put("controlBytes",n.controlBytes()); f.put("messages",n.messages()); f.put("peakPendingBytes",n.peakPendingBytes()); f.put("maxQueueWaitNs",n.maxQueueWaitNs()); return f;
    }
    static String encode(SustainedLoadExperiment.Measurement m) {
        StringBuilder b=new StringBuilder("{\n  \"resultSchemaVersion\": \""+SCHEMA+"\",\n  \"modelVersion\": \"sustained-ack-delay-1\",\n  \"timeDomain\": \"virtual\",\n  \"timeUnit\": \"ns\",\n  \"variant\": \""+m.condition().variant()+"\",\n  \"scenario\": \""+m.condition().scenario()+"\"");
        numeric(m).forEach((k,v)->b.append(",\n  \"").append(k).append("\": ").append(v));
        b.append(",\n  \"phaseColumns\": \"").append(PHASE).append("\",\n  \"phases\": [\n");
        b.append(String.join(",\n",m.phases().stream().map(p->"    ["+phaseRow(p)+"]").toList())).append("\n  ],\n  \"sampleColumns\": \"").append(SAMPLE).append("\",\n  \"samples\": [\n");
        return b.append(String.join(",\n",m.samples().stream().map(s->"    ["+sampleRow(s)+"]").toList())).append("\n  ]\n}\n").toString();
    }
    static List<long[]> arrays(String json,String name,int length) {
        Matcher block=Pattern.compile("\""+name+"\": \\[\\n(.*?)\\n  \\]",Pattern.DOTALL).matcher(json);
        require(block.find(),"missing "+name); String contents=block.group(1);
        Matcher rows=Pattern.compile("\\[(-?[0-9]+(?:,-?[0-9]+)*)\\]").matcher(contents); List<long[]> result=new ArrayList<>();
        while(rows.find()) { long[] values=Arrays.stream(rows.group(1).split(",")).mapToLong(Long::parseLong).toArray(); require(values.length==length,"wrong array width"); result.add(values); }
        return result;
    }
    static SustainedLoadExperiment.Measurement decode(String original) {
        String json=original.replace("\r\n","\n"); Map<String,String> text=new HashMap<>(); Map<String,Long> f=new HashMap<>();
        Matcher strings=Pattern.compile("\"([A-Za-z]+)\": \"([^\"\\\\\r\n]*)\"").matcher(json);
        while(strings.find()) require(text.put(strings.group(1),strings.group(2))==null,"duplicate string");
        Matcher numbers=Pattern.compile("\"([A-Za-z]+)\": (-?[0-9]+)").matcher(json);
        while(numbers.find()) require(f.put(numbers.group(1),Long.parseLong(numbers.group(2)))==null,"duplicate number");
        var c=new SustainedLoadExperiment.Condition(text.get("variant"),text.get("scenario"),integer(f,"intervalMs"),integer(f,"valueBytes"),integer(f,"mibps"));
        List<SustainedLoadExperiment.Phase> phases=new ArrayList<>();
        for(long[] a:arrays(json,"phases",13)) phases.add(new SustainedLoadExperiment.Phase(i(a[0]),i(a[1]),i(a[2]),i(a[3]),i(a[4]),i(a[5]),i(a[6]),i(a[7]),i(a[8]),i(a[9]),a[10],a[11],i(a[12])));
        List<SustainedLoadExperiment.Sample> samples=new ArrayList<>();
        for(long[] a:arrays(json,"samples",18)) samples.add(new SustainedLoadExperiment.Sample(a[0],i(a[1]),i(a[2]),i(a[3]),i(a[4]),i(a[5]),i(a[6]),i(a[7]),i(a[8]),i(a[9]),a[10],a[11],i(a[12]),i(a[13]),a[14],a[15],a[16],i(a[17])));
        var n=new LinkNetwork.Stats(number(f,"attemptedBytes"),number(f,"enqueuedBytes"),number(f,"deliveredBytes"),number(f,"droppedBytes"),number(f,"pendingBytes"),number(f,"dataBytes"),number(f,"controlBytes"),number(f,"messages"),number(f,"peakPendingBytes"),number(f,"maxQueueWaitNs"));
        int converged=integer(f,"converged"); require(converged==0 || converged==1,"invalid boolean");
        var m=new SustainedLoadExperiment.Measurement(c,integer(f,"loadMs"),number(f,"drainNs"),converged==1,integer(f,"pendingAtLoadEnd"),integer(f,"okDuringLoad"),integer(f,"watermarkOk"),integer(f,"watermarkCommit"),number(f,"catchupNs"),integer(f,"verifiedOkCopies"),integer(f,"missingOkCopies"),integer(f,"divergentKeys"),n,List.copyOf(phases),List.copyOf(samples));
        require(encode(m).equals(json),"noncanonical or incompatible JSON"); return m;
    }
    private static int i(long n) { return Math.toIntExact(n); }
    private static long number(Map<String,Long> f,String key) { require(f.containsKey(key),"missing "+key); return f.get(key); }
    private static int integer(Map<String,Long> f,String key) { return i(number(f,key)); }
    static void validate(SustainedLoadExperiment.Measurement m) {
        require(SustainedLoadExperiment.conditions(false).contains(m.condition()) && m.loadMs()==SustainedLoadExperiment.LOAD_MS,"invalid condition/duration");
        require(m.drainNs()>=0 && m.drainNs()<=ProtocolCluster.ms(SustainedLoadExperiment.DRAIN_MS) && m.drainNs()%ProtocolCluster.ms(100)==0,"invalid drain duration");
        require(m.phases().size()==3 && m.samples().size()==(m.loadMs()+m.drainNs()/1_000_000)/100+1,"missing phases/samples");
        int offered=0,ok=0,windowOk=0;
        for(int index=0;index<3;index++) {
            var p=m.phases().get(index);
            require(p.phase()==index && p.offered()==m.loadMs()/3/m.condition().intervalMs(),"invalid cohort coverage");
            require(p.ok()>=0 && p.busy()>=0 && p.timeout()>=0 && p.unavailable()>=0 && p.stale()>=0 && p.redirect()>=0 && p.pending()>=0 && p.other()==0,"invalid cohort count");
            require(p.ok()+p.busy()+p.timeout()+p.unavailable()+p.stale()+p.redirect()+p.other()+p.pending()==p.offered(),"cohort outcomes do not sum");
            require(p.ok()==0?p.p50Ns()==-1 && p.p99Ns()==-1:p.p50Ns()>=0 && p.p99Ns()>=p.p50Ns() && p.p99Ns()<=ProtocolCluster.ms(1000),"invalid successful latency");
            require(p.completedInWindow()>=0,"invalid completion window count"); offered+=p.offered(); ok+=p.ok(); windowOk+=p.completedInWindow();
        }
        require(m.okDuringLoad()==windowOk && windowOk<=ok && m.watermarkOk()>=0 && m.watermarkOk()<=ok && m.pendingAtLoadEnd()>=0 && m.pendingAtLoadEnd()<=offered,"invalid completion totals");
        require(m.verifiedOkCopies()>=ok && m.verifiedOkCopies()<=ok*3 && m.missingOkCopies()>=0 && m.verifiedOkCopies()+m.missingOkCopies()==ok*3 && m.divergentKeys()>=0,"invalid audit totals");
        require(!m.converged() || m.missingOkCopies()==0 && m.divergentKeys()==0,"false convergence");
        require(m.catchupNs()==-1 || m.catchupNs()>=0 && m.catchupNs()<=ProtocolCluster.ms(m.loadMs()/3L)+m.drainNs(),"invalid catchup duration");
        if(m.condition().variant().equals("PB")) require(m.watermarkCommit()==-1 && m.catchupNs()==0,"invalid PB watermark");
        long previousTime=-1; int previousOffered=0,previousOk=0;
        for(var s:m.samples()) {
            require(previousTime<0?s.timeNs()==0:s.timeNs()==previousTime+ProtocolCluster.ms(100),"unordered/missing sample");
            require(s.offered()>=previousOffered && s.ok()>=previousOk && s.ok()+s.busy()+s.timeout()+s.unavailable()+s.stale()+s.redirect()+s.other()+s.pending()==s.offered(),"invalid sample outcomes");
            require(s.offered()==Math.min(offered,s.timeNs()/ProtocolCluster.ms(m.condition().intervalMs())+1)
                    && s.ok()>=0 && s.busy()>=0 && s.timeout()>=0 && s.unavailable()>=0 && s.stale()>=0 && s.redirect()>=0 && s.other()==0 && s.pending()>=0,"invalid sampled offered schedule/counts");
            require(s.pendingBytes()>=s.pendingDataBytes() && s.pendingDataBytes()>=0 && s.kv0()>=0 && s.kv1()>=0 && s.kv2()>=0 && s.viewAdvance()>=0,"invalid sampled state");
            require(m.condition().variant().equals("PB")?s.logLag()==-1 && s.applyLag()==-1:s.logLag()>=0 && s.applyLag()>=0,"invalid replica lag");
            previousTime=s.timeNs(); previousOffered=s.offered(); previousOk=s.ok();
        }
        var last=m.samples().getLast();
        require(last.offered()==offered && last.ok()==ok && last.timeNs()==ProtocolCluster.ms(m.loadMs())+m.drainNs(),"sample totals differ");
        require(m.samples().get(m.loadMs()/100).pending()==m.pendingAtLoadEnd(),"load-end pending differs");
        var n=m.network();
        require(n.attemptedBytes()>=0 && n.enqueuedBytes()>=0 && n.deliveredBytes()>=0 && n.droppedBytes()>=0 && n.pendingBytes()>=0 && n.dataBytes()>=0 && n.controlBytes()>=0 && n.messages()>0 && n.maxQueueWaitNs()>=0,"negative network counts");
        require(n.attemptedBytes()==n.deliveredBytes()+n.droppedBytes()+n.pendingBytes() && n.enqueuedBytes()==n.dataBytes()+n.controlBytes(),"network byte mismatch");
        require(n.pendingBytes()==last.pendingBytes() && n.peakPendingBytes()>=n.pendingBytes() && n.peakPendingBytes()<=n.enqueuedBytes() && n.enqueuedBytes()<=n.attemptedBytes() && n.enqueuedBytes()>=n.deliveredBytes()+n.pendingBytes(),"network sample differs");
    }
    private static Map<String,Long> fields(String names,String values) {
        String[] keys=names.split(","),cells=values.split(","); Map<String,Long> result=new HashMap<>();
        for(int j=0;j<keys.length;j++) result.put(keys[j],Long.parseLong(cells[j])); return result;
    }
    private static void csvRow(StringBuilder csv,String kind,SustainedLoadExperiment.Condition c,Map<String,Long> f) {
        csv.append(kind).append(',').append(c.variant()).append(',').append(c.scenario()).append(',').append(c.intervalMs()).append(',').append(c.valueBytes()).append(',').append(c.mibps());
        for(String key:CSV.substring(CSV.indexOf("phase,")).split(",")) { csv.append(','); if(f.containsKey(key)) csv.append(f.get(key)); }
        csv.append('\n');
    }
    public static void regenerate(Path folder) throws IOException {
        Files.deleteIfExists(folder.resolve("completion.properties")); List<Path> files;
        try(var stream=Files.list(folder.resolve("results"))) { files=stream.filter(p->p.toString().endsWith(".json")).sorted().toList(); }
        require(files.size()==16 || files.size()==48,"expected quick 16 or full 48 cases");
        var expected=new HashSet<>(SustainedLoadExperiment.conditions(files.size()==16)); Set<SustainedLoadExperiment.Condition> seen=new HashSet<>();
        StringBuilder csv=new StringBuilder(CSV+"\n"),md=new StringBuilder("# 30초 연속 쓰기·느린 Backup\n\n가상 시간, open-loop. 10~20초 ACK 지연 200ms 후 복구. deadline 1초, 최대 추가 관측 30초.\n\n");
        Properties launch=new Properties(); try(var reader=Files.newBufferedReader(folder.resolve("launch.properties"))) { launch.load(reader); }
        for(String key:List.of("suiteExecutionId","gitCommit","sourceHash","backendHash")) md.append("- ").append(key).append(": `").append(launch.getProperty(key,"unknown")).append("`\n");
        md.append("\n|방식|시나리오|입력 간격 ms|값 B|MiB/s|제출|OK|BUSY|TIMEOUT|부하 중 OK/s|중간 구간 p50 ms|p99 ms|watermark 따라잡기 ms|추가 관측 ms|수렴|\n|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|\n");
        for(Path file:files) {
            var m=decode(Files.readString(file)); validate(m); var c=m.condition();
            require(expected.contains(c) && seen.add(c) && file.getFileName().toString().equals(c.id()+".json"),"duplicate/misnamed condition");
            Map<String,Long> totals=numeric(m);
            for(String key:List.of("offered","ok","busy","timeout","unavailable","stale","redirect","other","pending")) totals.put(key,0L);
            for(var p:m.phases()) { var values=fields(PHASE,phaseRow(p)); csvRow(csv,"phase",c,values); for(String key:List.of("offered","ok","busy","timeout","unavailable","stale","redirect","other","pending")) totals.merge(key,values.get(key),Long::sum); }
            csvRow(csv,"case",c,totals);
            for(var sample:m.samples()) csvRow(csv,"sample",c,fields(SAMPLE,sampleRow(sample)));
            md.append('|').append(c.variant()).append('|').append(c.scenario()).append('|').append(c.intervalMs()).append('|').append(c.valueBytes()).append('|').append(c.mibps())
                    .append('|').append(totals.get("offered")).append('|').append(totals.get("ok")).append('|').append(totals.get("busy")).append('|').append(totals.get("timeout"))
                    .append('|').append(String.format(Locale.ROOT,"%.3f",m.okDuringLoad()/30.0)).append('|').append(ms(m.phases().get(1).p50Ns())).append('|').append(ms(m.phases().get(1).p99Ns()))
                    .append('|').append(ms(m.catchupNs())).append('|').append(ms(m.drainNs())).append('|').append(m.converged()?"YES":"NO").append("|\n");
        }
        require(seen.equals(expected),"condition coverage incomplete");
        md.append("\n## 해석\n\n- 입력은 응답을 기다리지 않는다. BUSY/실패도 요청 수에서 빼지 않는다. 입력 재시도는 없으며 향후 요청의 경로만 응답에서 갱신한다.\n"
                + "- p50/p99는 제출 구간별 성공 요청만의 정확한 nearest-rank 값이다. 실패는 별도 집계한다. OK/s는 0~30초에 실제 완료한 수 / 30초다.\n"
                + "- summary.csv의 case는 전체 요약, phase는 제출 구간별 결과, sample은 100ms 간격 누적 상태다. phase completedInWindow는 완료 시각 기준이다.\n"
                + "- 따라잡기는 20초의 committed watermark를 노드 2가 적용한 첫 샘플이다. PB는 성공 전에 모두 ACK하므로 성공 watermark 지연은 0이다.\n"
                + "- 정상 시나리오도 같은 3구간으로 나눈다. 새 요청을 30초에 중단한 뒤 최대 30초 추가 관측한다. NO는 관측 상한 내 데이터 수렴 미확인이다.\n"
                + "- 마지막에 모든 성공 키의 값·복사본을 검사한다. TIMEOUT/UNAVAILABLE 요청은 적용 여부가 불확실하므로 존재할 수 있다.\n"
                + "- 실제 서버 처리량이 아니며 동시 처리 1건, 전체 이력 보관, checkpoint 없음이라는 현재 구현 조건의 가상 결과다.\n");
        Files.writeString(folder.resolve("summary.csv"),csv); Files.writeString(folder.resolve("results.md"),md);
        Files.writeString(folder.resolve("completion.properties"),"status=complete\nresultSchemaVersion="+SCHEMA+"\ncases="+files.size()+"\n");
    }
    private static String ms(long nanos) { return nanos<0?"—":String.format(Locale.ROOT,"%.3f",nanos/1_000_000.0); }
}
