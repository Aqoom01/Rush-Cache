package io.openkache.core.replication.experiment;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.regex.*;
import static io.openkache.core.replication.experiment.ReplicationExperiment.require;

/** 연속 부하 중 Primary 종료 실험. 보존 evidence와 실제 KV 적용을 별도 기록한다. */
public final class LoadCrashResearchMain {
    private static final String SCHEMA="a1-load-crash-json-1";
    private static final String PHASE=SustainedLoadResearchMain.PHASE;
    private static final String SAMPLE=SustainedLoadResearchMain.SAMPLE.replace("pendingDataBytes","pendingLiveDataBytes");
    private static final String CSV="kind,variant,scenario,intervalMs,valueBytes,mibps,"+PHASE+",timeNs,pendingBytes,pendingLiveDataBytes,logLag,applyLag,kv0,kv1,kv2,viewAdvance,loadMs,crashAtNs,firstPostCrashOkNs,failoverNs,preCrashOk,preCrashPending,evidenceCopiesAtCrash,retainedPreCrashAtEnd,retainedAllOkAtEnd,verifiedPreCrashKvCopies,verifiedAllOkKvCopies,inflightRetainedAtEnd,drainNs,converged,pendingAtLoadEnd,okDuringLoad,divergentKeys,attemptedBytes,enqueuedBytes,deliveredBytes,droppedBytes,dataBytes,controlBytes,messages,peakPendingBytes,maxQueueWaitNs";
    private LoadCrashResearchMain() {}
    public static void main(String[] args) throws IOException {
        if(args.length==2 && args[0].equals("--rebuild")) {regenerate(Path.of(args[1]));return;}
        if(args.length!=2 || !List.of("quick","full").contains(args[1])) throw new IllegalArgumentException("<output.dir> <quick|full> or --rebuild <output.dir>");
        Path folder=Path.of(args[0]);require(Files.isRegularFile(folder.resolve("launch.properties")),"run through common Runner");Files.createDirectory(folder.resolve("results"));
        for(var c:LoadCrashExperiment.conditions(args[1].equals("quick"))) Files.writeString(folder.resolve("results/"+c.id()+".json"),encode(LoadCrashExperiment.run(c)));
        regenerate(folder);
    }
    private static Map<String,Long> numeric(LoadCrashExperiment.Measurement m) {
        var c=m.condition();var n=m.network();Map<String,Long> f=new LinkedHashMap<>();
        f.put("intervalMs",(long)c.intervalMs());f.put("valueBytes",(long)c.valueBytes());f.put("mibps",(long)c.mibps());f.put("loadMs",(long)m.loadMs());
        f.put("crashAtNs",m.crashAtNs());f.put("firstPostCrashOkNs",m.firstPostCrashOkNs());f.put("failoverNs",m.firstPostCrashOkNs()<0?-1:m.firstPostCrashOkNs()-m.crashAtNs());
        f.put("preCrashOk",(long)m.preCrashOk());f.put("preCrashPending",(long)m.preCrashPending());f.put("evidenceCopiesAtCrash",(long)m.evidenceCopiesAtCrash());
        f.put("retainedPreCrashAtEnd",(long)m.retainedPreCrashAtEnd());f.put("retainedAllOkAtEnd",(long)m.retainedAllOkAtEnd());
        f.put("verifiedPreCrashKvCopies",(long)m.verifiedPreCrashKvCopies());f.put("verifiedAllOkKvCopies",(long)m.verifiedAllOkKvCopies());f.put("inflightRetainedAtEnd",(long)m.inflightRetainedAtEnd());
        f.put("drainNs",m.drainNs());f.put("converged",m.converged()?1L:0L);f.put("pendingAtLoadEnd",(long)m.pendingAtLoadEnd());f.put("okDuringLoad",(long)m.okDuringLoad());f.put("divergentKeys",(long)m.divergentKeys());
        f.put("attemptedBytes",n.attemptedBytes());f.put("enqueuedBytes",n.enqueuedBytes());f.put("deliveredBytes",n.deliveredBytes());f.put("droppedBytes",n.droppedBytes());f.put("pendingBytes",n.pendingBytes());
        f.put("dataBytes",n.dataBytes());f.put("controlBytes",n.controlBytes());f.put("messages",n.messages());f.put("peakPendingBytes",n.peakPendingBytes());f.put("maxQueueWaitNs",n.maxQueueWaitNs());return f;
    }
    static String encode(LoadCrashExperiment.Measurement m) {
        StringBuilder b=new StringBuilder("{\n  \"resultSchemaVersion\": \""+SCHEMA+"\",\n  \"modelVersion\": \"sustained-primary-crash-1\",\n  \"timeDomain\": \"virtual\",\n  \"timeUnit\": \"ns\",\n  \"variant\": \""+m.condition().variant()+"\",\n  \"scenario\": \""+m.condition().scenario()+"\",\n  \"retentionEvidence\": \""+(m.condition().variant().equals("PB")?"survivorKV":"survivorLogs")+"\"");
        numeric(m).forEach((k,v)->b.append(",\n  \"").append(k).append("\": ").append(v));
        b.append(",\n  \"phaseColumns\": \"").append(PHASE).append("\",\n  \"phases\": [\n");
        b.append(String.join(",\n",m.phases().stream().map(p->"    ["+SustainedLoadResearchMain.phaseRow(p)+"]").toList())).append("\n  ],\n  \"sampleColumns\": \"").append(SAMPLE).append("\",\n  \"samples\": [\n");
        return b.append(String.join(",\n",m.samples().stream().map(s->"    ["+SustainedLoadResearchMain.sampleRow(s)+"]").toList())).append("\n  ]\n}\n").toString();
    }
    static LoadCrashExperiment.Measurement decode(String original) {
        String json=original.replace("\r\n","\n");Map<String,String> text=new HashMap<>();Map<String,Long> f=new HashMap<>();
        Matcher strings=Pattern.compile("\"([A-Za-z]+)\": \"([^\"\\\\\r\n]*)\"").matcher(json);
        while(strings.find()) require(text.put(strings.group(1),strings.group(2))==null,"duplicate string");
        Matcher numbers=Pattern.compile("\"([A-Za-z]+)\": (-?[0-9]+)").matcher(json);
        while(numbers.find()) require(f.put(numbers.group(1),Long.parseLong(numbers.group(2)))==null,"duplicate number");
        var c=new LoadCrashExperiment.Condition(text.get("variant"),text.get("scenario"),integer(f,"intervalMs"),integer(f,"valueBytes"),integer(f,"mibps"));
        List<SustainedLoadExperiment.Phase> phases=new ArrayList<>();
        for(long[] a:SustainedLoadResearchMain.arrays(json,"phases",13)) phases.add(new SustainedLoadExperiment.Phase(i(a[0]),i(a[1]),i(a[2]),i(a[3]),i(a[4]),i(a[5]),i(a[6]),i(a[7]),i(a[8]),i(a[9]),a[10],a[11],i(a[12])));
        List<SustainedLoadExperiment.Sample> samples=new ArrayList<>();
        for(long[] a:SustainedLoadResearchMain.arrays(json,"samples",18)) samples.add(new SustainedLoadExperiment.Sample(a[0],i(a[1]),i(a[2]),i(a[3]),i(a[4]),i(a[5]),i(a[6]),i(a[7]),i(a[8]),i(a[9]),a[10],a[11],i(a[12]),i(a[13]),a[14],a[15],a[16],i(a[17])));
        var n=new LinkNetwork.Stats(number(f,"attemptedBytes"),number(f,"enqueuedBytes"),number(f,"deliveredBytes"),number(f,"droppedBytes"),number(f,"pendingBytes"),number(f,"dataBytes"),number(f,"controlBytes"),number(f,"messages"),number(f,"peakPendingBytes"),number(f,"maxQueueWaitNs"));
        int converged=integer(f,"converged");require(converged==0 || converged==1,"invalid boolean");
        var m=new LoadCrashExperiment.Measurement(c,integer(f,"loadMs"),number(f,"crashAtNs"),number(f,"firstPostCrashOkNs"),integer(f,"preCrashOk"),integer(f,"preCrashPending"),integer(f,"evidenceCopiesAtCrash"),integer(f,"retainedPreCrashAtEnd"),integer(f,"retainedAllOkAtEnd"),integer(f,"verifiedPreCrashKvCopies"),integer(f,"verifiedAllOkKvCopies"),integer(f,"inflightRetainedAtEnd"),number(f,"drainNs"),converged==1,integer(f,"pendingAtLoadEnd"),integer(f,"okDuringLoad"),integer(f,"divergentKeys"),n,List.copyOf(phases),List.copyOf(samples));
        require(encode(m).equals(json),"noncanonical or incompatible JSON");return m;
    }
    private static long number(Map<String,Long> f,String key) {require(f.containsKey(key),"missing "+key);return f.get(key);}
    private static int integer(Map<String,Long> f,String key) {return i(number(f,key));}
    private static int i(long n) {return Math.toIntExact(n);}
    static void validate(LoadCrashExperiment.Measurement m) {
        var c=m.condition();require(LoadCrashExperiment.conditions(false).contains(c) && m.loadMs()==30000 && m.crashAtNs()==ProtocolCluster.ms(11000),"invalid model inputs");
        require(m.drainNs()>=0 && m.drainNs()<=ProtocolCluster.ms(30000) && m.drainNs()%ProtocolCluster.ms(100)==0,"invalid observation limit");
        require(m.phases().size()==2 && m.samples().size()==301+m.drainNs()/ProtocolCluster.ms(100),"incomplete phase/sample coverage");
        int offered=0,ok=0,windows=0;
        for(int index=0;index<2;index++) {
            var p=m.phases().get(index);int duration=index==0?11000:19000;
            require(p.phase()==index && p.offered()==duration/c.intervalMs(),"incorrect offered cohort");
            require(p.ok()>=0 && p.busy()>=0 && p.timeout()>=0 && p.unavailable()>=0 && p.stale()>=0 && p.redirect()>=0 && p.pending()>=0 && p.other()==0,"invalid outcomes");
            require(p.ok()+p.busy()+p.timeout()+p.unavailable()+p.stale()+p.redirect()+p.pending()==p.offered(),"outcome sum differs");
            require(p.ok()==0?p.p50Ns()==-1 && p.p99Ns()==-1:p.p50Ns()>=0 && p.p99Ns()>=p.p50Ns() && p.p99Ns()<=ProtocolCluster.ms(1000),"invalid successful latency");
            require(p.completedInWindow()>=0,"negative completion count");offered+=p.offered();ok+=p.ok();windows+=p.completedInWindow();
        }
        require(m.preCrashOk()==m.phases().get(0).ok() && m.preCrashOk()==m.phases().get(0).completedInWindow() && m.preCrashOk()>0,"pre-crash boundary differs");
        require(m.preCrashPending()>=0 && m.evidenceCopiesAtCrash()>=m.preCrashOk() && m.evidenceCopiesAtCrash()<=2*m.preCrashOk(),"invalid pre-crash evidence");
        require(m.retainedPreCrashAtEnd()==m.preCrashOk() && m.retainedAllOkAtEnd()==ok,"lost successful request evidence");
        require(m.verifiedPreCrashKvCopies()>=0 && m.verifiedPreCrashKvCopies()<=m.preCrashOk()*2 && m.verifiedAllOkKvCopies()>=m.verifiedPreCrashKvCopies() && m.verifiedAllOkKvCopies()<=ok*2,"invalid applied copies");
        require(m.inflightRetainedAtEnd()>=0 && m.inflightRetainedAtEnd()<=m.preCrashPending() && m.pendingAtLoadEnd()>=0 && m.pendingAtLoadEnd()<=offered && m.divergentKeys()>=0,"invalid outstanding/audit count");
        require(m.phases().get(1).ok()==0?m.firstPostCrashOkNs()==-1:m.firstPostCrashOkNs()>=m.crashAtNs() && m.firstPostCrashOkNs()<=ProtocolCluster.ms(m.loadMs())+m.drainNs(),"first resumed OK differs");
        require(m.okDuringLoad()==windows && windows<=ok,"completion windows differ");
        if(c.variant().equals("PB")) require(m.evidenceCopiesAtCrash()==m.preCrashOk()*2 && m.verifiedPreCrashKvCopies()==m.preCrashOk()*2,"PB pre-crash OK missing on survivor");
        if(m.converged()) require(m.divergentKeys()==0 && m.verifiedAllOkKvCopies()==ok*2,"false convergence");
        long last=-1;int oldOffered=0,oldOk=0;
        for(var s:m.samples()) {
            require(last<0?s.timeNs()==0:s.timeNs()==last+ProtocolCluster.ms(100),"sample timing differs");
            require(s.offered()==Math.min(offered,s.timeNs()/ProtocolCluster.ms(c.intervalMs())+1) && s.offered()>=oldOffered && s.ok()>=oldOk,"sample offered/completion differs");
            require(s.ok()>=0 && s.busy()>=0 && s.timeout()>=0 && s.unavailable()>=0 && s.stale()>=0 && s.redirect()>=0 && s.other()==0 && s.pending()>=0,"negative sampled counts");
            require(s.ok()+s.busy()+s.timeout()+s.unavailable()+s.stale()+s.redirect()+s.pending()==s.offered(),"sample outcomes do not sum");
            require(s.timeNs()<m.crashAtNs()?s.kv0()>=0:s.kv0()==-1,"dead node KV included as survivor");
            require(s.kv1()>=0 && s.kv2()>=0 && s.pendingBytes()>=s.pendingDataBytes() && s.pendingDataBytes()>=0 && s.viewAdvance()>=0,"invalid survivor state");
            require(c.variant().equals("PB")?s.logLag()==-1 && s.applyLag()==-1:s.logLag()>=0 && s.applyLag()>=0,"invalid survivor lag");
            last=s.timeNs();oldOffered=s.offered();oldOk=s.ok();
        }
        var end=m.samples().getLast();require(end.offered()==offered && end.ok()==ok && end.timeNs()==ProtocolCluster.ms(m.loadMs())+m.drainNs(),"end sample differs");
        require(m.samples().get(300).pending()==m.pendingAtLoadEnd(),"pending at load end differs");
        var n=m.network();require(n.attemptedBytes()>=0 && n.enqueuedBytes()>=0 && n.deliveredBytes()>=0 && n.droppedBytes()>=0 && n.pendingBytes()>=0 && n.dataBytes()>=0 && n.controlBytes()>=0 && n.messages()>0 && n.maxQueueWaitNs()>=0,"invalid network counts");
        require(n.attemptedBytes()==n.deliveredBytes()+n.droppedBytes()+n.pendingBytes() && n.enqueuedBytes()==n.dataBytes()+n.controlBytes() && n.enqueuedBytes()<=n.attemptedBytes() && n.enqueuedBytes()>=n.deliveredBytes()+n.pendingBytes(),"byte accounting differs");
        require(n.pendingBytes()==end.pendingBytes() && n.peakPendingBytes()>=n.pendingBytes() && n.peakPendingBytes()<=n.enqueuedBytes(),"network/sample differs");
    }
    private static Map<String,Long> fields(String names,String values) {String[] keys=names.split(","),v=values.split(",");Map<String,Long> f=new HashMap<>();for(int j=0;j<keys.length;j++) f.put(keys[j],Long.parseLong(v[j]));return f;}
    private static void csvRow(StringBuilder b,String kind,LoadCrashExperiment.Condition c,Map<String,Long> f) {
        b.append(kind).append(',').append(c.variant()).append(',').append(c.scenario()).append(',').append(c.intervalMs()).append(',').append(c.valueBytes()).append(',').append(c.mibps());
        for(String key:CSV.substring(CSV.indexOf("phase,")).split(",")) {b.append(',');if(f.containsKey(key)) b.append(f.get(key));}b.append('\n');
    }
    public static void regenerate(Path folder) throws IOException {
        Files.deleteIfExists(folder.resolve("completion.properties"));List<Path> files;
        try(var stream=Files.list(folder.resolve("results"))) {files=stream.filter(p->p.toString().endsWith(".json")).sorted().toList();}
        require(files.size()==16 || files.size()==48,"expected quick 16 or full 48 cases");
        var expected=new HashSet<>(LoadCrashExperiment.conditions(files.size()==16));Set<LoadCrashExperiment.Condition> seen=new HashSet<>();
        StringBuilder csv=new StringBuilder(CSV+"\n"),md=new StringBuilder("# 연속 부하 중 Primary crash\n\n0~30초 open-loop, 11초 Primary 0 종료. SLOW_CRASH는 10~20초 노드 2 ACK +200ms.\n\n");
        Properties launch=new Properties();try(var reader=Files.newBufferedReader(folder.resolve("launch.properties"))) {launch.load(reader);}
        for(String key:List.of("suiteExecutionId","gitCommit","sourceHash","backendHash")) md.append("- ").append(key).append(": `").append(launch.getProperty(key,"unknown")).append("`\n");
        md.append("\n|방식|조건|간격 ms|값 B|MiB/s|failover ms|장애 전 OK|생존 증거 보존|장애 후 제출|장애 후 OK|장애 후 BUSY|UNAVAILABLE|TIMEOUT|전환 전 미완료|최종 수렴|\n|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|\n");
        for(Path file:files) {
            var m=decode(Files.readString(file));validate(m);var c=m.condition();require(expected.contains(c) && seen.add(c) && file.getFileName().toString().equals(c.id()+".json"),"duplicate/misnamed condition");
            Map<String,Long> values=numeric(m);for(String key:List.of("offered","ok","busy","timeout","unavailable","stale","redirect","other","pending")) values.put(key,0L);
            for(var p:m.phases()) {var f=fields(PHASE,SustainedLoadResearchMain.phaseRow(p));csvRow(csv,"phase",c,f);for(String key:List.of("offered","ok","busy","timeout","unavailable","stale","redirect","other","pending")) values.merge(key,f.get(key),Long::sum);}
            csvRow(csv,"case",c,values);for(var s:m.samples()) csvRow(csv,"sample",c,fields(SAMPLE,SustainedLoadResearchMain.sampleRow(s)));
            var post=m.phases().get(1);
            md.append('|').append(c.variant()).append('|').append(c.scenario()).append('|').append(c.intervalMs()).append('|').append(c.valueBytes()).append('|').append(c.mibps()).append('|').append(ms(values.get("failoverNs"))).append('|').append(m.preCrashOk()).append('|').append(m.retainedPreCrashAtEnd()).append('|').append(post.offered()).append('|').append(post.ok()).append('|').append(post.busy()).append('|').append(post.unavailable()).append('|').append(post.timeout()).append('|').append(m.preCrashPending()).append('|').append(m.converged()?"YES":"NO").append("|\n");
        }
        require(seen.equals(expected),"condition coverage incomplete");
        md.append("\n## 지표 정의\n\n- failover는 crash부터 crash 이후 제출한 새 쓰기의 첫 OK callback까지다. —는 이 실행의 제출 요청에서 성공 미관측이다.\n"
                + "- 장애 전 OK의 생존 증거는 PB에서 KV, VR에서 로그다. 실제 KV 적용 복사본 수는 verified*KvCopies로 별도 집계한다.\n"
                + "- 죽은 노드 0의 메모리는 보존 근거에서 제외한다. phase 0/1은 제출 시각이 crash 전/후인 요청이다.\n"
                + "- p50/p99는 성공한 요청만의 nearest-rank다. 전환 중 거절·TIMEOUT·재탐색을 결과 수에서 빼지 않는다.\n"
                + "- 새 입력은 30초에 끝나며 최대 추가 30초 관측한다. 그 이후 새 probe는 제출하지 않는다.\n"
                + "- pendingLiveDataBytes는 생존 링크만 센다. global pendingBytes에는 죽은 노드 관련 폐기 예정 frame도 포함될 수 있다.\n"
                + "- 전체 수렴과 첫 쓰기 재개는 다른 지표다. 미완료 연산은 로그에 남거나 적용될 수 있으며 취소가 아니다.\n"
                + "- 동시 쓰기 폭·느린 노드 정책·checkpoint는 기존 멘토 질문으로 유지한다. 실제 서버 성능이나 영구 복구 불능을 주장하지 않는다.\n");
        Files.writeString(folder.resolve("summary.csv"),csv);Files.writeString(folder.resolve("results.md"),md);Files.writeString(folder.resolve("completion.properties"),"status=complete\nresultSchemaVersion="+SCHEMA+"\ncases="+files.size()+"\n");
    }
    private static String ms(long ns) {return ns<0?"—":String.format(Locale.ROOT,"%.3f",ns/1_000_000.0);}
}
