package io.openkache.runner.benchmark.measurement;

import io.openkache.runner.benchmark.*;
import io.openkache.runner.benchmark.client.*;
import io.openkache.runner.benchmark.execution.*;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.security.*;
import java.util.*;

/** 별도 JVM sidecar bundle. 기존 성능 writer의 7파일/M18·M23 미지원 계약을 자동 변경하지 않는다. */
public final class JvmRunWriter {
    private JvmRunWriter() {}
    public record Export(Path directory, boolean valid, List<String> issues, String integritySha256) {
        public Export { issues=List.copyOf(issues); }
    }
    private record Population(TracePhase phase, ClientOutcome.Operation operation) {}
    public static Export write(Path directory, TraceConfig trace, ClosedLoopPlan plan, ClosedLoopResult run,
            JvmWindowCollector.Snapshot snapshot) throws IOException {
        Map<Population,long[]> expected=new HashMap<>();
        run.phases().forEach((phase,p) -> p.byOperation().forEach((op,c) -> expected.put(new Population(phase,op),new long[]{c.success(),c.errorResponse(),c.noResponse()})));
        return write(directory,trace,plan.runId(),plan.loadSlot(),plan.warmupRequests(),plan.measuredRequests(),run.passed(),run.startedNanos(),run.finishedNanos(),run.finalClient(),
                map("mode","closed","trace",trace,"plan",plan,"result",run),expected,snapshot);
    }
    public static Export write(Path directory, TraceConfig trace, OpenLoopPlan plan, OpenLoopResult run,
            JvmWindowCollector.Snapshot snapshot) throws IOException {
        Map<Population,long[]> expected=new HashMap<>();
        run.phases().forEach((phase,p) -> p.byOperation().forEach((op,c) -> expected.put(new Population(phase,op),new long[]{c.success(),c.errorResponse(),c.noResponse()})));
        return write(directory,trace,plan.runId(),plan.loadSlot(),plan.warmupRequests(),plan.measuredRequests(),run.passed(),run.startedNanos(),run.finishedNanos(),run.finalClient(),
                map("mode","open","trace",trace,"plan",plan,"result",run),expected,snapshot);
    }
    private static Export write(Path directory, TraceConfig trace, String runId, String slot, long warm, long measured,
            boolean passed, long started, long finished, ClientAccounting.Snapshot client, Object execution,
            Map<Population,long[]> expected, JvmWindowCollector.Snapshot s) throws IOException {
        Set<String> issues=new TreeSet<>(s.issues());
        if(!s.settings().runId().equals(runId) || !s.settings().loadSlot().equals(slot)) { throw new IllegalArgumentException("resource run identity mismatch"); }
        if(!passed) { issues.add("EXECUTION_NOT_PASSED"); }
        if(!s.samplerTerminated()) { issues.add("SAMPLER_NOT_TERMINATED"); }
        if(s.lateCallbacks()!=0) { issues.add("LATE_CALLBACK_AFTER_FINISH"); }
        if(s.anchorNanos()-started<0 || finished-s.finishedNanos()<0 || s.finishedNanos()-s.anchorNanos()<0) { issues.add("RUN_CLOCK_SCOPE"); }
        if(s.samples().isEmpty() || s.transitions().isEmpty()) { issues.add("NO_RESOURCE_OBSERVATIONS"); }
        Map<Population,long[]> observed=new HashMap<>(); Set<String> windowKeys=new HashSet<>();
        for(var w:s.requestWindows()) {
            long offset=w.startNanos()-s.anchorNanos();
            if(w.index()<0 || offset!=w.index()*JvmWindowCollector.PERIOD_NS || w.endNanos()-w.startNanos()<0
                    || w.startNanos()-s.finishedNanos()>0
                    || w.endNanos()!=s.anchorNanos()+Math.min((w.index()+1)*JvmWindowCollector.PERIOD_NS,s.finishedNanos()-s.anchorNanos())
                    || w.endNanos()-s.finishedNanos()>0 || w.success()<0 || w.errorResponse()<0 || w.noResponse()<0
                    || !windowKeys.add(w.index()+":"+w.cohortPhase()+":"+w.operation())) { issues.add("REQUEST_WINDOW_SHAPE"); }
            var totals=observed.computeIfAbsent(new Population(w.cohortPhase(),w.operation()),ignored->new long[3]);
            totals[0]+=w.success(); totals[1]+=w.errorResponse(); totals[2]+=w.noResponse();
        }
        for(var phase:TracePhase.values()) {
            long expectedPhase=phase==TracePhase.LOAD?trace.keyCount():phase==TracePhase.WARMUP?warm:measured;
            long total=0;
            for(var op:ClientOutcome.Operation.values()) {
                var key=new Population(phase,op); var actual=observed.getOrDefault(key,new long[3]); var wanted=expected.get(key);
                if(wanted==null || !Arrays.equals(actual,wanted)) { issues.add("COHORT_WINDOW_MISMATCH:"+phase+":"+op); }
                if(wanted!=null) { total+=wanted[0]+wanted[1]+wanted[2]; }
            }
            if(passed && total!=expectedPhase) { issues.add("PLAN_COHORT_MISMATCH:"+phase); }
        }
        long admitted=s.requestWindows().stream().mapToLong(JvmWindowCollector.RequestWindow::outcomes).sum();
        if(client.pending()!=0 || client.admitted()!=admitted) { issues.add("FINAL_WINDOW_ACCOUNTING"); }
        if(!s.samples().isEmpty()) {
            var first=s.samples().getFirst(); var last=s.samples().getLast();
            if(first.client().admitted()!=0 || last.client().admitted()!=client.admitted() || last.client().success()!=client.success()
                    || last.client().errorResponse()!=client.errorResponse() || last.client().noResponse()!=client.noResponse() || last.client().pending()!=0) { issues.add("FINAL_CLIENT_SNAPSHOT_MISMATCH"); }
            long seq=0; JvmWindowCollector.Sample previous=null;
            for(var sample:s.samples()) {
                if(sample.sequence()!=++seq || sample.collectionEndedNanos()-sample.collectionStartedNanos()<0
                        || sample.jvm().startedNanos()-sample.collectionStartedNanos()<0 || sample.collectionEndedNanos()-sample.jvm().endedNanos()<0
                        || sample.collectionStartedNanos()-s.anchorNanos()<0 || s.finishedNanos()-sample.collectionEndedNanos()<0
                        || sample.jvm().pid()!=first.jvm().pid()
                        || !sample.jvm().processInstanceId().equals(first.jvm().processInstanceId())) { issues.add("RESOURCE_SAMPLE_ID_OR_CLOCK"); }
                if(previous==null? !sample.baseline():sample.baseline() || sample.intervalStartNanos()!=previous.jvm().endedNanos()) { issues.add("RESOURCE_SAMPLE_INTERVAL"); }
                if(sample.intervalEndNanos()!=sample.jvm().endedNanos() || sample.intervalEndNanos()-sample.intervalStartNanos()<0) { issues.add("RESOURCE_SAMPLE_INTERVAL"); }
                if(!sample.jvm().values().keySet().equals(JvmWindowCollector.SOURCE_KEYS)) { issues.add("SOURCE_KEY_COVERAGE"); }
                for(String key:s.settings().requiredSources()) {
                    var value=sample.jvm().values().get(key);
                    if(value==null || !value.status().equals("measured")) { issues.add("REQUIRED_SOURCE_UNAVAILABLE:"+key); }
                }
                for(String key:List.of("process_cpu_ns","gc_count","gc_time_ns")) {
                    var after=sample.jvm().values().get(key); var before=previous==null?null:previous.jvm().values().get(key); var delta=sample.resourceDeltas().get(key);
                    if(after!=null && after.value()!=null && before!=null && before.value()!=null
                            && (after.value()<before.value() || delta==null || delta.value()==null || delta.value()!=after.value()-before.value())) { issues.add("RESOURCE_DELTA_MISMATCH:"+key); }
                }
                previous=sample;
            }
        }
        for(var stage:ClosedLoopResult.Stage.values()) {
            if(passed && s.transitions().stream().noneMatch(t -> t.stage()==stage)) { issues.add("MISSING_STAGE:"+stage); }
        }
        boolean valid=issues.isEmpty(); Path root=directory.toAbsolutePath().normalize(); Files.createDirectory(root);
        write(root.resolve("STARTED"),"JVM observations; COMPLETE means stored, not service/performance PASS.\n");
        List<Map<String,Object>> files=new ArrayList<>();
        add(root,"samples.jsonl",lines(s.samples()),files);
        add(root,"request-windows.jsonl",lines(s.requestWindows()),files);
        add(root,"execution.json",BenchmarkJson.encode(execution)+"\n",files);
        var report=map("schema_version","benchmark-jvm-observation-v1","run_id",runId,"load_slot",slot,"clock_id",s.settings().clockId(),
                "observation_token",s.token(),"settings",s.settings(),"anchor_ns",s.anchorNanos(),"finished_ns",s.finishedNanos(),"nominal_period_ns",JvmWindowCollector.PERIOD_NS,
                "validity",valid?"valid":"invalid","issues",List.copyOf(issues),"sampler_terminated",s.samplerTerminated(),"late_callbacks",s.lateCallbacks(),
                "sampled_peak_pending",s.sampledPeakPending(),"true_peak_pending",null,"peak_method","sampled_lower_bound",
                "source_processes",s.samples().isEmpty()?List.of():List.of(map("process_instance_id",s.samples().getFirst().jvm().processInstanceId(),"pid",s.samples().getFirst().jvm().pid(),"role",s.settings().role())),
                "transitions",s.transitions(),"sample_count",s.samples().size(),"request_window_count",s.requestWindows().size(),"observed_outcomes",admitted,
                "request_window_method","raw_terminal_ns_global_run_anchor_cohort_phase_separate","request_windows_sparse",true,"absent_request_buckets_zero_only_when_valid",true,
                "resource_window_method","actual_snapshot_intervals_stage_work_drain_cleanup_separate","resource_per_op_cost",null,
                "standard_result_M18_M23","unsupported_until_declared_replica_coverage_and_profile_integration",
                "limitations",List.of("one_process_observed_once_not_per_replica_sum","shared_fixture_includes_load_and_servers","resource_intervals_differ_from_nominal_request_windows",
                        "sampled_inflight_peak_not_true_peak","GC_time_is_cumulative_not_pause_distribution","RSS_allocation_pause_not_collected",
                        "same_process_clock_only","source_snapshot_requires_nonblocking_provider","no_physical_storage_or_formal_performance_claim","observer_overhead_not_qualified"));
        add(root,"jvm-result.json",BenchmarkJson.encode(report)+"\n",files);
        write(root.resolve("integrity.json"),BenchmarkJson.encode(map("schema_version","benchmark-jvm-integrity-v1","observation_token",s.token(),"complete",true,"validity",valid?"valid":"invalid","files",files))+"\n");
        String digest=sha(Files.readAllBytes(root.resolve("integrity.json")));
        write(root.resolve("COMPLETE"),"integrity_sha256="+digest+"\n"); return new Export(root,valid,List.copyOf(issues),digest);
    }
    private static String lines(Iterable<?> rows) { StringBuilder out=new StringBuilder(); for(Object row:rows){out.append(BenchmarkJson.encode(row)).append('\n');} return out.toString(); }
    private static Map<String,Object> map(Object... pairs) { return BenchmarkRunWriter.map(pairs); }
    private static void add(Path root,String name,String value,List<Map<String,Object>> files)throws IOException { Path p=root.resolve(name);write(p,value);files.add(map("path",name,"sha256",sha(Files.readAllBytes(p)))); }
    private static void write(Path file,String value)throws IOException { Path part=file.resolveSibling(file.getFileName()+".part");Files.writeString(part,value,StandardCharsets.UTF_8,StandardOpenOption.CREATE_NEW);Files.move(part,file,StandardCopyOption.ATOMIC_MOVE); }
    private static String sha(byte[] data) { try{return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(data));}catch(NoSuchAlgorithmException impossible){throw new IllegalStateException(impossible);} }
}
