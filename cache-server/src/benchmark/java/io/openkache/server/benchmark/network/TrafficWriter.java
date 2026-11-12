package io.openkache.server.benchmark.network;

import io.openkache.runner.benchmark.*;
import io.openkache.runner.benchmark.client.*;
import io.openkache.runner.benchmark.execution.*;
import io.openkache.runner.benchmark.measurement.*;
import io.openkache.server.observation.TcpTraffic;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.io.IOException;
import java.security.*;
import java.util.*;

/** Separate versioned sender-frame/initiator-exchange evidence. Default performance bundle unchanged. */
public final class TrafficWriter {
    private TrafficWriter(){}
    public record Export(Path directory,boolean valid,List<String> issues){public Export{issues=List.copyOf(issues);}}
    public static Export write(Path path,TraceConfig trace,ClosedLoopPlan plan,ClosedLoopResult run,TrafficRun observation)throws IOException{
        Map<String,Long> expected=new TreeMap<>();run.phases().forEach((phase,p)->p.byOperation().forEach((op,c)->expected.put(phase+":"+op,c.success()+c.errorResponse()+c.noResponse())));
        return write(path,trace,plan.runId(),plan.loadSlot(),run.passed(),run.finalClient(),expected,
            map("mode","closed","trace",trace,"plan",plan,"result",run),observation.network(),observation.logical());
    }
    public static Export write(Path path,TraceConfig trace,OpenLoopPlan plan,OpenLoopResult run,TrafficRun observation)throws IOException{
        Map<String,Long> expected=new TreeMap<>();run.phases().forEach((phase,p)->p.byOperation().forEach((op,c)->expected.put(phase+":"+op,c.success()+c.errorResponse()+c.noResponse())));
        return write(path,trace,plan.runId(),plan.loadSlot(),run.passed(),run.finalClient(),expected,
            map("mode","open","trace",trace,"plan",plan,"result",run),observation.network(),observation.logical());
    }
    private static Export write(Path path,TraceConfig trace,String run,String slot,boolean passed,ClientAccounting.Snapshot client,
            Map<String,Long> expected,Object execution,TcpTraffic.Snapshot s,LatencyCollector.Snapshot logical)throws IOException{
        if(!run.equals(logical.runId()) || !slot.equals(logical.loadSlot()))throw new IllegalArgumentException("logical run identity");
        Set<String> issues=new TreeSet<>(s.issues());if(!s.valid())issues.add("NETWORK_CAPTURE_INCOMPLETE");if(!passed)issues.add("EXECUTION_NOT_PASSED");
        if(logical.failure()!=null)issues.add("LOGICAL_CAPTURE_FAILED");long total=0;
        for(var phase:TracePhase.values())for(var op:ClientOutcome.Operation.values()){
            var tally=logical.counts().get(new LatencyCollector.Key(phase,op));long count=tally==null?0:tally.observed();total+=count;
            if(expected.getOrDefault(phase+":"+op,0L)!=count)issues.add("LOGICAL_COHORT_MISMATCH");
        }
        if(client.admitted()!=total || client.pending()!=0)issues.add("FINAL_LOGICAL_ACCOUNTING");
        if(!s.transitions().stream().map(TcpTraffic.Transition::stage).toList().equals(List.of(TcpTraffic.Stage.values())))issues.add("STAGE_COVERAGE");
        long clientFrames=sum(s,true,0L,null,TcpTraffic.Metric.OK),clientWritten=sum(s,false,0L,null,TcpTraffic.Metric.REQUESTS_WRITTEN);
        if(clientFrames==0 || clientWritten==0)issues.add("NO_CLIENT_WIRE_OBSERVATIONS");
        if(clientFrames!=clientWritten || sum(s,true,0L,null,TcpTraffic.Metric.SENT_BYTES)!=sum(s,false,0L,null,TcpTraffic.Metric.SENT_BYTES))issues.add("CLIENT_FRAME_EXCHANGE_MISMATCH");
        long replyFrames=s.rows().stream().filter(r->r.key().frame()&&r.key().to()==0).mapToLong(r->r.counts().get(TcpTraffic.Metric.OK)).sum();
        long replyBytes=s.rows().stream().filter(r->r.key().frame()&&r.key().to()==0).mapToLong(r->r.counts().get(TcpTraffic.Metric.SENT_BYTES)).sum();
        boolean exactFlow=sum(s,false,null,null,TcpTraffic.Metric.FAILED)==0 && sum(s,false,null,null,TcpTraffic.Metric.REJECTED)==0 && sum(s,true,null,null,TcpTraffic.Metric.FAILED)==0;
        if(exactFlow && (replyFrames!=sum(s,false,0L,null,TcpTraffic.Metric.REPLIES_READ) || replyBytes!=sum(s,false,0L,null,TcpTraffic.Metric.RECEIVED_BYTES)))issues.add("REPLY_FRAME_EXCHANGE_MISMATCH");
        for(var row:s.rows()){
            var c=row.counts();if(c.values().stream().anyMatch(n->n<0) || c.get(TcpTraffic.Metric.BEGUN)!=c.get(TcpTraffic.Metric.OK)+c.get(TcpTraffic.Metric.FAILED)+c.get(TcpTraffic.Metric.REJECTED))issues.add("ROW_ACCOUNTING");
            if(!row.key().frame() && !c.get(TcpTraffic.Metric.CREATED).equals(c.get(TcpTraffic.Metric.CLOSED)))issues.add("CONNECTION_NOT_CLOSED");
        }
        boolean valid=issues.isEmpty();Path root=path.toAbsolutePath().normalize();Files.createDirectory(root);write(root.resolve("STARTED"),"Network observations; COMPLETE means stored, not performance PASS.\n");
        List<Map<String,Object>> files=new ArrayList<>();add(root,"network.json",s,files);
        List<Map<String,Object>> logicalCounts=new ArrayList<>();logical.counts().forEach((k,v)->logicalCounts.add(map("phase",k.phase(),"operation",k.operation(),"tally",v)));
        logicalCounts.sort(Comparator.comparing(r->r.get("phase")+":"+r.get("operation")));
        add(root,"logical.json",map("runId",logical.runId(),"loadSlot",logical.loadSlot(),"clockId",logical.clockId(),"counts",logicalCounts,"series",logical.series(),"failure",logical.failure()),files);
        add(root,"execution.json",execution,files);
        add(root,"summary.json",map("schema_version","network-observation-v1","run_id",run,"load_slot",slot,"clock_id",logical.clockId(),"token",s.token(),
            "validity",valid?"valid":"invalid","issues",issues,"logical_outcomes",total,"process_count",1,
            "sender_frames",sum(s,true,null,null,TcpTraffic.Metric.OK),"sender_confirmed_bytes",sum(s,true,null,null,TcpTraffic.Metric.SENT_BYTES),
            "sender_exact_bytes",sum(s,true,null,null,TcpTraffic.Metric.WRITE_UNCERTAIN)==0?sum(s,true,null,null,TcpTraffic.Metric.SENT_BYTES):null,
            "tcp_ip_packets",null,"remote_process_bytes",null,"os_time_wait",null,
            "initiator_exchanges",sum(s,false,null,null,TcpTraffic.Metric.BEGUN),"connected",sum(s,false,null,null,TcpTraffic.Metric.CONNECTED),
            "window_method","terminal_window_admission_and_terminal_stages_separate","nominal_period_ns",1_000_000_000L,
            "flow_reconciliation",exactFlow?"exact_error_free_flow":"diagnostic_only_with_transport_failures",
            "connection_peak_method","observer_event_order_gauge_not_OS_peak",
            "preexisting_connections",null,"exchange_scope","calls_admitted_during_subscription_with_socket_close_retained",
            "frame_scope","configuration_matching_wire_writes_begun_during_subscription",
            "byte_layer","JDK_socket_stream_including_4byte_length_and_protocol_header","sum_method","sender_frames_only_never_add_exchange_or_receive_copies",
            "standard_M15_M16_M17","unsupported_until_declared_process_coverage_and_profile_integration",
            "limitations",List.of("process_local_configuration_subscription","cross_stage_intervals_not_prorated","no_per_logical_operation_attribution",
                "API_write_return_counts_not_TCP_delivery_or_IP_packets","throwing_write_partial_bytes_unknown","preexisting_connections_unobserved",
                "frame_window_and_exchange_cohort_may_differ_at_boundaries","no_OS_TIME_WAIT_attribution","no_formal_performance_or_overhead_qualification")),files);
        write(root.resolve("integrity.json"),ObservationJson.encode(map("schema_version","network-integrity-v1","token",s.token(),"validity",valid?"valid":"invalid","files",files))+"\n");
        write(root.resolve("COMPLETE"),"integrity_sha256="+sha(Files.readAllBytes(root.resolve("integrity.json")))+"\n");return new Export(root,valid,List.copyOf(issues));
    }
    public static long sum(TcpTraffic.Snapshot s,boolean frame,Long from,Integer kind,TcpTraffic.Metric metric){return s.rows().stream().filter(r->r.key().frame()==frame&&(from==null||r.key().from()==from)&&(kind==null||r.key().kind()==kind)).mapToLong(r->r.counts().get(metric)).sum();}
    private static Map<String,Object> map(Object...args){Map<String,Object> out=new LinkedHashMap<>();for(int i=0;i<args.length;i+=2)out.put((String)args[i],args[i+1]);return out;}
    private static void add(Path root,String file,Object value,List<Map<String,Object>> files)throws IOException{Path p=root.resolve(file);write(p,ObservationJson.encode(value)+"\n");files.add(map("path",file,"sha256",sha(Files.readAllBytes(p))));}
    private static void write(Path p,String s)throws IOException{Path part=p.resolveSibling(p.getFileName()+".part");Files.writeString(part,s,StandardCharsets.UTF_8,StandardOpenOption.CREATE_NEW);Files.move(part,p,StandardCopyOption.ATOMIC_MOVE);}
    private static String sha(byte[] data){try{return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(data));}catch(NoSuchAlgorithmException e){throw new IllegalStateException(e);}}
}
