package io.openkache.server.benchmark.pilot;

import io.openkache.core.api.*;
import io.openkache.core.model.*;
import io.openkache.core.replication.ionia.*;
import io.openkache.core.spi.TimeSource;
import io.openkache.runner.benchmark.*;
import io.openkache.runner.benchmark.client.*;
import io.openkache.runner.benchmark.execution.DrainProbe;
import io.openkache.runner.benchmark.measurement.*;
import io.openkache.server.benchmark.ionia.*;
import io.openkache.server.ionia.IoniaTcpTransport;
import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.security.*;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/** Two-load functional pilot, with one separate controller. Not a qualified performance driver.
 * Explicit client close from the control reader is intentional: peer failure cancels in-flight work.
 */
@SuppressWarnings("try")
public final class DistributedPilotMain {
    private DistributedPilotMain(){}
    private static final String PROTOCOL="ionia-pilot-control-v1";
    public static void main(String[] args){try{execute(args);}catch(Exception e){System.err.println("DISTRIBUTED_PILOT_FAILED: "+e);e.printStackTrace(System.err);System.exit(2);}}
    public static void execute(String[] a)throws Exception{
        if(Runtime.version().feature()!=21)throw new IllegalArgumentException("Java21 required");if(a.length==0)throw new IllegalArgumentException("prepare|verify|controller|load");
        switch(a[0]){
            case "prepare"->{count(a,5);prepare(Path.of(a[1]),Path.of(a[2]),Path.of(a[3]),a[4]);}
            case "verify"->{count(a,3);DistributedPlan.verify(Path.of(a[1]),Path.of(a[2]));System.out.println("DISTRIBUTED_INPUTS_VERIFIED");}
            case "controller"->{count(a,3);controller(Path.of(a[1]),Path.of(a[2]));}
            case "load"->{count(a,4);load(Path.of(a[1]),Path.of(a[2]),Integer.parseInt(a[3]));}
            default->throw new IllegalArgumentException("unknown distributed command");
        }
    }
    private static void count(String[] a,int n){if(a.length!=n)throw new IllegalArgumentException("wrong distributed arguments");}
    public static void prepare(Path topology,Path dataset,Path output,String endpoint)throws Exception{
        var p=PilotFiles.Plan.read(dataset.resolve("profile.properties"));DistributedPlan.requireProfile(p);var address=DistributedPlan.endpoint(endpoint);
        var members=PilotFiles.addresses(PilotFiles.readProperties(topology));if(members.values().contains(address))throw new IllegalArgumentException("controller endpoint conflicts with replica");
        PilotMain.prepare(topology,dataset,output);Path root=output.toRealPath();var run=PilotFiles.runProperties(root);
        Files.move(root.resolve("manifest.json"),root.resolve("single-load-reference-manifest.json"));
        Properties c=new Properties();c.setProperty("schema",DistributedPlan.SCHEMA);c.setProperty("run.id",run.getProperty("run.id"));c.setProperty("load.count","2");c.setProperty("controller.endpoint",endpoint);byte[] token=new byte[32];new SecureRandom().nextBytes(token);c.setProperty("control.token",HexFormat.of().formatHex(token));PilotFiles.properties(root.resolve("distributed.properties"),c);
        PilotFiles.json(root.resolve("manifest.json"),Map.of("schema_version",DistributedPlan.SCHEMA,"purpose","functional_pilot_not_performance_comparison","run_id",run.getProperty("run.id"),"replica_processes",members.size(),"load_processes",2,"controller_processes",1,"total_concurrency",2,"mapping",List.of(Map.of("slot",0,"stream",0,"wire_base",DistributedPlan.wireBase(0)),Map.of("slot",1,"stream",1,"wire_base",DistributedPlan.wireBase(1))),"input_sha256",PilotFiles.sha(root.resolve("INPUT_SHA256SUMS"))));
        PilotFiles.hashes(root,"DISTRIBUTED_SHA256SUMS",List.of("distributed.properties","manifest.json","INPUT_SHA256SUMS"));Files.writeString(root.resolve("DISTRIBUTED_PREPARED"),PilotFiles.sha(root.resolve("DISTRIBUTED_SHA256SUMS"))+"\n",StandardOpenOption.CREATE_NEW);System.out.println("DISTRIBUTED_RUN="+root);
    }
    private static void claim(Path root,String role)throws IOException{
        if(Files.exists(root.resolve("CLIENT_STARTED"))||Files.exists(root.resolve("state/distributed.abort")))throw new IOException("run already used/aborted");
        Files.writeString(root.resolve("state/"+role+".started"),""+ProcessHandle.current().pid(),StandardOpenOption.CREATE_NEW);
    }
    private static long deadline(PilotFiles.Plan p){return System.nanoTime()+TimeUnit.MILLISECONDS.toNanos(p.workMs());}
    private static long remaining(long until)throws TimeoutException{long n=until-System.nanoTime();if(n<=0)throw new TimeoutException("distributed work deadline");return n;}
    private static int millis(long nanos){return (int)Math.max(1,Math.min(Integer.MAX_VALUE,TimeUnit.NANOSECONDS.toMillis(nanos)+1));}
    private static O00BenchmarkClient.Diagnostics empty(){return new O00BenchmarkClient.Diagnostics(new ClientAccounting().snapshot(),new ObservedRpc.Snapshot(Map.of(),0,0,false),0,0,0,0,false,null);}
    private static Map<NodeId,InetSocketAddress> addresses(Path root)throws IOException{return PilotFiles.addresses(PilotFiles.readProperties(root.resolve("topology.properties")));}
    private static IoniaTcpTransport admin(Map<NodeId,InetSocketAddress> nodes,int socket){var c=new IoniaConfig(new NodeId(1),new NodeId(1),new ArrayList<>(nodes.keySet()),0);return new IoniaTcpTransport(new NodeId(0),c.configurationId(),nodes,socket);}
    private static DrainProbe.Observation applied(RemoteO00DrainProbe probe,long expected,long until,long barrier)throws Exception{
        long end=System.nanoTime()+Math.min(remaining(until),TimeUnit.MILLISECONDS.toNanos(barrier));DrainProbe.Observation last;
        do{last=probe.observe(expected);if(last.status()==DrainProbe.Status.READY)return last;if(last.status()==DrainProbe.Status.FAILED)throw new IOException(last.toString());Thread.sleep(10);}while(System.nanoTime()-end<0);throw new TimeoutException("global applied frontier: "+last);
    }
    private static Map<String,Object> diagnostic(O00BenchmarkClient.Diagnostics d){return Map.of("pending",d.logical().pending(),"raw",d.rpc().outstanding(),"active",d.active(),"occupied",d.occupiedSlots(),"closed",d.closed(),"close_error",d.closeError()==null?"":d.closeError());}
    private static void idle(O00BenchmarkClient client,boolean closed,long until)throws Exception{
        do{var d=client.diagnostics();if(d.closeError()!=null)throw new IOException("client close error");if(d.logical().pending()==0&&d.rpc().outstanding()==0&&d.active()==0&&d.occupiedSlots()==0&&d.closed()==closed)return;Thread.sleep(5);}while(remaining(until)>0);
    }
    private static Thread watch(Path root,Runnable abort){return Thread.ofVirtual().name("distributed-stop-file").start(()->{try{while(!Thread.currentThread().isInterrupted()){if(Files.exists(root.resolve("state/distributed.abort"))){abort.run();return;}Thread.sleep(50);}}catch(InterruptedException e){Thread.currentThread().interrupt();}});}
    private static void closeSocket(Socket socket){try{socket.close();}catch(IOException ignored){}}
    private static void failure(Path root,String name,Exception e,List<?> partial){try{PilotFiles.json(root.resolve(name),Map.of("passed",false,"error",e.toString(),"partial",partial));}catch(Exception write){System.err.println("Failure evidence could not be written: "+write);}}
    private static Properties hello(Path bundle,Path root,DistributedPlan.Input input,int slot)throws IOException{
        var p=DistributedWire.packet("HELLO");DistributedWire.put(p,"protocol",PROTOCOL);DistributedWire.put(p,"run.id",input.run().getProperty("run.id"));DistributedWire.put(p,"contract.sha256",input.contractHash());DistributedWire.put(p,"input.sha256",PilotFiles.sha(root.resolve("INPUT_SHA256SUMS")));DistributedWire.put(p,"bundle.sha256",PilotFiles.sha(bundle.resolve("BUNDLE_SHA256SUMS")));DistributedWire.put(p,"control.token",input.control().getProperty("control.token"));DistributedWire.put(p,"slot",slot);DistributedWire.put(p,"stream",slot);DistributedWire.put(p,"wire.base",DistributedPlan.wireBase(slot));DistributedWire.put(p,"pid",ProcessHandle.current().pid());DistributedWire.put(p,"instance.id",UUID.randomUUID());DistributedWire.put(p,"java.feature",21);return p;
    }
    static int validateHello(Properties actual,Properties expected,Set<Integer> seen)throws IOException{
        DistributedWire.kind(actual,"HELLO");for(String k:List.of("protocol","run.id","contract.sha256","input.sha256","bundle.sha256","control.token"))if(!Objects.equals(actual.getProperty(k),expected.getProperty(k)))throw new IOException("load handshake mismatch: "+k);
        int slot=Math.toIntExact(DistributedWire.number(actual,"slot"));try{DistributedPlan.slot(slot);UUID.fromString(DistributedWire.required(actual,"instance.id"));}catch(IllegalArgumentException e){throw new IOException("load identity",e);}
        if(DistributedWire.number(actual,"stream")!=slot||DistributedWire.number(actual,"wire.base")!=DistributedPlan.wireBase(slot)||DistributedWire.number(actual,"pid")<1||DistributedWire.number(actual,"java.feature")!=21||!seen.add(slot))throw new IOException("duplicate/invalid load slot");return slot;
    }
    private static Properties receive(BlockingQueue<Properties> queue,AtomicReference<Exception> error,long until)throws Exception{
        while(true){if(error.get()!=null)throw new IOException("distributed peer aborted",error.get());var p=queue.poll(Math.min(100,millis(remaining(until))),TimeUnit.MILLISECONDS);if(p!=null)return p;}
    }
    private static Map<String,Object> loadIdentity(Properties p)throws IOException{return Map.of("slot",DistributedWire.number(p,"slot"),"stream",DistributedWire.number(p,"stream"),"wire_base",DistributedWire.number(p,"wire.base"),"pid",DistributedWire.number(p,"pid"),"instance_id",DistributedWire.required(p,"instance.id"));}

    public static void controller(Path bundle,Path root)throws Exception{
        var input=DistributedPlan.verify(bundle,root);var p=input.plan();var nodes=addresses(root);var replicas=PilotMain.ready(bundle,root,input.run(),nodes.keySet());claim(root,"distributed-controller");long until=deadline(p);
        List<Socket> sockets=new CopyOnWriteArrayList<>();List<Thread> readers=new ArrayList<>();List<Map<String,Object>> phases=new ArrayList<>();var error=new AtomicReference<Exception>();Thread watcher=null;
        try(var server=new ServerSocket();var control=admin(nodes,p.socketMs());var probe=new RemoteO00DrainProbe(control,DistributedPilotMain::empty,new ArrayList<>(nodes.keySet()),new NodeId(1),input.run().getProperty("run.id"))){
            server.setReuseAddress(false);server.bind(new InetSocketAddress(input.endpoint().getPort()));
            Runnable abort=()->{error.compareAndSet(null,new IOException("controller aborted"));sockets.forEach(DistributedPilotMain::closeSocket);try{server.close();}catch(IOException ignored){}};
            watcher=watch(root,abort);Properties ready=new Properties();ready.setProperty("run.id",input.run().getProperty("run.id"));ready.setProperty("pid",""+ProcessHandle.current().pid());ready.setProperty("contract.sha256",input.contractHash());ready.setProperty("endpoint",input.control().getProperty("controller.endpoint"));Path part=root.resolve("state/controller.ready.properties.part");PilotFiles.properties(part,ready);Files.move(part,root.resolve("state/controller.ready.properties"),StandardCopyOption.ATOMIC_MOVE);
            Map<Integer,Socket> peers=new TreeMap<>();Map<Integer,Properties> identities=new TreeMap<>();Map<Integer,BlockingQueue<Properties>> queues=new TreeMap<>();Set<Integer> seen=new HashSet<>();Set<String> instances=new HashSet<>();Properties expected=hello(bundle,root,input,0);
            long registerUntil=System.nanoTime()+Math.min(remaining(until),TimeUnit.MILLISECONDS.toNanos(p.barrierMs()));
            while(peers.size()<2){server.setSoTimeout(millis(remaining(registerUntil)));Socket socket=server.accept();sockets.add(socket);socket.setSoTimeout(millis(remaining(registerUntil)));Properties h=DistributedWire.read(socket.getInputStream());int slot=validateHello(h,expected,seen);if(!instances.add(h.getProperty("instance.id")))throw new IOException("duplicate load instance");peers.put(slot,socket);identities.put(slot,h);queues.put(slot,new ArrayBlockingQueue<>(4));}
            for(int slot=0;slot<2;slot++){
                Socket socket=peers.get(slot);socket.setSoTimeout(millis(remaining(until)));var queue=queues.get(slot);
                readers.add(Thread.ofVirtual().name("controller-load-"+slot).start(()->{try{while(true){var frame=DistributedWire.read(socket.getInputStream());if("ABORT".equals(frame.getProperty("kind")))throw new IOException("load reported abort: "+frame.getProperty("error","unknown"));if(!queue.offer(frame))throw new IOException("control queue overflow");if("ACK".equals(frame.getProperty("kind")))return;}}catch(Exception e){error.compareAndSet(null,e);abort.run();}}));
            }
            long writes=0;var preflight=applied(probe,0,until,p.barrierMs());
            for(TracePhase phase:TracePhase.values()){
                long started=System.nanoTime();for(Socket peer:peers.values()){var go=DistributedWire.packet("RUN");DistributedWire.put(go,"phase",phase);DistributedWire.write(peer.getOutputStream(),go);}Files.writeString(root.resolve("state/phase-"+phase.name().toLowerCase(Locale.ROOT)+".started"),"controller_dispatched\n",StandardOpenOption.CREATE_NEW);
                var a=DistributedReports.read(receive(queues.get(0),error,until));var b=DistributedReports.read(receive(queues.get(1),error,until));
                if(a.slot()!=0||b.slot()!=1||a.phase()!=phase||b.phase()!=phase)throw new IOException("out-of-order phase/slot");
                var ea=DistributedPlan.expected(p,phase,0);var eb=DistributedPlan.expected(p,phase,1);DistributedReports.validate(a,ea);DistributedReports.validate(b,eb);long ended=System.nanoTime();writes+=ea.writes()+eb.writes();var drain=applied(probe,writes,until,p.barrierMs());
                Map<String,Object> row=new LinkedHashMap<>();row.put("phase",phase);row.put("requests",ea.requests()+eb.requests());row.put("expected_writes",writes);row.put("loads",List.of(DistributedReports.json(a),DistributedReports.json(b)));row.put("merged_histograms",DistributedReports.merge(a,b).stream().map(DistributedReports::histogramJson).toList());row.put("controller_window_ns",ended-started);row.put("controller_completion_window_ops_per_second",(ea.requests()+eb.requests())*1e9/(ended-started));row.put("window_semantics","dispatch_through_both_validated_reports_including_control_overhead_not_qualified_throughput");row.put("drain",drain);phases.add(row);PilotFiles.json(root.resolve("distributed-phase-"+phase.name().toLowerCase(Locale.ROOT)+".json"),row);
            }
            for(Socket peer:peers.values())DistributedWire.write(peer.getOutputStream(),DistributedWire.packet("CLOSE"));
            List<Map<String,Object>> loads=new ArrayList<>();Map<Integer,String> loadHashes=new HashMap<>();
            for(int slot=0;slot<2;slot++){
                var closed=receive(queues.get(slot),error,until);DistributedWire.kind(closed,"CLOSED");if(DistributedWire.number(closed,"slot")!=slot||!"true".equals(closed.getProperty("closed"))||!closed.getProperty("close.error","").isEmpty())throw new IOException("load close not confirmed");for(String k:List.of("pending","raw","active","occupied"))if(DistributedWire.number(closed,k)!=0)throw new IOException("load close not idle");
                byte[] bytes=Base64.getDecoder().decode(DistributedWire.required(closed,"result.base64"));String hash=PilotFiles.sha(bytes);if(!hash.equals(DistributedWire.required(closed,"result.sha256")))throw new IOException("load result transfer hash");Files.write(root.resolve("collected-load-"+slot+".json"),bytes,StandardOpenOption.CREATE_NEW);loadHashes.put(slot,hash);Map<String,Object> identity=new LinkedHashMap<>(loadIdentity(identities.get(slot)));identity.put("result_sha256",hash);identity.put("closed_idle",true);loads.add(identity);
            }
            var cleanup=applied(probe,writes,until,p.barrierMs());var audit=PilotMain.audit(nodes,p,input.run().getProperty("run.id"),writes);DistributedPlan.verify(bundle,root);remaining(until);
            for(Socket peer:peers.values())DistributedWire.write(peer.getOutputStream(),DistributedWire.packet("SUCCESS"));
            for(int slot=0;slot<2;slot++){var ack=receive(queues.get(slot),error,until);DistributedWire.kind(ack,"ACK");if(DistributedWire.number(ack,"slot")!=slot||!loadHashes.get(slot).equals(ack.getProperty("result.sha256")))throw new IOException("load completion ACK mismatch");}
            List<Map<String,Object>> replicaIds=new ArrayList<>();for(Properties r:replicas)replicaIds.add(Map.of("node_id",PilotFiles.number(r,"node.id"),"pid",PilotFiles.number(r,"pid"),"instance_id",r.getProperty("instance.id")));
            Map<String,Object> result=new LinkedHashMap<>();result.put("schema_version",DistributedPlan.SCHEMA);result.put("passed",true);result.put("purpose","functional_pilot_not_performance_comparison");result.put("run_id",input.run().getProperty("run.id"));result.put("controller_pid",ProcessHandle.current().pid());result.put("loads",loads);result.put("replicas",replicaIds);result.put("contract_sha256",input.contractHash());result.put("input_sha256",PilotFiles.sha(root.resolve("INPUT_SHA256SUMS")));result.put("bundle_sha256",PilotFiles.sha(bundle.resolve("BUNDLE_SHA256SUMS")));result.put("preflight",preflight);result.put("phases",phases);result.put("cleanup",cleanup);result.put("audit",audit);result.put("workload_writes",writes);result.put("completed_at",Instant.now().toString());result.put("limitations",List.of("trusted_private_control_network_no_TLS","per_host_monotonic_latency_only","controller_window_includes_control_overhead","no_remote_resource_or_storage_force_qualification","no_full_history_linearizability_or_power_loss_test"));
            PilotFiles.json(root.resolve("distributed-summary.json"),result);List<String> files=new ArrayList<>(List.of("distributed-summary.json","collected-load-0.json","collected-load-1.json"));for(var phase:TracePhase.values())files.add("distributed-phase-"+phase.name().toLowerCase(Locale.ROOT)+".json");PilotFiles.hashes(root,"DISTRIBUTED_RESULT_SHA256SUMS",files);Files.writeString(root.resolve("DISTRIBUTED_COMPLETE"),PilotFiles.sha(root.resolve("DISTRIBUTED_RESULT_SHA256SUMS"))+"\n",StandardOpenOption.CREATE_NEW);System.out.println("DISTRIBUTED_PASS="+root.toAbsolutePath());
        }catch(Exception e){failure(root,"DISTRIBUTED_FAILED.json",e,phases);throw e;}finally{if(watcher!=null)watcher.interrupt();sockets.forEach(DistributedPilotMain::closeSocket);readers.forEach(Thread::interrupt);}
    }

    public static void load(Path bundle,Path root,int slot)throws Exception{
        DistributedPlan.slot(slot);var input=DistributedPlan.verify(bundle,root);var p=input.plan();var nodes=addresses(root);claim(root,"distributed-load-"+slot);long until=deadline(p);List<Map<String,Object>> phases=new ArrayList<>();var error=new AtomicReference<Exception>();var queue=new ArrayBlockingQueue<Properties>(4);Thread reader=null,watcher=null;
        var settings=new O00BenchmarkClient.Settings(input.run().getProperty("run.id"),DistributedPlan.slotName(slot),2,DistributedPlan.wireBase(slot),new ArrayList<>(nodes.keySet()),new NodeId(1),new NodeId(2),TimeUnit.MILLISECONDS.toNanos(p.operationMs()),3,10_000_000,8,256,5_000_000);
        var membership=new IoniaConfig(new NodeId(1),new NodeId(1),new ArrayList<>(nodes.keySet()),0);
        try(var socket=new Socket();var rpc=new PilotRpcDiagnostics(new IoniaTcpTransport(new NodeId(0),membership.configurationId(),nodes,p.socketMs()));var client=new O00BenchmarkClient(settings,rpc,TimeSource.system())){
            socket.connect(input.endpoint(),p.socketMs());socket.setSoTimeout(millis(remaining(until)));Properties identity=hello(bundle,root,input,slot);DistributedWire.write(socket.getOutputStream(),identity);
            Runnable abort=()->{error.compareAndSet(null,new IOException("load aborted"));closeSocket(socket);client.close();};watcher=watch(root,abort);
            reader=Thread.ofVirtual().name("load-controller-reader").start(()->{try{while(true){var frame=DistributedWire.read(socket.getInputStream());if("ABORT".equals(frame.getProperty("kind")))throw new IOException("controller abort");if(!queue.offer(frame))throw new IOException("load command queue overflow");if("SUCCESS".equals(frame.getProperty("kind")))return;}}catch(Exception e){error.compareAndSet(null,e);abort.run();}});
            var generator=new TraceGenerator(p.trace());
            for(TracePhase phase:TracePhase.values()){
                var go=receive(queue,error,until);DistributedWire.kind(go,"RUN");if(!phase.name().equals(go.getProperty("phase")))throw new IOException("phase command out of order");
                long started=System.nanoTime(),total=DistributedPlan.count(p,phase),get=0,set=0,delete=0,attempts=0,retried=0;var digest=DistributedPlan.digest(phase,slot,total);var latency=new LatencyCollector(input.run().getProperty("run.id"),DistributedPlan.slotName(slot),"load-"+slot+"-pid-"+ProcessHandle.current().pid(),DistributedReports.RANGE);
                for(long index=slot;index<total;index+=2){
                    remaining(until);if(error.get()!=null)throw new IOException("controller/peer lost",error.get());var request=generator.at(phase,index);long due=System.nanoTime();var outcome=client.submit(request).toCompletableFuture().get(Math.min(p.operationMs()+1000,millis(remaining(until))),TimeUnit.MILLISECONDS);latency.outcome(request,due,outcome);
                    if(!outcome.id().equals(request.id(settings.runId(),settings.loadSlot()))||outcome.operation()!=ClientOutcome.Operation.of(request.command())||outcome.category()!=ClientOutcome.Category.SUCCESS||outcome.wireClientId()!=DistributedPlan.wireBase(slot)+request.clientId()||outcome.wireOperationId()!=request.sequence()+1){
                        Map<String,Object> details=new LinkedHashMap<>();details.put("phase",phase);details.put("global_index",index);details.put("stream",slot);details.put("completed_in_phase",get+set+delete);details.put("category",outcome.category());details.put("reason",outcome.reason());details.put("status",outcome.result()==null?"NONE":outcome.result().status());details.put("attempts",outcome.attempts());details.put("diagnostics",diagnostic(client.diagnostics()));details.put("transport",rpc.snapshot());PilotFiles.json(root.resolve("load-"+slot+"-failed-operation.json"),details);
                        throw new IOException("workload outcome failure phase="+phase+" index="+index+" status="+(outcome.result()==null?"NONE":outcome.result().status())+" "+outcome.category()+"/"+outcome.reason());
                    }
                    DistributedPlan.record(digest,request);switch(request.command()){case Command.Get ignored->get++;case Command.Set ignored->set++;case Command.Delete ignored->delete++;}attempts+=outcome.attempts().size();if(outcome.attempts().size()>1)retried++;
                }
                idle(client,false,until);long ended=System.nanoTime();var part=new DistributedPlan.Partition(get+set+delete,get,set,delete,HexFormat.of().formatHex(digest.digest()));var report=DistributedReports.phase(slot,phase,part,attempts,retried,ended-started,latency.freeze());DistributedReports.validate(report,DistributedPlan.expected(p,phase,slot));var row=DistributedReports.json(report);phases.add(row);PilotFiles.json(root.resolve("load-"+slot+"-phase-"+phase.name().toLowerCase(Locale.ROOT)+".json"),row);DistributedWire.write(socket.getOutputStream(),DistributedReports.packet(report));
            }
            DistributedWire.kind(receive(queue,error,until),"CLOSE");client.close();idle(client,true,until);DistributedPlan.verify(bundle,root);
            Map<String,Object> result=new LinkedHashMap<>();result.put("schema_version",DistributedPlan.SCHEMA);result.put("run_id",input.run().getProperty("run.id"));result.put("slot",slot);result.put("clock_id","load-"+slot+"-pid-"+ProcessHandle.current().pid());result.put("identity",loadIdentity(identity));result.put("contract_sha256",input.contractHash());result.put("closed_idle",diagnostic(client.diagnostics()));result.put("phases",phases);Path output=root.resolve("load-"+slot+"-result.json");PilotFiles.json(output,result);String hash=PilotFiles.sha(output);
            var closed=DistributedWire.packet("CLOSED");DistributedWire.put(closed,"slot",slot);DistributedWire.put(closed,"closed",true);DistributedWire.put(closed,"close.error","");for(String k:List.of("pending","raw","active","occupied"))DistributedWire.put(closed,k,0);DistributedWire.put(closed,"result.sha256",hash);DistributedWire.put(closed,"result.base64",Base64.getEncoder().encodeToString(Files.readAllBytes(output)));DistributedWire.write(socket.getOutputStream(),closed);
            DistributedWire.kind(receive(queue,error,until),"SUCCESS");Files.writeString(root.resolve("LOAD_"+slot+"_COMPLETE"),hash+"\n",StandardOpenOption.CREATE_NEW);var ack=DistributedWire.packet("ACK");DistributedWire.put(ack,"slot",slot);DistributedWire.put(ack,"result.sha256",hash);DistributedWire.write(socket.getOutputStream(),ack);System.out.println("DISTRIBUTED_LOAD_PASS slot="+slot);
        }catch(Exception e){failure(root,"LOAD_"+slot+"_FAILED.json",e,phases);throw e;}finally{if(watcher!=null)watcher.interrupt();if(reader!=null)reader.interrupt();}
    }
}
