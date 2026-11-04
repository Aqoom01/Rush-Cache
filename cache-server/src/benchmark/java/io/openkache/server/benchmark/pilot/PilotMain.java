package io.openkache.server.benchmark.pilot;

import io.openkache.core.api.*;
import io.openkache.core.model.*;
import io.openkache.core.replication.ionia.*;
import io.openkache.core.spi.TimeSource;
import io.openkache.runner.benchmark.*;
import io.openkache.runner.benchmark.client.*;
import io.openkache.runner.benchmark.execution.*;
import io.openkache.runner.benchmark.measurement.*;
import io.openkache.server.benchmark.ionia.*;
import io.openkache.server.ionia.*;
import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;

/** Standalone O00 functional pilot. Same binary for local distinct JVMs and remote nodes. */
public final class PilotMain {
    private PilotMain(){}
    private static final Set<String> DATA=Set.of("profile.properties","trace.properties","trace-metadata.json","load.trace","warmup.trace","measure.trace");
    public static void main(String[] args){try{execute(args);}catch(Exception e){System.err.println("PILOT FAILED: "+e);e.printStackTrace(System.err);System.exit(2);}}
    public static void execute(String[] a)throws Exception{
        if(Runtime.version().feature()!=21)throw new IllegalArgumentException("Java21 required");if(a.length==0)throw new IllegalArgumentException("dataset|prepare|verify|wait|run|node");
        switch(a[0]){
            case "dataset"->{count(a,3);dataset(Path.of(a[1]),Path.of(a[2]));}
            case "prepare"->{count(a,4);prepare(Path.of(a[1]),Path.of(a[2]),Path.of(a[3]));}
            case "verify"->{count(a,3);verify(Path.of(a[1]),Path.of(a[2]));System.out.println("PILOT_INPUTS_VERIFIED");}
            case "wait"->{count(a,3);waitForReady(Path.of(a[1]),Path.of(a[2]));}
            case "run"->{count(a,3);run(Path.of(a[1]),Path.of(a[2]));}
            case "node"->{count(a,4);node(Path.of(a[1]),Path.of(a[2]),Long.parseLong(a[3]));}
            default->throw new IllegalArgumentException("unknown pilot command");
        }
    }
    private static void count(String[] a,int n){if(a.length!=n)throw new IllegalArgumentException("wrong pilot arguments");}
    public static void dataset(Path profile,Path output)throws Exception{
        PilotFiles.Plan plan=PilotFiles.Plan.read(profile);Path root=output.toAbsolutePath().normalize();Files.createDirectory(root);Files.copy(profile,root.resolve("profile.properties"));
        Properties source=PilotFiles.readProperties(profile),trace=new Properties();for(String k:PilotFiles.TRACE)trace.setProperty(k,source.getProperty(k));PilotFiles.properties(root.resolve("trace.properties"),trace);
        TraceTool.main(new String[]{root.resolve("trace.properties").toString(),root.resolve("generated").toString(),"--emit-traces"});
        for(String name:List.of("load.trace","warmup.trace","measure.trace","trace-metadata.json"))Files.copy(root.resolve("generated").resolve(name),root.resolve(name));
        PilotFiles.verifyTrace(root,plan);PilotFiles.hashes(root,"DATASET_SHA256SUMS",new ArrayList<>(DATA));Files.writeString(root.resolve("DATASET_COMPLETE"),PilotFiles.sha(root.resolve("DATASET_SHA256SUMS"))+"\n",StandardOpenOption.CREATE_NEW);
        System.out.println("PILOT_DATASET="+root);
    }
    private static PilotFiles.Plan verifyDataset(Path root)throws Exception{
        if(!PilotFiles.verifyHashes(root,"DATASET_SHA256SUMS").equals(DATA)||!Files.readString(root.resolve("DATASET_COMPLETE")).trim().equals(PilotFiles.sha(root.resolve("DATASET_SHA256SUMS"))))throw new IOException("dataset incomplete/coverage");
        var plan=PilotFiles.Plan.read(root.resolve("profile.properties"));PilotFiles.verifyTrace(root,plan);return plan;
    }
    public static void prepare(Path topology,Path dataset,Path output)throws Exception{
        verifyDataset(dataset);Properties t=PilotFiles.readProperties(topology);if(!t.stringPropertyNames().equals(Set.of("schema","leader.id","replicas"))||!"ionia-pilot-topology-v1".equals(t.getProperty("schema"))||PilotFiles.number(t,"leader.id")!=1)throw new IllegalArgumentException("strict pilot topology/leader1 required");var nodes=PilotFiles.addresses(t);
        Path root=output.toAbsolutePath().normalize();Files.createDirectory(root);for(String f:DATA)Files.copy(dataset.resolve(f),root.resolve(f));Files.copy(topology,root.resolve("topology.properties"));
        for(String dir:List.of("nodes","data","state","ready"))Files.createDirectory(root.resolve(dir));
        Properties r=new Properties();r.setProperty("schema","ionia-o00-pilot-v1");r.setProperty("purpose","functional-pilot");r.setProperty("run.id","pilot-"+UUID.randomUUID());r.setProperty("load.slot","pilot-1");PilotFiles.properties(root.resolve("run.properties"),r);
        List<String> files=new ArrayList<>(DATA);files.addAll(List.of("topology.properties","run.properties"));
        for(NodeId id:nodes.keySet()){
            Properties p=new Properties();p.setProperty("node.id",""+id.value());p.setProperty("leader.id","1");p.setProperty("replicas",t.getProperty("replicas"));p.setProperty("data.directory","data/node-"+id.value());p.setProperty("storage.workers","2");p.setProperty("failure.timeout.ms","60000");
            String name="nodes/node-"+id.value()+".properties";PilotFiles.properties(root.resolve(name),p);files.add(name);
        }
        PilotFiles.hashes(root,"INPUT_SHA256SUMS",files);PilotFiles.json(root.resolve("manifest.json"),Map.of("schema_version","ionia-o00-pilot-v1","run_id",r.getProperty("run.id"),"purpose","functional_pilot_not_performance_comparison","replica_processes",nodes.size(),"load_processes",1,"dataset_sha256",PilotFiles.sha(dataset.resolve("DATASET_SHA256SUMS")),"input_sha256",PilotFiles.sha(root.resolve("INPUT_SHA256SUMS")),"persistence","replicated_RAM_log_stable_View_optional_KV_force_off"));
        System.out.println("PILOT_RUN="+root);
    }
    private static Properties verify(Path bundle,Path root)throws Exception{
        PilotFiles.verifyBundle(bundle);Properties run=PilotFiles.runProperties(root);PilotFiles.verifyTrace(root,PilotFiles.Plan.read(root.resolve("profile.properties")));return run;
    }
    public static void node(Path bundle,Path root,long id)throws Exception{
        Properties run=verify(bundle,root);var topology=PilotFiles.readProperties(root.resolve("topology.properties"));var addresses=PilotFiles.addresses(topology);NodeId self=new NodeId(id);if(!addresses.containsKey(self))throw new IllegalArgumentException("node not a member");
        Properties p=PilotFiles.readProperties(root.resolve("nodes/node-"+id+".properties"));
        if(!p.stringPropertyNames().equals(Set.of("node.id","leader.id","replicas","data.directory","storage.workers","failure.timeout.ms"))||PilotFiles.number(p,"node.id")!=id||PilotFiles.number(p,"leader.id")!=1||!p.getProperty("replicas").equals(topology.getProperty("replicas"))||!p.getProperty("data.directory").equals("data/node-"+id)||PilotFiles.integer(p,"storage.workers")!=2||PilotFiles.number(p,"failure.timeout.ms")!=60000)throw new IOException("node/profile mismatch");
        Path real=root.toRealPath(),parent=real.resolve("data").toRealPath();if(!parent.startsWith(real)||!real.resolve("state").toRealPath().startsWith(real))throw new IOException("data/state escapes run root");
        Path data=parent.resolve("node-"+id);if(Files.exists(data))throw new IOException("fresh data directory required; never delete/reuse old data");
        Files.writeString(real.resolve("state/node-"+id+".started"),""+ProcessHandle.current().pid(),StandardOpenOption.CREATE_NEW);Files.createDirectory(data);
        IoniaConfig config=new IoniaConfig(self,new NodeId(1),new ArrayList<>(addresses.keySet()),0);
        try(IoniaNode n=new IoniaNode(config,addresses,data,2,60000)){
            Thread shutdown=new Thread(n::close,"pilot-node-shutdown");Runtime.getRuntime().addShutdownHook(shutdown);
            try{
                Properties ready=new Properties();ready.setProperty("run.id",run.getProperty("run.id"));ready.setProperty("node.id",""+id);ready.setProperty("pid",""+ProcessHandle.current().pid());ready.setProperty("instance.id",UUID.randomUUID().toString());ready.setProperty("status","LISTENING");ready.setProperty("bundle.sha256",PilotFiles.sha(bundle.resolve("BUNDLE_SHA256SUMS")));ready.setProperty("input.sha256",PilotFiles.sha(real.resolve("INPUT_SHA256SUMS")));ready.setProperty("java.version",System.getProperty("java.version"));
                Path readyFile=real.resolve("state/node-"+id+".ready.properties"),readyPart=readyFile.resolveSibling(readyFile.getFileName()+".part");
                PilotFiles.properties(readyPart,ready);Files.move(readyPart,readyFile,StandardCopyOption.ATOMIC_MOVE);System.out.println("PILOT_NODE_LISTENING node="+id+" pid="+ProcessHandle.current().pid());
                while(!Files.exists(real.resolve("state/node-"+id+".stop"))){if(n.engine().failureReason().isPresent())throw new IOException("engine failure: "+n.engine().failureReason().orElseThrow());Thread.sleep(100);}
            }finally{Runtime.getRuntime().removeShutdownHook(shutdown);}
        }
        Files.writeString(real.resolve("state/node-"+id+".stopped"),"closed_after_stop_file\n",StandardOpenOption.CREATE_NEW);
    }
    static List<Properties> ready(Path bundle,Path root,Properties run,Set<NodeId> members)throws Exception{
        String code=PilotFiles.sha(bundle.resolve("BUNDLE_SHA256SUMS")),inputs=PilotFiles.sha(root.resolve("INPUT_SHA256SUMS"));List<Properties> result=new ArrayList<>();Set<String> ids=new HashSet<>();
        for(NodeId n:members){Properties p=PilotFiles.readProperties(PilotFiles.inside(root,"ready/node-"+n.value()+".ready.properties"));
            if(!run.getProperty("run.id").equals(p.getProperty("run.id"))||PilotFiles.number(p,"node.id")!=n.value()||!"LISTENING".equals(p.getProperty("status"))||!code.equals(p.getProperty("bundle.sha256"))||!inputs.equals(p.getProperty("input.sha256"))||!ids.add(p.getProperty("instance.id"))||PilotFiles.number(p,"pid")<1)throw new IOException("replica ready metadata mismatch: "+n);result.add(p);
        }return result;
    }
    private static O00BenchmarkClient.Diagnostics empty(){return new O00BenchmarkClient.Diagnostics(new ClientAccounting().snapshot(),new ObservedRpc.Snapshot(Map.of(),0,0,false),0,0,0,0,false,null);}
    private static IoniaTcpTransport admin(Map<NodeId,InetSocketAddress> addresses,int timeout){IoniaConfig c=new IoniaConfig(new NodeId(1),new NodeId(1),new ArrayList<>(addresses.keySet()),0);return new IoniaTcpTransport(new NodeId(0),c.configurationId(),addresses,timeout);}
    private static DrainProbe.Observation await(RemoteO00DrainProbe probe,long expected,long millis)throws Exception{
        long until=System.nanoTime()+TimeUnit.MILLISECONDS.toNanos(millis);DrainProbe.Observation last;
        do{last=probe.observe(expected);if(last.status()==DrainProbe.Status.READY)return last;if(last.status()==DrainProbe.Status.FAILED)throw new IOException(last.toString());Thread.sleep(20);}while(System.nanoTime()-until<0);throw new TimeoutException("remote readiness/apply: "+last);
    }
    public static void waitForReady(Path bundle,Path root)throws Exception{
        Properties run=verify(bundle,root);var addresses=PilotFiles.addresses(PilotFiles.readProperties(root.resolve("topology.properties")));ready(bundle,root,run,addresses.keySet());var plan=PilotFiles.Plan.read(root.resolve("profile.properties"));
        try(var transport=admin(addresses,plan.socketMs());var probe=new RemoteO00DrainProbe(transport,PilotMain::empty,new ArrayList<>(addresses.keySet()),new NodeId(1),run.getProperty("run.id"))){var observation=await(probe,0,plan.barrierMs());System.out.println("PILOT_REMOTE_READY "+observation.evidence());}
    }
    private static O00BenchmarkClient.Settings settings(String run,PilotFiles.Plan p,List<NodeId> nodes,long base,int streams){return new O00BenchmarkClient.Settings(run,"pilot-1",streams,base,nodes,new NodeId(1),nodes.get(1),TimeUnit.MILLISECONDS.toNanos(p.operationMs()),3,10_000_000,Math.max(8,p.concurrency()),256,5_000_000);}
    public static void run(Path bundle,Path root)throws Exception{
        if(Files.exists(root.resolve("distributed.properties")))throw new IOException("Use DistributedPilotMain for a distributed run");
        Properties props=verify(bundle,root);String run=props.getProperty("run.id");var addresses=PilotFiles.addresses(PilotFiles.readProperties(root.resolve("topology.properties")));var replicas=ready(bundle,root,props,addresses.keySet());var p=PilotFiles.Plan.read(root.resolve("profile.properties"));
        Files.writeString(root.resolve("CLIENT_STARTED"),"one load coordinator only\n",StandardOpenOption.CREATE_NEW);String clock="load-pid-"+ProcessHandle.current().pid();
        try(var control=admin(addresses,p.socketMs())){
            var client=O00TcpClients.open(settings(run,p,new ArrayList<>(addresses.keySet()),1_000_000,p.trace().streamCount()),addresses,p.socketMs(),TimeSource.system());
            try(client;var probe=new RemoteO00DrainProbe(control,client::diagnostics,new ArrayList<>(addresses.keySet()),new NodeId(1),run)){
                var latency=new LatencyCollector(run,"pilot-1",clock,TimeUnit.SECONDS.toNanos(60));var plan=new ClosedLoopPlan(run,"pilot-1",p.concurrency(),p.warm(),p.measured(),0,0,new ClosedLoopPlan.Limits(TimeUnit.MILLISECONDS.toNanos(p.workMs()),TimeUnit.MILLISECONDS.toNanos(p.operationMs()+1000),TimeUnit.SECONDS.toNanos(2),TimeUnit.MILLISECONDS.toNanos(p.barrierMs()),TimeUnit.SECONDS.toNanos(10),10_000_000,10000));
                var result=new ClosedLoopRunner(new TraceGenerator(p.trace()),plan,client,probe,latency).run();
                var metadata=new BenchmarkRunWriter.Metadata("O00","O00-PILOT-01",addresses.size(),null,null,clock,Set.of("M01","M03","M07","M10","M11"),Map.of("purpose","functional_pilot","replica_topology","separate_processes","remote_probe","view_and_followerRead_applied_prefix","persistence","RAM_replica_log_stable_View_KV_force_off"));
                var output=BenchmarkRunWriter.write(root.resolve("result"),metadata,p.trace(),plan,result,latency.freeze());
                if(!result.passed()||!output.valid())throw new IOException("workload invalid: "+result.failure()+" / "+output.invalidReasons());
                var audit=audit(addresses,p,run,result.expectedWrites());verify(bundle,root);
                List<Map<String,Object>> nodes=new ArrayList<>();for(Properties node:replicas)nodes.add(Map.of("node_id",PilotFiles.number(node,"node.id"),"pid",PilotFiles.number(node,"pid"),"instance_id",node.getProperty("instance.id")));
                PilotFiles.json(root.resolve("pilot-summary.json"),Map.of("schema_version","ionia-o00-pilot-v1","run_id",run,"purpose","functional_pilot_not_performance_comparison","passed",true,"load_pid",ProcessHandle.current().pid(),"replicas",nodes,"input_sha256",PilotFiles.sha(root.resolve("INPUT_SHA256SUMS")),"workload_expected_writes",result.expectedWrites(),"audit",audit,"completed_at",Instant.now().toString()));
                Files.writeString(root.resolve("PILOT_COMPLETE"),PilotFiles.sha(root.resolve("pilot-summary.json"))+"\n",StandardOpenOption.CREATE_NEW);System.out.println("PILOT_PASS="+root.toAbsolutePath());
            }
        }catch(Exception e){PilotFiles.json(root.resolve("PILOT_FAILED.json"),Map.of("passed",false,"error",e.toString(),"run_id",run));throw e;}
    }
    static Map<String,Object> audit(Map<NodeId,InetSocketAddress> addresses,PilotFiles.Plan p,String run,long baseline)throws Exception{
        List<NodeId> nodes=new ArrayList<>(addresses.keySet());Bytes key=Bytes.utf8("__pilot_audit/"+run),a=Bytes.utf8("pilot-value-A/"+run),b=Bytes.utf8("pilot-value-B/"+run);
        try(var client=O00TcpClients.open(settings(run+"-audit",p,nodes,2_000_000,1),addresses,p.socketMs(),TimeSource.system());var control=admin(addresses,p.socketMs());var probe=new RemoteO00DrainProbe(control,client::diagnostics,nodes,new NodeId(1),run)){
            List<Command> commands=List.of(new Command.Get(key),new Command.Set(key,a),new Command.Get(key),new Command.Delete(key),new Command.Get(key),new Command.Delete(key),new Command.Set(key,b),new Command.Get(key));
            ResultStatus[] expected={ResultStatus.MISS,ResultStatus.OK,ResultStatus.OK,ResultStatus.OK,ResultStatus.MISS,ResultStatus.MISS,ResultStatus.OK,ResultStatus.OK};long writes=baseline;List<Map<String,Object>> checks=new ArrayList<>();
            for(int i=0;i<commands.size();i++){
                var outcome=client.submit(new LogicalRequest(TracePhase.MEASURE,i,0,2,i,commands.get(i))).toCompletableFuture().get(p.operationMs()+1000,TimeUnit.MILLISECONDS);
                if(outcome.category()!=ClientOutcome.Category.SUCCESS||outcome.result().status()!=expected[i])throw new IOException("CRUD audit status at "+i);
                if(i==2&&!outcome.result().value().orElseThrow().equals(a)||i==7&&!outcome.result().value().orElseThrow().equals(b))throw new IOException("CRUD audit value");
                if(!(commands.get(i) instanceof Command.Get))writes++;await(probe,writes,p.barrierMs());checks.add(Map.of("step",i,"status",expected[i],"applied_frontier",writes));
                if(i==1||i==3||i==6)for(NodeId node:nodes){var request=new IoniaClientTransport.Request(0,new Command.Get(key),new ClientId(Long.MAX_VALUE-20),new OperationId(i+1),RoutingVersion.ZERO,OwnershipEpoch.ZERO,TimeUnit.MILLISECONDS.toNanos(p.operationMs()));var read=control.followerRead(node,request).toCompletableFuture().get(p.operationMs()+1000,TimeUnit.MILLISECONDS);Bytes wanted=i==1?a:i==6?b:null;
                    if(!read.nodeId().equals(node)||read.view()!=0||!read.clientId().equals(request.clientId())||!read.operationId().equals(request.operationId())||read.appliedIndex()!=writes||read.result().status()!=(wanted==null?ResultStatus.MISS:ResultStatus.OK)||wanted!=null&&!read.result().value().orElseThrow().equals(wanted))throw new IOException("per-replica audit failed: "+node);
                }
            }
            return Map.of("method","sequential_reserved_key_after_apply","passed",true,"steps",checks,"replica_value_checks",nodes.size()*3,"final_applied_frontier",writes,"not_claimed","global_history_linearizability_or_power_loss_durability");
        }
    }
}
