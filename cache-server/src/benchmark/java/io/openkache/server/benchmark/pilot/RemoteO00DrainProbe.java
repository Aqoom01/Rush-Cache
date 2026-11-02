package io.openkache.server.benchmark.pilot;

import io.openkache.core.api.*;
import io.openkache.core.model.*;
import io.openkache.core.replication.ionia.*;
import io.openkache.runner.benchmark.execution.DrainProbe;
import io.openkache.server.benchmark.ionia.O00BenchmarkClient;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/** Nonblocking remote applied-prefix observation using existing view and follower-read RPCs.
 * No recorded/commit/queue counters, storage force or peer-network drain is inferred.
 */
public final class RemoteO00DrainProbe implements DrainProbe,AutoCloseable {
    private record Pair(IoniaEngine.ViewInfo view,IoniaClientTransport.ReadReply read,long operation){}
    private record Sample(long expected,long ended,Observation observation){}
    private static final ClientId ADMIN=new ClientId(Long.MAX_VALUE-10);
    private final IoniaClientTransport transport;private final Supplier<O00BenchmarkClient.Diagnostics> client;
    private final List<NodeId> members;private final NodeId leader;private final Bytes key;private final AtomicLong sequence=new AtomicLong();
    private Sample last;private boolean pending,closed;
    public RemoteO00DrainProbe(IoniaClientTransport transport,Supplier<O00BenchmarkClient.Diagnostics> client,List<NodeId> members,NodeId leader,String run){
        this.transport=Objects.requireNonNull(transport);this.client=Objects.requireNonNull(client);this.members=List.copyOf(members);this.leader=leader;key=Bytes.utf8("__pilot_probe/"+run);
        new IoniaConfig(leader,leader,members,0);
    }
    @Override public Observation observe(long expected){return observe(expected,false);}
    @Override public Observation afterClose(long expected){return observe(expected,true);}
    private Observation observe(long expected,boolean closing){
        if(expected<0)throw new IllegalArgumentException("negative frontier");var d=client.get();Sample cached;boolean launch=false;
        synchronized(this){if(closed)return new Observation(Status.FAILED,"probe_closed",Map.of());cached=last;
            if(cached==null||cached.expected()!=expected||System.nanoTime()-cached.ended()>100_000_000L){cached=null;if(!pending){pending=true;launch=true;}}}
        if(launch)launch(expected);
        Map<String,Long> e=new LinkedHashMap<>();e.put("expected_writes",expected);e.put("client_pending",d.logical().pending());e.put("client_raw",(long)d.rpc().outstanding());e.put("client_active",(long)d.active());e.put("client_occupied",(long)d.occupiedSlots());e.put("client_closed",d.closed()?1L:0L);
        if(d.closeError()!=null||!closing&&d.closed())return new Observation(Status.FAILED,"client_closed_or_close_error",e);
        if(cached==null)return new Observation(Status.WAIT,"remote_sample_pending",e);
        e.putAll(cached.observation().evidence());if(cached.observation().status()!=Status.READY)return new Observation(cached.observation().status(),cached.observation().reason(),e);
        boolean idle=d.logical().pending()==0&&d.rpc().outstanding()==0&&d.active()==0&&d.occupiedSlots()==0;
        return new Observation(idle?Status.READY:Status.WAIT,idle?"remote_all_applied":"client_pending",e);
    }
    private void launch(long expected){
        Map<NodeId,CompletableFuture<Pair>> calls=new LinkedHashMap<>();
        try{
            for(NodeId node:members){long op=sequence.incrementAndGet();var request=new IoniaClientTransport.Request(0,new Command.Get(key),ADMIN,new OperationId(op),RoutingVersion.ZERO,OwnershipEpoch.ZERO,2_000_000_000L);
                calls.put(node,transport.probe(node).thenCombine(transport.followerRead(node,request),(v,r)->new Pair(v,r,op)).toCompletableFuture());}
            CompletableFuture.allOf(calls.values().toArray(CompletableFuture[]::new)).whenComplete((v,error)->{
                Observation observation;
                if(error!=null)observation=new Observation(Status.WAIT,"remote_unreachable_or_rpc_error",Map.of());
                else{
                    Map<String,Long> evidence=new LinkedHashMap<>();boolean waiting=false,failed=false;
                    for(var entry:calls.entrySet()){
                        var p=entry.getValue().join();var r=p.read();String prefix="node_"+entry.getKey().value()+"_";
                        evidence.put(prefix+"applied",r.appliedIndex());evidence.put(prefix+"view",p.view().view());
                        if(p.view().view()!=0||!p.view().leaderId().equals(leader))failed=true;
                        if(p.view().status()!=IoniaEngine.Status.NORMAL)waiting=true;
                        if(!r.nodeId().equals(entry.getKey())||r.view()!=0||!r.clientId().equals(ADMIN)||r.operationId().value()!=p.operation())failed=true;
                        if(r.result().status()!=ResultStatus.OK&&r.result().status()!=ResultStatus.MISS)waiting=true;
                        if(r.appliedIndex()>expected)failed=true;if(r.appliedIndex()!=expected)waiting=true;
                    }
                    observation=new Observation(failed?Status.FAILED:waiting?Status.WAIT:Status.READY,failed?"unexpected_remote_view_identity_or_frontier":waiting?"remote_apply_or_health_pending":"remote_all_applied",evidence);
                }
                synchronized(this){if(!closed)last=new Sample(expected,System.nanoTime(),observation);pending=false;}
            });
        }catch(RuntimeException e){synchronized(this){last=new Sample(expected,System.nanoTime(),new Observation(Status.WAIT,"remote_probe_exception",Map.of()));pending=false;}}
    }
    @Override public synchronized void close(){closed=true;}
}
