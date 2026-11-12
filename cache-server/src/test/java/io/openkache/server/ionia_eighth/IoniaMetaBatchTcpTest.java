package io.openkache.server.ionia_eighth;

import static org.junit.jupiter.api.Assertions.*;
import io.openkache.core.api.*;
import io.openkache.core.model.*;
import io.openkache.core.replication.ionia.*;
import io.openkache.core.replication.ionia_eighth.*;
import io.openkache.core.runtime.TimerEvent;
import io.openkache.core.spi.TimeSource;
import io.openkache.server.ionia.*;
import java.io.*;
import java.net.*;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

@Timeout(40)
class IoniaMetaBatchTcpTest {
    @TempDir Path directory;
    static RequestContext context(TimeSource time,long id){return new RequestContext(new ClientId(7),new OperationId(id),RoutingVersion.ZERO,OwnershipEpoch.ZERO,Deadline.after(time,Duration.ofSeconds(10)));}
    @Test void thirtyTwoConcurrentHotKeyWritesAndReadsUseFourRealMetaEnvelopes() throws Exception{
        List<NodeId> ids=List.of(new NodeId(1),new NodeId(2),new NodeId(3));Map<NodeId,InetSocketAddress> addresses=new LinkedHashMap<>();
        List<ServerSocket> reserved=new ArrayList<>();List<IoniaEighthNode> nodes=new ArrayList<>();
        try{
            for(NodeId id:ids){var socket=new ServerSocket(0);reserved.add(socket);addresses.put(id,new InetSocketAddress("127.0.0.1",socket.getLocalPort()));}
            for(var socket:reserved)socket.close();
            for(NodeId id:ids)nodes.add(new IoniaEighthNode(new IoniaConfig(id,ids.getFirst(),ids,0),addresses,directory.resolve("node"+id.value()),2,10000));
            var config=new IoniaConfig(ids.getFirst(),ids.getFirst(),ids,0);var time=TimeSource.system();Bytes key=Bytes.utf8("hot");var trace=new ConcurrentLinkedQueue<String>();
            try(var transport=new IoniaMetaBatchTcpTransport(new NodeId(0),IoniaMetaBatchSettings.configuration(config.configurationId()),addresses,5000);
                    var client=new IoniaMetaBatchClient(new ClientId(7),ids,ids.getFirst(),0,new Observed(transport,trace),time,new IoniaMetaBatchSettings(8,65536,1_000_000_000L,256,4))){
                List<CompletableFuture<Result>> writes=new ArrayList<>();for(int i=1;i<=32;i++)writes.add(client.execute(new Command.Set(key,Bytes.utf8("v"+i)),context(time,i)).toCompletableFuture());
                for(var write:writes)assertEquals(ResultStatus.OK,write.get(5,TimeUnit.SECONDS).status());
                List<CompletableFuture<Result>> reads=new ArrayList<>();for(int i=0;i<32;i++)reads.add(client.execute(new Command.Get(key),context(time,100+i)).toCompletableFuture());
                for(var read:reads){Result value=read.get(8,TimeUnit.SECONDS);assertEquals(ResultStatus.OK,value.status(),()->"value="+value+" client="+client.diagnostics()+" states="+nodes.stream().map(n->n.engine().progress()+"/"+n.engine().viewInfo()+"/"+n.engine().failureReason()).toList()+" RPC="+trace);assertEquals(32,value.valueVersion());}
                assertEquals(4,transport.batchCalls());assertEquals(32,client.diagnostics().sentItems());assertEquals(0,client.diagnostics().outstanding());
                var expired=new IoniaClientTransport.Request(0,new Command.Get(key),new ClientId(7),new OperationId(300),RoutingVersion.ZERO,OwnershipEpoch.ZERO,0);
                var fresh=new IoniaClientTransport.Request(0,new Command.Get(key),new ClientId(7),new OperationId(301),RoutingVersion.ZERO,OwnershipEpoch.ZERO,TimeUnit.SECONDS.toNanos(5));
                var pair=transport.metaBatch(ids.getFirst(),List.of(new IoniaMetaBatchTransport.Item(500,expired),new IoniaMetaBatchTransport.Item(501,fresh))).toCompletableFuture().get(5,TimeUnit.SECONDS);
                assertEquals(ResultStatus.TIMEOUT,pair.getFirst().reply().data().status());assertTrue(pair.getLast().reply().data()==null||pair.getLast().reply().data().status()==ResultStatus.OK);
                var deleted=client.execute(new Command.Delete(key),context(time,200));assertEquals(ResultStatus.OK,deleted.toCompletableFuture().get(5,TimeUnit.SECONDS).status());
                var miss=client.execute(new Command.Get(key),context(time,201));
                long end=System.nanoTime()+TimeUnit.SECONDS.toNanos(5);while(!miss.toCompletableFuture().isDone()&&System.nanoTime()<end){client.onTimer(new TimerEvent(IoniaEngine.TICK,time.monotonicNanos()));Thread.sleep(10);}
                assertEquals(ResultStatus.MISS,miss.toCompletableFuture().get(1,TimeUnit.SECONDS).status());
                for(var node:nodes)assertTrue(node.engine().failureReason().isEmpty());
                try(var files=Files.list(directory.resolve("node1"))){assertEquals(2,files.filter(p->p.getFileName().toString().matches("kv-[0-9]+[.]data")).count());}
                System.out.println("V06 actual TCP: 32 hot-key writes; 32 GETs -> 4 Meta envelopes E8; original Engine/S2; no throughput or p99 claim; profile="+client.diagnostics().profile());
            }
            // V06 stable View metadata intentionally refuses an O00 open of the same directory.
            for(var node:nodes)node.close();
            assertThrows(IOException.class,()->new IoniaNode(new IoniaConfig(ids.getFirst(),ids.getFirst(),ids,0),addresses,directory.resolve("node1"),2,10000));
        }finally{for(var node:nodes)node.close();for(var socket:reserved)socket.close();}
    }
    @Test void wrongConfigurationAndCorruptedEnvelopeNeverProduceBatchSuccess() throws Exception{
        for(boolean wrongConfig:new boolean[]{false,true}){
            try(var server=new ServerSocket(0);var executor=Executors.newVirtualThreadPerTaskExecutor()){
                var address=Map.of(new NodeId(1),new InetSocketAddress("127.0.0.1",server.getLocalPort()));
                Future<?> peer=executor.submit(()->{try(var socket=server.accept()){
                    var p=IoniaWire.read(socket.getInputStream());
                    IoniaWire.write(socket.getOutputStream(),wrongConfig?p.correlation():p.correlation()+1,p.target(),p.source(),wrongConfig?"wrong":p.configuration(),p.kind(),out->out.writeInt(0));
                }catch(IOException error){throw new UncheckedIOException(error);}});
                try(var transport=new IoniaMetaBatchTcpTransport(new NodeId(0),"fixture",address,1000)){
                    var request=new IoniaClientTransport.Request(0,new Command.Get(Bytes.utf8("k")),new ClientId(7),new OperationId(1),RoutingVersion.ZERO,OwnershipEpoch.ZERO,1_000_000_000L);
                    assertThrows(ExecutionException.class,()->transport.metaBatch(new NodeId(1),List.of(new IoniaMetaBatchTransport.Item(1,request))).toCompletableFuture().get(2,TimeUnit.SECONDS));
                }peer.get(2,TimeUnit.SECONDS);
            }
        }
    }

    private static final class Observed implements IoniaMetaBatchTransport{
        final IoniaMetaBatchTcpTransport raw;final Queue<String> trace;
        Observed(IoniaMetaBatchTcpTransport raw,Queue<String> trace){this.raw=raw;this.trace=trace;}
        private <T> CompletionStage<T> observe(String method,NodeId node,CompletionStage<T> stage){
            return stage.whenComplete((value,error)->trace.add(method+"->"+node+" "+(error==null?value:error.getClass().getName()+":"+error.getMessage())));
        }
        public CompletionStage<WriteAck> record(NodeId node,Request r){return observe("RECORD",node,raw.record(node,r));}
        public CompletionStage<Reply> execute(NodeId node,Request r){return observe("EXECUTE",node,raw.execute(node,r));}
        public CompletionStage<ReadReply> followerRead(NodeId node,Request r){return observe("READ",node,raw.followerRead(node,r));}
        public CompletionStage<IoniaEngine.ViewInfo> probe(NodeId node){return observe("PROBE",node,raw.probe(node));}
        public CompletionStage<List<Answer>> metaBatch(NodeId node,List<Item> items){return observe("BATCH",node,raw.metaBatch(node,items));}
    }
}
