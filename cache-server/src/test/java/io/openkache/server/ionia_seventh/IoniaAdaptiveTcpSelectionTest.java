package io.openkache.server.ionia_seventh;
import static org.junit.jupiter.api.Assertions.*;
import io.openkache.core.api.*;
import io.openkache.core.model.*;
import io.openkache.core.replication.ionia.*;
import io.openkache.core.replication.ionia_seventh.*;
import io.openkache.core.spi.TimeSource;
import io.openkache.server.ionia.*;
import java.net.*;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

@Timeout(30)
class IoniaAdaptiveTcpSelectionTest {
    @TempDir Path directory;
    @Test void V05_16_delayedReadReplyIsAvoidedAndExplorationRevisitsIt()throws Exception{
        var ids=List.of(new NodeId(1),new NodeId(2),new NodeId(3));Map<NodeId,InetSocketAddress> addresses=new LinkedHashMap<>();List<IoniaNode> nodes=new ArrayList<>();
        for(var id:ids)try(var socket=new ServerSocket(0)){addresses.put(id,new InetSocketAddress("127.0.0.1",socket.getLocalPort()));}
        try{
            for(var id:ids)nodes.add(new IoniaNode(new IoniaConfig(id,ids.getFirst(),ids,0),addresses,directory.resolve("node"+id.value()),2,5000));
            var membership=new IoniaConfig(ids.getFirst(),ids.getFirst(),ids,0);var time=TimeSource.system();var id=new ClientId(0);
            try(var tcp=new IoniaTcpTransport(new NodeId(0),membership.configurationId(),addresses,2000);var delayed=new DelayedReads(tcp,ids.get(1));
                var client=new IoniaAdaptiveReadClient(id,ids,ids.getFirst(),0,delayed,time,new IoniaReadSelectionSettings(IoniaReadSelectionSettings.Mode.ADAPTIVE,64,32,1,1000,250,4))){
                var key=Bytes.utf8("selection-key");long op=1;
                assertEquals(ResultStatus.OK,client.execute(new Command.Set(key,Bytes.utf8("v")),context(id,op++,time)).toCompletableFuture().get(5,TimeUnit.SECONDS).status());
                for(int i=0;i<12;i++)assertEquals(Bytes.utf8("v"),client.execute(new Command.Get(key),context(id,op++,time)).toCompletableFuture().get(5,TimeUnit.SECONDS).value().orElseThrow());
                var d=client.diagnostics();var slow=d.nodes().stream().filter(n->n.node().equals(ids.get(1))).findFirst().orElseThrow();var fast=d.nodes().stream().filter(n->n.node().equals(ids.get(2))).findFirst().orElseThrow();
                assertTrue(slow.rawSuccess()>0);assertTrue(fast.rawSuccess()>0);assertTrue(slow.ewmaNanos()>fast.ewmaNanos());assertTrue(fast.selections()>slow.selections());
                long before=slow.rawStarts();delayed.delay.set(false);
                for(int i=0;i<8;i++)client.execute(new Command.Get(key),context(id,op++,time)).toCompletableFuture().get(5,TimeUnit.SECONDS);
                var after=client.diagnostics().nodes().stream().filter(n->n.node().equals(ids.get(1))).findFirst().orElseThrow();assertTrue(after.rawStarts()>before,"exploration must revisit recovered candidate");
                for(var n:nodes)assertTrue(n.engine().failureReason().isEmpty());
            }
        }finally{for(var n:nodes)n.close();}
    }
    static RequestContext context(ClientId id,long operation,TimeSource time){return new RequestContext(id,new OperationId(operation),RoutingVersion.ZERO,OwnershipEpoch.ZERO,Deadline.after(time,Duration.ofSeconds(5)));}
    static final class DelayedReads implements IoniaClientTransport,AutoCloseable {
        final IoniaTcpTransport tcp;final NodeId slow;final AtomicBoolean delay=new AtomicBoolean(true);final ScheduledExecutorService timers=Executors.newSingleThreadScheduledExecutor();
        DelayedReads(IoniaTcpTransport tcp,NodeId slow){this.tcp=tcp;this.slow=slow;}
        public CompletionStage<WriteAck> record(NodeId n,Request r){return tcp.record(n,r);}public CompletionStage<Reply> execute(NodeId n,Request r){return tcp.execute(n,r);}
        public CompletionStage<MetaReply> metaQuery(NodeId n,Request r){return tcp.metaQuery(n,r);}public CompletionStage<IoniaEngine.ViewInfo> probe(NodeId n){return tcp.probe(n);}
        public CompletionStage<ReadReply> followerRead(NodeId n,Request r){
            var stage=tcp.followerRead(n,r);if(!n.equals(slow)||!delay.get())return stage;CompletableFuture<ReadReply> held=new CompletableFuture<>();
            stage.whenComplete((v,e)->timers.schedule(()->{if(e!=null)held.completeExceptionally(e);else held.complete(v);},80,TimeUnit.MILLISECONDS));return held;
        }
        public void close(){timers.shutdownNow();}
    }
    @Test void V05_18_clientSettingsStaySeparateFromOriginalServerProperties()throws Exception{
        Path file=directory.resolve("bad.properties");Files.writeString(file,"node.id=1\nreplicas=1@127.0.0.1:1,2@127.0.0.1:2,3@127.0.0.1:3\n");
        assertThrows(IllegalArgumentException.class,()->IoniaAdaptiveReadClientMain.main(new String[]{file.toString(),"get","k"}));
    }
}
