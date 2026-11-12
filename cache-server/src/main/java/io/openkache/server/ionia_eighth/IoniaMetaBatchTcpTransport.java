package io.openkache.server.ionia_eighth;

import io.openkache.core.model.*;
import io.openkache.core.replication.ionia.*;
import io.openkache.core.replication.ionia_eighth.*;
import io.openkache.server.ionia.*;
import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import io.openkache.server.observation.TcpTraffic;

/** Only Meta batches get a new physical TCP RPC. All other requests reuse O00 transport directly. */
public final class IoniaMetaBatchTcpTransport implements IoniaMetaBatchTransport,AutoCloseable {
    private final IoniaTcpTransport original;
    private final NodeId self;private final String configuration;private final Map<NodeId,InetSocketAddress> addresses;
    private final int timeoutMillis;private final ExecutorService io=Executors.newVirtualThreadPerTaskExecutor();
    private final Set<Socket> sockets=ConcurrentHashMap.newKeySet();private final AtomicLong sequence=new AtomicLong(),calls=new AtomicLong();
    private final AtomicBoolean closed=new AtomicBoolean();
    public IoniaMetaBatchTcpTransport(NodeId self,String configuration,Map<NodeId,InetSocketAddress> addresses,int timeoutMillis){
        original=new IoniaTcpTransport(self,configuration,addresses,timeoutMillis);this.self=self;this.configuration=configuration;this.addresses=Map.copyOf(addresses);this.timeoutMillis=timeoutMillis;
    }
    public long batchCalls(){return calls.get();}
    public CompletionStage<WriteAck> record(NodeId to,Request r){return original.record(to,r);}
    public CompletionStage<Reply> execute(NodeId to,Request r){return original.execute(to,r);}
    public CompletionStage<ReadReply> followerRead(NodeId to,Request r){return original.followerRead(to,r);}
    public CompletionStage<MetaReply> metaQuery(NodeId to,Request r){return original.metaQuery(to,r);}
    public CompletionStage<IoniaEngine.ViewInfo> probe(NodeId to){return original.probe(to);}
    public CompletionStage<List<Answer>> metaBatch(NodeId to,List<Item> request){
        List<Item> items=List.copyOf(request);IoniaMetaBatchCodec.validate(items);CompletableFuture<List<Answer>> result=new CompletableFuture<>();
        var traffic=TcpTraffic.exchange(configuration,self.value(),to.value(),IoniaMetaBatchCodec.META_BATCH,"");
        if(closed.get()||!addresses.containsKey(to)){traffic.rejected();return CompletableFuture.failedFuture(new IOException("closed transport/unknown replica"));}
        long correlation=sequence.incrementAndGet();if(correlation<=0){traffic.rejected();return CompletableFuture.failedFuture(new IOException("correlation exhausted"));}
        long start=System.nanoTime();calls.incrementAndGet();
        try{io.execute(()->{
            try(Socket socket=new Socket()){
                traffic.created(socket);
                sockets.add(socket);if(closed.get())throw new IOException("closed transport");
                List<Item> sent=atSend(items,start);
                long max=sent.stream().mapToLong(i->i.request().timeoutNanos()).max().orElse(0);
                int timeout=(int)Math.max(1,Math.min(timeoutMillis,TimeUnit.NANOSECONDS.toMillis(max)+1));
                traffic.connecting();socket.connect(addresses.get(to),Math.min(timeout,500));traffic.connected();socket.setSoTimeout(timeout);socket.setTcpNoDelay(true);
                List<Item> wire=atSend(items,start);
                IoniaWire.write(traffic.output(socket.getOutputStream()),correlation,self,to,configuration,IoniaMetaBatchCodec.META_BATCH,out->IoniaMetaBatchCodec.requests(out,wire));traffic.written();
                IoniaWire.Packet reply=IoniaWire.read(traffic.input(socket.getInputStream()));traffic.replyRead();
                if(reply.correlation()!=correlation||!reply.source().equals(to)||!reply.target().equals(self)||!reply.configuration().equals(configuration)||reply.kind()!=IoniaMetaBatchCodec.META_BATCH)throw new IOException("batch envelope mismatch");
                List<Answer> answers=IoniaMetaBatchCodec.answers(to,reply.body());traffic.succeeded();result.complete(answers);
            }catch(IOException|RuntimeException error){result.completeExceptionally(error);}finally{traffic.close();sockets.removeIf(Socket::isClosed);}
        });}catch(RejectedExecutionException error){traffic.rejected();result.completeExceptionally(error);}
        return result;
    }
    private static List<Item> atSend(List<Item> items,long start){
        return items.stream().map(item->{Request r=item.request();return new Item(item.token(),new Request(r.view(),r.command(),r.clientId(),r.operationId(),r.routingVersion(),r.ownershipEpoch(),
                Math.max(0,r.timeoutNanos()-Math.max(0,System.nanoTime()-start))));}).toList();
    }
    public void close(){if(!closed.compareAndSet(false,true))return;original.close();sockets.forEach(s->{try{s.close();}catch(IOException ignored){}});io.shutdownNow();}
}
