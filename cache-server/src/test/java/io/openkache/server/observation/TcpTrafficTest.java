package io.openkache.server.observation;

import static org.junit.jupiter.api.Assertions.*;
import io.openkache.core.model.NodeId;
import io.openkache.server.ionia.*;
import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import static io.openkache.server.observation.TcpTraffic.Metric.*;

@Timeout(10)
@SuppressWarnings("try") // Explicit closure is intentional to assert post-close counters and idempotence.
class TcpTrafficTest {
    static String config(){return "traffic-"+UUID.randomUUID();}
    static TcpTraffic.Scope scope(String c,int n){return TcpTraffic.open(c,Set.of(1L,2L),n);}
    static long sum(TcpTraffic.Snapshot s,boolean frame,TcpTraffic.Metric m){return s.rows().stream().filter(r->r.key().frame()==frame).mapToLong(r->r.counts().get(m)).sum();}
    static void wire(OutputStream out,String c)throws Exception{IoniaWire.write(out,1,new NodeId(1),new NodeId(2),c,IoniaWire.PROBE,o->o.writeLong(123));}
    @Test void enabledWireIsByteIdenticalAndCountsLengthPrefixExactly()throws Exception{
        String c=config();var baseline=new ByteArrayOutputStream();wire(baseline,c);
        try(var s=scope(c,10)){var observed=new ByteArrayOutputStream();wire(observed,c);s.close();assertArrayEquals(baseline.toByteArray(),observed.toByteArray());assertEquals(observed.size(),sum(s.snapshot(),true,SENT_BYTES));assertEquals(1,sum(s.snapshot(),true,OK));assertTrue(s.snapshot().valid());}
    }
    @Test void bulkReadsAndWritesAreNotCountedTwice()throws Exception{
        String c=config();try(var s=scope(c,10)){try(var e=TcpTraffic.exchange(c,0,1,4,"")){var out=new ByteArrayOutputStream();e.output(out).write(new byte[19]);assertEquals(19,e.input(new ByteArrayInputStream(out.toByteArray())).readAllBytes().length);e.written();e.replyRead();e.succeeded();}s.close();assertEquals(19,sum(s.snapshot(),false,SENT_BYTES));assertEquals(19,sum(s.snapshot(),false,RECEIVED_BYTES));}
    }
    @ParameterizedTest @ValueSource(booleans={false,true})
    void throwingPartialWriteIsUnknownNotInventedExactZero(boolean unchecked)throws Exception{
        String c=config();var actual=new ByteArrayOutputStream();try(var s=scope(c,10)){
            OutputStream broken=new OutputStream(){public void write(int b)throws IOException{actual.write(b);if(unchecked)throw new IllegalStateException("injected");throw new IOException("injected");}public void write(byte[] b,int o,int n)throws IOException{actual.write(b,o,Math.min(2,n));if(unchecked)throw new IllegalStateException("injected partial");throw new IOException("injected partial");}};
            if(unchecked){assertThrows(IllegalStateException.class,()->wire(broken,c));}else{assertThrows(IOException.class,()->wire(broken,c));}s.close();assertEquals(2,actual.size());assertEquals(0,sum(s.snapshot(),true,SENT_BYTES));assertEquals(1,sum(s.snapshot(),true,WRITE_UNCERTAIN));assertFalse(s.snapshot().valid());assertTrue(s.snapshot().issues().contains("WRITE_BYTES_UNCERTAIN"));
        }
    }
    @Test void registrationsAreExclusiveAndDifferentConfigurationsStaySeparate(){String c=config();try(var s=scope(c,10)){assertThrows(IllegalStateException.class,()->scope(c,10));try(var other=scope(config(),10)){try(var e=TcpTraffic.exchange(c,0,1,6,"")){e.succeeded();}other.close();assertTrue(other.snapshot().rows().isEmpty());}}try(var reopened=scope(c,10)){assertTrue(reopened.snapshot().rows().isEmpty());}}
    @Test void sealedPendingAndLateFinishAreInvalidAndOldSnapshotIsImmutable(){String c=config();var s=scope(c,10);var e=TcpTraffic.exchange(c,0,1,6,"");s.close();var before=s.snapshot();assertFalse(before.valid());assertEquals(1,before.pending());e.succeeded();e.close();e.close();assertEquals(0,s.snapshot().pending());assertEquals(1,s.snapshot().lateFinishes());assertEquals(1,before.pending());assertThrows(UnsupportedOperationException.class,()->before.rows().clear());}
    @Test void rowCapDoesNotEvictOrHideMissingObservations(){String c=config();try(var s=scope(c,1)){for(int k:new int[]{4,6})try(var e=TcpTraffic.exchange(c,0,1,k,"")){e.succeeded();}s.close();assertEquals(1,s.snapshot().rows().size());assertFalse(s.snapshot().valid());assertTrue(s.snapshot().issues().contains("ROW_CAPACITY"));}}
    @Test void fourConcurrentProducersPreserveAllTwoThousandExchanges()throws Exception{
        String c=config();try(var s=scope(c,64);var workers=Executors.newFixedThreadPool(4)){var start=new CyclicBarrier(4);List<Future<?>> jobs=new ArrayList<>();for(int t=0;t<4;t++)jobs.add(workers.submit(()->{start.await();for(int i=0;i<500;i++)try(var e=TcpTraffic.exchange(c,0,1,4,"")){e.succeeded();}return null;}));for(var f:jobs)f.get();s.close();assertTrue(s.snapshot().valid());assertEquals(2000,sum(s.snapshot(),false,OK));assertTrue(s.snapshot().peakExchanges()<=4);}
    }
    @Test void delayedExchangeRetainsAdmissionAndTerminalStagesAndActualWindow()throws Exception{String c=config();try(var s=scope(c,10)){var e=TcpTraffic.exchange(c,1,2,1,"Prepare");s.stage(TcpTraffic.Stage.LOAD,System.nanoTime());Thread.sleep(1100);e.succeeded();e.close();s.close();var row=s.snapshot().rows().getFirst();assertEquals(TcpTraffic.Stage.PREFLIGHT,row.key().startedStage());assertEquals(TcpTraffic.Stage.LOAD,row.key().terminalStage());assertTrue(row.key().window()>=1);assertEquals(1,row.counts().get(CROSS_STAGE));}}
    @Test void refusedConnectionHasNoFabricatedBytesOrMessagesAndClosesSocket()throws Exception{
        String c=config();int port;try(var unused=new ServerSocket(0)){port=unused.getLocalPort();}
        try(var s=scope(c,10);var transport=new IoniaTcpTransport(new NodeId(0),c,Map.of(new NodeId(1),new InetSocketAddress("127.0.0.1",port)),500)){
            assertThrows(ExecutionException.class,()->transport.probe(new NodeId(1)).toCompletableFuture().get(2,TimeUnit.SECONDS));assertTrue(s.awaitIdle(2_000_000_000L));s.close();var snap=s.snapshot();assertTrue(snap.valid());assertEquals(1,sum(snap,false,CONNECT_FAILED));assertEquals(1,sum(snap,false,CLOSED));assertEquals(0,sum(snap,true,OK));assertEquals(0,sum(snap,false,SENT_BYTES));
        }
    }
    @Test void wrongCorrelationPreservesWireEvidenceWithoutSuccessfulRpc()throws Exception{
        String c=config();try(var s=scope(c,20);var worker=Executors.newVirtualThreadPerTaskExecutor();var listener=new ServerSocket(0)){
            var server=worker.submit(()->{try(var socket=listener.accept()){var p=IoniaWire.read(socket.getInputStream());IoniaWire.write(socket.getOutputStream(),p.correlation()+1,new NodeId(1),new NodeId(0),c,p.kind(),o->o.writeLong(123));}return null;});
            try(var t=new IoniaTcpTransport(new NodeId(0),c,Map.of(new NodeId(1),new InetSocketAddress("127.0.0.1",listener.getLocalPort())),500)){
                assertThrows(ExecutionException.class,()->t.probe(new NodeId(1)).toCompletableFuture().get(2,TimeUnit.SECONDS));server.get();assertTrue(s.awaitIdle(2_000_000_000L));s.close();assertEquals(1,sum(s.snapshot(),false,FAILED));assertEquals(1,sum(s.snapshot(),false,REPLIES_READ));assertEquals(0,sum(s.snapshot(),false,OK));assertEquals(2,sum(s.snapshot(),true,OK));
            }
        }
    }
    @Test void invalidCoverageDoesNotThrowIntoTransportButInvalidatesCapture(){String c=config();try(var s=scope(c,10)){try(var e=TcpTraffic.exchange(c,99,1,100,"unknown")){e.succeeded();}s.close();assertFalse(s.snapshot().valid());assertEquals(2,s.snapshot().issues().size());}assertThrows(IllegalArgumentException.class,()->TcpTraffic.open("",Set.of(1L,2L),10));}
    @Test void disabledWireDoesNotCloseDestinationAndClosedTransportIsRejected()throws Exception{
        String c=config();boolean[] closed={false};var output=new ByteArrayOutputStream(){public void close(){closed[0]=true;}};wire(output,c);assertFalse(closed[0]);
        try(var s=scope(c,10);var t=new IoniaTcpTransport(new NodeId(0),c,Map.of(new NodeId(1),new InetSocketAddress("127.0.0.1",1)),100)){t.close();assertThrows(ExecutionException.class,()->t.probe(new NodeId(1)).toCompletableFuture().get());s.close();assertEquals(1,sum(s.snapshot(),false,REJECTED));assertEquals(0,sum(s.snapshot(),false,CREATED));}
    }
}
