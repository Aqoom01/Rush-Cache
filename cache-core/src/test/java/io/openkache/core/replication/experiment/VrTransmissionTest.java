package io.openkache.core.replication.experiment;

import io.openkache.core.api.*;
import io.openkache.core.model.*;
import io.openkache.core.replication.vr.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class VrTransmissionTest {
    @Test void normalResponseDependsOnNewEntryRatherThanHistory() {
        var shortHistory=NetworkCostExperiment.run(new NetworkCostExperiment.Condition("VR_INCREMENTAL","NORMAL",1,8192,1,100));
        var longHistory=NetworkCostExperiment.run(new NetworkCostExperiment.Condition("VR_INCREMENTAL","NORMAL",64,8192,1,100));
        assertEquals("OK",longHistory.outcome()); assertEquals(shortHistory.responseNs(),longHistory.responseNs());
        assertEquals(shortHistory.network(),longHistory.network());
        assertEquals(0,longHistory.maxViewAdvance());
    }
    @Test void idleHeartbeatCarriesNoLogData() {
        try(var c=new ProtocolCluster(ProtocolCluster.Variant.VR_INCREMENTAL,1,100,1,0)) {
            for(int i=1;i<=16;i++) {
                var f=c.set(0,i,"x-"+i,"v",1000); c.advance(10); assertEquals(ResultStatus.OK,f.join().status());
            }
            c.until(ProtocolCluster.ms((c.nowMs()/10+1)*10+9)); c.network.beginMeasurement(NetworkCostExperiment.MIB);
            c.advance(100);
            assertEquals(0,c.network.stats().dataBytes()); assertTrue(c.network.stats().controlBytes()>0);
            assertEquals(16,c.observation(2).applied());
        }
    }
    @Test void missingPrefixRequestsStateAndDoesNotApplyOutOfOrderEntry() {
        try(var c=new ProtocolCluster(ProtocolCluster.Variant.VR_INCREMENTAL,1,100,1)) {
            c.drop=e->e.to().value()==1 && (e.message() instanceof VrMessage.DeltaPrepare || e.message() instanceof VrMessage.StartView);
            var first=c.set(0,1,"x","one",1000); c.advance(10); assertEquals(ResultStatus.OK,first.join().status());
            c.drop=e->false;
            var second=c.set(0,2,"x","two",1000);
            var request=new VrMessage.Request(new ClientId(7),new OperationId(2),new Command.Set(Bytes.utf8("x"),Bytes.utf8("two")));
            c.nodes.get(1).onPeerMessage(new NodeId(0),new VrMessage.DeltaPrepare(0,2,List.of(request),1));
            assertEquals(0,c.observation(1).logLength()); assertEquals(ResultStatus.MISS,c.local(1,"x").status());
            c.advance(1200); assertEquals(ResultStatus.OK,second.join().status());
            assertEquals(Bytes.utf8("two"),c.local(1,"x").value().orElseThrow()); assertEquals(2,c.observation(1).logLength());
        }
    }
    @Test void lostCommitNoticeIsRecoveredBySmallHeartbeat() {
        try(var c=new ProtocolCluster(ProtocolCluster.Variant.VR_INCREMENTAL,1,100,1)) {
            c.drop=e->e.message() instanceof VrMessage.Commit;
            var f=c.set(0,1,"x","v",1000); c.advance(5); assertEquals(ResultStatus.OK,f.join().status());
            assertEquals(ResultStatus.MISS,c.local(2,"x").status());
            c.drop=e->false; c.advance(20);
            assertEquals(Bytes.utf8("v"),c.local(2,"x").value().orElseThrow());
        }
    }
    @Test void sendCompletionDoesNotReleaseWindowAndLostAckUsesBackoff() {
        var time=new io.openkache.core.replication.VirtualTimeSource();
        var kv=new io.openkache.core.kv.KvEngine(new NodeId(0),List.of(new io.openkache.core.replication.DirectMailbox()),time);
        List<io.openkache.core.spi.PeerEnvelope> sent=new ArrayList<>();
        var engine=new VrEngine(new NodeId(0),List.of(new NodeId(0),new NodeId(1),new NodeId(2)),kv,
                new io.openkache.core.replication.DirectMailbox(),e->{sent.add(e);return java.util.concurrent.CompletableFuture.completedFuture(null);},time,
                java.time.Duration.ofMillis(10),java.time.Duration.ofMillis(100));
        try {
            var f=engine.execute(new Command.Set(Bytes.utf8("x"),Bytes.utf8("v")),new RequestContext(new ClientId(7),new OperationId(1),RoutingVersion.ZERO,OwnershipEpoch.ZERO,new Deadline(ProtocolCluster.ms(5000)))).toCompletableFuture();
            for(int ms=0;ms<=990;ms+=10) tick(engine,time,ms);
            assertEquals(2,sent.stream().filter(e->e.message() instanceof VrMessage.DeltaPrepare).count());
            tick(engine,time,1000); assertEquals(4,sent.stream().filter(e->e.message() instanceof VrMessage.DeltaPrepare).count());
            tick(engine,time,2990); assertEquals(4,sent.stream().filter(e->e.message() instanceof VrMessage.DeltaPrepare).count());
            tick(engine,time,3000); assertEquals(6,sent.stream().filter(e->e.message() instanceof VrMessage.DeltaPrepare).count());
            engine.onPeerMessage(new NodeId(1),new VrMessage.PrepareOk(0,1)); assertEquals(ResultStatus.OK,f.join().status());
            tick(engine,time,4000);
            assertEquals(3,sent.stream().filter(e->e.to().value()==1 && e.message() instanceof VrMessage.DeltaPrepare).count());
        } finally { engine.close(); kv.close(); }
    }
    private void tick(VrEngine engine,io.openkache.core.replication.VirtualTimeSource time,int ms) {
        time.advanceTo(ProtocolCluster.ms(ms)); engine.onTimer(new io.openkache.core.runtime.TimerEvent(VrEngine.TICK,time.monotonicNanos()));
    }
    @Test void optimizedModePassesSameProtocolScenarioMatrix() {
        for(var scenario:ProtocolComparison.Scenario.values()) for(int delay:new int[]{1,5}) for(int seed=1;seed<=5;seed++)
            ProtocolComparison.run("VR_INCREMENTAL",scenario,delay,seed);
    }
}
