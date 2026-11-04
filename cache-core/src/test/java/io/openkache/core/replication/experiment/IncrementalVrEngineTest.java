package io.openkache.core.replication.experiment;

/** 같은 안전성 검사를 개선 모드에도 적용한다. 재전송 정책에 맞는 관측 시간을 사용한다. */
class IncrementalVrEngineTest extends VrEngineTest {
    @Override protected ProtocolCluster cluster(long seed) { return new ProtocolCluster(ProtocolCluster.Variant.VR_INCREMENTAL,1,100,seed); }
    @Override protected int catchupMs() { return 1500; }
    @Override protected int electionMs() { return 1500; }
    @Override protected int requestWaitMs() { return 800; }
    @Override protected void writeRound(ProtocolCluster c,int primary,int round,int isolated) {
        long end=c.now()+ProtocolCluster.ms(10000);
        java.util.concurrent.CompletableFuture<io.openkache.core.api.Result> pending=null;
        while(c.now()<end) {
            if(pending==null) {
                for(int i=0;i<3;i++) if(i!=isolated) {
                    var o=c.observation(i);
                    if(o.state()==io.openkache.core.replication.vr.VrEngine.State.NORMAL && o.view()%3==i) {
                        pending=c.set(i,round,"shared","value-"+round,1000); break;
                    }
                }
            }
            if(pending!=null && pending.isDone()) {
                if(pending.join().status()==io.openkache.core.api.ResultStatus.OK) return;
                pending=null;
            }
            c.advance(10);
        }
        org.junit.jupiter.api.Assertions.fail("same client operation never completed after retries");
    }
    @Override protected void waitForElection(ProtocolCluster c,int isolated) {
        long end=c.now()+ProtocolCluster.ms(10000);
        while(c.now()<end) {
            for(int i=0;i<3;i++) if(i!=isolated) {
                var o=c.observation(i);
                if(o.state()==io.openkache.core.replication.vr.VrEngine.State.NORMAL && o.view()%3==i) return;
            }
            c.advance(10);
        }
    }
    @Override protected void settleAfterHeal(ProtocolCluster c,String expected) {
        // 유실 시 재전송 간격이 4초까지 늘어난다. 즉시 복사본 일치를 가정하지 않고,
        // 유실을 계속 주입하면서 최대 10초 안에 모든 노드가 따라잡는지 검사한다.
        long end=c.now()+ProtocolCluster.ms(10000);
        while(c.now()<end) {
            boolean converged=true; long view=c.observation(0).view();
            for(int i=0;i<3;i++) {
                var r=c.local(i,"shared"); var o=c.observation(i);
                if(r.status()!=io.openkache.core.api.ResultStatus.OK || !r.value().orElseThrow().equals(io.openkache.core.model.Bytes.utf8(expected))
                        || o.view()!=view || o.state()!=io.openkache.core.replication.vr.VrEngine.State.NORMAL) converged=false;
            }
            if(converged) return;
            c.advance(10);
        }
    }
}
