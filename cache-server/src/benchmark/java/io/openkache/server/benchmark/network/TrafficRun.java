package io.openkache.server.benchmark.network;

import io.openkache.runner.benchmark.*;
import io.openkache.runner.benchmark.client.*;
import io.openkache.runner.benchmark.execution.*;
import io.openkache.runner.benchmark.measurement.LatencyCollector;
import io.openkache.server.observation.TcpTraffic;
import java.util.Set;

/** An experiment owns one process/configuration subscription; no new sampler thread. */
public final class TrafficRun implements RunLifecycle, ClosedLoopRunner.Observer, OpenLoopRunner.Observer, AutoCloseable {
    private final TcpTraffic.Scope scope;
    private final LatencyCollector logical;
    public TrafficRun(String run,String slot,String clock,String configuration,Set<Long> members){
        logical=new LatencyCollector(run,slot,clock,30_000_000_000L);scope=TcpTraffic.open(configuration,members,50_000);
    }
    @Override public void stage(ClosedLoopResult.Stage stage,long at){scope.stage(TcpTraffic.Stage.valueOf(stage.name()),at);}
    @Override public void outcome(LogicalRequest r,long due,ClientOutcome result){logical.outcome(r,due,result);}
    @Override public void outcome(OpenLoopResult.ScheduledOutcome e){logical.outcome(e);}
    @Override public void finished(long at){
        boolean idle=false;
        try{idle=scope.awaitIdle(3_000_000_000L);}catch(InterruptedException e){Thread.currentThread().interrupt();}
        finally{scope.close();}
        if(!idle || !scope.snapshot().valid())throw new IllegalStateException("network observation incomplete");
    }
    public TcpTraffic.Snapshot network(){return scope.snapshot();}
    public LatencyCollector.Snapshot logical(){return logical.freeze();}
    @Override public void close(){scope.close();}
}
