package io.openkache.core.replication.ionia_eighth;

import io.openkache.core.api.*;
import io.openkache.core.model.*;
import io.openkache.core.replication.ionia.*;
import io.openkache.core.runtime.TimerEvent;
import io.openkache.core.spi.TimeSource;
import java.util.*;
import java.util.concurrent.*;

/** O00 client and its exact Meta/index/View certification reused through a Meta-only coalescing adapter.
 * There is no adaptive follower routing or automatic library retry. Public cancellation does not cancel private protocol work.
 */
public final class IoniaMetaBatchClient implements AutoCloseable {
    private final IoniaClient delegate;
    private final IoniaBatchedMetaTransport batching;
    public IoniaMetaBatchClient(ClientId id,List<NodeId> replicas,NodeId leader,long view,IoniaMetaBatchTransport raw,TimeSource time,IoniaMetaBatchSettings settings){
        batching=new IoniaBatchedMetaTransport(raw,time,settings);batching.advanceView(view);
        NodeId follower=replicas.stream().filter(n->!n.equals(leader)).findFirst().orElseThrow();
        delegate=new IoniaClient(id,replicas,leader,view,batching,time,new IoniaClient.Options(follower,true,50_000_000L));
    }
    public CompletionStage<Result> execute(Command command,RequestContext context){
        CompletableFuture<Result> visible=new CompletableFuture<>();
        delegate.execute(command,context).whenComplete((value,error)->{if(error!=null)visible.completeExceptionally(error);else visible.complete(value);});
        return visible;
    }
    public void updateView(long view,NodeId leader){delegate.updateView(view,leader);batching.advanceView(view);}
    /** Discovery alone grants no read authority. O00 still certifies every GET. */
    public CompletionStage<IoniaEngine.ViewInfo> refreshView(){return delegate.refreshView().thenApply(info->{batching.advanceView(info.view());return info;});}
    public void onTimer(TimerEvent event){delegate.onTimer(event);batching.onTimer();}
    public IoniaBatchedMetaTransport.Diagnostics diagnostics(){return batching.diagnostics();}
    @Override public void close(){delegate.close();batching.close();}
}
