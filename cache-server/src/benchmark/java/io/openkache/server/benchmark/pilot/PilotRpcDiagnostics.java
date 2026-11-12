package io.openkache.server.benchmark.pilot;

import io.openkache.core.model.NodeId;
import io.openkache.core.replication.ionia.*;
import io.openkache.server.ionia.IoniaTcpTransport;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/** Bounded error/view diagnostics only; delegates every call, timeout and close unchanged. */
final class PilotRpcDiagnostics implements IoniaClientTransport,AutoCloseable {
    private final IoniaTcpTransport delegate;private final ArrayDeque<Map<String,Object>> errors=new ArrayDeque<>();private final Map<String,Object> views=new TreeMap<>();private final AtomicLong failures=new AtomicLong();
    PilotRpcDiagnostics(IoniaTcpTransport delegate){this.delegate=delegate;}
    private <T> CompletionStage<T> observed(String kind,NodeId node,CompletionStage<T> call){return call.whenComplete((reply,error)->{
        synchronized(this){if(error!=null){failures.incrementAndGet();Throwable cause=error;while(cause.getCause()!=null&&cause.getCause()!=cause)cause=cause.getCause();if(errors.size()==16)errors.removeFirst();errors.addLast(Map.of("kind",kind,"node",node.value(),"error",cause.getClass().getSimpleName(),"message",String.valueOf(cause.getMessage())));}else if(reply instanceof IoniaEngine.ViewInfo v){views.put("node_"+node.value(),v);}}
    });}
    synchronized Map<String,Object> snapshot(){return Map.of("rpc_failures",failures.get(),"last_errors",new ArrayList<>(errors),"last_views",new TreeMap<>(views));}
    @Override public CompletionStage<WriteAck> record(NodeId to,Request r){return observed("record",to,delegate.record(to,r));}
    @Override public CompletionStage<Reply> execute(NodeId to,Request r){return observed("execute",to,delegate.execute(to,r));}
    @Override public CompletionStage<ReadReply> followerRead(NodeId to,Request r){return observed("read",to,delegate.followerRead(to,r));}
    @Override public CompletionStage<MetaReply> metaQuery(NodeId to,Request r){return observed("meta",to,delegate.metaQuery(to,r));}
    @Override public CompletionStage<IoniaEngine.ViewInfo> probe(NodeId to){return observed("probe",to,delegate.probe(to));}
    @Override public void close(){delegate.close();}
}
