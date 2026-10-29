package io.openkache.core.replication.ionia_eighth;

import io.openkache.core.api.*;
import io.openkache.core.model.*;
import io.openkache.core.replication.ionia.*;
import io.openkache.core.spi.TimeSource;
import java.util.*;
import java.util.concurrent.*;

/** Thread-safe, timer-driven Meta coalescer over a borrowed transport. All I/O and completion callbacks run outside the control lock.
 * Pending admission includes queued and raw in-flight entries, even after caller cancellation/timeout/View invalidation.
 * A borrowed raw stage that never terminates retains its in-flight permits. No internal retries or stale Meta reuse.
 */
public final class IoniaBatchedMetaTransport implements IoniaClientTransport, AutoCloseable {
    private record Group(NodeId leader, long view) { }
    private static final class Entry {
        final long token, started; final Request request; final CompletableFuture<MetaReply> future = new CompletableFuture<>();
        Entry(long token, long started, Request request) { this.token=token; this.started=started; this.request=request; }
        long remaining(long now) { return Math.max(0, request.timeoutNanos() - Math.max(0, now-started)); }
        Request at(long now) { return new Request(request.view(), request.command(), request.clientId(), request.operationId(), request.routingVersion(), request.ownershipEpoch(), remaining(now)); }
        long bytes() { return 61L + request.command().key().size(); }
    }
    private record Flight(Group group, List<Entry> entries) { }
    public record Diagnostics(boolean closed, long view, int queued, int outstanding, int inFlight,
                              long admitted, long busy, long batches, long sentItems, long malformedBatches, long queuedTimeouts,
                              long queueWaitNanos, long maxQueueWaitNanos, String profile) { }
    private final Object lock = new Object();
    private final IoniaMetaBatchTransport raw;
    private final TimeSource time;
    private final IoniaMetaBatchSettings settings;
    private final Map<Group, ArrayDeque<Entry>> queues = new LinkedHashMap<>();
    private final Set<Flight> flights = Collections.newSetFromMap(new IdentityHashMap<>());
    private long view, sequence, admitted, busy, batches, sentItems, malformed, timedOut, waitTotal, waitMax;
    private int outstanding;
    private boolean closed;
    public IoniaBatchedMetaTransport(IoniaMetaBatchTransport raw, TimeSource time, IoniaMetaBatchSettings settings) {
        this.raw=Objects.requireNonNull(raw); this.time=Objects.requireNonNull(time); this.settings=Objects.requireNonNull(settings);
    }
    @Override public CompletionStage<WriteAck> record(NodeId to, Request r) { return raw.record(to,r); }
    @Override public CompletionStage<Reply> execute(NodeId to, Request r) { return raw.execute(to,r); }
    @Override public CompletionStage<ReadReply> followerRead(NodeId to, Request r) { return raw.followerRead(to,r); }
    @Override public CompletionStage<IoniaEngine.ViewInfo> probe(NodeId to) { return raw.probe(to); }
    @Override public CompletionStage<MetaReply> metaQuery(NodeId to, Request r) {
        Objects.requireNonNull(to); Objects.requireNonNull(r);
        List<Runnable> actions = new ArrayList<>(); CompletableFuture<MetaReply> result;
        synchronized (lock) {
            long now=time.monotonicNanos(); expire(now,actions);
            ResultStatus rejected = closed || r.view()<view || sequence==Long.MAX_VALUE ? ResultStatus.UNAVAILABLE
                    : !(r.command() instanceof Command.Get) || 65L+r.command().key().size()>settings.maxBytes() ? ResultStatus.BAD_REQUEST
                    : r.timeoutNanos()==0 ? ResultStatus.TIMEOUT : outstanding>=settings.pendingLimit() ? ResultStatus.BUSY : null;
            if (rejected!=null) { if(rejected==ResultStatus.BUSY)busy++; result=CompletableFuture.completedFuture(error(to,r,rejected)); }
            else {
                Entry entry=new Entry(++sequence,now,r); result=entry.future; outstanding++;admitted++;
                queues.computeIfAbsent(new Group(to,r.view()),ignored->new ArrayDeque<>()).add(entry);
            }
            drain(now,actions);
        }
        run(actions); return result;
    }
    public void onTimer() {
        List<Runnable> actions=new ArrayList<>();
        synchronized(lock){long now=time.monotonicNanos();expire(now,actions);drain(now,actions);}
        run(actions);
    }
    /** Same-view refresh preserves queued work. Old in-flight stage permits remain until its real completion. */
    public void advanceView(long next) {
        List<Runnable> actions=new ArrayList<>();
        synchronized(lock){
            if(next<view)throw new IllegalArgumentException("view regression"); view=next;
            Iterator<Map.Entry<Group,ArrayDeque<Entry>>> it=queues.entrySet().iterator();
            while(it.hasNext()){var q=it.next();if(q.getKey().view()<view){for(Entry e:q.getValue()){outstanding--;complete(actions,e,error(q.getKey().leader(),e.request,ResultStatus.UNAVAILABLE));}it.remove();}}
            for(Flight f:flights)if(f.group().view()<view)for(Entry e:f.entries())complete(actions,e,error(f.group().leader(),e.request,ResultStatus.UNAVAILABLE));
            drain(time.monotonicNanos(),actions);
        }run(actions);
    }
    private void expire(long now,List<Runnable> actions){
        Iterator<Map.Entry<Group,ArrayDeque<Entry>>> it=queues.entrySet().iterator();
        while(it.hasNext()){var group=it.next();Iterator<Entry> entries=group.getValue().iterator();
            while(entries.hasNext()){Entry e=entries.next();if(e.remaining(now)==0){entries.remove();outstanding--;timedOut++;complete(actions,e,error(group.getKey().leader(),e.request,ResultStatus.TIMEOUT));}}
            if(group.getValue().isEmpty())it.remove();
        }
        for(Flight f:flights)for(Entry e:f.entries())if(e.remaining(now)==0&&!e.future.isDone()){
            complete(actions,e,error(f.group().leader(),e.request,ResultStatus.TIMEOUT));
        }
    }
    private void drain(long now,List<Runnable> actions){
        if(closed)return;
        while(flights.size()<settings.inFlightLimit()){
            Group selected=null;List<Entry> prefix=null;
            for(var q:queues.entrySet()){
                List<Entry> candidates=new ArrayList<>();long bytes=4;
                for(Entry e:q.getValue()){if(candidates.size()==settings.maxEntries()||bytes+e.bytes()>settings.maxBytes())break;candidates.add(e);bytes+=e.bytes();}
                Entry first=q.getValue().getFirst();
                if(candidates.size()==settings.maxEntries()||bytes==settings.maxBytes()||candidates.size()<q.getValue().size()
                        ||now-first.started>=settings.maxDelayNanos()||candidates.stream().anyMatch(e->e.remaining(now)<=settings.maxDelayNanos())){
                    selected=q.getKey();prefix=candidates;break;
                }
            }
            if(selected==null)break;
            var queue=queues.get(selected);for(Entry e:prefix){queue.removeFirst();long waited=Math.max(0,now-e.started);waitTotal=saturate(waitTotal,waited);waitMax=Math.max(waitMax,waited);}
            if(queue.isEmpty())queues.remove(selected);
            Flight flight=new Flight(selected,List.copyOf(prefix));flights.add(flight);batches++;sentItems+=prefix.size();
            actions.add(()->dispatch(flight));
        }
    }
    private static long saturate(long a,long b){return a>Long.MAX_VALUE-b?Long.MAX_VALUE:a+b;}
    private void dispatch(Flight f){
        long now=time.monotonicNanos();List<IoniaMetaBatchTransport.Item> items=f.entries().stream().map(e->new IoniaMetaBatchTransport.Item(e.token,e.at(now))).toList();
        try { Objects.requireNonNull(raw.metaBatch(f.group().leader(),items),"null batch stage").whenComplete((answers,error)->finished(f,answers,error)); }
        catch(RuntimeException error){finished(f,null,error);}
    }
    private void finished(Flight f,List<IoniaMetaBatchTransport.Answer> answers,Throwable failure){
        // A borrowed implementation may return a mutable list; malformed/null elements fail the whole batch.
        try{if(answers!=null)answers=List.copyOf(answers);}catch(RuntimeException invalid){answers=null;}
        List<Runnable> actions=new ArrayList<>();
        synchronized(lock){
            if(!flights.remove(f))return;
            Map<Long,MetaReply> valid=new HashMap<>();boolean malformedReply=failure==null;
            if(failure==null&&answers!=null&&answers.size()==f.entries().size()){
                Map<Long,Entry> expected=new HashMap<>();for(Entry e:f.entries())expected.put(e.token,e);
                boolean ok=true;
                for(var a:answers){Entry e=a==null?null:expected.get(a.token());
                    if(e==null||valid.containsKey(a.token())||!matches(f.group(),e.request,a.reply())){ok=false;break;}valid.put(a.token(),a.reply());}
                malformedReply=!ok||valid.size()!=expected.size();
            }
            if(malformedReply)malformed++;
            long now=time.monotonicNanos();
            for(Entry e:f.entries()){
                outstanding--;
                ResultStatus status=e.remaining(now)==0?ResultStatus.TIMEOUT:closed||f.group().view()<view||failure!=null||malformedReply?ResultStatus.UNAVAILABLE:null;
                complete(actions,e,status==null?valid.get(e.token):error(f.group().leader(),e.request,status));
            }
            expire(now,actions);drain(now,actions);
        }run(actions);
    }
    private static boolean matches(Group g,Request r,MetaReply reply){return reply!=null&&g.leader().equals(reply.nodeId())&&r.view()==reply.view()
            &&r.clientId().equals(reply.clientId())&&r.operationId().equals(reply.operationId());}
    private static MetaReply error(NodeId node,Request r,ResultStatus status){return new MetaReply(node,r.view(),r.clientId(),r.operationId(),0,Result.of(status,RoutingVersion.ZERO,OwnershipEpoch.ZERO));}
    private static void complete(List<Runnable> actions,Entry e,MetaReply value){actions.add(()->e.future.complete(value));}
    // A synchronous borrowed transport can finish a batch while more dispatch actions are draining.
    // Trampoline avoids recursion proportional to pendingLimit; callbacks still execute outside lock.
    private final ThreadLocal<ArrayDeque<Runnable>> localActions=new ThreadLocal<>();
    private void run(List<Runnable> actions){
        ArrayDeque<Runnable> current=localActions.get();
        if(current!=null){current.addAll(actions);return;}
        current=new ArrayDeque<>(actions);localActions.set(current);
        try{while(!current.isEmpty())current.removeFirst().run();}finally{localActions.remove();}
    }
    public Diagnostics diagnostics(){synchronized(lock){return new Diagnostics(closed,view,queues.values().stream().mapToInt(Collection::size).sum(),outstanding,flights.size(),admitted,busy,batches,sentItems,malformed,timedOut,waitTotal,waitMax,settings.profileId());}}
    @Override public void close(){
        List<Runnable> actions=new ArrayList<>();
        synchronized(lock){if(closed)return;closed=true;
            for(var q:queues.entrySet())for(Entry e:q.getValue()){outstanding--;complete(actions,e,error(q.getKey().leader(),e.request,ResultStatus.UNAVAILABLE));}queues.clear();
            for(Flight f:flights)for(Entry e:f.entries())complete(actions,e,error(f.group().leader(),e.request,ResultStatus.UNAVAILABLE));
        }run(actions);
    }
}
