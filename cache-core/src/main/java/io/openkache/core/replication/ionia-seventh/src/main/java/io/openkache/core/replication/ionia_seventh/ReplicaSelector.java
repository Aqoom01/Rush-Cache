package io.openkache.core.replication.ionia_seventh;

import io.openkache.core.api.ResultStatus;
import io.openkache.core.model.*;
import io.openkache.core.replication.ionia.IoniaClientTransport;
import java.util.*;

/** Client의 제어 lock 아래서만 접근하는 순수 선택/관측 상태. */
final class ReplicaSelector {
    static final class State {
        int pending;long starts,terminal,success,failure,ignoredGeneration,ignoredSequence,selections;
        long ewma,at,cooldownUntil,feedbackSeq;boolean sampled,cooling;
    }
    static final class Ticket {
        final NodeId node;final long generation,sequence,started;final IoniaClientTransport.Request request;boolean done;
        Ticket(NodeId n,long g,long seq,long start,IoniaClientTransport.Request r){node=n;generation=g;sequence=seq;started=start;request=r;}
    }
    record Choice(NodeId node,String reason) { }
    final List<NodeId> nodes;final Map<NodeId,State> states=new LinkedHashMap<>();final IoniaReadSelectionSettings settings;
    final int seed;int cursor,exploreCursor;long generation,nextSequence,admitted;
    ReplicaSelector(List<NodeId> nodes,ClientId id,IoniaReadSelectionSettings settings){
        this.nodes=List.copyOf(nodes);this.settings=settings;for(NodeId n:nodes)states.put(n,new State());
        seed=Math.floorMod(Long.hashCode(id.value()),nodes.size());cursor=exploreCursor=seed;
    }
    private boolean eligible(NodeId n,NodeId leader,long now){State s=states.get(n);return !n.equals(leader)&&s.pending<settings.rpcLimitPerReplica()&&(!s.cooling||now-s.cooldownUntil>=0);}
    boolean fresh(State s,long now){long age=now-s.at;return s.sampled&&age>=0&&age<=settings.sampleTtlNanos();}
    long score(State s,long now){return saturatingMultiply(fresh(s,now)?s.ewma:settings.initialRttNanos(),1L+s.pending);}
    static long saturatingMultiply(long a,long b){return a>Long.MAX_VALUE/b?Long.MAX_VALUE:a*b;}
    Choice choose(NodeId leader,long now){
        List<NodeId> candidates=nodes.stream().filter(n->eligible(n,leader,now)).toList();
        if(candidates.isEmpty())return null;
        long ordinal=Math.incrementExact(admitted);boolean explore=settings.mode()==IoniaReadSelectionSettings.Mode.ADAPTIVE&&ordinal%settings.exploreEvery()==0;
        List<NodeId> best=candidates;String reason="ROUND_ROBIN";int start=cursor;
        if(explore){reason="EXPLORE";start=exploreCursor;}
        else if(settings.mode()==IoniaReadSelectionSettings.Mode.ADAPTIVE){
            reason="SCORE";long minimum=candidates.stream().mapToLong(n->score(states.get(n),now)).min().orElseThrow();
            best=candidates.stream().filter(n->score(states.get(n),now)==minimum).toList();
        }
        for(int i=0;i<nodes.size();i++){
            int pos=(start+i)%nodes.size();NodeId selected=nodes.get(pos);
            if(best.contains(selected)){admitted=ordinal;cursor=(pos+1)%nodes.size();if(explore)exploreCursor=cursor;states.get(selected).selections++;return new Choice(selected,reason);}
        }
        throw new AssertionError("eligible candidate disappeared");
    }
    Ticket begin(NodeId node,IoniaClientTransport.Request request,long now){
        State s=Objects.requireNonNull(states.get(node));if(s.pending>=settings.rpcLimitPerReplica())throw new IllegalStateException("raw READ capacity");
        long sequence=Math.incrementExact(nextSequence);nextSequence=sequence;s.pending++;s.starts++;
        return new Ticket(node,generation,sequence,now,request);
    }
    boolean finish(Ticket t,IoniaClientTransport.ReadReply reply,Throwable error,long now){
        if(t.done)return false;t.done=true;State s=states.get(t.node);s.pending--;s.terminal++;
        boolean ok=error==null&&reply!=null&&reply.nodeId().equals(t.node)&&reply.view()==t.request.view()
                &&reply.clientId().equals(t.request.clientId())&&reply.operationId().equals(t.request.operationId())
                &&(reply.result().status()==ResultStatus.OK||reply.result().status()==ResultStatus.MISS);
        if(ok)s.success++;else s.failure++;
        if(t.generation!=generation){s.ignoredGeneration++;return true;}
        if(t.sequence<=s.feedbackSeq){s.ignoredSequence++;return true;}s.feedbackSeq=t.sequence;
        if(!ok){s.cooling=true;s.cooldownUntil=now+settings.cooldownNanos();return true;}
        long sample=Math.max(1,Math.min(IoniaReadSelectionSettings.SAMPLE_MAX_NANOS,now-t.started));
        s.ewma=s.sampled?s.ewma+(sample-s.ewma)/8:sample;s.sampled=true;s.at=now;s.cooling=false;return true;
    }
    void newView(){
        generation=Math.incrementExact(generation);admitted=0;cursor=exploreCursor=seed;
        for(State s:states.values()){s.ewma=s.at=s.cooldownUntil=s.feedbackSeq=0;s.sampled=s.cooling=false;}
    }
}
