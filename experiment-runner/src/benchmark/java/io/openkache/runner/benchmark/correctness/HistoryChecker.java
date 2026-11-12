package io.openkache.runner.benchmark.correctness;

import io.openkache.core.replication.ionia.IoniaCommands;
import io.openkache.core.api.*;
import io.openkache.core.model.Bytes;
import io.openkache.runner.benchmark.client.ClientOutcome;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * 단일 monotonic clock의 GET/SET/DELETE 전수 이력을 제한된 전역 탐색으로 검사한다.
 * 실제 Bytes로 비교하며 실패/미완료 쓰기는 생략 또는 미확정 완료 확장을 허용한다.
 * 시간/상태/크기 한도와 불확실한 결과를 PASS로 변환하지 않는다. 성능용 무제한 checker가 아니다.
 */
public final class HistoryChecker {
    private HistoryChecker() {}
    public enum Verdict { PASS, VIOLATION, INCONCLUSIVE }
    public record Limits(int maxOperations,long maxStates,long maxNanos) {
        public Limits {if(maxOperations<1||maxOperations>24||maxStates<1||maxStates>10_000_000
                ||maxNanos<1||maxNanos>TimeUnit.MINUTES.toNanos(1)){throw new IllegalArgumentException("checker limits");}}
    }
    public record Result(String historyToken,Verdict verdict,String reason,long explored,
            int requiredOperations,int uncertainOperations,int omittedAdmissions,List<Integer> witness) {
        public Result {witness=List.copyOf(witness);}
    }
    private record Op(int historyIndex,HistoryRecorder.Operation operation,boolean required) {}
    private record State(long used,Map<Bytes,Bytes> values) {State{values=Map.copyOf(values);}}
    public static Result check(HistoryRecorder.Snapshot history,Map<Bytes,Bytes> knownInitialState,Limits limits){
        Objects.requireNonNull(history);Objects.requireNonNull(limits);
        if(knownInitialState==null){return result(history,Verdict.INCONCLUSIVE,"INITIAL_STATE_NOT_ATTESTED",0,0,0,0,List.of());}
        if(!history.failures().isEmpty()){return result(history,Verdict.INCONCLUSIVE,"INVALID_OR_INCOMPLETE_COLLECTION",0,0,0,0,List.of());}
        String invalid=validate(history);if(invalid!=null){return result(history,Verdict.INCONCLUSIVE,invalid,0,0,0,0,List.of());}
        List<Op> operations=new ArrayList<>();int required=0,uncertain=0,notAdmitted=0;
        for(int i=0;i<history.operations().size();i++){
            var op=history.operations().get(i);
            if(op.definitelyNotAdmitted()){notAdmitted++;continue;}
            boolean success=op.outcome()!=null&&op.outcome().category()==ClientOutcome.Category.SUCCESS;
            if(success){required++;operations.add(new Op(i,op,true));}
            else{uncertain++;if(!(op.request().command() instanceof Command.Get)){operations.add(new Op(i,op,false));}}
        }
        if(history.operations().isEmpty()){return result(history,Verdict.INCONCLUSIVE,"NO_OPERATIONS",0,0,0,0,List.of());}
        if(required==0){return result(history,Verdict.INCONCLUSIVE,"NO_SUCCESSFUL_RESULTS_TO_CHECK",0,0,uncertain,notAdmitted,List.of());}
        if(operations.size()>limits.maxOperations()){return result(history,Verdict.INCONCLUSIVE,"OPERATION_LIMIT",0,required,uncertain,notAdmitted,List.of());}
        Map<Bytes,Bytes> relevantInitial=new HashMap<>();for(var op:operations){var key=IoniaCommands.key(op.operation().request().command());if(knownInitialState.containsKey(key)){relevantInitial.put(key,knownInitialState.get(key));}}
        Search search=new Search(operations,limits);boolean found=search.run(0,relevantInitial,new ArrayList<>());
        if(search.cutoff!=null){return result(history,Verdict.INCONCLUSIVE,search.cutoff,search.explored,required,uncertain,notAdmitted,List.of());}
        if(!found){return result(history,Verdict.VIOLATION,"NO_LEGAL_COMPLETION_EXTENSION",search.explored,required,uncertain,notAdmitted,List.of());}
        boolean unresolved=uncertain>0||history.pendingCalls()>0;
        return result(history,unresolved?Verdict.INCONCLUSIVE:Verdict.PASS,
                unresolved?"LEGAL_EXTENSION_WITH_UNCERTAIN_OPERATIONS":"BOUNDED_HISTORY_HAS_LEGAL_LINEARIZATION",search.explored,required,uncertain,notAdmitted,search.witness);
    }
    private static Result result(HistoryRecorder.Snapshot h,Verdict v,String why,long explored,int required,int uncertain,int rejected,List<Integer> witness){return new Result(h.token(),v,why,explored,required,uncertain,rejected,witness);}
    private static String validate(HistoryRecorder.Snapshot h){
        Set<Object> ids=new HashSet<>();Set<Long> calls=new HashSet<>();long pending=0;
        for(var op:h.operations()){
            if(op.request()==null||op.calls().isEmpty()){return "EMPTY_OR_INVALID_OPERATION";}
            var id=op.request().id(h.runId(),h.loadSlot());if(!ids.add(id)){return "DUPLICATE_LOGICAL_OPERATION";}ClientOutcome first=null;
            for(var call:op.calls()){
                if(!calls.add(call.submission())||call.submission()<1||call.kind()==null){return "INVALID_SUBMISSION_SEQUENCE";}
                if(call.kind()==HistoryRecorder.Kind.PENDING){pending++;if(call.observedNanos()!=null){return "PENDING_HAS_TERMINAL";}}
                else if(call.observedNanos()==null||call.observedNanos()-call.invokedNanos()<0){return "INVALID_TERMINAL_CLOCK";}
                if(h.sealedNanos()-(call.observedNanos()==null?call.invokedNanos():call.observedNanos())<0){return "SEAL_PRECEDES_OBSERVATION";}
                var o=call.outcome();
                if(call.kind()==HistoryRecorder.Kind.INVALID){return "INVALID_OPERATION_OBSERVATION";}
                if(call.kind()==HistoryRecorder.Kind.OUTCOME){
                    if(o==null||!o.id().equals(id)||o.operation()!=ClientOutcome.Operation.of(op.request().command())
                            ||o.invokedNanos()-op.calls().getFirst().invokedNanos()<0||o.decidedNanos()-o.invokedNanos()<0
                            ||o.terminalNanos()-o.decidedNanos()<0||call.observedNanos()-o.terminalNanos()<0){return "INVALID_OUTCOME_IDENTITY_OR_CLOCK";}
                    if(first!=null&&!first.equals(o)){return "CONFLICTING_LOGICAL_OUTCOME";}first=o;
                }else if(o!=null){return "OUTCOME_KIND_MISMATCH";}
            }
        }
        return calls.size()!=h.submissions()||pending!=h.pendingCalls()?"HISTORY_COUNT_MISMATCH":null;
    }
    private static final class Search {
        final List<Op> ops;final Limits limits;final long started=System.nanoTime();final long requiredMask;
        final long[] predecessors;final Set<State> visited=new HashSet<>();long explored;String cutoff;List<Integer> witness=List.of();
        Search(List<Op> ops,Limits limits){
            this.ops=ops;this.limits=limits;long required=0;predecessors=new long[ops.size()];
            for(int i=0;i<ops.size();i++){
                if(ops.get(i).required()){required|=1L<<i;}
                for(int j=0;j<ops.size();j++){
                    if(i!=j&&ops.get(j).required()&&ops.get(i).operation().invokedNanos()-ops.get(j).operation().outcome().terminalNanos()>0){predecessors[i]|=1L<<j;}
                }
            }requiredMask=required;
        }
        boolean run(long used,Map<Bytes,Bytes> state,List<Integer> order){
            if(Thread.currentThread().isInterrupted()){cutoff="CHECKER_INTERRUPTED";return false;}
            if(explored>=limits.maxStates()){cutoff="STATE_LIMIT";return false;}
            if(System.nanoTime()-started>=limits.maxNanos()){cutoff="TIME_LIMIT";return false;}
            explored++;
            if((used&requiredMask)==requiredMask){witness=List.copyOf(order);return true;}
            if(!visited.add(new State(used,state))){return false;}
            for(int i=0;i<ops.size();i++){
                long bit=1L<<i;if((used&bit)!=0||(predecessors[i]&~used)!=0){continue;}
                var op=ops.get(i);Map<Bytes,Bytes> next=new HashMap<>(state);var command=op.operation().request().command();Bytes old=state.get(IoniaCommands.key(command));
                boolean valid;
                if(!op.required()){
                    if(command instanceof Command.Set set){next.put(set.key(),set.value());}else{next.remove(IoniaCommands.key(command));}valid=true;
                }else{
                    var result=op.operation().outcome().result();
                    if(result==null){continue;}
                    if(command instanceof Command.Get){valid=old==null?result.status()==ResultStatus.MISS&&result.value().isEmpty()
                            :result.status()==ResultStatus.OK&&result.value().equals(Optional.of(old));}
                    else if(command instanceof Command.Set set){valid=result.status()==ResultStatus.OK&&result.value().isEmpty();next.put(set.key(),set.value());}
                    else{valid=result.status()==(old==null?ResultStatus.MISS:ResultStatus.OK)&&result.value().isEmpty();next.remove(IoniaCommands.key(command));}
                }
                if(valid){order.add(op.historyIndex());if(run(used|bit,next,order)){return true;}order.removeLast();if(cutoff!=null){return false;}}
            }return false;
        }
    }
}
