package io.openkache.core.migration.strategy;

import io.openkache.core.migration.MigrationScheduler;
import io.openkache.core.migration.experiment.SchedulerAlgorithms;
import io.openkache.core.tablet.Change;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** 자체 관측·노드·저장·전송 port로 전략들을 연결하는 B3 상태 기계. 한 controller thread 전용. */
public final class B3Workflow {
    public enum Phase { OBSERVE, PREPARE, PLAN, TRANSFER, DRAIN, REMOVE, STABLE, SAFE_HOLD }
    public record DrainStatus(int remainingUnits,int unfinishedJobs,boolean metadataCleared){public DrainStatus{if(remainingUnits<0||unfinishedJobs<0)throw new IllegalArgumentException("invalid drain counters");}}
    /** 실제 서비스 또는 자체 fixture adapter. 생성자는 어떤 port도 실행하지 않는다. */
    public interface Environment {
        StrategySelection.Capabilities capabilities();
        ScalingSnapshot observe();
        void prepare(int node);
        boolean ready(int node);
        /** 현재 실제 owner 기준으로 아직 필요한 이동만 반환한다. 이미 publish된 작업의 정리는 drain에 포함한다. */
        List<Change> changes(ScalingPlan desired);
        DrainStrategy.Snapshot admission(List<Change> jobs,List<Integer> removedNodes);
        MigrationScheduler.Driver driver(Change change,String copy,CopyDrivers.Settings settings);
        long estimatedBytes(Change change);
        double dirtyBytes(Change change);
        long deadline(Change change);
        DrainStatus drainStatus(int node);
        void remove(int node);
        /** 모든 이동·정리·제거 완료 뒤 목표 active/unit 상태를 반영한다. */
        void applied(ScalingPlan plan);
    }
    public record Event(long sequence,int window,Phase phase,String reason){}
    private final StrategySelection selection;private final ScalingPolicy policy;private final PlacementPlanner planner;
    private final DrainStrategy drain;private final Environment environment;private final MigrationScheduler.Settings settings;
    private final CopyDrivers.Settings copySettings;
    private final List<Event> events=new ArrayList<>();private Phase phase=Phase.OBSERVE;private ScalingSnapshot before;
    private ScalingPlan desired;private List<Integer> removed=List.of();private MigrationScheduler scheduler;
    private List<Change> admitted=List.of();private int lastWindow=-1;private String reason="initial";
    private int lastAppliedWindow=-100;private String lastAppliedAction="";
    private boolean planFinalized;
    private boolean resumeTransfer;private RuntimeException lastFailure;
    public B3Workflow(StrategySelection selection,PerformanceModel model,List<Double> past,Environment environment,MigrationScheduler.Settings settings){
        this(selection,model,past,environment,settings,new CopyDrivers.Settings(4096,1024,10));
    }
    public B3Workflow(B3Configuration configuration,PerformanceModel model,List<Double> past,Environment environment){
        this(configuration.selection(),model,past,environment,configuration.scheduler(),configuration.copy());
    }
    private B3Workflow(StrategySelection selection,PerformanceModel model,List<Double> past,Environment environment,MigrationScheduler.Settings settings,CopyDrivers.Settings copySettings){
        this.selection=Objects.requireNonNull(selection);this.environment=Objects.requireNonNull(environment);this.settings=Objects.requireNonNull(settings);
        this.copySettings=Objects.requireNonNull(copySettings);
        selection.require(environment.capabilities());policy=ScalingPolicies.create(selection.scaler(),model,past);planner=PlacementPlanners.create(selection.planner(),model);drain=DrainStrategies.create(selection.drain());
    }
    public Phase phase(){return phase;}
    public String reason(){return reason;}
    public List<Event> events(){return List.copyOf(events);}
    public StrategySelection selection(){return selection;}
    public java.util.Optional<RuntimeException> lastFailure(){return java.util.Optional.ofNullable(lastFailure);}
    private void state(Phase next,int window,String detail){phase=next;reason=detail;events.add(new Event(events.size(),window,next,detail));}
    /** 한 번의 제어 step. 실제 시계·sleep·외부 프로세스를 사용하지 않는다. */
    public Phase step(){
        var observed=environment.observe();var snapshot=observed;
        if(lastAppliedWindow>observed.lastCompletedWindow())snapshot=new ScalingSnapshot(observed.config(),observed.window(),observed.active(),observed.units(),observed.reads(),observed.writes(),observed.cpu(),observed.p99(),observed.total(),observed.completed(),observed.hints(),lastAppliedWindow,lastAppliedAction);
        if(snapshot.window()<lastWindow)throw new IllegalStateException("window moved backwards");
        if(snapshot.window()!=lastWindow){policy.observe(snapshot.total());lastWindow=snapshot.window();}
        try{
            switch(phase){
                case OBSERVE->{
                    if((snapshot.window()+1)%snapshot.config().controlWindows()!=0||snapshot.window()-snapshot.lastCompletedWindow()<=snapshot.config().cooldownWindows())return phase;
                    before=snapshot;desired=policy.decide(snapshot);ScalingSupport.validate(desired,snapshot.config(),environment.capabilities().replicas());
                    if(desired.active().equals(snapshot.active())&&desired.units().equals(snapshot.units())){state(Phase.STABLE,snapshot.window(),"keep:"+desired.reason());break;}
                    for(int node:desired.active())if(!snapshot.active().contains(node))environment.prepare(node);
                    state(Phase.PREPARE,snapshot.window(),desired.reason());
                }
                case PREPARE->{if(desired.active().stream().allMatch(environment::ready))state(Phase.PLAN,snapshot.window(),"nodes-ready");}
                case PLAN->{
                    if(!planFinalized){var planned=planner.plan(before,desired);if(!planned.active().equals(desired.active()))throw new PlacementPlanners.PlanningRejected("planner changed prepared node set");desired=planned;ScalingSupport.validate(desired,before.config(),environment.capabilities().replicas());planFinalized=true;}
                    removed=before.active().stream().filter(n->!desired.active().contains(n)).toList();var jobs=environment.changes(desired);
                    var decision=drain.admit(environment.admission(jobs,removed));admitted=decision.admitted();
                    if(admitted.isEmpty()&&!jobs.isEmpty()){state(Phase.SAFE_HOLD,snapshot.window(),decision.reason());break;}
                    var tasks=new ArrayList<SchedulerAlgorithms.Task>();for(int i=0;i<admitted.size();i++){var job=admitted.get(i);tasks.add(new SchedulerAlgorithms.Task(i,job,environment.estimatedBytes(job),environment.dirtyBytes(job),environment.deadline(job)));}
                    var algorithm=new SchedulerAlgorithms(selection.scheduler(),tasks,settings,i->Math.max(0,environment.estimatedBytes(admitted.get(i))-sent(i)),i->environment.dirtyBytes(admitted.get(i)));
                    scheduler=new MigrationScheduler(admitted,settings,change->environment.driver(change,selection.copy(),copySettings),algorithm);state(Phase.TRANSFER,snapshot.window(),decision.reason());
                }
                case TRANSFER->{scheduler.step();if(scheduler.finished()){
                    if(scheduler.jobs().stream().anyMatch(j->j.status()!=MigrationScheduler.Status.DONE)){state(Phase.SAFE_HOLD,snapshot.window(),"failed-job-no-owner-rollback");break;}
                    if(!environment.changes(desired).isEmpty()){state(Phase.SAFE_HOLD,snapshot.window(),"partial-drain-jobs-remain");break;}
                    state(Phase.DRAIN,snapshot.window(),"migration-complete");}}
                case DRAIN->{boolean clear=true;for(int node:removed){var status=environment.drainStatus(node);clear&=drain.canRemove(status.remainingUnits(),status.unfinishedJobs(),status.metadataCleared());}if(clear)state(Phase.REMOVE,snapshot.window(),"drain-clear");}
                case REMOVE->{for(int node:removed){var status=environment.drainStatus(node);if(!drain.canRemove(status.remainingUnits(),status.unfinishedJobs(),status.metadataCleared())){state(Phase.SAFE_HOLD,snapshot.window(),"drain-changed-before-remove");return phase;}}
                    for(int node:removed)environment.remove(node);environment.applied(desired);lastAppliedWindow=snapshot.window();lastAppliedAction=desired.active().size()>before.active().size()?"out":desired.active().size()<before.active().size()?"in":"rebalance";state(Phase.STABLE,snapshot.window(),"applied");}
                case STABLE,SAFE_HOLD->{ /* 다음 cycle·복구는 caller가 명시적으로 요청한다. */ }
            }
        }catch(PlacementPlanners.PlanningRejected rejected){resumeTransfer=false;lastFailure=rejected;state(Phase.SAFE_HOLD,snapshot.window(),rejected.getMessage());}
        catch(RuntimeException failure){resumeTransfer=phase==Phase.TRANSFER&&scheduler!=null&&!scheduler.finished();lastFailure=failure;state(Phase.SAFE_HOLD,snapshot.window(),"execution-error:"+failure.getClass().getSimpleName());}
        return phase;
    }
    private long sent(int id){return scheduler==null?0:scheduler.jobs().get(id).sentBytes();}
    /** 실제 복구·용량 해제 뒤 미완료 계획을 다시 검사한다. 소유권이나 source 데이터를 되돌리지 않는다. */
    public void resume(){if(phase!=Phase.SAFE_HOLD)throw new IllegalStateException("not held");state(resumeTransfer?Phase.TRANSFER:Phase.PLAN,lastWindow,"external-recovery-recheck");resumeTransfer=false;}
    public void nextCycle(){if(phase!=Phase.STABLE)throw new IllegalStateException("not stable");before=null;desired=null;scheduler=null;planFinalized=false;resumeTransfer=false;lastFailure=null;state(Phase.OBSERVE,lastWindow,"next-cycle");}
}
