package io.openkache.core.migration.strategy;

import static org.junit.jupiter.api.Assertions.*;
import io.openkache.core.migration.MigrationScheduler;
import io.openkache.core.model.NodeId;
import io.openkache.core.model.TabletId;
import io.openkache.core.tablet.Change;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class B3WorkflowTest {
    static final ScalingConfig CONFIG=ScalingConfig.defaults();static PerformanceModel model;
    @BeforeAll static void calibration(){model=PerformanceModel.calibrate(CONFIG);}
    static final class Environment implements B3Workflow.Environment {
        int window=3,drivers,applied;double cpu=.1;boolean ready=true,metadata,fail;List<Integer> active=ScalingSupport.nodes(4);final Map<Integer,Integer> owners=new HashMap<>();final Set<Integer> prepared=new HashSet<>(),removed=new HashSet<>();final List<String> copies=new ArrayList<>();ScalingPlan target;
        Environment(){for(int i=0;i<8;i++)owners.put(i,i%4);}
        @Override public StrategySelection.Capabilities capabilities(){return new StrategySelection.Capabilities(false,false,false);}
        List<PartitionUnit> units(){var out=new ArrayList<PartitionUnit>();for(int i=0;i<8;i++)out.add(new PartitionUnit(List.of(i*4,i*4+1,i*4+2,i*4+3),List.of(owners.get(i))));return out;}
        @Override public ScalingSnapshot observe(){var cpus=new HashMap<Integer,Double>();active.forEach(n->cpus.put(n,cpu));double[] reads=new double[32];java.util.Arrays.fill(reads,3);return new ScalingSnapshot(CONFIG,window++,active,units(),reads,new double[32],cpus,.01,96,96,new long[8],-100,"");}
        @Override public void prepare(int node){prepared.add(node);}
        @Override public boolean ready(int node){return active.contains(node)||ready;}
        @Override public List<Change> changes(ScalingPlan plan){target=plan;var out=new ArrayList<Change>();for(var u:plan.units()){int id=u.keys().getFirst()/4,from=owners.get(id),to=u.holders().getFirst();if(from!=to)out.add(new Change(new TabletId(id),new NodeId(from),new NodeId(to)));}return out;}
        @Override public DrainStrategy.Snapshot admission(List<Change> jobs,List<Integer> removedNodes){var sizes=new HashMap<TabletId,Long>();var free=new HashMap<NodeId,Long>();for(var job:jobs){sizes.put(job.unit(),128L);free.put(job.to(),1_000_000L);}return new DrainStrategy.Snapshot(jobs,sizes,free,Set.of(),Set.of(),true,target.active().size(),1,metadata);}
        @Override public MigrationScheduler.Driver driver(Change change,String copy,CopyDrivers.Settings settings){drivers++;copies.add(copy);return new MigrationScheduler.Driver(){long sent;boolean done;
            @Override public void begin(){}
            @Override public void step(long budget){if(fail)return;sent+=Math.min(128-sent,budget);if(sent==128){done=true;owners.put((int)change.unit().value(),(int)change.to().value());}}
            @Override public long sentBytes(){return sent;}
            @Override public boolean done(){return done;}
            @Override public boolean failed(){return fail;}
            @Override public String state(){return done?"DONE":fail?"FAILED":"COPYING";}
            @Override public void stop(){}
        };}
        @Override public long estimatedBytes(Change change){return 128;}
        @Override public double dirtyBytes(Change change){return 0;}
        @Override public long deadline(Change change){return 10000;}
        @Override public B3Workflow.DrainStatus drainStatus(int node){int remaining=(int)owners.values().stream().filter(n->n==node).count();return new B3Workflow.DrainStatus(remaining,0,metadata);}
        @Override public void remove(int node){assertEquals(0,drainStatus(node).remainingUnits());assertTrue(metadata);removed.add(node);}
        @Override public void applied(ScalingPlan plan){applied++;active=plan.active();}
    }
    static B3Workflow workflow(Environment env,String copy,String scheduler){return new B3Workflow(new StrategySelection("aws-warmup",FixedTabletPlacement.NAME,scheduler,copy,"preflight-all"),model,List.of(),env,new MigrationScheduler.Settings(4,4,4096,1024,100));}
    @Test void settingsDriveWholeCompositionAndRemovalWaitsForMetadata(){for(String copy:CopyDrivers.METHODS){var env=new Environment();var flow=workflow(env,copy,"mongodb");for(int i=0;i<100&&flow.phase()!=B3Workflow.Phase.DRAIN;i++)flow.step();assertEquals(B3Workflow.Phase.DRAIN,flow.phase());assertTrue(env.drivers>0);assertTrue(env.copies.stream().allMatch(copy::equals));for(int i=0;i<3;i++)flow.step();assertTrue(env.removed.isEmpty());env.metadata=true;flow.step();flow.step();assertEquals(B3Workflow.Phase.STABLE,flow.phase());assertEquals(Set.of(2,3),env.removed);assertEquals(1,env.applied);}}
    @Test void unreadyNodesNeverReceiveMigrationWork(){var env=new Environment();env.cpu=1;env.ready=false;env.metadata=true;var flow=workflow(env,"pre-copy","fifo");flow.step();assertFalse(env.prepared.isEmpty());for(int i=0;i<4;i++)flow.step();assertEquals(B3Workflow.Phase.PREPARE,flow.phase());assertEquals(0,env.drivers);env.ready=true;for(int i=0;i<100&&flow.phase()!=B3Workflow.Phase.STABLE;i++)flow.step();assertEquals(B3Workflow.Phase.STABLE,flow.phase());assertTrue(env.drivers>0);}
    @Test void failedJobCannotTriggerRemovalOrApply(){var env=new Environment();env.fail=true;env.metadata=true;var flow=workflow(env,"post-copy","fifo");for(int i=0;i<100&&flow.phase()!=B3Workflow.Phase.SAFE_HOLD;i++)flow.step();assertEquals(B3Workflow.Phase.SAFE_HOLD,flow.phase());assertTrue(env.removed.isEmpty());assertEquals(0,env.applied);}
    @Test void preflightAndIncrementalAreSeparateModules(){var a=new Change(new TabletId(0),new NodeId(0),new NodeId(1));var b=new Change(new TabletId(1),new NodeId(0),new NodeId(2));var state=new DrainStrategy.Snapshot(List.of(a,b),Map.of(a.unit(),100L,b.unit(),100L),Map.of(a.to(),100L,b.to(),0L),Set.of(),Set.of(),true,2,1,true);assertTrue(DrainStrategies.create("preflight-all").admit(state).admitted().isEmpty());assertEquals(List.of(a),DrainStrategies.create("incremental-drain").admit(state).admitted());}
    @Test void planningMatrixUsesRegistryAndContainsOnlyThreeNonExecutingFlows(){var rows=RealNodePlan.configurations();assertEquals(1134,rows.size());assertEquals(Set.of("N","U","R"),rows.stream().map(r->r.get("flow")).collect(java.util.stream.Collectors.toSet()));for(var row:rows){assertDoesNotThrow(()->new StrategySelection(row.get("scaler").toString(),row.get("planner").toString(),row.get("scheduler").toString(),row.get("copy").toString(),row.get("drain").toString()));assertEquals(String.join("|",RealNodePlan.PHASES),row.get("phases"));}assertEquals(0,RealNodePlan.summary().get("experimentsStarted"));}
}
