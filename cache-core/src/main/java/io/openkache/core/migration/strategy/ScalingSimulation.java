package io.openkache.core.migration.strategy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.SplittableRandom;

/** 기존 Python FCFS·준비·모형 전송 backend의 Java 이식. 실제 노드/복사와 별개임을 기록한다. */
public final class ScalingSimulation {
    public static final String GENERATOR="b3-java-splittable-poisson-1";
    public static final List<String> SCENARIOS=List.of("step","spike","periodic","low","hot_range","moving_hotspot","hot_key");
    public static final List<String> CONTROLS=List.of("fixed_single");
    public record Request(double arrival,int key,boolean write){}
    private record Completion(double end,double start,boolean read){}
    private static final class Pending {
        final ScalingPlan plan;final List<Integer> old,added;final int ready,start;final Map<String,Object> event;long remaining;boolean published;
        Pending(ScalingPlan plan,List<Integer> old,List<Integer> added,int ready,int start,long remaining,Map<String,Object> event){this.plan=plan;this.old=List.copyOf(old);this.added=List.copyOf(added);this.ready=ready;this.start=start;this.remaining=remaining;this.event=event;}
    }
    private final ScalingConfig config;private final boolean replica;private List<Integer> active;private List<PartitionUnit> units;
    private final double[] free;private final long[] hints;private final PriorityQueue<Completion> completions=new PriorityQueue<>(Comparator.comparingDouble(Completion::end).thenComparingDouble(Completion::start).thenComparing(Completion::read));
    private Pending pending;private int lastComplete=-100,lastDirection;private double lastP99;private long requests,finished,misses,transferred,planned,reversals,splits,merges;
    private final List<Map<String,Object>> rows=new ArrayList<>(),actions=new ArrayList<>();
    public ScalingSimulation(ScalingConfig config,String method){
        this.config=config;replica=false;free=new double[config.maxNodes()];hints=new long[config.maxNodes()];var initial=new ArrayList<Integer>();
        if(replica){for(int n=0;n<config.maxNodes()&&initial.size()<config.initialNodes();n+=2)initial.add(n);for(int n=0;initial.size()<config.initialNodes();n++)if(!initial.contains(n))initial.add(n);}else initial.addAll(ScalingSupport.nodes(config.initialNodes()));active=List.copyOf(initial);
        var partitions=new ArrayList<PartitionUnit>();for(int i=0;i<config.keys();i+=4){var keys=List.of(i,i+1,i+2,i+3);int group=i/4;if(replica){if(config.maxNodes()<3)throw new IllegalArgumentException("RF3 needs three nodes");partitions.add(new PartitionUnit(keys,List.of(group%config.maxNodes(),(group+1)%config.maxNodes(),(group+2)%config.maxNodes())));}else partitions.add(new PartitionUnit(keys,List.of(group%config.initialNodes())));}units=List.copyOf(partitions);
        ScalingSupport.validate(new ScalingPlan(active,units,"initial",Map.of()),config,replica);
    }
    public boolean pending(){return pending!=null;}
    public int lastComplete(){return lastComplete;}
    public List<Map<String,Object>> rows(){return List.copyOf(rows);}
    public List<Map<String,Object>> actions(){return List.copyOf(actions);}
    public List<Integer> active(){return active;}
    public List<PartitionUnit> units(){return units;}
    /** 모형 window 진행. publish 뒤 새 요청은 새 assignment, 제거 노드의 기존 요청만 drain한다. */
    public void advance(int t){
        if(pending==null||t<pending.ready)return;var p=pending;
        if(!p.published){long remaining=0;if(replica){for(int n:p.added){long sent=Math.min(hints[n],config.transferBytes());hints[n]-=sent;transferred+=sent;remaining+=hints[n];}}
            else{long sent=Math.min(p.remaining,config.transferBytes());p.remaining-=sent;transferred+=sent;remaining=p.remaining;}if(remaining>0)return;
            int previous=units.size();active=p.plan.active();units=p.plan.units();splits+=Math.max(0,units.size()-previous);merges+=Math.max(0,previous-units.size());p.published=true;p.event.put("cutover_window",t);ScalingSupport.validate(p.plan,config,replica);}
        for(int n:p.old)if(!active.contains(n)&&free[n]>t+1e-9)return;
        p.event.put("completed_window",t);p.event.put("delay_windows",t-p.start);lastComplete=t;pending=null;
    }
    public ScalingSnapshot observe(int t,List<Request> events){
        Map<Integer,PartitionUnit> byKey=new HashMap<>();units.forEach(u->u.keys().forEach(k->byKey.put(k,u)));double[] reads=new double[config.keys()],writes=new double[config.keys()];var offered=new HashMap<Integer,Double>();var backlog=new HashMap<Integer,Double>();for(int n:active){offered.put(n,0.);backlog.put(n,Math.max(0,free[n]-t));}
        double penalty=pending!=null&&!pending.published&&t>=pending.ready?config.migrationPenalty():0;long late=0;
        for(var event:events){int key=event.key();if(key<0||key>=config.keys()||event.arrival()<t||event.arrival()>=t+1)throw new IllegalArgumentException("invalid request window");(event.write()?writes:reads)[key]++;
            var unit=byKey.get(key);var available=unit.holders().stream().filter(active::contains).toList();if(available.isEmpty())throw new IllegalStateException("unavailable key");
            List<Integer> destinations;double service;
            if(event.write()){destinations=available;service=config.writeService()/(1-penalty);if(replica)for(int n:unit.holders())if(!active.contains(n))hints[n]+=config.keyBytes();}
            else{destinations=List.of(available.stream().min(Comparator.<Integer>comparingDouble(n->free[n]).thenComparingInt(Integer::intValue)).orElseThrow());service=config.readService()/(1-penalty);}
            double end=Double.POSITIVE_INFINITY;for(int n:destinations){free[n]=Math.max(event.arrival(),free[n])+service;offered.merge(n,service,Double::sum);end=Math.min(end,free[n]);}
            completions.add(new Completion(end,event.arrival(),!event.write()));if(end-event.arrival()>config.slo())late++;
        }
        var latencies=new ArrayList<Double>();int completed=0;while(!completions.isEmpty()&&completions.peek().end()<=t+1){var event=completions.remove();completed++;if(event.read())latencies.add(event.end()-event.start());}
        if(!latencies.isEmpty())lastP99=Numerics.quantile(latencies.stream().mapToDouble(Double::doubleValue).toArray(),.99);requests+=events.size();finished+=completed;misses+=late;
        var cpu=new HashMap<Integer,Double>();active.forEach(n->cpu.put(n,Math.min(1,backlog.get(n)+offered.get(n))));var allocated=new HashSet<>(active);if(pending!=null){allocated.addAll(pending.old);allocated.addAll(pending.plan.active());}
        var row=new LinkedHashMap<String,Object>();row.put("window",t);row.put("requests",events.size());row.put("completed",completed);row.put("deadline_misses",late);row.put("read_p99",lastP99);row.put("p99_read_samples",latencies.size());row.put("ready_nodes",active.size());row.put("allocated_nodes",allocated.size());row.put("active",active);row.put("unit_count",units.size());row.put("cpu_mean",cpu.values().stream().mapToDouble(Double::doubleValue).average().orElseThrow());row.put("max_offered_work",offered.values().stream().mapToDouble(Double::doubleValue).max().orElse(0));row.put("excess_work",offered.values().stream().mapToDouble(v->Math.max(0,v-config.target())).sum());row.put("transferred_bytes",transferred);row.put("pending",pending!=null);rows.add(row);
        String action=actions.isEmpty()?"":actions.getLast().get("action").toString();return new ScalingSnapshot(config,t,active,units,reads,writes,cpu,lastP99,events.size(),completed,hints,lastComplete,action);
    }
    public void accept(ScalingPlan plan,int t){
        if(pending!=null)throw new IllegalStateException("overlapping reconfiguration");ScalingSupport.validate(plan,config,replica);if(plan.active().equals(active)&&plan.units().equals(units))return;
        int direction=Integer.compare(plan.active().size(),active.size());if(direction!=0){if(lastDirection!=0&&direction!=lastDirection)reversals++;lastDirection=direction;}
        var added=plan.active().stream().filter(n->!active.contains(n)).toList();long data=replica?added.stream().mapToLong(n->hints[n]).sum():ScalingSupport.movedBytes(units,plan.units(),config);Map<Integer,List<Integer>> old=new HashMap<>();units.forEach(u->u.keys().forEach(k->old.put(k,u.holders())));long churn=0;for(var u:plan.units())for(int k:u.keys())if(!old.get(k).equals(u.holders()))churn++;
        var event=new LinkedHashMap<String,Object>();event.put("start_window",t);event.put("old_nodes",active);event.put("new_nodes",plan.active());event.put("action",direction>0?"out":direction<0?"in":"rebalance");event.put("reason",plan.reason());event.put("planned_bytes",data);event.put("changed_keys",churn);event.put("old_units",units.size());event.put("new_units",plan.units().size());event.put("detail",plan.detail());event.put("completed_window",null);event.put("assignments",plan.units().stream().map(u->Map.of("keys",u.keys(),"holders",u.holders())).toList());actions.add(event);planned+=data;
        pending=new Pending(plan,active,added,t+(added.isEmpty()?0:config.provisionWindows()),t,data,event);
    }
    public Map<String,Object> metrics(){var out=new LinkedHashMap<String,Object>();out.put("requests",requests);out.put("completed",finished);out.put("outstanding",completions.size());out.put("deadline_miss_rate",(double)misses/Math.max(1,requests));out.put("node_windows",sum("allocated_nodes"));out.put("mean_ready_nodes",rows.stream().mapToDouble(r->number(r,"ready_nodes")).average().orElse(0));out.put("excess_work_area",rows.stream().mapToDouble(r->number(r,"excess_work")).sum());out.put("model_transfer_bytes",transferred);out.put("planned_bytes",planned);
        for(String action:List.of("out","in","rebalance"))out.put(action.equals("out")?"scale_out":action.equals("in")?"scale_in":"rebalance",actions.stream().filter(a->a.get("action").equals(action)).count());out.put("direction_reversals",reversals);out.put("completed_actions",actions.stream().filter(a->a.get("completed_window")!=null).count());out.put("pending_actions",pending==null?0:1);out.put("split_units",splits);out.put("merged_units",merges);out.put("max_units",rows.stream().mapToLong(r->((Number)r.get("unit_count")).longValue()).max().orElse(units.size()));return out;}
    private long sum(String key){return rows.stream().mapToLong(r->((Number)r.get(key)).longValue()).sum();}
    private static double number(Map<String,Object> row,String key){return ((Number)row.get(key)).doubleValue();}
    public static double demand(String scenario,int t,ScalingConfig c){return switch(scenario){case "step"->t<c.windows()/4?180:t<3*c.windows()/4?420:85;case "spike"->t>=c.windows()/3&&t<c.windows()/3+2?600:180;case "periodic"->230+140*Math.sin(2*Math.PI*t/c.period());case "low"->t<c.windows()/4?180:65;case "hot_range","moving_hotspot","hot_key"->210;default->throw new IllegalArgumentException("unknown scenario");};}
    public static List<List<Request>> trace(String scenario,long seed,ScalingConfig c){var random=new SplittableRandom(seed);var trace=new ArrayList<List<Request>>();for(int t=0;t<c.windows();t++){
        double[] probabilities=new double[c.keys()];Arrays.fill(probabilities,1.0/c.keys());if(List.of("hot_range","moving_hotspot","hot_key").contains(scenario)){int hotCount=scenario.equals("hot_key")?1:4,start=scenario.equals("moving_hotspot")?(t/24*8)%c.keys():0;Arrays.fill(probabilities,.30/(c.keys()-hotCount));for(int k=0;k<hotCount;k++)probabilities[(start+k)%c.keys()]=.70/hotCount;}
        int count=ResearchRandom.poisson(demand(scenario,t,c),random);var events=new ArrayList<Request>();double[] arrivals=new double[count];int[] keys=new int[count];for(int i=0;i<count;i++){double p=random.nextDouble(),sum=0;int key=c.keys()-1;for(int k=0;k<c.keys();k++){sum+=probabilities[k];if(p<sum){key=k;break;}}keys[i]=key;arrivals[i]=t+random.nextDouble();}Arrays.sort(arrivals);for(int i=0;i<count;i++)events.add(new Request(Math.min(Math.nextDown(t+1.0),Math.rint(arrivals[i]*1e9)/1e9),keys[i],random.nextDouble()<c.writeRatio()));trace.add(List.copyOf(events));}return List.copyOf(trace);}
    public static List<Double> history(String scenario,long seed,ScalingConfig c){var random=new SplittableRandom(100000+seed);var result=new ArrayList<Double>();for(int t=-c.period()*c.trainingPeriods();t<0;t++)result.add((double)ResearchRandom.poisson(demand(scenario,t,c),random));return List.copyOf(result);}
    public static Map<String,Object> execute(String method,String scenario,long seed,ScalingConfig c,PerformanceModel model){
        var events=trace(scenario,seed,c);var past=history(scenario,seed,c);var cluster=new ScalingSimulation(c,method);ScalingPolicy policy=CONTROLS.contains(method)?null:ScalingPolicies.create(method,model,past);var decisions=new ArrayList<Map<String,Object>>();long planningNanos=0;
        for(int t=0;t<c.windows();t++){cluster.advance(t);var snapshot=cluster.observe(t,events.get(t));if(policy==null)continue;policy.observe(snapshot.total());if((t+1)%c.controlWindows()!=0)continue;
            if(cluster.pending())decisions.add(Map.of("window",t,"reason","action-in-progress"));else if(t-cluster.lastComplete()<=c.cooldownWindows())decisions.add(Map.of("window",t,"reason","cooldown"));else{long start=System.nanoTime();var plan=policy.decide(snapshot);planningNanos+=System.nanoTime()-start;decisions.add(Map.of("window",t,"reason",plan.reason(),"detail",plan.detail()));cluster.accept(plan,t+1);}}
        var row=new LinkedHashMap<String,Object>();row.put("decision_scope","node-count-only");row.put("placement_policy",FixedTabletPlacement.VERSION);row.put("method",method);row.put("scenario",scenario);row.put("seed",seed);row.put("generator",GENERATOR);row.put("workload_hash",B3Json.hash(eventRows(events)));row.put("history_hash",B3Json.hash(past));row.put("metrics",cluster.metrics());row.put("windows",cluster.rows());row.put("decisions",decisions);row.put("actions",cluster.actions());row.put("planningNanos",planningNanos);return row;
    }
    public static List<List<List<Object>>> eventRows(List<List<Request>> trace){return trace.stream().map(window->window.stream().map(r->List.<Object>of(r.arrival(),r.key(),r.write())).toList()).toList();}
}
