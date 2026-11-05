package io.openkache.core.migration.strategy;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** load-slice centroid 보상·결정론적 상태 전이의 Q backup 연구 변형. */
public final class TiramolaPolicy implements NodeCountPolicy {
    private final List<double[]> data;private final double[][] q;private long updates;
    public TiramolaPolicy(PerformanceModel model){data=new ArrayList<>(model.rewardData());q=new double[model.config().maxNodes()+1][3];if(data.isEmpty())throw new IllegalArgumentException("reward training data required");}
    @Override public String name(){return "tiramola";}
    private double reward(int nodes,double total,double slo){
        var candidates=data.stream().filter(r->r[0]==nodes&&Math.abs(r[1]-total)<=Math.max(20,total*.2)).toList();
        if(candidates.isEmpty())candidates=data.stream().filter(r->r[0]==nodes).sorted(Comparator.comparingDouble(r->Math.abs(r[1]-total))).limit(3).toList();
        if(candidates.isEmpty())throw new IllegalArgumentException("no reward data for node count");double[][] points=candidates.stream().map(r->new double[]{r[1],r[2],r[3]}).toArray(double[][]::new);int[] labels=points.length>=4?Numerics.twoMeans(points):new int[points.length];
        int[] counts=new int[2];for(int label:labels)counts[label]++;int largest=counts[1]>counts[0]?1:0;double latency=0,throughput=0;for(int i=0;i<points.length;i++)if(labels[i]==largest){latency+=points[i][1]/counts[largest];throughput+=points[i][2]/counts[largest];}
        return throughput/100-.30*nodes-25*Math.max(0,latency/slo-1);
    }
    @Override public Decision decideCount(ScalingSnapshot s){
        var c=s.config();data.add(new double[]{s.active().size(),s.total(),s.p99(),s.completed()});var rewards=new LinkedHashMap<Integer,Double>();for(int n=c.minNodes();n<=c.maxNodes();n++)rewards.put(n,reward(n,s.total(),c.slo()));
        for(int it=0;it<20;it++){double[][] old=java.util.Arrays.stream(q).map(double[]::clone).toArray(double[][]::new);for(int n:rewards.keySet())for(int a=0;a<3;a++){int next=n+a-1;if(!rewards.containsKey(next)){q[n][a]=-1e6;continue;}double target=rewards.get(next)-.05*Math.abs(a-1)+.8*java.util.Arrays.stream(old[next]).max().orElseThrow();q[n][a]+=.30*(target-q[n][a]);updates++;}}
        int n=s.active().size(),action=1;for(int a:new int[]{0,2})if(q[n][a]>q[n][action])action=a;
        var rewardRows=rewards.entrySet().stream().map(e->Map.<String,Object>of("nodes",e.getKey(),"reward",e.getValue())).toList();
        return NodeCountPolicy.count(s,n+action-1,"load-slice-Q-backup",Map.of("q_values",java.util.Arrays.stream(q[n]).boxed().toList(),"rewards",rewardRows,"updates",updates,"alpha",.30,"gamma",.8,"transition_penalty",.05));
    }
    public long updates(){return updates;}
}
