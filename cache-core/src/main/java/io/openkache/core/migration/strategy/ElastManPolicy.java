package io.openkache.core.migration.strategy;

import java.util.Map;

/** logistic feedforward + PI/deadband. controller thread 전용이며 실제 시스템 재현은 아니다. */
public final class ElastManPolicy implements NodeCountPolicy {
    private final PerformanceModel model;private Double previous,filtered;private double u,integral;
    public ElastManPolicy(PerformanceModel model){this.model=model;u=model.capacity();}
    @Override public String name(){return "elastman";}
    @Override public Decision decideCount(ScalingSnapshot s){
        var c=s.config();filtered=filtered==null?s.p99():.5*filtered+.5*s.p99();double error=(c.slo()-filtered)/c.slo();
        boolean spike=previous==null||Math.abs(s.total()-previous)>Math.max(1,previous)*.2;previous=s.total();String reason;
        if(spike){double ratio=java.util.Arrays.stream(s.writes()).sum()/Math.max(1,s.total());u=model.limit(ratio)*.95;reason="feedforward-logistic";}
        else if(Math.abs(error)<=.1)return NodeCountPolicy.count(s,s.active().size(),"PI-deadband",Map.of("filtered_p99",filtered));
        else{integral=ScalingSupport.clip(integral+error,-5,5);u=ScalingSupport.clip(u+8*error+integral,20,95);reason="feedback-PI";}
        return NodeCountPolicy.count(s,(int)Math.ceil(s.total()/u),reason,Map.of("target_throughput",u,"filtered_p99",filtered,"integral",integral));
    }
}
