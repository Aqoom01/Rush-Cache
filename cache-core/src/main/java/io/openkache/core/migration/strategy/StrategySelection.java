package io.openkache.core.migration.strategy;

import io.openkache.core.migration.experiment.SchedulerAlgorithms;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/** 설정으로 선택하는 B3 전략 묶음. registry 이름·조합·기능 전제를 I/O 없이 검증한다. */
public record StrategySelection(String scaler,String planner,String scheduler,String copy,String drain) {
    public record Capabilities(boolean replicas,boolean splitMerge,boolean nativeHints){}
    public StrategySelection {
        if(scaler.equals("aws-target-warmup"))scaler="aws-warmup";
        if(!ScalingPolicies.METHODS.contains(scaler)||!PlacementPlanners.METHODS.contains(planner)
                ||!SchedulerAlgorithms.METHODS.contains(scheduler)||!DrainStrategies.METHODS.contains(drain)
                ||!CopyDrivers.METHODS.contains(copy))throw new IllegalArgumentException("unknown B3 strategy selection");

    }
    public static StrategySelection from(Properties p){return new StrategySelection(p.getProperty("strategy.scaler","elastman"),p.getProperty("strategy.planner",FixedTabletPlacement.NAME),p.getProperty("strategy.scheduler","fifo"),p.getProperty("strategy.copy","pre-copy"),p.getProperty("strategy.drain","preflight-all"));}
    public void require(Capabilities c){ /* count-only policies require no split, replica or hint support. */ }
    public Map<String,Object> record(){var out=new LinkedHashMap<String,Object>();out.put("scaler",scaler);out.put("planner",planner);out.put("scheduler",scheduler);out.put("copy",copy);out.put("drain",drain);return java.util.Collections.unmodifiableMap(out);}
    public static Map<String,Object> available(){var out=new LinkedHashMap<String,Object>();out.put("scaler",ScalingPolicies.METHODS);out.put("planner",PlacementPlanners.METHODS);out.put("scheduler",SchedulerAlgorithms.METHODS);out.put("copy",CopyDrivers.METHODS);out.put("drain",DrainStrategies.METHODS);return out;}
}
