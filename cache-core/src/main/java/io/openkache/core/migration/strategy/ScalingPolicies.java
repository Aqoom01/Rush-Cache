package io.openkache.core.migration.strategy;

import java.util.List;

/** B3 전용 이름→새 상태 인스턴스 registry. 실험끼리 PI/Q/history 상태를 공유하지 않는다. */
public final class ScalingPolicies {
    public static final List<String> LITERATURE=List.of("scads","elastman","estore","accordion","pstore","tiramola","pax");
    public static final List<String> METHODS=List.of("scads","elastman","estore","accordion","pstore","tiramola","pax","aws-warmup");
    private ScalingPolicies(){}
    public static NodeCountPolicy create(String name,PerformanceModel model,List<Double> past){
        return switch(name){case "scads"->new ScadsPolicy(model);case "elastman"->new ElastManPolicy(model);case "estore"->new EStorePolicy();case "accordion"->new AccordionPolicy(model);case "pstore"->new PStorePolicy(model,past);case "tiramola"->new TiramolaPolicy(model);case "pax"->new PaxPolicy(past);case "aws-warmup","aws-target-warmup"->new AwsWarmupPolicy();default->throw new IllegalArgumentException("unknown B3 scaling policy: "+name);};
    }
}
