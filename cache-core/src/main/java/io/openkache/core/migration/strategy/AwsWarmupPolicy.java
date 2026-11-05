package io.openkache.core.migration.strategy;

import java.util.Map;

/** CPU target tracking·추가 scale-in warm-up gate의 B3 직렬 재구성 변형. */
public final class AwsWarmupPolicy implements NodeCountPolicy {
    private int seen=-100,warmUntil=-1;
    @Override public String name(){return "aws-warmup";}
    @Override public Decision decideCount(ScalingSnapshot s){
        if(seen!=s.lastCompletedWindow()){seen=s.lastCompletedWindow();if(s.lastAction().equals("out"))warmUntil=seen+Math.max(1,s.config().provisionWindows());}
        double cpu=s.cpu().values().stream().mapToDouble(Double::doubleValue).average().orElseThrow();
        int desired=Math.max(s.config().minNodes(),Math.min(s.config().maxNodes(),(int)Math.ceil(s.active().size()*cpu/s.config().target())));
        if(desired<s.active().size()&&s.window()<warmUntil)return NodeCountPolicy.count(s,s.active().size(),"warmup-block-scale-in",Map.of("warm_until",warmUntil,"desired",desired));
        return NodeCountPolicy.count(s,desired,"cpu-target",Map.of("mean_cpu",cpu,"target",s.config().target(),"desired",desired,"warm_until",warmUntil));
    }
}
