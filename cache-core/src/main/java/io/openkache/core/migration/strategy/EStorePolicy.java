package io.openkache.core.migration.strategy;
import java.util.Map;
/** E-Store CPU watermarks의 수 판단만 사용한다. hot-key 세분화와 Greedy Extended는 제외한다. */
public final class EStorePolicy implements NodeCountPolicy {
    @Override public String name(){return "estore";}
    @Override public Decision decideCount(ScalingSnapshot s){
        double cpu=s.cpu().values().stream().mapToDouble(Double::doubleValue).average().orElseThrow();
        int delta=cpu>s.config().high()?1:cpu<s.config().low()?-1:0;
        return NodeCountPolicy.count(s,s.active().size()+delta,"CPU-watermarks-count",Map.of("cpu_mean",cpu,"delta",delta));
    }
}