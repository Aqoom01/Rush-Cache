package io.openkache.core.migration.strategy;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
/** PAX의 ARIMA·평균 CPU 판단을 count-only로 적용. partition 선택·휴면 replica·hint는 제외한다. */
public final class PaxPolicy implements NodeCountPolicy {
    private final List<Double> history;
    public PaxPolicy(List<Double> past){history=new ArrayList<>(past);}
    @Override public String name(){return "pax";}
    @Override public void observe(double total){history.add(total);}
    public static double[] forecast(List<Double> past,int steps){return Numerics.arima(past.stream().mapToDouble(Double::doubleValue).toArray(),steps);}
    @Override public Decision decideCount(ScalingSnapshot s){
        var c=s.config();double[] fit=forecast(history,c.provisionWindows());double prediction=fit[0];
        double work=java.util.Arrays.stream(s.reads()).sum()*c.readService()+java.util.Arrays.stream(s.writes()).sum()*c.writeService();
        double predictedWork=work*prediction/Math.max(1,s.total()),cpu=predictedWork/s.active().size();int desired=s.active().size();
        double middle=(c.low()+c.high())/2;
        if(cpu>c.high())desired=Math.max(desired,(int)Math.ceil(predictedWork/middle));
        else if(cpu<c.low())desired=Math.min(desired,(int)Math.ceil(predictedWork/middle));
        return NodeCountPolicy.count(s,desired,"ARIMA-average-count",Map.of("prediction",prediction,"phi",fit[1],"theta",fit[2],"predicted_cpu",cpu,"native_partition_selection",false));
    }
}