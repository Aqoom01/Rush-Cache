package io.openkache.core.migration.strategy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/** SPAR + 시간×노드 수 동적 계획법. 비용은 allocated node-window이며 실제 요금이 아니다. */
public final class PStorePolicy implements NodeCountPolicy {
    public record Move(int window,int before,int after,int duration){}
    public record Schedule(double cost,List<Move> moves){public Schedule{moves=List.copyOf(moves);}}
    private final PerformanceModel model;private final List<Double> history;private final SparPredictor predictor;
    public PStorePolicy(PerformanceModel model,List<Double> past){this.model=model;history=new ArrayList<>(past);predictor=new SparPredictor(past,model.config());}
    @Override public String name(){return "pstore";}
    @Override public void observe(double total){history.add(total);}
    @Override public Decision decideCount(ScalingSnapshot s){var predicted=predictor.predict(history);var schedule=schedule(predicted,s.active().size(),model.capacity()*.9,s.config());
        if(schedule.moves().isEmpty())return NodeCountPolicy.count(s,(int)Math.ceil(s.total()/(model.capacity()*.9)),"prediction-infeasible-reactive-fallback",Map.of("prediction",predicted));
        return NodeCountPolicy.count(s,schedule.moves().getFirst().after(),"SPAR-dynamic-program",Map.of("prediction",predicted,"node_window_cost",schedule.cost(),"schedule",schedule.moves().stream().map(m->List.of(m.window(),m.before(),m.after(),m.duration())).toList()));}
    public static int moveDuration(int before,int after,ScalingConfig c){if(before==after)return c.controlWindows();double moved=(double)c.keys()*c.keyBytes()*Math.abs(after-before)/Math.max(before,after);int duration=(int)Math.ceil(moved/c.transferBytes())+(after>before?c.provisionWindows():0);return Math.max(c.controlWindows(),(int)Math.ceil((double)duration/c.controlWindows())*c.controlWindows());}
    public static Schedule schedule(List<Double> predictions,int initial,double capacity,ScalingConfig c){
        int horizon=predictions.size();double[][] cost=new double[horizon+1][c.maxNodes()+1];int[][] choice=new int[horizon][c.maxNodes()+1];for(int t=0;t<horizon;t++)Arrays.fill(cost[t],Double.POSITIVE_INFINITY);
        for(int t=horizon-1;t>=0;t--)for(int before=c.minNodes();before<=c.maxNodes();before++)for(int after=c.minNodes();after<=c.maxNodes();after++){
            int duration=moveDuration(before,after,c);if(t+duration>horizon)continue;double effective=Math.min(before,after)*capacity*(before==after?1:1-c.migrationPenalty());boolean fits=true;for(int k=t;k<t+duration-1;k++)if(predictions.get(k)>effective)fits=false;
            if(!fits||predictions.get(t+duration-1)>after*capacity)continue;double value=Math.max(before,after)*duration+cost[t+duration][after];if(value<cost[t][before]-1e-9){cost[t][before]=value;choice[t][before]=after;}}
        var moves=new ArrayList<Move>();if(Double.isFinite(cost[0][initial])){int t=0,before=initial;while(t<horizon){int after=choice[t][before];if(after==0)throw new IllegalStateException("broken DP schedule");int duration=moveDuration(before,after,c);moves.add(new Move(t,before,after,duration));t+=duration;before=after;}}
        return new Schedule(cost[0][initial],moves);
    }
}
