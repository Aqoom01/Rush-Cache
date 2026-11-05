package io.openkache.core.migration.strategy;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

/** B3 전략의 제약 검사·LPT adapter. 공통 서비스 계약을 변경하지 않는다. */
public final class ScalingSupport {
    private ScalingSupport() {}
    public static List<Integer> nodes(int count){return IntStream.range(0,count).boxed().toList();}
    public static double work(PartitionUnit u,double[] r,double[] w,ScalingConfig c){return work(u,r,w,u.holders(),c);}
    public static double work(PartitionUnit u,double[] r,double[] w,List<Integer> active,ScalingConfig c){
        long holders=u.holders().stream().filter(active::contains).count();
        if(holders==0) throw new IllegalArgumentException("no active replica");
        double reads=0,writes=0;for(int k:u.keys()){reads+=r[k];writes+=w[k];}
        return reads/holders*c.readService()+writes*c.writeService();
    }
    public static Map<Integer,Double> loads(List<PartitionUnit> units,List<Integer> active,double[] r,double[] w,ScalingConfig c){
        Map<Integer,Double> out=new LinkedHashMap<>();active.forEach(n->out.put(n,0.));
        for(var u:units) for(int n:u.holders()) if(out.containsKey(n)) out.merge(n,work(u,r,w,active,c),Double::sum);
        return out;
    }
    public static List<PartitionUnit> sorted(List<PartitionUnit> units){return units.stream().sorted(Comparator.comparingInt(u->u.keys().getFirst())).toList();}
    public static long movedBytes(List<PartitionUnit> old,List<PartitionUnit> next,ScalingConfig c){
        Map<Integer,List<Integer>> previous=new HashMap<>();old.forEach(u->u.keys().forEach(k->previous.put(k,u.holders())));long total=0;
        for(var u:next) for(int k:u.keys()) for(int n:u.holders()) if(!previous.get(k).contains(n)) total=Math.addExact(total,c.keyBytes());return total;
    }
    public static void validate(ScalingPlan p,ScalingConfig c,boolean replica){
        if(p.active().size()<c.minNodes()||p.active().size()>c.maxNodes()||new HashSet<>(p.active()).size()!=p.active().size()
                ||p.active().stream().anyMatch(n->n<0||n>=c.maxNodes())) throw new IllegalArgumentException("invalid active nodes");
        boolean[] keys=new boolean[c.keys()];long[] memory=new long[c.maxNodes()];
        for(var u:p.units()){
            if(u.holders().stream().anyMatch(n->n>=c.maxNodes())||u.holders().stream().noneMatch(p.active()::contains)
                    ||!replica&&!p.active().containsAll(u.holders())) throw new IllegalArgumentException("unavailable partition");
            for(int k:u.keys()){if(k>=c.keys()||keys[k]) throw new IllegalArgumentException("duplicate/missing key");keys[k]=true;}
            for(int n:u.holders()){memory[n]=Math.addExact(memory[n],Math.multiplyExact(u.keys().size(),c.keyBytes()));if(memory[n]>c.nodeBytes()) throw new IllegalArgumentException("memory capacity");}
        }
        for(boolean present:keys)if(!present)throw new IllegalArgumentException("missing key");
    }
    static int coldest(Map<Integer,Double> loads){return loads.keySet().stream().min(Comparator.<Integer>comparingDouble(loads::get).thenComparingInt(Integer::intValue)).orElseThrow();}
    static int hottest(Map<Integer,Double> loads){return loads.keySet().stream().max(Comparator.<Integer>comparingDouble(loads::get).thenComparingInt(n->-n)).orElseThrow();}
    static double clip(double x,double low,double high){return Math.max(low,Math.min(high,x));}
}
