package io.openkache.core.migration.strategy;

import java.util.List;
import java.util.Map;

/** 정책이 읽는 관측값. 배열도 복사하며 controller thread에서 생성·사용한다. */
public record ScalingSnapshot(ScalingConfig config,int window,List<Integer> active,List<PartitionUnit> units,
        double[] reads,double[] writes,Map<Integer,Double> cpu,double p99,double total,double completed,
        long[] hints,int lastCompletedWindow,String lastAction) {
    public ScalingSnapshot {
        active=List.copyOf(active);units=List.copyOf(units);reads=reads.clone();writes=writes.clone();cpu=java.util.Collections.unmodifiableMap(new java.util.TreeMap<>(cpu));hints=hints.clone();
        if(reads.length!=config.keys()||writes.length!=config.keys()||hints.length!=config.maxNodes()||active.isEmpty()
                || !Double.isFinite(p99)||p99<0||!Double.isFinite(total)||total<0||!Double.isFinite(completed)||completed<0)
            throw new IllegalArgumentException("invalid scaling snapshot");
        for(double v:reads) if(!Double.isFinite(v)||v<0) throw new IllegalArgumentException("invalid read rate");
        for(double v:writes) if(!Double.isFinite(v)||v<0) throw new IllegalArgumentException("invalid write rate");
    }
    @Override public double[] reads(){return reads.clone();}
    @Override public double[] writes(){return writes.clone();}
    @Override public long[] hints(){return hints.clone();}
}
