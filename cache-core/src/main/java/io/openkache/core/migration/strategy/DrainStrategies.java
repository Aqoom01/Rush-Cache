package io.openkache.core.migration.strategy;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/** Scylla식 전체 사전 검사와 MongoDB식 부분 drain의 B3 연구 변형. */
public final class DrainStrategies {
    public static final List<String> METHODS=List.of("preflight-all","incremental-drain");
    private DrainStrategies(){}
    public static DrainStrategy create(String name){
        if(!METHODS.contains(name))throw new IllegalArgumentException("unknown B3 drain: "+name);
        return new DrainStrategy(){
            @Override public String name(){return name;}
            @Override public Decision admit(Snapshot s){
                if(s.nodesRemaining()<s.replicaMinimum())return new Decision(List.of(),"replica-floor");
                if(!s.executorEnabled())return new Decision(List.of(),"executor-paused");
                var free=new HashMap<>(s.freeBytes());var admitted=new ArrayList<io.openkache.core.tablet.Change>();String reason="admitted";
                for(var job:s.jobs()){
                    Long size=s.bytes().get(job.unit());if(size==null||size<0)throw new IllegalArgumentException("missing job size");
                    boolean allowed=!s.unavailable().contains(job.from())&&!s.unavailable().contains(job.to())&&!s.blockedUnits().contains(job.unit())&&free.getOrDefault(job.to(),0L)>=size;
                    if(!allowed){reason="blocked-jobs-remain";if(name.equals("preflight-all"))return new Decision(List.of(),"preflight-rejected");continue;}
                    admitted.add(job);free.merge(job.to(),-size,Long::sum);
                }
                return new Decision(admitted,reason);
            }
        };
    }
}
