package io.openkache.core.migration.strategy;

import io.openkache.core.model.NodeId;
import io.openkache.core.model.TabletId;
import io.openkache.core.tablet.Change;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** 자체 B3 실행기의 scale-in 승인 경계. actual replica repair를 대신하지 않는다. */
public interface DrainStrategy {
    record Snapshot(List<Change> jobs,Map<TabletId,Long> bytes,Map<NodeId,Long> freeBytes,Set<NodeId> unavailable,
            Set<TabletId> blockedUnits,boolean executorEnabled,int nodesRemaining,int replicaMinimum,boolean metadataCleared){
        public Snapshot{jobs=List.copyOf(jobs);bytes=Map.copyOf(bytes);freeBytes=Map.copyOf(freeBytes);unavailable=Set.copyOf(unavailable);blockedUnits=Set.copyOf(blockedUnits);if(nodesRemaining<0||replicaMinimum<1)throw new IllegalArgumentException("invalid replica floor");}
    }
    record Decision(List<Change> admitted,String reason){public Decision{admitted=List.copyOf(admitted);}}
    String name();
    Decision admit(Snapshot snapshot);
    /** 잔여 Tablet와 미완료 작업이 없고 관리 metadata가 정리된 경우만 제거한다. */
    default boolean canRemove(int remainingUnits,int unfinishedJobs,boolean metadataCleared){return remainingUnits==0&&unfinishedJobs==0&&metadataCleared;}
}
