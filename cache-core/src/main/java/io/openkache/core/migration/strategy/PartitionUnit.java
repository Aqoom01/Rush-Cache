package io.openkache.core.migration.strategy;

import java.util.HashSet;
import java.util.List;

/** 연구용 key 집합과 복제 holder. 실제 Tablet split/merge를 수행하지 않는 불변 입력 모델이다. */
public record PartitionUnit(List<Integer> keys, List<Integer> holders) {
    public PartitionUnit {
        keys=List.copyOf(keys); holders=List.copyOf(holders);
        if(keys.isEmpty()||holders.isEmpty()||new HashSet<>(keys).size()!=keys.size()||new HashSet<>(holders).size()!=holders.size()
                ||keys.stream().anyMatch(k->k<0)||holders.stream().anyMatch(n->n<0)) throw new IllegalArgumentException("invalid partition unit");
        for(int i=1;i<keys.size();i++) if(keys.get(i)<=keys.get(i-1)) throw new IllegalArgumentException("keys must be sorted");
    }
    public PartitionUnit at(int node) { return new PartitionUnit(keys,List.of(node)); }
}
