package io.openkache.core.replication.chain;

import io.openkache.core.api.Command;
import io.openkache.core.api.Result;
import io.openkache.core.api.ResultStatus;
import io.openkache.core.model.*;
import java.util.*;

/** owner 전용 key별 버전 관리. 로그 위치는 프로토콜 버전이며 KV의 공개 valueVersion과 구분한다. */
final class CraqVersions {
    private record Value(Bytes bytes,long kvVersion) {}
    private final Map<Bytes,NavigableMap<Integer,Value>> objects=new HashMap<>();

    void applied(int index,Command command,Result result) {
        Bytes key=command instanceof Command.Set s?s.key():((Command.Delete)command).key();
        var versions=objects.computeIfAbsent(key,k->{
            NavigableMap<Integer,Value> map=new TreeMap<>(); map.put(0,new Value(null,0)); return map;
        });
        versions.put(index,command instanceof Command.Set s?new Value(s.value(),result.valueVersion()):new Value(null,0));
    }
    int latest(Bytes key) { var map=objects.get(key); return map==null?0:map.lastKey(); }
    boolean contains(Bytes key,int version) { var map=objects.get(key); return map==null?version==0:map.containsKey(version); }
    Result read(Bytes key,int version,boolean exists,OwnershipEpoch epoch) {
        var map=objects.get(key);
        Value value=map==null && version==0?new Value(null,0):map==null?null:map.get(version);
        if(value==null) throw new IllegalStateException("CRAQ selected version was reclaimed or never applied");
        if(value.bytes()==null) return Result.of(ResultStatus.MISS,RoutingVersion.ZERO,epoch);
        return new Result(ResultStatus.OK,exists?Optional.empty():Optional.of(value.bytes()),value.kvVersion(),
                RoutingVersion.ZERO,epoch,Optional.empty());
    }
    // 조회 중에는 오래된 응답도 올 수 있으므로 해당 key는 완료/timeout까지 과거 버전을 보존한다.
    void collect(int acknowledged,Set<Bytes> pinned) {
        for(var entry:objects.entrySet()) if(!pinned.contains(entry.getKey())) {
            Integer clean=entry.getValue().floorKey(acknowledged);
            if(clean!=null) entry.getValue().headMap(clean,false).clear();
        }
    }
    int retained() { return objects.values().stream().mapToInt(Map::size).sum(); }
}
