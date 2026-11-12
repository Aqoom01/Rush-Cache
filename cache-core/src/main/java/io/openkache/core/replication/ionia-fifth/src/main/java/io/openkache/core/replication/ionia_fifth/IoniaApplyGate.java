package io.openkache.core.replication.ionia_fifth;

import io.openkache.core.model.Bytes;
import java.util.*;

/** owner 전용 key 허가/준비 FIFO. 실제 thread나 저장소 큐의 개수를 제어하지 않는다. */
public final class IoniaApplyGate {
    /** immutable counters. starts = released + resetDiscarded + activeKeys. */
    public record Snapshot(int limit,int activeKeys,int readyKeys,int available,int peakActive,
                           long starts,long released,long resetDiscarded,long resets) { }
    private final int limit;
    private final LinkedHashSet<Bytes> ready=new LinkedHashSet<>();
    private final Set<Bytes> active=new HashSet<>();
    private int peak;
    private long starts,released,discarded,resets;
    public IoniaApplyGate(int limit){if(limit<1 || limit>65536)throw new IllegalArgumentException("invalid apply limit");this.limit=limit;}
    /** 아직 실행 중이지 않은 queue head key만 넣는다. 중복 준비 통지는 순서를 바꾸지 않는다. */
    public void ready(Bytes key){Objects.requireNonNull(key);if(active.contains(key))throw new IllegalStateException("active key cannot be requeued");ready.add(key);}
    /** 빈 slot이 있을 때 가장 오래 기다린 독립 key를 가져오고 허가를 잡는다. */
    public Optional<Bytes> acquire(){
        if(active.size()>=limit || ready.isEmpty())return Optional.empty();
        Bytes key=ready.removeFirst();if(!active.add(key))throw new IllegalStateException("duplicate active key");
        starts++;peak=Math.max(peak,active.size());return Optional.of(key);
    }
    /** 호출자 timeout/cancel이 아니라 저장소 작업의 물리 완료 뒤 owner에서 반환한다. */
    public void completed(Bytes key){if(!active.remove(key))throw new IllegalStateException("no active permit");released++;}
    /** 반드시 옛 물리 작업 drain/reset 뒤 호출한다. 다른 installation의 허가를 재사용하지 않는다. */
    public void resetAfterDrain(){discarded+=active.size();active.clear();ready.clear();resets++;}
    public Snapshot snapshot(){return new Snapshot(limit,active.size(),ready.size(),limit-active.size(),peak,starts,released,discarded,resets);}
}
