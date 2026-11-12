package io.openkache.server.routing;

import io.openkache.core.migration.TransferBatch;
import io.openkache.core.migration.precopy.PreCopyProtocol;
import io.openkache.core.model.Bytes;
import io.openkache.core.model.TabletId;
import io.openkache.core.tablet.KeyHash;
import io.openkache.core.tablet.Tablet;
import io.openkache.core.tablet.TabletRange;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Consumer;

/**
 * B1 노드 하나의 메모리 저장소. 쓰기·freeze 확인·dirty hook은 모두 같은 worker thread에서 실행되므로 서로 끼어들지 않는다
 * (B3 protocol의 "같은 owner thread" 전제). B3 {@link PreCopyProtocol.Source} 경계를 구현한다(stop-and-copy Source 포함).
 *
 * <p>Tablet 전체를 훑는 두 작업은 worker를 오래 막지 않도록 background thread에서 한다.
 * <ul>
 *   <li>snapshot: 이동 시작 전에 {@link #scan}으로 미리 만든다. pre-copy는 {@link #preTrack}로 쓰기 추적을 먼저 켜고 만들므로 그
 *       사이 바뀐 key는 dirty로 다시 보내진다. stop-and-copy는 {@link #reserveFreeze}로 freeze를 먼저 걸고 만든다.
 *       protocol이 begin에서 {@link #snapshot}을 부르면 미리 만든 목록을 돌려준다.
 *   <li>cleanup: commit 뒤 source 범위 삭제를 background로 시작하고 끝날 때까지 false를 돌려준다(B3 계약: 반복 호출 안전).
 *       이 범위는 이미 다른 노드 소유라 worker가 쓰지 않는다.
 * </ul>
 * 표시하지 않은 메서드는 worker thread 전용이다. data는 background 읽기·삭제를 위해 동시성 map이다.
 */
final class NodeStore implements PreCopyProtocol.Source, AutoCloseable {

    private final Map<Bytes, Bytes> data = new ConcurrentHashMap<>();
    private final Set<TabletId> frozen = new HashSet<>();
    private final Set<TabletId> reservedFreeze = new HashSet<>();
    private final Map<TabletId, Tracker> trackers = new HashMap<>();
    private final Map<TabletId, PreTrack> preTracked = new HashMap<>();
    private final Map<TabletId, Set<Bytes>> pendingReplay = new HashMap<>();
    private final Map<TabletId, List<TransferBatch.Record>> prepared = new HashMap<>();
    private final Map<TabletId, Future<?>> removals = new HashMap<>();
    private final ExecutorService background = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "openkache-store-background");
        t.setDaemon(true);
        return t;
    });
    private long version;

    private record Tracker(TabletRange range, Consumer<Bytes> onWrite) {}

    private record PreTrack(TabletRange range, Set<Bytes> keys) {}

    Optional<Bytes> get(Bytes key) {
        return Optional.ofNullable(data.get(key));
    }

    /** 성공한 SET의 값 버전(노드 단위 증가값). */
    long set(Bytes key, Bytes value) {
        data.put(key, value);
        notifyWrite(key);
        return ++version;
    }

    boolean delete(Bytes key) {
        boolean existed = data.remove(key) != null;
        if (existed) notifyWrite(key);
        return existed;
    }

    /** 이 Tablet이 freeze됐으면 쓰기를 BUSY로 돌려보낸다. */
    boolean writeBlocked(TabletId tablet) {
        return frozen.contains(tablet);
    }

    int size() {
        return data.size();
    }

    /** 이동 수신: 범위 검증은 호출자가 한다. dirty hook은 부르지 않는다(이 노드는 아직 주인이 아니다). */
    void load(Bytes key, Optional<Bytes> value) {
        if (value.isPresent()) data.put(key, value.get());
        else data.remove(key);
    }

    /** 해시 범위의 항목을 지금 모두 지우고 지운 수를 돌려준다. */
    int deleteRange(TabletRange range) {
        int before = data.size();
        data.keySet().removeIf(key -> range.contains(KeyHash.of(key)));
        return before - data.size();
    }

    private void notifyWrite(Bytes key) {
        if (trackers.isEmpty() && preTracked.isEmpty()) return;
        long hash = KeyHash.of(key);
        for (Tracker t : trackers.values()) {
            if (t.range().contains(hash)) t.onWrite().accept(key);
        }
        for (PreTrack t : preTracked.values()) {
            if (t.range().contains(hash)) t.keys().add(key);
        }
    }

    // --- 이동 준비 (worker thread) ---

    /** stop-and-copy: protocol begin 전에 freeze를 걸어 둔다. begin의 첫 {@link #freeze}가 이 예약을 가져간다. */
    boolean reserveFreeze(Tablet tablet) {
        if (!frozen.add(tablet.id())) return false;
        reservedFreeze.add(tablet.id());
        return true;
    }

    /** pre-copy: protocol begin 전부터 쓰인 key를 모은다. begin의 {@link #startTracking}이 이어받아 dirty로 넘긴다. */
    boolean preTrack(Tablet tablet) {
        if (frozen.contains(tablet.id()) || trackers.containsKey(tablet.id()) || preTracked.containsKey(tablet.id())) {
            return false;
        }
        preTracked.put(tablet.id(), new PreTrack(tablet.range(), new LinkedHashSet<>()));
        return true;
    }

    /** background에서 만든 snapshot을 begin에 넘길 준비를 한다. */
    void prepareSnapshot(Tablet tablet, List<TransferBatch.Record> records) {
        prepared.put(tablet.id(), records);
    }

    /** begin 전에 취소되면 예약과 준비물을 지운다. */
    void cancelPrepared(Tablet tablet) {
        if (reservedFreeze.remove(tablet.id())) frozen.remove(tablet.id());
        preTracked.remove(tablet.id());
        pendingReplay.remove(tablet.id());
        prepared.remove(tablet.id());
    }

    /** 어느 thread에서나 호출할 수 있다. 동시 쓰기와는 약한 일관성이다(호출자가 freeze나 추적으로 보완한다). */
    List<TransferBatch.Record> scan(TabletRange range) {
        List<TransferBatch.Record> records = new ArrayList<>();
        for (Map.Entry<Bytes, Bytes> e : data.entrySet()) {
            if (range.contains(KeyHash.of(e.getKey()))) records.add(new TransferBatch.Record(e.getKey(), e.getValue()));
        }
        return records;
    }

    /** background thread에서 실행한다. */
    void inBackground(Runnable task) {
        background.execute(task);
    }

    // --- B3 Source 경계 ---

    @Override
    public boolean freeze(Tablet tablet) {
        if (reservedFreeze.remove(tablet.id())) return true;
        return frozen.add(tablet.id());
    }

    @Override
    public boolean frozen(Tablet tablet) {
        return frozen.contains(tablet.id());
    }

    @Override
    public List<TransferBatch.Record> snapshot(Tablet tablet) {
        // begin 전에 모은 dirty key는 여기서 넘긴다. PreCopyProtocol은 startTracking이 반환된 뒤에야 추적을 켜므로
        // startTracking 안에서 넘기면 버려진다.
        Set<Bytes> early = pendingReplay.remove(tablet.id());
        Tracker tracker = trackers.get(tablet.id());
        if (early != null && tracker != null) early.forEach(tracker.onWrite());
        List<TransferBatch.Record> ready = prepared.remove(tablet.id());
        return ready != null ? ready : scan(tablet.range());
    }

    @Override
    public void unfreeze(Tablet tablet) {
        frozen.remove(tablet.id());
    }

    /** 처음 호출에 background 삭제를 시작하고, 끝난 뒤의 호출에서 true. */
    @Override
    public boolean cleanup(Tablet tablet) {
        Future<?> removal = removals.get(tablet.id());
        if (removal == null) {
            TabletRange range = tablet.range();
            removals.put(tablet.id(), background.submit(() -> deleteRange(range)));
            return false;
        }
        if (!removal.isDone()) return false;
        removals.remove(tablet.id());
        return true;
    }

    @Override
    public boolean startTracking(Tablet tablet, Consumer<Bytes> onWrite) {
        if (frozen.contains(tablet.id()) || trackers.containsKey(tablet.id())) return false;
        trackers.put(tablet.id(), new Tracker(tablet.range(), onWrite));
        PreTrack early = preTracked.remove(tablet.id());
        if (early != null) pendingReplay.put(tablet.id(), early.keys());
        return true;
    }

    @Override
    public void stopTracking(Tablet tablet) {
        trackers.remove(tablet.id());
        preTracked.remove(tablet.id());
        pendingReplay.remove(tablet.id());
    }

    @Override
    public Optional<Bytes> read(Tablet tablet, Bytes key) {
        return get(key);
    }

    @Override
    public void close() {
        background.shutdownNow();
    }
}
