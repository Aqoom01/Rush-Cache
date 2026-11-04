package io.openkache.core.migration.postcopy;

import io.openkache.core.api.Command;
import io.openkache.core.api.Redirect;
import io.openkache.core.api.Result;
import io.openkache.core.api.ResultStatus;
import io.openkache.core.migration.JobState;
import io.openkache.core.migration.MigrationJob;
import io.openkache.core.migration.MigrationPublication;
import io.openkache.core.migration.TransferProgress;
import io.openkache.core.model.Bytes;
import io.openkache.core.model.OwnershipEpoch;
import io.openkache.core.tablet.KeyHash;
import io.openkache.core.tablet.RoutingTable;
import io.openkache.core.tablet.Tablet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * 소유권 선전환 뒤 priority/background pull로 채우는 in-process post-copy.
 * source-owned MigrationProtocol/abort 계약과 분리한다. 모든 호출과 source 요청·commit은 같은 controller thread 전용이다.
 * 미도착 GET은 BUSY로 재시도하며, source 장애는 FAILED/UNAVAILABLE로 남긴다.
 * 실제 서버·peer/lease/failover adapter가 아니며 source는 복사가 끝날 때까지 보존·fence해야 한다.
 */
public final class PostCopyProtocol {
    public interface Source {
        boolean freeze(Tablet tablet);
        boolean frozen(Tablet tablet);
        void unfreeze(Tablet tablet);
        /** freeze 상태의 범위 내 key 전체. 값은 미리 복사하지 않는다. */
        List<Bytes> keys(Tablet tablet);
        /** source 생존 여부. false면 미도착 key를 MISS로 취급하지 않는다. */
        boolean available(Tablet tablet);
        /** 빈 Optional은 응답 대기/누락이며 key 부재와 다르다. 같은 requestId 재호출은 멱등이어야 한다. */
        Optional<PullReply> pull(MigrationJob session, OwnershipEpoch epoch, long requestId, Bytes key);
        /** 범위만 삭제한다. 부분 실패는 false이며 재시도 가능해야 한다. */
        boolean cleanup(Tablet tablet);
    }

    private final Source source;
    private final RoutingTable expectedTable;
    private RoutingTable publishedTable;
    private MigrationJob session;
    private PostCopyState state = PostCopyState.PENDING;
    private boolean ownsFreeze;
    private boolean committed;
    private final Map<Bytes, Bytes> data = new HashMap<>();
    private final Set<Bytes> resolved = new HashSet<>();
    private final Set<Bytes> remainingKeys = new LinkedHashSet<>();
    private final Set<Bytes> priority = new LinkedHashSet<>();
    private Bytes pendingKey;
    private PullReply pendingReply;
    private long requestId;
    private long paidBytes;
    private long sentBytes;
    private long completedPulls;

    public PostCopyProtocol(Source source, RoutingTable expectedTable) {
        this.source = Objects.requireNonNull(source, "source");
        this.expectedTable = Objects.requireNonNull(expectedTable, "expectedTable");
    }

    public String name() { return "post-copy"; }
    public PostCopyState state() { return state; }
    public RoutingTable expectedTable() { return expectedTable; }
    public long sentBytes() { return sentBytes; }
    public long completedPulls() { return completedPulls; }
    public int remainingKeys() { return remainingKeys.size(); }
    private Tablet tablet() { return expectedTable.get(session.change().unit()); }

    /** source freeze와 key manifest만 확보한다. 데이터 전송 전에 외부 routing CAS를 수행한다. */
    public PostCopyState begin(MigrationJob candidate) {
        Objects.requireNonNull(candidate, "job");
        if (session != null) throw new IllegalStateException("already begun");
        if (candidate.state() != JobState.PENDING || !candidate.progress().equals(TransferProgress.initial())
                || !candidate.variant().equals(name())
                || !expectedTable.get(candidate.change().unit()).owner().equals(candidate.change().from())) {
            throw new IllegalArgumentException("invalid pending job");
        }
        session = candidate;
        try {
            ownsFreeze = source.freeze(tablet());
            if (!ownsFreeze) return abort();
            for (Bytes key : List.copyOf(source.keys(tablet()))) {
                if (!tablet().range().contains(KeyHash.of(key)) || !remainingKeys.add(key)) {
                    throw new IllegalArgumentException("invalid source key manifest");
                }
            }
            state = PostCopyState.SWITCHING;
            return state;
        } catch (RuntimeException failure) {
            abort();
            throw failure;
        }
    }

    public boolean readyToCommit() {
        return state == PostCopyState.SWITCHING && source.frozen(tablet()) && source.available(tablet());
    }

    /** readyToCommit 확인 → 외부 CAS → 이 호출을 하나의 controller 작업으로 실행한다. */
    public void committed(RoutingTable published) {
        committed(expectedTable, published);
    }

    /** 다른 Tablet의 전환을 보존하는 실제 CAS 전·후 표를 받는다. 같은 controller 작업에서 호출한다. */
    public void committed(RoutingTable before, RoutingTable published) {
        if (!readyToCommit()) throw new IllegalStateException("not ready to commit");
        MigrationPublication.verify(session, expectedTable, before, published);
        publishedTable = published;
        committed = true;
        state = PostCopyState.PULLING;
    }

    /** destination의 GET/SET/DELETE. caller가 현재 routing의 owner를 먼저 판정해야 한다. */
    public Result execute(Command command) {
        Objects.requireNonNull(command, "command");
        if (session == null) throw new IllegalStateException("begin first");
        Bytes key = switch (command) {
            case Command.Get get -> get.key();
            case Command.Set set -> set.key();
            case Command.Delete delete -> delete.key();
            default -> throw new IllegalArgumentException("post-copy supports GET/SET/DELETE");
        };
        if (!tablet().range().contains(KeyHash.of(key))) throw new IllegalArgumentException("key outside moving tablet");
        if (!committed) {
            return Result.redirect(ResultStatus.MOVED, new Redirect(session.change().from()),
                    expectedTable.version(), tablet().epoch());
        }
        return switch (command) {
            case Command.Set set -> {
                resolve(key);
                data.put(key, set.value());
                yield result(ResultStatus.OK);
            }
            case Command.Delete delete -> {
                // unresolved인 기존 key도 논리적으로 존재하므로 삭제 성공이며 baseline 부활을 막는다.
                boolean existed = data.containsKey(key) || (!resolved.contains(key) && remainingKeys.contains(key));
                resolve(key);
                data.remove(key);
                yield result(existed ? ResultStatus.OK : ResultStatus.MISS);
            }
            case Command.Get get -> {
                if (data.containsKey(key)) yield Result.ok(data.get(key), 0, publishedTable.version(),
                        publishedTable.get(tablet().id()).epoch());
                if (resolved.contains(key) || !remainingKeys.contains(key)) yield result(ResultStatus.MISS);
                priority.add(key);
                yield result(state == PostCopyState.FAILED ? ResultStatus.UNAVAILABLE : ResultStatus.BUSY);
            }
            default -> throw new IllegalArgumentException("post-copy supports GET/SET/DELETE");
        };
    }

    private Result result(ResultStatus status) {
        return Result.of(status, publishedTable.version(), publishedTable.get(tablet().id()).epoch());
    }

    private void resolve(Bytes key) {
        resolved.add(key); // DELETE tombstone도 남겨 늦은 pull이 최신 write를 덮지 못하게 한다.
        remainingKeys.remove(key);
        priority.remove(key);
    }

    /** 이번 tick byte 예산만 사용한다. 미사용 예산을 다음 tick에 적립하지 않는다. */
    public PostCopyState step(long budgetBytes) {
        if (budgetBytes < 0) throw new IllegalArgumentException("negative budget");
        if (session == null) throw new IllegalStateException("begin first");
        if (state == PostCopyState.CLEANING_UP) {
            if (!source.available(tablet()) || !source.frozen(tablet())) return fail();
            if (source.cleanup(tablet())) {
                source.unfreeze(tablet());
                ownsFreeze = false;
                state = PostCopyState.DONE;
            }
            return state;
        }
        if (state != PostCopyState.PULLING) return state;
        if (!source.available(tablet()) || !source.frozen(tablet())) return fail();
        long remaining = budgetBytes;
        while (true) {
            if (pendingKey != null && pendingReply == null && resolved.contains(pendingKey)) {
                // 응답 대기 중 성공한 write/delete가 key를 해결했으면 잃어버린 pull을 기다리지 않는다.
                pendingKey = null;
                requestId = Math.incrementExact(requestId);
            }
            if (pendingKey == null) {
                if (remainingKeys.isEmpty()) { state = PostCopyState.CLEANING_UP; break; }
                pendingKey = first(priority.isEmpty() ? remainingKeys : priority);
            }
            if (pendingReply == null) {
                Optional<PullReply> response = source.pull(session, tablet().epoch(), requestId, pendingKey);
                if (response.isEmpty()) break;
                PullReply reply = response.orElseThrow();
                if (reply.session() != session || !reply.epoch().equals(tablet().epoch())
                        || reply.requestId() != requestId || !reply.key().equals(pendingKey) || !reply.valid()) break;
                // manifest 내 key의 부재는 source 손실/adapter 오류이며 정상 MISS로 위장하지 않는다.
                if (reply.value().isEmpty()) return fail();
                pendingReply = reply;
            }
            long used = Math.min(remaining, pendingReply.serializedBytes() - paidBytes);
            remaining -= used;
            paidBytes += used;
            sentBytes = Math.addExact(sentBytes, used);
            if (paidBytes != pendingReply.serializedBytes()) break;
            if (!resolved.contains(pendingKey)) data.put(pendingKey, pendingReply.value().orElseThrow());
            resolve(pendingKey);
            completedPulls++;
            requestId = Math.incrementExact(requestId);
            pendingKey = null;
            pendingReply = null;
            paidBytes = 0;
            if (remaining == 0) break;
        }
        return state;
    }

    private static Bytes first(Set<Bytes> keys) {
        Iterator<Bytes> iterator = keys.iterator();
        Bytes key = iterator.next();
        return key;
    }

    /** commit 전에는 staging을 버리고 source를 재개한다. commit 뒤에는 취소를 거부한다. */
    public PostCopyState abort() {
        if (session == null) throw new IllegalStateException("begin first");
        if (committed || state == PostCopyState.ABORTED) return state;
        if (ownsFreeze) source.unfreeze(tablet());
        ownsFreeze = false;
        data.clear();
        resolved.clear();
        remainingKeys.clear();
        priority.clear();
        state = PostCopyState.ABORTED;
        return state;
    }

    /** 외부 timeout/source 장애. 소유권과 destination 성공 쓰기·삭제를 보존하고 source는 정리하지 않는다. */
    public PostCopyState fail() {
        if (session == null) throw new IllegalStateException("begin first");
        if (!committed) return abort();
        if (state != PostCopyState.DONE) state = PostCopyState.FAILED;
        return state;
    }

    /** 같은 source 데이터와 fence가 복구된 뒤 진행을 재개한다. */
    public PostCopyState resume() {
        if (state != PostCopyState.FAILED || !source.available(tablet()) || !source.frozen(tablet())) {
            throw new IllegalStateException("source is not safely recoverable");
        }
        state = PostCopyState.PULLING;
        return state;
    }

    /** source는 begin 전/commit 전 abort 때만 쓰기를 재개한다. */
    public boolean acceptsWrite() {
        return !committed && (state == PostCopyState.PENDING || state == PostCopyState.ABORTED)
                && (session == null || !source.frozen(tablet()));
    }

    /** 완료/장애 검증용 불변 destination 데이터. 실제 client는 execute를 사용한다. */
    public Map<Bytes, Bytes> data() {
        return java.util.Collections.unmodifiableMap(new java.util.LinkedHashMap<>(data));
    }
}
