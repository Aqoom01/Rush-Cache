package io.openkache.core.replication.ionia;

import io.openkache.core.api.Command;
import io.openkache.core.model.ClientId;
import io.openkache.core.model.OperationId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * 한 노드의 메모리 쓰기 기록과 연속된 순서 로그. 모든 접근은 해당 Engine의 owner에서만 한다.
 *
 * <p>요청 수락, 순서 확정(commit), KV 적용은 서로 다른 상태다. 기록 압축, 재시작 후 내구성, 복구는 제공하지 않는다.
 */
public final class IoniaLog {

    /** 재시도에도 유지하는 논리 쓰기 식별자. */
    public record RequestId(ClientId clientId, OperationId operationId) {
        public RequestId {
            Objects.requireNonNull(clientId, "clientId");
            Objects.requireNonNull(operationId, "operationId");
        }
    }

    private final Map<RequestId, Command> recorded = new LinkedHashMap<>();
    private final List<IoniaMessage.LogEntry> ordered = new ArrayList<>();
    private final Map<RequestId, IoniaMessage.LogEntry> byRequest = new HashMap<>();
    private long commitIndex;
    private long appliedIndex;

    /** 쓰기를 기록한다. 같은 ID/내용은 무시하고 같은 ID의 다른 내용은 상태 변경 없이 거절한다. */
    public void record(ClientId clientId, OperationId operationId, Command command) {
        RequestId id = new RequestId(clientId, operationId);
        requireWrite(command);
        Command existing = recorded.get(id);
        if (existing != null && !existing.equals(command)) {
            throw new IllegalArgumentException("request ID is already used for another command");
        }
        recorded.putIfAbsent(id, command);
    }

    /** leader가 쓰기에 순서를 부여한다. 재시도면 기존 항목을 반환한다. index는 1부터 시작한다. */
    public IoniaMessage.LogEntry order(ClientId clientId, OperationId operationId, Command command) {
        record(clientId, operationId, command);
        RequestId id = new RequestId(clientId, operationId);
        IoniaMessage.LogEntry existing = byRequest.get(id);
        if (existing != null) {
            return existing;
        }
        IoniaMessage.LogEntry entry =
                new IoniaMessage.LogEntry(lastLogIndex() + 1, clientId, operationId, command);
        ordered.add(entry);
        byRequest.put(id, entry);
        return entry;
    }

    /**
     * follower가 순서 제안을 수락한다. batch 전체 검사 후 추가하므로 실패 시 부분 기록하지 않는다.
     * 일치하는 항목의 재전송은 허용하고 중간 누락/충돌/동일 요청의 중복 순서 배정은 거절한다.
     */
    public void accept(IoniaMessage.Prepare prepare) {
        Objects.requireNonNull(prepare, "prepare");
        long next = lastLogIndex() + 1;
        Map<RequestId, Long> incoming = new HashMap<>();
        for (IoniaMessage.LogEntry entry : prepare.entries()) {
            RequestId id = idOf(entry);
            Command known = recorded.get(id);
            if (known != null && !known.equals(entry.command())) {
                throw new IllegalArgumentException("request payload conflicts with the local record");
            }
            IoniaMessage.LogEntry assigned = byRequest.get(id);
            if (assigned != null && !assigned.equals(entry)) {
                throw new IllegalArgumentException("request already has another log position");
            }
            if (incoming.putIfAbsent(id, entry.index()) != null) {
                throw new IllegalArgumentException("request occurs twice in prepare");
            }
            if (entry.index() < next) {
                if (!entry.equals(ordered.get(Math.toIntExact(entry.index() - 1)))) {
                    throw new IllegalArgumentException("ordered log conflict");
                }
            } else if (entry.index() == next) {
                next++;
            } else {
                throw new IllegalArgumentException("ordered log has a gap");
            }
        }
        for (IoniaMessage.LogEntry entry : prepare.entries()) {
            recorded.putIfAbsent(idOf(entry), entry.command());
            if (entry.index() > lastLogIndex()) {
                ordered.add(entry);
                byRequest.put(idOf(entry), entry);
            }
        }
    }

    /** 보유한 연속 구간만 commit한다. 오래된 commit 재전송은 상태를 되돌리지 않는다. */
    public void commitThrough(long index) {
        if (index < 0 || index > lastLogIndex()) {
            throw new IllegalArgumentException("commit index is outside the local log");
        }
        commitIndex = Math.max(commitIndex, index);
    }

    /** KV 적용 성공 후 호출한다. commit 이전 적용과 연속 적용 위치 건너뛰기를 거절한다. */
    public void markApplied(long index) {
        if (index < 1 || index > commitIndex) {
            throw new IllegalArgumentException("only committed entries may be applied");
        }
        if (index <= appliedIndex) {
            return;
        }
        if (index != appliedIndex + 1) {
            throw new IllegalArgumentException("applied index must be a contiguous prefix");
        }
        appliedIndex = index;
    }

    /** index 위치의 항목. 없는 항목은 Optional.empty(). owner에서만 읽는다. */
    public Optional<IoniaMessage.LogEntry> entry(long index) {
        if (index < 1 || index > lastLogIndex()) {
            return Optional.empty();
        }
        return Optional.of(ordered.get(Math.toIntExact(index - 1)));
    }

    /** firstIndex부터 마지막 항목까지의 불변 복사본. 재전송에 사용한다. */
    public List<IoniaMessage.LogEntry> entriesFrom(long firstIndex) {
        if (firstIndex < 1 || firstIndex > lastLogIndex() + 1) {
            throw new IllegalArgumentException("first index is outside the local log");
        }
        return List.copyOf(ordered.subList(Math.toIntExact(firstIndex - 1), ordered.size()));
    }

    /** Pending durability records in local arrival order; finalized requests remain in the dedup table. */
    public List<IoniaMessage.LogEntry> durabilityEntries() {
        List<IoniaMessage.LogEntry> entries = new ArrayList<>();
        recorded.forEach((id, command) -> {
            IoniaMessage.LogEntry orderedEntry = byRequest.get(id);
            if (orderedEntry == null || orderedEntry.index() > appliedIndex)
                entries.add(new IoniaMessage.LogEntry(entries.size() + 1L, id.clientId(), id.operationId(), command));
        });
        return List.copyOf(entries);
    }

    /** 아직 순서가 없는 기록까지 포함한 논리 쓰기 수. */
    public int recordedCount() {
        return recorded.size();
    }

    /** 연속된 순서 로그의 끝. 0은 빈 로그다. */
    public long lastLogIndex() {
        return ordered.size();
    }

    /** 확정된 연속 구간의 끝. */
    public long commitIndex() {
        return commitIndex;
    }

    /** KV에 실제 적용한 연속 구간의 끝. */
    public long appliedIndex() {
        return appliedIndex;
    }

    private static RequestId idOf(IoniaMessage.LogEntry entry) {
        return new RequestId(entry.clientId(), entry.operationId());
    }

    private static void requireWrite(Command command) {
        Objects.requireNonNull(command, "command");
        if (command instanceof Command.Get) {
            throw new IllegalArgumentException("GET must not be added to the write log");
        }
    }
}
