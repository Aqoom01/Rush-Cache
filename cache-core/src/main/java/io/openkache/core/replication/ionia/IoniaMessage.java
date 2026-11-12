package io.openkache.core.replication.ionia;

import io.openkache.core.api.Command;
import io.openkache.core.model.ClientId;
import io.openkache.core.model.OperationId;
import io.openkache.core.spi.PeerMessage;
import java.util.List;
import java.util.Objects;

/**
 * IONIA O00의 순서 합의·View Change·재시작 복구 메시지.
 *
 * <p>모든 메시지는 불변이며 thread 간 공유할 수 있다. 송수신 노드는 공통 {@code PeerEnvelope}가 표현한다. 이 타입은
 * 메시지 형식만 검증하며, 송신자가 leader인지, view가 현재 설정과 같은지, 로그가 기존 내용과 일치하는지는 이후 엔진이
 * 검사해야 한다. TCP codec과 클라이언트의 쓰기 기록 요청·ACK는 이 타입에 포함하지 않는다.
 */
public sealed interface IoniaMessage extends PeerMessage {

    /** 메시지가 속한 IONIA 세대 번호. 0 이상. */
    long view();

    /**
     * 순서 로그의 한 항목. 아직 순서가 확정되거나 KV에 적용되었다는 뜻은 아니다.
     *
     * @param index 순서 로그의 위치. 1 이상이며 KV의 로컬 valueVersion과 별개
     * @param clientId 요청을 발급한 클라이언트
     * @param operationId 클라이언트 내 논리 연산 ID. 재시도에서도 유지한다
     * @param command 적용할 SET 또는 DELETE. GET은 상태 변경 로그에 기록하지 않는다
     */
    record LogEntry(long index, ClientId clientId, OperationId operationId, Command command) {
        public LogEntry {
            requirePositiveIndex("index", index);
            Objects.requireNonNull(clientId, "clientId");
            Objects.requireNonNull(operationId, "operationId");
            Objects.requireNonNull(command, "command");
            if (!IoniaCommands.isWrite(command)) {
                throw new IllegalArgumentException("Only SET and DELETE can be added to the write log");
            }
        }
    }

    /**
     * leader가 follower에게 보내는 연속된 로그 항목의 순서 제안.
     *
     * @param view 메시지의 View 번호. 0 이상
     * @param entries 비어 있지 않고 index가 1씩 증가하는 항목들. 불변 복사본을 보관한다
     */
    record Prepare(long view, List<LogEntry> entries) implements IoniaMessage {
        public Prepare {
            requireView(view);
            entries = List.copyOf(Objects.requireNonNull(entries, "entries"));
            if (entries.isEmpty()) {
                throw new IllegalArgumentException("prepare entries must not be empty");
            }
            for (int i = 1; i < entries.size(); i++) {
                long previousIndex = entries.get(i - 1).index();
                if (previousIndex == Long.MAX_VALUE || entries.get(i).index() != previousIndex + 1) {
                    throw new IllegalArgumentException("prepare entries must have consecutive increasing indices");
                }
            }
        }
    }

    /**
     * follower가 순서 제안을 로그에 받아들였다는 확인. 클라이언트의 기록 ACK나 KV 적용 완료 응답과 다르다.
     *
     * @param view 메시지의 View 번호. 0 이상
     * @param lastLogIndex 이 위치까지 로그를 연속해서 수락했다는 뜻. 1 이상
     */
    record PrepareAck(long view, long lastLogIndex) implements IoniaMessage {
        public PrepareAck {
            requireView(view);
            requirePositiveIndex("lastLogIndex", lastLogIndex);
        }
    }

    /**
     * leader가 이 위치까지 순서를 확정했음을 알린다. 수신만으로 KV에 적용된 것은 아니다.
     *
     * @param view 메시지의 View 번호. 0 이상
     * @param commitIndex 순서가 확정된 연속 구간의 끝. 0이면 아직 확정된 항목이 없음
     */
    record Commit(long view, long commitIndex) implements IoniaMessage {
        public Commit {
            requireView(view);
            if (commitIndex < 0) {
                throw new IllegalArgumentException("commitIndex must be >= 0: " + commitIndex);
            }
        }
    }

    record Applied(long view, long appliedIndex) implements IoniaMessage {
        public Applied { requireView(view); if (appliedIndex < 0) throw new IllegalArgumentException("negative applied index"); }
    }
    record StartViewChange(long view) implements IoniaMessage {
        public StartViewChange { requireView(view); }
    }
    record DoViewChange(long view, IoniaRecovery.State state) implements IoniaMessage {
        public DoViewChange { requireView(view); Objects.requireNonNull(state, "state"); }
    }
    record StartView(long view, IoniaRecovery.State state) implements IoniaMessage {
        public StartView { requireView(view); Objects.requireNonNull(state, "state"); }
    }
    record RecoveryRequest(long view, java.util.UUID nonce) implements IoniaMessage {
        public RecoveryRequest { requireView(view); Objects.requireNonNull(nonce, "nonce"); }
    }
    /** Only the current normal leader includes state; sender membership is checked by the engine. */
    record RecoveryResponse(long view, java.util.UUID nonce, IoniaRecovery.State state) implements IoniaMessage {
        public RecoveryResponse { requireView(view); Objects.requireNonNull(nonce, "nonce"); }
    }

    private static void requireView(long view) {
        if (view < 0) {
            throw new IllegalArgumentException("view must be >= 0: " + view);
        }
    }

    private static void requirePositiveIndex(String name, long index) {
        if (index < 1) {
            throw new IllegalArgumentException(name + " must be >= 1: " + index);
        }
    }
}
