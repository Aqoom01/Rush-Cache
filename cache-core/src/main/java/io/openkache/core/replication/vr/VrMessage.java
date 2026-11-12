package io.openkache.core.replication.vr;

import io.openkache.core.api.Command;
import io.openkache.core.model.ClientId;
import io.openkache.core.model.OperationId;
import io.openkache.core.spi.PeerMessage;
import java.util.List;
import java.util.Objects;

/** VR 연구용 불변 메시지. op-number는 로그의 1-based 위치다. 전체 로그 전송은 상태 전달을 단순화한 선택이다. */
public sealed interface VrMessage extends PeerMessage {
    long view();
    /** 재시도 식별자는 client + operation. 같은 식별자를 다른 명령에 재사용하면 BAD_REQUEST다. */
    record Request(ClientId client, OperationId operation, Command command) {
        public Request { Objects.requireNonNull(client); Objects.requireNonNull(operation); Objects.requireNonNull(command); }
    }
    record Prepare(long view, List<Request> log, int commit) implements VrMessage {
        public Prepare { log = checked(view, log, commit); }
    }
    /** firstOp는 1-based 시작 위치다. 앞 로그가 없으면 적용하지 않고 상태를 요청한다. */
    record DeltaPrepare(long view, int firstOp, List<Request> entries, int commit) implements VrMessage {
        public DeltaPrepare {
            entries=List.copyOf(entries);
            if(view<0 || firstOp<1 || entries.isEmpty() || commit<0 || commit>(long)firstOp-1+entries.size())
                throw new IllegalArgumentException("invalid incremental prepare");
        }
    }
    /** 로그가 없는 heartbeat/확정 통지. commit은 마지막 확정 위치다. */
    record Commit(long view,int commit) implements VrMessage {
        public Commit { if(view<0 || commit<0) throw new IllegalArgumentException(); }
    }
    /** DOVIEWCHANGE 수신 확인. 전송 제어용이며 과반수 확정 ACK로 사용하지 않는다. */
    record ViewChangeAck(long view) implements VrMessage {
        public ViewChangeAck { if(view<0) throw new IllegalArgumentException(); }
    }
    record PrepareOk(long view, int length) implements VrMessage {
        public PrepareOk { if (view < 0 || length < 0) throw new IllegalArgumentException(); }
    }
    record StartViewChange(long view) implements VrMessage {
        public StartViewChange { if (view < 0) throw new IllegalArgumentException(); }
    }
    record DoViewChange(long view, long lastNormal, List<Request> log, int commit) implements VrMessage {
        public DoViewChange {
            log = checked(view, log, commit);
            if (lastNormal < 0 || lastNormal >= view) throw new IllegalArgumentException("invalid last normal view");
        }
    }
    record StartView(long view, List<Request> log, int commit) implements VrMessage {
        public StartView { log = checked(view, log, commit); }
    }
    record GetState(long view) implements VrMessage {
        public GetState { if (view < 0) throw new IllegalArgumentException(); }
    }
    private static List<Request> checked(long view, List<Request> log, int commit) {
        if (view < 0 || commit < 0 || commit > log.size()) throw new IllegalArgumentException("invalid view/log/commit");
        return List.copyOf(log);
    }
}
