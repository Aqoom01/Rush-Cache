package io.openkache.runner.benchmark;

import io.openkache.core.api.Command;
import java.util.Objects;

/**
 * 실제 송신 전의 논리 요청. 값과 바이트열은 불변이며 thread 간 공유 가능하다.
 * globalIndex는 phase 안의 0-based 위치, sequence는 해당 stream 안의 0-based 위치다.
 */
public record LogicalRequest(
        TracePhase phase, long globalIndex, int streamId, long clientId,
        long sequence, Command command) {
    public LogicalRequest {
        Objects.requireNonNull(phase, "phase");
        Objects.requireNonNull(command, "command");
        if (globalIndex < 0 || streamId < 0 || clientId < 0 || sequence < 0) {
            throw new IllegalArgumentException("request indices must be nonnegative");
        }
    }

    /** 실행·부하 slot과 결합한 ID. retry는 이 ID와 command를 그대로 보존해야 한다. */
    public RequestId id(String runId, String loadSlot) {
        return new RequestId(runId, loadSlot, clientId, sequence);
    }

    /** 런타임 ID는 canonical trace hash에 포함하지 않아 방식 간 동일 입력 비교를 유지한다. */
    public record RequestId(String runId, String loadSlot, long clientId, long sequence) {
        public RequestId {
            if (Objects.requireNonNull(runId, "runId").isBlank()
                    || Objects.requireNonNull(loadSlot, "loadSlot").isBlank()
                    || clientId < 0 || sequence < 0) {
                throw new IllegalArgumentException("invalid request ID");
            }
        }
    }
}

