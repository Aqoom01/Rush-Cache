package io.openkache.runner.benchmark.client;

import io.openkache.core.api.Command;
import io.openkache.core.api.Result;
import io.openkache.core.api.ResultStatus;
import io.openkache.runner.benchmark.LogicalRequest;
import java.util.List;
import java.util.Objects;

/** 논리 요청의 불변 terminal 관찰. callback thread와 무관하게 공유 가능하다. */
public record ClientOutcome(LogicalRequest.RequestId id, Operation operation, Category category,
        Reason reason, Result result, long invokedNanos, long decidedNanos, long terminalNanos,
        long wireClientId, long wireOperationId, int rounds, List<Attempt> attempts) {
    public ClientOutcome {
        Objects.requireNonNull(id); Objects.requireNonNull(operation);
        Objects.requireNonNull(category); Objects.requireNonNull(reason);
        attempts = List.copyOf(attempts);
    }

    /** client API 첫 호출부터 terminal 관찰까지의 ns. histogram 집계는 후속이다. */
    public long elapsedNanos() { return terminalNanos - invokedNanos; }

    /** 원본 client 결과/timeout을 adapter가 관찰하기까지의 ns. publication 대기는 제외한다. */
    public long observedElapsedNanos() { return decidedNanos - invokedNanos; }

    /** 미응답이어도 호출이 시작되었으면 실제 반영 가능성을 배제하지 않는다. */
    public boolean executionUnknown() { return category == Category.NO_RESPONSE && !attempts.isEmpty(); }

    public enum Category { SUCCESS, ERROR_RESPONSE, NO_RESPONSE }
    public enum Reason {
        SUCCESS, CLIENT_ERROR, CLIENT_TIMEOUT, DEADLINE, CANCELLED, CLOSED,
        DISCOVERY_FAILED, CLIENT_EXCEPTION, MALFORMED_RESULT
    }
    public enum Operation {
        GET, SET, DELETE;
        public static Operation of(Command command) {
            return switch (command) {
                case Command.Get ignored -> GET;
                case Command.Set ignored -> SET;
                case Command.Delete ignored -> DELETE;
            };
        }
    }

    /**
     * 원본 client.execute 호출 한 번. protocol RPC/message 개수와 별개다.
     * endedNanos/status는 terminal 시점에 아직 미완료면 null이다.
     */
    public record Attempt(int number, long startedNanos, Long endedNanos,
            ResultStatus status, String errorKind) {}
}

