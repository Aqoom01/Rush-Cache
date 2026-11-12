package io.openkache.server.benchmark.ionia_second;

import io.openkache.core.api.*;
import io.openkache.core.model.NodeId;
import io.openkache.core.replication.ionia.IoniaEngine;
import io.openkache.core.replication.ionia.IoniaClientTransport;
import io.openkache.core.replication.ionia_second.IoniaLeaderReadClient;
import io.openkache.core.runtime.TimerEvent;
import io.openkache.core.spi.TimeSource;
import io.openkache.runner.benchmark.LogicalRequest;
import io.openkache.runner.benchmark.client.*;
import io.openkache.server.benchmark.ionia.*;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletionStage;

/**
 * 실제 V01 Leader-read client를 공통 benchmark 수명주기에 연결한다. public 호출은 thread-safe다.
 * O00의 deadline/ID/retry/관찰/상한/close를 재사용하며 데이터 GET의 현재 Leader 추종은 V01 client가 수행한다.
 */
public final class V01BenchmarkClient implements BenchmarkClient {
    /** V01에는 고정 readReplica/인증 해제 옵션이 없다. leader는 초기 탐색 기준이며 이후 View를 따른다. */
    public record Settings(String runId, String loadSlot, int streamCount, long wireClientBase,
            List<NodeId> replicas, NodeId leader, long totalTimeoutNanos,
            int maxRounds, long retryBackoffNanos, int maxInFlight, int maxOutstandingRpc,
            long timerPeriodNanos) {
        public Settings {
            replicas = List.copyOf(replicas);
            // 공유 실행 제한/namespace 검증만 재사용한다. 아래 readReplica 자리의 leader는 V01 라우팅에 사용하지 않는다.
            new O00BenchmarkClient.Settings(runId, loadSlot, streamCount, wireClientBase, replicas,
                    leader, leader, totalTimeoutNanos, maxRounds, retryBackoffNanos, maxInFlight,
                    maxOutstandingRpc, timerPeriodNanos);
        }
        O00BenchmarkClient.Settings runtimeSettings() {
            return new O00BenchmarkClient.Settings(runId, loadSlot, streamCount, wireClientBase, replicas,
                    leader, leader, totalTimeoutNanos, maxRounds, retryBackoffNanos, maxInFlight,
                    maxOutstandingRpc, timerPeriodNanos);
        }
    }
    private final O00BenchmarkClient runtime;

    public V01BenchmarkClient(Settings settings, IoniaClientTransport transport, TimeSource time) {
        Objects.requireNonNull(settings);
        runtime = new O00BenchmarkClient(settings.runtimeSettings(), transport, time, (id, observed, clock) -> {
            var client = new IoniaLeaderReadClient(id, settings.replicas(), settings.leader(), 0, observed, clock);
            return new IoniaBenchmarkSession() {
                @Override public CompletionStage<IoniaEngine.ViewInfo> refreshView() { return client.refreshView(); }
                @Override public CompletionStage<Result> execute(Command command, RequestContext context) { return client.execute(command, context); }
                @Override public void onTimer(TimerEvent event) { client.onTimer(event); }
                @Override public void close() { client.close(); }
            };
        });
    }
    @Override public CompletionStage<ClientOutcome> submit(LogicalRequest request) { return runtime.submit(request); }
    @Override public boolean cancel(LogicalRequest.RequestId id) { return runtime.cancel(id); }
    @Override public ClientAccounting.Snapshot snapshot() { return runtime.snapshot(); }
    public O00BenchmarkClient.Diagnostics diagnostics() { return runtime.diagnostics(); }
    @Override public void close() { runtime.close(); }
    O00BenchmarkClient runtime() { return runtime; }
}
