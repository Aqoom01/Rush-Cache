package io.openkache.server.benchmark.variants;

import io.openkache.core.api.*;
import io.openkache.core.replication.ionia.*;
import io.openkache.core.runtime.TimerEvent;
import io.openkache.server.benchmark.ionia.IoniaBenchmarkSession;
import java.util.concurrent.CompletionStage;
import java.util.function.*;

/** 실제 파생 client의 비차단 네 메서드만 연결한다. 복제 정책/결과를 재구현하지 않는다. */
public final class VariantSessions {
    private VariantSessions() {}
    public static IoniaBenchmarkSession connect(Supplier<CompletionStage<IoniaEngine.ViewInfo>> refresh,
            BiFunction<Command, RequestContext, CompletionStage<Result>> execute,
            Consumer<TimerEvent> timer, Runnable close) {
        return new IoniaBenchmarkSession() {
            @Override public CompletionStage<IoniaEngine.ViewInfo> refreshView() { return refresh.get(); }
            @Override public CompletionStage<Result> execute(Command command, RequestContext context) { return execute.apply(command, context); }
            @Override public void onTimer(TimerEvent event) { timer.accept(event); }
            @Override public void close() { close.run(); }
        };
    }
}
