package io.openkache.server.benchmark.ionia;

import io.openkache.core.api.*;
import io.openkache.core.model.ClientId;
import io.openkache.core.replication.ionia.*;
import io.openkache.core.runtime.TimerEvent;
import io.openkache.core.spi.TimeSource;
import java.util.concurrent.CompletionStage;

/**
 * 장기 benchmark 세션의 좁은 client 연결 경계. 복제/읽기 정책은 실제 원본 또는 파생 client가 소유한다.
 * 모든 호출은 비차단이며 adapter가 timer/총deadline/동일ID retry·close 수명주기를 소유한다.
 */
public interface IoniaBenchmarkSession extends AutoCloseable {
    CompletionStage<IoniaEngine.ViewInfo> refreshView();
    CompletionStage<Result> execute(Command command, RequestContext context);
    void onTimer(TimerEvent event);
    @Override void close();

    /** client ID마다 한 번 생성하며 반드시 전달받은 관찰 transport/time을 사용한다. */
    @FunctionalInterface
    interface Factory {
        IoniaBenchmarkSession open(ClientId clientId, IoniaClientTransport transport, TimeSource time);
    }
}
