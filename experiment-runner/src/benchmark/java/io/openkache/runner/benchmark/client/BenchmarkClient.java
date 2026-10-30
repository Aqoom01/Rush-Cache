package io.openkache.runner.benchmark.client;

import io.openkache.runner.benchmark.LogicalRequest;
import java.util.concurrent.CompletionStage;

/** 논리 생성기와 전송 adapter 사이의 실험용 경계. 여러 thread에서 호출 가능하다. */
public interface BenchmarkClient extends AutoCloseable {
    /**
     * 요청을 제출한다. admission 거절/ID 오용은 failed stage이며 logical admitted에 포함하지 않는다.
     * 반환 stage의 toCompletableFuture copy를 cancel해도 요청을 취소하지 않는다.
     */
    CompletionStage<ClientOutcome> submit(LogicalRequest request);

    /** 명시적 logical 취소. 이미 보낸 요청의 실행/원격 취소를 보장하지 않는다. */
    boolean cancel(LogicalRequest.RequestId id);

    ClientAccounting.Snapshot snapshot();

    /** 미완료 요청을 CLOSED로 끝낸다. 이후 raw 응답은 성공으로 소급하지 않는다. */
    @Override void close();
}

