package io.openkache.core.runtime;

/**
 * owner worker에게 작업을 넘기는 크기 제한 큐.
 *
 * <p>{@link #offer(Runnable)}는 block하지 않는다. 큐가 가득 찼거나 닫혔으면 즉시 {@code false}를 반환하므로, Netty
 * EventLoop에서 호출하고 실패 시 {@code BUSY}로 응답할 수 있다. 받아들인 작업은 owner worker thread에서 받아들인 순서대로
 * 하나씩 실행된다.
 */
public interface Mailbox {

    /** 작업을 큐에 넣는다. 넣었으면 {@code true}, 가득 찼거나 닫혔으면 {@code false}. */
    boolean offer(Runnable task);

    /** 실행 중인 작업을 제외하고 대기할 수 있는 최대 작업 수. */
    int capacity();
}
