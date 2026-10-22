package io.openkache.core.replication;

import io.openkache.core.runtime.Mailbox;

/**
 * 넘겨받은 작업을 호출 thread에서 즉시 실행하는 {@link Mailbox}.
 *
 * <p>테스트 thread 하나가 세 노드의 owner 역할을 모두 맡게 되어 메시지 전달·적용·응답이 결정적인 순서로 일어난다. 엔진 코드는
 * 그대로 mailbox에 작업을 넘기므로, 실제 {@code OwnerWorker}로 바꿔도 동작은 같아야 한다. 가득 차는 일이 없으므로
 * {@code BUSY} 경로는 이 mailbox로 재현할 수 없다.
 */
public final class DirectMailbox implements Mailbox {

    @Override
    public boolean offer(Runnable task) {
        task.run();
        return true;
    }

    @Override
    public int capacity() {
        return Integer.MAX_VALUE;
    }
}
