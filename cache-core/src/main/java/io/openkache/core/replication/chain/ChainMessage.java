package io.openkache.core.replication.chain;

import io.openkache.core.api.Command;
import io.openkache.core.api.Result;
import io.openkache.core.model.ClientId;
import io.openkache.core.model.OperationId;
import io.openkache.core.spi.PeerMessage;
import java.util.Objects;

/** Chain 연구용 peer 메시지. epoch는 외부 master가 설치한 구성 번호, index는 1-based다. */
public sealed interface ChainMessage extends PeerMessage {
    long epoch();
    record Request(ClientId client,OperationId operation,Command command) {
        public Request {
            Objects.requireNonNull(client); Objects.requireNonNull(operation); Objects.requireNonNull(command);
            if(!(command instanceof Command.Set) && !(command instanceof Command.Delete)) throw new IllegalArgumentException("write required");
        }
    }
    record Forward(long epoch,int index,Request request) implements ChainMessage {
        public Forward { check(epoch,index); Objects.requireNonNull(request); }
    }
    /** successor의 적용 위치. 전송 창만 전진시키며 tail 확정으로 사용하지 않는다. */
    record Progress(long epoch,int applied) implements ChainMessage {
        public Progress { check(epoch,applied); }
    }
    /** tail에서 시작하여 역방향으로 전달하는 누적 확정 확인. */
    record Ack(long epoch,int committed) implements ChainMessage {
        public Ack { check(epoch,committed); }
    }
    /** CacheEngine future를 가진 head로 tail이 결과를 중계한다. 역방향 Ack와 별개다. */
    record Completed(long epoch,int index,ClientId client,OperationId operation,Result result) implements ChainMessage {
        public Completed { check(epoch,index); Objects.requireNonNull(client); Objects.requireNonNull(operation); Objects.requireNonNull(result); }
    }
    private static void check(long epoch,int position) {
        if(epoch<1 || position<0) throw new IllegalArgumentException();
    }
}
