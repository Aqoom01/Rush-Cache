package io.openkache.core.replication.raft;

import io.openkache.core.api.Command;
import io.openkache.core.model.ClientId;
import io.openkache.core.model.OperationId;
import io.openkache.core.spi.PeerMessage;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** 고정 구성 Raft RPC. 인덱스는 1부터 시작하며 0은 빈 로그의 sentinel이다. */
public sealed interface RaftMessage extends PeerMessage {
    long term();
    record Request(ClientId client, OperationId operation, Command command) {
        public Request { Objects.requireNonNull(client); Objects.requireNonNull(operation); Objects.requireNonNull(command); }
    }
    /** 빈 request는 새 리더가 현재 term의 commit을 확립하는 no-op이다. */
    record Entry(long term, Optional<Request> request) {
        public Entry { if(term<1) throw new IllegalArgumentException(); Objects.requireNonNull(request); }
    }
    record Vote(long term, int lastIndex, long lastTerm) implements RaftMessage {
        public Vote { check(term,lastIndex,lastTerm); }
    }
    record VoteReply(long term, boolean granted) implements RaftMessage {
        public VoteReply { if(term<0) throw new IllegalArgumentException(); }
    }
    record Append(long term, long rpc, int prevIndex, long prevTerm, List<Entry> entries, int commit) implements RaftMessage {
        public Append {
            check(term,prevIndex,prevTerm); entries=List.copyOf(entries);
            if(rpc<0 || commit<0 || entries.stream().anyMatch(e->e.term()>term)) throw new IllegalArgumentException();
        }
    }
    /** matched는 성공 시 이번 RPC가 검증한 끝, 실패 시 follower의 마지막 로그 위치다. */
    record AppendReply(long term, long rpc, boolean success, int matched) implements RaftMessage {
        public AppendReply { if(term<0 || rpc<0 || matched<0) throw new IllegalArgumentException(); }
    }
    private static void check(long term,int index,long logTerm) {
        if(term<0 || index<0 || logTerm<0 || logTerm>term || (index==0)!=(logTerm==0)) throw new IllegalArgumentException();
    }
}
