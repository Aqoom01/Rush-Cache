package io.openkache.core.replication.experiment;

import io.openkache.core.api.Command;
import io.openkache.core.replication.primarybackup.*;
import io.openkache.core.replication.vr.VrMessage;
import io.openkache.core.replication.raft.RaftMessage;
import io.openkache.core.replication.chain.ChainMessage;
import io.openkache.core.replication.rose.RoseMessage;
import io.openkache.core.spi.PeerMessage;

/**
 * 실제 wire codec이 아닌 명시적 논리 인코딩 v1. 공통 frame 32B(type/from/to/view/length),
 * 정수 ID 8B, 길이 4B, command tag 1B, key/value는 실제 Bytes 길이를 사용한다.
 */
final class LogicalMessageSize {
    record Size(long bytes, boolean data) {}
    private LogicalMessageSize() {}
    static Size of(PeerMessage message) {
        if(message instanceof RoseMessage.Append m) return new Size(52+command(m.request().command()),true);
        if(message instanceof RoseMessage.Applied) return new Size(36,false);
        if(message instanceof ChainMessage.Forward m) return new Size(52+command(m.request().command()),true);
        if(message instanceof ChainMessage.VersionQuery m) return new Size(44L+m.key().size(),false);
        if(message instanceof ChainMessage.VersionReply m) return new Size(48L+m.key().size(),false);
        if(message instanceof ChainMessage.Ack || message instanceof ChainMessage.Progress) return new Size(36,false);
        if(message instanceof ChainMessage.Completed m) return new Size(65+m.result().value().map(v->(long)v.size()).orElse(0L),false);
        if(message instanceof RaftMessage.Vote) return new Size(44,false);
        if(message instanceof RaftMessage.VoteReply) return new Size(33,false);
        if(message instanceof RaftMessage.AppendReply) return new Size(45,false);
        if(message instanceof RaftMessage.Append m) {
            long bytes=60;
            for(var e:m.entries()) bytes+=9+(e.request().isPresent()?16+command(e.request().orElseThrow().command()):0);
            return new Size(bytes,!m.entries().isEmpty());
        }
        if (message instanceof PrimaryBackupMessage.Replicate m) return new Size(32+8+command(m.command()),true);
        if (message instanceof PrimaryBackupMessage.ReplicateAck) return new Size(40,false);
        if (message instanceof CrashOnlyPrimaryBackupEngine.Control) return new Size(36,false);
        if (message instanceof VrMessage.Prepare m) return new Size(44+log(m.log()),true);
        if (message instanceof VrMessage.DeltaPrepare m) return new Size(52+log(m.entries()),true);
        if (message instanceof VrMessage.Commit) return new Size(40,false);
        if (message instanceof VrMessage.ViewChangeAck) return new Size(32,false);
        if (message instanceof VrMessage.StartView m) return new Size(44+log(m.log()),true);
        if (message instanceof VrMessage.DoViewChange m) return new Size(52+log(m.log()),true);
        if (message instanceof VrMessage.PrepareOk) return new Size(40,false);
        if (message instanceof VrMessage.StartViewChange || message instanceof VrMessage.GetState) return new Size(32,false);
        throw new IllegalArgumentException("unmodeled message: "+message.getClass());
    }
    private static long log(java.util.List<VrMessage.Request> requests) {
        long bytes=0;
        for (var r:requests) bytes=Math.addExact(bytes,16+command(r.command()));
        return bytes;
    }
    private static long command(Command command) {
        return switch(command) {
            case Command.Set s -> 9L+s.key().size()+s.value().size();
            case Command.Get g -> 5L+g.key().size();
            case Command.Delete d -> 5L+d.key().size();
            case Command.Exists e -> 5L+e.key().size();
            case Command.Count ignored -> 1;
        };
    }
}
