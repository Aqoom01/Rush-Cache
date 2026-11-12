package io.openkache.core.replication.experiment;

import io.openkache.core.api.*;
import io.openkache.core.model.*;
import io.openkache.core.replication.raft.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class RaftTest {
    @Test void majorityCommitReadAndDuplicateIdentity() {
        try(var c=new RaftCluster(1,100,1,0)) {
            int l=c.readyLeader(1000),slow=(l+1)%3;
            c.isolate(slow);
            var f=c.set(l,1,"a","one",500); c.advance(20); assertEquals(ResultStatus.OK,f.join().status());
            var read=c.execute(l,2,new Command.Get(Bytes.utf8("a")),500); c.advance(20);
            assertEquals(Bytes.utf8("one"),read.join().value().orElseThrow());
            int length=c.nodes.get(l).observation().logLength();
            assertEquals(ResultStatus.OK,c.set(l,1,"a","one",500).join().status());
            assertEquals(length,c.nodes.get(l).observation().logLength());
            assertEquals(ResultStatus.BAD_REQUEST,c.set(l,1,"a","different",500).join().status());
        }
    }
    @Test void leaderCrashRetainsAcknowledgedWrite() {
        for(int seed=1;seed<=20;seed++) try(var c=new RaftCluster(2,100,seed,0)) {
            int l=c.readyLeader(1000); var f=c.set(l,1,"before","v",500); c.advance(30);
            assertEquals(ResultStatus.OK,f.join().status()); c.crash(l);
            int next=c.readyLeader(1500); assertNotEquals(l,next);
            var post=c.set(next,2,"after","v2",500); c.advance(30);
            assertEquals(ResultStatus.OK,post.join().status());
            for(int i=0;i<3;i++) if(i!=l) assertEquals(Bytes.utf8("v"),c.local(i,"before").value().orElseThrow());
        }
    }
    @Test void minorityLeaderCannotCommitAndConflictingSuffixIsReplaced() {
        try(var c=new RaftCluster(1,100,2,0)) {
            int old=c.readyLeader(1000); c.isolate(old);
            var orphan=c.set(old,1,"orphan","bad",100); c.advance(500);
            assertEquals(ResultStatus.TIMEOUT,orphan.join().status());
            int next=c.readyLeader(1000); assertNotEquals(old,next);
            var accepted=c.set(next,2,"winner","good",500); c.advance(20); assertEquals(ResultStatus.OK,accepted.join().status());
            c.heal(); c.advance(2500);
            for(int i=0;i<3;i++) {
                assertEquals(ResultStatus.MISS,c.local(i,"orphan").status());
                assertEquals(Bytes.utf8("good"),c.local(i,"winner").value().orElseThrow());
                assertEquals(c.nodes.get(next).logSnapshot().toCompletableFuture().join(),c.nodes.get(i).logSnapshot().toCompletableFuture().join());
            }
        }
    }
    @Test void staleCandidateLogIsRejectedAndTermHasOneVote() {
        try(var c=new RaftCluster(1,100,3,0)) {
            int l=c.readyLeader(1000),voter=(l+1)%3,other=(l+2)%3;
            var f=c.set(l,1,"committed","v",500); c.advance(30); assertEquals(ResultStatus.OK,f.join().status());
            long t=c.nodes.get(voter).observation().term()+1;
            List<RaftMessage.VoteReply> replies=new ArrayList<>();
            c.drop=e->{ if(e.from().value()==voter && e.message() instanceof RaftMessage.VoteReply r) replies.add(r); return true; };
            c.nodes.get(voter).onPeerMessage(new NodeId(l),new RaftMessage.Vote(t,0,0));
            assertFalse(replies.getLast().granted());
            var o=c.nodes.get(voter).observation(); var entries=c.nodes.get(voter).logSnapshot().toCompletableFuture().join();
            c.nodes.get(voter).onPeerMessage(new NodeId(l),new RaftMessage.Vote(t,o.logLength(),entries.getLast().term()));
            assertTrue(replies.getLast().granted());
            c.nodes.get(voter).onPeerMessage(new NodeId(other),new RaftMessage.Vote(t,o.logLength(),entries.getLast().term()));
            assertFalse(replies.getLast().granted());
        }
    }
    @Test void lostDuplicatedDelayedRepliesAndDeterministicReplay() {
        assertEquals(trace(91),trace(91));
    }
    private List<RaftEngine.Observation> trace(int seed) {
        try(var c=new RaftCluster(3,100,seed,1_048_576)) {
            int l=c.readyLeader(1000); c.duplicate=true;
            c.drop=e->e.message() instanceof RaftMessage.AppendReply;
            var pending=c.set(l,1,"late","v",100); c.advance(150);
            assertEquals(ResultStatus.TIMEOUT,pending.join().status());
            c.drop=e->false; c.advance(2200);
            var retry=c.set(c.readyLeader(1000),1,"late","v",500); c.advance(100);
            assertEquals(ResultStatus.OK,retry.join().status());
            for(int i=0;i<3;i++) assertEquals(Bytes.utf8("v"),c.local(i,"late").value().orElseThrow());
            return c.nodes.stream().map(RaftEngine::observation).toList();
        }
    }
    @Test void currentTermNoopCommitsInheritedEntryWithoutOriginalClientOk() {
        try(var c=new RaftCluster(1,100,7,0)) {
            int old=c.readyLeader(1000);
            c.drop=e->e.message() instanceof RaftMessage.AppendReply;
            var pending=c.set(old,1,"pending","v",500); c.advance(10);
            assertFalse(pending.isDone()); c.crash(old); assertEquals(ResultStatus.UNAVAILABLE,pending.join().status());
            c.drop=e->false; int l=c.readyLeader(1500); c.advance(30);
            assertEquals(Bytes.utf8("v"),c.local(l,"pending").value().orElseThrow());
            var entries=c.nodes.get(l).logSnapshot().toCompletableFuture().join();
            assertTrue(entries.getLast().request().isEmpty()); assertEquals(c.nodes.get(l).observation().term(),entries.getLast().term());
        }
    }
    @Test void oldTermCannotAdvanceCommitWithoutCurrentTermQuorum() {
        try(var c=new RaftCluster(1,100,8,0)) {
            int old=c.readyLeader(1000);
            c.drop=e->e.message() instanceof RaftMessage.AppendReply;
            c.set(old,1,"pending","v",500); c.advance(10); c.crash(old);
            c.drop=e->e.message() instanceof RaftMessage.Append a && a.entries().stream().anyMatch(x->x.request().isEmpty());
            c.advance(500); int l=c.leader(); assertTrue(l>=0);
            assertEquals(ResultStatus.MISS,c.local(l,"pending").status());
            c.drop=e->false; c.advance(2500);
            assertEquals(Bytes.utf8("v"),c.local(c.readyLeader(1000),"pending").value().orElseThrow());
        }
    }
    @Test void noQuorumReadsAlsoFailAndStaleEpochIsRejected() {
        try(var c=new RaftCluster(1,100,9,0)) {
            int l=c.readyLeader(1000); c.isolate(l);
            var f=c.execute(l,1,new Command.Count(),100); c.advance(120); assertEquals(ResultStatus.TIMEOUT,f.join().status());
            Result stale=c.nodes.get(l).execute(new Command.Count(),new RequestContext(new ClientId(4),new OperationId(3),RoutingVersion.ZERO,
                    new OwnershipEpoch(0),new Deadline(Long.MAX_VALUE))).toCompletableFuture().join();
            assertEquals(ResultStatus.STALE_EPOCH,stale.status());
        }
    }
}
