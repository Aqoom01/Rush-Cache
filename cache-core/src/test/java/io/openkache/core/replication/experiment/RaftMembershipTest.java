package io.openkache.core.replication.experiment;

import io.openkache.core.api.*;
import io.openkache.core.failure.*;
import io.openkache.core.model.*;
import io.openkache.core.replication.raft.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class RaftMembershipTest {
    private void signal(RaftCluster c,int observer,int member,MemberStatus status) {
        c.nodes.get(observer).onMembershipChanged(new MembershipEvent(new NodeId(member),
                status==MemberStatus.DEAD?MemberStatus.SUSPECT:MemberStatus.ALIVE,status,0,c.now()));
    }
    @Test void suspectWaitsButDeadStartsNormalQuorumElectionAndPreservesData() {
        try(var c=new RaftCluster(1,100,2,0,true)) {
            int old=c.readyLeader(1000);
            var bootstrap=c.nodes.get(old).electionEvents();
            assertTrue(bootstrap.stream().anyMatch(e->e.kind().equals("STARTED")));
            assertTrue(bootstrap.stream().anyMatch(e->e.kind().equals("BECAME_LEADER")));
            assertThrows(UnsupportedOperationException.class,()->bootstrap.clear());
            var written=c.set(old,1,"before","value",500); c.advance(30);
            assertEquals(ResultStatus.OK,written.join().status());
            long term=c.nodes.get(old).observation().term();
            int next=(old+1)%3;
            c.crash(old); c.advance(5);
            signal(c,next,old,MemberStatus.SUSPECT); c.advance(300);
            assertEquals(-1,c.leader());
            assertEquals(term,c.nodes.get(next).observation().term());
            signal(c,next,old,MemberStatus.DEAD);
            signal(c,next,old,MemberStatus.DEAD);
            int winner=c.readyLeader(300);
            assertEquals(next,winner);
            assertEquals(term+1,c.nodes.get(winner).observation().term());
            assertEquals("SWIM_DEAD",c.nodes.get(winner).failureObservation().trigger());
            var journal=c.nodes.get(winner).electionEvents().stream().filter(e->e.term()==term+1).toList();
            assertEquals(java.util.List.of("STARTED","BECAME_LEADER"),journal.stream().map(e->e.kind()).toList());
            assertTrue(journal.stream().allMatch(e->e.origin().equals("SWIM_DEAD")));
            assertTrue(journal.get(1).atNanos()>=journal.get(0).atNanos());
            assertEquals(0,c.nodes.get(winner).droppedElectionEvents());
            assertEquals(new NodeId(old),c.nodes.get(winner).failureObservation().failedLeader());
            assertEquals(Bytes.utf8("value"),c.local(winner,"before").value().orElseThrow());
        }
    }
    @Test void tcpHeartbeatOverridesUdpFalsePositiveAndWrongMemberDoesNotTriggerElection() {
        try(var c=new RaftCluster(1,100,3,0,true)) {
            int leader=c.readyLeader(1000),follower=(leader+1)%3;
            c.advance(30);
            long term=c.nodes.get(leader).observation().term();
            signal(c,follower,(leader+2)%3,MemberStatus.DEAD);
            for(int i=0;i<40;i++) {
                signal(c,follower,leader,MemberStatus.DEAD);
                c.advance(20);
            }
            assertEquals(leader,c.leader());
            assertEquals(term,c.nodes.get(follower).observation().term());
        }
    }
    @Test void earlyDeadWaitsForHeartbeatSilenceAndCannotBypassQuorum() {
        try(var c=new RaftCluster(1,100,4,0,true)) {
            int old=c.readyLeader(1000),candidate=(old+1)%3;
            c.advance(30); c.isolate(candidate); c.advance(2);
            long term=c.nodes.get(candidate).observation().term();
            signal(c,candidate,old,MemberStatus.DEAD); c.advance(40);
            assertEquals(term,c.nodes.get(candidate).observation().term());
            c.advance(100);
            assertEquals(RaftEngine.Role.CANDIDATE,c.nodes.get(candidate).observation().role());
            var request=c.set(candidate,9,"blocked","value",100); c.advance(120);
            assertNotEquals(ResultStatus.OK,request.join().status());
        }
    }
    @Test void missingDeadUsesLongWatchdog() {
        try(var c=new RaftCluster(1,100,5,0,true)) {
            int old=c.readyLeader(1000); c.advance(30); c.crash(old);
            c.advance(500); assertEquals(-1,c.leader());
            int next=c.readyLeader(1000);
            assertEquals("FALLBACK",c.nodes.get(next).failureObservation().trigger());
        }
    }
    @Test void delayedSignalFromPriorTermIsIgnoredAfterNewLeaderHeartbeat() {
        try(var c=new RaftCluster(1,100,6,0,true)) {
            int old=c.readyLeader(1000); c.advance(30);
            long stale=c.now(); int next=(old+1)%3;
            c.crash(old); c.advance(5); signal(c,next,old,MemberStatus.DEAD);
            c.advance(300); int winner=c.readyLeader(500); c.advance(30);
            int observer=3-old-winner;
            long term=c.nodes.get(observer).observation().term();
            c.nodes.get(observer).onMembershipChanged(new MembershipEvent(new NodeId(winner),
                    MemberStatus.SUSPECT,MemberStatus.DEAD,0,stale));
            c.advance(300);
            assertEquals(winner,c.leader());
            assertEquals(term,c.nodes.get(observer).observation().term());
        }
    }
}
