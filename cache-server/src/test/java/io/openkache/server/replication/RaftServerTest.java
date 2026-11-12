package io.openkache.server.replication;

import java.nio.charset.StandardCharsets;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import static io.openkache.server.replication.PbServerTest.*;
import static org.junit.jupiter.api.Assertions.*;

class RaftServerTest {
    private static String state(String host,int peer) throws Exception {
        return new String(request(host,peer,35,3,new byte[0]).value(),StandardCharsets.UTF_8);
    }
    private static long number(String json,String field) {
        var m=java.util.regex.Pattern.compile("\""+field+"\":(-?\\d+)").matcher(json); assertTrue(m.find()); return Long.parseLong(m.group(1));
    }
    private static int leader(List<String> hosts,int peer,int excluded) throws Exception {
        long end=System.nanoTime()+8_000_000_000L;
        while(System.nanoTime()<end) {
            for(int i=0;i<3;i++) if(i!=excluded) {
                String s=state(hosts.get(i),peer);
                if(s.contains("\"role\":\"LEADER\"") && number(s,"readyNs")>=0 && number(s,"applied")==number(s,"logLength")) return i;
            }
            Thread.sleep(20);
        }
        throw new AssertionError("Raft election not ready");
    }
    @Test void electReplicateDeduplicateCrashAndReelect() throws Exception {
        int client=port(),peer=port(); while(client==peer) peer=port();
        List<String> hosts=List.of("127.0.0.1","127.0.0.2","127.0.0.3"); var nodes=new ArrayList<PbServer>();
        try {
            for(int i=0;i<3;i++) nodes.add(PbServer.start(new PbNodeConfig(i,hosts,client,peer,0,50,700,10,"RAFT",1)));
            int old=leader(hosts,peer,-1); long term=number(state(hosts.get(old),peer),"epoch");
            assertEquals(0,request(hosts.get(old),client,2,1,command(1,term,"before","v")).status());
            assertEquals(0,request(hosts.get(old),client,2,1,command(1,term,"before","v")).status());
            assertEquals(2,request(hosts.get(old),client,2,1,command(1,term,"before","different")).status());
            nodes.get(old).close(); int next=leader(hosts,peer,old); String s=state(hosts.get(next),peer);
            long nextTerm=number(s,"epoch"); assertTrue(nextTerm>term);
            assertEquals(nextTerm,number(s,"electionTerm"));
            assertTrue(s.contains("\"raftElectionEvents\":["),s);
            assertTrue(s.contains("\"kind\":\"STARTED\""),s);
            assertTrue(s.contains("\"kind\":\"BECAME_LEADER\""),s);
            assertEquals(0,number(s,"raftElectionEventsDropped"));
            assertTrue(number(s,"readyNs")>=number(s,"detectedNs"));
            assertEquals(0,request(hosts.get(next),client,2,1,command(2,nextTerm,"after","new")).status());
            assertEquals(0,request(hosts.get(next),client,2,1,command(1,nextTerm,"before","v")).status());
            assertEquals(5,request(hosts.get(next),client,2,1,command(3,term,"stale","v")).status());
            for(int i=0;i<3;i++) if(i!=old) assertArrayEquals("v".getBytes(StandardCharsets.UTF_8),request(hosts.get(i),peer,36,3,"before".getBytes(StandardCharsets.UTF_8)).value());
        } finally { for(var node:nodes) node.close(); }
    }
    @ParameterizedTest
    @ValueSource(strings={"SWIM","SWIM_LIFEGUARD"})
    void membershipDetectsActualLeaderAndRaftReelects(String detector) throws Exception {
        int client=port(),peer=port(),failure=port();
        while(client==peer) peer=port();
        while(failure==client || failure==peer) failure=port();
        List<String> hosts=List.of("127.0.0.1","127.0.0.2","127.0.0.3"); var nodes=new ArrayList<PbServer>();
        try {
            for(int i=0;i<3;i++) nodes.add(PbServer.start(new PbNodeConfig(i,hosts,client,peer,0,50,400,10,
                    "RAFT",7,"INCREMENTAL","TAIL_ONLY",0,1000,detector,failure,100,25,25,200,1000,8,2,1)));
            int old=leader(hosts,peer,-1);
            long term=number(state(hosts.get(old),peer),"epoch");
            assertEquals(0,request(hosts.get(old),client,2,1,command(501,term,"fd-before","preserved")).status());
            nodes.get(old).close();
            int next=leader(hosts,peer,old); String s=state(hosts.get(next),peer);
            long nextTerm=number(s,"epoch");
            assertTrue(nextTerm>term,s);
            assertTrue(s.contains("\"failureDetector\":\""+detector+"\""),s);
            assertTrue(s.contains("\"raftElectionOrigin\":\"SWIM_DEAD\""),s);
            assertEquals(old,number(s,"raftFailedLeader"));
            assertTrue(number(s,"raftFdDeadNs")>=0,s);
            assertTrue(number(s,"detectedNs")>=number(s,"raftFdDeadNs"),s);
            assertEquals(0,request(hosts.get(next),client,2,1,command(502,nextTerm,"fd-after","new")).status());
            assertArrayEquals("preserved".getBytes(StandardCharsets.UTF_8),
                    request(hosts.get(next),client,1,1,command(503,nextTerm,"fd-before",null)).value());
            assertEquals(5,request(hosts.get(next),client,2,1,command(504,term,"stale","v")).status());
        } finally { for(var node:nodes) node.close(); }
    }
}
