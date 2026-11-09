package io.openkache.server.replication;

import java.nio.charset.StandardCharsets;
import java.util.*;
import org.junit.jupiter.api.Test;
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
            assertTrue(number(s,"readyNs")>=number(s,"detectedNs"));
            assertEquals(0,request(hosts.get(next),client,2,1,command(2,nextTerm,"after","new")).status());
            assertEquals(0,request(hosts.get(next),client,2,1,command(1,nextTerm,"before","v")).status());
            assertEquals(5,request(hosts.get(next),client,2,1,command(3,term,"stale","v")).status());
            for(int i=0;i<3;i++) if(i!=old) assertArrayEquals("v".getBytes(StandardCharsets.UTF_8),request(hosts.get(i),peer,36,3,"before".getBytes(StandardCharsets.UTF_8)).value());
        } finally { for(var node:nodes) node.close(); }
    }
}
