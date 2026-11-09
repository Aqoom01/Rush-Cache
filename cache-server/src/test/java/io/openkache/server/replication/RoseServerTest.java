package io.openkache.server.replication;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import org.junit.jupiter.api.Test;
import static io.openkache.server.replication.ChainServerTest.*;
import static io.openkache.server.replication.PbServerTest.*;
import static org.junit.jupiter.api.Assertions.*;

/** 실제 TCP 3노드 Rosé 변형: bounded-lag 쓰기, 비Primary redirect, master freeze/repair/open 후 최장 prefix 승격. */
class RoseServerTest {
    static byte[] roseRepair(long epoch,int primary,int... survivors) {
        var b=ByteBuffer.allocate(1+8+8+4+8*survivors.length).put((byte)1).putLong(epoch).putLong(primary).putInt(survivors.length);
        for(int s:survivors) b.putLong(s); return b.array();
    }
    static byte[] roseFreeze(long epoch) { return ByteBuffer.allocate(9).put((byte)0).putLong(epoch).array(); }
    private static int applied(String status) {
        int i=status.indexOf("\"applied\":"); return Integer.parseInt(status.substring(i+10,status.indexOf(',',i)));
    }
    @Test void boundedLagWritesAndMasterPromotesLongestPrefixAfterPrimaryCrash() throws Exception {
        for(int limit:new int[]{0,4}) {
            int client=port(),peer=port(); while(client==peer) peer=port(); var nodes=new ArrayList<PbServer>();
            try {
                for(int i=0;i<3;i++) nodes.add(PbServer.start(new PbNodeConfig(i,HOSTS,client,peer,0,50,500,10,"ROSE",1,"INCREMENTAL","TAIL_ONLY",limit,200)));
                awaitRunning(peer,0,1,2);
                for(int i=0;i<8;i++) { var r=request(HOSTS.get(0),client,2,1,command(10+i,1,"k"+i,"v")); assertEquals(0,r.status()); assertEquals(1,r.epoch()); }
                var redirect=request(HOSTS.get(1),client,2,1,command(50,1,"x","v")); assertEquals(4,redirect.status()); assertEquals("redirect-node=0",redirect.detail());
                for(int i=0;i<8;i++) awaitLocal(peer,List.of(0,1,2),"k"+i,"v");
                assertTrue(status(0,peer).contains("\"limit\":"+limit+",\"state\":\"RUNNING\",\"primary\":0"));
                nodes.getFirst().close();
                for(int n:new int[]{1,2}) { var r=request(HOSTS.get(n),peer,38,3,roseFreeze(2)); assertEquals(0,r.status(),r.detail()); assertEquals("applied=8",r.detail()); }
                int primary=applied(status(1,peer))>=applied(status(2,peer))?1:2;
                for(int n:new int[]{1,2}) assertEquals(0,request(HOSTS.get(n),peer,38,3,roseRepair(2,primary,1,2)).status());
                long until=System.nanoTime()+5_000_000_000L; boolean repaired=false;
                while(System.nanoTime()<until && !repaired) {
                    repaired=status(primary,peer).contains("\"state\":\"REPAIRING\",\"primary\":"+primary+",\"logLength\":8,\"applied\":8,\"minAck\":8");
                    if(!repaired) Thread.sleep(20);
                }
                assertTrue(repaired,status(1,peer)+status(2,peer));
                assertArrayEquals(request(HOSTS.get(1),peer,37,3,new byte[0]).value(),request(HOSTS.get(2),peer,37,3,new byte[0]).value());
                for(int n:new int[]{1,2}) assertEquals(0,request(HOSTS.get(n),peer,38,3,open(2,8)).status());
                var after=request(HOSTS.get(primary),client,2,1,command(100,2,"after","new")); assertEquals(0,after.status()); assertEquals(2,after.epoch());
                assertEquals(5,request(HOSTS.get(primary),client,2,1,command(101,1,"stale","x")).status());
                int backup=primary==1?2:1;
                var r=request(HOSTS.get(backup),client,2,1,command(102,2,"direct","x")); assertEquals(4,r.status()); assertEquals("redirect-node="+primary,r.detail());
                for(int i=0;i<8;i++) awaitLocal(peer,List.of(1,2),"k"+i,"v");
                awaitLocal(peer,List.of(1,2),"after","new");
            } finally { for(var node:nodes) node.close(); }
        }
    }
    @Test void logDigestCoversLogsLargerThanOneFrame() throws Exception {
        int client=port(),peer=port(); while(client==peer) peer=port(); var nodes=new ArrayList<PbServer>();
        try {
            for(int i=0;i<3;i++) nodes.add(PbServer.start(new PbNodeConfig(i,HOSTS,client,peer,0,50,500,10,"ROSE",1,"INCREMENTAL","TAIL_ONLY",-1,200)));
            awaitRunning(peer,0,1,2);
            String big="x".repeat(8192);
            for(int i=0;i<300;i++) {
                long limit=System.nanoTime()+3_000_000_000L; PbServerTest.Reply r;
                do { r=request(HOSTS.get(0),client,2,1,command(10+i,1,"big"+i,big)); } while(r.status()==6 && System.nanoTime()<limit);
                assertEquals(0,r.status(),"write "+i);
            }
            long limit=System.nanoTime()+5_000_000_000L;
            while(System.nanoTime()<limit && !(status(1,peer).contains("\"applied\":300") && status(2,peer).contains("\"applied\":300"))) Thread.sleep(20);
            var d0=request(HOSTS.get(0),peer,37,3,new byte[0]); assertEquals(0,d0.status(),d0.detail()); assertEquals(36,d0.value().length);
            for(int n=1;n<3;n++) assertArrayEquals(d0.value(),request(HOSTS.get(n),peer,37,3,new byte[0]).value());
        } finally { for(var node:nodes) node.close(); }
    }
}
