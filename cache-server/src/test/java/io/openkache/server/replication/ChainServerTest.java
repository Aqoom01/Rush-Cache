package io.openkache.server.replication;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import org.junit.jupiter.api.Test;
import static io.openkache.server.replication.PbServerTest.*;
import static org.junit.jupiter.api.Assertions.*;

/** 실제 TCP 3노드 Chain/CRAQ와 controller 역할의 master 제어(opcode38)·로그 digest(37)를 검증한다. */
class ChainServerTest {
    static final List<String> HOSTS=List.of("127.0.0.1","127.0.0.2","127.0.0.3");
    static byte[] freeze(long epoch,int... survivors) {
        var b=ByteBuffer.allocate(1+8+4+8*survivors.length).put((byte)0).putLong(epoch).putInt(survivors.length);
        for(int s:survivors) b.putLong(s); return b.array();
    }
    static byte[] repair(long epoch) { return ByteBuffer.allocate(9).put((byte)1).putLong(epoch).array(); }
    static byte[] open(long epoch,int length) { return ByteBuffer.allocate(13).put((byte)2).putLong(epoch).putInt(length).array(); }
    static String status(int node,int peer) throws Exception { return new String(request(HOSTS.get(node),peer,35,3,new byte[0]).value(),StandardCharsets.UTF_8); }
    static void awaitRunning(int peer,int... nodes) throws Exception {
        long limit=System.nanoTime()+5_000_000_000L;
        while(System.nanoTime()<limit) {
            boolean ready=true;
            for(int n:nodes) try { ready&=status(n,peer).contains("\"state\":\"RUNNING\""); } catch(Exception e) { ready=false; }
            if(ready) return; Thread.sleep(20);
        }
        fail("chain did not reach RUNNING");
    }
    static void awaitLocal(int peer,List<Integer> nodes,String key,String value) throws Exception {
        long limit=System.nanoTime()+3_000_000_000L;
        while(System.nanoTime()<limit) {
            boolean good=true;
            for(int n:nodes) { var r=request(HOSTS.get(n),peer,36,3,key.getBytes(StandardCharsets.UTF_8)); good&=r.status()==0 && Arrays.equals(value.getBytes(StandardCharsets.UTF_8),r.value()); }
            if(good) return; Thread.sleep(20);
        }
        fail("value "+key+" missing on some node");
    }
    static byte[] get(String key) { return get(key,1); }
    static byte[] get(String key,long epoch) { return command(1,epoch,key,null); }
    /** head는 이전 쓰기의 역방향 Ack가 도착하기 전 새 쓰기에 BUSY(6)를 돌려준다. client는 같은 요청을 재시도한다. */
    static PbServerTest.Reply setRetry(int node,int client,long id,long epoch,String key,String value) throws Exception {
        long limit=System.nanoTime()+3_000_000_000L; PbServerTest.Reply r=null;
        while(System.nanoTime()<limit) {
            r=request(HOSTS.get(node),client,2,1,command(id,epoch,key,value));
            if(r.status()!=6) return r;
            Thread.sleep(1);
        }
        return r;
    }
    @Test void headWritesTailReadsAndNonTailRequestsRedirect() throws Exception {
        int client=port(),peer=port(); while(client==peer) peer=port(); var nodes=new ArrayList<PbServer>();
        try {
            for(int i=0;i<3;i++) nodes.add(PbServer.start(new PbNodeConfig(i,HOSTS,client,peer,0,50,500,10,"CHAIN",1,"INCREMENTAL","TAIL_ONLY",0,200)));
            awaitRunning(peer,0,1,2);
            var ok=request(HOSTS.get(0),client,2,1,command(1,1,"k","v")); assertEquals(0,ok.status()); assertEquals(1,ok.epoch());
            var redirect=request(HOSTS.get(1),client,2,1,command(2,1,"k2","v")); assertEquals(4,redirect.status()); assertEquals("redirect-node=0",redirect.detail());
            var headRead=request(HOSTS.get(0),client,1,1,get("k")); assertEquals(4,headRead.status()); assertEquals("redirect-node=2",headRead.detail());
            var tailRead=request(HOSTS.get(2),client,1,1,get("k")); assertEquals(0,tailRead.status()); assertArrayEquals("v".getBytes(StandardCharsets.UTF_8),tailRead.value());
            awaitLocal(peer,List.of(0,1,2),"k","v");
            assertTrue(status(0,peer).contains("\"chain\":[0,1,2]") && status(0,peer).contains("\"readMode\":\"TAIL_ONLY\""));
            var digest0=request(HOSTS.get(0),peer,37,3,new byte[0]); var digest2=request(HOSTS.get(2),peer,37,3,new byte[0]);
            assertEquals(0,digest0.status()); assertEquals(36,digest0.value().length); assertArrayEquals(digest0.value(),digest2.value());
            assertEquals(2,request(HOSTS.get(0),peer,38,3,new byte[]{9}).status());
        } finally { for(var node:nodes) node.close(); }
    }
    @Test void masterFreezeRepairOpenAfterMiddleCrashPreservesWritesAndFencesOldEpoch() throws Exception {
        int client=port(),peer=port(); while(client==peer) peer=port(); var nodes=new ArrayList<PbServer>();
        try {
            for(int i=0;i<3;i++) nodes.add(PbServer.start(new PbNodeConfig(i,HOSTS,client,peer,0,50,500,10,"CHAIN",1,"INCREMENTAL","TAIL_ONLY",0,200)));
            awaitRunning(peer,0,1,2);
            for(int i=0;i<8;i++) assertEquals(0,setRetry(0,client,10+i,1,"k"+i,"v").status());
            for(int i=0;i<8;i++) awaitLocal(peer,List.of(0,1,2),"k"+i,"v");
            nodes.get(1).close();
            for(int n:new int[]{0,2}) { var r=request(HOSTS.get(n),peer,38,3,freeze(2,0,2)); assertEquals(0,r.status(),r.detail()); assertEquals("applied=8",r.detail()); }
            assertEquals(8,request(HOSTS.get(0),peer,38,3,freeze(2,0,2)).status(),"second freeze must be rejected");
            for(int n:new int[]{0,2}) assertEquals(0,request(HOSTS.get(n),peer,38,3,repair(2)).status());
            long limit=System.nanoTime()+5_000_000_000L; boolean repaired=false;
            while(System.nanoTime()<limit && !repaired) {
                repaired=true;
                for(int n:new int[]{0,2}) repaired&=status(n,peer).contains("\"state\":\"REPAIRING\",\"chain\":[0,2],\"logLength\":8,\"applied\":8,\"acknowledged\":8");
                if(!repaired) Thread.sleep(20);
            }
            assertTrue(repaired,status(0,peer)+status(2,peer));
            assertArrayEquals(request(HOSTS.get(0),peer,37,3,new byte[0]).value(),request(HOSTS.get(2),peer,37,3,new byte[0]).value());
            assertEquals(8,request(HOSTS.get(0),peer,38,3,open(2,7)).status(),"wrong length must be rejected");
            for(int n:new int[]{0,2}) assertEquals(0,request(HOSTS.get(n),peer,38,3,open(2,8)).status());
            var after=setRetry(0,client,100,2,"after","new"); assertEquals(0,after.status()); assertEquals(2,after.epoch());
            assertEquals(5,setRetry(0,client,101,1,"stale","x").status());
            var read=request(HOSTS.get(2),client,1,1,get("after",2)); assertEquals(0,read.status()); assertArrayEquals("new".getBytes(StandardCharsets.UTF_8),read.value());
            for(int i=0;i<8;i++) awaitLocal(peer,List.of(0,2),"k"+i,"v");
            String s=status(0,peer); assertTrue(s.contains("\"epoch\":2") && s.contains("\"detectedNs\":") && !s.contains("\"readyNs\":-1"));
        } finally { for(var node:nodes) node.close(); }
    }
    @Test void craqServesCleanReadsLocallyOnEveryNode() throws Exception {
        int client=port(),peer=port(); while(client==peer) peer=port(); var nodes=new ArrayList<PbServer>();
        try {
            for(int i=0;i<3;i++) nodes.add(PbServer.start(new PbNodeConfig(i,HOSTS,client,peer,0,50,500,10,"CHAIN",1,"INCREMENTAL","CRAQ",0,200)));
            awaitRunning(peer,0,1,2);
            assertEquals(0,request(HOSTS.get(0),client,2,1,command(1,1,"k","v")).status());
            long limit=System.nanoTime()+3_000_000_000L;
            while(System.nanoTime()<limit && !status(0,peer).contains("\"acknowledged\":1")) Thread.sleep(10);
            for(int n=0;n<3;n++) { var r=request(HOSTS.get(n),client,1,1,get("k")); assertEquals(0,r.status(),"node "+n); assertArrayEquals("v".getBytes(StandardCharsets.UTF_8),r.value()); }
            var miss=request(HOSTS.get(1),client,1,1,get("absent")); assertEquals(1,miss.status());
            assertTrue(status(1,peer).contains("\"readMode\":\"CRAQ\"") && !status(1,peer).contains("\"localReads\":0,"));
        } finally { for(var node:nodes) node.close(); }
    }
}
