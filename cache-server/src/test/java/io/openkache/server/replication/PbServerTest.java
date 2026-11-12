package io.openkache.server.replication;

import java.io.*;
import java.net.*;
import java.nio.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class PbServerTest {
    record Reply(int status,long epoch,String detail,byte[] value) {}
    static int port() throws IOException { try(var s=new ServerSocket(0)) { return s.getLocalPort(); } }
    static Reply request(String host,int port,int opcode,int messageClass,byte[] body) throws IOException {
        try(var socket=new Socket()) {
            socket.connect(new InetSocketAddress(host,port),1000); socket.setSoTimeout(2000);
            var out=new DataOutputStream(socket.getOutputStream()); out.writeInt(24+body.length);
            out.writeShort(0x4f4b); out.writeByte(1); out.writeByte(messageClass); out.writeByte(opcode); out.writeByte(0); out.writeShort(0);
            out.writeLong(1); out.writeLong(0); out.write(body); out.flush();
            var in=new DataInputStream(socket.getInputStream()); int length=in.readInt(); byte[] frame=in.readNBytes(length);
            var b=ByteBuffer.wrap(frame); b.position(6); int status=Short.toUnsignedInt(b.getShort()); b.position(24);
            long epoch=b.getLong(); b.getLong(); byte[] detail=new byte[b.getInt()]; b.get(detail); byte[] value=new byte[b.getInt()]; b.get(value);
            return new Reply(status,epoch,new String(detail,StandardCharsets.UTF_8),value);
        }
    }
    static byte[] command(long id,long epoch,String key,String value) {
        byte[] k=key.getBytes(StandardCharsets.UTF_8),v=value==null?null:value.getBytes(StandardCharsets.UTF_8);
        ByteBuffer b=ByteBuffer.allocate(32+k.length+(v==null?0:4+v.length));
        b.putLong(99).putLong(id).putLong(epoch).putInt(1000).putInt(k.length).put(k);
        if(v!=null) b.putInt(v.length).put(v); return b.array();
    }
    static void awaitHeartbeat(List<String> hosts,int peerPort) throws Exception {
        long limit=System.nanoTime()+3_000_000_000L;
        while(System.nanoTime()<limit) {
            boolean ready=true;
            for(int i=1;i<3;i++) ready&=new String(request(hosts.get(i),peerPort,35,3,new byte[0]).value(),StandardCharsets.UTF_8).contains("\"heartbeatSeen\":true");
            if(ready) return;
            Thread.sleep(10);
        }
        fail("initial Primary heartbeat not received");
    }
    static void awaitFencedPrimary(List<String> hosts,int peerPort) throws Exception {
        long limit=System.nanoTime()+5_000_000_000L;
        while(System.nanoTime()<limit) {
            String status=new String(request(hosts.getFirst(),peerPort,35,3,new byte[0]).value(),StandardCharsets.UTF_8);
            if(status.contains("\"fenceState\":\"SERVING\"")) return;
            Thread.sleep(10);
        }
        fail("initial fenced Primary did not acquire a lease");
    }
    @Test void disconnectedBackupTimesOutWithoutFalseSuccess() throws Exception {
        int client=port(),peer=port(); while(client==peer) peer=port();
        List<String> hosts=List.of("127.0.0.1","127.0.0.2","127.0.0.3"); List<PbServer> nodes=new ArrayList<>();
        try {
            for(int i=0;i<3;i++) nodes.add(PbServer.start(new PbNodeConfig(i,hosts,client,peer,0,50,1000,10)));
            awaitHeartbeat(hosts,peer);
            assertEquals(0,request(hosts.getFirst(),client,2,1,command(1,1,"warm","v")).status());
            nodes.get(2).close();
            Reply missing=request(hosts.getFirst(),client,2,1,command(2,1,"partial","new"));
            assertEquals(7,missing.status());
            assertEquals(1,request(hosts.getFirst(),peer,36,3,"partial".getBytes(StandardCharsets.UTF_8)).status());
            assertArrayEquals("new".getBytes(StandardCharsets.UTF_8),request(hosts.get(1),peer,36,3,"partial".getBytes(StandardCharsets.UTF_8)).value());
        } finally { for(var node:nodes) node.close(); }
    }
    @Test void realTcpReplicationRedirectAndCrashPromotion() throws Exception {
        int client=port(),peer=port(); while(client==peer) peer=port();
        List<String> hosts=List.of("127.0.0.1","127.0.0.2","127.0.0.3"); List<PbServer> nodes=new ArrayList<>();
        try {
            for(int i=0;i<3;i++) nodes.add(PbServer.start(new PbNodeConfig(i,hosts,client,peer,0,50,1000,10)));
            awaitHeartbeat(hosts,peer);
            Reply set=null;
            for(int attempt=0;attempt<5;attempt++) {
                set=request(hosts.getFirst(),client,2,1,command(1,1,"before","value"));
                if(set.status()==0) break;
            }
            assertNotNull(set); assertEquals(0,set.status());
            for(String host:hosts) assertArrayEquals("value".getBytes(StandardCharsets.UTF_8),request(host,peer,36,3,"before".getBytes(StandardCharsets.UTF_8)).value());
            Reply redirect=request(hosts.get(1),client,2,1,command(2,1,"bad","value"));
            assertEquals(4,redirect.status()); assertEquals("redirect-node=0",redirect.detail());
            nodes.getFirst().close();
            long limit=System.nanoTime()+5_000_000_000L; Reply after=null;
            while(System.nanoTime()<limit) {
                after=request(hosts.get(1),client,2,1,command(3,2,"after","new"));
                if(after.status()==0) break;
                Thread.sleep(20);
            }
            assertNotNull(after); assertEquals(0,after.status()); assertEquals(2,after.epoch());
            assertEquals(5,request(hosts.get(1),client,1,1,command(4,1,"before",null)).status());
            for(int i=1;i<3;i++) assertArrayEquals("value".getBytes(StandardCharsets.UTF_8),request(hosts.get(i),peer,36,3,"before".getBytes(StandardCharsets.UTF_8)).value());
        } finally { for(var node:nodes) node.close(); }
    }
    @Test void swimDeadEventPromotesTheDesignatedBackup() throws Exception {
        int client=port(),peer=port(),failure=port();
        while(peer==client) peer=port();
        while(failure==client || failure==peer) failure=port();
        List<String> hosts=List.of("127.0.0.1","127.0.0.2","127.0.0.3"); List<PbServer> nodes=new ArrayList<>();
        try {
            for(int i=0;i<3;i++) nodes.add(PbServer.start(new PbNodeConfig(i,hosts,client,peer,0,50,1000,10,
                    "PB_FENCED",1,"INCREMENTAL","TAIL_ONLY",0,1000,"SWIM",failure,100,25,25,250,0,8,2,1)));
            awaitFencedPrimary(hosts,peer);
            assertEquals(0,request(hosts.getFirst(),client,2,1,command(20,1,"before-swim","value")).status());
            nodes.getFirst().close();
            long limit=System.nanoTime()+5_000_000_000L; Reply after=null;
            while(System.nanoTime()<limit) {
                after=request(hosts.get(1),client,2,1,command(21,2,"after-swim","new"));
                if(after.status()==0) break;
                Thread.sleep(20);
            }
            assertNotNull(after); assertEquals(0,after.status()); assertEquals(2,after.epoch());
            String status=nodes.get(1).statusJson();
            assertTrue(status.contains("\"failureDetector\":\"SWIM\""),status);
            assertTrue(status.contains("\"primaryFailureStatus\":\"DEAD\""),status);
            assertArrayEquals("value".getBytes(StandardCharsets.UTF_8),request(hosts.get(1),client,1,1,command(22,2,"before-swim",null)).value());
            assertTrue(status.contains("\"promotionVotes\":2"),status);
        } finally { for(var node:nodes) node.close(); }
    }

    @Test void swimLifeguardDeadEventPromotesAndExportsAwareness() throws Exception {
        int client=port(),peer=port(),failure=port();
        while(peer==client) peer=port();
        while(failure==client || failure==peer) failure=port();
        List<String> hosts=List.of("127.0.0.1","127.0.0.2","127.0.0.3"); List<PbServer> nodes=new ArrayList<>();
        try {
            for(int i=0;i<3;i++) nodes.add(PbServer.start(new PbNodeConfig(i,hosts,client,peer,0,50,1000,10,
                    "PB_FENCED",1,"INCREMENTAL","TAIL_ONLY",0,1000,"SWIM_LIFEGUARD",failure,100,25,25,250,0,8,2,1)));
            awaitFencedPrimary(hosts,peer);
            assertEquals(0,request(hosts.getFirst(),client,2,1,command(30,1,"before-lifeguard","value")).status());
            nodes.getFirst().close();
            long limit=System.nanoTime()+7_000_000_000L; Reply after=null;
            while(System.nanoTime()<limit) {
                after=request(hosts.get(1),client,2,1,command(31,2,"after-lifeguard","new"));
                if(after.status()==0) break;
                Thread.sleep(20);
            }
            assertNotNull(after); assertEquals(0,after.status()); assertEquals(2,after.epoch());
            String status=nodes.get(1).statusJson();
            assertTrue(status.contains("\"failureDetector\":\"SWIM_LIFEGUARD\""),status);
            assertTrue(status.contains("\"primaryFailureStatus\":\"DEAD\""),status);
            assertTrue(status.contains("\"fdAwarenessScore\":"),status);
            assertTrue(status.contains("\"promotionVotes\":2"),status);
        } finally { for(var node:nodes) node.close(); }
    }
}
