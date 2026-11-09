package io.openkache.server.replication;

import java.nio.charset.StandardCharsets;
import java.util.*;
import org.junit.jupiter.api.Test;
import static io.openkache.server.replication.PbServerTest.*;
import static org.junit.jupiter.api.Assertions.*;

class VrServerTest {
    @Test void quorumSurvivesOneBackupDisconnect() throws Exception {
        int client=port(),peer=port(); while(client==peer) peer=port();
        List<String> hosts=List.of("127.0.0.1","127.0.0.2","127.0.0.3"); var nodes=new ArrayList<PbServer>();
        try {
            for(int i=0;i<3;i++) nodes.add(PbServer.start(new PbNodeConfig(i,hosts,client,peer,0,50,1000,10,"VR")));
            awaitHeartbeat(hosts,peer); nodes.get(2).close();
            assertEquals(0,request(hosts.getFirst(),client,2,1,command(1,0,"quorum","v")).status());
            assertArrayEquals("v".getBytes(StandardCharsets.UTF_8),request(hosts.get(1),peer,36,3,"quorum".getBytes(StandardCharsets.UTF_8)).value());
        } finally { for(var node:nodes) node.close(); }
    }
    @Test void crashViewChangePreservesCommittedWriteAndDeduplicates() throws Exception {
        int client=port(),peer=port(); while(client==peer) peer=port();
        List<String> hosts=List.of("127.0.0.1","127.0.0.2","127.0.0.3"); var nodes=new ArrayList<PbServer>();
        try {
            for(int i=0;i<3;i++) nodes.add(PbServer.start(new PbNodeConfig(i,hosts,client,peer,0,50,1000,10,"VR")));
            awaitHeartbeat(hosts,peer);
            assertEquals(0,request(hosts.getFirst(),client,2,1,command(1,0,"before","v")).status());
            assertEquals(0,request(hosts.getFirst(),client,2,1,command(1,0,"before","v")).status());
            nodes.getFirst().close();
            long limit=System.nanoTime()+8_000_000_000L; Reply after=null;
            while(System.nanoTime()<limit) {
                after=request(hosts.get(1),client,2,1,command(2,1,"after","new"));
                if(after.status()==0) break;
                Thread.sleep(20);
            }
            assertNotNull(after); assertEquals(0,after.status()); assertEquals(1,after.epoch());
            assertEquals(0,request(hosts.get(1),client,2,1,command(1,1,"before","v")).status());
            assertEquals(2,request(hosts.get(1),client,2,1,command(1,1,"before","different")).status());
            for(int i=1;i<3;i++) {
                var reply=request(hosts.get(i),peer,36,3,"before".getBytes(StandardCharsets.UTF_8));
                assertArrayEquals("v".getBytes(StandardCharsets.UTF_8),reply.value());
            }
        } finally { for(var node:nodes) node.close(); }
    }
    @Test void catchupModeIsSelectedByPropertyAndPreservesCommittedWriteAcrossCrash() throws Exception {
        var file=java.nio.file.Files.createTempFile("vr-catchup",".properties");
        String peers="node.id=0\npeer.0=127.0.0.1\npeer.1=127.0.0.2\npeer.2=127.0.0.3\n";
        try {
            java.nio.file.Files.writeString(file,"protocol=VR\nvr.mode=INCREMENTAL_CATCHUP\n"+peers);
            assertEquals("INCREMENTAL_CATCHUP",PbNodeConfig.load(file).vrMode());
            java.nio.file.Files.writeString(file,"protocol=VR\n"+peers);
            assertEquals("INCREMENTAL",PbNodeConfig.load(file).vrMode());
            java.nio.file.Files.writeString(file,"protocol=VR\nvr.mode=FULL_LOG\n"+peers);
            assertThrows(IllegalArgumentException.class,()->PbNodeConfig.load(file));
        } finally { java.nio.file.Files.deleteIfExists(file); }
        int client=port(),peer=port(); while(client==peer) peer=port();
        List<String> hosts=List.of("127.0.0.1","127.0.0.2","127.0.0.3"); var nodes=new ArrayList<PbServer>();
        try {
            for(int i=0;i<3;i++) nodes.add(PbServer.start(new PbNodeConfig(i,hosts,client,peer,0,50,1000,10,"VR",1,"INCREMENTAL_CATCHUP")));
            awaitHeartbeat(hosts,peer);
            for(int i=0;i<8;i++) assertEquals(0,request(hosts.getFirst(),client,2,1,command(10+i,0,"before-"+i,"v")).status());
            String status=new String(request(hosts.getFirst(),peer,35,3,new byte[0]).value(),StandardCharsets.UTF_8);
            assertTrue(status.contains("\"vrMode\":\"INCREMENTAL_CATCHUP\"") && status.contains("\"COMMIT_BOUNDED\":"),status);
            long limit=System.nanoTime()+3_000_000_000L; boolean replicated=false;
            while(System.nanoTime()<limit && !replicated) {
                replicated=true;
                for(int i=1;i<3;i++) for(int k=0;k<8;k++) replicated&=request(hosts.get(i),peer,36,3,("before-"+k).getBytes(StandardCharsets.UTF_8)).status()==0;
                if(!replicated) Thread.sleep(20);
            }
            assertTrue(replicated,"catch-up mode did not replicate committed writes to both backups");
            nodes.getFirst().close();
            limit=System.nanoTime()+8_000_000_000L; Reply after=null;
            while(System.nanoTime()<limit) {
                after=request(hosts.get(1),client,2,1,command(2,1,"after","new"));
                if(after.status()==0) break;
                Thread.sleep(20);
            }
            assertNotNull(after); assertEquals(0,after.status()); assertEquals(1,after.epoch());
            for(int i=1;i<3;i++) for(int k=0;k<8;k++)
                assertArrayEquals("v".getBytes(StandardCharsets.UTF_8),request(hosts.get(i),peer,36,3,("before-"+k).getBytes(StandardCharsets.UTF_8)).value());
        } finally { for(var node:nodes) node.close(); }
    }
}
