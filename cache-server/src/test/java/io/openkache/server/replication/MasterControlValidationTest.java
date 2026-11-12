package io.openkache.server.replication;

import java.util.Arrays;
import org.junit.jupiter.api.Test;
import static io.openkache.server.replication.PbServerTest.*;
import static io.openkache.server.replication.ChainServerTest.*;
import static io.openkache.server.replication.RoseServerTest.*;
import static org.junit.jupiter.api.Assertions.*;

class MasterControlValidationTest {
    private static byte[] trailing(byte[] body) {
        byte[] bad=Arrays.copyOf(body,body.length+1);bad[bad.length-1]=127;return bad;
    }
    private static void state(int peer,String expected,int epoch) throws Exception {
        // 엔진 명령은 owner mailbox에 비동기로 들어간다. BAD_REQUEST 뒤에 예약된 변경도 검출한다.
        Thread.sleep(60);
        String s=status(1,peer);
        assertTrue(s.contains("\"epoch\":"+epoch+","),s);
        assertTrue(s.contains("\"state\":\""+expected+"\""),s);
    }
    @Test void trailingBytesCannotFreezeRepairOrOpenEitherProtocol() throws Exception {
        for(String protocol:new String[]{"CHAIN","ROSE"}) {
            int client=port(),peer=port();while(peer==client) peer=port();
            try(var server=PbServer.start(new PbNodeConfig(1,HOSTS,client,peer,0,50,500,10,protocol,1,"INCREMENTAL","TAIL_ONLY",0,200))) {
                assertTrue(server.statusJson().contains("\"protocol\":\""+protocol+"\""));
                boolean chain=protocol.equals("CHAIN");
                byte[] stale=chain?freeze(1,1,2):roseFreeze(1);
                assertEquals(8,request(HOSTS.get(1),peer,38,3,stale).status());
                state(peer,"RUNNING",1);
                assertTrue(status(1,peer).contains("\"detectedNs\":-1"));
                byte[] f=chain?freeze(2,1,2):roseFreeze(2);
                assertEquals(2,request(HOSTS.get(1),peer,38,3,trailing(f)).status());
                state(peer,"RUNNING",1);
                assertTrue(status(1,peer).contains("\"detectedNs\":-1"));
                assertEquals(0,request(HOSTS.get(1),peer,38,3,f).status());state(peer,"FROZEN",2);
                byte[] repair=chain?repair(2):roseRepair(2,2,1,2);
                assertEquals(2,request(HOSTS.get(1),peer,38,3,trailing(repair)).status());state(peer,"FROZEN",2);
                assertEquals(0,request(HOSTS.get(1),peer,38,3,repair).status());state(peer,"REPAIRING",2);
                byte[] o=open(2,0);
                assertEquals(2,request(HOSTS.get(1),peer,38,3,trailing(o)).status());state(peer,"REPAIRING",2);
                assertEquals(0,request(HOSTS.get(1),peer,38,3,o).status());state(peer,"RUNNING",2);
            }
        }
    }
}
