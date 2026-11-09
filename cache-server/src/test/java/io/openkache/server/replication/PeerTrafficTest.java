package io.openkache.server.replication;

import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class PeerTrafficTest {
    @Test void concurrentCallbacksRetainExactOpcodeAndOverallTotals() throws Exception {
        var traffic=new PeerTraffic();
        try(var pool=Executors.newFixedThreadPool(4)) {
            var futures=new java.util.ArrayList<Future<?>>();
            for(int task=0;task<4;task++) {
                final int opcode=49+task;
                futures.add(pool.submit(()->{ for(int i=0;i<1000;i++) traffic.record(opcode,opcode); }));
            }
            for(var f:futures) f.get(5,TimeUnit.SECONDS);
        }
        String json=traffic.json();
        assertTrue(json.contains("\"totalFrames\":4000"));assertTrue(json.contains("\"totalBytes\":202000"));
        for(int opcode=49;opcode<=52;opcode++) assertTrue(json.contains("\""+opcode+"\":{\"frames\":1000,\"bytes\":"+(opcode*1000)+"}"));
        assertThrows(IllegalArgumentException.class,()->traffic.record(256,1));
    }
}
