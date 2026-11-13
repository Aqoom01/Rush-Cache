package io.openkache.server.integration;

import io.openkache.core.api.*;
import io.openkache.core.model.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import static org.junit.jupiter.api.Assertions.*;

class FixedLptIntegrationTest {
    @TempDir Path directory;
    @ParameterizedTest @ValueSource(strings={"STOP","PRE","POST"})
    void originalFixedPlannerScalesAndDrainsWithoutDroppingEmptyTablets(String copy)throws Exception{
        Properties p=FullIntegratedCacheTest.properties("O00",true);p.remove("protocol");p.setProperty("a1.method","VR");p.setProperty("a2.method","O00");p.setProperty("placement","FIXED_LPT");p.setProperty("copy",copy);
        var config=new IntegrationConfig(p);Path events=directory.resolve("events.jsonl");
        var authorities=List.of(new JointCacheTest.Store(),new JointCacheTest.Store(),new JointCacheTest.Store());var data=List.of(new JointCacheTest.Store(),new JointCacheTest.Store(),new JointCacheTest.Store());
        try(var journal=new JsonLine(events);var cache=new IntegratedCache(config,java.util.stream.IntStream.range(0,3).mapToObj(g->(ReplicatedBackend)new JointBackend(config,authorities.get(g),data.get(g))).toList(),journal)){
            assertTrue(cache.ready());int count=cache.table().size();
            // Leave several metadata tablets empty: they must still drain when their group is removed.
            for(int i=0;i<3;i++)assertEquals(ResultStatus.OK,cache.execute(new IntegrationWire.Request(new Command.Set(Bytes.utf8("k"+i),Bytes.utf8("v")),1,i+1,0,5000),0).status());
            cache.scaleTo(3).get();drain(cache);cache.scaleTo(1).get();drain(cache);
            assertEquals(count,cache.table().size());assertTrue(cache.status().contains("\"activeGroups\":[0]"),cache.status());
            for(int i=0;i<3;i++)assertEquals(Optional.of(Bytes.utf8("v")),cache.execute(new IntegrationWire.Request(new Command.Get(Bytes.utf8("k"+i)),1,i+100,0,5000),0).value());
            assertTrue(Files.readString(events).contains("\"sourcePolicy\":\"fixed-tablet-lpt-v1\""));
            assertTrue(ReplicaDiagnostics.number(cache.status(),"routingPublications")>1);
        }
    }
    private static void drain(IntegratedCache cache)throws Exception{
        long until=System.nanoTime()+TimeUnit.SECONDS.toNanos(8);while(!cache.status().contains("\"pendingJobs\":0")&&System.nanoTime()<until)Thread.sleep(5);
        assertEquals(0,ReplicaDiagnostics.number(cache.status(),"unresolvedMigrations"),cache.status());assertFalse(cache.status().contains("SAFE_HOLD"),cache.status());
    }
}
