package io.openkache.server.integration;

import io.openkache.core.api.*;
import io.openkache.core.model.*;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Stream;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import static org.junit.jupiter.api.Assertions.*;

/** Every new A3 profile drives BOTH real replication layers and B3, with autonomous watchdogs held back. */
class LabJointCacheTest {
    @TempDir Path directory;
    static Stream<String> methods(){return FailureMethods.LAB.stream().sorted();}
    @ParameterizedTest @MethodSource("methods")
    void detectorDrivesJointFailoverAndMigration(String method)throws Exception{
        Properties p=LabFailureRuntimeTest.settings(method);p.remove("protocol");p.setProperty("a1.method","VR");p.setProperty("a2.method","O00");
        p.setProperty("replica.port","28301");p.setProperty("failure.grace.ms","500");p.setProperty("fd.deadline.ms","600");p.setProperty("fd.heartbeat.ms","60");
        p.setProperty("fd.probe.timeout.ms","60");p.setProperty("request.timeout.ms","10000");p.setProperty("copy","POST");
        var config=new IntegrationConfig(p);var replicas=new ArrayList<AutoCloseable>();var failed=new ArrayList<AutoCloseable>();
        try(var journal=new JsonLine(directory.resolve("joint.jsonl"))){try{
            for(int g=0;g<2;g++)for(int n=0;n<3;n++)for(boolean a1:List.of(true,false)){
                var node=IntegrationReplicaMain.startNative(config.layer(a1),g,n,directory.resolve("g"+g+"n"+n+"-"+a1),false);replicas.add(node);if(g==0&&n==0)failed.add(node);
            }
            try(var cache=new IntegratedCache(config,List.of(ReplicatedBackend.connect(config,0),ReplicatedBackend.connect(config,1)),journal)){
                long until=System.nanoTime()+TimeUnit.SECONDS.toNanos(10);while(!cache.ready()&&System.nanoTime()<until)Thread.sleep(20);assertTrue(cache.ready());
                Thread.sleep(900);
                for(boolean a1:List.of(true,false))for(int n=0;n<3;n++){
                    String status=ReplicaDiagnostics.request(config.layer(a1),0,n,null);assertTrue(status.contains("\"runtimeHealthy\":true"),status);
                    assertTrue(ReplicaDiagnostics.number(status,"receivedFrames")>0,status);
                }
                var moving=cache.table().ownedBy(new NodeId(0)).getFirst();Bytes key=null;for(int i=0;key==null;i++){Bytes k=Bytes.utf8("joint"+i);if(moving.range().contains(config.scheme().keyHash(k)))key=k;}
                assertEquals(ResultStatus.OK,send(cache,new Command.Set(key,Bytes.utf8("before")),1).status());
                for(var node:failed)node.close();
                until=System.nanoTime()+TimeUnit.SECONDS.toNanos(12);boolean recovered=false;
                while(System.nanoTime()<until){
                    String a1=ReplicaDiagnostics.request(config.layer(true),0,1,null),a2=ReplicaDiagnostics.request(config.layer(false),0,1,null);
                    if(ReplicaDiagnostics.number(a1,"epoch")>0&&ReplicaDiagnostics.number(a2,"view")>0&&a2.contains("\"role\":\"NORMAL\"")){recovered=true;break;}Thread.sleep(20);
                }
                assertTrue(recovered,method+cache.status());
                assertEquals(Optional.of(Bytes.utf8("before")),send(cache,new Command.Get(key),2).value());
                assertEquals(ResultStatus.OK,send(cache,new Command.Set(key,Bytes.utf8("after")),3).status());
                cache.move(moving.id().value(),1);until=System.nanoTime()+TimeUnit.SECONDS.toNanos(15);
                while(!cache.status().contains("\"pendingJobs\":0")&&System.nanoTime()<until)Thread.sleep(10);
                assertEquals(0,ReplicaDiagnostics.number(cache.status(),"unresolvedMigrations"),cache.status());
                assertEquals(1,cache.table().get(moving.id()).owner().value());
                assertEquals(Optional.of(Bytes.utf8("after")),send(cache,new Command.Get(key),4).value());
            }
        }finally{Collections.reverse(replicas);for(var r:replicas)r.close();}}
    }
    private static Result send(IntegratedCache cache,Command command,long id)throws Exception{
        long until=System.nanoTime()+TimeUnit.SECONDS.toNanos(12);Result result;
        do{result=cache.execute(new IntegrationWire.Request(command,1,id,0,10000),0);if(result.status()==ResultStatus.BUSY)Thread.sleep(10);}while(result.status()==ResultStatus.BUSY&&System.nanoTime()<until);
        return result;
    }
}
