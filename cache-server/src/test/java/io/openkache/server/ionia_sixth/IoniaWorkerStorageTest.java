package io.openkache.server.ionia_sixth;

import static org.junit.jupiter.api.Assertions.*;
import io.openkache.server.ionia.IoniaFileStorage;
import io.openkache.core.api.*;
import io.openkache.core.model.*;
import io.openkache.core.replication.ionia_sixth.IoniaWorkerSettings;
import io.openkache.core.spi.TimeSource;
import java.io.*;
import java.net.*;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

@Timeout(40)
class IoniaWorkerStorageTest {
    @TempDir Path directory;
    static final NodeId A=new NodeId(1);
    static RequestContext context(long id){return new RequestContext(new ClientId(9),new OperationId(id),RoutingVersion.ZERO,OwnershipEpoch.ZERO,Deadline.after(TimeSource.system(),Duration.ofSeconds(10)));}
    static Bytes keyFor(int shard,int shards){for(int i=0;i<10000;i++){Bytes key=Bytes.utf8("key-"+i);if(Math.floorMod(key.hashCode(),shards)==shard)return key;}throw new AssertionError("key search");}
    static Result run(IoniaWorkerFileStorage s,Command command,long id)throws Exception{return s.execute(command,context(id)).toCompletableFuture().get(5,TimeUnit.SECONDS);}
    static void idle(IoniaWorkerFileStorage s)throws Exception{
        long end=System.nanoTime()+TimeUnit.SECONDS.toNanos(5);
        while(System.nanoTime()-end<0){var d=s.diagnostics();if(d.activeTasks()==0&&d.workers().stream().allMatch(w->w.submitted()==w.finished()+w.rejected()))return;Thread.sleep(1);}
        fail("storage did not drain: "+s.diagnostics());
    }
    static Map<Path,byte[]> snapshot(Path directory)throws IOException{
        Map<Path,byte[]> result=new HashMap<>();try(var paths=Files.walk(directory)){for(Path p:paths.filter(Files::isRegularFile).toList())result.put(directory.relativize(p),Files.readAllBytes(p));}return result;
    }
    @Test void V04B_03_realWorkersOverlapAcrossFixedShardsAndIdleExcessWorkers()throws Exception{
        for(int workers:new int[]{1,2,4,8}){
            CountDownLatch entered=new CountDownLatch(Math.min(workers,4)),release=new CountDownLatch(1);
            Set<Long> threads=ConcurrentHashMap.newKeySet();Map<Integer,Long> shardThreads=new ConcurrentHashMap<>();
            try(var store=new IoniaWorkerFileStorage(A,directory.resolve("w"+workers),new IoniaWorkerSettings(4,workers),TimeSource.system(),false,(shard,reset)->{
                if(!reset){threads.add(Thread.currentThread().threadId());shardThreads.put(shard,Thread.currentThread().threadId());entered.countDown();
                    if(!release.await(5,TimeUnit.SECONDS))throw new IOException("fixture latch timeout");}
            })){
                List<CompletableFuture<Result>> writes=new ArrayList<>();
                try{
                    for(int shard=0;shard<4;shard++)writes.add(store.execute(new Command.Set(keyFor(shard,4),Bytes.utf8("v")),context(shard+1)).toCompletableFuture());
                    assertTrue(entered.await(5,TimeUnit.SECONDS));assertEquals(Math.min(workers,4),threads.size());
                    assertEquals(Math.min(workers,4),store.diagnostics().activeTasks());
                }finally{release.countDown();}
                for(var write:writes)assertEquals(ResultStatus.OK,write.get(5,TimeUnit.SECONDS).status());idle(store);
                assertEquals(Math.min(workers,4),store.diagnostics().peakTasks());assertEquals(workers,store.diagnostics().workers().size());
                for(int shard=0;shard<4;shard++)assertEquals(shardThreads.get(shard).longValue(),store.diagnostics().workers().get(shard%workers).lastThreadId());
                try(var files=Files.list(directory.resolve("w"+workers))){assertEquals(4,files.filter(p->p.getFileName().toString().matches("kv-[0-9]+[.]data")).count());}
            }finally{release.countDown();}
        }
    }
    @Test void V04B_04_sameShardFifoSurvivesCallerCancellationWhileOtherWorkerProgresses()throws Exception{
        Bytes first=Bytes.utf8("Aa"),second=Bytes.utf8("BB");assertEquals(first.hashCode(),second.hashCode());
        int shard=Math.floorMod(first.hashCode(),4);Bytes cold=keyFor((shard+1)%4,4);
        CountDownLatch entered=new CountDownLatch(1),release=new CountDownLatch(1);AtomicBoolean once=new AtomicBoolean(true);
        try(var store=new IoniaWorkerFileStorage(A,directory,new IoniaWorkerSettings(4,2),TimeSource.system(),false,(s,reset)->{
            if(!reset&&s==shard&&once.compareAndSet(true,false)){entered.countDown();if(!release.await(5,TimeUnit.SECONDS))throw new IOException("fixture timeout");}
        })){
            try{
                var pending=store.execute(new Command.Set(first,Bytes.utf8("a")),context(1)).toCompletableFuture();assertTrue(entered.await(5,TimeUnit.SECONDS));
                var queued=store.execute(new Command.Set(second,Bytes.utf8("b")),context(2)).toCompletableFuture();pending.cancel(false);
                assertEquals(ResultStatus.OK,run(store,new Command.Set(cold,Bytes.utf8("c")),3).status());assertFalse(queued.isDone());
                release.countDown();assertEquals(ResultStatus.OK,queued.get(5,TimeUnit.SECONDS).status());
                assertEquals(Bytes.utf8("a"),run(store,new Command.Get(first),4).value().orElseThrow());
                assertEquals(Bytes.utf8("b"),run(store,new Command.Get(second),5).value().orElseThrow());
            }finally{release.countDown();}
        }
    }
    @Test void V04B_05_resetCoversEveryFileEvenWhenFilesOutnumberWorkers()throws Exception{
        CountDownLatch entered=new CountDownLatch(1),release=new CountDownLatch(1);AtomicBoolean once=new AtomicBoolean(true);
        try(var store=new IoniaWorkerFileStorage(A,directory,new IoniaWorkerSettings(4,1),TimeSource.system(),false,(shard,reset)->{
            if(!reset&&once.compareAndSet(true,false)){entered.countDown();if(!release.await(5,TimeUnit.SECONDS))throw new IOException("fixture timeout");}
        })){
            try{
                var write=store.execute(new Command.Set(keyFor(0,4),Bytes.utf8("old")),context(1));assertTrue(entered.await(5,TimeUnit.SECONDS));
                for(int shard=1;shard<4;shard++)store.execute(new Command.Set(keyFor(shard,4),Bytes.utf8("old")),context(shard+1));
                var reset=store.reset().toCompletableFuture();assertFalse(reset.isDone());release.countDown();write.toCompletableFuture().get(5,TimeUnit.SECONDS);reset.get(5,TimeUnit.SECONDS);
                for(int shard=0;shard<4;shard++){assertEquals(ResultStatus.MISS,run(store,new Command.Get(keyFor(shard,4)),10+shard).status());assertEquals(0,Files.size(directory.resolve("kv-"+shard+".data")));}
            }finally{release.countDown();}
        }
    }
    @Test void V04B_06_hookFailureDoesNotKillWorkerOrFabricateSuccess()throws Exception{
        AtomicBoolean fail=new AtomicBoolean(true);
        try(var store=new IoniaWorkerFileStorage(A,directory,new IoniaWorkerSettings(4,2),TimeSource.system(),false,(shard,reset)->{if(fail.getAndSet(false))throw new IOException("injected I/O failure");})){
            assertThrows(ExecutionException.class,()->run(store,new Command.Set(keyFor(0,4),Bytes.utf8("bad")),1));
            assertEquals(ResultStatus.MISS,run(store,new Command.Get(keyFor(0,4)),2).status());
            assertEquals(ResultStatus.OK,run(store,new Command.Set(keyFor(0,4),Bytes.utf8("good")),3).status());idle(store);
            assertEquals(0,store.diagnostics().activeTasks());
        }
    }
    @Test void V04B_07_changingOnlyWorkersReopensSameLayoutAndFormat()throws Exception{
        Bytes key=keyFor(3,4);
        try(var store=new IoniaWorkerFileStorage(A,directory,new IoniaWorkerSettings(4,1),TimeSource.system(),true)){run(store,new Command.Set(key,Bytes.utf8("persisted")),1);}
        byte[] layout=Files.readAllBytes(directory.resolve("io-layout-v1.txt"));
        try(var store=new IoniaWorkerFileStorage(A,directory,new IoniaWorkerSettings(4,4),TimeSource.system(),false)){assertEquals(Bytes.utf8("persisted"),run(store,new Command.Get(key),2).value().orElseThrow());}
        assertArrayEquals(layout,Files.readAllBytes(directory.resolve("io-layout-v1.txt")));
        try(var original=new IoniaFileStorage(A,directory,4,TimeSource.system(),false)){
            assertEquals(Bytes.utf8("persisted"),original.execute(new Command.Get(key),context(3)).toCompletableFuture().get(5,TimeUnit.SECONDS).value().orElseThrow());
        }
    }
    @Test void V04B_08_wrongShardLayoutRejectsWithoutTouchingStoredRecords()throws Exception{
        try(var store=new IoniaWorkerFileStorage(A,directory,new IoniaWorkerSettings(4,1),TimeSource.system(),false)){run(store,new Command.Set(keyFor(3,4),Bytes.utf8("safe")),1);}
        var before=snapshot(directory);assertThrows(IOException.class,()->new IoniaWorkerFileStorage(A,directory,new IoniaWorkerSettings(2,2),TimeSource.system(),false));
        var after=snapshot(directory);assertEquals(before.keySet(),after.keySet());for(Path p:before.keySet())assertArrayEquals(before.get(p),after.get(p));
        try(var store=new IoniaWorkerFileStorage(A,directory,new IoniaWorkerSettings(4,2),TimeSource.system(),false)){assertEquals(ResultStatus.OK,run(store,new Command.Get(keyFor(3,4)),2).status());}
    }
    @Test void V04B_08_missingOrCorruptManifestAndUnexpectedShardFailClosed()throws Exception{
        Path legacy=directory.resolve("legacy");
        try(var store=new IoniaFileStorage(A,legacy,2,TimeSource.system(),false)){store.execute(new Command.Set(Bytes.utf8("k"),Bytes.utf8("v")),context(1)).toCompletableFuture().get(5,TimeUnit.SECONDS);}
        var before=snapshot(legacy);assertThrows(IOException.class,()->new IoniaWorkerFileStorage(A,legacy,new IoniaWorkerSettings(2,1),TimeSource.system(),false));
        assertFalse(Files.exists(legacy.resolve("io-layout-v1.txt")));for(Path p:before.keySet())assertArrayEquals(before.get(p),snapshot(legacy).get(p));
        Path extra=directory.resolve("extra");try(var store=new IoniaWorkerFileStorage(A,extra,new IoniaWorkerSettings(2,1),TimeSource.system(),false)){assertEquals(2,store.diagnostics().storageShards());}
        Files.write(extra.resolve("kv-2.data"),new byte[]{1});assertThrows(IOException.class,()->new IoniaWorkerFileStorage(A,extra,new IoniaWorkerSettings(2,1),TimeSource.system(),false));
        Files.writeString(extra.resolve("io-layout-v1.txt"),"corrupt");assertThrows(IOException.class,()->new IoniaWorkerFileStorage(A,extra,new IoniaWorkerSettings(2,1),TimeSource.system(),false));
    }
    @Test void V04B_06_closeRejectsRequestsAndResetAndReleasesDirectoryLock()throws Exception{
        var store=new IoniaWorkerFileStorage(A,directory,new IoniaWorkerSettings(4,2),TimeSource.system(),false);store.close();
        assertEquals(ResultStatus.UNAVAILABLE,run(store,new Command.Get(Bytes.utf8("k")),1).status());
        assertThrows(ExecutionException.class,()->store.reset().toCompletableFuture().get(5,TimeUnit.SECONDS));
        try(var reopened=new IoniaWorkerFileStorage(A,directory,new IoniaWorkerSettings(4,1),TimeSource.system(),false)){assertTrue(reopened.diagnostics().workers().size()==1);}
    }
    @Test void V04B_08_exclusiveDirectoryLockAndTrailingRecordRepairRemainSupported()throws Exception{
        try(var store=new IoniaWorkerFileStorage(A,directory,new IoniaWorkerSettings(4,2),TimeSource.system(),false)){
            run(store,new Command.Set(keyFor(0,4),Bytes.utf8("good")),1);
            assertThrows(java.nio.channels.OverlappingFileLockException.class,()->new IoniaWorkerFileStorage(A,directory,new IoniaWorkerSettings(4,1),TimeSource.system(),false));
        }
        Files.write(directory.resolve("kv-0.data"),new byte[]{0,0},StandardOpenOption.APPEND);
        try(var store=new IoniaWorkerFileStorage(A,directory,new IoniaWorkerSettings(4,1),TimeSource.system(),false)){assertEquals(Bytes.utf8("good"),run(store,new Command.Get(keyFor(0,4)),2).value().orElseThrow());}
    }
    @Test void V04B_06_boundedWorkerMailboxRejectsWithoutExecutingOrLosingAccounting()throws Exception{
        CountDownLatch entered=new CountDownLatch(1),release=new CountDownLatch(1);AtomicBoolean first=new AtomicBoolean(true);
        try(var store=new IoniaWorkerFileStorage(A,directory,new IoniaWorkerSettings(4,1),TimeSource.system(),false,(shard,reset)->{
            if(!reset&&first.compareAndSet(true,false)){entered.countDown();if(!release.await(5,TimeUnit.SECONDS))throw new IOException("fixture timeout");}
        },2)){
            try{
                var active=store.execute(new Command.Set(keyFor(0,4),Bytes.utf8("a")),context(1)).toCompletableFuture();assertTrue(entered.await(5,TimeUnit.SECONDS));
                var q1=store.execute(new Command.Set(keyFor(1,4),Bytes.utf8("b")),context(2)).toCompletableFuture();
                var q2=store.execute(new Command.Set(keyFor(2,4),Bytes.utf8("c")),context(3)).toCompletableFuture();
                assertEquals(ResultStatus.BUSY,run(store,new Command.Set(keyFor(3,4),Bytes.utf8("rejected")),4).status());
                release.countDown();active.get(5,TimeUnit.SECONDS);q1.get(5,TimeUnit.SECONDS);q2.get(5,TimeUnit.SECONDS);idle(store);
                var worker=store.diagnostics().workers().getFirst();assertEquals(4,worker.submitted());assertEquals(3,worker.finished());assertEquals(1,worker.rejected());
                assertEquals(ResultStatus.MISS,run(store,new Command.Get(keyFor(3,4)),5).status());
            }finally{release.countDown();}
        }
    }

}
