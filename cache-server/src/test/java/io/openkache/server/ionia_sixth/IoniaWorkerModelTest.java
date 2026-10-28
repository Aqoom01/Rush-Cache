package io.openkache.server.ionia_sixth;

import static org.junit.jupiter.api.Assertions.*;
import static io.openkache.server.ionia_sixth.IoniaWorkerStorageTest.*;
import io.openkache.core.api.*;
import io.openkache.core.model.Bytes;
import io.openkache.core.replication.ionia_sixth.IoniaWorkerSettings;
import io.openkache.core.spi.TimeSource;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

/** 단일 제출 thread와 고정 shard FIFO의 실제 파일 결과를 참조 상태와 비교한다. */
@Timeout(45)
class IoniaWorkerModelTest {
    @TempDir Path directory;
    record Expected(ResultStatus status,Optional<Bytes> value) { }
    @Test void V04B_09_fixedSeedCrudAgainstReferenceStateAtEveryWorkerCount()throws Exception{
        List<Bytes> keys=List.of(Bytes.utf8("Aa"),Bytes.utf8("BB"),Bytes.empty(),keyFor(1,4),keyFor(2,4));
        for(int workers:new int[]{1,2,4,8})for(int seed=0;seed<16;seed++){
            Random random=new Random(seed);Map<Bytes,Bytes> state=new HashMap<>();List<Expected> expected=new ArrayList<>();List<CompletableFuture<Result>> futures=new ArrayList<>();
            try(var store=new IoniaWorkerFileStorage(A,directory.resolve("w"+workers+"-s"+seed),new IoniaWorkerSettings(4,workers),TimeSource.system(),false)){
                for(int i=0;i<64;i++){
                    Bytes key=keys.get(random.nextInt(keys.size()));Command command;
                    switch(random.nextInt(3)){
                        case 0->{Bytes value=Bytes.utf8("value-"+i);command=new Command.Set(key,value);state.put(key,value);expected.add(new Expected(ResultStatus.OK,Optional.empty()));}
                        case 1->{command=new Command.Get(key);Bytes value=state.get(key);expected.add(new Expected(value==null?ResultStatus.MISS:ResultStatus.OK,Optional.ofNullable(value)));}
                        default->{command=new Command.Delete(key);expected.add(new Expected(state.remove(key)==null?ResultStatus.MISS:ResultStatus.OK,Optional.empty()));}
                    }
                    futures.add(store.execute(command,context(i+1)).toCompletableFuture());
                }
                for(int i=0;i<futures.size();i++){Result actual=futures.get(i).get(5,TimeUnit.SECONDS);assertEquals(expected.get(i).status(),actual.status(),"W="+workers+" seed="+seed+" index="+i);assertEquals(expected.get(i).value(),actual.value());}
                idle(store);assertTrue(store.diagnostics().peakTasks()<=Math.min(workers,4));
            }
        }
        System.out.println("V04b storage: S4, W1/2/4/8, 16 seeds x 64 CRUD; queued FIFO/reference comparison, not distributed linearizability proof");
    }
}
