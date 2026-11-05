package io.openkache.core.migration.experiment;

import static org.junit.jupiter.api.Assertions.*;
import io.openkache.core.migration.postcopy.PostCopyProtocol;
import io.openkache.core.migration.precopy.PreCopyProtocol;
import io.openkache.core.migration.stopcopy.StopAndCopyProtocol;
import io.openkache.core.migration.strategy.CopyDrivers;
import io.openkache.core.tablet.Change;
import io.openkache.core.tablet.RoutingTable;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class CopyDriversTest {
    @Test void installationFailureAfterPublishRetriesBeforeAnySourceCleanup(){
        for(String copy:CopyDrivers.METHODS){
            var fixture=new MultiMigrationFixture(1,8,1,"independent",copy,1024,256);var handle=fixture.handles.getFirst();var expected=Map.copyOf(handle.sourceData);var attempts=new AtomicInteger();var commits=new AtomicInteger();var selected=new CopyDrivers.Session[1];
            var binding=new CopyDrivers.Binding(){
                @Override public RoutingTable current(){return fixture.routing.get();}
                @Override public boolean compareAndSet(RoutingTable before,RoutingTable after){boolean result=fixture.routing.compareAndSet(before,after);if(result)commits.incrementAndGet();return result;}
                @Override public PreCopyProtocol.Source source(){return handle.source;}
                @Override public StopAndCopyProtocol.Destination stopDestination(){return handle.snapshotReceiver;}
                @Override public PreCopyProtocol.Destination preDestination(){return handle.receiver;}
                @Override public PostCopyProtocol.Source postSource(){return handle.source;}
                @Override public void installed(Change change,String variant,RoutingTable before,RoutingTable after){
                    if(attempts.incrementAndGet()==1)throw new IllegalStateException("transient install failure");
                    if(copy.equals("post-copy"))fixture.nodes.get(change.to()).tablets.put(change.unit(),selected[0]::postData);
                    else{handle.destinationData.putAll(copy.equals("pre-copy")?handle.receiver.data():handle.snapshotReceiver.data);fixture.nodes.get(change.to()).tablets.put(change.unit(),()->handle.destinationData);}
                }
            };
            selected[0]=CopyDrivers.create(copy,handle.change,binding,new CopyDrivers.Settings(1024,256,10));var driver=selected[0];driver.begin();boolean injected=false;
            for(int step=0;step<1000&&!driver.done();step++){
                long before=driver.sentBytes();try{driver.step(1024);}catch(IllegalStateException transientFailure){assertFalse(injected);injected=true;assertTrue(driver.published());assertFalse(handle.sourceData.isEmpty(),"source survives failed install");}
                assertTrue(driver.sentBytes()-before<=1024);
            }
            assertTrue(injected);assertTrue(driver.done(),copy);assertEquals(1,commits.get(),copy);assertEquals(2,attempts.get(),copy);assertTrue(handle.sourceData.isEmpty());assertEquals(expected,copy.equals("post-copy")?driver.postData():handle.destinationData);
        }
    }
}
