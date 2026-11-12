package io.openkache.server.replication;

import io.openkache.core.kv.KvStorage;
import io.openkache.core.runtime.Mailbox;
import io.openkache.core.model.NodeId;
import io.openkache.core.spi.*;
import io.openkache.core.replication.raft.RaftMessage;
import java.nio.file.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class PbExperimentRuntimeTest {
    @TempDir Path root;
    @Test void freshNvmeDirectoryCannotDestroyAnExistingStore() throws Exception {
        Path store=root.resolve("new-store");
        var runtime=new PbExperimentRuntime(store,false,null);
        assertInstanceOf(KvStorage.Nvme.class,runtime.storage());
        Path sentinel=store.resolve("keep.txt");Files.writeString(sentinel,"keep");
        assertThrows(IllegalStateException.class,runtime::storage);
        assertEquals("keep",Files.readString(sentinel));
    }
    @Test void injectedIoCompletionStallExpiresAndRecordsEvidence() throws Exception {
        Path fault=root.resolve("fault.properties");
        Files.writeString(fault,"kind=io-stall\nuntil.epoch.ms="+(System.currentTimeMillis()+80)+"\n");
        var runtime=new PbExperimentRuntime(null,false,fault);
        var ran=new AtomicBoolean();long began=System.nanoTime();
        Mailbox direct=new Mailbox() {
            @Override public boolean offer(Runnable task) { task.run();return true; }
            @Override public int capacity() { return 1; }
        };
        assertTrue(runtime.kvMailbox(direct).offer(()->ran.set(true)));
        assertTrue(ran.get());assertTrue(System.nanoTime()-began>=30_000_000);
        assertTrue(runtime.json().contains("\"injected_io_stall_tasks\":1"));
    }
    @Test void localPartitionIsScopedToPlaneAndAutomaticallyExpires() throws Exception {
        Path fault=root.resolve("network.properties");
        var sent=new AtomicInteger();
        Transport transport=e->{ sent.incrementAndGet();return CompletableFuture.completedFuture(null); };
        var envelope=new PeerEnvelope(new NodeId(0),new NodeId(1),new RaftMessage.Vote(1,0,0));
        try(var runtime=new PbExperimentRuntime(null,false,fault)) {
            Files.writeString(fault,"kind=partition\nscope=udp\nuntil.epoch.ms="+(System.currentTimeMillis()+10000));
            runtime.transport(transport,"udp").send(envelope).toCompletableFuture().join();
            runtime.transport(transport,"raft").send(envelope).toCompletableFuture().join();
            assertEquals(1,sent.get());assertFalse(runtime.dropIncoming("raft"));
            Files.writeString(fault,"kind=partition\nscope=both\nuntil.epoch.ms="+(System.currentTimeMillis()+10000));
            assertTrue(runtime.dropIncoming("raft"));
            runtime.transport(transport,"raft").send(envelope).toCompletableFuture().join();assertEquals(1,sent.get());
            Files.writeString(fault,"kind=partition\nscope=both\nuntil.epoch.ms=1");
            assertFalse(runtime.dropIncoming("raft"));
            runtime.transport(transport,"raft").send(envelope).toCompletableFuture().join();assertEquals(2,sent.get());
        }
    }
}
