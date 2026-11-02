package io.openkache.runner.benchmark.measurement;

import static org.junit.jupiter.api.Assertions.*;
import static io.openkache.runner.benchmark.measurement.JvmFixtures.*;
import io.openkache.runner.benchmark.execution.*;
import java.nio.file.*;
import java.util.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@Timeout(15)
class JvmRunnerIntegrationTest {
    @ParameterizedTest @ValueSource(booleans={false,true})
    void bothRunnersExportQualifiedResourceEvidenceAndPreserveLatencyObserver(boolean open)throws Exception{
        var client=new Memory();var generator=generator();var jvm=new JvmWindowCollector(settings(),JvmSnapshotSource.system(),client::snapshot);var latency=new LatencyCollector("run","slot","clock",ms(30_000));
        Path root=Path.of("build","jvm-model-fixtures");Files.createDirectories(root);Path output=root.resolve((open?"open":"closed")+"-"+UUID.randomUUID());JvmRunWriter.Export exported;
        if(open){var plan=open();var run=new OpenLoopRunner(generator,plan,client,ready(),new OpenLoopRunner.Observer(){@Override public void outcome(OpenLoopResult.ScheduledOutcome e){jvm.outcome(e);latency.outcome(e);}},jvm).run();assertTrue(run.passed(),run.toString());exported=JvmRunWriter.write(output,generator.config(),plan,run,jvm.snapshot());}
        else{var plan=closed();var run=new ClosedLoopRunner(generator,plan,client,ready(),(r,d,o)->{jvm.outcome(r,d,o);latency.outcome(r,d,o);},jvm).run();assertTrue(run.passed(),run.toString());exported=JvmRunWriter.write(output,generator.config(),plan,run,jvm.snapshot());}
        assertTrue(exported.valid(),exported.toString());assertEquals(19,jvm.snapshot().requestWindows().stream().mapToLong(JvmWindowCollector.RequestWindow::outcomes).sum());assertEquals(19,latency.freeze().counts().values().stream().mapToLong(LatencyCollector.Tally::observed).sum());assertEquals(1,client.closes);assertEquals(8,jvm.snapshot().transitions().size());try(var files=Files.list(output)){assertEquals(7,files.count());}System.out.println("JVM_MODEL_EXPORT="+output.toAbsolutePath());
    }
    @ParameterizedTest @ValueSource(booleans={false,true})
    void collectionFailureCannotSkipClientCleanupOrPassRun(boolean open){var client=new Memory();var jvm=new JvmWindowCollector(settings(),()->{throw new IllegalStateException("broken source");},client::snapshot);if(open){assertFalse(new OpenLoopRunner(generator(),open(),client,ready(),jvm,jvm).run().passed());}else{assertFalse(new ClosedLoopRunner(generator(),closed(),client,ready(),jvm,jvm).run().passed());}assertEquals(1,client.closes);assertTrue(jvm.snapshot().samplerTerminated());assertFalse(jvm.snapshot().valid());}
    @ParameterizedTest @ValueSource(booleans={false,true})
    void backendFailureStillFinishesSamplerAndPreservesPartialEvidence(boolean open){var client=new Memory();client.fail=true;var jvm=new JvmWindowCollector(settings(),JvmSnapshotSource.system(),client::snapshot);if(open){assertFalse(new OpenLoopRunner(generator(),open(),client,ready(),jvm,jvm).run().passed());}else{assertFalse(new ClosedLoopRunner(generator(),closed(),client,ready(),jvm,jvm).run().passed());}assertEquals(1,client.closes);assertTrue(jvm.snapshot().samplerTerminated());assertTrue(jvm.snapshot().samples().stream().anyMatch(s->s.client().noResponse()>0));}
}
