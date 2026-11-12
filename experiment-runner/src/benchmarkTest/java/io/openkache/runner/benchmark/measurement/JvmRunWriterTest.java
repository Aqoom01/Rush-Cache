package io.openkache.runner.benchmark.measurement;

import static org.junit.jupiter.api.Assertions.*;
import static io.openkache.runner.benchmark.measurement.JvmFixtures.*;
import io.openkache.runner.benchmark.execution.*;
import java.nio.file.*;
import java.security.MessageDigest;
import java.util.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

class JvmRunWriterTest {
    @TempDir Path directory;
    @Test void validStoreHasVerifiableHashAndCannotOverwrite()throws Exception{
        var client=new Memory();var jvm=new JvmWindowCollector(settings(),JvmSnapshotSource.system(),client::snapshot);var gen=generator();var plan=closed();var result=new ClosedLoopRunner(gen,plan,client,ready(),jvm,jvm).run();assertTrue(result.passed());
        var out=JvmRunWriter.write(directory.resolve("valid"),gen.config(),plan,result,jvm.snapshot());assertTrue(out.valid());var digest=HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(out.directory().resolve("integrity.json"))));
        assertEquals("integrity_sha256="+digest+"\n",Files.readString(out.directory().resolve("COMPLETE")));assertThrows(FileAlreadyExistsException.class,()->JvmRunWriter.write(out.directory(),gen.config(),plan,result,jvm.snapshot()));
        assertTrue(Files.readString(out.directory().resolve("jvm-result.json")).contains("sampled_lower_bound"));
    }
    @Test void missingOutcomeObserverCannotProduceQualifiedEvidence()throws Exception{
        var client=new Memory();var jvm=new JvmWindowCollector(settings(),JvmSnapshotSource.system(),client::snapshot);var gen=generator();var plan=closed();var result=new ClosedLoopRunner(gen,plan,client,ready(),(r,d,o)->{},jvm).run();assertTrue(result.passed());
        Path parent=Path.of("build","jvm-model-controls");Files.createDirectories(parent);var out=JvmRunWriter.write(parent.resolve("missing-observer-"+UUID.randomUUID()),gen.config(),plan,result,jvm.snapshot());assertFalse(out.valid());assertTrue(out.issues().contains("FINAL_WINDOW_ACCOUNTING"));assertTrue(Files.exists(out.directory().resolve("COMPLETE")));System.out.println("JVM_MODEL_CONTROL="+out.directory());
    }
    @Test void wrongRunSnapshotIsRejectedBeforeCreatingDirectory(){
        var client=new Memory();var jvm=new JvmWindowCollector(settings(),JvmSnapshotSource.system(),client::snapshot);var gen=generator();var plan=closed();var result=new ClosedLoopRunner(gen,plan,client,ready(),jvm,jvm).run();var s=jvm.snapshot();
        var other=new JvmWindowCollector.Settings("other",s.settings().loadSlot(),s.settings().clockId(),s.settings().role(),s.settings().limits(),s.settings().requiredSources());
        var forged=new JvmWindowCollector.Snapshot(s.token(),other,s.anchorNanos(),s.finishedNanos(),s.samples(),s.transitions(),s.requestWindows(),s.issues(),s.samplerTerminated(),s.lateCallbacks(),s.sampledPeakPending());
        assertThrows(IllegalArgumentException.class,()->JvmRunWriter.write(directory.resolve("wrong"),gen.config(),plan,result,forged));assertFalse(Files.exists(directory.resolve("wrong")));
    }
    @Test void sourceFailureProducesInvalidCompleteEvidenceRatherThanMissingOrPassing()throws Exception{
        var client=new Memory();var jvm=new JvmWindowCollector(settings(),()->{throw new IllegalStateException("injected failure");},client::snapshot);var gen=generator();var plan=closed();var result=new ClosedLoopRunner(gen,plan,client,ready(),jvm,jvm).run();assertFalse(result.passed());
        Path parent=Path.of("build","jvm-model-controls");Files.createDirectories(parent);var out=JvmRunWriter.write(parent.resolve("source-failure-"+UUID.randomUUID()),gen.config(),plan,result,jvm.snapshot());assertFalse(out.valid());assertTrue(out.issues().contains("EXECUTION_NOT_PASSED"));assertTrue(Files.exists(out.directory().resolve("COMPLETE")));System.out.println("JVM_MODEL_CONTROL="+out.directory());
    }
    @Test void forgedDerivedCpuDeltaCannotBeExportedAsValid()throws Exception{
        var client=new Memory();var jvm=new JvmWindowCollector(settings(),JvmSnapshotSource.system(),client::snapshot);var gen=generator();var plan=closed();var result=new ClosedLoopRunner(gen,plan,client,ready(),jvm,jvm).run();assertTrue(result.passed());var s=jvm.snapshot();
        var samples=new ArrayList<>(s.samples());var old=samples.get(1);var deltas=new HashMap<>(old.resourceDeltas());var actual=deltas.get("process_cpu_ns").value();assertNotNull(actual);deltas.put("process_cpu_ns",JvmSnapshotSource.Value.measured(actual+1,"ns"));
        samples.set(1,new JvmWindowCollector.Sample(old.sequence(),old.stage(),old.intervalStartNanos(),old.intervalEndNanos(),old.collectionStartedNanos(),old.collectionEndedNanos(),old.baseline(),old.jvm(),old.client(),deltas,old.clientDeltas()));
        var forged=new JvmWindowCollector.Snapshot(s.token(),s.settings(),s.anchorNanos(),s.finishedNanos(),samples,s.transitions(),s.requestWindows(),s.issues(),s.samplerTerminated(),s.lateCallbacks(),s.sampledPeakPending());
        var out=JvmRunWriter.write(directory.resolve("forged"),gen.config(),plan,result,forged);assertFalse(out.valid());assertTrue(out.issues().contains("RESOURCE_DELTA_MISMATCH:process_cpu_ns"));
    }
}
