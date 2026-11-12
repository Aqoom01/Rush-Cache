package io.openkache.runner.benchmark.correctness;

import static org.junit.jupiter.api.Assertions.*;
import static io.openkache.runner.benchmark.correctness.CorrectnessFixtures.*;
import io.openkache.core.api.*;
import io.openkache.runner.benchmark.measurement.CorrectnessRunWriter;
import io.openkache.runner.benchmark.execution.DrainProbe;
import io.openkache.runner.benchmark.client.ClientOutcome;
import java.nio.file.*;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class CorrectnessRunWriterTest {
    @TempDir Path directory;
    @Test void exportsFullPayloadAndVerifiableCompletionHashWithoutOverwriting()throws Exception{
        var h=recorder();success(h,0,new Command.Set(KEY,VALUE),plain(ResultStatus.OK),10,20);success(h,1,new Command.Get(KEY),value(VALUE),30,40);var s=h.seal(50);Path output=directory.resolve("pass");
        var export=CorrectnessRunWriter.write(output,s,Map.of(),limits(),null,null);assertEquals(HistoryChecker.Verdict.PASS,export.historyResult().verdict());
        var lines=Files.readAllLines(output.resolve("history.jsonl"));assertEquals(2,lines.size());assertTrue(lines.get(0).contains("\"base64\":\"aw==\""));assertTrue(lines.get(1).contains("\"base64\":\"YQ==\""));
        var digest=HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(output.resolve("integrity.json"))));assertEquals("integrity_sha256="+digest+"\n",Files.readString(output.resolve("COMPLETE")));
        assertTrue(Files.readString(output.resolve("ack-ledger.json")).contains("not_run"));assertEquals(7,Files.list(output).count());
        assertThrows(FileAlreadyExistsException.class,()->CorrectnessRunWriter.write(output,s,Map.of(),limits(),null,null));
    }
    @Test void completeFileMeansStoredEvidenceAndDoesNotConvertAViolationToPass()throws Exception{
        var h=recorder();success(h,0,new Command.Set(KEY,VALUE),plain(ResultStatus.OK),10,20);success(h,1,new Command.Get(KEY),plain(ResultStatus.MISS),30,40);
        var e=CorrectnessRunWriter.write(directory.resolve("violation"),h.seal(50),Map.of(),limits(),null,null);assertEquals(HistoryChecker.Verdict.VIOLATION,e.historyResult().verdict());assertTrue(Files.exists(e.directory().resolve("COMPLETE")));assertTrue(Files.readString(e.directory().resolve("correctness.json"),StandardCharsets.UTF_8).contains("VIOLATION"));
    }
    @Test void rejectsCrossRunEvidenceBeforeCreatingDirectory(){var a=recorder();success(a,0,new Command.Set(KEY,VALUE),plain(ResultStatus.OK),10,20);var sa=a.seal(30);var b=recorder();success(b,0,new Command.Set(KEY,VALUE),plain(ResultStatus.OK),10,20);var l=AckLedger.build(b.seal(30),Set.of(KEY),true);Path output=directory.resolve("mismatch");
        assertThrows(IllegalArgumentException.class,()->CorrectnessRunWriter.write(output,sa,Map.of(),limits(),l,null));assertFalse(Files.exists(output));}
    @Test void exportsPersistentSemanticControlsAndRejectsForgedAuditPass()throws Exception{
        Path root=Path.of("build","correctness-control-fixtures");Files.createDirectories(root);
        var good=recorder();success(good,0,new Command.Set(KEY,VALUE),plain(ResultStatus.OK),10,20);var s=good.seal(50);var ledger=AckLedger.build(s,Set.of(KEY),true);
        var read=request(1,new Command.Get(KEY));var missing=new AckLedger.Read(read,outcome(read,ClientOutcome.Category.SUCCESS,plain(ResultStatus.MISS),61,65),"clock");
        var audit=AckLedger.audit(ledger,"injected-value-loss",60,new DrainProbe.Observation(DrainProbe.Status.READY,"control",Map.of()),List.of(missing));assertEquals(HistoryChecker.Verdict.VIOLATION,audit.verdict());
        var forged=new AckLedger.Audit(audit.historyToken(),HistoryChecker.Verdict.PASS,audit.reason(),audit.barrierLabel(),audit.barrierNanos(),audit.readiness(),audit.items(),audit.excludedKeys());
        assertThrows(IllegalArgumentException.class,()->CorrectnessRunWriter.write(directory.resolve("forged-pass"),s,Map.of(),limits(),ledger,forged));
        exportControl(root,"ack-loss",s,ledger,audit,HistoryChecker.Verdict.PASS);
        exportControl(root,"pass",s,null,null,HistoryChecker.Verdict.PASS);
        var stale=recorder();success(stale,0,new Command.Set(KEY,VALUE),plain(ResultStatus.OK),10,20);success(stale,1,new Command.Get(KEY),plain(ResultStatus.MISS),30,40);
        exportControl(root,"stale-read",stale.seal(50),null,null,HistoryChecker.Verdict.VIOLATION);
        var pending=recorder();pending.begin(request(0,new Command.Set(KEY,VALUE)),10);success(pending,1,new Command.Get(KEY),value(VALUE),30,40);
        exportControl(root,"pending",pending.seal(50),null,null,HistoryChecker.Verdict.INCONCLUSIVE);
    }
    private static void exportControl(Path root,String name,HistoryRecorder.Snapshot s,AckLedger.Ledger l,AckLedger.Audit a,HistoryChecker.Verdict expected)throws Exception{
        var e=CorrectnessRunWriter.write(root.resolve(name+"-"+UUID.randomUUID()),s,Map.of(),limits(),l,a);assertEquals(expected,e.historyResult().verdict());System.out.println("CORRECTNESS_CONTROL="+e.directory());
    }
}
