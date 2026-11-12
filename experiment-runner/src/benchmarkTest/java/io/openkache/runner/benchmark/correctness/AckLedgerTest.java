package io.openkache.runner.benchmark.correctness;

import static org.junit.jupiter.api.Assertions.*;
import static io.openkache.runner.benchmark.correctness.CorrectnessFixtures.*;
import io.openkache.core.api.*;
import io.openkache.core.model.Bytes;
import io.openkache.runner.benchmark.client.ClientOutcome;
import io.openkache.runner.benchmark.execution.DrainProbe;
import java.util.*;
import org.junit.jupiter.api.Test;

class AckLedgerTest {
    private static final DrainProbe.Observation READY=new DrainProbe.Observation(DrainProbe.Status.READY,"local_quiescent",Map.of("applied",1L));
    private static AckLedger.Ledger ledger(){var h=recorder();success(h,0,new Command.Set(KEY,VALUE),plain(ResultStatus.OK),10,20);return AckLedger.build(h.seal(30),Set.of(KEY),true);}
    private static AckLedger.Read read(Result result,long start,String clock){var r=request(1,new Command.Get(KEY));return new AckLedger.Read(r,outcome(r,ClientOutcome.Category.SUCCESS,result,start,start+2),clock);}
    @Test void acknowledgedSetMatchesExactPayload(){var a=AckLedger.audit(ledger(),"drained",30,READY,List.of(read(value(VALUE),31,"clock")));assertEquals(HistoryChecker.Verdict.PASS,a.verdict());}
    @Test void acknowledgedSetMissOrWrongBytesIsAViolation(){for(var result:List.of(plain(ResultStatus.MISS),value(Bytes.utf8("wrong")))){assertEquals(HistoryChecker.Verdict.VIOLATION,AckLedger.audit(ledger(),"drained",30,READY,List.of(read(result,31,"clock"))).verdict());}}
    @Test void deleteRequiresAbsenceAndDetectsResurrection(){var h=recorder();success(h,0,new Command.Delete(KEY),plain(ResultStatus.OK),10,20);var l=AckLedger.build(h.seal(30),Set.of(KEY),true);
        assertEquals(HistoryChecker.Verdict.PASS,AckLedger.audit(l,"rejoin",30,READY,List.of(read(plain(ResultStatus.MISS),31,"clock"))).verdict());
        assertEquals(HistoryChecker.Verdict.VIOLATION,AckLedger.audit(l,"rejoin",30,READY,List.of(read(value(VALUE),31,"clock"))).verdict());}
    @Test void overwriteAndUnknownNamespaceAreExcludedRatherThanCalledLost(){var h=recorder();success(h,0,new Command.Set(KEY,VALUE),plain(ResultStatus.OK),10,20);success(h,1,new Command.Set(KEY,Bytes.utf8("b")),plain(ResultStatus.OK),30,40);var s=h.seal(50);
        assertEquals("MULTIPLE_OR_UNCERTAIN_MUTATIONS",AckLedger.build(s,Set.of(KEY),true).exclusions().get(KEY));
        var unknown=AckLedger.build(s,Set.of(KEY),false);assertEquals("NAMESPACE_NOT_ATTESTED_EXCLUSIVE",unknown.exclusions().get(KEY));assertEquals(HistoryChecker.Verdict.INCONCLUSIVE,AckLedger.audit(unknown,"x",50,READY,List.of()).verdict());}
    @Test void timeoutAndUnfinishedMutationsAreNotAcknowledged(){var h=recorder();record(h,0,new Command.Set(KEY,VALUE),ClientOutcome.Category.NO_RESPONSE,null,10,20);assertTrue(AckLedger.build(h.seal(30),Set.of(KEY),true).entries().isEmpty());
        var pending=recorder();pending.begin(request(0,new Command.Set(KEY,VALUE)),10);assertTrue(AckLedger.build(pending.seal(20),Set.of(KEY),true).entries().isEmpty());}
    @Test void unknownBarrierClockOrPreBarrierReadCannotPass(){var l=ledger();
        assertEquals(HistoryChecker.Verdict.INCONCLUSIVE,AckLedger.audit(l,"x",30,null,List.of(read(value(VALUE),31,"clock"))).verdict());
        assertEquals(HistoryChecker.Verdict.INCONCLUSIVE,AckLedger.audit(l,"x",19,READY,List.of(read(value(VALUE),31,"clock"))).verdict());
        assertEquals(HistoryChecker.Verdict.INCONCLUSIVE,AckLedger.audit(l,"x",30,READY,List.of(read(value(VALUE),29,"clock"))).verdict());
        assertEquals(HistoryChecker.Verdict.INCONCLUSIVE,AckLedger.audit(l,"x",30,READY,List.of(read(value(VALUE),31,"other_host"))).verdict());}
    @Test void missingDuplicateOrUnsuccessfulAuditReadCannotPass(){var l=ledger();var read=read(value(VALUE),31,"clock");
        for(var reads:List.of(List.<AckLedger.Read>of(),List.of(read,read))){assertEquals(HistoryChecker.Verdict.INCONCLUSIVE,AckLedger.audit(l,"x",30,READY,reads).verdict());}
        var r=request(1,new Command.Get(KEY));var failed=new AckLedger.Read(r,outcome(r,ClientOutcome.Category.NO_RESPONSE,null,31,40),"clock");assertEquals(HistoryChecker.Verdict.INCONCLUSIVE,AckLedger.audit(l,"x",30,READY,List.of(failed)).verdict());}
    @Test void retrySubmissionsDoNotDuplicateMutationButUncertainSecondWriteExcludesKey(){var h=recorder();var r=request(0,new Command.Set(KEY,VALUE));var t=h.begin(r,9);h.finish(t,null,new java.util.concurrent.RejectedExecutionException(),9);success(h,0,r.command(),plain(ResultStatus.OK),10,20);assertEquals(1,AckLedger.build(h.seal(30),Set.of(KEY),true).entries().size());
        var h2=recorder();success(h2,0,r.command(),plain(ResultStatus.OK),10,20);h2.begin(request(1,new Command.Delete(KEY)),25);var l=AckLedger.build(h2.seal(30),Set.of(KEY),true);assertTrue(l.entries().isEmpty());assertEquals("MULTIPLE_OR_UNCERTAIN_MUTATIONS",l.exclusions().get(KEY));}
}
