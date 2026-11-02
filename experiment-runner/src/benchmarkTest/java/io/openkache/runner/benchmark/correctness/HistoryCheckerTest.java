package io.openkache.runner.benchmark.correctness;

import static org.junit.jupiter.api.Assertions.*;
import static io.openkache.runner.benchmark.correctness.CorrectnessFixtures.*;
import io.openkache.core.api.*;
import io.openkache.core.model.Bytes;
import io.openkache.runner.benchmark.client.ClientOutcome;
import java.util.*;
import org.junit.jupiter.api.Test;

class HistoryCheckerTest {
    @Test void concurrentDoubleDeleteCannotBothRemoveOneExistingValue(){
        var h=recorder();success(h,0,new Command.Delete(KEY),plain(ResultStatus.OK),10,40);success(h,1,new Command.Delete(KEY),plain(ResultStatus.OK),20,30);
        assertEquals(HistoryChecker.Verdict.VIOLATION,HistoryChecker.check(h.seal(50),Map.of(KEY,VALUE),limits()).verdict());
    }
    @Test void concurrentOperationsCanHaveALegalNonCompletionOrder(){
        var h=recorder();success(h,0,new Command.Set(KEY,VALUE),plain(ResultStatus.OK),10,50);success(h,1,new Command.Get(KEY),value(VALUE),20,30);
        var result=HistoryChecker.check(h.seal(60),Map.of(),limits());assertEquals(HistoryChecker.Verdict.PASS,result.verdict());assertEquals(List.of(0,1),result.witness());
    }
    @Test void staleMissAfterCompletedSetIsAViolation(){
        var h=recorder();success(h,0,new Command.Set(KEY,VALUE),plain(ResultStatus.OK),10,20);success(h,1,new Command.Get(KEY),plain(ResultStatus.MISS),30,40);
        assertEquals(HistoryChecker.Verdict.VIOLATION,HistoryChecker.check(h.seal(50),Map.of(),limits()).verdict());
    }
    @Test void wrongBytesAndDeleteReturnSemanticsAreCheckedExactly(){
        var wrong=recorder();success(wrong,0,new Command.Get(KEY),value(Bytes.utf8("wrong")),10,20);
        assertEquals(HistoryChecker.Verdict.VIOLATION,HistoryChecker.check(wrong.seal(30),Map.of(KEY,VALUE),limits()).verdict());
        var delete=recorder();success(delete,0,new Command.Delete(KEY),plain(ResultStatus.MISS),10,20);
        assertEquals(HistoryChecker.Verdict.VIOLATION,HistoryChecker.check(delete.seal(30),Map.of(KEY,VALUE),limits()).verdict());
        var legal=recorder();success(legal,0,new Command.Delete(KEY),plain(ResultStatus.OK),10,20);success(legal,1,new Command.Get(KEY),plain(ResultStatus.MISS),30,40);
        assertEquals(HistoryChecker.Verdict.PASS,HistoryChecker.check(legal.seal(50),Map.of(KEY,VALUE),limits()).verdict());
    }
    @Test void timedOutWriteMayTakeEffectAfterItsLogicalDeadlineButCannotPassTheRun(){
        var h=recorder();record(h,0,new Command.Set(KEY,VALUE),ClientOutcome.Category.NO_RESPONSE,null,10,20);success(h,1,new Command.Get(KEY),value(VALUE),30,40);
        var result=HistoryChecker.check(h.seal(50),Map.of(),limits());assertEquals(HistoryChecker.Verdict.INCONCLUSIVE,result.verdict());assertEquals(List.of(0,1),result.witness());
        var bad=recorder();record(bad,0,new Command.Set(KEY,VALUE),ClientOutcome.Category.NO_RESPONSE,null,10,20);success(bad,1,new Command.Get(KEY),value(Bytes.utf8("other")),30,40);
        assertEquals(HistoryChecker.Verdict.VIOLATION,HistoryChecker.check(bad.seal(50),Map.of(),limits()).verdict());
    }
    @Test void pendingWriteCanExplainALaterReadButCannotPrecedeItsOwnInvocation(){
        var h=recorder();h.begin(request(0,new Command.Set(KEY,VALUE)),10);success(h,1,new Command.Get(KEY),value(VALUE),30,40);
        assertEquals(HistoryChecker.Verdict.INCONCLUSIVE,HistoryChecker.check(h.seal(50),Map.of(),limits()).verdict());
        var bad=recorder();success(bad,0,new Command.Get(KEY),value(VALUE),10,20);bad.begin(request(1,new Command.Set(KEY,VALUE)),30);
        assertEquals(HistoryChecker.Verdict.VIOLATION,HistoryChecker.check(bad.seal(40),Map.of(),limits()).verdict());
    }
    @Test void unknownInitialStateAndNoObservedOperationsAreNotPass(){
        var h=recorder();success(h,0,new Command.Get(KEY),plain(ResultStatus.MISS),10,20);var s=h.seal(30);
        assertEquals(HistoryChecker.Verdict.INCONCLUSIVE,HistoryChecker.check(s,null,limits()).verdict());
        assertEquals(HistoryChecker.Verdict.INCONCLUSIVE,HistoryChecker.check(recorder().seal(30),Map.of(),limits()).verdict());
    }
    @Test void operationStateAndTimeBudgetsReturnInconclusive(){
        var h=recorder();success(h,0,new Command.Set(KEY,VALUE),plain(ResultStatus.OK),10,20);success(h,1,new Command.Get(KEY),value(VALUE),30,40);var s=h.seal(50);
        assertEquals("OPERATION_LIMIT",HistoryChecker.check(s,Map.of(),new HistoryChecker.Limits(1,100,1_000_000_000)).reason());
        assertEquals("STATE_LIMIT",HistoryChecker.check(s,Map.of(),new HistoryChecker.Limits(24,1,1_000_000_000)).reason());
        assertEquals("TIME_LIMIT",HistoryChecker.check(s,Map.of(),new HistoryChecker.Limits(24,100,1)).reason());
    }
    @Test void interruptIsPreservedAndNeverReportedAsPass(){
        var h=recorder();success(h,0,new Command.Get(KEY),plain(ResultStatus.MISS),10,20);
        Thread.currentThread().interrupt();try{assertEquals("CHECKER_INTERRUPTED",HistoryChecker.check(h.seal(30),Map.of(),limits()).reason());assertTrue(Thread.currentThread().isInterrupted());}finally{Thread.interrupted();}
    }
    @Test void forgedMissingCallsAndCountsCannotBeQualified(){
        var h=recorder();success(h,0,new Command.Get(KEY),plain(ResultStatus.MISS),10,20);var s=h.seal(30);
        var forged=new HistoryRecorder.Snapshot(s.token(),s.runId(),s.loadSlot(),s.clockId(),s.sealedNanos(),s.limits(),s.operations(),List.of(),2,s.retainedPayloadBytes(),0);
        assertEquals("HISTORY_COUNT_MISMATCH",HistoryChecker.check(forged,Map.of(),limits()).reason());
    }
    @Test void failedReadIsRecordedButDoesNotCreateAStateMutation(){
        var h=recorder();success(h,0,new Command.Set(KEY,VALUE),plain(ResultStatus.OK),10,20);record(h,1,new Command.Get(KEY),ClientOutcome.Category.NO_RESPONSE,null,30,40);
        assertEquals(HistoryChecker.Verdict.INCONCLUSIVE,HistoryChecker.check(h.seal(50),Map.of(),limits()).verdict());
    }
}
