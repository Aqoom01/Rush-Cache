package io.openkache.runner.benchmark.correctness;

import static org.junit.jupiter.api.Assertions.*;
import static io.openkache.runner.benchmark.correctness.CorrectnessFixtures.*;
import io.openkache.core.api.*;
import io.openkache.runner.benchmark.client.*;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;

class HistoryRecorderTest {
    @Test void recordsEverySubmissionAndFullCrudResultsWithoutChangingAccounting()throws Exception{
        var h=recorder();var raw=new Memory();try(var c=new RecordingBenchmarkClient(raw,h,raw.clock::get)){
            c.submit(request(0,new Command.Set(KEY,VALUE))).toCompletableFuture().get();c.submit(request(1,new Command.Get(KEY))).toCompletableFuture().get();
            c.submit(request(2,new Command.Delete(KEY))).toCompletableFuture().get();assertEquals(3,c.snapshot().success());
        }
        var s=h.seal(raw.clock.get());assertEquals(3,s.operations().size());assertEquals(3,s.submissions());assertTrue(s.collectionComplete());
        assertEquals(VALUE,s.operations().get(1).outcome().result().value().orElseThrow());assertEquals(1,raw.closes);
        assertEquals(HistoryChecker.Verdict.PASS,HistoryChecker.check(s,Map.of(),limits()).verdict());
    }
    @Test void rejectedAdmissionCanRetryTheSameLogicalIdWithoutInventingAnEffect()throws Exception{
        var h=recorder();var raw=new Memory();raw.rejectFirst=true;var r=request(0,new Command.Set(KEY,VALUE));
        try(var c=new RecordingBenchmarkClient(raw,h,raw.clock::get)){
            assertThrows(ExecutionException.class,()->c.submit(r).toCompletableFuture().get());c.submit(r).toCompletableFuture().get();
        }
        var s=h.seal(raw.clock.get());assertEquals(1,s.operations().size());assertEquals(2,s.submissions());assertEquals(HistoryRecorder.Kind.NOT_ADMITTED,s.operations().getFirst().calls().getFirst().kind());
        assertEquals(1,raw.ledger.snapshot().admitted());assertEquals(HistoryChecker.Verdict.PASS,HistoryChecker.check(s,Map.of(),limits()).verdict());
    }
    @Test void duplicateActiveFutureIsOneLogicalOperationWithTwoObservedSubmissions()throws Exception{
        var h=recorder();var raw=new Memory();raw.hold=true;var c=new RecordingBenchmarkClient(raw,h,raw.clock::get);var r=request(0,new Command.Set(KEY,VALUE));
        var a=c.submit(r);raw.clock.incrementAndGet();var b=c.submit(r);raw.release();assertEquals(a.toCompletableFuture().get(),b.toCompletableFuture().get());c.close();
        var s=h.seal(raw.clock.get());assertEquals(2,s.submissions());assertEquals(1,s.operations().size());assertTrue(s.collectionComplete());assertEquals(1,raw.ledger.snapshot().admitted());
    }
    @Test void pendingAndLateCompletionRemainVisibleAndSealedSnapshotIsImmutable()throws Exception{
        var h=recorder();var raw=new Memory();raw.hold=true;var c=new RecordingBenchmarkClient(raw,h,raw.clock::get);var stage=c.submit(request(0,new Command.Set(KEY,VALUE)));
        var s=h.seal(raw.clock.get());assertEquals(1,s.pendingCalls());assertFalse(s.collectionComplete());raw.release();stage.toCompletableFuture().get();
        assertSame(s,h.seal(raw.clock.get()));assertEquals(1,h.lateCallbacksAfterSeal());assertEquals(1,s.pendingCalls());
        assertThrows(UnsupportedOperationException.class,()->s.operations().clear());assertThrows(ExecutionException.class,()->c.submit(request(1,new Command.Get(KEY))).toCompletableFuture().get());c.close();
    }
    @Test void capacityStopsBeforeDelegateAndMakesTheHistoryInconclusive()throws Exception{
        var h=new HistoryRecorder("run","slot","clock",new HistoryRecorder.Limits(1,1,100));var raw=new Memory();
        try(var c=new RecordingBenchmarkClient(raw,h,raw.clock::get)){
            c.submit(request(0,new Command.Get(KEY))).toCompletableFuture().get();
            assertThrows(ExecutionException.class,()->c.submit(request(1,new Command.Get(KEY))).toCompletableFuture().get());assertEquals(1,raw.calls);
        }
        var s=h.seal(raw.clock.get());assertTrue(s.failures().contains("HISTORY_CAPACITY_EXCEEDED"));assertEquals(HistoryChecker.Verdict.INCONCLUSIVE,HistoryChecker.check(s,Map.of(),limits()).verdict());
    }
    @Test void oversizedResponseCannotBeSilentlyDroppedOrReportedAsARecordedSuccess()throws Exception{
        var h=new HistoryRecorder("run","slot","clock",new HistoryRecorder.Limits(2,2,1));var raw=new Memory();raw.values.put(KEY,VALUE);
        try(var c=new RecordingBenchmarkClient(raw,h,raw.clock::get)){
            assertThrows(ExecutionException.class,()->c.submit(request(0,new Command.Get(KEY))).toCompletableFuture().get());assertEquals(1,raw.ledger.snapshot().success());
        }
        var s=h.seal(raw.clock.get());assertFalse(s.collectionComplete());assertEquals(HistoryRecorder.Kind.INVALID,s.operations().getFirst().calls().getFirst().kind());
    }
    @Test void conflictingRequestIdentityOrClockCannotEnterAQualifiedHistory(){
        var h=recorder();var r=request(0,new Command.Get(KEY));var ticket=h.begin(r,10);
        assertThrows(IllegalArgumentException.class,()->h.begin(request(0,new Command.Delete(KEY)),11));
        assertThrows(IllegalArgumentException.class,()->h.finish(ticket,outcome(r,ClientOutcome.Category.SUCCESS,plain(ResultStatus.MISS),5,8),null,20));
        assertFalse(h.seal(30).collectionComplete());
    }
    @Test void cancellingAFutureCopyDoesNotCancelTheRecordedRawOperation()throws Exception{
        var h=recorder();var raw=new Memory();raw.hold=true;try(var c=new RecordingBenchmarkClient(raw,h,raw.clock::get)){
            var stage=c.submit(request(0,new Command.Set(KEY,VALUE)));assertTrue(stage.toCompletableFuture().cancel(true));assertEquals(0,raw.cancels);
            raw.release();assertEquals(ClientOutcome.Category.SUCCESS,stage.toCompletableFuture().get().category());assertTrue(h.seal(raw.clock.get()).collectionComplete());
            c.cancel(request(0,new Command.Set(KEY,VALUE)).id("run","slot"));assertEquals(1,raw.cancels);
        }
    }
    @Test void terminalCallbackAndSealClockMisuseAreRecordedAsInvalid(){
        var h=recorder();var r=request(0,new Command.Get(KEY));var t=h.begin(r,10);var o=outcome(r,ClientOutcome.Category.SUCCESS,plain(ResultStatus.MISS),11,20);
        h.finish(t,o,null,21);assertThrows(IllegalStateException.class,()->h.finish(t,o,null,22));
        var s=h.seal(5);assertTrue(s.failures().contains("SEAL_CLOCK_ORDER"));assertTrue(s.failures().contains("DUPLICATE_TERMINAL_CALLBACK"));
    }
}
