package io.openkache.core.replication.ionia_third;

import static org.junit.jupiter.api.Assertions.*;
import io.openkache.core.model.*;
import io.openkache.core.api.Command;
import io.openkache.core.replication.ionia.*;
import java.util.*;
import org.junit.jupiter.api.Test;

class IoniaBudgetHistoryTest {
    private static final NodeId B = new NodeId(2), C = new NodeId(3);
    private static final Bytes AKEY = Bytes.utf8("a"), BKEY = Bytes.utf8("b"), CKEY = Bytes.utf8("c");
    private static IoniaBudgetHistory history(int n) { return new IoniaBudgetHistory(new IoniaHistorySettings(n)); }

    @Test void V02_01_zeroMatchesOriginalAcrossReportsRewritesTrimsAndRebuilds() {
        for (int seed = 0; seed < 64; seed++) {
            Random random = new Random(seed); IoniaHistory original = new IoniaHistory(); IoniaBudgetHistory variant = history(0);
            for (int step = 1; step <= 200; step++) {
                Bytes key = Bytes.utf8("k" + random.nextInt(4));
                original.modified(key, step); variant.modified(key, step);
                for (NodeId node : List.of(B, C)) {
                    int progress = random.nextInt(step + 1); original.report(node, progress); variant.report(node, progress);
                }
                Set<NodeId> active = switch (random.nextInt(4)) { case 0 -> Set.of(); case 1 -> Set.of(B); case 2 -> Set.of(C); default -> Set.of(B, C); };
                int local = random.nextInt(step + 1); original.trim(active, local); variant.trim(active, local, step);
                assertEquals(original.size(), variant.size()); assertEquals(original.lastTrimmedIndex(), variant.lastTrimmedIndex());
                for (int i = 0; i < 5; i++) assertEquals(original.index(Bytes.utf8("k" + i)), variant.index(Bytes.utf8("k" + i)));
                if (step % 37 == 0) {
                    List<IoniaMessage.LogEntry> entries = List.of(entry(step, key)); original.rebuild(entries); variant.rebuild(entries);
                    assertEquals(original.lastTrimmedIndex(), variant.lastTrimmedIndex()); assertEquals(original.index(key), variant.index(key));
                }
            }
        }
    }
    private static IoniaMessage.LogEntry entry(long index, Bytes key) {
        return new IoniaMessage.LogEntry(index, new ClientId(1), new OperationId(index), new Command.Set(key, Bytes.utf8("v")));
    }
    @Test void V02_05_documentedCutoffsDifferFromTheZeroControl() {
        for (int n : new int[]{0, 1, 2, 3, Integer.MAX_VALUE}) {
            IoniaBudgetHistory h = history(n); h.modified(AKEY, 10); h.trim(Set.of(), 10, 0);
            // Establish the specified L=10 state without changing the selected target policy.
            if (n > 0) { h.rebuild(List.of(entry(10, AKEY))); }
            h.modified(AKEY, 20); h.modified(BKEY, 60); h.modified(CKEY, 90); h.report(B, 80); h.report(C, 70);
            h.trim(Set.of(B, C), 100, 1);
            long expected = n == 0 ? 70 : n == 1 ? 60 : n == 2 ? 20 : 10;
            assertEquals(expected, h.lastTrimmedIndex(), "N=" + n);
            assertEquals(n <= 1 ? 1 : n == 2 ? 2 : 3, h.size()); assertEquals(90, h.index(CKEY));
        }
    }
    @Test void V02_03_04_unknownActiveAndRegressingSafeBoundNeverAdvanceLti() {
        IoniaBudgetHistory h = history(1); h.modified(AKEY, 20); h.modified(BKEY, 30);
        h.trim(Set.of(B), 40, 1); assertEquals(0, h.lastTrimmedIndex()); assertEquals(1, h.snapshot().overshoot());
        h.report(B, 25); h.trim(Set.of(B), 40, 2); assertEquals(20, h.lastTrimmedIndex());
        h.modified(AKEY, 40); h.trim(Set.of(B, C), 50, 3); assertEquals(20, h.lastTrimmedIndex());
        assertEquals(IoniaBudgetHistory.Reason.NO_PROGRESS, h.snapshot().lastDecision().orElseThrow().reason());
        h.report(B, 1); h.report(C, 40); h.trim(Set.of(B, C), 50, 4); assertEquals(20, h.lastTrimmedIndex());
    }
    @Test void V02_06_safePrefixCanBeInsufficientAndNoSafeEntryIsNotMissingTelemetry() {
        IoniaBudgetHistory h = history(1); h.modified(AKEY, 20); h.modified(BKEY, 60); h.modified(CKEY, 90);
        h.trim(Set.of(), 10, 1); assertEquals(3, h.size());
        assertEquals(OptionalInt.of(0), h.snapshot().lastDecision().orElseThrow().eligible());
        assertEquals(IoniaBudgetHistory.Reason.NO_SAFE_PREFIX, h.snapshot().lastDecision().orElseThrow().reason());
        h.trim(Set.of(), 30, 2); assertEquals(2, h.size()); assertEquals(1, h.snapshot().overshoot()); assertEquals(20, h.lastTrimmedIndex());
    }
    @Test void V02_02_belowAndAtTargetKeepLtiAndDoNotComputeEligibility() {
        IoniaBudgetHistory h = history(2); h.modified(AKEY, 5); h.trim(Set.of(), 9, 1);
        assertEquals(0, h.lastTrimmedIndex()); h.modified(BKEY, 8); h.trim(Set.of(), 10, 2);
        assertEquals(2, h.size()); assertEquals(OptionalInt.empty(), h.snapshot().lastDecision().orElseThrow().eligible());
        assertEquals(IoniaBudgetHistory.Reason.BELOW_TARGET, h.snapshot().lastDecision().orElseThrow().reason());
    }
    @Test void V02_07_latestRewriteAndTiedIndicesAreRemovedAsOnePrefix() {
        IoniaBudgetHistory h = history(1); h.modified(AKEY, 5); h.modified(AKEY, 50); h.modified(BKEY, 20); h.modified(CKEY, 20);
        h.trim(Set.of(), 30, 1); assertEquals(1, h.size()); assertEquals(50, h.index(AKEY)); assertEquals(20, h.lastTrimmedIndex());
        assertEquals(2, h.snapshot().lastDecision().orElseThrow().removed());
        h.modified(BKEY, 60); h.modified(BKEY, 3); h.trim(Set.of(), 55, 2); assertEquals(60, h.index(BKEY)); assertEquals(50, h.index(AKEY));
    }
    @Test void V02_03_zeroAdvancesEvenWithNoEntriesAndRebuildPreservesOriginalBoundary() {
        IoniaBudgetHistory h = history(0); h.trim(Set.of(), 12, 1); assertEquals(12, h.index(AKEY));
        h.rebuild(List.of(entry(20, AKEY))); h.trim(Set.of(), 19, 2); assertEquals(1, h.size()); assertEquals(20, h.lastTrimmedIndex());
        h.trim(Set.of(), 20, 3); assertEquals(1, h.size());
        h.trim(Set.of(), 21, 4); assertEquals(0, h.size());
    }
    @Test void V02_12_rebuildClearsReportsAndImmutableDiagnostics() {
        IoniaBudgetHistory h = history(1); h.modified(AKEY, 5); h.modified(BKEY, 6); h.report(B, 100); h.trim(Set.of(B), 100, 1);
        IoniaBudgetHistory.Snapshot old = h.snapshot(); h.rebuild(List.of(entry(7, CKEY), entry(8, AKEY)));
        assertEquals(1, old.entries()); assertEquals(0, h.snapshot().trimCalls()); assertTrue(h.snapshot().lastDecision().isEmpty());
        h.trim(Set.of(B), 100, 2); assertEquals(2, h.size()); assertEquals(7, h.lastTrimmedIndex());
    }
    @Test void V02_invariantsNeverUnderestimateAnOmittedLatestModification() {
        for (int n : new int[]{0, 1, 2, Integer.MAX_VALUE}) for (int seed = 0; seed < 64; seed++) {
            Random random = new Random(seed); IoniaBudgetHistory h = history(n); Map<Bytes, Long> latest = new HashMap<>();
            long previousLti = 0;
            for (long index = 1; index <= 80; index++) {
                Bytes key = Bytes.utf8("key" + random.nextInt(8)); latest.put(key, index); h.modified(key, index);
                long safe = random.nextLong(index + 1); h.trim(Set.of(), safe, index);
                assertTrue(h.lastTrimmedIndex() >= previousLti); previousLti = h.lastTrimmedIndex();
                for (var entry : latest.entrySet()) {
                    assertTrue(h.index(entry.getKey()) >= entry.getValue(), "N=" + n + " seed=" + seed);
                    if (entry.getValue() > safe && entry.getValue() > h.lastTrimmedIndex()) assertEquals(entry.getValue().longValue(), h.index(entry.getKey()));
                }
            }
        }
    }
    @Test void V02_invalidIndicesAreRejected() {
        IoniaBudgetHistory h = history(1); assertThrows(IllegalArgumentException.class, () -> h.modified(AKEY, 0));
        assertThrows(IllegalArgumentException.class, () -> h.report(B, -1)); assertThrows(IllegalArgumentException.class, () -> h.trim(Set.of(), -1, 0));
    }
}
