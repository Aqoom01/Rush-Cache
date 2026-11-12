package io.openkache.core.replication.ionia_fourth;

import static org.junit.jupiter.api.Assertions.*;
import io.openkache.core.api.Command;
import io.openkache.core.model.*;
import io.openkache.core.replication.ionia.*;
import java.util.*;
import org.junit.jupiter.api.Test;

class IoniaBatchPlannerTest {
    private static final NodeId PEER = new NodeId(2);
    private static final IoniaPrepareSizer SIZER = new IoniaPrepareSizer("cfg");
    private static Command.Set set(int size) { return new Command.Set(Bytes.utf8("k"), Bytes.copyOf(new byte[size])); }
    private static void append(IoniaLog log, IoniaBatchPlanner planner, long id, Command command, long now) {
        var entry = log.order(new ClientId(1), new OperationId(id), command); planner.ordered(entry.index(), now);
    }
    @Test void V03_01_entryBoundariesAndCompletePrefixDrain() {
        for (int cap : new int[]{1,2,3,7}) for (int count : new int[]{cap-1,cap,cap+1,cap*3+1}) {
            IoniaLog log = new IoniaLog(); IoniaBatchPlanner p = new IoniaBatchPlanner(new IoniaBatchSettings(cap, 4096, 0), SIZER);
            for (int i=1;i<=count;i++) append(log,p,i,set(i),0);
            long matched=0;
            while (matched<count) {
                var batch=p.select(PEER,matched,log,0).orElseThrow(); assertTrue(batch.entries().size()<=cap);
                assertEquals(matched+1,batch.entries().getFirst().index());
                matched=batch.entries().getLast().index(); p.acknowledged(PEER,matched);
            }
            assertTrue(p.select(PEER,matched,log,0).isEmpty());
        }
    }
    @Test void V03_02_byteBoundaryAndSingletonOversizeNeverFragmentAWrite() {
        long one=SIZER.headerBytes()+SIZER.entryBytes(set(11));
        for (int budget : new int[]{1,(int)one-1,(int)one,(int)one+1,(int)(one+SIZER.entryBytes(set(11)))}) {
            IoniaLog log=new IoniaLog(); var p=new IoniaBatchPlanner(new IoniaBatchSettings(99,budget,0),SIZER);
            for(int i=1;i<=3;i++) append(log,p,i,set(11),0);
            var b=p.select(PEER,0,log,0).orElseThrow(); assertEquals(SIZER.packetBytes(b.entries()),b.encodedBytes());
            assertTrue(b.encodedBytes()<=budget || b.entries().size()==1); assertEquals(one>budget,b.oversized());
        }
    }
    @Test void V03_03_13_oldestAgeSurvivesContinuousArrivalAndDuplicate() {
        IoniaLog log=new IoniaLog(); var p=new IoniaBatchPlanner(new IoniaBatchSettings(99,65536,40_000_000),SIZER);
        append(log,p,1,set(1),0);
        for(long t=1;t<40;t++) { append(log,p,t+1,set(1),t*1_000_000); p.ordered(1,t*1_000_000); assertTrue(p.select(PEER,0,log,t*1_000_000).isEmpty()); }
        var batch=p.select(PEER,0,log,40_000_000).orElseThrow(); assertEquals(40_000_000,batch.oldestAgeNanos());
        assertEquals(IoniaBatchPlanner.Cause.AGE,batch.cause()); assertTrue(p.select(PEER,0,log,59_999_999).isEmpty());
        var retry=p.select(PEER,0,log,60_000_000).orElseThrow(); assertEquals(60_000_000,retry.oldestAgeNanos());
        assertEquals(IoniaBatchPlanner.Cause.RETRY,retry.cause());
    }
    @Test void V03_04_14_15_localSendNeverAdvancesAndUrgentCannotSkipOccupiedFlight() {
        IoniaLog log=new IoniaLog(); var p=new IoniaBatchPlanner(new IoniaBatchSettings(2,65536,60_000_000_000L),SIZER);
        for(int i=1;i<=5;i++) append(log,p,i,set(1),0);
        var b=p.select(PEER,0,log,0).orElseThrow(); assertEquals(2,b.entries().getLast().index()); p.urgent(5);
        assertTrue(p.select(PEER,0,log,19_999_999).isEmpty());
        assertEquals(1,p.select(PEER,0,log,20_000_000).orElseThrow().entries().getFirst().index());
        p.acknowledged(PEER,1); assertTrue(p.select(PEER,1,log,20_000_000).isEmpty());
        p.acknowledged(PEER,2); assertEquals(3,p.select(PEER,2,log,20_000_000).orElseThrow().entries().getFirst().index());
        p.acknowledged(PEER,4); assertEquals(5,p.select(PEER,4,log,20_000_000).orElseThrow().entries().getFirst().index());
    }
    @Test void V03_06_restoredLongPrefixAckCanExceedCurrentChunkEnd() {
        IoniaLog log=new IoniaLog(); var p=new IoniaBatchPlanner(new IoniaBatchSettings(1,1000,0),SIZER);
        for(int i=1;i<=5;i++) append(log,p,i,set(1),0);
        p.select(PEER,0,log,0).orElseThrow(); p.acknowledged(PEER,4);
        assertEquals(5,p.select(PEER,4,log,0).orElseThrow().entries().getFirst().index());
    }
    @Test void V03_14_partialAcksAndExpandedRetriesRetainEachEntriesActualFirstSend() {
        IoniaLog log=new IoniaLog();var p=new IoniaBatchPlanner(new IoniaBatchSettings(2,1000,0),SIZER);
        append(log,p,1,set(1),0);p.select(PEER,0,log,0).orElseThrow();
        append(log,p,2,set(1),1);append(log,p,3,set(1),1);
        p.select(PEER,0,log,20_000_000).orElseThrow();p.acknowledged(PEER,1);
        p.select(PEER,1,log,40_000_000).orElseThrow();
        assertEquals(20_000_000,p.snapshot(Map.of(PEER,1L),3,40_000_000).peers().get(PEER).flight().orElseThrow().firstSendNanos());
        p.acknowledged(PEER,2);p.select(PEER,2,log,60_000_000).orElseThrow();
        assertEquals(40_000_000,p.snapshot(Map.of(PEER,2L),3,60_000_000).peers().get(PEER).flight().orElseThrow().firstSendNanos());
    }
    @Test void V03_07_installClearsFlightAndMakesRecoveredTailUrgent() {
        IoniaLog log=new IoniaLog(); var p=new IoniaBatchPlanner(new IoniaBatchSettings(2,1000,60_000_000_000L),SIZER);
        append(log,p,1,set(1),0); assertTrue(p.select(PEER,0,log,0).isEmpty());
        p.installed(log,5); assertEquals(IoniaBatchPlanner.Cause.URGENT,p.select(PEER,0,log,5).orElseThrow().cause());
        p.installed(log,6); assertEquals(0,p.select(PEER,0,log,6).orElseThrow().oldestAgeNanos());
    }
    @Test void V03_18_snapshotsAreImmutableAndAttemptCountersAreExclusive() {
        IoniaLog log=new IoniaLog(); var p=new IoniaBatchPlanner(new IoniaBatchSettings(1,1,0),SIZER);
        append(log,p,1,set(1),0); p.select(PEER,0,log,0).orElseThrow();
        var before=p.snapshot(Map.of(PEER,0L),1,10); p.select(PEER,0,log,20_000_000).orElseThrow();
        var after=p.snapshot(Map.of(PEER,0L),1,20_000_000);
        assertEquals(1,before.attempts()); assertEquals(2,after.attempts()); assertEquals(1,after.retries());
        assertEquals(2,after.oversized()); assertEquals(after.attempts(),after.causes().values().stream().mapToLong(Long::longValue).sum());
        assertThrows(UnsupportedOperationException.class,()->before.peers().clear());
    }
    @Test void V03_02_configurationUsesModifiedUtfAndRejectsUnencodableId() {
        assertEquals(1,IoniaPrepareSizer.modifiedUtfLength("a")); assertEquals(2,IoniaPrepareSizer.modifiedUtfLength("\u0000"));
        assertEquals(3,IoniaPrepareSizer.modifiedUtfLength("한")); assertEquals(6,IoniaPrepareSizer.modifiedUtfLength("😀"));
        assertEquals(65535,IoniaPrepareSizer.modifiedUtfLength("a".repeat(65535)));
        assertThrows(IllegalArgumentException.class,()->new IoniaPrepareSizer("a".repeat(65536)));
    }
    @Test void V03_02_kvAdmissionChecksExactSizeBeforeAnyAck() {
        Bytes value=Bytes.copyOf(new byte[(int)IoniaPrepareSizer.MAX_KV_PAYLOAD-18]);
        assertTrue(SIZER.admissible(new Command.Set(Bytes.utf8("k"),value)));
        assertFalse(SIZER.admissible(new Command.Set(Bytes.utf8("kk"),value)));
        assertFalse(SIZER.admissible(new Command.Get(Bytes.utf8("k"))));
    }
    @Test void V03_00_settingsRequireAllThreeValuesAndDistinctStableProfiles() {
        Properties p=new Properties(); assertThrows(IllegalArgumentException.class,()->IoniaBatchSettings.parse(p));
        p.setProperty(IoniaBatchSettings.ENTRIES,"2"); p.setProperty(IoniaBatchSettings.BYTES,"4096");
        for(String value:List.of("-1","1","19","60001","999999999999999999999","+20","20.0")) {
            p.setProperty(IoniaBatchSettings.DELAY,value); assertThrows(IllegalArgumentException.class,()->IoniaBatchSettings.parse(p));
        }
        p.setProperty(IoniaBatchSettings.DELAY,"20"); var s=IoniaBatchSettings.parse(p);
        assertEquals(64,s.profileId().length()); assertNotEquals(s.profileId(),new IoniaBatchSettings(3,4096,20_000_000).profileId());
        assertNotEquals(s.profileId(),new IoniaBatchSettings(2,4097,20_000_000).profileId());
        assertNotEquals(s.profileId(),new IoniaBatchSettings(2,4096,40_000_000).profileId());
        assertThrows(IllegalArgumentException.class,()->new IoniaBatchSettings(0,1,0));
        assertThrows(IllegalArgumentException.class,()->new IoniaBatchSettings(1,0,0));
        assertThrows(IllegalArgumentException.class,()->new IoniaBatchSettings(1,IoniaBatchSettings.MAX_PACKET_BYTES+1,0));
    }
}
