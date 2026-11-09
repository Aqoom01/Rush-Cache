package io.openkache.core.replication.experiment;

import io.openkache.core.api.*;
import io.openkache.core.model.*;
import io.openkache.core.replication.chain.*;
import io.openkache.core.spi.PeerEnvelope;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class CraqTest {
    private ChainCluster cluster() { return new ChainCluster(1,200,0,ChainEngine.ReadMode.CRAQ); }
    private void put(ChainCluster c,long id,String key,String value) {
        var f=c.set(id,key,value,5000); c.advance(20); assertEquals(ResultStatus.OK,f.join().status());
    }
    private CompletableFuture<Result> get(ChainCluster c,int node,String key,int timeout) {
        return c.execute(node,900,new Command.Get(Bytes.utf8(key)),timeout);
    }
    private Result read(ChainCluster c,int node,String key) { var f=get(c,node,key,5000); c.advance(20); return f.join(); }

    @Test void cleanReadsAndMissingKeysAreLocalOnEveryNode() {
        try(var c=cluster()) {
            put(c,1,"a","old"); long messages=c.network.stats().messages();
            for(int n=0;n<3;n++) {
                assertEquals(Bytes.utf8("old"),get(c,n,"a",100).join().value().orElseThrow());
                assertEquals(ResultStatus.MISS,get(c,n,"missing",100).join().status());
                Result exists=c.execute(n,901,new Command.Exists(Bytes.utf8("a")),100).join();
                assertEquals(ResultStatus.OK,exists.status()); assertTrue(exists.value().isEmpty()); assertEquals(1,exists.valueVersion());
                assertEquals(0,c.nodes.get(n).readObservation().queryAttempts());
            }
            assertEquals(messages,c.network.stats().messages());
        }
    }
    @Test void dirtyReadSelectsTailOldVersionUntilWriteReachesTail() {
        try(var c=cluster()) {
            put(c,1,"a","old");
            c.drop=e->e.message() instanceof ChainMessage.Forward && e.to().value()==2;
            var write=c.set(2,"a","new",5000); c.advance(20);
            assertEquals(Bytes.utf8("new"),c.local(0,"a").value().orElseThrow());
            for(int n=0;n<3;n++) assertEquals(Bytes.utf8("old"),read(c,n,"a").value().orElseThrow());
            assertFalse(write.isDone()); assertEquals(1,c.nodes.get(0).readObservation().dirtyReads());
            c.drop=e->false; c.advance(1200); assertEquals(ResultStatus.OK,write.join().status());
            for(int n=0;n<3;n++) assertEquals(Bytes.utf8("new"),read(c,n,"a").value().orElseThrow());
        }
    }
    @Test void tailConfirmedValueIsReadableBeforeReverseAck() {
        try(var c=cluster()) {
            put(c,1,"a","old"); c.drop=e->e.message() instanceof ChainMessage.Ack;
            var write=c.set(2,"a","new",1000); c.advance(20); assertEquals(ResultStatus.OK,write.join().status());
            assertEquals(1,c.nodes.get(0).observation().sent());
            assertEquals(Bytes.utf8("new"),read(c,0,"a").value().orElseThrow());
            assertEquals(1,c.nodes.get(0).readObservation().dirtyReads());
            // dirty 판정은 key별이다. 다른 key의 쓰기가 이 key의 로컬 읽기를 막지 않는다.
            assertEquals(ResultStatus.MISS,get(c,0,"absent",100).join().status());
            c.drop=e->false; c.advance(1200);
            assertEquals(1,c.nodes.get(0).readObservation().retainedVersions());
        }
    }
    @Test void firstUncommittedInsertIsMissingRatherThanVisible() {
        try(var c=cluster()) {
            c.drop=e->e.message() instanceof ChainMessage.Forward && e.to().value()==2;
            c.set(1,"new-key","uncommitted",5000); c.advance(20);
            for(int n=0;n<3;n++) assertEquals(ResultStatus.MISS,read(c,n,"new-key").status());
        }
    }
    @Test void deleteTombstoneAndRecreateDoNotConfuseResetKvVersion() {
        try(var c=cluster()) {
            put(c,1,"a","old"); c.drop=e->e.message() instanceof ChainMessage.Forward && e.to().value()==2;
            var deleted=c.execute(0,2,new Command.Delete(Bytes.utf8("a")),5000); c.advance(20);
            assertEquals(Bytes.utf8("old"),read(c,0,"a").value().orElseThrow());
            assertEquals(ResultStatus.OK,c.execute(2,901,new Command.Exists(Bytes.utf8("a")),100).join().status());
            c.drop=e->false; c.advance(1200); assertEquals(ResultStatus.OK,deleted.join().status());
            for(int n=0;n<3;n++) assertEquals(ResultStatus.MISS,read(c,n,"a").status());
            c.drop=e->e.message() instanceof ChainMessage.Forward && e.to().value()==2;
            var recreated=c.set(3,"a","again",5000); c.advance(20);
            assertEquals(ResultStatus.MISS,read(c,0,"a").status());
            c.drop=e->false; c.advance(1200); assertEquals(ResultStatus.OK,recreated.join().status());
            for(int n=0;n<3;n++) {
                Result r=read(c,n,"a"); assertEquals(Bytes.utf8("again"),r.value().orElseThrow()); assertEquals(1,r.valueVersion());
            }
        }
    }
    @Test void pendingQueryPinsOldValueAcrossAckGcAndNewWrites() {
        try(var c=cluster()) {
            put(c,1,"a","old"); List<PeerEnvelope> held=new ArrayList<>();
            c.drop=e->{
                if(e.message() instanceof ChainMessage.VersionReply) { held.add(e); return true; }
                return e.message() instanceof ChainMessage.Forward && e.to().value()==2;
            };
            var write=c.set(2,"a","second",5000); c.advance(20);
            var reading=get(c,0,"a",5000); c.advance(20); assertFalse(reading.isDone());
            assertFalse(held.isEmpty()); assertEquals(1,((ChainMessage.VersionReply)held.getFirst().message()).version());
            c.drop=e->e.message() instanceof ChainMessage.VersionReply; c.advance(1200);
            assertEquals(ResultStatus.OK,write.join().status()); put(c,3,"a","third");
            assertTrue(c.nodes.get(0).readObservation().retainedVersions()>=3);
            PeerEnvelope old=held.getFirst(); c.nodes.get(0).onPeerMessage(old.from(),old.message());
            assertEquals(Bytes.utf8("old"),reading.join().value().orElseThrow());
            assertEquals(1,c.nodes.get(0).readObservation().retainedVersions());
            assertEquals(Bytes.utf8("third"),get(c,0,"a",100).join().value().orElseThrow());
            c.nodes.get(0).onPeerMessage(old.from(),old.message()); assertEquals(1,c.nodes.get(0).readObservation().dirtyReads());
        }
    }
    @Test void lostQueryRetriesAndDeadlineDiscardsLateReply() {
        try(var c=cluster()) {
            put(c,1,"a","old"); c.drop=e->e.message() instanceof ChainMessage.Ack;
            c.set(2,"a","new",5000); c.advance(20);
            c.drop=e->e.message() instanceof ChainMessage.VersionQuery || e.message() instanceof ChainMessage.Ack;
            var retry=get(c,0,"a",3000); c.advance(100); assertFalse(retry.isDone());
            c.drop=e->e.message() instanceof ChainMessage.Ack; c.advance(1100);
            assertEquals(Bytes.utf8("new"),retry.join().value().orElseThrow());
            assertEquals(2,c.nodes.get(0).readObservation().queryAttempts());
            List<PeerEnvelope> held=new ArrayList<>();
            c.drop=e->{ if(e.message() instanceof ChainMessage.VersionReply) { held.add(e); return true; } return e.message() instanceof ChainMessage.Ack; };
            var expired=get(c,0,"a",10); c.advance(20); assertEquals(ResultStatus.TIMEOUT,expired.join().status());
            for(var e:held) c.nodes.get(0).onPeerMessage(e.from(),e.message());
            assertEquals(ResultStatus.TIMEOUT,expired.join().status()); assertEquals(0,c.nodes.get(0).readObservation().pendingReads());
        }
    }
    @Test void wrongPeerEpochKeyAndUnknownVersionRepliesCannotCompleteRead() {
        try(var c=cluster()) {
            put(c,1,"a","old"); c.drop=e->e.message() instanceof ChainMessage.Ack;
            c.set(2,"a","new",5000); c.advance(20);
            c.drop=e->e.message() instanceof ChainMessage.Ack || e.message() instanceof ChainMessage.VersionQuery;
            var reading=get(c,0,"a",1000); var node=c.nodes.get(0);
            node.onPeerMessage(new NodeId(1),new ChainMessage.VersionReply(1,1,Bytes.utf8("a"),2));
            node.onPeerMessage(new NodeId(2),new ChainMessage.VersionReply(2,1,Bytes.utf8("a"),2));
            node.onPeerMessage(new NodeId(2),new ChainMessage.VersionReply(1,1,Bytes.utf8("other"),2));
            node.onPeerMessage(new NodeId(2),new ChainMessage.VersionReply(1,1,Bytes.utf8("a"),99));
            assertFalse(reading.isDone());
            node.onPeerMessage(new NodeId(2),new ChainMessage.VersionReply(1,1,Bytes.utf8("a"),2));
            assertEquals(Bytes.utf8("new"),reading.join().value().orElseThrow());
        }
    }
    @Test void everyCrashPositionPreservesReadsAfterRepairAndDuplicates() {
        for(int dead=0;dead<3;dead++) try(var c=cluster()) {
            put(c,1,"a","old"); c.duplicate=true; c.crash(dead); c.advance(1500);
            put(c,2,"a","new");
            for(int n=0;n<3;n++) if(n!=dead) {
                assertEquals(Bytes.utf8("new"),read(c,n,"a").value().orElseThrow());
                assertEquals(1,c.nodes.get(n).readObservation().retainedVersions());
            }
            c.assertPrefix();
        }
    }
    @Test void configurationInvalidatesPendingReadAndDoesNotServeWhileFrozen() {
        try(var c=cluster()) {
            put(c,1,"a","old"); c.drop=e->e.message() instanceof ChainMessage.Ack;
            c.set(2,"a","new",5000); c.advance(20);
            List<PeerEnvelope> held=new ArrayList<>();
            c.drop=e->{ if(e.message() instanceof ChainMessage.VersionReply) { held.add(e); return true; } return e.message() instanceof ChainMessage.Ack; };
            var reading=get(c,0,"a",5000); c.advance(20);
            c.nodes.get(0).installConfiguration(2,List.of(new NodeId(0),new NodeId(1))).toCompletableFuture().join();
            assertEquals(ResultStatus.UNAVAILABLE,reading.join().status());
            assertEquals(ResultStatus.UNAVAILABLE,get(c,0,"a",100).join().status());
            for(var e:held) c.nodes.get(0).onPeerMessage(e.from(),e.message());
            assertEquals(ResultStatus.UNAVAILABLE,reading.join().status());
            assertEquals(0,c.nodes.get(0).readObservation().pendingReads());
        }
    }
    @Test void isolatedDirtyReadTimesOutAndHealsWithoutMasterRemovingLiveNode() {
        try(var c=cluster()) {
            put(c,1,"a","old"); c.isolate(0); c.set(2,"a","new",5000); c.advance(20);
            var reading=get(c,0,"a",20); c.advance(30); assertEquals(ResultStatus.TIMEOUT,reading.join().status());
            assertEquals(Bytes.utf8("old"),get(c,1,"a",100).join().value().orElseThrow());
            assertEquals(1,c.masterEpoch); c.heal(); c.advance(1200);
            assertEquals(Bytes.utf8("new"),get(c,0,"a",100).join().value().orElseThrow());
        }
    }
    @Test void countStillUsesTailAndWritesStillUseHead() {
        try(var c=cluster()) {
            assertEquals(new NodeId(2),c.execute(0,1,new Command.Count(),100).join().redirect().orElseThrow().target());
            assertEquals(new NodeId(0),c.execute(1,2,new Command.Set(Bytes.utf8("a"),Bytes.utf8("v")),100).join().redirect().orElseThrow().target());
            var f=get(c,0,"none",100); assertEquals(ResultStatus.MISS,f.join().status());
        }
    }
}
