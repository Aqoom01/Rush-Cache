package io.openkache.core.replication.experiment;

import io.openkache.core.api.*;
import io.openkache.core.model.Bytes;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/** 작은 단일 키 이력의 전수 순열 검사. TIMEOUT/UNAVAILABLE 쓰기는 적용 또는 미적용 둘 다 허용한다. */
class RegisterHistoryTest {
    private record Op(long start,long end,boolean write,String value,boolean definite) {}
    private static boolean linearizable(List<Op> history) { return search(history,0,null); }
    private static boolean search(List<Op> h,int done,String value) {
        if(done==(1<<h.size())-1) return true;
        for(int i=0;i<h.size();i++) {
            if((done&(1<<i))!=0) continue;
            Op op=h.get(i); boolean blocked=false;
            for(int j=0;j<h.size();j++) if(j!=i && (done&(1<<j))==0 && h.get(j).definite() && h.get(j).end()<op.start()) { blocked=true; break; }
            if(blocked) continue;
            int next=done|(1<<i);
            if(!op.definite() && search(h,next,value)) return true;
            if(op.write()) { if(search(h,next,op.value())) return true; }
            else if(Objects.equals(value,op.value()) && search(h,next,value)) return true;
        }
        return false;
    }
    @Test void checkerRejectsStaleReadAndAllowsUncertainWrite() {
        assertFalse(linearizable(List.of(new Op(0,2,true,"new",true),new Op(3,4,false,"old",true))));
        assertTrue(linearizable(List.of(new Op(0,2,true,"maybe",false),new Op(3,4,false,"maybe",true))));
    }
    @Test void minorityAndMajorityOverlappingWritesHaveLegalReadHistory() {
        for(var variant:List.of(ProtocolCluster.Variant.VR,ProtocolCluster.Variant.VR_INCREMENTAL))
            for(int seed=1;seed<=10;seed++) try(var c=new ProtocolCluster(variant,1,100,seed)) {
            c.jitter=5; c.duplicate=true;
            List<Op> h=new ArrayList<>();
            long start=c.now(); var initial=c.set(0,1,"x","initial",1000); long s=start;
            initial.thenAccept(r->h.add(new Op(s,c.now(),true,"initial",r.status()==ResultStatus.OK))); c.advance(30);
            c.isolate(0); long minorityStart=c.now(); var minority=c.set(0,2,"x","minority",60);
            minority.thenAccept(r->h.add(new Op(minorityStart,Long.MAX_VALUE,true,"minority",false)));
            c.advance(160); long majorityStart=c.now(); var majority=c.set(1,3,"x","majority",1000);
            majority.thenAccept(r->{ assertEquals(ResultStatus.OK,r.status()); h.add(new Op(majorityStart,c.now(),true,"majority",true)); }); c.advance(30);
            long readStart=c.now(); var read=c.submit(1,4,new Command.Get(Bytes.utf8("x")),1000);
            read.thenAccept(r->h.add(new Op(readStart,c.now(),false,new String(r.value().orElseThrow().toByteArray(),java.nio.charset.StandardCharsets.UTF_8),true)));
            c.advance(30); assertTrue(read.isDone()); assertEquals(4,h.size()); assertTrue(linearizable(h),h.toString());
        }
    }
}
