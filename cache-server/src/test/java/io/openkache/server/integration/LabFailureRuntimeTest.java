package io.openkache.server.integration;

import com.openkache.failuredetector.experiment.DetectorProfile;
import com.openkache.failuredetector.product.ProductAlgorithm;
import io.openkache.core.failure.*;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Stream;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import static org.junit.jupiter.api.Assertions.*;

/** All source-backed profiles must deliver actual socket evidence, not merely appear in a registry. */
class LabFailureRuntimeTest {
    @TempDir Path directory;
    static Stream<String> methods(){return FailureMethods.LAB.stream().sorted();}
    static Properties settings(String method){
        Properties p=IntegratedCacheTest.properties("VR");p.setProperty("failure.detector",method);p.setProperty("replica.port","28101");
        p.setProperty("failure.grace.ms","100");p.setProperty("fd.heartbeat.ms","40");p.setProperty("fd.deadline.ms","240");
        p.setProperty("fd.confirm.ms","120");p.setProperty("fd.probe.timeout.ms","40");p.setProperty("fd.phi.threshold","2");
        return p;
    }
    @ParameterizedTest @MethodSource("methods")
    void everyProfileHasRealTransportAndBothSurvivorsObserveAStoppedPeer(String method)throws Exception{
        var config=new IntegrationConfig(settings(method));var nodes=new ArrayList<FailureRuntime>();
        var states=new ArrayList<ConcurrentMap<Long,MemberStatus>>();for(int i=0;i<3;i++)states.add(new ConcurrentHashMap<>());
        try(var journal=new JsonLine(directory.resolve("fd.jsonl"))){
            try{
                for(int n=0;n<3;n++){int observer=n;nodes.add(FailureRuntime.start(config,0,n,e->states.get(observer).put(e.member().value(),e.current()),journal));}
                long until=System.nanoTime()+TimeUnit.SECONDS.toNanos(3);
                while(nodes.stream().anyMatch(n->ReplicaDiagnostics.number(n.json(),"sentFrames")==0)&&System.nanoTime()<until)Thread.sleep(10);
                Thread.sleep(600);
                for(int n=0;n<3;n++){
                    assertTrue(nodes.get(n).json().contains("\"runtimeHealthy\":true"),nodes.get(n).json());
                    assertTrue(ReplicaDiagnostics.number(nodes.get(n).json(),"sentFrames")>0,nodes.get(n).json());
                    assertTrue(states.get(n).values().stream().allMatch(s->s==MemberStatus.ALIVE),method+states+nodes.get(n).json());
                    if(method.equals("IGNITE_TCP_RING")||method.equals("AEROSPIKE_MESH"))assertTrue(ReplicaDiagnostics.number(nodes.get(n).json(),"tcpSentFrames")>0,nodes.get(n).json());
                }
                nodes.get(0).close();until=System.nanoTime()+TimeUnit.SECONDS.toNanos(8);
                while((states.get(1).get(0L)!=MemberStatus.DEAD||states.get(2).get(0L)!=MemberStatus.DEAD)&&System.nanoTime()<until)Thread.sleep(10);
                for(int n=1;n<3;n++)assertEquals(MemberStatus.DEAD,states.get(n).get(0L),method+" node="+n+" "+nodes.get(n).json()+states);
                if(method.equals("F6"))assertTrue(ReplicaDiagnostics.number(nodes.get(1).json(),"helperNegativeEvidence")>0,nodes.get(1).json());
            }finally{for(var n:nodes)n.close();}
        }
    }
    @Test void originalProfileRegistriesCannotBeSilentlyOmitted(){
        for(var profile:DetectorProfile.values())assertTrue(FailureMethods.LAB.contains(profile.code().toUpperCase(Locale.ROOT)),profile.name());
        for(var product:ProductAlgorithm.values()){
            String canonical=product==ProductAlgorithm.SWIM_LIFEGUARD?"F3":FailureMethods.canonical(product.id());
            assertTrue(FailureMethods.LAB.contains(canonical),product.name());
        }
    }
    @Test void missingHelperResponsesAreNotIndependentNegativeEvidence()throws Exception{
        var config=new IntegrationConfig(settings("F6"));var nodes=new ArrayList<FailureRuntime>();var observed=new ConcurrentHashMap<Long,MemberStatus>();
        try(var journal=new JsonLine(directory.resolve("inconclusive.jsonl"))){try{
            for(int n=0;n<3;n++){int index=n;nodes.add(FailureRuntime.start(config,0,n,e->{if(index==1)observed.put(e.member().value(),e.current());},journal));}
            Thread.sleep(500);nodes.get(0).close();nodes.get(2).close();Thread.sleep(1400);
            assertEquals(MemberStatus.SUSPECT,observed.get(0L),nodes.get(1).json());
            assertFalse(observed.containsValue(MemberStatus.DEAD),nodes.get(1).json());
            assertEquals(0,ReplicaDiagnostics.number(nodes.get(1).json(),"helperNegativeEvidence"));
            assertTrue(ReplicaDiagnostics.number(nodes.get(1).json(),"pendingProbes")<=2);
        }finally{for(var n:nodes)n.close();}}
    }
    @Test void unsupportedHelperTopologyAndInvalidTimingAreRejectedBeforeBinding(){
        Properties p=settings("F6");p.setProperty("fd.verify.helpers","3");Properties topology=p;assertThrows(IllegalArgumentException.class,()->new IntegrationConfig(topology));
        p=settings("F0");p.setProperty("fd.deadline.ms","10");Properties invalid=p;assertThrows(IllegalArgumentException.class,()->new IntegrationConfig(invalid));
    }
}
