package io.openkache.server.benchmark.pilot;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class PilotInputsTest {
    @TempDir Path temp;
    private Path profile()throws IOException{
        Path p=temp.resolve("profile.properties");Files.writeString(p,"""
            purpose=functional-pilot
            seed=101
            key.count=16
            key.bytes=24
            value.bytes=100
            mix.get=80
            mix.set=15
            mix.delete=5
            stream.count=2
            distribution=UNIFORM
            hot.probability=0
            warmup.requests=8
            measured.requests=48
            concurrency=2
            work.timeout.ms=120000
            barrier.timeout.ms=30000
            operation.timeout.ms=5000
            socket.timeout.ms=1500
            """);return p;
    }
    private Path topology()throws IOException{Path p=temp.resolve("topology.properties");Files.writeString(p,"schema=ionia-pilot-topology-v1\nleader.id=1\nreplicas=1@127.0.0.1:19001,2@127.0.0.1:19002,3@127.0.0.1:19003\n");return p;}
    @Test void canonicalDatasetAndRunInputsRoundTripAndRefuseOverwrite()throws Exception{
        Path profile=profile(),dataset=temp.resolve("dataset"),run=temp.resolve("run"),topology=topology();PilotMain.dataset(profile,dataset);PilotMain.prepare(topology,dataset,run);
        assertEquals(16,PilotFiles.Plan.read(run.resolve("profile.properties")).trace().keyCount());assertEquals(11,PilotFiles.verifyHashes(run,"INPUT_SHA256SUMS").size());assertNotNull(PilotFiles.runProperties(run));
        assertThrows(FileAlreadyExistsException.class,()->PilotMain.dataset(profile,dataset));assertThrows(FileAlreadyExistsException.class,()->PilotMain.prepare(topology,dataset,run));
        Files.writeString(run.resolve("measure.trace"),"tampered",StandardOpenOption.APPEND);assertThrows(IOException.class,()->PilotFiles.runProperties(run));
    }
    @Test void datasetTamperIsRejectedBeforeCreatingRun()throws Exception{
        Path dataset=temp.resolve("dataset");PilotMain.dataset(profile(),dataset);Files.writeString(dataset.resolve("load.trace"),"bad");Path run=temp.resolve("run");
        assertThrows(IOException.class,()->PilotMain.prepare(topology(),dataset,run));assertFalse(Files.exists(run));
    }
    @Test void recomputedFileHashesCannotHideWrongGeneratorTrace()throws Exception{
        Path dataset=temp.resolve("dataset");PilotMain.dataset(profile(),dataset);var plan=PilotFiles.Plan.read(dataset.resolve("profile.properties"));Files.writeString(dataset.resolve("measure.trace"),"wrong");
        assertThrows(IOException.class,()->PilotFiles.verifyTrace(dataset,plan));
    }
    @Test void profileRequiresExactFieldsAndBoundedFunctionalWork()throws Exception{
        Path p=profile();Files.writeString(p,"unexpected=true\n",StandardOpenOption.APPEND);assertThrows(IllegalArgumentException.class,()->PilotFiles.Plan.read(p));
        Files.writeString(p,Files.readString(p).replace("unexpected=true\n","").replace("concurrency=2","concurrency=3"));assertThrows(IllegalArgumentException.class,()->PilotFiles.Plan.read(p));
    }
    @Test void topologyRejectsDuplicateEndpointsIdsAndWrongGroupSize(){
        for(String spec:List.of("1@127.0.0.1:1,2@127.0.0.1:1,3@127.0.0.1:3","1@127.0.0.1:1,1@127.0.0.1:2,3@127.0.0.1:3","1@127.0.0.1:1,2@127.0.0.1:2")){
            Properties p=new Properties();p.setProperty("replicas",spec);assertThrows(IllegalArgumentException.class,()->PilotFiles.addresses(p));
        }
    }
    @Test void hashManifestRejectsEscapesDuplicatesAndUncoveredLibrary()throws Exception{
        assertThrows(IOException.class,()->PilotFiles.inside(temp,"../outside"));Files.writeString(temp.resolve("data"),"ok");PilotFiles.hashes(temp,"HASHES",List.of("data"));assertEquals(Set.of("data"),PilotFiles.verifyHashes(temp,"HASHES"));
        Files.writeString(temp.resolve("HASHES"),Files.readString(temp.resolve("HASHES")),StandardOpenOption.APPEND);assertThrows(IOException.class,()->PilotFiles.verifyHashes(temp,"HASHES"));
        Files.createDirectory(temp.resolve("lib"));Files.writeString(temp.resolve("lib/one.jar"),"one");PilotFiles.hashes(temp,"BUNDLE_SHA256SUMS",List.of("lib/one.jar"));PilotFiles.verifyBundle(temp);
        Files.writeString(temp.resolve("lib/two.jar"),"two");assertThrows(IOException.class,()->PilotFiles.verifyBundle(temp));
    }
}
