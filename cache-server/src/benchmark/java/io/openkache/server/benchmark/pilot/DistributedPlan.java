package io.openkache.server.benchmark.pilot;

import io.openkache.core.api.Command;
import io.openkache.runner.benchmark.*;
import java.io.*;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.security.*;
import java.util.*;

/** Two fixed logical streams, with unchanged canonical requests and disjoint wire namespaces. */
public final class DistributedPlan {
    public static final String SCHEMA="ionia-distributed-pilot-v1";
    private static final Set<String> FIELDS=Set.of("schema","run.id","load.count","controller.endpoint","control.token");
    private DistributedPlan(){}
    public record Input(Properties run,PilotFiles.Plan plan,Properties control,InetSocketAddress endpoint,String contractHash){}
    public record Partition(long requests,long gets,long sets,long deletes,String sha256){public long writes(){return sets+deletes;}}
    public static int slot(int slot){if(slot<0||slot>1)throw new IllegalArgumentException("load slot must be 0 or 1");return slot;}
    public static String slotName(int slot){return "load-"+slot(slot);}
    public static long wireBase(int slot){return 10_000_000L+1_000_000L*slot(slot);}
    public static void requireProfile(PilotFiles.Plan p){if(p.trace().streamCount()!=2||p.concurrency()!=2)throw new IllegalArgumentException("first distributed pilot requires streams=2 and total concurrency=2");}
    public static InetSocketAddress endpoint(String spec){
        int colon=spec.lastIndexOf(':');if(colon<1)throw new IllegalArgumentException("controller host:port required");
        int port=Integer.parseInt(spec.substring(colon+1));if(port<1||port>65535)throw new IllegalArgumentException("controller port");
        var address=new InetSocketAddress(spec.substring(0,colon),port);if(address.isUnresolved())throw new IllegalArgumentException("unresolved controller");return address;
    }
    public static Input verify(Path bundle,Path root)throws Exception{
        PilotFiles.verifyBundle(bundle);Properties run=PilotFiles.runProperties(root);var plan=PilotFiles.Plan.read(root.resolve("profile.properties"));requireProfile(plan);PilotFiles.verifyTrace(root,plan);
        if(!PilotFiles.verifyHashes(root,"DISTRIBUTED_SHA256SUMS").equals(Set.of("distributed.properties","manifest.json","INPUT_SHA256SUMS")))throw new IOException("distributed hash coverage");
        String hash=PilotFiles.sha(root.resolve("DISTRIBUTED_SHA256SUMS"));if(!Files.readString(root.resolve("DISTRIBUTED_PREPARED")).trim().equals(hash))throw new IOException("distributed prepare incomplete");
        Properties p=PilotFiles.readProperties(root.resolve("distributed.properties"));
        if(!p.stringPropertyNames().equals(FIELDS)||!SCHEMA.equals(p.getProperty("schema"))||!run.getProperty("run.id").equals(p.getProperty("run.id"))||!"2".equals(p.getProperty("load.count"))||!p.getProperty("control.token","").matches("[a-f0-9]{64}"))throw new IOException("distributed identity/profile");
        return new Input(run,plan,p,endpoint(p.getProperty("controller.endpoint")),hash);
    }
    public static long count(PilotFiles.Plan p,TracePhase phase){return switch(phase){case LOAD->p.trace().keyCount();case WARMUP->p.warm();case MEASURE->p.measured();};}
    public static long partitionCount(long total,int slot){slot(slot);if(total<0)throw new IllegalArgumentException("negative total");return total/2+(total%2>slot?1:0);}
    public static MessageDigest digest(TracePhase phase,int slot,long total){
        try{var d=MessageDigest.getInstance("SHA-256");try(var out=new DataOutputStream(new DigestOutputStream(OutputStream.nullOutputStream(),d))){out.write("IONIA-PARTITION-1".getBytes(StandardCharsets.US_ASCII));out.writeInt(phase.tag());out.writeInt(slot(slot));out.writeLong(total);out.writeInt(2);}return d;}
        catch(IOException|NoSuchAlgorithmException e){throw new IllegalStateException(e);}
    }
    /** Same record bytes as TraceWriter, hashing only the records owned by this stream. */
    public static void record(MessageDigest digest,LogicalRequest r)throws IOException{
        try(var out=new DataOutputStream(new DigestOutputStream(OutputStream.nullOutputStream(),digest))){
            out.writeLong(r.globalIndex());out.writeInt(r.streamId());out.writeLong(r.clientId());out.writeLong(r.sequence());
            byte[] value=null;switch(r.command()){case Command.Get ignored->out.writeByte(0);case Command.Set s->{out.writeByte(1);value=s.value().toByteArray();}case Command.Delete ignored->out.writeByte(2);}
            byte[] key=r.command().key().toByteArray();out.writeInt(key.length);out.write(key);out.writeInt(value==null?-1:value.length);if(value!=null)out.write(value);
        }
    }
    public static Partition expected(PilotFiles.Plan p,TracePhase phase,int slot)throws IOException{
        slot(slot);long total=count(p,phase),get=0,set=0,delete=0;var d=digest(phase,slot,total);var generator=new TraceGenerator(p.trace());
        for(long i=slot;i<total;i+=2){var r=generator.at(phase,i);record(d,r);switch(r.command()){case Command.Get ignored->get++;case Command.Set ignored->set++;case Command.Delete ignored->delete++;}}
        return new Partition(get+set+delete,get,set,delete,HexFormat.of().formatHex(d.digest()));
    }
    public static long[] byOperation(Partition p){return new long[]{p.gets(),p.sets(),p.deletes()};}
}
