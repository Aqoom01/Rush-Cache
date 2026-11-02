package io.openkache.server.benchmark.pilot;

import io.openkache.core.model.NodeId;
import io.openkache.runner.benchmark.*;
import io.openkache.runner.benchmark.measurement.ObservationJson;
import java.io.*;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.security.*;
import java.util.*;

/** Strict portable inputs and hashes for a functional pilot, not a ready performance manifest. */
public final class PilotFiles {
    private PilotFiles(){}
    public static final Set<String> TRACE=Set.of("seed","key.count","key.bytes","value.bytes","mix.get","mix.set","mix.delete","stream.count","distribution","hot.probability","warmup.requests","measured.requests");
    private static final Set<String> PLAN;
    static{Set<String> fields=new HashSet<>(TRACE);fields.addAll(Set.of("purpose","concurrency","work.timeout.ms","barrier.timeout.ms","operation.timeout.ms","socket.timeout.ms"));PLAN=Set.copyOf(fields);}
    public record Plan(TraceConfig trace,long warm,long measured,int concurrency,long workMs,long barrierMs,long operationMs,int socketMs){
        public static Plan read(Path file)throws IOException{
            Properties p=readProperties(file);if(!p.stringPropertyNames().equals(PLAN)||!"functional-pilot".equals(p.getProperty("purpose")))throw new IllegalArgumentException("exact functional pilot profile required");
            TraceConfig t=new TraceConfig(number(p,"seed"),number(p,"key.count"),integer(p,"key.bytes"),integer(p,"value.bytes"),integer(p,"mix.get"),integer(p,"mix.set"),integer(p,"mix.delete"),integer(p,"stream.count"),TraceConfig.Distribution.valueOf(p.getProperty("distribution")),Double.parseDouble(p.getProperty("hot.probability")));
            long w=number(p,"warmup.requests"),m=number(p,"measured.requests"),work=number(p,"work.timeout.ms"),barrier=number(p,"barrier.timeout.ms"),operation=number(p,"operation.timeout.ms");int c=integer(p,"concurrency"),s=integer(p,"socket.timeout.ms");
            long total=Math.addExact(t.keyCount(),Math.addExact(w,m));long upper=Math.multiplyExact(total,37L+t.keyBytes()+t.valueBytes());
            if(t.keyCount()>1_000_000||w<1||m<1||total>2_000_000||upper>512L*1024*1024||t.streamCount()>32||c<1||c>t.streamCount()||work<1000||work>900000||barrier<500||barrier>60000||operation<100||operation>60000||s<50||s>10000)throw new IllegalArgumentException("pilot count/size/time limits");
            return new Plan(t,w,m,c,work,barrier,operation,s);
        }
    }
    public static Properties readProperties(Path p)throws IOException{Properties v=new Properties();try(Reader r=Files.newBufferedReader(p,StandardCharsets.UTF_8)){v.load(r);}return v;}
    public static long number(Properties p,String key){return Long.parseLong(Objects.requireNonNull(p.getProperty(key),key).trim());}
    public static int integer(Properties p,String key){return Math.toIntExact(number(p,key));}
    public static void properties(Path p,Properties value)throws IOException{try(Writer w=Files.newBufferedWriter(p,StandardCharsets.UTF_8,StandardOpenOption.CREATE_NEW)){value.store(w,"IONIA functional pilot");}}
    public static void json(Path p,Object value)throws IOException{Path part=p.resolveSibling(p.getFileName()+".part");Files.writeString(part,ObservationJson.encode(value)+"\n",StandardCharsets.UTF_8,StandardOpenOption.CREATE_NEW);Files.move(part,p,StandardCopyOption.ATOMIC_MOVE);}
    public static String sha(Path p)throws IOException{MessageDigest d=digest();try(InputStream in=Files.newInputStream(p)){byte[] buffer=new byte[65536];int n;while((n=in.read(buffer))!=-1)d.update(buffer,0,n);}return HexFormat.of().formatHex(d.digest());}
    public static String sha(byte[] bytes){return HexFormat.of().formatHex(digest().digest(bytes));}
    private static MessageDigest digest(){try{return MessageDigest.getInstance("SHA-256");}catch(NoSuchAlgorithmException e){throw new IllegalStateException(e);}}
    public static void hashes(Path root,String name,List<String> files)throws IOException{
        StringBuilder out=new StringBuilder();for(String f:new TreeSet<>(files))out.append(sha(inside(root,f))).append("  ").append(f).append('\n');Files.writeString(root.resolve(name),out,StandardCharsets.UTF_8,StandardOpenOption.CREATE_NEW);
    }
    public static Set<String> verifyHashes(Path root,String name)throws IOException{
        Set<String> seen=new HashSet<>();for(String line:Files.readAllLines(root.resolve(name),StandardCharsets.UTF_8)){
            if(line.length()<67||!line.substring(0,64).matches("[0-9a-f]{64}")||!line.substring(64,66).equals("  "))throw new IOException("invalid hash line");
            String file=line.substring(66);if(!seen.add(file)||!sha(inside(root,file)).equals(line.substring(0,64)))throw new IOException("hash mismatch/duplicate: "+file);
        }if(seen.isEmpty())throw new IOException("empty hash manifest");return Set.copyOf(seen);
    }
    public static Path inside(Path root,String file)throws IOException{
        Path r=root.toRealPath(),p=r.resolve(file).normalize();if(Path.of(file).isAbsolute()||file.contains("\\")||!p.startsWith(r)||p.equals(r))throw new IOException("path escapes owned input root");
        if(Files.exists(p)&&!p.toRealPath().startsWith(r))throw new IOException("input symlink escapes root");return p;
    }
    public static Map<NodeId,InetSocketAddress> addresses(Properties p){
        String spec=Objects.requireNonNull(p.getProperty("replicas"),"replicas");Map<NodeId,InetSocketAddress> out=new LinkedHashMap<>();
        for(String item:spec.split(",")){
            String[] v=item.trim().split("@",-1);if(v.length!=2)throw new IllegalArgumentException("replicas=id@host:port,...");int at=v[1].lastIndexOf(':');if(at<=0)throw new IllegalArgumentException("host:port");int port=Integer.parseInt(v[1].substring(at+1));if(port<1||port>65535)throw new IllegalArgumentException("port");
            var address=new InetSocketAddress(v[1].substring(0,at),port);if(address.isUnresolved()||out.put(new NodeId(Long.parseLong(v[0])),address)!=null)throw new IllegalArgumentException("unresolved/duplicate replica");
        }
        if((out.size()!=3&&out.size()!=5)||new HashSet<>(out.values()).size()!=out.size())throw new IllegalArgumentException("3/5 distinct endpoints required");
        for(int i=1;i<=out.size();i++)if(!out.containsKey(new NodeId(i)))throw new IllegalArgumentException("pilot replica IDs must be 1..N");return Collections.unmodifiableMap(out);
    }
    public static void verifyBundle(Path bundle)throws IOException{
        Set<String> declared=verifyHashes(bundle,"BUNDLE_SHA256SUMS");Set<String> actual=new HashSet<>();try(var files=Files.list(bundle.resolve("lib"))){for(Path p:files.toList()){if(!Files.isRegularFile(p)||!p.getFileName().toString().endsWith(".jar"))throw new IOException("unexpected lib entry");actual.add("lib/"+p.getFileName());}}
        if(!declared.equals(actual))throw new IOException("bundle classpath/hash coverage mismatch");
    }
    public static Properties runProperties(Path root)throws IOException{
        Set<String> declared=verifyHashes(root,"INPUT_SHA256SUMS");Properties topology=readProperties(root.resolve("topology.properties"));Map<NodeId,InetSocketAddress> nodes=addresses(topology);
        Set<String> expected=new HashSet<>(List.of("profile.properties","trace.properties","trace-metadata.json","load.trace","warmup.trace","measure.trace","topology.properties","run.properties"));for(var id:nodes.keySet())expected.add("nodes/node-"+id.value()+".properties");
        if(!declared.equals(expected))throw new IOException("input manifest coverage");Properties run=readProperties(root.resolve("run.properties"));if(!run.stringPropertyNames().equals(Set.of("schema","run.id","purpose","load.slot"))||!run.getProperty("schema").equals("ionia-o00-pilot-v1")||!run.getProperty("purpose").equals("functional-pilot")||!run.getProperty("run.id").matches("[a-zA-Z0-9_-]{1,96}"))throw new IOException("pilot run identity");return run;
    }
    public static void verifyTrace(Path root,Plan plan)throws IOException{
        TraceGenerator generator=new TraceGenerator(plan.trace());for(var phase:TracePhase.values()){
            long n=phase==TracePhase.LOAD?plan.trace().keyCount():phase==TracePhase.WARMUP?plan.warm():plan.measured();var s=TraceWriter.write(generator,phase,n,OutputStream.nullOutputStream());if(!s.sha256().equals(sha(root.resolve(phase.name().toLowerCase(Locale.ROOT)+".trace"))))throw new IOException("generator/trace mismatch: "+phase);
        }
    }
}
