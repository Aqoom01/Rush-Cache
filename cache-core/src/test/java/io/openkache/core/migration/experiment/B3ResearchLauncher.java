package io.openkache.core.migration.experiment;

import io.openkache.core.migration.strategy.B3Json;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/** Python 없는 독립 연구 launcher. --help가 기본값이며 명시 명령 때만 backend를 실행한다. */
public final class B3ResearchLauncher {
    static final Map<String,String> MAINS=Map.of("comparison","ComparisonResearchMain","sensitivity","ComparisonResearchMain","movement","MovementResearchMain","scheduler","SchedulerResearchMain","safety","ExecutionSafetyResearchMain","literature","LiteratureResearchMain","policy","ExecutionPolicyResearchMain");
    private B3ResearchLauncher(){}
    public static void main(String[] args) throws IOException,InterruptedException {
        if(args.length==0||args.length==1&&args[0].equals("--help")){System.out.println("B3ResearchLauncher <comparison|sensitivity|movement|scheduler|safety|literature|policy> --profile quick|full --repeat N --output <folder>; 명시 호출 시 연구 실험 실행");return;}
        String suite=args[0],profile="quick",output=null;int repeats=2;if(!MAINS.containsKey(suite))throw new IllegalArgumentException("unknown B3 suite");
        for(int i=1;i<args.length;i+=2){if(i+1==args.length)throw new IllegalArgumentException("missing option value");switch(args[i]){case "--profile"->profile=args[i+1];case "--repeat"->repeats=Integer.parseInt(args[i+1]);case "--output"->output=args[i+1];default->throw new IllegalArgumentException("unknown option "+args[i]);}}
        if(output==null||!List.of("quick","full").contains(profile)||repeats<1||repeats>100)throw new IllegalArgumentException("invalid suite settings");
        run(suite,profile,repeats,Path.of(output));
    }
    static void run(String suite,String requestedProfile,int repeats,Path output) throws IOException,InterruptedException {
        Path root=Path.of("").toAbsolutePath().normalize();if(!Files.isRegularFile(root.resolve("settings.gradle.kts")))throw new IllegalArgumentException("repository root required");
        Path main=root.resolve("cache-core/build/classes/java/main"),test=root.resolve("cache-core/build/classes/java/test");String backend="io.openkache.core.migration.experiment."+MAINS.get(suite);
        if(!Files.exists(test.resolve(backend.replace('.','/')+".class")))throw new IllegalArgumentException("compile cache-core testClasses first");
        var sources=sources(root);var classes=new ArrayList<Path>();classes.addAll(files(main));classes.addAll(files(test));String sourceHash=digest(root,sources),backendHash=digest(root,classes);Path launcher=root.resolve("cache-core/src/test/java/io/openkache/core/migration/experiment/B3ResearchLauncher.java");
        String id="run-"+Instant.now().toString().replace(':','-')+"-"+UUID.randomUUID().toString().substring(0,8);Path folder=output.toAbsolutePath().normalize().resolve(id);Files.createDirectories(folder);
        String profile=(suite.equals("sensitivity")?"sensitivity-":"")+requestedProfile;var metadata=new LinkedHashMap<String,Object>();metadata.put("suiteExecutionId",id);metadata.put("gitCommit",git(root,"rev-parse","HEAD").strip());metadata.put("workingTreeDirty",!git(root,"status","--porcelain").isBlank());metadata.put("sourceHash",sourceHash);metadata.put("sourceHashAlgorithm","relative-path-nul-content-nul-sha256-v1");metadata.put("backendHash",backendHash);metadata.put("runnerHash",sha(Files.readAllBytes(launcher)));metadata.put("configPath",launcher.toString());metadata.put("backend.option.profile",profile);metadata.put("repeat.count",repeats);metadata.put("execution.launcher","b3-standalone-java");
        var entries=new ArrayList<Map<String,Object>>();for(int i=1;i<=repeats;i++){var item=new LinkedHashMap<String,Object>();item.put("repeat",i);item.put("status","not-started");entries.add(item);}var manifest=new LinkedHashMap<String,Object>();manifest.put("status","running");manifest.put("suite",suite);manifest.put("profile",profile);manifest.put("metadata",metadata);manifest.put("repeats",entries);save(folder,manifest);
        try{
            for(var entry:entries){int number=((Number)entry.get("repeat")).intValue();Path target=Files.createDirectory(folder.resolve(String.format(java.util.Locale.ROOT,"repeat-%03d",number)));String javaExecutable=Path.of(System.getProperty("java.home"),"bin",System.getProperty("os.name").startsWith("Windows")?"java.exe":"java").toString();
                var command=List.of(javaExecutable,"-Xmx2g","-cp",main+java.io.File.pathSeparator+test,backend,target.toString(),profile,""+number,""+repeats);var launch=new Properties();metadata.forEach((k,v)->launch.setProperty(k,v.toString()));launch.setProperty("execution.command",javaExecutable);launch.setProperty("execution.arguments",B3Json.encode(command.subList(1,command.size())));try(var writer=Files.newBufferedWriter(target.resolve("launch.properties"),StandardCharsets.UTF_8)){launch.store(writer,"B3 standalone Java");}
                entry.put("status","running");save(folder,manifest);Process process=new ProcessBuilder(command).directory(root.toFile()).redirectErrorStream(true).redirectOutput(target.resolve("backend.log").toFile()).start();
                if(!process.waitFor(1800,TimeUnit.SECONDS)){process.destroyForcibly();process.waitFor();throw new IOException("backend timeout");}if(process.exitValue()!=0)throw new IOException("backend failed: "+process.exitValue());
                var completion=new Properties();try(var reader=Files.newBufferedReader(target.resolve("completion.properties"),StandardCharsets.UTF_8)){completion.load(reader);}if(!completion.getProperty("status","").equals("complete"))throw new IOException("missing completion barrier");
                entry.put("status","complete");var hashes=new LinkedHashMap<String,String>();for(Path path:files(target))hashes.put(target.relativize(path).toString().replace('\\','/'),sha(Files.readAllBytes(path)));entry.put("files",hashes);save(folder,manifest);System.out.println(suite+" "+number+"/"+repeats+": "+target);
            }
            if(!sourceHash.equals(digest(root,sources(root)))||!backendHash.equals(digest(root,classes)))throw new IOException("sources/classes changed during execution");manifest.put("status","complete");
        }catch(IOException|RuntimeException|InterruptedException error){manifest.put("status","failed");manifest.put("error",error.toString());entries.stream().filter(e->e.get("status").equals("running")).forEach(e->e.put("status","failed"));throw error;}
        finally{save(folder,manifest);}
    }
    private static void save(Path folder,Map<String,Object> manifest) throws IOException {Files.writeString(folder.resolve("manifest.json"),B3Json.encode(manifest)+"\n",StandardCharsets.UTF_8);}
    private static List<Path> sources(Path root) throws IOException {var out=new ArrayList<Path>();out.addAll(files(root.resolve("cache-core/src/main")));out.addAll(files(root.resolve("cache-core/src/test/java/io/openkache/core/migration")));return out;}
    private static List<Path> files(Path folder) throws IOException {try(var paths=Files.walk(folder)){return paths.filter(Files::isRegularFile).sorted().toList();}}
    private static String git(Path root,String... args) throws IOException,InterruptedException {var command=new ArrayList<String>();command.add("git");command.addAll(List.of(args));Process process=new ProcessBuilder(command).directory(root.toFile()).redirectErrorStream(true).start();String output=new String(process.getInputStream().readAllBytes(),StandardCharsets.UTF_8);if(process.waitFor()!=0)throw new IOException("git metadata unavailable");return output;}
    static String digest(Path root,List<Path> paths) throws IOException {try{var hash=MessageDigest.getInstance("SHA-256");for(var p:paths.stream().sorted(Comparator.comparing(path->root.relativize(path).toString().replace('\\','/'))).toList()){hash.update(root.relativize(p).toString().replace('\\','/').getBytes(StandardCharsets.UTF_8));hash.update((byte)0);hash.update(Files.readAllBytes(p));hash.update((byte)0);}return HexFormat.of().formatHex(hash.digest());}catch(NoSuchAlgorithmException error){throw new IllegalStateException(error);}}
    static String sha(byte[] value){try{return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(value));}catch(NoSuchAlgorithmException error){throw new IllegalStateException(error);}}
}
