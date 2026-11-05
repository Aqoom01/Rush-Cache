import io.openkache.core.migration.strategy.B3Json;
import java.nio.file.*;
import java.nio.charset.StandardCharsets;
import java.security.*;
import java.util.*;
import java.util.stream.*;

public class B3RunAudit {
    static Path root=Path.of("").toAbsolutePath().normalize();
    static List<Path> files(Path dir) throws Exception {try(var stream=Files.walk(dir)){return stream.filter(Files::isRegularFile).sorted().toList();}}
    static String sha(Path file) throws Exception {return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(file)));}
    static String digest(List<Path> paths) throws Exception {
        var h=MessageDigest.getInstance("SHA-256");
        for(var p:paths.stream().sorted(Comparator.comparing(x->root.relativize(x).toString().replace('\\','/'))).toList()){
            h.update(root.relativize(p).toString().replace('\\','/').getBytes(StandardCharsets.UTF_8));h.update((byte)0);h.update(Files.readAllBytes(p));h.update((byte)0);
        }return HexFormat.of().formatHex(h.digest());
    }
    static Map<String,Object> json(Path file) throws Exception {return B3Json.object(B3Json.decode(Files.readString(file)));}
    static void require(boolean value,String reason){if(!value)throw new IllegalStateException(reason);}
    static Path latest(String relative) throws Exception {
        var candidates=new ArrayList<Path>();try(var dirs=Files.list(root.resolve(relative))){
            for(var p:dirs.filter(Files::isDirectory).toList()){Path m=p.resolve("manifest.json");if(!Files.exists(m))continue;
                var data=json(m);if(data.get("metadata") instanceof Map<?,?> meta&&"b3-standalone-java".equals(meta.get("execution.launcher")))candidates.add(p);}
        }
        return candidates.stream().max(Comparator.comparing(p->p.getFileName().toString())).orElseThrow();
    }
    public static void main(String[] args) throws Exception {
        var inputs=List.of(
            "experiments/b3-live-migration/scaling-decision/runs",
            "experiments/b3-live-migration/java/literature-quick/runs",
            "experiments/b3-live-migration/execution-policy-comparison/runs",
            "experiments/b3-live-migration/java/policy-quick/runs",
            "experiments/b3-live-migration/scheduler-comparison/runs",
            "experiments/b3-live-migration/java/scheduler-quick/runs");
        var sourceFiles=new ArrayList<Path>();sourceFiles.addAll(files(root.resolve("cache-core/src/main")));sourceFiles.addAll(files(root.resolve("cache-core/src/test/java/io/openkache/core/migration")));
        String source=digest(sourceFiles);var classFiles=new ArrayList<Path>();classFiles.addAll(files(root.resolve("cache-core/build/classes/java/main")));classFiles.addAll(files(root.resolve("cache-core/build/classes/java/test")));String backend=digest(classFiles);
        var mains=Map.of("literature","LiteratureResearchMain","policy","ExecutionPolicyResearchMain","safety","ExecutionSafetyResearchMain","comparison","ComparisonResearchMain","sensitivity","ComparisonResearchMain","movement","MovementResearchMain","scheduler","SchedulerResearchMain");
        var auditRows=new ArrayList<Map<String,Object>>();long total=0,derived=0,pairedFiles=0,pairedEvidence=0;
        for(String input:inputs){
            Path folder=latest(input);var manifest=json(folder.resolve("manifest.json"));require("complete".equals(manifest.get("status")),"not complete: "+folder);var meta=B3Json.object(manifest.get("metadata"));require(source.equals(meta.get("sourceHash")),"source mismatch: "+folder);require(backend.equals(meta.get("backendHash")),"class mismatch: "+folder);
            String suite=manifest.get("suite").toString();var repeats=(List<?>)manifest.get("repeats");Map<String,String> firstDerived=null,firstEvidence=null;long runCases=0;
            for(Object object:repeats){
                var repeat=B3Json.object(object);int number=((Number)repeat.get("repeat")).intValue();Path target=folder.resolve(String.format(Locale.ROOT,"repeat-%03d",number));require("complete".equals(repeat.get("status")),"repeat not complete");
                var signed=B3Json.object(repeat.get("files"));for(var entry:signed.entrySet())require(sha(target.resolve(entry.getKey())).equals(entry.getValue()),"file digest mismatch: "+target+"/"+entry.getKey());
                var before=new TreeMap<String,String>();for(var entry:signed.entrySet())if(entry.getKey().endsWith(".csv")||entry.getKey().endsWith(".md"))before.put(entry.getKey(),entry.getValue().toString());
                var main=Class.forName("io.openkache.core.migration.experiment."+mains.get(suite));try{main.getMethod("main",String[].class).invoke(null,(Object)new String[]{"--rebuild",target.toString()});}catch(java.lang.reflect.InvocationTargetException failure){throw new IllegalStateException("rebuild failed "+target,failure.getCause());}
                for(var e:before.entrySet())require(sha(target.resolve(e.getKey())).equals(e.getValue()),"regeneration mismatch: "+target+"/"+e.getKey());derived+=before.size();
                var stable=new TreeMap<>(before);stable.keySet().removeIf(k->k.endsWith("timings.csv")); for(var key:new ArrayList<>(stable.keySet()))if(key.endsWith(".md")){String content=Files.readString(target.resolve(key)).replaceAll("(?m)^실행 정보:.*$", "실행 정보: <repeat provenance>");stable.put(key,HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(content.getBytes(StandardCharsets.UTF_8))));}
                if(firstDerived==null)firstDerived=stable;else{require(firstDerived.equals(stable),"repeat derived mismatch: "+folder);pairedFiles+=stable.size();}
                var evidence=new TreeMap<String,String>();try(var originals=Files.list(target.resolve("results"))){
                    var originalsList=originals.filter(p->p.toString().endsWith(".json")).sorted().toList();Properties completion=new Properties();try(var reader=Files.newBufferedReader(target.resolve("completion.properties"))){completion.load(reader);}require(originalsList.size()==Integer.parseInt(completion.getProperty("cases")),"case count mismatch");runCases+=originalsList.size();total+=originalsList.size();
                    for(var file:originalsList){var row=json(file);if(row.get("evidenceHash") instanceof String h)evidence.put(file.getFileName().toString(),h);}
                }
                if(firstEvidence==null)firstEvidence=evidence;else{require(firstEvidence.equals(evidence),"repeat scientific evidence mismatch: "+folder);pairedEvidence+=evidence.size();}
                System.out.println("PASS "+suite+" "+manifest.get("profile")+" repeat="+number+" derived="+before.size());
            }
            var row=new LinkedHashMap<String,Object>();row.put("suite",suite);row.put("profile",manifest.get("profile"));row.put("folder",root.relativize(folder).toString().replace('\\','/'));row.put("repeats",repeats.size());row.put("cases",runCases);row.put("manifestSha256",sha(folder.resolve("manifest.json")));auditRows.add(row);
        }
        require(total==4684,"unexpected total: "+total);
        var report=new LinkedHashMap<String,Object>();report.put("status","VERIFIED");report.put("launcher","b3-standalone-java");report.put("sourceHash",source);report.put("backendHash",backend);report.put("totalVirtualCases",total);report.put("regeneratedDerivedFiles",derived);report.put("pairedDerivedFiles",pairedFiles);report.put("pairedScientificRecords",pairedEvidence);report.put("realNodeScenariosExecuted",0);report.put("runs",auditRows);
        Files.writeString(root.resolve("experiments/b3-live-migration/java/run-audit.json"),B3Json.encode(report)+"\n");
        System.out.println(B3Json.encode(report));
    }
}
