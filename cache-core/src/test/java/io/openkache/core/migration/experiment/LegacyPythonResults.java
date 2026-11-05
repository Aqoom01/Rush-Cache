package io.openkache.core.migration.experiment;

import io.openkache.core.migration.strategy.B3Json;
import io.openkache.core.migration.strategy.ScalingConfig;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

/** 과거 Python JSON을 JDK만으로 검사·요약한다. NumPy 요청 생성기를 재실행했다고 표시하지 않는다. */
final class LegacyPythonResults {
    public record Task(int id,int cpu,long memory,int owner,List<Integer> eligible){public Task{eligible=List.copyOf(eligible);}}
    public record Input(String scenario,List<Task> tasks,List<Integer> active,int cpu,long memory,int minimum,int maximum,int replicas){public Input{tasks=List.copyOf(tasks);active=List.copyOf(active);}}
    static boolean admitted(Input input){return input.active().size()>=Math.max(input.minimum(),input.replicas())&&input.active().size()<=input.maximum();}
    static boolean fits(Input input,List<Integer> assigned){if(!admitted(input)||assigned.size()!=input.tasks().size())return false;for(int i=0;i<assigned.size();i++)if(!input.active().contains(assigned.get(i))||!input.tasks().get(i).eligible().contains(assigned.get(i)))return false;
        for(int node:input.active()){long memory=0;int cpu=0;for(int i=0;i<assigned.size();i++)if(assigned.get(i)==node){memory+=input.tasks().get(i).memory();cpu+=input.tasks().get(i).cpu();}if(memory>input.memory()||cpu>input.cpu())return false;}return true;}
    static long moved(Input input,List<Integer> assigned){long cost=0;for(int i=0;i<assigned.size();i++)if(assigned.get(i)!=input.tasks().get(i).owner())cost+=input.tasks().get(i).memory();return cost;}
    static Long oracle(Input input){if(!admitted(input))return null;long best=enumerate(input,new ArrayList<>(),Long.MAX_VALUE);return best==Long.MAX_VALUE?null:best;}
    private static long enumerate(Input input,List<Integer> assigned,long best){int at=assigned.size();if(at==input.tasks().size())return fits(input,assigned)?Math.min(best,moved(input,assigned)):best;for(int node:input.tasks().get(at).eligible())if(input.active().contains(node)){assigned.add(node);best=enumerate(input,assigned,best);assigned.removeLast();}return best;}
    private LegacyPythonResults(){}
    static String encode(Object value){
        if(value instanceof Map<?,?> map){var sorted=new TreeMap<String,Object>();map.forEach((k,v)->sorted.put(k.toString(),v));return sorted.entrySet().stream().map(e->B3Json.encode(e.getKey())+":"+encode(e.getValue())).collect(java.util.stream.Collectors.joining(",","{","}"));}
        if(value instanceof List<?> list)return list.stream().map(LegacyPythonResults::encode).collect(java.util.stream.Collectors.joining(",","[","]"));
        if(value instanceof Double d){if(!Double.isFinite(d))throw new IllegalArgumentException("nonfinite legacy number");if(d==0)return Double.doubleToRawLongBits(d)<0?"-0.0":"0.0";double abs=Math.abs(d);var decimal=BigDecimal.valueOf(d).stripTrailingZeros();if(abs>=1e-4&&abs<1e16){String text=decimal.toPlainString();return text.contains(".")?text:text+".0";}int exponent=decimal.precision()-decimal.scale()-1;String mantissa=decimal.movePointLeft(exponent).toPlainString();return mantissa+"e"+String.format(java.util.Locale.ROOT,"%+03d",exponent);}
        return B3Json.encode(value);
    }
    static String digest(Object value){return B3ResearchLauncher.sha(encode(value).getBytes(StandardCharsets.UTF_8));}
    static Map<String,Object> read(Path file) throws IOException {return B3Json.object(B3Json.decode(Files.readString(file,StandardCharsets.UTF_8)));}
    static void validatePolicy(Map<String,Object> record){
        JavaStudyResults.require("b3-execution-policy-json-1".equals(record.get("resultSchemaVersion")),"unknown Python policy schema");var evidence=new LinkedHashMap<>(record);for(String key:List.of("evidenceHash","planningNanos","provenance"))evidence.remove(key);JavaStudyResults.require(digest(evidence).equals(record.get("evidenceHash")),"legacy evidence mismatch");var input=B3Json.object(record.get("input"));JavaStudyResults.require(digest(input).equals(record.get("inputHash")),"legacy input mismatch");
        if(record.get("group").equals("feasibility")){
            var tasks=JavaStudyResults.objects(input.get("tasks")).stream().map(t->new Task((int)JavaStudyResults.n(t,"id"),(int)JavaStudyResults.n(t,"cpu"),JavaStudyResults.n(t,"memory"),(int)JavaStudyResults.n(t,"owner"),ints(t.get("eligible")))).toList();
            var in=new Input(input.get("case").toString(),tasks,ints(input.get("active")),(int)JavaStudyResults.n(input,"cpu"),JavaStudyResults.n(input,"memory"),(int)JavaStudyResults.n(input,"minimum"),(int)JavaStudyResults.n(input,"maximum"),(int)JavaStudyResults.n(input,"replicas"));Long optimal=oracle(in);var metrics=B3Json.object(record.get("metrics"));Object assignment=record.get("assignment");
            JavaStudyResults.require((optimal==null)==(record.get("oracleMinimumBytes")==null),"legacy oracle availability");if(optimal!=null)JavaStudyResults.require(optimal==((Number)record.get("oracleMinimumBytes")).longValue(),"legacy oracle minimum");
            JavaStudyResults.require(JavaStudyResults.n(metrics,"feasible")== (optimal==null?0:1)&&JavaStudyResults.n(metrics,"conservative_rejection")== (assignment==null&&optimal!=null?1:0),"legacy feasibility accounting");
            if(assignment!=null){var a=ints(assignment);JavaStudyResults.require(fits(in,a)&&moved(in,a)==JavaStudyResults.n(metrics,"moved_bytes")&&JavaStudyResults.n(metrics,"accepted")==1&&record.get("outcome").equals("accepted"),"unsafe legacy placement");if(record.get("method").equals("accordion"))JavaStudyResults.require(optimal!=null&&moved(in,a)==optimal,"nonoptimal legacy placement");}
            else JavaStudyResults.require(JavaStudyResults.n(metrics,"accepted")==0&&record.get("outcome").equals("rejected")&&metrics.get("moved_bytes")==null&&(!record.get("method").equals("accordion")||optimal==null),"forged legacy rejection");
        }else{
            var normalized=new LinkedHashMap<>(record);normalized.put("config",B3Json.object(B3Json.decode(B3Json.encode(config(B3Json.object(input.get("config")))))));JavaStudyResults.validateQueue(normalized);
            JavaStudyResults.require(input.get("workloadHash") instanceof String hash&&hash.matches("[0-9a-f]{64}")&&input.get("historyHash") instanceof String history&&history.matches("[0-9a-f]{64}"),"legacy input hashes required");
        }
    }
    static void rebuildPolicy(Path folder,String profile) throws IOException {
        int expected=profile.equals("full")?820:profile.equals("quick")?136:0;if(expected==0)throw new IllegalArgumentException("unknown profile");var records=new ArrayList<Map<String,Object>>();var identities=new java.util.HashSet<String>();String provenance=null;
        for(int i=0;i<expected;i++){var row=read(folder.resolve("results").resolve(String.format(java.util.Locale.ROOT,"result-%04d.json",i)));validatePolicy(row);String signature=B3Json.hash(row.get("provenance"));if(provenance==null)provenance=signature;JavaStudyResults.require(provenance.equals(signature),"mixed legacy provenance");String id=row.get("group")+":"+row.get("case")+":"+row.get("seed")+":"+row.get("method")+":"+row.get("inputHash");JavaStudyResults.require(identities.add(id),"duplicate legacy condition");var copy=new LinkedHashMap<>(row);copy.put("scenario",row.get("case"));records.add(copy);}
        try(var paths=Files.list(folder.resolve("results"))){JavaStudyResults.require(paths.filter(p->p.toString().endsWith(".json")).count()==expected,"extra legacy originals");}JavaStudyResults.summarize(folder,records,"과거 Python 정책 원본의 Java 재생성");
        Files.writeString(folder.resolve("legacy-validation.md"),"과거 evidence/input checksum·배치 oracle·queue 집계를 Java로 확인했다. NumPy PCG64 요청열을 새로 생성한 검증은 아니다. 기존 완료 결과의 수치를 새 Java 실행으로 표시하지 않는다.\n",StandardCharsets.UTF_8);
    }
    static void rebuildLiterature(Path folder,String profile) throws IOException {
        var completion=JavaStudyResults.properties(folder.resolve("completion.properties"));JavaStudyResults.require("b3-literature-algorithms-json-1".equals(completion.getProperty("resultSchemaVersion")),"unknown legacy literature schema");var records=new ArrayList<Map<String,Object>>();var expected=new java.util.HashSet<String>();for(String scenario:io.openkache.core.migration.strategy.ScalingSimulation.SCENARIOS)for(int seed=1;seed<=LiteratureResearchMain.seeds(profile);seed++)for(String method:LiteratureResearchMain.METHODS)expected.add(scenario+":"+seed+":"+method);String provenance=null;
        try(var paths=Files.list(folder.resolve("results"))){for(Path path:paths.filter(p->p.toString().endsWith(".json")).sorted().toList()){
            var record=read(path);JavaStudyResults.require("b3-literature-algorithms-json-1".equals(record.get("resultSchemaVersion")),"mixed legacy schema");var evidence=new LinkedHashMap<>(record);Object checksum=evidence.remove("evidence_hash");JavaStudyResults.require(digest(evidence).equals(checksum),"legacy literature checksum");String id=record.get("scenario")+":"+record.get("seed")+":"+record.get("method");JavaStudyResults.require(expected.remove(id),"duplicate/unknown legacy identity");String signature=digest(record.get("provenance"));if(provenance==null)provenance=signature;JavaStudyResults.require(provenance.equals(signature),"mixed legacy provenance");
            var config=config(B3Json.object(record.get("config")));var normalized=new LinkedHashMap<>(record);normalized.put("config",B3Json.object(B3Json.decode(B3Json.encode(config))));normalized.put("group",record.get("comparison_group"));normalized.put("outcome","observed");JavaStudyResults.validateQueue(normalized);
            var input=read(folder.resolve("inputs").resolve(record.get("scenario")+"-"+record.get("seed")+".json"));JavaStudyResults.require(digest(input.get("events")).equals(record.get("workload_hash"))&&digest(input.get("history")).equals(record.get("history_hash")),"changed saved legacy trace");var windows=JavaStudyResults.objects(record.get("windows"));if(!(input.get("events") instanceof List<?> events))throw new IllegalArgumentException("saved trace required");for(int t=0;t<windows.size();t++)JavaStudyResults.require(events.get(t) instanceof List<?> requests&&requests.size()==JavaStudyResults.n(windows.get(t),"requests"),"legacy request count");records.add(normalized);
        }}JavaStudyResults.require(expected.isEmpty(),"missing legacy literature original");JavaStudyResults.summarize(folder,records,"과거 Python 선행 방법 원본의 Java 재생성");
    }
    private static List<Integer> ints(Object value){if(!(value instanceof List<?> list))throw new IllegalArgumentException("integer list required");return list.stream().map(v->{if(!(v instanceof Number n)||n.doubleValue()!=n.intValue())throw new IllegalArgumentException("integer required");return n.intValue();}).toList();}
    private static ScalingConfig config(Map<String,Object> input){var p=new Properties();for(var entry:input.entrySet()){String key=entry.getKey().replace('_','.');p.setProperty("scaling."+key,entry.getValue().toString());}return ScalingConfig.from(p);}
}
