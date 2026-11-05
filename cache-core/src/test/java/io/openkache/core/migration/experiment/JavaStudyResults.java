package io.openkache.core.migration.experiment;

import io.openkache.core.migration.strategy.B3Csv;
import io.openkache.core.migration.strategy.B3Json;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/** 새 Java 연구 schema의 원본·evidence·파생 파일. 예전 Python schema와 혼합하지 않는다. */
final class JavaStudyResults {
    private JavaStudyResults(){}
    static Properties properties(Path file) throws IOException {var p=new Properties();try(var reader=Files.newBufferedReader(file,StandardCharsets.UTF_8)){p.load(reader);}return p;}
    static void write(Path file,String schema,int index,Map<String,Object> record,Map<String,Object> context) throws IOException {
        var body=new LinkedHashMap<>(record);Object timing=body.remove("planningNanos");var row=new LinkedHashMap<>(context);row.put("resultSchemaVersion",schema);row.put("caseIndex",index);row.put("record",body);row.put("evidenceHash",B3Json.hash(body));if(timing!=null)row.put("planningNanos",timing);row.put("timeDomain","virtual");row.put("storageLayer","b3-java-fcfs-model-no-real-KV");row.put("executionId",context.get("suiteExecutionId")+"-r"+context.get("runNumber")+"-case"+index);
        Files.writeString(file,B3Json.encode(row)+"\n",StandardCharsets.UTF_8);
    }
    static List<Map<String,Object>> read(Path folder,String schema,int expected) throws IOException {
        var rows=new ArrayList<Map<String,Object>>();String provenance=null;
        for(int i=0;i<expected;i++){Path file=folder.resolve("results").resolve(String.format(java.util.Locale.ROOT,"case-%04d.json",i));var row=B3Json.object(B3Json.decode(Files.readString(file,StandardCharsets.UTF_8)));
            require(schema.equals(row.get("resultSchemaVersion"))&&n(row,"caseIndex")==i,"schema/identity mismatch");var record=B3Json.object(row.get("record"));require(B3Json.hash(record).equals(row.get("evidenceHash")),"changed Java study evidence");
            String signature=List.of("sourceHash","backendHash","runnerHash","suiteExecutionId","runNumber").stream().map(k->String.valueOf(row.get(k))).collect(java.util.stream.Collectors.joining("|"));if(provenance==null)provenance=signature;require(provenance.equals(signature),"mixed provenance");if(row.containsKey("planningNanos")){require(n(row,"planningNanos")>=0,"negative planning time");record.put("planningNanos",row.get("planningNanos"));}rows.add(record);}
        try(var files=Files.list(folder.resolve("results"))){require(files.count()==expected,"extra or missing originals");}return rows;
    }
    static void complete(Path folder,String schema,String profile,int count) throws IOException {Files.writeString(folder.resolve("completion.properties"),"status=complete\nresultSchemaVersion="+schema+"\nprofile="+profile+"\ncases="+count+"\n",StandardCharsets.UTF_8);}
    static void summarize(Path folder,List<Map<String,Object>> records,String title) throws IOException {
        var fields=new LinkedHashSet<String>(List.of("group","scenario","method","seed","outcome"));var output=new ArrayList<Map<String,Object>>();
        for(var record:records){var row=new LinkedHashMap<String,Object>();for(String key:List.of("group","scenario","method","seed","outcome"))row.put(key,record.get(key));var metrics=B3Json.object(record.get("metrics"));row.putAll(metrics);fields.addAll(metrics.keySet());output.add(row);}
        Files.writeString(folder.resolve("summary.csv"),B3Csv.write(new ArrayList<>(fields),output),StandardCharsets.UTF_8);var md=new StringBuilder("# "+title+"\n\nJava/JDK 독립 연구 모형, "+records.size()+"회. 실제 서버·원 제품 재현이 아니다. 진행 중 action을 완료로 표시하지 않는다.\n\n| 방법 | 실행 | 모형 기한 초과율 평균 | node-window 평균 | 미완료 action 합계 |\n|---|---:|---:|---:|---:|\n");
        if(records.stream().allMatch(r->r.containsKey("planningNanos"))){var times=records.stream().map(r->Map.<String,Object>of("group",r.get("group"),"scenario",r.get("scenario"),"method",r.get("method"),"seed",r.get("seed"),"planning_nanos",r.get("planningNanos"))).toList();Files.writeString(folder.resolve("timings.csv"),B3Csv.write(List.of("group","scenario","method","seed","planning_nanos"),times),StandardCharsets.UTF_8);}
        for(String method:records.stream().map(r->r.get("method").toString()).distinct().toList()){
            var selected=records.stream().filter(r->method.equals(r.get("method"))).toList();var latency=selected.stream().map(r->B3Json.object(r.get("metrics"))).filter(m->m.containsKey("deadline_miss_rate")).toList();
            md.append("| ").append(method).append(" | ").append(selected.size()).append(" | ");if(latency.isEmpty())md.append("해당 없음 | 해당 없음 | 해당 없음");else md.append(String.format(java.util.Locale.ROOT,"%.6f | %.3f | %d",latency.stream().mapToDouble(m->d(m,"deadline_miss_rate")).average().orElseThrow(),latency.stream().mapToDouble(m->d(m,"node_windows")).average().orElseThrow(),latency.stream().mapToLong(m->n(m,"pending_actions")).sum()));md.append(" |\n");}
        md.append("\nJava의 명시적 generator·수치 해법 버전으로 실행한다. 이전 NumPy/SciPy 실행의 수치를 Java 실행 결과로 재사용하지 않는다. 원본·모형·입력 hash와 미완료 상태를 함께 해석한다.\n");Files.writeString(folder.resolve("results.md"),md,StandardCharsets.UTF_8);
    }
    static long n(Map<String,Object> map,String key){Object v=map.get(key);if(!(v instanceof Number number)||!Double.isFinite(number.doubleValue())||number.doubleValue()!=number.longValue())throw new IllegalArgumentException("integer required: "+key);return number.longValue();}
    static double d(Map<String,Object> map,String key){if(!(map.get(key) instanceof Number number)||!Double.isFinite(number.doubleValue()))throw new IllegalArgumentException("finite metric required: "+key);return number.doubleValue();}
    static void require(boolean condition,String reason){if(!condition)throw new IllegalArgumentException(reason);}
    static void validateQueue(Map<String,Object> row){
        var metrics=B3Json.object(row.get("metrics"));var windows=objects(row.get("windows"));var actions=objects(row.get("actions"));var config=B3Json.object(row.get("config"));require(windows.size()==n(config,"windows"),"missing windows");
        long requests=0,completed=0,late=0,nodeWindows=0;for(int i=0;i<windows.size();i++){var w=windows.get(i);require(n(w,"window")==i,"window identity");requests+=n(w,"requests");completed+=n(w,"completed");late+=n(w,"deadline_misses");nodeWindows+=n(w,"allocated_nodes");require(n(w,"ready_nodes")>=n(config,"minNodes")&&n(w,"ready_nodes")<=n(w,"allocated_nodes")&&n(w,"allocated_nodes")<=n(config,"maxNodes"),"node readiness bounds");}
        require(requests==n(metrics,"requests")&&completed==n(metrics,"completed")&&requests==completed+n(metrics,"outstanding")&&nodeWindows==n(metrics,"node_windows"),"queue accounting");require(Math.abs((double)late/Math.max(1,requests)-d(metrics,"deadline_miss_rate"))<1e-12,"deadline accounting");require(actions.stream().filter(a->a.get("completed_window")==null).count()==n(metrics,"pending_actions"),"pending count");
        long previous=-1;for(var a:actions){require(n(a,"start_window")>previous,"overlapping action");if(a.get("completed_window")!=null){require(n(a,"start_window")<=n(a,"cutover_window")&&n(a,"cutover_window")<=n(a,"completed_window"),"cutover barrier");previous=n(a,"completed_window");}else require(a==actions.getLast(),"pending action must be last");}
    }
    static void validateFixedQueue(Map<String,Object> row,io.openkache.core.migration.strategy.ScalingConfig config){
        validateQueue(row);
        require("node-count-only".equals(row.get("decision_scope"))&&io.openkache.core.migration.strategy.FixedTabletPlacement.VERSION.equals(row.get("placement_policy")),"count-only placement identity");
        var metrics=B3Json.object(row.get("metrics"));
        require(n(metrics,"split_units")==0&&n(metrics,"merged_units")==0&&n(metrics,"rebalance")==0,"B2 operation in B3 result");
        var initial=new io.openkache.core.migration.strategy.ScalingSimulation(config,"fixed_single");
        var units=initial.units();var active=initial.active();
        for(var window:objects(row.get("windows")))require(n(window,"unit_count")==units.size(),"Tablet refinement detected");
        var trace=io.openkache.core.migration.strategy.ScalingSimulation.trace(row.get("scenario").toString(),n(row,"seed"),config);
        for(var action:objects(row.get("actions"))){
            var old=((List<?>)action.get("old_nodes")).stream().map(v->((Number)v).intValue()).toList();
            var next=((List<?>)action.get("new_nodes")).stream().map(v->((Number)v).intValue()).toList();
            require(old.equals(active)&&!old.equals(next)&&next.equals(io.openkache.core.migration.strategy.ScalingSupport.nodes(next.size())),"non-count action");
            var actual=objects(action.get("assignments")).stream().map(u->new io.openkache.core.migration.strategy.PartitionUnit(
                    ((List<?>)u.get("keys")).stream().map(v->((Number)v).intValue()).toList(),
                    ((List<?>)u.get("holders")).stream().map(v->((Number)v).intValue()).toList())).toList();
            int t=(int)n(action,"start_window")-1;require(t>=0&&t<config.windows(),"decision window");
            double[] reads=new double[config.keys()],writes=new double[config.keys()];
            for(var request:trace.get(t))(request.write()?writes:reads)[request.key()]++;
            var cpus=new java.util.TreeMap<Integer,Double>();active.forEach(node->cpus.put(node,0.));
            var snapshot=new io.openkache.core.migration.strategy.ScalingSnapshot(config,t,active,units,reads,writes,cpus,0,trace.get(t).size(),0,new long[config.maxNodes()],-100,"");
            var expected=io.openkache.core.migration.strategy.FixedTabletPlacement.units(snapshot,next.size());
            require(actual.equals(expected),"method-specific placement detected");
            require(n(action,"planned_bytes")==io.openkache.core.migration.strategy.ScalingSupport.movedBytes(units,actual,config),"fixed placement byte accounting");
            io.openkache.core.migration.strategy.ScalingSupport.validate(new io.openkache.core.migration.strategy.ScalingPlan(next,actual,"audit",Map.of()),config,false);
            active=next;units=actual;
        }
    }
    static List<Map<String,Object>> objects(Object value){if(!(value instanceof List<?> list))throw new IllegalArgumentException("list required");return list.stream().map(B3Json::object).toList();}
}
