package io.openkache.core.migration.experiment;

import io.openkache.core.migration.strategy.B3Json;
import io.openkache.core.migration.strategy.PerformanceModel;
import io.openkache.core.migration.strategy.ScalingConfig;
import io.openkache.core.migration.strategy.ScalingSimulation;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** 선행 8개 알고리즘의 Java/JDK backend. Python·외부 interpreter를 실행하지 않는다. */
public final class LiteratureResearchMain {
    static final String SCHEMA="b3-node-count-fixed-placement-java-1";
    static final List<String> METHODS=List.of("scads","elastman","estore","accordion","pstore","tiramola","pax","fixed_single");
    private LiteratureResearchMain(){}
    static int seeds(String profile){if(!List.of("quick","full").contains(profile))throw new IllegalArgumentException("unknown profile");return profile.equals("quick")?2:5;}
    static int expected(String profile){return METHODS.size()*ScalingSimulation.SCENARIOS.size()*seeds(profile);}
    public static void main(String[] args) throws IOException {
        if(args.length==2&&args[0].equals("--rebuild")){Path folder=Path.of(args[1]);var p=JavaStudyResults.properties(folder.resolve("completion.properties"));JavaStudyResults.require(SCHEMA.equals(p.getProperty("resultSchemaVersion")),"old coupled-placement results are not B3 count-only results");rebuild(folder,p.getProperty("profile"));return;}
        if(args.length!=4)throw new IllegalArgumentException("<folder> <full|quick> <repeat> <repeats>");Path folder=Path.of(args[0]);String profile=args[1];int repeat=Integer.parseInt(args[2]),repeats=Integer.parseInt(args[3]);if(repeat<1||repeat>repeats)throw new IllegalArgumentException("invalid repeat");
        var config=ScalingConfig.defaults().profile(profile.equals("quick"));seeds(profile);var context=BaselineResearchMain.context(JavaStudyResults.properties(folder.resolve("launch.properties")),repeat,repeats);context.put("modelConfig",config);context.put("nodeCount",config.initialNodes());var model=PerformanceModel.calibrate(config);Path originals=Files.createDirectory(folder.resolve("results")),inputs=Files.createDirectory(folder.resolve("inputs"));
        Files.writeString(folder.resolve("calibration.json"),B3Json.encode(Map.of("model",model.record(),"queue_calibration",model.observations(),"reward_training",model.rewardData()))+"\n",StandardCharsets.UTF_8);int index=0;
        for(String scenario:ScalingSimulation.SCENARIOS)for(int seed=1;seed<=seeds(profile);seed++){
            Files.writeString(inputs.resolve(scenario+"-"+seed+".json"),B3Json.encode(Map.of("generator",ScalingSimulation.GENERATOR,"events",ScalingSimulation.eventRows(ScalingSimulation.trace(scenario,seed,config)),"history",ScalingSimulation.history(scenario,seed,config)))+"\n",StandardCharsets.UTF_8);
            for(String method:METHODS){var record=record(method,scenario,seed,config,model);JavaStudyResults.write(originals.resolve(String.format(java.util.Locale.ROOT,"case-%04d.json",index)),SCHEMA,index,record,context);index++;}}
        rebuild(folder,profile);JavaStudyResults.complete(folder,SCHEMA,profile,index);
    }
    static Map<String,Object> record(String method,String scenario,long seed,ScalingConfig config,PerformanceModel model){var row=new LinkedHashMap<>(ScalingSimulation.execute(method,scenario,seed,config,model));row.put("group",group(method));row.put("config",config);row.put("outcome","observed");row.put("calibration",model.record());return row;}
    static String group(String method){return "fixed-tablets-single-owner";}
    static void rebuild(Path folder,String profile) throws IOException {
        var rows=JavaStudyResults.read(folder,SCHEMA,expected(profile));var expected=new java.util.HashSet<String>();for(String scenario:ScalingSimulation.SCENARIOS)for(int seed=1;seed<=seeds(profile);seed++)for(String method:METHODS)expected.add(scenario+":"+seed+":"+method);
        var seen=new java.util.HashSet<String>();var config=ScalingConfig.defaults().profile(profile.equals("quick"));
        for(var row:rows){String method=row.get("method").toString(),scenario=row.get("scenario").toString();long seed=JavaStudyResults.n(row,"seed");String id=scenario+":"+seed+":"+method;JavaStudyResults.require(expected.contains(id)&&seen.add(id),"changed/duplicate literature condition");JavaStudyResults.require(B3Json.hash(row.get("config")).equals(B3Json.hash(config))&&group(method).equals(row.get("group")),"changed config/group");JavaStudyResults.validateFixedQueue(row,config);
            var input=B3Json.object(B3Json.decode(Files.readString(folder.resolve("inputs").resolve(scenario+"-"+seed+".json"),StandardCharsets.UTF_8)));JavaStudyResults.require(ScalingSimulation.GENERATOR.equals(input.get("generator"))&&B3Json.hash(input.get("events")).equals(row.get("workload_hash"))&&B3Json.hash(input.get("history")).equals(row.get("history_hash")),"changed workload/history");}
        JavaStudyResults.require(seen.equals(expected),"missing literature condition");JavaStudyResults.summarize(folder,rows,"노드 수 판단 7개·고정 배치 Java 결과");
    }
}
