package io.openkache.core.migration.strategy;

import io.openkache.core.migration.experiment.SchedulerAlgorithms;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** N/U/R 계획표만 만든다. Environment·서버·프로토콜·부하 실행 기능을 호출하지 않는다. */
public final class RealNodePlan {
    public static final List<String> PHASES=List.of("observe","decide","prepare","plan","schedule","copy","publish","settle","scale_in","drain","remove","observe_again");
    public static final List<String> FREQUENT=List.of("U01-load-spike-oscillation","U02-moving-skew","U03-dirty-down-up","U04-ready-delay","U05-transfer-jitter","U06-shared-endpoints","U07-scale-in-capacity-fragmentation");
    public static final List<String> RARE=List.of("R01-source-before-publish-crash","R02-destination-before-publish-crash","R03-source-after-publish-partition","R04-destination-after-publish-crash","R05-controller-restart","R06-partial-cleanup-retry","R07-duplicate-stale-messages");
    private record Profile(String scaler,String planner,String capability,List<String> copies){}
    private RealNodePlan(){}
    private static List<Profile> profiles(){return ScalingPolicies.LITERATURE.stream().map(s->new Profile(s,FixedTabletPlacement.NAME,"core",CopyDrivers.METHODS)).toList();}
    public static List<Map<String,Object>> configurations(){var rows=new ArrayList<Map<String,Object>>();var ids=new java.util.HashSet<String>();
        for(String flow:List.of("N","U","R"))for(var p:profiles())for(String scheduler:SchedulerAlgorithms.METHODS)for(String copy:p.copies())for(String drain:DrainStrategies.METHODS){
            var selection=new StrategySelection(p.scaler(),p.planner(),scheduler,copy,drain);var identity=new LinkedHashMap<String,Object>(selection.record());identity.put("flow",flow);identity.put("capability",p.capability());identity.put("threshold_ms",200);identity.put("max_rounds",10);
            String id="RN-"+B3Json.hash(identity).substring(0,12);if(!ids.add(id))throw new IllegalStateException("duplicate configuration");var row=new LinkedHashMap<String,Object>();row.put("config_id",id);row.putAll(identity);var problems=new ArrayList<String>();if(!flow.equals("N"))problems.addAll(FREQUENT);if(flow.equals("R"))problems.addAll(RARE);row.put("problems",String.join("|",problems));row.put("phases",String.join("|",PHASES));rows.add(row);}
        if(rows.size()!=1134||rows.stream().filter(r->r.get("capability").equals("core")).count()!=1134)throw new IllegalStateException("plan matrix changed");return List.copyOf(rows);
    }
    public static Map<String,Object> summary(){var out=new LinkedHashMap<String,Object>();out.put("status","PLANNED_NOT_RUN");out.put("activeFlows",List.of("N","U","R"));out.put("problemSubsetEnumeration",false);out.put("allAssignedProblemsIncluded",true);out.put("coreProfiles",7);out.put("splitMergeProfiles",0);out.put("paxNativeProfiles",0);out.put("schedulers",9);out.put("drains",2);out.put("coreConfigurationsPerFlow",378);out.put("configurationsPerFlow",378);out.put("coreConfigurations",1134);out.put("optionalConfigurations",0);out.put("totalConfigurations",1134);out.put("workloadSeedsPerConfiguration",5);out.put("coreRunsPerEnvironment",5670);out.put("optionalRunsPerEnvironment",0);out.put("totalRunsPerEnvironment",5670);out.put("physicalServersCreated",0);out.put("experimentsStarted",0);out.put("generator","b3-java-count-only-plan-2");out.put("phases",PHASES);return out;}
    public static void write(Path folder) throws IOException {
        Files.createDirectories(folder);var configurations=configurations();Files.writeString(folder.resolve("configurations.csv"),B3Csv.write(new ArrayList<>(configurations.getFirst().keySet()),configurations),StandardCharsets.UTF_8);
        var runs=new ArrayList<Map<String,Object>>();for(var c:configurations)for(int seed=1;seed<=5;seed++){var row=new LinkedHashMap<String,Object>();row.put("run_id",c.get("config_id")+"-s"+seed);row.put("config_id",c.get("config_id"));row.put("flow",c.get("flow"));row.put("seed",seed);row.put("capability",c.get("capability"));row.put("status","PLANNED_NOT_RUN");runs.add(row);}Files.writeString(folder.resolve("runs-per-environment.csv"),B3Csv.write(new ArrayList<>(runs.getFirst().keySet()),runs),StandardCharsets.UTF_8);
        var rare=new ArrayList<>(FREQUENT);rare.addAll(RARE);var stories=List.of(Map.of("flow","N","name","기본적인 정상 흐름","all_injected_problems","","meaning","정상 조건의 전체 처리 단계"),Map.of("flow","U","name","자주 발생하는 불안정한 흐름","all_injected_problems",String.join("|",FREQUENT),"meaning","할당된 일곱 조건 모두 포함"),Map.of("flow","R","name","가끔 발생하는 불안정한 흐름","all_injected_problems",String.join("|",rare),"meaning","할당된 일곱 조건과 일곱 장애 모두 포함"));
        Files.writeString(folder.resolve("stories.csv"),B3Csv.write(List.of("flow","name","all_injected_problems","meaning"),stories),StandardCharsets.UTF_8);Files.writeString(folder.resolve("plan-summary.json"),B3Json.encode(summary())+"\n",StandardCharsets.UTF_8);
    }
}
