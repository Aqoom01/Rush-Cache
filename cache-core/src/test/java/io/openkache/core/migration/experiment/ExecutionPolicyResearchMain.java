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
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

/** 배치 가능성·준비 지연의 독립 Java backend. 새 schema는 Java generator를 명시한다. */
public final class ExecutionPolicyResearchMain {
    static final String SCHEMA="b3-count-delay-fixed-placement-java-1";
    private record Spec(String group,String scenario,int seed,String method,int provision,long bandwidth,double penalty){}
    private ExecutionPolicyResearchMain(){}
    static ScalingConfig config(String profile,int provision,long bandwidth,double penalty){var p=new Properties();p.setProperty("scaling.windows",profile.equals("full")?"64":"32");p.setProperty("scaling.forecast.horizon",profile.equals("full")?"24":"16");p.setProperty("scaling.provision.windows",""+provision);p.setProperty("scaling.transfer.bytes",""+bandwidth);p.setProperty("scaling.migration.penalty",""+penalty);return ScalingConfig.from(p);}
    static List<Spec> specs(String profile){int seeds=LiteratureResearchMain.seeds(profile);var out=new ArrayList<Spec>();for(String scenario:List.of("step","spike","periodic","low"))for(int seed=1;seed<=seeds;seed++)for(int provision:profile.equals("full")?new int[]{0,4,12}:new int[]{0,4})for(long bandwidth:profile.equals("full")?new long[]{256,2048}:new long[]{512,2048})for(double penalty:profile.equals("full")?new double[]{0,.30}:new double[]{.15})for(String method:List.of("aws-warmup","elastman","pstore"))out.add(new Spec("delay",scenario,seed,method,provision,bandwidth,penalty));return out;}
    public static void main(String[] args) throws IOException {
        if(args.length==2&&args[0].equals("--rebuild")){Path folder=Path.of(args[1]);var p=JavaStudyResults.properties(folder.resolve("completion.properties"));rebuild(folder,p.getProperty("profile"));return;}
        if(args.length==3&&args[0].equals("--legacy-rebuild")){LegacyPythonResults.rebuildPolicy(Path.of(args[1]),args[2]);return;}
        if(args.length!=4)throw new IllegalArgumentException("<folder> <full|quick> <repeat> <repeats>");Path folder=Path.of(args[0]);String profile=args[1];int repeat=Integer.parseInt(args[2]),repeats=Integer.parseInt(args[3]);if(repeat<1||repeat>repeats)throw new IllegalArgumentException("invalid repeat");var specs=specs(profile);var context=BaselineResearchMain.context(JavaStudyResults.properties(folder.resolve("launch.properties")),repeat,repeats);var model=PerformanceModel.calibrate(config(profile,2,2048,.15));Path originals=Files.createDirectory(folder.resolve("results"));
        Files.writeString(folder.resolve("calibration.json"),B3Json.encode(Map.of("model",model.record(),"observations",model.observations(),"rewards",model.rewardData()))+"\n",StandardCharsets.UTF_8);
        for(int i=0;i<specs.size();i++){var spec=specs.get(i);Map<String,Object> row;
            {
                var c=config(profile,spec.provision(),spec.bandwidth(),spec.penalty());row=new LinkedHashMap<>(ScalingSimulation.execute(spec.method(),spec.scenario(),spec.seed(),c,model));row.put("group","delay");row.put("outcome","observed");row.put("config",c);var metrics=B3Json.object(row.get("metrics"));var actions=JavaStudyResults.objects(row.get("actions"));var done=actions.stream().filter(a->a.get("completed_window")!=null).toList();metrics.put("mean_action_delay_windows",done.isEmpty()?null:done.stream().mapToDouble(a->JavaStudyResults.d(a,"delay_windows")).average().orElseThrow());var decisions=JavaStudyResults.objects(row.get("decisions"));metrics.put("decisions_blocked_by_rebalance",decisions.stream().filter(d->d.get("reason").equals("action-in-progress")).count());metrics.put("warmup_scale_in_blocks",decisions.stream().filter(d->d.get("reason").equals("warmup-block-scale-in")).count());metrics.put("invariant_violations",0);row.put("metrics",metrics);
            }
            JavaStudyResults.write(originals.resolve(String.format(java.util.Locale.ROOT,"case-%04d.json",i)),SCHEMA,i,row,context);
        }rebuild(folder,profile);JavaStudyResults.complete(folder,SCHEMA,profile,specs.size());
    }
    static void rebuild(Path folder,String profile) throws IOException {
        var specs=specs(profile);var rows=JavaStudyResults.read(folder,SCHEMA,specs.size());for(int i=0;i<rows.size();i++){var row=rows.get(i);var spec=specs.get(i);JavaStudyResults.require(spec.group().equals(row.get("group"))&&spec.method().equals(row.get("method"))&&spec.scenario().equals(row.get("scenario"))&&spec.seed()==JavaStudyResults.n(row,"seed"),"changed policy condition");
            {var config=config(profile,spec.provision(),spec.bandwidth(),spec.penalty());JavaStudyResults.require(B3Json.hash(config).equals(B3Json.hash(row.get("config"))),"changed delay config");JavaStudyResults.validateFixedQueue(row,config);JavaStudyResults.require(B3Json.hash(ScalingSimulation.eventRows(ScalingSimulation.trace(spec.scenario(),spec.seed(),config))).equals(row.get("workload_hash"))&&B3Json.hash(ScalingSimulation.history(spec.scenario(),spec.seed(),config)).equals(row.get("history_hash")),"changed delay input");}}
        JavaStudyResults.summarize(folder,rows,"고정 배치의 증감 준비 지연 Java 결과");
    }
}
