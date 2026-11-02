package io.openkache.core.replication.experiment;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import static io.openkache.core.replication.experiment.ReplicationExperiment.require;

/** 공통 Runner용 네 방식 비교와 Chain 위치별 crash 검증 backend. */
public final class ChainResearchMain {
    private ChainResearchMain() {}
    static List<ElectionComparison.Condition> conditions(boolean quick) {
        List<ElectionComparison.Condition> result=new ArrayList<>(ElectionComparison.conditions(quick));
        for(var s:ElectionComparison.Scenario.values()) for(int seed=1;seed<=(quick?1:3);seed++)
            for(int value:new int[]{64,8192}) for(int rate:quick?new int[]{1}:new int[]{1,10})
                result.add(new ElectionComparison.Condition("CHAIN",s,seed,value,rate));
        return List.copyOf(result);
    }
    public static void main(String[] args) throws IOException {
        if(args.length==2 && args[0].equals("--rebuild")) { regenerate(Path.of(args[1])); return; }
        if(args.length!=2 || !List.of("quick","full").contains(args[1])) throw new IllegalArgumentException("<output.dir> <quick|full> or --rebuild <output.dir>");
        Path folder=Path.of(args[0]); require(Files.isRegularFile(folder.resolve("launch.properties")),"use common Runner");
        Files.createDirectory(folder.resolve("results"));
        for(var c:conditions(args[1].equals("quick"))) Files.writeString(folder.resolve("results/"+c.id()+".json"),
                ElectionResearchMain.encode(ElectionResearchMain.row(ElectionComparison.run(c))));
        ChainPositionExperiment.writeOriginals(folder); regenerate(folder);
    }
    static void regenerate(Path folder) throws IOException {
        Files.deleteIfExists(folder.resolve("completion.properties"));
        ChainPositionExperiment.regenerate(folder); ElectionResearchMain.regenerate(folder,true);
    }
}
