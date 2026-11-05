package io.openkache.core.migration.strategy;

import io.openkache.core.migration.MigrationScheduler;
import java.util.List;
import java.util.Properties;

/** 한 설정에서 선택·예산·복사 인자를 읽는다. I/O나 실험 실행을 하지 않는 불변 값이다. */
public record B3Configuration(String flow,StrategySelection selection,ScalingConfig scaling,
        MigrationScheduler.Settings scheduler,CopyDrivers.Settings copy,StrategySelection.Capabilities capabilities) {
    public B3Configuration{if(!List.of("N","U","R").contains(flow))throw new IllegalArgumentException("only N/U/R flows are active");selection.require(capabilities);}
    public static B3Configuration from(Properties p){
        for(String key:p.stringPropertyNames())if(key.startsWith("strategy.")&&!List.of("strategy.scaler","strategy.planner","strategy.scheduler","strategy.copy","strategy.drain").contains(key))throw new IllegalArgumentException("unknown strategy option: "+key);
        var capabilities=new StrategySelection.Capabilities(bool(p,"capability.replicas"),bool(p,"capability.split.merge"),bool(p,"capability.native.hints"));
        var scheduler=new MigrationScheduler.Settings(integer(p,"scheduler.concurrent.jobs",4),integer(p,"scheduler.jobs.per.node",4),number(p,"scheduler.cluster.bytes.per.tick",16384),number(p,"scheduler.node.bytes.per.tick",4096),number(p,"scheduler.max.job.ticks",10000));
        var copy=new CopyDrivers.Settings(integer(p,"copy.batch.bytes",4096),number(p,"copy.dirty.threshold.bytes",1024),integer(p,"copy.max.live.rounds",10));
        return new B3Configuration(p.getProperty("scenario.flow","N"),StrategySelection.from(p),ScalingConfig.from(p),scheduler,copy,capabilities);
    }
    private static long number(Properties p,String key,long fallback){return Long.parseLong(p.getProperty(key,""+fallback));}
    private static int integer(Properties p,String key,int fallback){return Integer.parseInt(p.getProperty(key,""+fallback));}
    private static boolean bool(Properties p,String key){String value=p.getProperty(key,"false");if(!value.equals("true")&&!value.equals("false"))throw new IllegalArgumentException("boolean required: "+key);return Boolean.parseBoolean(value);}
}
