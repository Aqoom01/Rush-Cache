package io.openkache.core.migration.strategy;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Properties;

/** 기본 동작은 목록 출력뿐이다. 검증·계획 명령은 실험·서버 실행을 하지 않는다. */
public final class B3Main {
    private B3Main(){}
    public static void main(String[] args) throws IOException {
        if(args.length==0||args.length==1&&args[0].equals("--list")){System.out.println(B3Json.encode(StrategySelection.available()));return;}
        if(args.length==2&&args[0].equals("--plan")){RealNodePlan.write(Path.of(args[1]));System.out.println(B3Json.encode(RealNodePlan.summary()));return;}
        if(args.length==2&&args[0].equals("--validate")){
            var p=new Properties();try(var reader=Files.newBufferedReader(Path.of(args[1]),StandardCharsets.UTF_8)){p.load(reader);}var config=B3Configuration.from(p);
            var out=new LinkedHashMap<String,Object>();out.put("status","VALID_CONFIG_NOT_RUN");out.put("configuration",config);out.put("realNodeExperimentsStarted",0);System.out.println(B3Json.encode(out));return;
        }
        throw new IllegalArgumentException("B3Main --list | --validate <properties> | --plan <output>; 실험 실행은 별도 명시 명령만 허용");
    }
    private static boolean bool(Properties p,String key){String value=p.getProperty(key,"false");if(!value.equals("true")&&!value.equals("false"))throw new IllegalArgumentException("boolean required: "+key);return Boolean.parseBoolean(value);}
}
