package io.openkache.core.replication.experiment;

import io.openkache.core.replication.chain.ChainEngine;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.regex.*;
import static io.openkache.core.replication.experiment.ReplicationExperiment.require;

/** 공통 Runner의 CRAQ 혼합 부하 backend. 기존 읽기 경로 실험과 별도 schema/run을 사용한다. */
public final class CraqMixedResearchMain {
    private CraqMixedResearchMain() {}
    private static final String SCHEMA="a1-craq-mixed-json-1";
    private static final List<String> CONDITIONS=List.of("writePercent","keys","valueBytes","ackDelayMs","seed");
    private static final List<String> NUMBERS;
    static { List<String> names=new ArrayList<>(CONDITIONS); names.addAll(CraqMixedExperiment.METRICS); NUMBERS=List.copyOf(names); }
    static String encode(CraqMixedExperiment.Row row) {
        var c=row.condition(); Map<String,Long> numbers=new LinkedHashMap<>();
        numbers.put("writePercent",(long)c.writePercent()); numbers.put("keys",(long)c.keys()); numbers.put("valueBytes",(long)c.valueBytes());
        numbers.put("ackDelayMs",(long)c.ackDelayMs()); numbers.put("seed",(long)c.seed());
        numbers.putAll(row.metrics());
        var text=new StringBuilder("{\n  \"resultSchemaVersion\": \""+SCHEMA+"\",\n  \"timeDomain\": \"virtual\",\n  \"timeUnit\": \"ns\",\n  \"mode\": \""+c.mode()+"\"");
        for(String key:NUMBERS) text.append(",\n  \"").append(key).append("\": ").append(numbers.get(key));
        return text.append("\n}\n").toString();
    }
    static CraqMixedExperiment.Row decode(String original) {
        String json=original.replace("\r\n","\n"); Map<String,Long> numbers=new LinkedHashMap<>();
        Matcher fields=Pattern.compile("\"([A-Za-z0-9]+)\": ([0-9]+)").matcher(json);
        while(fields.find()) require(numbers.put(fields.group(1),Long.parseLong(fields.group(2)))==null,"duplicate mixed field");
        require(numbers.keySet().equals(new HashSet<>(NUMBERS)),"mixed fields differ");
        Matcher mode=Pattern.compile("\"mode\": \"([A-Z_]+)\"").matcher(json); require(mode.find(),"mixed mode missing");
        var c=new CraqMixedExperiment.Condition(ChainEngine.ReadMode.valueOf(mode.group(1)),Math.toIntExact(numbers.get("writePercent")),
                Math.toIntExact(numbers.get("keys")),Math.toIntExact(numbers.get("valueBytes")),Math.toIntExact(numbers.get("ackDelayMs")),Math.toIntExact(numbers.get("seed")));
        Map<String,Long> metrics=new LinkedHashMap<>(); for(String key:CraqMixedExperiment.METRICS) metrics.put(key,numbers.get(key));
        var row=new CraqMixedExperiment.Row(c,metrics);
        require(encode(row).equals(json),"mixed schema/syntax differs"); CraqMixedExperiment.validate(row); return row;
    }
    public static void main(String[] args) throws IOException {
        if(args.length==2 && args[0].equals("--rebuild")) { regenerate(Path.of(args[1])); return; }
        if(args.length!=2 || !List.of("quick","full").contains(args[1])) throw new IllegalArgumentException("<output.dir> <quick|full> or --rebuild <output.dir>");
        Path folder=Path.of(args[0]); require(Files.isRegularFile(folder.resolve("launch.properties")),"use common Runner");
        Files.createDirectory(folder.resolve("results"));
        for(var c:CraqMixedExperiment.conditions(args[1].equals("quick"))) Files.writeString(folder.resolve("results/"+c.id()+".json"),encode(CraqMixedExperiment.run(c)));
        regenerate(folder);
    }
    static void regenerate(Path folder) throws IOException {
        Files.deleteIfExists(folder.resolve("completion.properties"));
        List<Path> files; try(var paths=Files.list(folder.resolve("results"))) { files=paths.filter(p->p.toString().endsWith(".json")).sorted().toList(); }
        require(files.size()==24 || files.size()==288,"mixed condition count differs");
        Set<CraqMixedExperiment.Condition> expected=new HashSet<>(CraqMixedExperiment.conditions(files.size()==24)),seen=new HashSet<>();
        StringBuilder csv=new StringBuilder("mode,"+String.join(",",NUMBERS)+"\n"); long reads=0,writes=0,busy=0,invalid=0;
        for(Path file:files) {
            var row=decode(Files.readString(file)); var c=row.condition();
            require(expected.contains(c) && seen.add(c) && file.getFileName().toString().equals(c.id()+".json"),"mixed coverage differs");
            csv.append(c.mode()); Matcher fields=Pattern.compile("\"([A-Za-z0-9]+)\": ([0-9]+)").matcher(encode(row));
            while(fields.find()) csv.append(',').append(fields.group(2)); csv.append('\n');
            reads+=row.get("readOk"); writes+=row.get("writeOk"); busy+=row.get("writeBusy"); invalid+=row.get("invalidReads");
        }
        require(seen.equals(expected),"mixed coverage incomplete");
        Files.writeString(folder.resolve("summary.csv"),csv);
        Files.writeString(folder.resolve("results.md"),"# CRAQ 혼합 부하 결과\n\n조건 "+files.size()+"개, 읽기 성공 "+reads+", 쓰기 성공 "+writes+", 쓰기 BUSY "+busy+", 읽기 구간 검증 위반 "+invalid+".\n\n"
                +"- 고정 3노드, 1MiB/s, 전파 1ms. 정상 상태 입력 3초, 2ms마다 1건(1500건), 요청 deadline 1000ms, 입력 후 최대 10초 drain. 재시도 없음.\n"
                +"- 쓰기 비율 0/1/5/10/25/50%, key 수 1/64, 값 64/8192B, tail Ack/Completed 추가 지연 0/20ms, seed 1/2/3. quick은 0/10/50%,8192B,seed1.\n"
                +"- 같은 seed/설정은 두 모드에 동일한 연산·key·값·도착 시각을 준다. CRAQ는 읽기를 순환 배치, Chain은 tail 배치.\n"
                +"- 쓰기는 기존 단일 in-flight 엔진이다. 설정한 쓰기 비율은 제출 비율이며 실제 성공 비율과 다르다. 성공/BUSY/TIMEOUT/기타를 함께 보고한다.\n"
                +"- key별 실제 tail KV 적용 이벤트와 GET 제출/응답의 전체 순서를 대조한다. 각 GET 값이 요청 시작 당시 또는 요청 중 확정된 값인지 확인한다. 다중 key transaction 검증은 아니다.\n"
                +"- warmup 제외, drain 포함 peer 비용. CPU·client 링크·값 응답 비용·GC·디스크 없음. 모델 p99/로컬 0ns를 실제 지연·처리량으로 주장하지 않는다.\n"
                +"- 운영 지연 예산·쓰기 보존 요구와 실 서버 검증은 후속이며 이 탐색 실험으로 멘토의 결정을 대신하지 않는다.\n");
        Files.writeString(folder.resolve("completion.properties"),"status=complete\nresultSchemaVersion="+SCHEMA+"\ncases="+files.size()+"\n");
    }
}
