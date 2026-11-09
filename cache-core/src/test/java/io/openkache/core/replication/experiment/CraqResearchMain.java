package io.openkache.core.replication.experiment;

import io.openkache.core.replication.chain.ChainEngine;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.regex.*;
import static io.openkache.core.replication.experiment.ReplicationExperiment.require;

/** 공통 Runner의 CRAQ 비교 backend. 원본을 엄격하게 검증한 뒤 CSV를 재생성한다. */
public final class CraqResearchMain {
    private CraqResearchMain() {}
    private static final String SCHEMA="a1-craq-json-1";
    private static final String HEADER="mode,scenario,valueBytes,mibps,reads,correct,node0,node1,node2,localReads,dirtyReads,queryAttempts,peerBytes,peerMessages,p50Ns,p99Ns";
    private static final List<String> NUMBERS=List.of(HEADER.substring(HEADER.indexOf("valueBytes")).split(","));
    static String encode(CraqComparison.Measurement m) {
        var c=m.condition();
        long[] values={c.valueBytes(),c.mibps(),m.reads(),m.correct(),m.node0(),m.node1(),m.node2(),m.localReads(),m.dirtyReads(),m.queryAttempts(),m.peerBytes(),m.peerMessages(),m.p50Ns(),m.p99Ns()};
        var b=new StringBuilder("{\n  \"resultSchemaVersion\": \""+SCHEMA+"\",\n  \"timeDomain\": \"virtual\",\n  \"timeUnit\": \"ns\",\n  \"mode\": \""+c.mode()+"\",\n  \"scenario\": \""+c.scenario()+"\"");
        for(int i=0;i<values.length;i++) b.append(",\n  \"").append(NUMBERS.get(i)).append("\": ").append(values[i]);
        return b.append("\n}\n").toString();
    }
    static CraqComparison.Measurement decode(String original) {
        String json=original.replace("\r\n","\n"); Map<String,Long> numbers=new LinkedHashMap<>();
        Matcher matcher=Pattern.compile("\"([A-Za-z0-9]+)\": ([0-9]+)").matcher(json);
        while(matcher.find()) require(numbers.put(matcher.group(1),Long.parseLong(matcher.group(2)))==null,"duplicate number");
        require(numbers.keySet().equals(new HashSet<>(NUMBERS)),"CRAQ fields differ");
        Matcher mode=Pattern.compile("\"mode\": \"([A-Z_]+)\"").matcher(json),scenario=Pattern.compile("\"scenario\": \"([A-Z_]+)\"").matcher(json);
        require(mode.find() && scenario.find(),"CRAQ condition missing");
        var c=new CraqComparison.Condition(ChainEngine.ReadMode.valueOf(mode.group(1)),CraqComparison.Scenario.valueOf(scenario.group(1)),
                Math.toIntExact(numbers.get("valueBytes")),Math.toIntExact(numbers.get("mibps")));
        var m=new CraqComparison.Measurement(c,numbers.get("reads"),numbers.get("correct"),numbers.get("node0"),numbers.get("node1"),numbers.get("node2"),
                numbers.get("localReads"),numbers.get("dirtyReads"),numbers.get("queryAttempts"),numbers.get("peerBytes"),numbers.get("peerMessages"),numbers.get("p50Ns"),numbers.get("p99Ns"));
        require(encode(m).equals(json),"CRAQ schema/syntax differs"); CraqComparison.validate(m); return m;
    }
    public static void main(String[] args) throws IOException {
        if(args.length==2 && args[0].equals("--rebuild")) { regenerate(Path.of(args[1])); return; }
        if(args.length!=2 || !List.of("quick","full").contains(args[1])) throw new IllegalArgumentException("<output.dir> <quick|full> or --rebuild <output.dir>");
        Path folder=Path.of(args[0]); require(Files.isRegularFile(folder.resolve("launch.properties")),"use common Runner");
        Files.createDirectory(folder.resolve("results"));
        for(var c:CraqComparison.conditions(args[1].equals("quick"))) Files.writeString(folder.resolve("results/"+c.id()+".json"),encode(CraqComparison.run(c)));
        regenerate(folder);
    }
    static void regenerate(Path folder) throws IOException {
        Files.deleteIfExists(folder.resolve("completion.properties"));
        List<Path> files; try(var stream=Files.list(folder.resolve("results"))) { files=stream.filter(p->p.toString().endsWith(".json")).sorted().toList(); }
        require(files.size()==8 || files.size()==32,"CRAQ condition count differs");
        Set<CraqComparison.Condition> expected=new HashSet<>(CraqComparison.conditions(files.size()==8)),seen=new HashSet<>();
        StringBuilder csv=new StringBuilder(HEADER+"\n"),md=new StringBuilder("# Chain / CRAQ 읽기 경로 비교\n\n가상 시간, 정상/미전달 쓰기/역방향 ACK 차단/tail crash 복구 후 읽기. 조건별 GET 90개를 동시에 제출한다.\n\n"
                +"|모드|시나리오|값 B|MiB/s|정확한 읽기|노드별 client 읽기|dirty 조회|peer B|p99 ms|\n|---|---|---:|---:|---:|---|---:|---:|---:|\n");
        for(Path file:files) {
            var m=decode(Files.readString(file)); var c=m.condition();
            require(expected.contains(c) && seen.add(c) && file.getFileName().toString().equals(c.id()+".json"),"CRAQ coverage differs");
            List<String> numbers=new ArrayList<>(); Matcher matcher=Pattern.compile("\"([A-Za-z0-9]+)\": ([0-9]+)").matcher(encode(m));
            while(matcher.find()) numbers.add(matcher.group(2));
            csv.append(c.mode()).append(',').append(c.scenario()).append(',').append(String.join(",",numbers)).append('\n');
            md.append('|').append(c.mode()).append('|').append(c.scenario()).append('|').append(c.valueBytes()).append('|').append(c.mibps())
                    .append('|').append(m.correct()).append('|').append(m.node0()).append('/').append(m.node1()).append('/').append(m.node2())
                    .append('|').append(m.dirtyReads()).append('|').append(m.peerBytes()).append('|').append(String.format(Locale.ROOT,"%.3f",m.p99Ns()/1e6)).append("|\n");
        }
        require(seen.equals(expected),"CRAQ coverage incomplete");
        md.append("\n- clean CRAQ는 각 생존 노드에서 로컬 응답한다. dirty CRAQ는 tail의 확정 key 버전만 조회하고 값은 로컬 보존 버전에서 반환한다.\n"
                +"- IN_FLIGHT는 middle→tail Forward 차단: tail의 이전 값 반환. ACK_DELAY는 역방향 Ack만 차단: tail의 새 확정 값 반환.\n"
                +"- TAIL_CRASH는 정확한 영구 crash oracle 200ms와 전 생존자 freeze/복구/open 후 읽는다. 재구성 중 가용성 측정은 이 표의 범위가 아니다.\n"
                +"- 방향별 1ms 전파와 명시적 논리 메시지 크기, 1/10MiB/s를 사용한다. 클라이언트 링크·값 응답 직렬화·CPU·GC·디스크는 모델링하지 않았다.\n"
                +"- 따라서 0ns는 로컬 CPU 비용을 두지 않은 모델값이며 처리량/실제 지연 우위를 뜻하지 않는다. peer 비용과 읽기 경로 분산만 비교한다.\n"
                +"- dirty 조건에서는 CRAQ의 tail client 읽기는 30개로 줄지만 버전 조회 60개가 추가된다. 총 tail 작업량 감소라고 해석하면 안 된다.\n"
                +"- 논문의 일부 핵심 경로를 구현한 연구 prototype이다. weak consistency 모드·pipelining·master 합의·영속화·실제 TCP는 범위 밖이다.\n");
        Files.writeString(folder.resolve("summary.csv"),csv); Files.writeString(folder.resolve("results.md"),md);
        Files.writeString(folder.resolve("completion.properties"),"status=complete\nresultSchemaVersion="+SCHEMA+"\ncases="+files.size()+"\n");
    }
}
