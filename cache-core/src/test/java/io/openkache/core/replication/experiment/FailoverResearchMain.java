package io.openkache.core.replication.experiment;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.regex.*;
import static io.openkache.core.replication.experiment.ReplicationExperiment.require;

/** 공통 Runner용 27조건 crash 실험. 원본을 다시 읽어 검증한 뒤 요약을 만든다. */
public final class FailoverResearchMain {
    private static final String SCHEMA = "a1-crash-json-1";
    private static final String[] FIELDS = {"delay", "timeout", "retry", "detection", "transition", "clientAndWrite", "total", "preserved", "redirects"};
    private FailoverResearchMain() {}
    public static void main(String[] args) throws IOException {
        if (args.length == 2 && args[0].equals("--rebuild")) { regenerate(Path.of(args[1])); return; }
        if (args.length != 1) throw new IllegalArgumentException("<output.dir> or --rebuild <output.dir>");
        Path folder = Path.of(args[0]);
        require(Files.isRegularFile(folder.resolve("launch.properties")), "run through common Runner");
        Files.createDirectory(folder.resolve("results"));
        for (int d : new int[] {1, 5, 10}) for (int t : new int[] {50, 100, 200}) for (int r : new int[] {1, 10, 25}) {
            var m = FailoverExperiment.run(d, t, r);
            Files.writeString(folder.resolve("results/" + id(m) + ".json"), encode(m));
        }
        regenerate(folder);
    }
    private static String id(FailoverExperiment.Measurement m) { return "d"+m.delay()+"-t"+m.timeout()+"-r"+m.retry(); }
    static String encode(FailoverExperiment.Measurement m) {
        long[] values = {m.delay(), m.timeout(), m.retry(), m.detection(), m.transition(), m.clientAndWrite(), m.total(), m.preserved(), m.redirects()};
        StringBuilder b = new StringBuilder("{\n  \"resultSchemaVersion\": \""+SCHEMA+"\",\n  \"timeDomain\": \"virtual\",\n  \"timeUnit\": \"ms\"");
        for (int i=0; i<FIELDS.length; i++) b.append(",\n  \"").append(FIELDS[i]).append("\": ").append(values[i]);
        return b.append("\n}\n").toString();
    }
    static FailoverExperiment.Measurement decode(String json) {
        Map<String, Long> fields = new HashMap<>();
        Matcher matcher = Pattern.compile("\"([A-Za-z]+)\": ([0-9]+)").matcher(json);
        while (matcher.find()) require(fields.put(matcher.group(1), Long.parseLong(matcher.group(2))) == null, "duplicate field");
        require(fields.keySet().equals(Set.of(FIELDS)), "missing or unknown field");
        var m = new FailoverExperiment.Measurement(Math.toIntExact(fields.get("delay")), Math.toIntExact(fields.get("timeout")),
                Math.toIntExact(fields.get("retry")), fields.get("detection"), fields.get("transition"), fields.get("clientAndWrite"),
                fields.get("total"), Math.toIntExact(fields.get("preserved")), Math.toIntExact(fields.get("redirects")));
        require(encode(m).equals(json.replace("\r\n", "\n")), "incompatible JSON");
        return m;
    }
    public static void regenerate(Path folder) throws IOException {
        Files.deleteIfExists(folder.resolve("completion.properties"));
        List<Path> files;
        try (var stream = Files.list(folder.resolve("results"))) { files = stream.filter(p -> p.toString().endsWith(".json")).sorted().toList(); }
        require(files.size() == 27, "expected 27 cases");
        Set<String> ids = new HashSet<>();
        StringBuilder csv = new StringBuilder(String.join(",", FIELDS)+"\n");
        StringBuilder md = new StringBuilder("# A1 단일 Primary crash 실험\n\n가상 시간(ms), heartbeat 10ms, timer tick 1ms. 각 조건은 독립 3노드로 시작한다.\n\n");
        Properties launch = new Properties();
        try (var reader = Files.newBufferedReader(folder.resolve("launch.properties"))) { launch.load(reader); }
        for (String key : List.of("suiteExecutionId", "gitCommit", "sourceHash", "backendHash")) md.append("- ").append(key).append(": `").append(launch.getProperty(key, "unknown")).append("`\n");
        md.append("\n|편도 지연|감지 timeout|재시도 간격|감지|전환|클라이언트 대기+복제|전체 failover|보존된 기존 쓰기|Redirect|\n|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n");
        for (Path file : files) {
            var m = decode(Files.readString(file));
            require(List.of(1,5,10).contains(m.delay()) && List.of(50,100,200).contains(m.timeout()) && List.of(1,10,25).contains(m.retry()), "unknown condition");
            require(ids.add(id(m)) && file.getFileName().toString().equals(id(m)+".json"), "duplicate/misnamed condition");
            require(m.preserved()==10 && m.redirects()>0 && m.transition()==2L*m.delay(), "invalid preservation/readiness");
            require(m.detection()>=0 && m.detection()<=m.timeout() && m.clientAndWrite()>=2L*m.delay()
                    && m.total()==m.detection()+m.transition()+m.clientAndWrite(), "invalid phase durations");
            String row = m.delay()+","+m.timeout()+","+m.retry()+","+m.detection()+","+m.transition()+","+m.clientAndWrite()+","+m.total()+","+m.preserved()+","+m.redirects();
            csv.append(row).append('\n'); md.append('|').append(row.replace(',', '|')).append("|\n");
        }
        md.append("\n27条件すべてではなく、27조건 모두 기존 OK 쓰기 10개와 복구 후 첫 쓰기를 생존 노드 둘에서 검증했다.\n"
                + "\n분단·재시작·두 번째 장애·실제 TCP/SSD 성능은 검증하지 않았다. 클라이언트 접속 실패는 즉시 통지하며 재시도 간격만 모델링한다.\n"
                + "고유 키로 순차 쓰기 완료 후 crash하며, 미완료 쓰기·덮어쓰기에 대한 선형화 가능성 검증은 아니다. 반복은 결정성 점검이다.\n");
        Files.writeString(folder.resolve("summary.csv"), csv);
        Files.writeString(folder.resolve("results.md"), md);
        Files.writeString(folder.resolve("completion.properties"), "status=complete\nresultSchemaVersion="+SCHEMA+"\ncases=27\n");
    }
}

