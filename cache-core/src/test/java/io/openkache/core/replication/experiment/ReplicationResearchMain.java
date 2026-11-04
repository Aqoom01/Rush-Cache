package io.openkache.core.replication.experiment;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.regex.*;
import static io.openkache.core.replication.experiment.ReplicationExperiment.require;

/** 공통 research Runner 연결. 27개 JSON을 검증한 뒤 요약과 완료 표식을 생성한다. */
public final class ReplicationResearchMain {
    public static final String SCHEMA = "a1-replication-json-1";
    private ReplicationResearchMain() {}

    public static void main(String[] args) throws IOException {
        if (args.length == 2 && args[0].equals("--rebuild")) {
            regenerate(Path.of(args[1]));
            return;
        }
        if (args.length != 2) throw new IllegalArgumentException("<output.dir> <quick|full> or --rebuild <output.dir>");
        int requests = switch (args[1]) {
            case "quick" -> 10;
            case "full" -> 100;
            default -> throw new IllegalArgumentException("unknown profile");
        };
        Path folder = Path.of(args[0]);
        require(Files.isRegularFile(folder.resolve("launch.properties")), "run through common Runner");
        Path originals = Files.createDirectory(folder.resolve("results"));
        for (var condition : ReplicationExperiment.conditions()) {
            var measurement = ReplicationExperiment.run(condition, requests);
            Files.writeString(originals.resolve(condition.id() + ".json"), encode(measurement), StandardCharsets.UTF_8);
        }
        regenerate(folder);
        System.out.println("27조건 완료: " + folder);
    }

    static String encode(ReplicationExperiment.Measurement m) {
        return "{\n  \"resultSchemaVersion\": \"" + SCHEMA + "\",\n"
                + "  \"timeDomain\": \"virtual\",\n  \"timeUnit\": \"ms\",\n"
                + "  \"variant\": \"primary-backup-sync\",\n"
                + "  \"delayMs\": " + m.condition().delayMs() + ",\n"
                + "  \"extraAckMs\": " + m.condition().extraAckMs() + ",\n"
                + "  \"deadlineMs\": " + m.condition().deadlineMs() + ",\n"
                + "  \"requests\": " + m.requests() + ",\n"
                + "  \"ok\": " + m.ok() + ",\n"
                + "  \"timeouts\": " + m.timeouts() + ",\n"
                + "  \"responseMs\": " + m.responseMs() + ",\n"
                + "  \"successMs\": " + m.successMs() + ",\n"
                + "  \"verifiedCopies\": " + m.verifiedCopies() + ",\n"
                + "  \"timeoutPartialWrites\": " + m.timeoutPartialWrites() + ",\n"
                + "  \"messages\": " + m.messages() + "\n}\n";
    }

    /** 이 backend가 쓰는 고정 schema만 읽는다. 임의 JSON parser가 아니며 수정·중복·추가 필드를 거절한다. */
    static ReplicationExperiment.Measurement decode(String json) {
        Map<String, Long> numbers = new HashMap<>();
        Matcher matcher = Pattern.compile("\"([A-Za-z]+)\": (-?[0-9]+)").matcher(json);
        while (matcher.find()) require(numbers.put(matcher.group(1), Long.parseLong(matcher.group(2))) == null, "duplicate field");
        var c = new ReplicationExperiment.Condition(integer(numbers, "delayMs"), integer(numbers, "extraAckMs"), integer(numbers, "deadlineMs"));
        var m = new ReplicationExperiment.Measurement(c, integer(numbers, "requests"), integer(numbers, "ok"),
                integer(numbers, "timeouts"), number(numbers, "responseMs"), number(numbers, "successMs"),
                integer(numbers, "verifiedCopies"), integer(numbers, "timeoutPartialWrites"), integer(numbers, "messages"));
        require(json.replace("\r\n", "\n").equals(encode(m)), "noncanonical or incompatible JSON");
        return m;
    }

    private static long number(Map<String, Long> values, String name) {
        require(values.containsKey(name), "missing " + name);
        return values.get(name);
    }
    private static int integer(Map<String, Long> values, String name) { return Math.toIntExact(number(values, name)); }

    /** 원본 전체를 읽어 조건·개수·지표를 검증한다. 실패한 재생성에는 완료 표식을 남기지 않는다. */
    public static void regenerate(Path folder) throws IOException {
        Files.deleteIfExists(folder.resolve("completion.properties"));
        List<Path> files;
        try (var stream = Files.list(folder.resolve("results"))) {
            files = stream.filter(p -> p.toString().endsWith(".json")).sorted().toList();
        }
        require(files.size() == 27, "expected 27 JSON files");
        Map<ReplicationExperiment.Condition, ReplicationExperiment.Measurement> rows = new HashMap<>();
        int requests = -1;
        for (Path file : files) {
            var m = decode(Files.readString(file, StandardCharsets.UTF_8));
            require(file.getFileName().toString().equals(m.condition().id() + ".json"), "filename mismatch");
            require(ReplicationExperiment.conditions().contains(m.condition()), "unknown condition");
            require(rows.put(m.condition(), m) == null, "duplicate condition");
            require(m.requests() == 10 || m.requests() == 100, "invalid request count");
            require(requests == -1 || requests == m.requests(), "mixed profiles");
            requests = m.requests();
            // 독립적인 모델 예상값과 실제 엔진 측정 결과를 대조한다. deadline과 같아도 timeout이다.
            long expected = 2L * m.condition().delayMs() + m.condition().extraAckMs();
            boolean success = expected < m.condition().deadlineMs();
            require(m.ok() == (success ? requests : 0) && m.timeouts() == (success ? 0 : requests), "outcome mismatch");
            require(m.responseMs() == Math.min(expected, m.condition().deadlineMs()), "response mismatch");
            require(m.successMs() == (success ? expected : -1), "success latency mismatch");
            require(m.verifiedCopies() == m.ok() * 3 && m.messages() == requests * 4, "replication mismatch");
            require(m.timeoutPartialWrites() == m.timeouts(), "partial write mismatch");
        }
        StringBuilder csv = new StringBuilder("delay_ms,extra_ack_ms,deadline_ms,requests,ok,timeouts,response_ms,success_ms,verified_copies,timeout_partial_writes,messages\n");
        StringBuilder md = new StringBuilder("# A1 동기 SET 복제 실험 결과\n\n"
                + "고정 3노드 · 가상 시간(ms) · 조건별 " + requests + "요청 · 27조건.\n\n"
                + provenance(folder) + "\n\n"
                + "|단방향 지연|추가 ACK 지연|deadline|OK|TIMEOUT|응답 시간|성공 응답 시간|timeout 후 부분 적용|\n"
                + "|---:|---:|---:|---:|---:|---:|---:|---:|\n");
        for (var c : ReplicationExperiment.conditions()) {
            var m = rows.get(c);
            require(m != null, "missing condition");
            csv.append(c.delayMs()).append(',').append(c.extraAckMs()).append(',').append(c.deadlineMs()).append(',')
                    .append(m.requests()).append(',').append(m.ok()).append(',').append(m.timeouts()).append(',')
                    .append(m.responseMs()).append(',').append(m.successMs()).append(',').append(m.verifiedCopies()).append(',')
                    .append(m.timeoutPartialWrites()).append(',').append(m.messages()).append('\n');
            md.append('|').append(c.delayMs()).append('|').append(c.extraAckMs()).append('|').append(c.deadlineMs())
                    .append('|').append(m.ok()).append('|').append(m.timeouts()).append('|').append(m.responseMs())
                    .append('|').append(m.successMs() < 0 ? "—" : m.successMs()).append('|').append(m.timeoutPartialWrites()).append("|\n");
        }
        md.append("\n## 해석과 한계\n\n"
                + "- 전원 ACK 대기이므로 느린 Backup의 추가 ACK 지연이 응답 시간에 반영된다.\n"
                + "- 기대 완료 시간은 2 × 단방향 지연 + 추가 ACK 지연이다. deadline 이상이면 TIMEOUT이다.\n"
                + "- TIMEOUT은 롤백이 아니다. 늦은 메시지를 모두 전달한 후 일부 복제본의 값이 남는 경우를 별도 집계했다.\n"
                + "- 각 요청 후 timer·늦은 메시지를 모두 배출한다. 연속 부하·timeout 직후 재시도·처리량 실험이 아니다.\n"
                + "- 고유 키와 고정 지연, 즉시 로컬 처리 모델이다. 반복은 재현 확인이며 성능 분산 표본이 아니다.\n"
                + "- success_ms=-1은 성공 요청 없음이다. 실제 서버 성능·failover·분단 안전성은 측정하지 않았다.\n");
        Files.writeString(folder.resolve("summary.csv"), csv, StandardCharsets.UTF_8);
        Files.writeString(folder.resolve("results.md"), md, StandardCharsets.UTF_8);
        Files.writeString(folder.resolve("completion.properties"), "status=complete\nresultSchemaVersion=" + SCHEMA + "\ncases=27\n", StandardCharsets.UTF_8);
    }

    private static String provenance(Path folder) throws IOException {
        Properties launch = new Properties();
        try (var reader = Files.newBufferedReader(folder.resolve("launch.properties"), StandardCharsets.UTF_8)) {
            launch.load(reader);
        }
        return "실행 ID: `" + launch.getProperty("suiteExecutionId", "test") + "` · 반복: `" + folder.getFileName()
                + "` · 기준 commit: `" + launch.getProperty("gitCommit", "unknown")
                + "` · source SHA-256: `" + launch.getProperty("sourceHash", "unknown")
                + "` · backend SHA-256: `" + launch.getProperty("backendHash", "unknown")
                + "`. 미커밋 변경 포함 여부와 무관하게 실제 실행 소스는 source hash로 식별한다."
                + " 전체 실행 설정·명령은 launch.properties 및 상위 manifest.json에 보관한다.";
    }
}
