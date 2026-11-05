package io.openkache.runner;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * window마다 바뀌는 라우팅 계획: 해시 구간(Tablet)별로 window마다 어느 서버({@code target.hosts} 번호)로 보낼지.
 *
 * <p>Runner는 계획을 재생만 한다. 계획을 누가 어떻게 만들었는지(배치 정책 등)는 모른다. 파일 형식(텍스트, 줄 단위):
 *
 * <pre>
 * # 주석
 * tablets 64
 * tablet &lt;시작 해시값, 부호 없는 10진수&gt; &lt;서버 번호&gt;    ← 시작점 오름차순, 첫 시작점은 0. tablets 수만큼
 * window 0
 * window 1
 * move &lt;tablet 번호&gt; &lt;서버 번호&gt;                       ← 이 window부터 바뀐 주인
 * ...
 * </pre>
 *
 * <p>불변이다. 여러 thread에서 읽을 수 있다.
 */
final class Schedule {

    private final long[] starts;
    /** {@code owners[w][i]}: window {@code w}에 Tablet {@code i}를 맡는 서버 번호. */
    private final int[][] owners;

    private Schedule(long[] starts, int[][] owners) {
        this.starts = starts;
        this.owners = owners;
    }

    static Schedule load(Path file) {
        try {
            return parse(Files.readAllLines(file, StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new ConfigException("failed to read schedule: " + file, e);
        }
    }

    static Schedule parse(List<String> lines) {
        long[] starts = null;
        int[] initial = null;
        int filled = 0;
        List<int[]> windows = new ArrayList<>();
        for (String raw : lines) {
            String line = raw.strip();
            if (line.isEmpty() || line.startsWith("#")) {
                continue;
            }
            String[] f = line.split("\\s+");
            try {
                switch (f[0]) {
                    case "tablets" -> {
                        int n = Integer.parseInt(f[1]);
                        starts = new long[n];
                        initial = new int[n];
                    }
                    case "tablet" -> {
                        require(starts != null && filled < starts.length, "unexpected tablet line: " + line);
                        starts[filled] = Long.parseUnsignedLong(f[1]);
                        initial[filled] = owner(f[2]);
                        require(filled == 0 ? starts[0] == 0 : Long.compareUnsigned(starts[filled - 1], starts[filled]) < 0,
                                "tablet starts must begin at 0 and increase: " + line);
                        filled++;
                    }
                    case "window" -> {
                        require(starts != null && filled == starts.length, "window before all tablets: " + line);
                        require(Integer.parseInt(f[1]) == windows.size(), "windows must be numbered 0, 1, ...: " + line);
                        windows.add(windows.isEmpty() ? initial.clone() : windows.getLast().clone());
                    }
                    case "move" -> {
                        require(!windows.isEmpty(), "move before window: " + line);
                        int tablet = Integer.parseInt(f[1]);
                        require(tablet >= 0 && tablet < starts.length, "unknown tablet: " + line);
                        windows.getLast()[tablet] = owner(f[2]);
                    }
                    default -> throw new ConfigException("unknown schedule line: " + line);
                }
            } catch (NumberFormatException | ArrayIndexOutOfBoundsException e) {
                throw new ConfigException("malformed schedule line: " + line, e);
            }
        }
        require(starts != null && filled == starts.length && !windows.isEmpty(), "schedule needs tablets and at least one window");
        return new Schedule(starts, windows.toArray(int[][]::new));
    }

    int windows() {
        return owners.length;
    }

    int tablets() {
        return starts.length;
    }

    /** 계획에 나오는 가장 큰 서버 번호. */
    int maxTarget() {
        return Arrays.stream(owners).flatMapToInt(Arrays::stream).max().orElse(0);
    }

    /** window {@code window}에 해시값 {@code keyHash}의 key를 보낼 서버 번호. */
    int target(int window, long keyHash) {
        return owners[window][tabletOf(keyHash)];
    }

    /** {@code keyHash}를 맡는 Tablet 번호(시작점이 keyHash 이하인 마지막 Tablet). */
    int tabletOf(long keyHash) {
        int lo = 0;
        int hi = starts.length - 1;
        while (lo < hi) {
            int mid = (lo + hi + 1) >>> 1;
            if (Long.compareUnsigned(starts[mid], keyHash) <= 0) {
                lo = mid;
            } else {
                hi = mid - 1;
            }
        }
        return lo;
    }

    /** window {@code window}에 주인이 바뀐 Tablet 수(window 0은 0). */
    int movesAt(int window) {
        if (window == 0) {
            return 0;
        }
        int moved = 0;
        for (int i = 0; i < starts.length; i++) {
            moved += owners[window][i] != owners[window - 1][i] ? 1 : 0;
        }
        return moved;
    }

    private static int owner(String text) {
        int owner = Integer.parseInt(text);
        require(owner >= 0, "owner must be >= 0: " + text);
        return owner;
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new ConfigException(message);
        }
    }
}
