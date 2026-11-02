package io.openkache.core.migration.experiment;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/** 선행 알고리즘 연구 backend의 Python 프로세스를 공통 Runner에 연결한다. */
public final class LiteratureResearchMain {
    private LiteratureResearchMain() {}

    public static void main(String[] args) throws IOException, InterruptedException {
        if (args.length != 4) throw new IllegalArgumentException("<결과 폴더> <full|quick> <반복> <전체 반복>");
        Path script = Path.of("experiments/b3-live-migration/scaling-decision/backend/experiment.py").toAbsolutePath();
        if (!Files.isRegularFile(script)) throw new IllegalStateException("연구 저장소 루트에서 실행해야 한다");
        ProcessBuilder builder = new ProcessBuilder("python", "-B", script.toString(), "run", args[0], args[1], args[2], args[3]);
        builder.environment().put("PYTHONDONTWRITEBYTECODE", "1");
        builder.environment().put("PYTHONIOENCODING", "utf-8");
        builder.environment().put("OPENBLAS_NUM_THREADS", "1");
        builder.environment().put("OMP_NUM_THREADS", "1");
        builder.inheritIO();
        int status = builder.start().waitFor();
        if (status != 0) throw new IllegalStateException("선행 알고리즘 backend 실패: exit=" + status);
    }
}
