package io.openkache.server.benchmark.variants;

import static org.junit.jupiter.api.Assertions.*;
import java.nio.file.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.TimeUnit;

/** 새 TCP 그룹의 준비 단계만 기존 Windows 10% gate로 확인한다. Linux에서 검사 자체를 생략하지 않는다. */
final class VariantPortGate {
    private VariantPortGate() {}
    static void await(String group)throws Exception{
        if(!System.getProperty("os.name").toLowerCase(Locale.ROOT).startsWith("windows")){return;}
        Path parent=Path.of("build","remaining-variant-port-gates");Files.createDirectories(parent);
        Path evidence=parent.resolve(group+"-"+UUID.randomUUID()+".json").toAbsolutePath();
        Path log=evidence.resolveSibling(evidence.getFileName()+".log");
        Path script=Path.of("..","experiments","ionia-validation","Wait-LocalTcpBudget.ps1").toAbsolutePath();
        Process gate=new ProcessBuilder("pwsh.exe","-NoProfile","-NonInteractive","-ExecutionPolicy","Bypass","-File",script.toString(),
                "-EvidencePath",evidence.toString(),"-WaitSeconds","600").redirectErrorStream(true).redirectOutput(log.toFile()).start();
        System.out.println("REMAINING_TCP_GATE="+evidence);
        try{assertTrue(gate.waitFor(620,TimeUnit.SECONDS),"gate process wait: "+log);assertEquals(0,gate.exitValue(),new String(Files.readAllBytes(log),StandardCharsets.UTF_8));}
        finally{if(gate.isAlive()){gate.destroyForcibly();}}
    }
}
