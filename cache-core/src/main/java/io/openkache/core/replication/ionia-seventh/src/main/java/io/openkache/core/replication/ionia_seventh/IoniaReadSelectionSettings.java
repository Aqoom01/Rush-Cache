package io.openkache.core.replication.ionia_seventh;

import java.nio.charset.StandardCharsets;
import java.security.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

/** V05 불변 정책. ms 설정은 로컬 monotonic ns로 변환하며 서버 wire 설정과 독립이다. */
public record IoniaReadSelectionSettings(Mode mode,int logicalLimit,int rpcLimitPerReplica,
        int initialRttMillis,int sampleTtlMillis,int cooldownMillis,int exploreEvery) {
    public enum Mode { ADAPTIVE, ROUND_ROBIN }
    public static final long SAMPLE_MAX_NANOS=TimeUnit.SECONDS.toNanos(30),WRITE_FALLBACK_NANOS=50_000_000;
    public static final Set<String> KEYS=Set.of("read.selection","read.logical.limit","read.rpc.limit.per.replica",
            "read.rtt.initial.ms","read.sample.ttl.ms","read.failure.cooldown.ms","read.explore.every");
    public IoniaReadSelectionSettings {
        Objects.requireNonNull(mode);bound(logicalLimit,65536);bound(rpcLimitPerReplica,65536);bound(exploreEvery,65536);
        bound(initialRttMillis,60000);bound(sampleTtlMillis,60000);bound(cooldownMillis,60000);
    }
    private static void bound(int n,int max){if(n<1||n>max)throw new IllegalArgumentException("V05 setting outside 1.."+max);}
    /** 필수 read.* 설정. membership 키는 조립부가 검사하며 알 수 없는 read 키는 거절한다. */
    public static IoniaReadSelectionSettings parse(Properties p){
        Objects.requireNonNull(p);
        for(String key:p.stringPropertyNames())if(key.startsWith("read.")&&!KEYS.contains(key))throw new IllegalArgumentException("unknown setting: "+key);
        Mode mode=switch(p.getProperty("read.selection","")){case "adaptive"->Mode.ADAPTIVE;case "round-robin"->Mode.ROUND_ROBIN;default->throw new IllegalArgumentException("missing/invalid read.selection");};
        return new IoniaReadSelectionSettings(mode,number(p,"read.logical.limit"),number(p,"read.rpc.limit.per.replica"),
                number(p,"read.rtt.initial.ms"),number(p,"read.sample.ttl.ms"),number(p,"read.failure.cooldown.ms"),number(p,"read.explore.every"));
    }
    private static int number(Properties p,String key){
        String s=p.getProperty(key);if(s==null||!s.matches("[0-9]+"))throw new IllegalArgumentException("missing/invalid "+key);
        try{return Integer.parseInt(s);}catch(NumberFormatException e){throw new IllegalArgumentException("invalid "+key,e);}
    }
    public long initialRttNanos(){return TimeUnit.MILLISECONDS.toNanos(initialRttMillis);}
    public long sampleTtlNanos(){return TimeUnit.MILLISECONDS.toNanos(sampleTtlMillis);}
    public long cooldownNanos(){return TimeUnit.MILLISECONDS.toNanos(cooldownMillis);}
    /** 고정 순서 UTF-8 hash. client 정책 metadata이며 서버 configuration ID가 아니다. */
    public String profileId(){
        String value="v05a|mode="+mode+"|logical="+logicalLimit+"|rpc="+rpcLimitPerReplica+"|r0="+initialRttNanos()
                +"|ttl="+sampleTtlNanos()+"|cooldown="+cooldownNanos()+"|explore="+exploreEvery
                +"|ewma=1/8|max="+SAMPLE_MAX_NANOS+"|tie=clientSeedCircular|writeFallback="+WRITE_FALLBACK_NANOS;
        try{return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8)));}
        catch(NoSuchAlgorithmException e){throw new AssertionError(e);}
    }
}
