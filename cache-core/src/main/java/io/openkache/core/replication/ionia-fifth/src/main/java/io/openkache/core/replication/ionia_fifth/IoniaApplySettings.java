package io.openkache.core.replication.ionia_fifth;

import io.openkache.core.replication.ionia.IoniaConfig;
import java.nio.charset.StandardCharsets;
import java.security.*;
import java.util.*;

/** V04a 불변 정책. 동시 적용 key 허가 수와 고정 파일 shard 수는 서로 다른 제어다. */
public record IoniaApplySettings(int maxInFlightKeys, int storageShards) {
    public static final String LIMIT="apply.max.in.flight.keys", SHARDS="storage.workers";
    public IoniaApplySettings {
        if(maxInFlightKeys<1 || maxInFlightKeys>65536 || storageShards<1 || storageShards>256)
            throw new IllegalArgumentException("invalid V04a key concurrency/storage shards");
    }
    /** 시작 전에 두 필수 양수 정수 설정을 검증한다. 실행 중 변경하지 않는다. */
    public static IoniaApplySettings parse(Properties properties) {
        return new IoniaApplySettings(number(properties,LIMIT),number(properties,SHARDS));
    }
    private static int number(Properties p,String key) {
        String value=Objects.requireNonNull(p).getProperty(key);
        if(value==null || !value.matches("[0-9]+"))throw new IllegalArgumentException("missing/invalid "+key);
        try{return Integer.parseInt(value);}catch(NumberFormatException error){throw new IllegalArgumentException("invalid "+key,error);}
    }
    /** key 허가·고정 파일 배치·FIFO 정책을 식별하는 SHA-256. 모든 thread에서 사용 가능. */
    public String profileId() {
        String policy="v04a|maxInFlightKeys="+maxInFlightKeys+"|storageShards="+storageShards+"|ready=fifoKeys|sameKey=fifo|release=physicalCompletion";
        try{return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(policy.getBytes(StandardCharsets.UTF_8)));}
        catch(NoSuchAlgorithmException impossible){throw new AssertionError(impossible);}
    }
    /** 서버·client·stable View의 profile을 일치시킨다. 파일 배치 변경도 기존 디렉터리 재사용을 거절한다. */
    public String configurationId(IoniaConfig membership) {
        String id=Objects.requireNonNull(membership).configurationId()+"|algorithm=V04a|profile="+profileId();
        long utf=0;for(int i=0;i<id.length();i++){int c=id.charAt(i);utf+=c>=1&&c<=127?1:c<=2047?2:3;}
        if(utf>65535)throw new IllegalArgumentException("configuration exceeds writeUTF limit");
        return id;
    }
}
