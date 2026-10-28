package io.openkache.core.replication.ionia_sixth;

import io.openkache.core.replication.ionia.IoniaConfig;
import java.nio.charset.StandardCharsets;
import java.security.*;
import java.util.*;

/** V04b 불변 실행 정책. 파일 shard S와 실제 전용 I/O worker W를 독립 설정한다. */
public record IoniaWorkerSettings(int storageShards,int ioWorkers) {
    public static final String SHARDS="storage.shards", WORKERS="storage.io.workers";
    public static final int MAILBOX_CAPACITY=65536;
    public IoniaWorkerSettings {
        if(storageShards<1 || storageShards>256 || ioWorkers<1 || ioWorkers>256)
            throw new IllegalArgumentException("V04b S/W must be in 1..256");
    }
    /** 시작 시 필수 두 정수 설정을 검증한다. 예전 storage.workers 키는 사용하지 않는다. */
    public static IoniaWorkerSettings parse(Properties properties){return new IoniaWorkerSettings(number(properties,SHARDS),number(properties,WORKERS));}
    private static int number(Properties properties,String key){
        String value=Objects.requireNonNull(properties).getProperty(key);
        if(value==null || !value.matches("[0-9]+"))throw new IllegalArgumentException("missing/invalid "+key);
        try{return Integer.parseInt(value);}catch(NumberFormatException error){throw new IllegalArgumentException("invalid "+key,error);}
    }
    /** 같은 파일의 모든 작업은 같은 worker로 향한다. 실행 중 담당자를 바꾸지 않는다. */
    public int workerForShard(int shard){if(shard<0 || shard>=storageShards)throw new IllegalArgumentException("invalid shard");return shard%ioWorkers;}
    /** 실행 정책과 고정 KV force 모드를 식별한다. 최적 설정을 의미하지 않는다. */
    public String profileId(){
        String policy="v04b|storageShards="+storageShards+"|ioWorkers="+ioWorkers+"|assignment=shardModuloWorkers|mailbox=65536|kvForce=false|recordFormat=ioniaKvV1";
        try{return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(policy.getBytes(StandardCharsets.UTF_8)));}
        catch(NoSuchAlgorithmException impossible){throw new AssertionError(impossible);}
    }
    /** 서버/client/stable View가 같은 실행 profile을 사용하도록 구성한다. */
    public String configurationId(IoniaConfig config){
        String id=Objects.requireNonNull(config).configurationId()+"|algorithm=V04b|profile="+profileId();
        long utf=0;for(int i=0;i<id.length();i++){int c=id.charAt(i);utf+=c>=1&&c<=127?1:c<=2047?2:3;}
        if(utf>65535)throw new IllegalArgumentException("configuration exceeds writeUTF limit");return id;
    }
}
