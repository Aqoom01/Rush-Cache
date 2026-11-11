package io.openkache.server.replication;

import io.openkache.core.model.NodeId;
import java.net.InetSocketAddress;
import java.nio.file.*;
import java.io.IOException;
import java.util.*;

/** PB 실험 전용 고정 3노드 설정. 기본 서버 설정과 core 계약은 변경하지 않는다. */
public record PbNodeConfig(int node,List<String> hosts,int clientPort,int peerPort,int adminPort,
                           int heartbeatMs,int timeoutMs,int tickMs,String protocol,long electionSeed,String vrMode,
                           String chainReadMode,int roseLimit,int retryMs,
                           String failureDetector,int failurePort,int failureProbeIntervalMs,
                           int failureDirectTimeoutMs,int failureIndirectTimeoutMs,int failureSuspicionMs,
                           int failureStartupGraceMs,int failureAwarenessMaxMultiplier,
                           int failureSuspicionMaxTimeoutMultiplier,int failureMaxConfirmations) {
    /** 기존 비교 실험은 고정 timeout을, PB_FENCED 기존 호출은 SWIM baseline을 유지한다. */
    public PbNodeConfig(int node,List<String> hosts,int clientPort,int peerPort,int adminPort,
            int heartbeatMs,int timeoutMs,int tickMs,String protocol,long electionSeed,String vrMode,
            String chainReadMode,int roseLimit,int retryMs) {
        this(node,hosts,clientPort,peerPort,adminPort,heartbeatMs,timeoutMs,tickMs,protocol,electionSeed,vrMode,
                chainReadMode,roseLimit,retryMs,protocol.equals("PB_FENCED")?"SWIM":"FIXED_TIMEOUT",7002,200,50,50,500,3000,8,2,1);
    }
    /** chain.read.mode(TAIL_ONLY|CRAQ)·rose.limit(-1/0/양수)·retry.ms(Chain/Rosé 재전송·제어 간격)는 해당 protocol에서만 쓴다. */
    public PbNodeConfig(int node,List<String> hosts,int clientPort,int peerPort,int adminPort,int heartbeatMs,int timeoutMs,int tickMs,String protocol,long electionSeed,String vrMode) {
        this(node,hosts,clientPort,peerPort,adminPort,heartbeatMs,timeoutMs,tickMs,protocol,electionSeed,vrMode,"TAIL_ONLY",0,1000);
    }
    /** vr.mode 생략은 INCREMENTAL이며 이전 실측 설정과 같다. VR 이외 protocol에서는 무시한다. */
    public PbNodeConfig(int node,List<String> hosts,int clientPort,int peerPort,int adminPort,int heartbeatMs,int timeoutMs,int tickMs,String protocol,long electionSeed) {
        this(node,hosts,clientPort,peerPort,adminPort,heartbeatMs,timeoutMs,tickMs,protocol,electionSeed,"INCREMENTAL");
    }
    public PbNodeConfig(int node,List<String> hosts,int clientPort,int peerPort,int adminPort,int heartbeatMs,int timeoutMs,int tickMs,String protocol) {
        this(node,hosts,clientPort,peerPort,adminPort,heartbeatMs,timeoutMs,tickMs,protocol,1);
    }
    public PbNodeConfig(int node,List<String> hosts,int clientPort,int peerPort,int adminPort,int heartbeatMs,int timeoutMs,int tickMs) {
        this(node,hosts,clientPort,peerPort,adminPort,heartbeatMs,timeoutMs,tickMs,"PB");
    }
    public PbNodeConfig {
        hosts=List.copyOf(hosts);
        if(!Set.of("PB","PB_FENCED","VR","RAFT","CHAIN","ROSE").contains(protocol) || !Set.of("INCREMENTAL","INCREMENTAL_CATCHUP").contains(vrMode)
                || !Set.of("TAIL_ONLY","CRAQ").contains(chainReadMode) || !Set.of("FIXED_TIMEOUT","SWIM","SWIM_LIFEGUARD").contains(failureDetector)
                || (!failureDetector.equals("FIXED_TIMEOUT") && !Set.of("PB","PB_FENCED","RAFT").contains(protocol))
                || (protocol.equals("PB_FENCED") && failureDetector.equals("FIXED_TIMEOUT"))
                || roseLimit< -1 || retryMs<1 || node<0 || node>2 || hosts.size()!=3 || hosts.stream().anyMatch(String::isBlank)
                || clientPort<1 || clientPort>65535 || peerPort<1 || peerPort>65535 || adminPort<0 || adminPort>65535
                || clientPort==peerPort || clientPort==adminPort || peerPort==adminPort
                || failurePort<1 || failurePort>65535
                || (!failureDetector.equals("FIXED_TIMEOUT") && (failurePort==clientPort || failurePort==peerPort || failurePort==adminPort))
                || heartbeatMs<1 || timeoutMs<=heartbeatMs || tickMs<1 || tickMs>heartbeatMs
                || failureDirectTimeoutMs<1 || failureIndirectTimeoutMs<1 || failureSuspicionMs<1 || failureStartupGraceMs<0
                || failureAwarenessMaxMultiplier<0 || failureSuspicionMaxTimeoutMultiplier<1 || failureMaxConfirmations<0 || failureMaxConfirmations>1
                || failureProbeIntervalMs<failureDirectTimeoutMs+failureIndirectTimeoutMs)
            throw new IllegalArgumentException("invalid PB node configuration");
    }
    /** UTF-8 properties를 읽고 알 수 없는 key를 거절한다. */
    public static PbNodeConfig load(Path path) throws IOException {
        Properties p=new Properties(); try(var reader=Files.newBufferedReader(path)) { p.load(reader); }
        String protocol=p.getProperty("protocol","PB");
        Set<String> keys=Set.of("chain.read.mode","rose.limit","retry.ms","vr.mode","election.seed","protocol","node.id","peer.0","peer.1","peer.2","client.port","peer.port","admin.port","heartbeat.ms","timeout.ms","tick.ms",
                "failure.detector","failure.port","failure.probe.interval.ms","failure.direct.timeout.ms","failure.indirect.timeout.ms","failure.suspicion.ms","failure.startup.grace.ms",
                "failure.lifeguard.awareness.max.multiplier","failure.lifeguard.suspicion.max.multiplier","failure.lifeguard.max.confirmations");
        for(String key:p.stringPropertyNames()) if(!keys.contains(key)) throw new IllegalArgumentException("unknown PB key: "+key);
        return new PbNodeConfig(Integer.parseInt(p.getProperty("node.id")),List.of(p.getProperty("peer.0"),p.getProperty("peer.1"),p.getProperty("peer.2")),
                number(p,"client.port",7000),number(p,"peer.port",7001),number(p,"admin.port",9091),
                number(p,"heartbeat.ms",50),number(p,"timeout.ms",500),number(p,"tick.ms",10),protocol,Long.parseLong(p.getProperty("election.seed","1")),p.getProperty("vr.mode","INCREMENTAL"),
                p.getProperty("chain.read.mode","TAIL_ONLY"),number(p,"rose.limit",0),number(p,"retry.ms",1000),
                p.getProperty("failure.detector",protocol.equals("PB_FENCED")?"SWIM":"FIXED_TIMEOUT"),number(p,"failure.port",7002),number(p,"failure.probe.interval.ms",200),
                number(p,"failure.direct.timeout.ms",50),number(p,"failure.indirect.timeout.ms",50),number(p,"failure.suspicion.ms",500),number(p,"failure.startup.grace.ms",3000),
                number(p,"failure.lifeguard.awareness.max.multiplier",8),number(p,"failure.lifeguard.suspicion.max.multiplier",2),number(p,"failure.lifeguard.max.confirmations",1));
    }
    private static int number(Properties p,String key,int fallback) { return Integer.parseInt(p.getProperty(key,Integer.toString(fallback))); }
    Map<NodeId,InetSocketAddress> addresses() {
        Map<NodeId,InetSocketAddress> map=new HashMap<>();
        for(int i=0;i<3;i++) map.put(new NodeId(i),new InetSocketAddress(hosts.get(i),peerPort));
        return map;
    }
    Map<NodeId,InetSocketAddress> failureAddresses() {
        Map<NodeId,InetSocketAddress> map=new HashMap<>();
        for(int i=0;i<3;i++) map.put(new NodeId(i),new InetSocketAddress(hosts.get(i),failurePort));
        return map;
    }
    boolean swimFailureDetector() { return !failureDetector.equals("FIXED_TIMEOUT"); }
    boolean lifeguardFailureDetector() { return failureDetector.equals("SWIM_LIFEGUARD"); }
}
