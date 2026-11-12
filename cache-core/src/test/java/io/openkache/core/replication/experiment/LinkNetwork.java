package io.openkache.core.replication.experiment;

import io.openkache.core.spi.PeerEnvelope;
import java.util.function.BooleanSupplier;

/** 방향별 독립 FIFO 전송 큐. 직렬화 완료 + 고정 전파 지연 뒤 전달한다. 대역폭 0은 준비 구간의 무제한 전송이다. */
final class LinkNetwork {
    record Stats(long attemptedBytes,long enqueuedBytes,long deliveredBytes,long droppedBytes,long pendingBytes,
            long dataBytes,long controlBytes,long messages,long peakPendingBytes,long maxQueueWaitNs) {}
    final ReplicationExperiment.Scheduler scheduler;
    final long propagationNs;
    final long[][] available=new long[3][3];
    final long[][] pendingData=new long[3][3];
    long bytesPerSecond;
    long attemptedBytes,enqueuedBytes,deliveredBytes,droppedBytes,pendingBytes,dataBytes,controlBytes,messages,peakPendingBytes,maxQueueWaitNs;
    long pendingDataBytes;
    LinkNetwork(ReplicationExperiment.Scheduler scheduler,int delayMs,long bytesPerSecond) {
        if(delayMs<0 || bytesPerSecond<0) throw new IllegalArgumentException();
        this.scheduler=scheduler; propagationNs=ProtocolCluster.ms(delayMs); this.bytesPerSecond=bytesPerSecond;
    }
    static long serializationNs(long bytes,long rate) {
        if(bytes<0 || rate<=0) throw new IllegalArgumentException();
        long numerator=Math.multiplyExact(bytes,1_000_000_000L);
        return numerator/rate+(numerator%rate==0?0:1);
    }
    void send(PeerEnvelope envelope,BooleanSupplier connected,Runnable deliver) {
        send(envelope,connected,deliver,0);
    }
    void send(PeerEnvelope envelope,BooleanSupplier connected,Runnable deliver,int extraDelayMs) {
        if(extraDelayMs<0) throw new IllegalArgumentException("negative frame delay");
        var size=LogicalMessageSize.of(envelope.message()); long bytes=size.bytes();
        messages++; attemptedBytes+=bytes;
        if(!connected.getAsBoolean()) { droppedBytes+=bytes; return; }
        enqueuedBytes+=bytes; pendingBytes+=bytes; peakPendingBytes=Math.max(peakPendingBytes,pendingBytes);
        if(size.data()) { dataBytes+=bytes; pendingDataBytes+=bytes; } else controlBytes+=bytes;
        int from=Math.toIntExact(envelope.from().value()),to=Math.toIntExact(envelope.to().value());
        if(size.data()) pendingData[from][to]+=bytes;
        long now=scheduler.time.monotonicNanos(),start=Math.max(now,available[from][to]);
        maxQueueWaitNs=Math.max(maxQueueWaitNs,start-now);
        long end=Math.addExact(start,bytesPerSecond==0?0:serializationNs(bytes,bytesPerSecond));
        available[from][to]=end;
        scheduler.schedule(Math.addExact(Math.addExact(end,propagationNs),ProtocolCluster.ms(extraDelayMs)),()->{
            pendingBytes-=bytes;
            if(size.data()) pendingDataBytes-=bytes;
            if(size.data()) pendingData[from][to]-=bytes;
            if(connected.getAsBoolean()) { deliveredBytes+=bytes; deliver.run(); }
            else droppedBytes+=bytes;
        });
    }
    void beginMeasurement(long rate) {
        if(rate<=0 || pendingBytes!=0) throw new IllegalStateException("measurement must start with empty links");
        bytesPerSecond=rate;
        for(var row:available) java.util.Arrays.fill(row,scheduler.time.monotonicNanos());
        attemptedBytes=enqueuedBytes=deliveredBytes=droppedBytes=pendingBytes=dataBytes=controlBytes=messages=peakPendingBytes=maxQueueWaitNs=0;
        pendingDataBytes=0;
        for(var row:pendingData) java.util.Arrays.fill(row,0);
    }
    Stats stats() { return new Stats(attemptedBytes,enqueuedBytes,deliveredBytes,droppedBytes,pendingBytes,dataBytes,controlBytes,messages,peakPendingBytes,maxQueueWaitNs); }
    long livePendingData(java.util.Set<Integer> dead) {
        long bytes=0;
        for(int from=0;from<3;from++) for(int to=0;to<3;to++) if(!dead.contains(from) && !dead.contains(to)) bytes+=pendingData[from][to];
        return bytes;
    }
}
