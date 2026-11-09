package io.openkache.server.replication;

/** 고정 opcode 256개만 집계한다. 송신 완료 callback과 진단 thread가 같은 lock으로 snapshot을 읽는다. */
final class PeerTraffic {
    private final long[] frames=new long[256],bytes=new long[256];
    private long totalFrames,totalBytes;
    synchronized void record(int opcode,long size) {
        if(opcode<0 || opcode>255 || size<0) throw new IllegalArgumentException();
        frames[opcode]++;bytes[opcode]+=size;totalFrames++;totalBytes+=size;
    }
    synchronized String json() {
        StringBuilder out=new StringBuilder("{\"totalFrames\":").append(totalFrames)
                .append(",\"totalBytes\":").append(totalBytes).append(",\"opcodes\":{");
        boolean first=true;
        for(int i=0;i<256;i++) if(frames[i]!=0) {
            if(!first) out.append(',');first=false;
            out.append('"').append(i).append("\":{\"frames\":").append(frames[i])
                    .append(",\"bytes\":").append(bytes[i]).append('}');
        }
        return out.append("}}").toString();
    }
}
