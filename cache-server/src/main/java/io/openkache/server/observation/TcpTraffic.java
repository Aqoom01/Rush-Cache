package io.openkache.server.observation;

import java.io.*;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/** Opt-in process-local counters. No payload/IDs, callbacks, files or protocol decisions. */
public final class TcpTraffic {
    private TcpTraffic() {}
    private static final Map<String,Scope> SCOPES=new ConcurrentHashMap<>();
    private static final String PROCESS=UUID.randomUUID().toString();
    private static final Set<String> PEER_TYPES=Set.of("","Prepare","PrepareAck","Commit","Applied","StartViewChange","DoViewChange","StartView","RecoveryRequest","RecoveryResponse");
    public enum Stage { PREFLIGHT, LOAD, LOAD_DRAIN, WARMUP, WARMUP_DRAIN, MEASURE, FINAL_DRAIN, CLEANUP }
    public enum Metric { BEGUN, OK, FAILED, REJECTED, CREATED, CONNECT_ATTEMPTS, CONNECTED, CONNECT_FAILED, CLOSED,
        CLOSE_UNCONFIRMED, REQUESTS_WRITTEN, REPLIES_READ, SENT_BYTES, RECEIVED_BYTES, WRITE_UNCERTAIN, READ_ERRORS,
        WRITE_ERRORS, CROSS_STAGE, ELAPSED_NS }
    public record Key(boolean frame, long window, Stage startedStage, Stage terminalStage, long from, long to, int kind, String peerType) {}
    public record Row(Key key, Map<Metric,Long> counts) { public Row { counts=Map.copyOf(counts); } }
    public record Transition(Stage stage,long requestedNanos,long observedNanos) {}
    public record Snapshot(String token,String processInstanceId,long pid,String configuration,Set<Long> members,
            long startedNanos,long finishedNanos,boolean sealed,long pending,long pendingFrames,long activeConnections,
            long peakConnections,long peakExchanges,long lateFinishes,List<Row> rows,List<Transition> transitions,List<String> issues) {
        public Snapshot { members=Set.copyOf(members);rows=List.copyOf(rows);transitions=List.copyOf(transitions);issues=List.copyOf(issues); }
        public boolean valid(){return sealed && pending==0 && pendingFrames==0 && activeConnections==0 && lateFinishes==0 && issues.isEmpty();}
    }
    public static Scope open(String configuration,Set<Long> members,int maxRows) {
        Objects.requireNonNull(configuration);members=Set.copyOf(members);
        if(configuration.isBlank() || configuration.length()>4096 || members.size()<2 || members.size()>64
                || members.stream().anyMatch(n->n<=0) || maxRows<1 || maxRows>100_000)throw new IllegalArgumentException("traffic scope");
        Scope s=new Scope(configuration,members,maxRows);
        if(SCOPES.putIfAbsent(configuration,s)!=null)throw new IllegalStateException("configuration already observed in this process");
        return s;
    }
    public static Exchange exchange(String config,long from,long to,int kind,String peerType) {
        Scope s=SCOPES.get(config);return s==null?Exchange.NONE:s.begin(false,from,to,kind,peerType);
    }
    public static Exchange frame(String config,long from,long to,int kind) {
        Scope s=SCOPES.get(config);return s==null?Exchange.NONE:s.begin(true,from,to,kind,"");
    }
    public static final class Scope implements AutoCloseable {
        private final String config,token=UUID.randomUUID().toString();
        private final Set<Long> members;private final int maxRows;private final long start=System.nanoTime();
        private final Map<Key,long[]> rows=new HashMap<>();private final Set<String> issues=new TreeSet<>();
        private final List<Transition> transitions=new ArrayList<>();
        private Stage stage=Stage.PREFLIGHT;private boolean sealed;private long end,pending,frames,active,peak,peakRpc,late;
        private Scope(String c,Set<Long> m,int n){config=c;members=m;maxRows=n;}
        public synchronized void stage(Stage next,long at){
            if(sealed)throw new IllegalStateException("traffic scope sealed");
            if(at-start<0 || !transitions.isEmpty() && at-transitions.getLast().requestedNanos()<0)issues.add("STAGE_CLOCK");
            if(transitions.size()>=64){issues.add("STAGE_CAPACITY");return;}
            stage=Objects.requireNonNull(next);transitions.add(new Transition(stage,at,System.nanoTime()));
        }
        private synchronized Exchange begin(boolean frame,long from,long to,int kind,String peerType){
            if(sealed){issues.add("START_AFTER_SEAL");return Exchange.NONE;}
            if((from!=0&&!members.contains(from)) || (to!=0&&!members.contains(to)) || kind<1 || kind>7){issues.add("UNDECLARED_ENDPOINT_OR_KIND");from=-1;to=-1;kind=0;}
            if(peerType==null || !PEER_TYPES.contains(peerType)){issues.add("UNKNOWN_PEER_TYPE");peerType="";}
            if(frame)frames++;else{pending++;peakRpc=Math.max(peakRpc,pending);}
            return new Exchange(this,frame,from,to,kind,peerType,stage);
        }
        private synchronized void connected(){active++;peak=Math.max(peak,active);}
        private synchronized void finish(Exchange e){
            if(e.frame)frames--;else pending--;
            if(e.connected && e.socket!=null && e.socket.isClosed())active--;
            if(sealed){late++;issues.add("FINISH_AFTER_SEAL");notifyAll();return;}
            long now=System.nanoTime();if(now-e.started<0){issues.add("CLOCK_ORDER");notifyAll();return;}
            if(e.uncertain)issues.add("WRITE_BYTES_UNCERTAIN");
            boolean closed=e.socket!=null&&e.socket.isClosed();
            if(e.socket!=null&&!closed)issues.add("SOCKET_CLOSE_UNCONFIRMED");
            Key k=new Key(e.frame,(now-start)/1_000_000_000L,e.stage,stage,e.from,e.to,e.kind,e.peerType);
            long[] row=rows.get(k);
            if(row==null){if(rows.size()>=maxRows){issues.add("ROW_CAPACITY");notifyAll();return;}row=new long[Metric.values().length];rows.put(k,row);}
            long[] values={1,e.ok?1:0,!e.ok&&!e.rejected?1:0,e.rejected?1:0,e.socket!=null?1:0,e.connectAttempt?1:0,e.connected?1:0,
                e.connectAttempt&&!e.connected?1:0,closed?1:0,e.socket!=null&&!closed?1:0,e.written?1:0,e.reply?1:0,e.sent,e.received,
                e.uncertain?1:0,e.readErrors,e.writeErrors,e.stage!=stage?1:0,now-e.started};
            try{for(int i=0;i<values.length;i++)row[i]=Math.addExact(row[i],values[i]);}catch(ArithmeticException overflow){issues.add("COUNTER_OVERFLOW");}
            notifyAll();
        }
        public synchronized boolean awaitIdle(long timeoutNanos)throws InterruptedException{
            if(timeoutNanos<1||timeoutNanos>10_000_000_000L)throw new IllegalArgumentException("idle timeout");
            long until=System.nanoTime()+timeoutNanos;
            while(pending!=0||frames!=0){long left=until-System.nanoTime();if(left<=0)return false;TimeUnit.NANOSECONDS.timedWait(this,left);}return true;
        }
        @Override public synchronized void close(){
            if(sealed)return;sealed=true;end=System.nanoTime();if(pending!=0||frames!=0)issues.add("IN_FLIGHT_AT_SEAL");
            SCOPES.remove(config,this);
        }
        public synchronized Snapshot snapshot(){
            List<Row> copy=new ArrayList<>();rows.forEach((k,v)->{Map<Metric,Long> c=new EnumMap<>(Metric.class);for(var m:Metric.values())c.put(m,v[m.ordinal()]);copy.add(new Row(k,c));});
            copy.sort(Comparator.comparing((Row r)->r.key().frame()).thenComparing(r->r.key().window()).thenComparing(r->r.key().startedStage()).thenComparing(r->r.key().terminalStage())
                .thenComparing(r->r.key().from()).thenComparing(r->r.key().to()).thenComparing(r->r.key().kind()).thenComparing(r->r.key().peerType()));
            return new Snapshot(token,PROCESS,ProcessHandle.current().pid(),config,members,start,sealed?end:System.nanoTime(),sealed,pending,frames,active,peak,peakRpc,late,copy,transitions,List.copyOf(issues));
        }
    }
    public static final class Exchange implements AutoCloseable {
        private static final Exchange NONE=new Exchange();
        private final Scope owner;private final boolean frame;private final long from,to,started;private final int kind;private final String peerType;private final Stage stage;
        private final AtomicBoolean finished=new AtomicBoolean();
        private Socket socket;private boolean connectAttempt,connected,written,reply,ok,rejected,uncertain;private long sent,received,readErrors,writeErrors;
        private Exchange(){owner=null;frame=false;from=to=started=kind=0;peerType="";stage=Stage.PREFLIGHT;}
        private Exchange(Scope s,boolean f,long a,long b,int k,String p,Stage stage){owner=s;frame=f;from=a;to=b;kind=k;peerType=p;this.stage=stage;started=System.nanoTime();}
        public void created(Socket s){if(owner!=null)socket=s;}
        public void connecting(){if(owner!=null)connectAttempt=true;}
        public void connected(){if(owner!=null){connected=true;owner.connected();}}
        public void written(){if(owner!=null)written=true;}
        public void replyRead(){if(owner!=null)reply=true;}
        public void succeeded(){if(owner!=null)ok=true;}
        public void rejected(){if(owner!=null)rejected=true;close();}
        public InputStream input(InputStream source){
            if(owner==null)return source;
            return new FilterInputStream(source){
                @Override public int read()throws IOException{try{int n=in.read();if(n>=0)received++;return n;}catch(IOException|RuntimeException e){readErrors++;throw e;}}
                @Override public int read(byte[] b,int off,int len)throws IOException{try{int n=in.read(b,off,len);if(n>0)received+=n;return n;}catch(IOException|RuntimeException e){readErrors++;throw e;}}
            };
        }
        public OutputStream output(OutputStream destination){
            if(owner==null)return destination;
            return new FilterOutputStream(destination){
                @Override public void write(int b)throws IOException{try{out.write(b);sent++;}catch(IOException|RuntimeException e){uncertain=true;writeErrors++;throw e;}}
                @Override public void write(byte[] b,int off,int len)throws IOException{try{out.write(b,off,len);sent+=len;}catch(IOException|RuntimeException e){uncertain=true;writeErrors++;throw e;}}
                @Override public void flush()throws IOException{try{out.flush();}catch(IOException|RuntimeException e){uncertain=true;writeErrors++;throw e;}}
            };
        }
        @Override public void close(){if(owner!=null&&finished.compareAndSet(false,true)){owner.finish(this);socket=null;}}
    }
}
