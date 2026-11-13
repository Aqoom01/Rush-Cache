package io.openkache.core.migration.strategy;

import io.openkache.core.migration.MigrationJob;
import io.openkache.core.migration.MigrationPublication;
import io.openkache.core.migration.MigrationScheduler;
import io.openkache.core.migration.postcopy.PostCopyProtocol;
import io.openkache.core.migration.precopy.PreCopyProtocol;
import io.openkache.core.migration.stopcopy.StopAndCopyProtocol;
import io.openkache.core.tablet.Change;
import io.openkache.core.tablet.RoutingTable;
import io.openkache.core.api.Command;
import io.openkache.core.api.Result;
import io.openkache.core.model.Bytes;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** 세 복사 프로토콜을 동일 Scheduler.Driver로 선택한다. 실제 저장·전송은 자체 Binding으로 주입한다. */
public final class CopyDrivers {
    public static final List<String> METHODS=List.of("stop-and-copy","pre-copy","post-copy");
    /** 모든 호출은 같은 controller thread. publish와 protocol 통지는 한 호출에서 순서대로 수행한다. */
    public interface Binding {
        RoutingTable current();
        /** B1과 같은 key 변환. 기존 fixture는 기본 해시를 유지한다. */
        default long keyHash(Bytes key) { return io.openkache.core.tablet.KeyHash.of(key); }
        /** 소유권 게시 전 적용 barrier. 멱등이어야 하며 실패하면 CAS를 수행하지 않는다. */
        default void beforePublish(Change change,String copy,RoutingTable before,RoutingTable after) { }
        boolean compareAndSet(RoutingTable before,RoutingTable after);
        PreCopyProtocol.Source source();
        StopAndCopyProtocol.Destination stopDestination();
        PreCopyProtocol.Destination preDestination();
        PostCopyProtocol.Source postSource();
        /** publish 뒤 설치/내구화 barrier. 실패 시 재호출되므로 멱등해야 하며 성공 전 source cleanup은 진행하지 않는다. */
        void installed(Change change,String copy,RoutingTable before,RoutingTable after);
    }
    public record Settings(int batchBytes,long dirtyThreshold,int maxLiveRounds){public Settings{if(batchBytes<1||dirtyThreshold<1||maxLiveRounds<1)throw new IllegalArgumentException("invalid copy settings");}}
    /** Post-copy 요청·데이터 및 Pre-copy 관측은 해당 방식에서만 사용한다. */
    public interface Session extends MigrationScheduler.Driver {
        boolean published();
        boolean acceptsWrite();
        Result postRequest(Command command);
        Map<Bytes,Bytes> postData();
        List<PreCopyProtocol.RoundEvent> dirtyRounds();
        /** 연구 관측용. 선택하지 않은 방식의 accessor는 null이다. 상태 변경은 같은 controller에서만 한다. */
        StopAndCopyProtocol stopProtocol();
        PreCopyProtocol preProtocol();
        PostCopyProtocol postProtocol();
    }
    private CopyDrivers(){}
    public static Session create(String name,Change change,Binding binding,Settings settings){
        if(!METHODS.contains(name))throw new IllegalArgumentException("unknown B3 copy: "+name);return new Driver(name,change,binding,settings);
    }
    private static final class Driver implements Session {
        final String name;final Change change;final Binding binding;final Settings settings;
        RoutingTable expected,publishedBefore,publishedAfter;MigrationJob job;StopAndCopyProtocol stop;PreCopyProtocol pre;PostCopyProtocol post;boolean published,installed;
        Driver(String name,Change change,Binding binding,Settings settings){this.name=name;this.change=Objects.requireNonNull(change);this.binding=Objects.requireNonNull(binding);this.settings=Objects.requireNonNull(settings);}
        @Override public void begin(){if(job!=null)throw new IllegalStateException("already begun");expected=binding.current();job=MigrationJob.pending(change,name);
            switch(name){case "stop-and-copy"->{stop=new StopAndCopyProtocol(binding.source(),binding.stopDestination(),expected,settings.batchBytes());stop.begin(job);}case "pre-copy"->{pre=new PreCopyProtocol(binding.source(),binding.preDestination(),expected,new PreCopyProtocol.Settings(settings.batchBytes(),settings.dirtyThreshold(),settings.maxLiveRounds()),binding::keyHash);pre.begin(job);}case "post-copy"->{post=new PostCopyProtocol(binding.postSource(),expected,binding::keyHash);post.begin(job);}default->throw new IllegalStateException(name);}}
        private boolean ready(){return stop!=null?stop.readyToCommit():pre!=null?pre.readyToCommit():post!=null&&post.readyToCommit();}
        @Override public void step(long budget){if(job==null)throw new IllegalStateException("begin first");if(published&&!installed){install();return;}if(ready()){
            var before=binding.current();var after=before.apply(change);MigrationPublication.verify(job,expected,before,after);binding.beforePublish(change,name,before,after);if(!binding.compareAndSet(before,after))return;
            if(stop!=null)stop.committed(before,after);else if(pre!=null)pre.committed(before,after);else post.committed(before,after);published=true;publishedBefore=before;publishedAfter=after;install();
        }else if(stop!=null)stop.step(budget);else if(pre!=null)pre.step(budget);else post.step(budget);}
        @Override public long sentBytes(){return stop!=null?stop.job().progress().sentBytes():pre!=null?pre.job().progress().sentBytes():post!=null?post.sentBytes():0;}
        @Override public String state(){return stop!=null?stop.state().name():pre!=null?pre.state().name():post!=null?post.state().name():"PENDING";}
        private void install(){binding.installed(change,name,publishedBefore,publishedAfter);installed=true;}
        @Override public boolean done(){return installed&&state().equals("DONE");}
        @Override public boolean failed(){return state().equals("ABORTED")||state().equals("FAILED");}
        @Override public void stop(){if(stop!=null)stop.abort();else if(pre!=null)pre.abort();else if(post!=null)post.fail();}
        @Override public boolean published(){return published;}
        @Override public boolean acceptsWrite(){return stop!=null?stop.acceptsWrite():pre!=null?pre.acceptsWrite():post==null||post.acceptsWrite();}
        @Override public Result postRequest(Command command){if(post==null)throw new IllegalStateException("post-copy session required");return post.execute(command);}
        @Override public Map<Bytes,Bytes> postData(){if(post==null)throw new IllegalStateException("post-copy session required");return post.data();}
        @Override public List<PreCopyProtocol.RoundEvent> dirtyRounds(){return pre==null?List.of():pre.roundEvents();}
        @Override public StopAndCopyProtocol stopProtocol(){return stop;}
        @Override public PreCopyProtocol preProtocol(){return pre;}
        @Override public PostCopyProtocol postProtocol(){return post;}
    }
}
