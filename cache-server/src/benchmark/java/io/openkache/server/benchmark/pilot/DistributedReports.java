package io.openkache.server.benchmark.pilot;

import io.openkache.runner.benchmark.TracePhase;
import io.openkache.runner.benchmark.client.ClientOutcome;
import io.openkache.runner.benchmark.measurement.*;
import java.io.IOException;
import java.math.BigInteger;
import java.util.*;

/** Strict phase reconciliation and histogram bucket merge, never percentile averaging. */
public final class DistributedReports {
    public static final long RANGE=60_000_000_000L;
    private static final long FINITE=new LatencyHistogram(RANGE).snapshot().highestFiniteNanos();
    private DistributedReports(){}
    public record Series(ClientOutcome.Operation operation,LatencyCollector.Kind kind,LatencyHistogram.Snapshot histogram){}
    public record Phase(int slot,TracePhase phase,DistributedPlan.Partition partition,long attempts,long retried,long durationNanos,List<Series> series){public Phase{series=List.copyOf(series);}}
    public static Phase phase(int slot,TracePhase phase,DistributedPlan.Partition partition,long attempts,long retried,long duration,LatencyCollector.Snapshot snapshot)throws IOException{
        if(snapshot.failure()!=null)throw new IOException("collector failure");List<Series> series=new ArrayList<>();
        for(var s:snapshot.series()){
            var p=s.population();if(p.phase()!=phase||p.outcome()!=ClientOutcome.Category.SUCCESS||p.reason()!=ClientOutcome.Reason.SUCCESS||p.censored()||!"SEMANTIC_SUCCESS".equals(p.responseStatus()))throw new IOException("unexpected pilot population");
            series.add(new Series(p.operation(),p.kind(),s.histogram()));
        }
        var result=new Phase(slot,phase,partition,attempts,retried,duration,series);validate(result,partition);return result;
    }
    public static void validate(Phase p,DistributedPlan.Partition expected)throws IOException{
        DistributedPlan.slot(p.slot());if(!p.partition().equals(expected)||p.durationNanos()<1||p.attempts()<expected.requests()||p.retried()<0||p.retried()>expected.requests())throw new IOException("phase partition/accounting mismatch");
        Set<String> keys=new HashSet<>();long[] counts=DistributedPlan.byOperation(expected);
        for(var s:p.series()){
            if(!keys.add(s.operation()+"/"+s.kind()))throw new IOException("duplicate histogram population");var h=s.histogram();validateHistogram(h);
            if(h.count()!=counts[s.operation().ordinal()]||h.count()==0)throw new IOException("histogram population count");
        }
        for(var op:ClientOutcome.Operation.values())for(var kind:LatencyCollector.Kind.values())if(counts[op.ordinal()]>0&&!keys.contains(op+"/"+kind))throw new IOException("missing histogram population");
    }
    private static void validateHistogram(LatencyHistogram.Snapshot h)throws IOException{
        long finite=0;Set<Long> bounds=new HashSet<>();bounds.add(0L);for(long n=1;n<=RANGE;){bounds.add(n);long step=Math.max(1,(n+99)/100);if(Long.MAX_VALUE-n<step)break;n+=step;}
        for(var b:h.buckets().entrySet()){if(!bounds.contains(b.getKey())||b.getValue()<1)throw new IOException("histogram bucket");finite=Math.addExact(finite,b.getValue());}
        if(!LatencyHistogram.BIN_VERSION.equals(h.binVersion())||h.highestFiniteNanos()!=FINITE||h.count()<1||h.overflowCount()<0||Math.addExact(finite,h.overflowCount())!=h.count()||h.sumNanos()==null||h.minimumNanos()==null||h.maximumNanos()==null||h.minimumNanos()<0||h.maximumNanos()<h.minimumNanos()||h.sumNanos().compareTo(BigInteger.valueOf(h.minimumNanos()).multiply(BigInteger.valueOf(h.count())))<0||h.sumNanos().compareTo(BigInteger.valueOf(h.maximumNanos()).multiply(BigInteger.valueOf(h.count())))>0)throw new IOException("histogram integrity/range");
    }
    public static List<Series> merge(Phase a,Phase b)throws IOException{
        if(a.slot()==b.slot()||a.phase()!=b.phase())throw new IOException("merge slot/phase mismatch");validate(a,a.partition());validate(b,b.partition());List<Series> result=new ArrayList<>();
        for(var op:ClientOutcome.Operation.values())for(var kind:LatencyCollector.Kind.values()){
            List<LatencyHistogram.Snapshot> parts=new ArrayList<>();for(var p:List.of(a,b))for(var s:p.series())if(s.operation()==op&&s.kind()==kind)parts.add(s.histogram());if(parts.isEmpty())continue;
            long count=0,overflow=0,min=Long.MAX_VALUE,max=0;BigInteger sum=BigInteger.ZERO;NavigableMap<Long,Long> buckets=new TreeMap<>();
            for(var h:parts){count=Math.addExact(count,h.count());overflow=Math.addExact(overflow,h.overflowCount());sum=sum.add(h.sumNanos());min=Math.min(min,h.minimumNanos());max=Math.max(max,h.maximumNanos());h.buckets().forEach((bound,n)->buckets.merge(bound,n,Math::addExact));}
            result.add(new Series(op,kind,new LatencyHistogram.Snapshot(LatencyHistogram.BIN_VERSION,FINITE,count,sum,min,max,overflow,buckets)));
        }return List.copyOf(result);
    }
    public static Properties packet(Phase p){
        var out=DistributedWire.packet("REPORT");DistributedWire.put(out,"slot",p.slot());DistributedWire.put(out,"phase",p.phase());var n=p.partition();
        DistributedWire.put(out,"requests",n.requests());DistributedWire.put(out,"gets",n.gets());DistributedWire.put(out,"sets",n.sets());DistributedWire.put(out,"deletes",n.deletes());DistributedWire.put(out,"partition.sha256",n.sha256());DistributedWire.put(out,"attempts",p.attempts());DistributedWire.put(out,"retried",p.retried());DistributedWire.put(out,"duration.ns",p.durationNanos());DistributedWire.put(out,"series.count",p.series().size());
        for(int i=0;i<p.series().size();i++){var s=p.series().get(i);var h=s.histogram();String k="h."+i+".";DistributedWire.put(out,k+"operation",s.operation());DistributedWire.put(out,k+"kind",s.kind());DistributedWire.put(out,k+"version",h.binVersion());DistributedWire.put(out,k+"finite",h.highestFiniteNanos());DistributedWire.put(out,k+"count",h.count());DistributedWire.put(out,k+"sum",h.sumNanos());DistributedWire.put(out,k+"min",h.minimumNanos());DistributedWire.put(out,k+"max",h.maximumNanos());DistributedWire.put(out,k+"overflow",h.overflowCount());
            StringJoiner bins=new StringJoiner(",");h.buckets().forEach((upper,count)->bins.add(upper+":"+count));DistributedWire.put(out,k+"buckets",bins);}
        return out;
    }
    public static Phase read(Properties p)throws IOException{
        try{DistributedWire.kind(p,"REPORT");int slot=Math.toIntExact(DistributedWire.number(p,"slot"));var phase=TracePhase.valueOf(DistributedWire.required(p,"phase"));var part=new DistributedPlan.Partition(DistributedWire.number(p,"requests"),DistributedWire.number(p,"gets"),DistributedWire.number(p,"sets"),DistributedWire.number(p,"deletes"),DistributedWire.required(p,"partition.sha256"));List<Series> result=new ArrayList<>();int size=Math.toIntExact(DistributedWire.number(p,"series.count"));if(size<0||size>9)throw new IOException("histogram series limit");
            for(int i=0;i<size;i++){String k="h."+i+".";NavigableMap<Long,Long> buckets=new TreeMap<>();String raw=p.getProperty(k+"buckets","");if(!raw.isEmpty())for(String item:raw.split(",")){String[] pair=item.split(":",-1);if(pair.length!=2||buckets.put(Long.parseLong(pair[0]),Long.parseLong(pair[1]))!=null)throw new IOException("duplicate/malformed histogram bucket");}
                var h=new LatencyHistogram.Snapshot(DistributedWire.required(p,k+"version"),DistributedWire.number(p,k+"finite"),DistributedWire.number(p,k+"count"),new BigInteger(DistributedWire.required(p,k+"sum")),DistributedWire.number(p,k+"min"),DistributedWire.number(p,k+"max"),DistributedWire.number(p,k+"overflow"),buckets);result.add(new Series(ClientOutcome.Operation.valueOf(DistributedWire.required(p,k+"operation")),LatencyCollector.Kind.valueOf(DistributedWire.required(p,k+"kind")),h));}
            var r=new Phase(slot,phase,part,DistributedWire.number(p,"attempts"),DistributedWire.number(p,"retried"),DistributedWire.number(p,"duration.ns"),result);validate(r,part);return r;
        }catch(IllegalArgumentException|ArithmeticException e){throw new IOException("invalid phase report",e);}
    }
    public static Map<String,Object> histogramJson(Series s){var h=s.histogram();List<Map<String,Long>> bins=new ArrayList<>();h.buckets().forEach((upper,count)->bins.add(Map.of("upper_ns",upper,"count",count)));
        Map<String,Object> m=new LinkedHashMap<>();m.put("operation",s.operation());m.put("kind",s.kind());m.put("bin_version",h.binVersion());m.put("highest_finite_ns",h.highestFiniteNanos());m.put("count",h.count());m.put("sum_ns",h.sumNanos());m.put("minimum_ns",h.minimumNanos());m.put("maximum_ns",h.maximumNanos());m.put("overflow",h.overflowCount());m.put("buckets",bins);m.put("mean_ns",h.meanNanos());m.put("p50",h.quantile(50,100));m.put("p95",h.quantile(95,100));m.put("p99",h.quantile(99,100));m.put("p999",h.quantile(999,1000));return m;
    }
    public static Map<String,Object> json(Phase p){return Map.of("slot",p.slot(),"phase",p.phase(),"partition",p.partition(),"attempts",p.attempts(),"retried",p.retried(),"duration_ns",p.durationNanos(),"histograms",p.series().stream().map(DistributedReports::histogramJson).toList());}
}
