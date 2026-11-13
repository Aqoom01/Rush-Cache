"""Approximate resource deltas within a completed local measurement window.

A1/A2 share a PID: CPU/RSS must never be added once per layer. Missing native
TCP counters stay unavailable, and outbound observations are not total network IO.
"""
from datetime import datetime
import json
import os
from pathlib import Path


def epoch_ns(value):
    return int(datetime.fromisoformat(value.strip().replace('Z','+00:00')).timestamp()*1e9)


def cpu_ticks(proc_stat):
    fields=proc_stat[proc_stat.rfind(')')+2:].split()
    return int(fields[11])+int(fields[12])


def rss_kib(status):
    values=[line.split()[1] for line in status.splitlines() if line.startswith('VmRSS:')]
    return int(values[0]) if values else None


def summarize_resources(run):
    run=Path(run)
    start=epoch_ns((run/'runtime/load-result/MEASURE_STARTED').read_text())
    events=[json.loads(line) for line in (run/'runtime/gateway/events.jsonl').read_text().splitlines()]
    stop=next(epoch_ns(e['atUtc']) for e in events if e['event']=='automatic-control-stopped')
    samples=[s for s in json.loads((run/'resource-samples.json').read_text()) if start<=s['collected_epoch_ns']<=stop]
    if len(samples)<2:
        return dict(available=False,reason='Fewer than two in-measurement samples')
    first,last=samples[0],samples[-1]
    roles=sorted(set(first['processes'])&set(last['processes']))
    cores={};cpu_seconds={};issues=[]
    for role in roles:
        a,b=first['processes'][role],last['processes'][role]
        if not (a.get('active') and b.get('active') and a.get('stat') and b.get('stat')):
            issues.append(role+': inactive or missing CPU stat');continue
        if (a['state']['pid'],a['state']['start_ticks'])!=(b['state']['pid'],b['state']['start_ticks']):
            issues.append(role+': process identity changed');continue
        delta=(b['monotonic_ns']-a['monotonic_ns'])/1e9
        seconds=(cpu_ticks(b['stat'])-cpu_ticks(a['stat']))/os.sysconf('SC_CLK_TCK')
        if delta<=0 or seconds<0:
            issues.append(role+': invalid counter interval');continue
        cpu_seconds[role]=seconds;cores[role]=seconds/delta
    peaks=[]
    for sample in samples:
        rss=[rss_kib(p.get('status','')) for p in sample['processes'].values() if p.get('active')]
        if len(rss)==8 and all(v is not None for v in rss):peaks.append(sum(rss))
    network={}
    for layer in ('A1','A2'):
        a={n['pid']:(n['a1_authority'] if layer=='A1' else n) for n in first['nodes']}
        b={n['pid']:(n['a1_authority'] if layer=='A1' else n) for n in last['nodes']}
        if len(a)==6 and a.keys()==b.keys() and all(a[p].get('tcpObservationEnabled') and b[p].get('tcpObservationEnabled')
               and isinstance(a[p].get('tcpSentStreamBytes'),int) and isinstance(b[p].get('tcpSentStreamBytes'),int) for p in a):
            network[layer]=sum(b[p]['tcpSentStreamBytes']-a[p]['tcpSentStreamBytes'] for p in a)
        else:network[layer]=None
    return dict(available=not issues,issues=issues,samples=len(samples),
                approximateWindowSeconds=(last['collected_epoch_ns']-first['collected_epoch_ns'])/1e9,
                cpuSecondsByRole=cpu_seconds,averageCpuCoresByRole=cores,
                averageAllJvmCpuCores=sum(cores.values()) if len(cores)==8 else None,
                averageReplicaCpuCores=sum(v for role,v in cores.items() if role.startswith('replica-')),
                peakSampledAllJvmRssKiB=max(peaks) if peaks else None,
                observedNativeOutboundTcpBytes=network,
                scope='Local sampled window; CPU/RSS count each PID once. Native outbound bytes are partial observations; null means unavailable. Fault runs with missing/dead processes are not full-resource comparisons.')
