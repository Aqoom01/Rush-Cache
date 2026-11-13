#!/usr/bin/env python3
"""Create reviewable follow-ups from completed paired screening; execute one named job at a time."""
import argparse
import csv
import hashlib
import itertools
import json
from pathlib import Path
import random
import subprocess
import sys
import time
from analyze_core_focus import summarize


def identity(row):
    return row['a1'],row['a2'],row['read_selection']


def pick_candidate(rows):
    best=max(r['successFractionMedian'] for r in rows)
    eligible=[r for r in rows if r['successFractionMedian']>=best-.01]
    return min(eligible,key=lambda r:r['successP99MsMedian'])


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--screen-root',required=True)
    parser.add_argument('--bundle',required=True)
    parser.add_argument('--output',required=True)
    parser.add_argument('--execute-job')
    args=parser.parse_args();root=Path(args.screen_root).resolve();bundle=Path(args.bundle).resolve();output=Path(args.output).resolve()
    if args.execute_job:
        jobs=json.loads((output/'jobs.json').read_text());job=next(j for j in jobs if j['name']==args.execute_job)
        assert not (output/(job['name']+'-run')).exists(),'Use a fresh result directory; do not overwrite runs'
        command=[sys.executable,str(bundle/'tools/integration.py'),'batch','--matrix',str(output/job['matrix']),
                 '--bundle',str(bundle),'--output',str(output/(job['name']+'-run')),'--execute',
                 '--limit',str(job['runs']),'--timeout',str(job['timeoutSeconds'])]
        command.extend('--plan-arg='+value for value in job['commonArguments'])
        (output/(job['name']+'-invocation.json')).write_text(json.dumps(dict(command=command,startedUtcNs=time.time_ns(),
            driverSha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest()),indent=2)+'\n')
        start=time.monotonic()
        with (output/(job['name']+'-console.log')).open('w') as log:
            completed=subprocess.run(command,stdout=log,stderr=subprocess.STDOUT)
        status=dict(name=job['name'],exitCode=completed.returncode,wallSeconds=time.monotonic()-start,completedUtcNs=time.time_ns())
        (output/(job['name']+'-process-complete.json')).write_text(json.dumps(status,indent=2)+'\n')
        print(json.dumps(status));raise SystemExit(completed.returncode)
    report=summarize(root)
    assert report['completedRuns']==report['expectedRuns']==126,'Finish paired screening before selecting follow-ups'
    ranked=[r for r in report['provisionalOrder'] if r['liveProcessDownReports']==0]
    assert ranked,'No valid candidates; inspect failures before follow-up'
    fast=pick_candidate(ranked)
    consensus=pick_candidate([r for r in ranked if r['a1'] in ('RAFT','VR')])
    choices=[]
    for row in [dict(a1='RAFT',a2='O00',read_selection='ADAPTIVE'),fast,consensus,*ranked]:
        if identity(row) not in [identity(c) for c in choices]:
            choices.append(dict(zip(('a1','a2','read_selection'),identity(row))))
        if len(choices)==3:break
    tested=[dict(zip(('a1','a2','read_selection'),identity(fast)))]
    second=consensus if identity(consensus)!=identity(tested[0]) else next(r for r in ranked if r['a1']!=tested[0]['a1'])
    tested.append(dict(zip(('a1','a2','read_selection'),identity(second))))
    assert len(tested)==2
    output.mkdir(parents=True,exist_ok=False)
    selection=dict(candidates=choices,stressAndMigrationCandidates=tested,selection='Baseline plus fast and RAFT/VR candidates. Within 1 percentage point of the best median success fraction, choose the lowest median successful-request p99; exclude invalid, failed-request, or live-process-DOWN runs. Fill distinct candidates if overlapping.',
        screenSourceSha256=hashlib.sha256((root/'a1-a2-screen/batch-results.json').read_bytes()).hexdigest(),
        scope='Data-dependent selection is followed by longer repeats; screening is not a final winner.')
    (output/'selection.json').write_text(json.dumps(selection,indent=2)+'\n')
    jobs=[]

    def make_job(name,rows,common,purpose,timeout=240):
        random.Random('20261113-'+name).shuffle(rows)
        path=output/(name+'.csv')
        with path.open('w',newline='') as stream:
            writer=csv.DictWriter(stream,fieldnames=list(rows[0]));writer.writeheader();writer.writerows(rows)
        jobs.append(dict(name=name,runs=len(rows),matrix=path.name,matrixSha256=hashlib.sha256(path.read_bytes()).hexdigest(),timeoutSeconds=timeout,
                         commonArguments=common,purpose=purpose))

    def row(candidate,seed):
        return dict(**candidate,fd='F3',distribution='RANGE',split='NONE',placement='NONE',scaler='fixed',
                    scheduler='fifo',copy='PRE',drain='preflight-all',scenario='NORMAL',workload='HOT_RANGE',
                    seed=str(seed),key_mode='DEFAULT')

    def override(candidate,seed,**values):
        result=row(candidate,seed);result.update(values);return result

    def common(rate,seconds,keys=128,value=1024,initial=2,read=80):
        return ['--groups','2','--initial-groups',str(initial),'--keys',str(keys),'--value-bytes',str(value),
                '--read',str(read),'--concurrency','64','--requests',str(rate*seconds),'--rate',str(rate),'--warmup',str(rate*10)]

    variants=[dict(a1=consensus['a1'],a2=a2,read_selection=read) for a2 in ('O00','V01','V02','V03','V04','V05','V06','V07')
              for read in (('ADAPTIVE','ROUND_ROBIN') if a2=='V06' else ('ADAPTIVE',))]
    make_job('a2-working-set',[row(c,s) for c,s in itertools.product(variants,(3,4,5))],common(1000,60,8192,4096),
             'A2 variants under one RAFT/VR authority, larger than the 4096-entry history target. Compare within this workload; local page cache is not NVMe qualification. Revisit finalist selection if this contradicts tiny-set screening.',timeout=600)
    for rate in (300,1000):
        make_job('long-normal-'+str(rate),[row(c,s) for c,s in itertools.product(choices,(3,4,5))],common(rate,120),
                 'Longer paired steady-load validation at moderate/stress rates; new seeds distinct from screening.')
    make_job('copy-scale',[override(c,s,copy=copy,scenario='SCALE_CYCLE') for c,s,copy in itertools.product(tested,(3,4,5),('STOP','PRE','POST'))],
             common(300,90,256,4096,1)+['--override','migration.bytes.per.tick=8192','--override','scheduler.node.bytes.per.tick=8192'],
             'Matched scale out and drain back to one group; compare actual copy methods under identical bandwidth budget.')
    profiles=[('ORDER','NONE','NONE'),('ORDER','MIDPOINT','GREEDY'),('ORDER','HEAT','GREEDY'),
              ('ORDER','HEAT','SLICER'),('RANGE','HEAT','GREEDY'),('RING_ALLOC','NONE','NONE')]
    make_job('ordered-hot-balance',[override(c,s,distribution=d,split=split,placement=p,scenario='SCALE_CYCLE',key_mode='ORDERED')
             for c,s,(d,split,p) in itertools.product(tested,(3,4,5),profiles)],
             common(300,90,256,4096,1)+['--override','migration.bytes.per.tick=8192','--override','scheduler.node.bytes.per.tick=8192'],
             'Ordered hot range contrasts: contiguous hotspot versus hashing; midpoint/heat split and greedy/Slicer placement. Same ordered keys in every row.')
    fds=('F3','REDIS_CLUSTER','PHI_GOSSIP','IGNITE_TCP_RING','AEROSPIKE_MESH')
    make_job('fd-crash',[override(c,s,fd=fd,copy='POST',scenario='CRASH_SCALE',fault_layer='A2')
             for c,s,fd in itertools.product(tested,(3,4,5),fds)],common(300,90,128,1024,1,100),
             'Product-profile defaults; actual co-located JVM hosting A2 leader is terminated during movement. Read-only measured load separates availability from ambiguous writes.')
    (output/'jobs.json').write_text(json.dumps(jobs,indent=2)+'\n')
    print(json.dumps(dict(selection=selection,jobs=jobs),indent=2))


if __name__=='__main__':
    main()
