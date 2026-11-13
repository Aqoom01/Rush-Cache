#!/usr/bin/env python3
"""Server-side planning from independently verified cloud receipts, without an LLM or laptop."""
import argparse
import csv
import itertools
import json
from pathlib import Path
import random
import statistics
import tempfile
from cloud_archive import Aws,BUCKET,check,digest
from integration import A1_PROTOCOLS,A2_PROTOCOLS,row_plan_arguments
from core_followups import pick_candidate


def save(path,value):Path(path).write_text(json.dumps(value,indent=2)+'\n')


def identity(row):return row['a1'],row['a2'],row.get('read_selection','ADAPTIVE')


def candidate(row):return dict(zip(('a1','a2','read_selection'),identity(row)))


def row(c,seed=1,**values):
    result=dict(**candidate(c),fd='F3',distribution='RANGE',split='NONE',placement='NONE',scaler='fixed',
                scheduler='fifo',copy='PRE',drain='preflight-all',scenario='NORMAL',workload='HOT_RANGE',seed=str(seed),key_mode='DEFAULT')
    result.update(values);return result


def workload(rate,seconds,keys=128,value=1024,initial=2,read=80):
    return ['--rate',str(rate),'--requests',str(rate*seconds),'--warmup',str(rate*10),'--keys',str(keys),
            '--value-bytes',str(value),'--initial-groups',str(initial),'--read',str(read),'--concurrency','64']


def write_phase(plan,name,pairs,common):
    random.Random('core-cloud-'+name).shuffle(pairs)
    rows=[r for r,_ in pairs]
    for r in rows:row_plan_arguments(r)
    fields=list(dict.fromkeys(k for r in rows for k in r))
    with (plan/(name+'.csv')).open('w',newline='') as stream:
        writer=csv.DictWriter(stream,fieldnames=fields);writer.writeheader();writer.writerows(rows)
    save(plan/(name+'.json'),dict(planArguments=common,perRowArguments=[a for _,a in pairs],expectedCases=len(rows)))


def read_receipts(root,expected=None,require_cleanup=True):
    records=[];bindings=set()
    for path in sorted(Path(root).glob('*/backup-verified.json')):
        receipt=json.loads(path.read_text());cleanup_path=path.with_name('cleanup-complete.json')
        cleanup=json.loads(cleanup_path.read_text()) if require_cleanup or cleanup_path.exists() else {}
        check(receipt.get('verified') is True and (not require_cleanup or cleanup.get('terraformDestroyed') is True),'unverified or unclean cloud batch')
        token=receipt['binding']['batch'];check(token not in bindings,'duplicate batch');bindings.add(token)
        for record in receipt['experimentResults']:
            records.append(dict(**record,backupManifest=receipt['manifestKey'],batch=token,phase=receipt.get('phase','manual'),cleanupConfirmed=cleanup.get('terraformDestroyed') is True))
    if expected is not None:check(len(records)==expected,'missing cloud results: expected '+str(expected)+', got '+str(len(records)))
    return records


def valid(record):
    result=record.get('result') or {}
    return record['status']=='PASS' and all(result.get(k) is True for k in ('correctnessValid','migrationValid','coverageValid','jointComponentsValid','fdRuntimeValid')) and result.get('failed')==0 and result.get('fdLiveProcessDownReports',0)==0


def aggregate(records,minimum):
    groups={}
    for record in records:groups.setdefault(identity(record['selectors']),[]).append(record)
    choices=[]
    for key,items in groups.items():
        if len(items)<minimum or not all(valid(r) for r in items):continue
        results=[r['result'] for r in items]
        c=dict(zip(('a1','a2','read_selection'),key))
        c.update(successFractionMedian=statistics.median((r['ok']+r['miss'])/r['offered'] for r in results),
                 successP99MsMedian=statistics.median(r['successP99FromScheduleNanos']/1e6 for r in results))
        choices.append(c)
    check(choices,'no valid candidate; do not fabricate a winner')
    return sorted(choices,key=lambda c:(-c['successFractionMedian'],c['successP99MsMedian']))


def prepare(plan):
    plan.mkdir(parents=True,exist_ok=True)
    c=dict(a1='RAFT',a2='O00',read_selection='ADAPTIVE')
    write_phase(plan,'pilot',[(row(c),workload(rate,30)) for rate in (50,150,500,1500)],[])


def calibrate(plan,records):
    good=[r['result'] for r in records if valid(r)]
    check(good,'no valid EC2 pilot')
    rates=sorted(good,key=lambda r:r['configuredRate'])
    safe=[r for r in rates if (r['ok']+r['miss'])/r['offered']>=.99]
    check(safe,'no stable pilot rate; lower offered load and repeat preflight')
    moderate=safe[-1]['configuredRate'];higher=[r['configuredRate'] for r in rates if r['configuredRate']>moderate]
    if higher:high=higher[0]
    else:
        check(len(rates)>=2,'need two valid pilot operating points')
        moderate=rates[-2]['configuredRate'];high=rates[-1]['configuredRate']
    save(plan/'rates.json',dict(moderate=moderate,high=high,pilotResults=records,
                               highSaturationObserved=any(r['configuredRate']==high and (r['ok']+r['miss'])/r['offered']<.99 for r in rates)))
    pairs=[]
    for a1,a2 in itertools.product(A1_PROTOCOLS,A2_PROTOCOLS):
        for mode in (('ADAPTIVE','ROUND_ROBIN') if a2=='V06' else ('ADAPTIVE',)):
            pairs.append((row(dict(a1=a1,a2=a2,read_selection=mode)),[]))
    check(len(pairs)==63,'wrong screening design')
    write_phase(plan,'screen',pairs,workload(high,60))


def choose_initial(plan,records):
    choices=aggregate(records,1);fast=pick_candidate(choices)
    consensus=pick_candidate([c for c in choices if c['a1'] in ('RAFT','VR')])
    save(plan/'screen-selection.json',dict(fast=candidate(fast),consensus=candidate(consensus),ranked=choices,records=records))
    rates=json.loads((plan/'rates.json').read_text());pairs=[]
    for a2,seed in itertools.product(A2_PROTOCOLS,(2,3)):
        for mode in (('ADAPTIVE','ROUND_ROBIN') if a2=='V06' else ('ADAPTIVE',)):
            pairs.append((row(dict(a1=consensus['a1'],a2=a2,read_selection=mode),seed),[]))
    check(len(pairs)==18,'wrong working-set design')
    write_phase(plan,'working-set',pairs,workload(rates['high'],60,8192,4096))


def followups(plan,records):
    first=json.loads((plan/'screen-selection.json').read_text());rates=json.loads((plan/'rates.json').read_text())
    robust=candidate(pick_candidate(aggregate(records,2)));fast=first['fast']
    if identity(robust)==identity(fast):
        fast=candidate(next(c for c in first['ranked'] if identity(c)!=identity(robust)))
    tested=[fast,robust];baseline=dict(a1='RAFT',a2='O00',read_selection='ADAPTIVE');final=[]
    for c in [baseline,*tested,*first['ranked']]:
        if identity(c) not in [identity(x) for x in final]:final.append(candidate(c))
        if len(final)==3:break
    save(plan/'selection.json',dict(migrationCandidates=tested,finalists=final,workingSetResults=records,
                                   rule='Best median success fraction band (1 percentage point), then p99; distinct small/large working-set candidates plus baseline. Follow-ups use fresh seeds.'))
    io=['--override','migration.bytes.per.tick=8192','--override','scheduler.node.bytes.per.tick=8192']
    profiles=[('ORDER','NONE','NONE'),('ORDER','MIDPOINT','GREEDY'),('ORDER','HEAT','GREEDY'),
              ('ORDER','HEAT','SLICER'),('RANGE','HEAT','GREEDY'),('RING_ALLOC','NONE','NONE')]
    write_phase(plan,'balance',[(row(c,s,distribution=d,split=split,placement=p,scenario='SCALE_CYCLE',key_mode='ORDERED'),[])
                for c,s,(d,split,p) in itertools.product(tested,(2,3),profiles)],workload(rates['moderate'],90,256,4096,1)+io)
    write_phase(plan,'copy',[(row(c,s,copy=copy,scenario='SCALE_CYCLE'),[])
                for c,s,copy in itertools.product(tested,(2,3),('STOP','PRE','POST'))],workload(rates['moderate'],90,256,4096,1)+io)
    fds=('F3','REDIS_CLUSTER','PHI_GOSSIP','IGNITE_TCP_RING','AEROSPIKE_MESH')
    write_phase(plan,'failure',[(row(c,s,fd=fd,copy='POST',scenario='CRASH_SCALE',fault_layer='A2'),[])
                for c,s,fd in itertools.product(tested,(2,3),fds)],workload(rates['moderate'],90,128,1024,1,100))
    write_phase(plan,'final',[(row(c,s),workload(rate,120)) for c,s,rate in itertools.product(final,(4,5,6),(rates['moderate'],rates['high']))],[])


def choose_component(records,fields,allow_read_failures=False,require_move=False):
    groups={}
    for record in records:groups.setdefault(tuple(record['selectors'][k] for k in fields),[]).append(record)
    scores=[]
    for key,items in groups.items():
        if len(items)!=2:continue
        def eligible(r):
            result=r.get('result') or {}
            flags=r['status']=='PASS' and all(result.get(k) is True for k in ('correctnessValid','migrationValid','coverageValid','jointComponentsValid','fdRuntimeValid')) and result.get('fdLiveProcessDownReports',0)==0
            return flags and (allow_read_failures or result.get('failed')==0) and (not require_move or (r.get('gateway') or {}).get('completedMigrations',0)>0)
        if not all(eligible(r) for r in items):continue
        results=[r['result'] for r in items]
        scores.append(dict(zip(fields,key),successFractionMedian=statistics.median((r['ok']+r['miss'])/r['offered'] for r in results),
                           successP99MsMedian=statistics.median(r['successP99FromScheduleNanos']/1e6 for r in results)))
    check(scores,'no valid repeated component choice; preserve results instead of inventing a winner')
    chosen=pick_candidate(scores)
    return {k:chosen[k] for k in fields},scores


def finalize_selection(plan,records):
    check({p:sum(r['phase']==p for r in records) for p in ('balance','copy','failure')}==dict(balance=24,copy=12,failure=20),'missing component evidence')
    selection=json.loads((plan/'selection.json').read_text());rates=json.loads((plan/'rates.json').read_text());joint=[];evidence=[]
    for c in selection['migrationCandidates']:
        selected=[r for r in records if identity(r['selectors'])==identity(c)]
        b,bs=choose_component([r for r in selected if r['phase']=='balance'],('distribution','split','placement'))
        copy,cs=choose_component([r for r in selected if r['phase']=='copy'],('copy',),require_move=True)
        fd,fs=choose_component([r for r in selected if r['phase']=='failure'],('fd',),allow_read_failures=True)
        joint.append(dict(**c,**b,**copy,**fd));evidence.append(dict(a=c,balance=bs,copy=cs,failure=fs))
    baseline=dict(a1='RAFT',a2='O00',read_selection='ADAPTIVE',fd='F3',distribution='RANGE',split='NONE',placement='NONE',copy='PRE')
    pairs=[]
    for setting,seed,rate in itertools.product([baseline,*joint],(4,5,6),(rates['moderate'],rates['high'])):
        values={k:v for k,v in setting.items() if k not in ('a1','a2','read_selection')}
        pairs.append((row(setting,seed,scenario='SCALE_CYCLE',key_mode='ORDERED',**values),workload(rate,120,256,4096,1)))
    io=['--override','migration.bytes.per.tick=8192','--override','scheduler.node.bytes.per.tick=8192']
    write_phase(plan,'final',pairs,io)
    selection.update(finalJointSettings=[baseline,*joint],componentEvidence=evidence,
                     finalScope='New-seed scale-cycle validation of the combined A1/A2 + selected B1/B2 + copy + FD choices. Failed reads count against FD availability; false live-process DOWN is excluded.')
    save(plan/'selection.json',selection)


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('action',choices=('prepare','calibrate','select','followups','finalize-selection','report'))
    p.add_argument('--plan',default='core-plan');p.add_argument('--receipts',default='cloud-state');p.add_argument('--upload-report',action='store_true')
    args=p.parse_args();plan=Path(args.plan);plan.mkdir(parents=True,exist_ok=True)
    if args.action=='prepare':prepare(plan)
    elif args.action=='calibrate':calibrate(plan,read_receipts(args.receipts,4))
    elif args.action=='select':choose_initial(plan,read_receipts(args.receipts,63))
    elif args.action=='followups':followups(plan,read_receipts(args.receipts,18))
    elif args.action=='finalize-selection':finalize_selection(plan,read_receipts(args.receipts,56))
    else:
        records=read_receipts(args.receipts,require_cleanup=False)
        final_comparisons={}
        for record in records:
            if record.get('phase')=='final' and valid(record):
                rate=record['result']['configuredRate'];final_comparisons.setdefault(rate,[]).append(record)
        comparisons={}
        for rate,items in final_comparisons.items():
            strategies={}
            fields=('a1','a2','read_selection','fd','distribution','split','placement','copy','scheduler','drain')
            for item in items:strategies.setdefault(tuple(item['selectors'].get(k) for k in fields),[]).append(item)
            comparisons[str(rate)]=[dict(zip(fields,key),repetitions=len(group),
                                       successFractionMedian=statistics.median((r['result']['ok']+r['result']['miss'])/r['result']['offered'] for r in group),
                                       successP99MsMedian=statistics.median(r['result']['successP99FromScheduleNanos']/1e6 for r in group))
                                    for key,group in strategies.items() if len({r['result']['seed'] for r in group})>=3]
        counts={phase:sum(r.get('phase')==phase for r in records) for phase in sorted({r.get('phase','manual') for r in records})}
        expected=dict(pilot=4,screen=63,**{'working-set':18},balance=24,copy=12,failure=20,final=18)
        report=dict(expectedCoreCases=155,expectedPilotCases=4,collectedCases=len(records),complete=counts==expected and all(r['cleanupConfirmed'] for r in records),
                    allPassed=all(r['status']=='PASS' for r in records),
                    phaseCounts=counts,finalComparisons=comparisons,
                    statuses={s:sum(r['status']==s for r in records) for s in sorted({r['status'] for r in records})},
                    selection=json.loads((plan/'selection.json').read_text()) if (plan/'selection.json').exists() else {},
                    rates=json.loads((plan/'rates.json').read_text()) if (plan/'rates.json').exists() else {},results=records,
                    scope='Conditional prototype results on this EC2 workload. Pilot, screening, large-set, migration and failure evidence remain distinct; no global optimality or NVMe hardware qualification claim.')
        save(plan/'report.json',report)
        if args.upload_report:
            import os
            key='results/system-integration-core/ci-'+os.environ['CI_PIPELINE_ID']+'/REPORT.json';aws=Aws();aws.put(key,plan/'report.json')
            with tempfile.TemporaryDirectory() as temp:
                downloaded=Path(temp)/'report.json';aws.get(key,downloaded);check(digest(downloaded)==digest(plan/'report.json'),'summary upload mismatch')
        check(report['complete'],'incomplete core campaign; inspect verified partial results')
    print(json.dumps(dict(action=args.action,plan=str(plan))))


if __name__=='__main__':main()
