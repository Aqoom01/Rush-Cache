"""Report every planned run and paired differences; never hide failed/pending evidence."""
import argparse
import csv
import json
from pathlib import Path
import random
import statistics
import hashlib
import time

from raft_campaign_plan import DETECTORS


def write_csv(path, rows):
    if not rows: return
    with path.open('w',newline='') as out:
        writer=csv.DictWriter(out,fieldnames=list(rows[0]));writer.writeheader();writer.writerows(rows)


def median_interval(values):
    """Exploratory paired-run bootstrap, not a bootstrap of individual requests."""
    if len(values)<5: return None,None
    rng=random.Random(1701)
    samples=sorted(statistics.median(rng.choices(values,k=len(values))) for _ in range(2000))
    return samples[49],samples[1949]


def summarize(root):
    manifest=json.loads((root/'manifest.json').read_text());rows=[];groups={}
    for spec in manifest['matrix']:
        path=root/spec['case']/'result.json'
        data=json.loads(path.read_text()) if path.exists() else {}
        def ms(key): return data[key]/1e6 if data.get(key) is not None else None
        seconds=data.get('normalDurationNs',0)/1e9
        successes=data.get('normalSuccesses',data.get('normalRequests',0))
        def per_second(key): return data[key]/seconds if key in data and seconds else None
        def per_success(key): return data[key]/successes if key in data and successes else None
        row=dict(case=spec['case'],scenario=spec['scenario'],detector=spec['detector'],repeat=spec['repeat'],
                 originalLeader=data.get('originalLeader'),finalLeader=data.get('finalLeader'),
                 faultTarget=next((e['node'] for e in data.get('faultEvents',[]) if 'node' in e),None),
                 status='passed' if data.get('ok') else 'failed' if data else 'pending',error=data.get('error',''),
                 recoveryMs=ms('recoveryUpperNs'),preserved=data.get('preservedSuccesses'),
                 falseDeadEvents=data.get('falseDeadEvents'),falseLeaderDeadEvents=data.get('falseLeaderDeadEvents'),
                 falseDeadPerLiveNodeMinute=data.get('falseDeadPerLiveNodeMinute'),
                 electionStarts=data.get('electionStarts'),leadershipAcquisitions=data.get('leadershipAcquisitions'),
                 leaderIdentityChanges=data.get('leaderIdentityChanges'),sameLeaderReelections=data.get('sameLeaderReelections'),
                 electionsWhileLeaderAlive=data.get('electionsWhileLeaderAlive'),
                 leaderChangesWhileLeaderAlive=data.get('leaderChangesWhileLeaderAlive'),fallbackElections=data.get('fallbackElections'),
                 faultWindowAttemptSuccessRate=data.get('faultWindowAttemptSuccessRate'),
                 normalP50Ms=ms('normalP50Ns'),normalP99Ms=ms('normalP99Ns'),normalSuccesses=successes if data else None,
                 normalSuccessRate=data.get('normalSuccessRate'),normalAchievedSuccessRate=data.get('normalAchievedSuccessRate'),
                 normalMissedSlots=data.get('normalMissedSlots'),normalMinimumSamplesMet=data.get('normalMinimumSamplesMet'),
                 normalSeconds=seconds if data else None,
                 replicationBytesPerSet=per_success('normalReplicationBytes'),fdBytesPerSet=per_success('normalFdBytes'),
                 replicationBytesPerSecond=per_second('normalReplicationBytes'),fdBytesPerSecond=per_second('normalFdBytes'),
                 fdAwarenessMax=data.get('fdAwarenessMax'),fdProbeIntervalMaxMs=ms('fdProbeIntervalMaxNs'),
                 boundaryUncertainRaftEvents=data.get('boundaryUncertainRaftEvents'))
        if spec['detector']=='FIXED_TIMEOUT':
            # No SWIM detector exists here; zero would imply a measured false-positive rate.
            for key in ('falseDeadEvents','falseLeaderDeadEvents','falseDeadPerLiveNodeMinute','fdAwarenessMax','fdProbeIntervalMaxMs'):
                row[key]=None
        if data.get('ok'):
            if data.get('schema')!='raft-scenario-1' or not data.get('verifiedCopies') or not data.get('preservedSuccesses'):
                raise ValueError('incomplete safety audit: '+str(path))
            if data['spec']!=spec: raise ValueError('result does not match manifest: '+str(path))
        groups.setdefault((spec['scenario'],spec['detector']),[]).append(row)
        rows.append(row)
    write_csv(root/'runs.csv',rows)
    summary=[]
    metrics=('recoveryMs','falseLeaderDeadEvents','electionStarts','leaderIdentityChanges','normalP50Ms','normalP99Ms',
             'replicationBytesPerSecond','fdBytesPerSecond','normalSuccessRate','faultWindowAttemptSuccessRate')
    for (scenario,detector),planned in groups.items():
        passed=[r for r in planned if r['status']=='passed']
        row=dict(scenario=scenario,detector=detector,planned=len(planned),passed=len(passed),
                 failed=sum(r['status']=='failed' for r in planned),pending=sum(r['status']=='pending' for r in planned))
        for key in metrics:
            values=[r[key] for r in passed if r[key] is not None]
            row.update({key+'N':len(values),key+'Median':statistics.median(values) if values else None,
                        key+'Min':min(values) if values else None,key+'Max':max(values) if values else None})
        summary.append(row)
    write_csv(root/'conditions.csv',summary)
    index={(r['scenario'],r['detector'],r['repeat']):r for r in rows if r['status']=='passed'}
    paired=[];comparisons=[]
    for scenario in dict.fromkeys(r['scenario'] for r in rows):
        for left,right in zip(DETECTORS,DETECTORS[1:]):
            relevant=('normalP50Ms','normalP99Ms','replicationBytesPerSecond','fdBytesPerSecond') if scenario=='healthy' else (
                ('recoveryMs',) if scenario=='crash' else ('falseLeaderDeadEvents','electionStarts','leaderIdentityChanges','faultWindowAttemptSuccessRate'))
            for metric in relevant:
                if left=='FIXED_TIMEOUT' and metric=='falseLeaderDeadEvents': continue
                deltas=[]
                for repeat in sorted({r['repeat'] for r in rows if r['scenario']==scenario}):
                    a=index.get((scenario,left,repeat));b=index.get((scenario,right,repeat))
                    if a is None or b is None or a[metric] is None or b[metric] is None: continue
                    delta=b[metric]-a[metric];deltas.append(delta)
                    paired.append(dict(scenario=scenario,left=left,right=right,repeat=repeat,metric=metric,
                                       leftValue=a[metric],rightValue=b[metric],deltaRightMinusLeft=delta))
                low,high=median_interval(deltas)
                comparisons.append(dict(scenario=scenario,left=left,right=right,metric=metric,pairs=len(deltas),
                                        medianDeltaRightMinusLeft=statistics.median(deltas) if deltas else None,
                                        exploratoryBootstrap95Low=low,exploratoryBootstrap95High=high))
    write_csv(root/'paired-comparisons.csv',paired);write_csv(root/'comparison-summary.csv',comparisons)
    complete=all(r['status']=='passed' for r in rows)
    lines=['# Raft + Failure Detector comparison','',
           f'Backend: {manifest["backend"]}. Passed {sum(r["status"]=="passed" for r in rows)}/{len(rows)}. Complete: {complete}.','',
           f'Study: {manifest.get("study","legacy")}. Fixed settings: {manifest.get("fixedSettings","see node properties")}.','',
           f'Network model: {manifest.get("networkModel")}. Contention: {manifest.get("contentionModel")}.','',
           '## Questions and evidence','',
           '1. External detector: crash recovery versus live-leader election starts.',
           '2. Lifeguard: observer slowdown false original-leader DEAD, candidacies, actual leader changes, awareness and request success.',
           '3. Healthy cost: per-run latency, achieved request rate, missed slots and application bytes/second plus bytes/successful SET.','',
           '|Scenario|Detector|Passed/planned|Recovery median ms|False leader DEAD median|Election starts median|Leader changes median|',
           '|---|---|---:|---:|---:|---:|---:|']
    def show(value): return 'N/A' if value is None else f'{value:.3f}' if isinstance(value,float) else str(value)
    for s in summary:
        lines.append('|'+ '|'.join([s['scenario'],s['detector'],f'{s["passed"]}/{s["planned"]}',
                    *[show(s[k+'Median']) for k in ('recoveryMs','falseLeaderDeadEvents','electionStarts','leaderIdentityChanges')]])+'|')
    lines+=['','## Failed/pending conditions','']
    lines += [f'- {r["case"]}: {r["status"]}; {r["error"]}' for r in rows if r['status']!='passed'] or ['None.']
    lines+=['','## Interpretation limits','',
            '- Failed/pending rows are retained. Success-only summaries must not conceal failures.',
            '- Election starts are node-level candidacies, not replacements. Same-node reelections are separate.',
            '- Baseline SWIM DEAD/awareness metrics are N/A because no SWIM detector exists; they are not measured zeros.',
            '- Journal times distinguish pre-fault, definite post-fault and uncertain clock boundaries.',
            '- Live leaders under replication delay may legitimately lose leadership; a live-process count alone does not prove an unnecessary election.',
            '- Paired repeat-seed differences and bootstrap intervals are exploratory, unadjusted for multiple comparisons; <5 pairs have no interval.',
            '- Individual requests are not independent repetitions. Zero observed events does not prove zero risk.',
            '- Recovery is an upper estimate to the first successful post-fault SET, not full throughput recovery.',
            '- Clock RTT bounds exclude clock drift. Diagnostic polling affects all modes.',
            '- Bytes are application protocol counters, excluding TCP/IP headers/retransmissions and AWS billing traffic.',
            '- One paced client does not measure maximum throughput. Check achieved rate and >=1000 healthy samples before interpreting p99.',
            '- Fault-window success includes attempts fully contained within the fault interval; boundary-crossing attempts remain in the raw history.',
            '- I/O stall is software delay, not physical SSD outage. Process restart durability is out of scope.',
            '- Local results validate implementation, not EC2 performance.',
            '- No winner is selected automatically. Report recovery/false-positive/overhead tradeoffs and adverse outcomes together.','']
    (root/'report.md').write_text('\n'.join(lines))
    (root/'analysis-provenance.json').write_text(json.dumps(dict(
        generatedAtEpochNs=time.time_ns(),summarizerSha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        manifestSha256=hashlib.sha256((root/'manifest.json').read_bytes()).hexdigest(),
        pairedBootstrapSeed=1701,pairedBootstrapIterations=2000),indent=2)+'\n')
    return complete


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('out',type=Path)
    if not summarize(p.parse_args().out):raise SystemExit('campaign incomplete: see report.md')
