"""Summarize only verified Raft FD runs; keep fallback runs visible."""
import argparse
import csv
import json
from pathlib import Path
import statistics


def analyze(root):
    rows=[]
    for path in root.rglob('*.json'):
        data=json.loads(path.read_text())
        if not isinstance(data,dict) or data.get('schema')!='a1-real-election-1' or data.get('protocol')!='RAFT':
            continue
        if not data.get('ok'):
            raise ValueError(f'failed run: {path}')
        if (data['failoverNs']<=0 or data['newEpoch']<=data['initialEpoch']
                or data['originalLeader']==data['newLeader'] or data['staleEpochStatus']!=5
                or data['backupWriteStatus']!=4 or data['preservedSuccesses']!=data['normalRequests']):
            raise ValueError(f'invalid safety/timing evidence: {path}')
        rows.append(data)
    if not rows:
        raise ValueError('no verified Raft FD results')
    if len({r['runId'] for r in rows})!=len(rows):
        raise ValueError('duplicate run IDs; choose one raw-results directory')
    summary=[]
    for condition in sorted({(r.get('failureDetector','FIXED_TIMEOUT'),r['valueBytes'],r['timeoutMs']) for r in rows}):
        group=[r for r in rows if (r.get('failureDetector','FIXED_TIMEOUT'),r['valueBytes'],r['timeoutMs'])==condition]
        median=lambda key:statistics.median(r[key]/1e6 for r in group)
        summary.append(dict(detector=condition[0],valueBytes=condition[1],timeoutMs=condition[2],runs=len(group),
                            failoverMedianMs=median('failoverNs'),failoverMaxMs=max(r['failoverNs']/1e6 for r in group),
                            promotionMedianMs=median('promotionNs'),setP99MedianMs=median('normalP99Ns'),
                            fallbackRuns=sum(r.get('fallbackUsed',False) for r in group),
                            swimDeadRuns=sum(r.get('electionOrigin')=='SWIM_DEAD' for r in group)))
    with (root/'raft-fd-summary.csv').open('w',newline='') as out:
        writer=csv.DictWriter(out,fieldnames=list(summary[0]));writer.writeheader();writer.writerows(summary)
    (root/'raft-fd-summary.json').write_text(json.dumps(summary,indent=2)+'\n')
    print(json.dumps(summary,indent=2))


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('raw',type=Path)
    analyze(p.parse_args().raw)
