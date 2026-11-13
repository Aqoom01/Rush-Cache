#!/usr/bin/env python3
"""Summarize native screening without ranking invalid or incomplete repetitions."""
import argparse
import csv
import json
from pathlib import Path
import statistics

REQUIRED=('correctnessValid','migrationValid','coverageValid','jointComponentsValid','fdRuntimeValid')


def summarize(root):
    with (root/'a1-a2-screen.csv').open(newline='') as stream:
        matrix=list(csv.DictReader(stream))
    source=root/'a1-a2-screen/batch-results.json'
    rows=json.loads(source.read_text()) if source.exists() else []
    expected={}
    for row in matrix:
        key=(row['a1'],row['a2'],row['read_selection'])
        expected.setdefault(key,set()).add(int(row['seed']))
    groups={key:[] for key in expected}
    tapes={}
    statuses={}
    for row in rows:
        selectors=row['selectors'];result=row.get('result') or {}
        statuses[row['status']]=statuses.get(row['status'],0)+1
        key=(selectors['a1'],selectors['a2'],selectors.get('read_selection','ADAPTIVE'))
        assert key in groups
        if result:
            assert result['offered']==sum(result[k] for k in ('ok','miss','failed','notAdmitted'))
            tape=tapes.setdefault(result['seed'],result['requestTapeSha256'])
            assert tape==result['requestTapeSha256'],'Different workload tape; do not compare as paired runs'
        groups[key].append(row)
    records=[]
    for (a1,a2,read),runs in groups.items():
        valid=[r for r in runs if r['status']=='PASS' and r['cleanup']['complete'] and
               r.get('result') and all(r['result'].get(flag) is True for flag in REQUIRED)]
        record=dict(a1=a1,a2=a2,read_selection=read,completed=len(runs),valid=len(valid),
                    expected=len(expected[(a1,a2,read)]),pairedComplete={int(r['selectors']['seed']) for r in valid}==expected[(a1,a2,read)])
        if valid:
            results=[r['result'] for r in valid]
            record.update(successFractionMedian=statistics.median((r['ok']+r['miss'])/r['offered'] for r in results),
                          goodputMedian=statistics.median(r['terminalGoodputIncludingDrain'] for r in results),
                          goodputMin=min(r['terminalGoodputIncludingDrain'] for r in results),
                          goodputMax=max(r['terminalGoodputIncludingDrain'] for r in results),
                          successP99MsMedian=statistics.median(r['successP99FromScheduleNanos']/1e6 for r in results),
                          successP99MsMin=min(r['successP99FromScheduleNanos']/1e6 for r in results),
                          successP99MsMax=max(r['successP99FromScheduleNanos']/1e6 for r in results),
                          failed=sum(r['failed'] for r in results),notAdmitted=sum(r['notAdmitted'] for r in results),
                          liveProcessDownReports=sum(r.get('fdLiveProcessDownReports',0) for r in results))
        records.append(record)
    ranked=sorted((r for r in records if r['pairedComplete'] and r['failed']==0),
                  key=lambda r:(-r['successFractionMedian'],r['successP99MsMedian']))
    report=dict(expectedRuns=len(matrix),completedRuns=len(rows),statuses=statuses,
                expectedConfigurations=len(expected),pairedComplete=sum(r['pairedComplete'] for r in records),
                sameRequestTapeWithinEachSeed=True,requestTapes=tapes,
                provisionalOrder=ranked,configurations=records,
                interpretation='Local fixed offered-rate screening. Order prioritizes success fraction then successful-request p99. Two seeds do not establish a final winner; check rejection, FD observations, and longer paired repeats.')
    return report


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root',required=True)
    parser.add_argument('--save',action='store_true')
    args=parser.parse_args();root=Path(args.root)
    report=summarize(root)
    if args.save:
        temporary=root/'screen-analysis.json.tmp'
        temporary.write_text(json.dumps(report,indent=2)+'\n');temporary.replace(root/'screen-analysis.json')
        columns=['a1','a2','read_selection','completed','valid','expected','pairedComplete','successFractionMedian',
                 'goodputMedian','goodputMin','goodputMax','successP99MsMedian','successP99MsMin','successP99MsMax',
                 'failed','notAdmitted','liveProcessDownReports']
        with (root/'screen-analysis.csv').open('w',newline='') as stream:
            writer=csv.DictWriter(stream,fieldnames=columns);writer.writeheader();writer.writerows(report['configurations'])
    print(json.dumps({key:report[key] for key in ('expectedRuns','completedRuns','statuses','pairedComplete')},indent=2))
    print(json.dumps(report['provisionalOrder'][:5],indent=2))


if __name__=='__main__':
    main()
