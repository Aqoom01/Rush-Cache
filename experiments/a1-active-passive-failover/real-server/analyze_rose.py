"""Rosé 단일 파티션 변형 L별 30회 원본 검증·CSV·그래프."""
import argparse
import csv
import hashlib
import json
from pathlib import Path
import statistics

LIMITS=(0,1,4,16,-1)


def validate(rows,full=True):
    if full:
        assert len(rows)==30 and {(r['limit'],r['valueBytes'],r['repeat']) for r in rows}=={(l,v,n) for l in LIMITS for v in (64,8192) for n in (1,2,3)}
    assert len({r['runId'] for r in rows})==len(rows)
    for r in rows:
        assert r['schema']=='a1-real-rose-1' and r['ok'] and r['preCrashDigestsEqual'] and r['protocol']=='ROSE' and r['limit'] in LIMITS and r['normalThreads']==8
        n=r['normal'];s=sorted(n['samplesNs']);assert len(s)==n['writes']==512 and min(s)>0 and n['p50Ns']==s[255] and n['p99Ns']==s[506]
        assert r['normalVerifiedCopies']==1536
        assert all(x['limit']==r['limit'] and x['applied']==x['logLength']==542 for x in r['statesNormal'])
        if r['limit']>=0:
            assert r['statesNormal'][0]['peakLag']<=max(1,r['limit'])
            assert r['lostOkWrites']<=r['limit']
        else:
            assert r['statesNormal'][0]['backpressureCount']==0
        if r['limit']==0:
            assert r['lostOkWrites']==0
        assert r['crashOkWrites']>=256 and r['crashUnknownWrites']<=8 and r['crashWriterErrors']==[]
        assert r['freezeReplies'][0].startswith('applied=') and r['freezeReplies'][1].startswith('applied=')
        assert r['newEpoch']==2 and r['newPrimary'] in (1,2) and r['repairedLength']>=542
        assert r['failoverNs']==r['firstNewPrimaryOkNs']-r['faultCallBeginNs']>0 and r['detectWaitNs']>=r['timeoutMs']*1_000_000
        assert all(x['epoch']==2 and x['state']=='RUNNING' and x['primary']==r['newPrimary'] for x in r['statesAfter'])
        assert r['promotionNs']>=0 and r['staleEpochStatus']==5 and r['backupWriteStatus']==4
        assert r['postCrashVerifiedCopies']==2*(512+r['crashOkWrites']-r['lostOkWrites']+1)


def summarize(r):
    n=r['normal'];f=[x['applied'] for x in r['statesFrozen']]
    return dict(limit=r['limit'],valueBytes=r['valueBytes'],repeat=r['repeat'],normalP50Ms=n['p50Ns']/1e6,normalP99Ms=n['p99Ns']/1e6,
                opsPerSecond=n['opsPerSecond'],busyRetries=n['busyRetries'],peakLag=r['statesNormal'][0]['peakLag'],backpressureCount=r['statesNormal'][0]['backpressureCount'],
                crashOkWrites=r['crashOkWrites'],crashUnknownWrites=r['crashUnknownWrites'],unknownWritesKept=r['unknownWritesKept'],lostOkWrites=r['lostOkWrites'],
                frozenAppliedGap=abs(f[0]-f[1]),newPrimary=r['newPrimary'],failoverMs=r['failoverNs']/1e6,detectWaitMs=r['detectWaitNs']/1e6,
                freezeMs=r['freezeNs']/1e6,repairMs=r['repairNs']/1e6,openMs=r['openNs']/1e6,promotionMs=r['promotionNs']/1e6,repairedLength=r['repairedLength'])


def main():
    p=argparse.ArgumentParser();p.add_argument('raw',type=Path);p.add_argument('--output',type=Path,default=Path(__file__).resolve().parent);args=p.parse_args()
    paths=sorted(args.raw.glob('ROSE-L*-v*-r*.json'));rows=[json.loads(x.read_text(encoding='utf-8')) for x in paths];validate(rows)
    out=args.output;data=out/'rose-results';data.mkdir(parents=True,exist_ok=True);(out/'figures').mkdir(exist_ok=True)
    runs=[summarize(r) for r in rows];summary=[]
    for size in (64,8192):
        for limit in LIMITS:
            g=[x for x in runs if (x['limit'],x['valueBytes'])==(limit,size)];med=lambda k:statistics.median(x[k] for x in g)
            summary.append(dict(limit=limit,valueBytes=size,runs=len(g),normalP50MsMedian=med('normalP50Ms'),normalP99MsMedian=med('normalP99Ms'),
                                opsPerSecondMedian=med('opsPerSecond'),busyRetriesTotal=sum(x['busyRetries'] for x in g),peakLagMax=max(x['peakLag'] for x in g),
                                backpressureTotal=sum(x['backpressureCount'] for x in g),lostOkWritesMax=max(x['lostOkWrites'] for x in g),lostOkWritesTotal=sum(x['lostOkWrites'] for x in g),
                                crashOkWritesTotal=sum(x['crashOkWrites'] for x in g),unknownTotal=sum(x['crashUnknownWrites'] for x in g),
                                failoverMsMedian=med('failoverMs'),failoverMsMin=min(x['failoverMs'] for x in g),failoverMsMax=max(x['failoverMs'] for x in g),
                                repairMsMedian=med('repairMs'),promotionMsMedian=med('promotionMs')))
    for name,records in [('runs',runs),('summary',summary)]:
        with (data/f'{name}.csv').open('w',encoding='utf-8',newline='') as f:
            w=csv.DictWriter(f,fieldnames=list(records[0]),lineterminator='\n');w.writeheader();w.writerows(records)
    meta={'runCount':len(rows),'normalWrites':512*len(rows),'crashOkWrites':sum(x['crashOkWrites'] for x in runs),'lostOkWrites':sum(x['lostOkWrites'] for x in runs),
          'boundViolations':0,'distributionSha256':json.loads((args.raw/'manifest.json').read_text(encoding='utf-8'))['distSha256'],
          'rawSha256':{x.name:hashlib.sha256(x.read_bytes()).hexdigest() for x in paths}}
    (data/'sources.json').write_text(json.dumps(meta,indent=2)+'\n',encoding='utf-8')
    import matplotlib
    matplotlib.use('Agg')
    from matplotlib import pyplot as plt
    fig,axes=plt.subplots(1,3,figsize=(16,5));labels=[('L=0\n(sync)' if l==0 else 'L=∞\n(async)' if l<0 else f'L={l}') for l in LIMITS];colors={64:'#497eb8',8192:'#d29041'}
    for si,size in enumerate((64,8192)):
        for li,limit in enumerate(LIMITS):
            g=[x for x in runs if (x['limit'],x['valueBytes'])==(limit,size)];xs=[li+(si-.5)*.25]*len(g)
            axes[0].scatter(xs,[x['normalP99Ms'] for x in g],color=colors[size],label=f'{size}B p99' if li==0 else None)
            axes[0].scatter(xs,[x['normalP50Ms'] for x in g],color=colors[size],marker='_',label=f'{size}B p50' if li==0 else None)
            axes[1].scatter(xs,[x['lostOkWrites'] for x in g],color=colors[size],label=f'{size}B lost OK writes' if li==0 else None)
            axes[2].scatter(xs,[x['opsPerSecond'] for x in g],color=colors[size],label=f'{size}B' if li==0 else None)
    axes[1].plot(range(4),[max(0,l) for l in LIMITS[:4]],color='#888',linestyle='--',label='bound L')
    axes[0].set_title('8 concurrent writers: SET latency (incl. BUSY retries)');axes[0].set_ylabel('ms')
    axes[1].set_title('Primary SIGKILL during writes: OK-acknowledged writes lost');axes[1].set_ylabel('count')
    axes[2].set_title('8 concurrent writers: observed throughput');axes[2].set_ylabel('ops/s')
    for ax in axes:
        ax.set_xticks(range(5),labels);ax.set_ylim(bottom=0);ax.grid(axis='y',alpha=.2);ax.legend(fontsize=7)
    fig.suptitle('Rosé-inspired single-partition bounded-lag variant on 3 EC2 nodes, 3 runs per condition');fig.tight_layout();fig.savefig(out/'figures/rose-bounded-lag.png');plt.close(fig)
    lines=['# Rosé 단일 파티션 변형 실서버: L별 쓰기 지연·backpressure·crash 유실 상한','',
           '관련 이슈: S15P31D111-187 후속(11단계). `RoseEngine`(bounded-lag Primary-Backup 단일 파티션·op 상한 변형)을 실제 TCP에 연결했다. 원 논문의 다중 파티션 coordinated apply·snapshot·지역 복제·영속 WAL은 재현하지 않았다. 외부 master 역할은 정확한 Primary crash를 아는 controller가 맡는다.','',
           '## 실험 조건','',
           '- 전용 EC2 세 대, 같은 AZ·m6gd.medium·Ubuntu24.04 arm64·Java21·인메모리 KV·private TCP. Primary 0, Backup 1/2, retry.ms=200. controller/client는 Primary 호스트.',
           '- L ∈ {0(전원 적용 ACK 동기), 1, 4, 16, -1(무제한 비동기)} × 값64B/8KiB × 3회 = 30회.',
           '- 정상: warm30 후 closed-loop client 8개가 각각 64건(총512건) SET. 지연은 BUSY 재시도를 포함한 client 관측값이며 peakLag/backpressureCount는 Primary 상태값이다.',
           '- crash: client 8개가 계속 쓰는 중 각 client가 32건 이상 OK를 받은 시점에 Primary JVM SIGKILL → controller가 timeout.ms(500) 고정 대기 → 두 Backup FREEZE → 적용 prefix가 긴 쪽을 새 Primary로 REPAIR → 로그 digest 일치·minAck 확인 → OPEN → 새 Primary 첫 SET OK. OK를 받았는데 새 Primary에 없는 key를 유실로 센다. 응답 없이 끊긴 요청은 unknown으로 따로 센다.','',
           '## 결과 (3회 중앙값, 합·최대는 3회 기준)','',
           '|값 B|L|SET p50/p99 ms|ops/s|BUSY 재시도|peakLag 최대|backpressure 합|crash 전 OK 합|unknown 합|유실 최대/합 (상한 L)|failover ms|repair ms|promotion ms|',
           '|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|']
    for s in summary:
        bound='∞' if s['limit']<0 else str(s['limit'])
        lines.append(f"|{s['valueBytes']}|{bound}|{s['normalP50MsMedian']:.2f}/{s['normalP99MsMedian']:.2f}|{s['opsPerSecondMedian']:.0f}|{s['busyRetriesTotal']}|{s['peakLagMax']}|{s['backpressureTotal']}|{s['crashOkWritesTotal']}|{s['unknownTotal']}|{s['lostOkWritesMax']}/{s['lostOkWritesTotal']} ({bound})|{s['failoverMsMedian']:.1f} ({s['failoverMsMin']:.1f}~{s['failoverMsMax']:.1f})|{s['repairMsMedian']:.1f}|{s['promotionMsMedian']:.1f}|")
    lines+=['','![rose](figures/rose-bounded-lag.png)','',
            f"30회 모두 정상512건 OK·복사본1,536개 일치, crash 전 OK {meta['crashOkWrites']:,}건 중 유실 {meta['lostOkWrites']}건이며 L≥0 조건에서 유실이 L을 넘은 실행은 없었다. L=0은 유실 0이다. 모든 실행에서 생존자 로그 digest 일치, epoch1 쓰기 STALE_EPOCH, Backup 직접 쓰기 redirect를 확인했다.",'',
            '## 해석','',
            '- L=0은 모든 Backup 적용 ACK 뒤 응답하므로 유실이 없고 지연이 가장 길다. L>0은 로컬 적용 뒤 바로 응답하며 Backup 최소 ACK와의 거리가 L에 닿으면 backpressure로 새 쓰기를 미룬다. 유실 수는 crash 순간의 실제 lag이며 상한 L 안에 있다.',
            '- 동시 client 8개에서는 L=1~16 모두 lag가 L에 닿아 backpressure가 자주 발생했고(3회 합 약1,300~1,600회), peakLag 최대는 정확히 L이다. L=∞는 lag가 수백 op까지 커져 crash 한 번에 OK 쓰기 수백 건이 사라졌다. 느린 Backup·원거리 복제 조건은 이번 실측에 없다.',
            '- 정상 p50/p99(약20ms/150~215ms)는 client 8개가 한 Primary를 공유하며 적용 중(BUSY) 재시도를 반복한 결과다(3회 합 BUSY 재시도 약3,500~14,000회). 단일 client 조건의 PB/VR/Raft 지연과 직접 비교하지 않는다. L=∞가 가장 짧은 이유는 Backup ACK를 기다리지 않아 BUSY 구간이 줄기 때문이다.',
            '- failover는 controller의 고정 감지 대기(500ms)를 포함하며 실제 failure detector가 아니다. 새 Primary 선택은 적용 prefix 길이 비교이고 두 Backup 길이가 다르면 짧은 쪽을 peer 경로로 복구한다.',
            '- 단일 파티션·op 상한 변형이므로 Rosé 원 논문의 보장·성능 수치와 직접 비교하지 않는다. PB(전원 ACK)와 L=0이 같은 보장 수준이고 L>0·무제한은 성공 쓰기 유실을 허용하는 다른 보장 수준이다.','',
            '[실행별 CSV](rose-results/runs.csv) · [조건별 요약 CSV](rose-results/summary.csv) · [SHA 근거](rose-results/sources.json)','']
    (out/'rose-results.md').write_text('\n'.join(lines),encoding='utf-8');print(json.dumps({k:v for k,v in meta.items() if k!='rawSha256'},indent=2))


if __name__=='__main__':main()
